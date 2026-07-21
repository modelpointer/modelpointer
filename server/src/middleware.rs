use std::{
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::{Duration, Instant},
};

use axum::{
    Json,
    body::Body,
    extract::{Request, State},
    http::{HeaderMap, HeaderValue, StatusCode, header},
    middleware::Next,
    response::{IntoResponse, Response},
};
use rand::Rng;
use serde::Serialize;
use sha2::{Digest, Sha256};
use tower::{Layer, Service};
use tower_http::trace::{MakeSpan, OnRequest, OnResponse, TraceLayer};
use tracing::{Span, error, field::Empty, info, info_span, warn};

use crate::server::GatewayState;
use modelpointer_core::observability::metrics::{Metrics, method_to_static_str};

#[derive(Clone, Debug)]
pub struct ApiKeyIdentity {
    pub key_id: String,
    /// Populated from the auth record; retained for logging / future use.
    #[allow(dead_code)]
    pub uid: String,
}

/// Middleware to validate Bearer token against configured API key
/// Only active when router has an API key configured
pub async fn auth_middleware(
    State(app_state): State<Arc<GatewayState>>,
    mut request: Request<Body>,
    next: Next,
) -> Response {
    if !app_state.auth_required {
        request.extensions_mut().insert(ApiKeyIdentity {
            key_id: "anonymous".to_string(),
            uid: "anonymous".to_string(),
        });
        return next.run(request).await;
    }

    let repo = &app_state.api_key_repo;
    let Some(token) = extract_api_key(request.headers()) else {
        return create_error(
            StatusCode::UNAUTHORIZED,
            "missing_api_key",
            "Missing API key. Use Authorization: Bearer <token> or x-api-key header.",
        );
    };

    let key_hash = sha256_hex(token);
    let lookup_result = match repo.find_active_by_hash(&key_hash).await {
        Ok(result) => result,
        Err(e) => {
            error!("api key lookup failed: {}", e);
            return create_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "api_key_lookup_failed",
                "API key validation service is temporarily unavailable.",
            );
        }
    };

    match lookup_result {
        Some(record) => {
            let _status = record.status;
            request.extensions_mut().insert(ApiKeyIdentity {
                key_id: record.id,
                uid: record.uid,
            });
        }
        None => {
            return create_error(
                StatusCode::UNAUTHORIZED,
                "invalid_api_key",
                "Invalid, expired, or inactive API key.",
            );
        }
    }

    next.run(request).await
}

pub fn extract_api_key(headers: &HeaderMap) -> Option<&str> {
    if let Some(value) = headers
        .get(header::AUTHORIZATION)
        .and_then(|h| h.to_str().ok())
        && let Some(token) = value.strip_prefix("Bearer ")
        && !token.is_empty()
    {
        return Some(token);
    }

    headers.get("x-api-key").and_then(|h| h.to_str().ok())
}

fn sha256_hex(input: &str) -> String {
    let digest = Sha256::digest(input.as_bytes());
    format!("{digest:x}")
}

/// Alphanumeric characters for request ID generation (as bytes for O(1) indexing)
const REQUEST_ID_CHARS: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";

/// Generate OpenAI-compatible request ID based on endpoint.
fn generate_request_id(path: &str) -> String {
    let prefix = if path.contains("/chat/completions") {
        "chatcmpl-"
    } else if path.contains("/completions") {
        "cmpl-"
    } else if path.contains("/generate") {
        "gnt-"
    } else if path.contains("/responses") {
        "resp-"
    } else {
        "req-"
    };

    // Generate a random string similar to OpenAI's format
    // Use byte array indexing (O(1)) instead of chars().nth() (O(n))
    let mut rng = rand::rng();
    let random_part: String = (0..24)
        .map(|_| {
            let idx = rng.random_range(0..REQUEST_ID_CHARS.len());
            REQUEST_ID_CHARS[idx] as char
        })
        .collect();

    format!("{prefix}{random_part}")
}

/// Extension type for storing request ID
#[derive(Clone, Debug)]
pub struct RequestId(pub String);

/// Tower Layer for request ID middleware
#[derive(Clone)]
pub struct RequestIdLayer {
    headers: Arc<Vec<String>>,
}

impl RequestIdLayer {
    pub fn new(headers: Vec<String>) -> Self {
        Self {
            headers: Arc::new(headers),
        }
    }
}

impl<S> Layer<S> for RequestIdLayer {
    type Service = RequestIdMiddleware<S>;

    fn layer(&self, inner: S) -> Self::Service {
        RequestIdMiddleware {
            inner,
            headers: self.headers.clone(),
        }
    }
}

/// Tower Service for request ID middleware
#[derive(Clone)]
pub struct RequestIdMiddleware<S> {
    inner: S,
    headers: Arc<Vec<String>>,
}

impl<S> Service<Request> for RequestIdMiddleware<S>
where
    S: Service<Request, Response = Response> + Send + 'static,
    S::Future: Send + 'static,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future =
        Pin<Box<dyn std::future::Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, mut req: Request) -> Self::Future {
        let headers = self.headers.clone();

        // Extract request ID from headers or generate new one
        let mut request_id = None;

        for header_name in headers.iter() {
            if let Some(header_value) = req.headers().get(header_name)
                && let Ok(value) = header_value.to_str()
            {
                request_id = Some(value.to_string());
                break;
            }
        }

        let request_id = request_id.unwrap_or_else(|| generate_request_id(req.uri().path()));

        // Insert request ID into request extensions for other middleware/handlers to use
        req.extensions_mut().insert(RequestId(request_id.clone()));

        // Call the inner service
        let future = self.inner.call(req);

        Box::pin(async move {
            let mut response = future.await?;

            // Add request ID to response headers
            response.headers_mut().insert(
                "x-request-id",
                HeaderValue::from_str(&request_id)
                    .unwrap_or_else(|_| HeaderValue::from_static("invalid-request-id")),
            );

            Ok(response)
        })
    }
}

/// Custom span maker that includes request ID
#[derive(Clone, Debug)]
pub struct RequestSpan;

impl<B> MakeSpan<B> for RequestSpan {
    fn make_span(&mut self, request: &Request<B>) -> Span {
        // Don't try to extract request ID here - it won't be available yet
        // The RequestIdLayer runs after TraceLayer creates the span
        info_span!(
            target: "modelpointer::otel-trace",
            "http_request",
            method = %request.method(),
            uri = %request.uri(),
            version = ?request.version(),
            request_id = Empty,  // Will be set later
            status_code = Empty,
            latency = Empty,
            error = Empty,
            module = "modelpointer"
        )
    }
}

/// Custom on_request handler
#[derive(Clone, Debug)]
pub struct RequestLogger;

impl<B> OnRequest<B> for RequestLogger {
    fn on_request(&mut self, request: &Request<B>, span: &Span) {
        let _enter = span.enter();

        // Try to get the request ID from extensions
        // This will work if RequestIdLayer has already run
        if let Some(request_id) = request.extensions().get::<RequestId>() {
            span.record("request_id", request_id.0.as_str());
        }

        let method = method_to_static_str(request.method().as_str());
        let path = normalize_path_for_metrics(request.uri().path());
        Metrics::record_http_request(method, &path);

        // Log the request start
        info!(
            target: "modelpointer::request",
            "started processing request"
        );
    }
}

pub const HEADER_X_MP_ERROR_CODE: &str = "X-MP-Error-Code";

#[derive(Serialize)]
struct ErrorResponse<'a> {
    error: ErrorDetail<'a>,
}

#[derive(Serialize)]
struct ErrorDetail<'a> {
    #[serde(rename = "type")]
    error_type: &'static str,
    code: &'a str,
    message: &'a str,
}

pub fn create_error(
    status: StatusCode,
    code: impl Into<String>,
    message: impl Into<String>,
) -> Response {
    let code_str = code.into();
    let message_str = message.into();

    let mut headers = HeaderMap::with_capacity(1);
    headers.insert(
        HEADER_X_MP_ERROR_CODE,
        HeaderValue::from_str(&code_str).unwrap(),
    );

    (
        status,
        headers,
        Json(ErrorResponse {
            error: ErrorDetail {
                error_type: status_code_to_str(status),
                code: &code_str,
                message: &message_str,
            },
        }),
    )
        .into_response()
}

fn status_code_to_str(status_code: StatusCode) -> &'static str {
    status_code
        .canonical_reason()
        .unwrap_or("Unknown Status Code")
}

pub fn extract_error_code_from_response<B>(response: &Response<B>) -> &str {
    response
        .headers()
        .get(HEADER_X_MP_ERROR_CODE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default()
}

/// Custom on_response handler
#[derive(Clone, Debug)]
pub struct ResponseLogger {
    _start_time: Instant,
}

impl Default for ResponseLogger {
    fn default() -> Self {
        Self {
            _start_time: Instant::now(),
        }
    }
}

impl<B> OnResponse<B> for ResponseLogger {
    fn on_response(self, response: &Response<B>, latency: Duration, span: &Span) {
        let status = response.status();
        let status_code = status.as_u16();

        let error_code = extract_error_code_from_response(response);

        // Layer 1: HTTP metrics
        Metrics::record_http_response(status_code, error_code);

        // Record these in the span for structured logging/observability tools
        span.record("status_code", status_code);
        // Use microseconds as integer to avoid format! string allocation
        span.record("latency", latency.as_micros() as u64);

        // Log the response completion
        let _enter = span.enter();
        if status.is_server_error() {
            error!(
                target: "modelpointer::response",
                "request failed with server error"
            );
        } else if status.is_client_error() {
            warn!(
                target: "modelpointer::response",
                "request failed with client error"
            );
        } else {
            info!(
                target: "modelpointer::response",
                "finished processing request"
            );
        }
    }
}

/// Create a configured TraceLayer for HTTP logging
/// Note: Actual request/response logging with request IDs is done in RequestIdService
pub fn create_logging_layer() -> TraceLayer<
    tower_http::classify::SharedClassifier<tower_http::classify::ServerErrorsAsFailures>,
    RequestSpan,
    RequestLogger,
    ResponseLogger,
> {
    TraceLayer::new_for_http()
        .make_span_with(RequestSpan)
        .on_request(RequestLogger)
        .on_response(ResponseLogger::default())
}

/// Normalize path for metrics to avoid high cardinality.
/// Replaces dynamic segments (IDs, UUIDs) with `{id}` placeholder.
/// Only allocates when normalization is needed; uses single-pass with byte offsets.
fn normalize_path_for_metrics(path: &str) -> String {
    let bytes = path.as_bytes();
    let mut segment_start = 0;
    let mut segment_idx = 0;
    let mut result: Option<String> = None;

    for (pos, &b) in bytes.iter().enumerate() {
        if b == b'/' || pos == bytes.len() - 1 {
            // Determine segment end (include last char if not a slash)
            let segment_end = if b == b'/' { pos } else { pos + 1 };
            let segment = &path[segment_start..segment_end];

            // Check segments after index 2 for dynamic IDs
            if segment_idx > 2 && !segment.is_empty() && is_dynamic_id(segment) {
                // Initialize result with everything before this segment
                let result = result.get_or_insert_with(|| {
                    let mut s = String::with_capacity(path.len());
                    s.push_str(&path[..segment_start]);
                    s
                });
                result.push_str("{id}");
            } else if let Some(ref mut r) = result {
                // Already normalizing, append this segment as-is
                r.push_str(segment);
            }

            // Add slash after segment (except at end)
            if b == b'/' {
                if let Some(ref mut r) = result {
                    r.push('/');
                }
                segment_start = pos + 1;
                segment_idx += 1;
            }
        }
    }

    result.unwrap_or_else(|| path.to_owned())
}

/// Check if segment looks like a dynamic ID (prefixed ID, UUID, or numeric).
#[inline]
fn is_dynamic_id(s: &str) -> bool {
    // Prefixed IDs: resp_xxx, chatcmpl_xxx (len > 10 with underscore)
    if s.len() > 10 && s.contains('_') {
        return true;
    }
    // UUIDs: 32+ hex chars with dashes
    if s.len() >= 32 && s.bytes().all(|b| b.is_ascii_hexdigit() || b == b'-') {
        return true;
    }
    // Numeric IDs
    !s.is_empty() && s.bytes().all(|b| b.is_ascii_digit())
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── is_dynamic_id ─────────────────────────────────────────────────────────

    #[test]
    fn dynamic_id_numeric() {
        assert!(is_dynamic_id("12345"));
        assert!(is_dynamic_id("0"));
    }

    #[test]
    fn dynamic_id_uuid_hex() {
        assert!(is_dynamic_id("550e8400e29b41d4a716446655440000")); // 32 hex chars
        assert!(is_dynamic_id("550e8400-e29b-41d4-a716-446655440000")); // UUID with dashes
    }

    #[test]
    fn dynamic_id_prefixed_long_id() {
        assert!(is_dynamic_id("resp_abc1234567890")); // > 10 chars, has _
        assert!(is_dynamic_id("chatcmpl_abc123456789")); // > 10 chars, has _
    }

    #[test]
    fn dynamic_id_short_with_underscore_not_id() {
        assert!(!is_dynamic_id("resp_abc")); // only 8 chars, not > 10
    }

    #[test]
    fn dynamic_id_regular_words_not_id() {
        assert!(!is_dynamic_id("chat"));
        assert!(!is_dynamic_id("completions"));
        assert!(!is_dynamic_id("v1"));
        assert!(!is_dynamic_id("messages"));
    }

    #[test]
    fn dynamic_id_empty_string_not_id() {
        assert!(!is_dynamic_id(""));
    }

    // ── normalize_path_for_metrics ────────────────────────────────────────────

    #[test]
    fn normalize_static_paths_unchanged() {
        assert_eq!(
            normalize_path_for_metrics("/v1/chat/completions"),
            "/v1/chat/completions"
        );
        assert_eq!(normalize_path_for_metrics("/v1/messages"), "/v1/messages");
        assert_eq!(
            normalize_path_for_metrics("/v1/embeddings"),
            "/v1/embeddings"
        );
        assert_eq!(normalize_path_for_metrics("/health"), "/health");
    }

    #[test]
    fn normalize_replaces_prefixed_id_in_fourth_segment() {
        // /v1/responses/<id> — id is at segment index 3
        assert_eq!(
            normalize_path_for_metrics("/v1/responses/resp_abc1234567890"),
            "/v1/responses/{id}"
        );
    }

    #[test]
    fn normalize_replaces_numeric_id_with_trailing_segment() {
        assert_eq!(
            normalize_path_for_metrics("/v1/threads/123456/messages"),
            "/v1/threads/{id}/messages"
        );
    }

    #[test]
    fn normalize_replaces_uuid_segment() {
        assert_eq!(
            normalize_path_for_metrics("/v1/responses/550e8400e29b41d4a716446655440000"),
            "/v1/responses/{id}"
        );
    }

    #[test]
    fn normalize_three_segment_path_not_changed() {
        // /v1/responses has segments at idx 0,1,2 — none qualify (need idx > 2)
        assert_eq!(normalize_path_for_metrics("/v1/responses"), "/v1/responses");
    }

    #[test]
    fn normalize_id_in_second_segment_not_replaced() {
        // "12345" is at segment index 1, only idx > 2 are replaced
        assert_eq!(
            normalize_path_for_metrics("/12345/completions"),
            "/12345/completions"
        );
    }

    #[test]
    fn normalize_chatcmpl_id() {
        assert_eq!(
            normalize_path_for_metrics("/v1/responses/chatcmpl_abc123456789/cancel"),
            "/v1/responses/{id}/cancel"
        );
    }
}
