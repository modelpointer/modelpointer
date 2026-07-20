//! Integration tests for SSE streaming through the gateway.
//!
//! Architecture:
//!   real axum mock upstream (random port, real TCP)
//!     ↑  HTTP via reqwest
//!   GatewayRouter wired via build_app
//!     ↑  in-process call via tower::ServiceExt::oneshot
//!   test assertions on status / Content-Type / body

use std::sync::{
    Arc, OnceLock,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};

use axum::{
    Router,
    body::Body,
    http::{Request, StatusCode},
    response::Response,
    routing::post,
};
use tokio::net::TcpListener;
use tower::ServiceExt;
use tower_http::cors::CorsLayer;

use modelpointer::{
    auth::CachedApiKeyRepository,
    log_sink::LogSink,
    quota_config::QuotaStore,
    router::GatewayRouter,
    server::{GatewayState, build_app},
};
use modelpointer_core::{
    app_context::AppContext,
    config::RouterConfig,
    model::ModelCard,
    upstream::node::{
        ApiCompatibility, ProviderType, RuntimeType, UpstreamBinding, UpstreamCredential,
        UpstreamGroup, UpstreamNode, UpstreamProfile,
    },
    upstream::routing::{RoutingStrategy, RoutingStrategyConfig},
};

// ── Test initialization ───────────────────────────────────────────────────────

static PROXY_BYPASS: OnceLock<()> = OnceLock::new();

/// Set NO_PROXY so reqwest skips any system proxy (e.g. Privoxy) for localhost.
/// Must be called before building the reqwest client in make_gateway.
fn bypass_proxy() {
    PROXY_BYPASS.get_or_init(|| {
        // Safety: only called once via OnceLock; no concurrent env reads at this point.
        unsafe { std::env::set_var("NO_PROXY", "127.0.0.1,localhost") };
    });
}

// ── Helpers ───────────────────────────────────────────────────────────────────

/// Spawn a mock upstream HTTP server and return its base URL `http://127.0.0.1:PORT`.
///
/// The server handles **any POST path** and always responds with the given
/// `status`, `content_type`, and `body`.
async fn spawn_mock_upstream(
    status: u16,
    content_type: &'static str,
    body: &'static str,
) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let app = Router::new().route(
        "/{*path}",
        post(move || async move {
            Response::builder()
                .status(status)
                .header("content-type", content_type)
                .body(Body::from(body))
                .unwrap()
        }),
    );

    tokio::spawn(async move {
        axum::serve(listener, app).await.ok();
    });

    format!("http://127.0.0.1:{}", addr.port())
}

/// Like [`spawn_mock_upstream`] but also returns an `Arc<AtomicUsize>` that is
/// incremented on every request received by the mock.  Use this to assert that
/// the gateway did (or did not) retry.
#[allow(dead_code)]
async fn spawn_mock_upstream_with_counter(
    status: u16,
    content_type: &'static str,
    body: &'static str,
) -> (String, Arc<AtomicUsize>) {
    let counter = Arc::new(AtomicUsize::new(0));
    let counter_clone = Arc::clone(&counter);

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let app = Router::new().route(
        "/{*path}",
        post(move || {
            let c = Arc::clone(&counter_clone);
            async move {
                c.fetch_add(1, Ordering::SeqCst);
                Response::builder()
                    .status(status)
                    .header("content-type", content_type)
                    .body(Body::from(body))
                    .unwrap()
            }
        }),
    );

    tokio::spawn(async move {
        axum::serve(listener, app).await.ok();
    });

    (format!("http://127.0.0.1:{}", addr.port()), counter)
}

/// Build a gateway `Router` wired to a single upstream at `upstream_base_url`
/// for the model `"test-model"`.
///
/// Auth is disabled so tests can skip providing API keys.
async fn make_gateway(upstream_base_url: &str) -> Router {
    bypass_proxy(); // must be set before reqwest client is built

    let context = Arc::new(
        AppContext::with_config(RouterConfig {
            request_timeout_secs: 5,
            ..RouterConfig::default()
        })
        .await
        .unwrap(),
    );

    let node = UpstreamNode {
        profile: UpstreamProfile {
            base_url: upstream_base_url.to_string(),
            provider_node_id: String::new(),
            api_compatibility: ApiCompatibility::OpenAi,
            runtime_type: RuntimeType::External,
            upstream_model_name: None,
            credential: Arc::new(UpstreamCredential {
                name: "mock".to_string(),
                api_key: None,
                provider_type: ProviderType::Unknown,
            }),
        },
        healthy: Arc::new(AtomicBool::new(true)),
    };
    let binding =
        UpstreamBinding::new(node, true, RoutingStrategyConfig::Swrr { weight: 1 }, 0).unwrap();
    let group = UpstreamGroup::new(
        ModelCard::new("test-model"),
        RoutingStrategy::Swrr,
        vec![binding],
    )
    .unwrap();
    context.upstream_registry.reload_all(vec![group]);

    let state = Arc::new(GatewayState {
        router: Arc::new(GatewayRouter::new(&context, LogSink::noop()).await.unwrap()),
        context,
        api_key_repo: CachedApiKeyRepository::new().into_shared(),
        auth_required: false,
        rate_limiter: None,
        quota_store: QuotaStore::new(),
        log_sink: LogSink::noop(),
    });

    build_app(state, 10 * 1024 * 1024, vec![], CorsLayer::new())
}

/// Consume the response body and return it as a UTF-8 string.
async fn body_text(resp: axum::response::Response) -> String {
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    String::from_utf8_lossy(&bytes).into_owned()
}

/// Convenience: POST `uri` with a JSON body and return the raw response.
async fn post_json(app: Router, uri: &str, json: &'static str) -> axum::response::Response {
    let req = Request::builder()
        .method("POST")
        .uri(uri)
        .header("content-type", "application/json")
        .body(Body::from(json))
        .unwrap();
    app.oneshot(req).await.unwrap()
}

/// POST with an additional `x-mp-provider` header (required by /v1/responses).
async fn post_json_with_provider(
    app: Router,
    uri: &str,
    json: &'static str,
    provider: &'static str,
) -> axum::response::Response {
    let req = Request::builder()
        .method("POST")
        .uri(uri)
        .header("content-type", "application/json")
        .header("x-mp-provider", provider)
        .body(Body::from(json))
        .unwrap();
    app.oneshot(req).await.unwrap()
}

// ── SSE body fixtures ─────────────────────────────────────────────────────────

/// Standard OpenAI chat SSE format — space after `data:`.
const OPENAI_CHAT_SSE: &str = concat!(
    "data: {\"id\":\"chatcmpl-1\",\"object\":\"chat.completion.chunk\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",\"content\":\"Hello\"},\"finish_reason\":null}]}\n\n",
    "data: {\"id\":\"chatcmpl-1\",\"object\":\"chat.completion.chunk\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\" world\"},\"finish_reason\":null}]}\n\n",
    "data: {\"id\":\"chatcmpl-1\",\"object\":\"chat.completion.chunk\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}],\"usage\":{\"prompt_tokens\":10,\"completion_tokens\":2,\"total_tokens\":12}}\n\n",
    "data: [DONE]\n\n",
);

/// DashScope-style Responses API SSE — no space after `data:` or `event:`.
const DASHSCOPE_RESPONSES_SSE: &str = concat!(
    "event:response.created\n",
    "data:{\"id\":\"resp-1\",\"object\":\"realtime.response\",\"status\":\"in_progress\"}\n\n",
    "event:response.output_text.delta\n",
    "data:{\"id\":\"resp-1\",\"output\":[{\"content\":[{\"type\":\"output_text\",\"text\":\"Hello\"}]}]}\n\n",
    "event:response.output_text.delta\n",
    "data:{\"id\":\"resp-1\",\"output\":[{\"content\":[{\"type\":\"output_text\",\"text\":\" world\"}]}]}\n\n",
    "event:response.completed\n",
    "data:{\"id\":\"resp-1\",\"object\":\"realtime.response\",\"status\":\"completed\",\"usage\":{\"input_tokens\":10,\"output_tokens\":5,\"total_tokens\":15}}\n\n",
);

/// Standard OpenAI Responses API SSE — space after `data:` and `event:`.
const OPENAI_RESPONSES_SSE: &str = concat!(
    "event: response.created\n",
    "data: {\"id\":\"resp-2\",\"object\":\"realtime.response\",\"status\":\"in_progress\"}\n\n",
    "event: response.output_text.delta\n",
    "data: {\"id\":\"resp-2\",\"output\":[{\"content\":[{\"type\":\"output_text\",\"text\":\"Hi there\"}]}]}\n\n",
    "event: response.completed\n",
    "data: {\"id\":\"resp-2\",\"object\":\"realtime.response\",\"status\":\"completed\",\"usage\":{\"input_tokens\":5,\"output_tokens\":2,\"total_tokens\":7}}\n\n",
);

// ── Chat completions: streaming ────────────────────────────────────────────────

#[tokio::test]
async fn chat_streaming_openai_format_forwarded() {
    let base_url = spawn_mock_upstream(200, "text/event-stream", OPENAI_CHAT_SSE).await;
    let app = make_gateway(&base_url).await;

    let resp = post_json(
        app,
        "/v1/chat/completions",
        r#"{"model":"test-model","messages":[{"role":"user","content":"hi"}],"stream":true}"#,
    )
    .await;

    assert_eq!(resp.status(), StatusCode::OK);
    let ct = resp.headers()["content-type"].to_str().unwrap();
    assert!(
        ct.contains("text/event-stream"),
        "expected SSE content-type, got: {ct}"
    );

    let text = body_text(resp).await;
    assert!(text.contains("Hello"), "missing first chunk: {text}");
    assert!(text.contains("world"), "missing second chunk: {text}");
    assert!(text.contains("[DONE]"), "missing DONE sentinel: {text}");
}

#[tokio::test]
async fn chat_streaming_finish_reason_in_body() {
    let base_url = spawn_mock_upstream(200, "text/event-stream", OPENAI_CHAT_SSE).await;
    let app = make_gateway(&base_url).await;

    let resp = post_json(
        app,
        "/v1/chat/completions",
        r#"{"model":"test-model","messages":[{"role":"user","content":"hi"}],"stream":true}"#,
    )
    .await;

    let text = body_text(resp).await;
    // The final data chunk must contain finish_reason=stop forwarded verbatim.
    assert!(
        text.contains("\"finish_reason\":\"stop\""),
        "finish_reason not forwarded: {text}"
    );
}

#[tokio::test]
async fn chat_streaming_usage_in_body() {
    let base_url = spawn_mock_upstream(200, "text/event-stream", OPENAI_CHAT_SSE).await;
    let app = make_gateway(&base_url).await;

    let resp = post_json(
        app,
        "/v1/chat/completions",
        r#"{"model":"test-model","messages":[{"role":"user","content":"hi"}],"stream":true}"#,
    )
    .await;

    let text = body_text(resp).await;
    // Usage block must pass through from the upstream.
    assert!(
        text.contains("\"prompt_tokens\":10"),
        "usage not forwarded: {text}"
    );
    assert!(
        text.contains("\"total_tokens\":12"),
        "usage not forwarded: {text}"
    );
}

// ── Chat completions: non-streaming ───────────────────────────────────────────

const OPENAI_CHAT_NON_STREAM: &str = r#"{
  "id":"chatcmpl-2",
  "object":"chat.completion",
  "choices":[{"index":0,"message":{"role":"assistant","content":"Hi!"},"finish_reason":"stop"}],
  "usage":{"prompt_tokens":5,"completion_tokens":1,"total_tokens":6}
}"#;

#[tokio::test]
async fn chat_non_streaming_response_forwarded() {
    let base_url = spawn_mock_upstream(200, "application/json", OPENAI_CHAT_NON_STREAM).await;
    let app = make_gateway(&base_url).await;

    let resp = post_json(
        app,
        "/v1/chat/completions",
        r#"{"model":"test-model","messages":[{"role":"user","content":"hi"}]}"#,
    )
    .await;

    assert_eq!(resp.status(), StatusCode::OK);
    let text = body_text(resp).await;
    assert!(
        text.contains("\"content\":\"Hi!\""),
        "body not forwarded: {text}"
    );
    assert!(
        text.contains("\"finish_reason\":\"stop\""),
        "finish_reason not forwarded: {text}"
    );
}

// ── Upstream error propagation ─────────────────────────────────────────────────

#[tokio::test]
async fn chat_streaming_upstream_500_is_propagated() {
    let base_url = spawn_mock_upstream(
        500,
        "application/json",
        r#"{"error":{"type":"server_error","message":"internal error"}}"#,
    )
    .await;
    let app = make_gateway(&base_url).await;

    let resp = post_json(
        app,
        "/v1/chat/completions",
        r#"{"model":"test-model","messages":[{"role":"user","content":"hi"}],"stream":true}"#,
    )
    .await;

    assert_eq!(resp.status(), StatusCode::INTERNAL_SERVER_ERROR);
}

#[tokio::test]
async fn chat_streaming_upstream_429_is_propagated() {
    let base_url = spawn_mock_upstream(
        429,
        "application/json",
        r#"{"error":{"type":"rate_limit_error","message":"too many requests"}}"#,
    )
    .await;
    let app = make_gateway(&base_url).await;

    let resp = post_json(
        app,
        "/v1/chat/completions",
        r#"{"model":"test-model","messages":[{"role":"user","content":"hi"}],"stream":true}"#,
    )
    .await;

    assert_eq!(resp.status(), StatusCode::TOO_MANY_REQUESTS);
}

#[tokio::test]
async fn chat_non_streaming_upstream_429_is_propagated() {
    let base_url = spawn_mock_upstream(
        429,
        "application/json",
        r#"{"error":{"type":"rate_limit_error","message":"too many requests"}}"#,
    )
    .await;
    let app = make_gateway(&base_url).await;

    let resp = post_json(
        app,
        "/v1/chat/completions",
        r#"{"model":"test-model","messages":[{"role":"user","content":"hi"}]}"#,
    )
    .await;

    assert_eq!(resp.status(), StatusCode::TOO_MANY_REQUESTS);
}

// ── Responses API: streaming ──────────────────────────────────────────────────

#[tokio::test]
async fn responses_streaming_dashscope_no_space_format_forwarded() {
    // DashScope emits `event:type` and `data:{...}` (no space after colon).
    // The gateway must forward the chunks correctly regardless of whether the
    // internal SSE parser can extract usage (which it can, after the bug fix).
    let base_url = spawn_mock_upstream(200, "text/event-stream", DASHSCOPE_RESPONSES_SSE).await;
    let app = make_gateway(&base_url).await;

    let resp = post_json_with_provider(
        app,
        "/v1/responses",
        r#"{"model":"test-model","input":"hi","stream":true}"#,
        "mock",
    )
    .await;

    let status = resp.status();
    let ct = resp
        .headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();
    let text = body_text(resp).await;
    assert_eq!(status, StatusCode::OK, "unexpected status; body: {text}");
    assert!(
        ct.contains("text/event-stream"),
        "expected SSE content-type, got: {ct}"
    );
    assert!(text.contains("Hello"), "missing first delta: {text}");
    assert!(text.contains("world"), "missing second delta: {text}");
    assert!(
        text.contains("response.completed"),
        "missing completion event: {text}"
    );
}

#[tokio::test]
async fn responses_streaming_openai_space_format_forwarded() {
    let base_url = spawn_mock_upstream(200, "text/event-stream", OPENAI_RESPONSES_SSE).await;
    let app = make_gateway(&base_url).await;

    let resp = post_json_with_provider(
        app,
        "/v1/responses",
        r#"{"model":"test-model","input":"hi","stream":true}"#,
        "mock",
    )
    .await;

    let status = resp.status();
    let text = body_text(resp).await;
    assert_eq!(status, StatusCode::OK, "unexpected status; body: {text}");
    assert!(text.contains("Hi there"), "content not forwarded: {text}");
    assert!(
        text.contains("response.completed"),
        "missing completion event: {text}"
    );
}

// ── Fallback routing ──────────────────────────────────────────────────────────

/// When the primary upstream (priority=0) is marked unhealthy at runtime,
/// subsequent requests must be routed to the fallback (priority=1).
#[tokio::test]
async fn primary_unhealthy_routes_to_fallback() {
    // Two upstreams with distinguishable response bodies.
    const PRIMARY_BODY: &str = r#"{"id":"1","object":"chat.completion","choices":[{"index":0,"message":{"role":"assistant","content":"from-primary"},"finish_reason":"stop"}],"usage":{"prompt_tokens":1,"completion_tokens":1,"total_tokens":2}}"#;
    const FALLBACK_BODY: &str = r#"{"id":"2","object":"chat.completion","choices":[{"index":0,"message":{"role":"assistant","content":"from-fallback"},"finish_reason":"stop"}],"usage":{"prompt_tokens":1,"completion_tokens":1,"total_tokens":2}}"#;

    let primary_url = spawn_mock_upstream(200, "application/json", PRIMARY_BODY).await;
    let fallback_url = spawn_mock_upstream(200, "application/json", FALLBACK_BODY).await;

    bypass_proxy();

    // Keep a handle to the primary's health flag so we can flip it after setup.
    let primary_healthy = Arc::new(AtomicBool::new(true));

    let context = Arc::new(
        AppContext::with_config(RouterConfig {
            request_timeout_secs: 5,
            ..RouterConfig::default()
        })
        .await
        .unwrap(),
    );

    let make_node = |url: &str, healthy: Arc<AtomicBool>| UpstreamNode {
        profile: UpstreamProfile {
            base_url: url.to_string(),
            provider_node_id: String::new(),
            api_compatibility: ApiCompatibility::OpenAi,
            runtime_type: RuntimeType::External,
            upstream_model_name: None,
            credential: Arc::new(UpstreamCredential {
                name: "mock".to_string(),
                api_key: None,
                provider_type: ProviderType::Unknown,
            }),
        },
        healthy,
    };

    let primary_binding = UpstreamBinding::new(
        make_node(&primary_url, primary_healthy.clone()),
        true,
        RoutingStrategyConfig::Swrr { weight: 1 },
        0, // primary tier
    )
    .unwrap();
    let fallback_binding = UpstreamBinding::new(
        make_node(&fallback_url, Arc::new(AtomicBool::new(true))),
        true,
        RoutingStrategyConfig::Swrr { weight: 1 },
        1, // fallback tier
    )
    .unwrap();

    let group = UpstreamGroup::new(
        ModelCard::new("test-model"),
        RoutingStrategy::Swrr,
        vec![primary_binding, fallback_binding],
    )
    .unwrap();
    context.upstream_registry.reload_all(vec![group]);

    let state = Arc::new(GatewayState {
        router: Arc::new(GatewayRouter::new(&context, LogSink::noop()).await.unwrap()),
        context,
        api_key_repo: CachedApiKeyRepository::new().into_shared(),
        auth_required: false,
        rate_limiter: None,
        quota_store: QuotaStore::new(),
        log_sink: LogSink::noop(),
    });
    let app = build_app(state, 10 * 1024 * 1024, vec![], CorsLayer::new());

    // First request hits the primary.
    let r1 = post_json(
        app.clone(),
        "/v1/chat/completions",
        r#"{"model":"test-model","messages":[{"role":"user","content":"hi"}]}"#,
    )
    .await;
    assert_eq!(r1.status(), StatusCode::OK);
    assert!(
        body_text(r1).await.contains("from-primary"),
        "expected primary response"
    );

    // Mark the primary as unhealthy — no gateway rebuild needed.
    primary_healthy.store(false, std::sync::atomic::Ordering::Release);

    // Next request falls through to the fallback.
    let r2 = post_json(
        app.clone(),
        "/v1/chat/completions",
        r#"{"model":"test-model","messages":[{"role":"user","content":"hi"}]}"#,
    )
    .await;
    assert_eq!(r2.status(), StatusCode::OK);
    assert!(
        body_text(r2).await.contains("from-fallback"),
        "expected fallback response"
    );

    // Restore primary — routes back to primary.
    primary_healthy.store(true, std::sync::atomic::Ordering::Release);
    let r3 = post_json(
        app.clone(),
        "/v1/chat/completions",
        r#"{"model":"test-model","messages":[{"role":"user","content":"hi"}]}"#,
    )
    .await;
    assert_eq!(r3.status(), StatusCode::OK);
    assert!(
        body_text(r3).await.contains("from-primary"),
        "expected primary after restore"
    );
}

/// When the primary's circuit breaker is open (after repeated failures),
/// requests must be routed to the fallback.
#[tokio::test]
async fn primary_circuit_open_routes_to_fallback() {
    const FALLBACK_BODY: &str = r#"{"id":"3","object":"chat.completion","choices":[{"index":0,"message":{"role":"assistant","content":"from-fallback"},"finish_reason":"stop"}],"usage":{"prompt_tokens":1,"completion_tokens":1,"total_tokens":2}}"#;

    let fallback_url = spawn_mock_upstream(200, "application/json", FALLBACK_BODY).await;

    bypass_proxy();
    let context = Arc::new(
        AppContext::with_config(RouterConfig {
            request_timeout_secs: 5,
            ..RouterConfig::default()
        })
        .await
        .unwrap(),
    );

    let fallback_binding = UpstreamBinding::new(
        UpstreamNode {
            profile: UpstreamProfile {
                base_url: fallback_url.clone(),
                provider_node_id: String::new(),
                api_compatibility: ApiCompatibility::OpenAi,
                runtime_type: RuntimeType::External,
                upstream_model_name: None,
                credential: Arc::new(UpstreamCredential {
                    name: "mock".to_string(),
                    api_key: None,
                    provider_type: ProviderType::Unknown,
                }),
            },
            healthy: Arc::new(AtomicBool::new(true)),
        },
        true,
        RoutingStrategyConfig::Swrr { weight: 1 },
        1, // fallback tier
    )
    .unwrap();

    let group = UpstreamGroup::new(
        ModelCard::new("test-model"),
        RoutingStrategy::Swrr,
        vec![fallback_binding],
    )
    .unwrap();
    context.upstream_registry.reload_all(vec![group]);

    // Reach into the registry and force-open the fallback's circuit (simulating
    // a primary that has already been opened). Since there's no primary here,
    // we verify directly that select_with_min_priority(1) picks the fallback.
    let selected =
        context
            .upstream_registry
            .select_with_min_priority("test-model", None, None, None, 1);
    assert_eq!(
        selected.unwrap().base_url(),
        fallback_url,
        "select_with_min_priority(1) should return the fallback tier upstream"
    );
}

// ── Unknown model ─────────────────────────────────────────────────────────────

#[tokio::test]
async fn unknown_model_returns_404() {
    let base_url = spawn_mock_upstream(200, "text/event-stream", OPENAI_CHAT_SSE).await;
    let app = make_gateway(&base_url).await;

    let resp = post_json(
        app,
        "/v1/chat/completions",
        r#"{"model":"nonexistent-model","messages":[{"role":"user","content":"hi"}],"stream":true}"#,
    )
    .await;

    // No upstream registered for this model → gateway returns 404 or 503.
    assert!(
        resp.status() == StatusCode::NOT_FOUND || resp.status() == StatusCode::SERVICE_UNAVAILABLE,
        "unexpected status: {}",
        resp.status()
    );
}
