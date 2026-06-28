// Copyright 2023-2024 SGLang Team
// Copyright 2026 ModelPointer
//
// SPDX-License-Identifier: Apache-2.0
//
// This file is adapted from sgl-model-gateway/src/observability/metrics.rs in the
// SGLang project (https://github.com/sgl-project/sglang).

use std::{
    borrow::Cow,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    sync::{atomic::{AtomicUsize, Ordering}, Arc},
    time::Duration,
};

use dashmap::{mapref::entry::Entry, DashMap};
use metrics::{counter, describe_counter, describe_gauge, describe_histogram, gauge, histogram};
use metrics_exporter_prometheus::{Matcher, PrometheusBuilder};
use once_cell::sync::Lazy;

// =============================================================================
// STRING INTERNING
// =============================================================================
//
// Dynamic strings (model_id, worker URLs, paths) are interned to avoid repeated
// heap allocations. The interner uses Arc<str> which is cheap to clone and
// allows the metrics crate to store references without repeated allocations.
//
// Performance characteristics:
// - First occurrence: One allocation + DashMap insert
// - Subsequent occurrences: DashMap lookup + Arc::clone (very cheap)
// - Memory: Strings are never freed (acceptable for bounded label cardinality)
//
// Safety cap: MAX_INTERNER_ENTRIES limits growth from high-cardinality inputs
// (e.g. arbitrary model_id values sent by attackers). Known strings already
// interned continue to be served from the fast path; new unknown strings
// beyond the cap are returned as "other" without being inserted.
// TODO: normalize unregistered model_id to "unknown" at call sites to avoid
// filling the interner with invalid model names before the cap is reached.

/// Hard limit on the number of entries in the string interner.
/// Acts as a safety net against DoS via high-cardinality label injection.
const MAX_INTERNER_ENTRIES: usize = 4096;

/// Global string interner for metric labels.
/// Uses DashMap for lock-free concurrent access.
static STRING_INTERNER: Lazy<DashMap<String, Arc<str>>> = Lazy::new(DashMap::new);

/// Atomic counter tracking the number of entries actually inserted into
/// STRING_INTERNER.  Only the thread that wins the DashMap `Vacant` slot
/// increments this counter, so it never drifts above the real map size.
/// This gives a strict cap without the N-1 over-count that occurred when
/// fetch_add was done before entry() and multiple threads raced on the same key.
static INTERNER_COUNT: AtomicUsize = AtomicUsize::new(0);

/// Intern a string, returning a cheaply-cloneable Arc<str>.
///
/// This function is designed for high-throughput scenarios where the same
/// strings (model IDs, worker URLs) appear repeatedly. The first call allocates,
/// subsequent calls just clone the Arc (very cheap - just a ref count increment).
pub(crate) fn intern_string(s: &str) -> Arc<str> {
    // Fast path: already interned (shared read, no exclusive lock).
    if let Some(v) = STRING_INTERNER.get(s) {
        return Arc::clone(v.value());
    }

    // Slow path: acquire the DashMap shard lock via entry().
    //
    // Only the thread that obtains the Vacant slot increments INTERNER_COUNT.
    // Concurrent threads racing on the same key will see Occupied after the
    // winner releases the shard lock — the counter is never incremented more
    // than once per unique key, eliminating the previous N-1 drift.
    match STRING_INTERNER.entry(s.to_string()) {
        Entry::Occupied(e) => Arc::clone(e.get()),
        Entry::Vacant(e) => {
            // Claim a slot. Undo if the cap is already reached.
            let prev = INTERNER_COUNT.fetch_add(1, Ordering::Relaxed);
            if prev >= MAX_INTERNER_ENTRIES {
                INTERNER_COUNT.fetch_sub(1, Ordering::Relaxed);
                return Arc::from("other");
            }
            let arc: Arc<str> = Arc::from(s);
            e.insert(arc.clone());
            arc
        }
    }
}

#[allow(dead_code)]
pub(crate) fn interner_size() -> usize {
    STRING_INTERNER.len()
}

// =============================================================================
// STATIC STRING CONSTANTS
// =============================================================================

/// Static string constants for boolean labels to avoid allocations.
pub const STREAMING_TRUE: &str = "true";
pub const STREAMING_FALSE: &str = "false";

pub const fn bool_to_static_str(b: bool) -> &'static str {
    if b {
        STREAMING_TRUE
    } else {
        STREAMING_FALSE
    }
}

/// Static lookup table for common HTTP status codes to avoid allocations.
/// Returns a static string for known codes, or None for unknown codes.
#[inline]
pub fn status_code_to_static_str(code: u16) -> Option<&'static str> {
    match code {
        200 => Some("200"),
        201 => Some("201"),
        204 => Some("204"),
        400 => Some("400"),
        401 => Some("401"),
        403 => Some("403"),
        404 => Some("404"),
        408 => Some("408"),
        422 => Some("422"),
        429 => Some("429"),
        500 => Some("500"),
        502 => Some("502"),
        503 => Some("503"),
        504 => Some("504"),
        _ => None,
    }
}

/// Static HTTP method strings to avoid allocations on every request.
pub(crate) mod http_methods {
    pub const GET: &str = "GET";
    pub const POST: &str = "POST";
    pub const PUT: &str = "PUT";
    pub const DELETE: &str = "DELETE";
    pub const PATCH: &str = "PATCH";
    pub const HEAD: &str = "HEAD";
    pub const OPTIONS: &str = "OPTIONS";
}

/// Convert HTTP method to static string. Returns the method as-is for unknown methods.
#[inline]
pub fn method_to_static_str(method: &str) -> &'static str {
    match method {
        "GET" => http_methods::GET,
        "POST" => http_methods::POST,
        "PUT" => http_methods::PUT,
        "DELETE" => http_methods::DELETE,
        "PATCH" => http_methods::PATCH,
        "HEAD" => http_methods::HEAD,
        "OPTIONS" => http_methods::OPTIONS,
        _ => "OTHER",
    }
}

/// Get status code as Cow - static for common codes, allocated for rare ones.
#[inline]
pub fn status_code_to_cow(code: u16) -> Cow<'static, str> {
    match status_code_to_static_str(code) {
        Some(s) => Cow::Borrowed(s),
        None => Cow::Owned(code.to_string()),
    }
}

// =============================================================================
// PROMETHEUS CONFIG
// =============================================================================

#[derive(Debug, Clone)]
pub struct PrometheusConfig {
    pub port: u16,
    pub host: String,
    pub duration_buckets: Option<Vec<f64>>,
    pub ttft_buckets: Option<Vec<f64>>,
}

impl Default for PrometheusConfig {
    fn default() -> Self {
        Self {
            port: 29000,
            host: "0.0.0.0".to_string(),
            duration_buckets: None,
            ttft_buckets: None,
        }
    }
}

pub(crate) fn init_metrics() {
    // Layer 1: HTTP entry point
    describe_counter!(
        "mg_http_requests_total",
        "Total HTTP requests by method and path"
    );
    describe_counter!(
        "mg_http_responses_total",
        "Total HTTP responses by status_code and error_code"
    );

    // Layer 2: Gateway end-to-end (includes retries)
    describe_counter!(
        "mg_gateway_requests_total",
        "Total gateway requests by model, endpoint, streaming"
    );
    describe_histogram!(
        "mg_gateway_duration_seconds",
        "End-to-end gateway request duration by model and endpoint (includes retries)"
    );
    describe_counter!(
        "mg_gateway_errors_total",
        "Gateway errors by model, endpoint, error_type"
    );

    // Layer 3: Upstream single-attempt calls
    describe_counter!(
        "mg_upstream_requests_total",
        "Per-attempt upstream requests by model, provider, status_code"
    );
    describe_histogram!(
        "mg_upstream_duration_seconds",
        "Per-attempt upstream call duration by model and provider"
    );
    describe_histogram!(
        "mg_upstream_ttft_seconds",
        "Time to first token for streaming requests by model and provider"
    );

    // Layer 4: Retry
    describe_counter!(
        "mg_retry_attempts_total",
        "Retry attempts by model, endpoint, status_code that triggered the retry"
    );
    describe_counter!(
        "mg_retry_exhausted_total",
        "Requests that exhausted all retry attempts by model and endpoint"
    );

    // Layer 5: Circuit breaker
    describe_gauge!(
        "mg_worker_cb_state",
        "Circuit breaker state per worker (0=closed, 1=open, 2=half_open)"
    );
    describe_counter!(
        "mg_worker_cb_transitions_total",
        "Circuit breaker state transitions by worker, from, to"
    );
    describe_gauge!(
        "mg_worker_cb_consecutive_failures",
        "Current consecutive failure count per worker"
    );
    describe_gauge!(
        "mg_worker_cb_consecutive_successes",
        "Current consecutive success count per worker"
    );

    // Layer 6: Worker pool
    describe_gauge!(
        "mg_worker_pool_total",
        "Total registered workers by model"
    );
    describe_gauge!(
        "mg_worker_available_total",
        "Available workers (circuit not open) by model"
    );

    // Layer 7: Token usage (TPOT only; per-key token counters are in the access log)
    describe_histogram!(
        "mg_upstream_tpot_ms",
        "Time per output token for streaming requests (ms/token) by model and provider"
    );
}

pub fn start_prometheus(config: PrometheusConfig) {
    init_metrics();

    let duration_matcher = Matcher::Suffix(String::from("duration_seconds"));
    let duration_bucket: Vec<f64> = config.duration_buckets.unwrap_or_else(|| {
        vec![
            0.1, 0.5, 1.0, 2.5, 5.0, 10.0, 15.0, 30.0, 60.0, 120.0, 180.0, 240.0,
        ]
    });

    let ttft_matcher = Matcher::Suffix(String::from("ttft_seconds"));
    let ttft_bucket: Vec<f64> = config
        .ttft_buckets
        .unwrap_or_else(|| vec![0.5, 1.0, 2.0, 3.0, 4.0, 5.0]);

    // TPOT buckets in milliseconds per token
    let tpot_matcher = Matcher::Suffix(String::from("tpot_ms"));
    let tpot_bucket: Vec<f64> = vec![5.0, 10.0, 20.0, 30.0, 50.0, 75.0, 100.0, 150.0, 200.0];

    let ip_addr: IpAddr = config
        .host
        .parse()
        .unwrap_or(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)));
    let socket_addr = SocketAddr::new(ip_addr, config.port);

    PrometheusBuilder::new()
        .with_http_listener(socket_addr)
        .upkeep_timeout(Duration::from_secs(5 * 60))
        .set_buckets_for_metric(duration_matcher, &duration_bucket)
        .expect("failed to set duration bucket")
        .set_buckets_for_metric(ttft_matcher, &ttft_bucket)
        .expect("failed to set ttft bucket")
        .set_buckets_for_metric(tpot_matcher, &tpot_bucket)
        .expect("failed to set tpot bucket")
        .install()
        .expect("failed to install Prometheus metrics exporter");
}

// =============================================================================
// LABEL CONSTANTS
// =============================================================================

pub mod metrics_labels {
    // Endpoints
    pub const ENDPOINT_CHAT: &str = "chat";
    pub const ENDPOINT_MESSAGES: &str = "messages";
    pub const ENDPOINT_EMBEDDINGS: &str = "embeddings";
    pub const ENDPOINT_RESPONSES: &str = "responses";
    pub const ENDPOINT_RESPONSES_RETRIEVE: &str = "responses_retrieve";
    pub const ENDPOINT_RESPONSES_DELETE: &str = "responses_delete";
    pub const ENDPOINT_RESPONSES_INPUT_ITEMS: &str = "responses_input_items";

    // Error types
    pub const ERROR_NO_UPSTREAM: &str = "no_upstream";
    pub const ERROR_TIMEOUT: &str = "timeout";
    pub const ERROR_BACKEND: &str = "backend_error";
    pub const ERROR_VALIDATION: &str = "validation_error";
    pub const ERROR_INTERNAL: &str = "internal_error";
}

// =============================================================================
// METRICS IMPL
// =============================================================================

pub struct Metrics;

impl Metrics {
    // =========================================================================
    // Layer 1: HTTP entry point
    // =========================================================================

    pub fn record_http_request(method: &'static str, path: &str) {
        let path_interned = intern_string(path);
        counter!(
            "mg_http_requests_total",
            "method" => method,
            "path" => path_interned,
        )
        .increment(1);
    }

    pub fn record_http_response(status_code: u16, error_code: &str) {
        let status_str: Cow<'static, str> = status_code_to_cow(status_code);
        let error_interned = intern_string(error_code);
        counter!(
            "mg_http_responses_total",
            "status_code" => status_str,
            "error_code" => error_interned
        )
        .increment(1);
    }

    // =========================================================================
    // Layer 2: Gateway end-to-end
    // =========================================================================

    pub fn record_gateway_request(model_id: &str, endpoint: &'static str, streaming: &'static str) {
        let model = intern_string(model_id);
        counter!(
            "mg_gateway_requests_total",
            "model" => model,
            "endpoint" => endpoint,
            "streaming" => streaming
        )
        .increment(1);
    }

    pub fn record_gateway_duration(model_id: &str, endpoint: &'static str, duration: Duration) {
        let model = intern_string(model_id);
        histogram!(
            "mg_gateway_duration_seconds",
            "model" => model,
            "endpoint" => endpoint
        )
        .record(duration.as_secs_f64());
    }

    pub fn record_gateway_error(
        model_id: &str,
        endpoint: &'static str,
        error_type: &'static str,
    ) {
        let model = intern_string(model_id);
        counter!(
            "mg_gateway_errors_total",
            "model" => model,
            "endpoint" => endpoint,
            "error_type" => error_type
        )
        .increment(1);
    }

    // =========================================================================
    // Layer 3: Upstream single-attempt calls
    // =========================================================================

    pub fn record_upstream_request(model_id: &str, provider: &str, status_code: u16) {
        let model = intern_string(model_id);
        let provider_interned = intern_string(provider);
        let status_str: Cow<'static, str> = status_code_to_cow(status_code);
        counter!(
            "mg_upstream_requests_total",
            "model" => model,
            "provider" => provider_interned,
            "status_code" => status_str
        )
        .increment(1);
    }

    pub fn record_upstream_duration(model_id: &str, provider: &str, duration: Duration) {
        let model = intern_string(model_id);
        let provider_interned = intern_string(provider);
        histogram!(
            "mg_upstream_duration_seconds",
            "model" => model,
            "provider" => provider_interned
        )
        .record(duration.as_secs_f64());
    }

    pub fn record_upstream_ttft(model_id: &str, provider: &str, duration: Duration) {
        let model = intern_string(model_id);
        let provider_interned = intern_string(provider);
        histogram!(
            "mg_upstream_ttft_seconds",
            "model" => model,
            "provider" => provider_interned
        )
        .record(duration.as_secs_f64());
    }

    // =========================================================================
    // Layer 4: Retry
    // =========================================================================

    pub fn record_retry_attempt(model_id: &str, endpoint: &'static str, status_code: u16) {
        let model = intern_string(model_id);
        let status_str: Cow<'static, str> = status_code_to_cow(status_code);
        counter!(
            "mg_retry_attempts_total",
            "model" => model,
            "endpoint" => endpoint,
            "status_code" => status_str
        )
        .increment(1);
    }

    pub fn record_retry_exhausted(model_id: &str, endpoint: &'static str) {
        let model = intern_string(model_id);
        counter!(
            "mg_retry_exhausted_total",
            "model" => model,
            "endpoint" => endpoint
        )
        .increment(1);
    }

    // =========================================================================
    // Layer 5: Circuit breaker
    // =========================================================================

    pub fn set_worker_cb_state(worker: &str, state_code: u8) {
        let worker_interned = intern_string(worker);
        gauge!(
            "mg_worker_cb_state",
            "worker" => worker_interned
        )
        .set(state_code as f64);
    }

    pub fn record_worker_cb_transition(worker: &str, from: &'static str, to: &'static str) {
        let worker_interned = intern_string(worker);
        counter!(
            "mg_worker_cb_transitions_total",
            "worker" => worker_interned,
            "from" => from,
            "to" => to
        )
        .increment(1);
    }

    pub fn set_worker_cb_consecutive_failures(worker: &str, count: u32) {
        let worker_interned = intern_string(worker);
        gauge!(
            "mg_worker_cb_consecutive_failures",
            "worker" => worker_interned
        )
        .set(count as f64);
    }

    pub fn set_worker_cb_consecutive_successes(worker: &str, count: u32) {
        let worker_interned = intern_string(worker);
        gauge!(
            "mg_worker_cb_consecutive_successes",
            "worker" => worker_interned
        )
        .set(count as f64);
    }

    // =========================================================================
    // Layer 6: Worker pool
    // =========================================================================

    pub fn set_worker_pool_total(model_id: &str, count: usize) {
        let model = intern_string(model_id);
        gauge!(
            "mg_worker_pool_total",
            "model" => model
        )
        .set(count as f64);
    }

    pub fn set_worker_available_total(model_id: &str, count: usize) {
        let model = intern_string(model_id);
        gauge!(
            "mg_worker_available_total",
            "model" => model
        )
        .set(count as f64);
    }

    // =========================================================================
    // Layer 7: TPOT
    // =========================================================================

    /// Record Time Per Output Token for streaming requests (milliseconds per token).
    pub fn record_tpot(model_id: &str, provider: &str, tpot_ms: f64) {
        let model = intern_string(model_id);
        let provider_interned = intern_string(provider);
        histogram!(
            "mg_upstream_tpot_ms",
            "model" => model,
            "provider" => provider_interned
        )
        .record(tpot_ms);
    }

    // =========================================================================
    // Worker cleanup
    // =========================================================================

    pub fn remove_worker_metrics(worker_url: &str) {
        let worker = intern_string(worker_url);
        gauge!("mg_worker_cb_consecutive_failures", "worker" => Arc::clone(&worker)).set(0.0);
        gauge!("mg_worker_cb_consecutive_successes", "worker" => Arc::clone(&worker)).set(0.0);
        // -1 signals "removed" until metrics-rs supports deletion
        gauge!("mg_worker_cb_state", "worker" => worker).set(-1.0);
    }
}

#[cfg(test)]
mod tests {
    use std::net::TcpListener;

    use super::*;

    #[test]
    fn test_prometheus_config_default() {
        let config = PrometheusConfig::default();
        assert_eq!(config.port, 29000);
        assert_eq!(config.host, "0.0.0.0");
    }

    #[test]
    fn test_bool_to_static_str() {
        assert_eq!(bool_to_static_str(true), "true");
        assert_eq!(bool_to_static_str(false), "false");
    }

    #[test]
    fn test_status_code_to_static_str() {
        assert_eq!(status_code_to_static_str(200), Some("200"));
        assert_eq!(status_code_to_static_str(404), Some("404"));
        assert_eq!(status_code_to_static_str(500), Some("500"));
        assert_eq!(status_code_to_static_str(418), None);
    }

    #[test]
    fn test_status_code_to_cow() {
        let cow_200 = status_code_to_cow(200);
        assert!(matches!(cow_200, Cow::Borrowed(_)));
        assert_eq!(cow_200, "200");

        let cow_418 = status_code_to_cow(418);
        assert!(matches!(cow_418, Cow::Owned(_)));
        assert_eq!(cow_418, "418");
    }

    #[test]
    fn test_method_to_static_str() {
        assert_eq!(method_to_static_str("GET"), "GET");
        assert_eq!(method_to_static_str("POST"), "POST");
        assert_eq!(method_to_static_str("UNKNOWN"), "OTHER");
    }

    #[test]
    fn test_intern_string_returns_same_arc() {
        let s1 = intern_string("test_model");
        let s2 = intern_string("test_model");
        assert!(Arc::ptr_eq(&s1, &s2));
    }

    #[test]
    fn test_intern_string_different_strings() {
        let s1 = intern_string("model_a");
        let s2 = intern_string("model_b");
        assert!(!Arc::ptr_eq(&s1, &s2));
    }

    #[test]
    fn test_interner_size_grows() {
        let initial_size = interner_size();
        let unique = format!("unique_test_string_{}", initial_size);
        intern_string(&unique);
        assert!(interner_size() > initial_size);
    }

    #[test]
    fn test_port_already_in_use() {
        let port = 29123;
        if let Ok(_listener) = TcpListener::bind(("127.0.0.1", port)) {
            let config = PrometheusConfig {
                port,
                host: "127.0.0.1".to_string(),
                duration_buckets: None,
                ttft_buckets: None,
            };
            assert_eq!(config.port, port);
        }
    }
}
