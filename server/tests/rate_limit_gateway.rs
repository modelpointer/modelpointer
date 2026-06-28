//! Full-stack rate-limit integration tests.
//!
//! These tests wire a real Redis-backed rate limiter into the gateway Router
//! and verify that RPM/TPM limits are enforced end-to-end through the HTTP
//! request pipeline.
//!
//! Requires a running Redis at `REDIS_URL` (default: redis://127.0.0.1:6379).
//! Tests are skipped automatically when Redis is unreachable.

use std::sync::{atomic::AtomicBool, Arc, OnceLock};

use axum::{
    body::Body,
    http::{Request, StatusCode},
    response::Response,
    routing::post,
    Router,
};
use tokio::net::TcpListener;
use tower::ServiceExt;
use tower_http::cors::CorsLayer;
use uuid::Uuid;

use modelpointer::{
    auth::CachedApiKeyRepository,
    quota_config::QuotaStore,
    rate_limit_redis::RedisRateLimiter,
    router::GatewayRouter,
    server::{build_app, GatewayState},
};
use modelpointer_core::{
    app_context::AppContext,
    config::RouterConfig,
    model::ModelCard,
    rate_limit::RateLimiter,
    upstream::node::{
        ApiCompatibility, RuntimeType, UpstreamBinding, UpstreamCredential, UpstreamGroup,
        UpstreamNode, UpstreamProfile, ProviderType,
    },
    upstream::routing::{RoutingStrategy, RoutingStrategyConfig},
};

// ── Init ──────────────────────────────────────────────────────────────────────

static PROXY_BYPASS: OnceLock<()> = OnceLock::new();

fn bypass_proxy() {
    PROXY_BYPASS.get_or_init(|| {
        // Safety: called once via OnceLock before any reqwest client is built.
        unsafe { std::env::set_var("NO_PROXY", "127.0.0.1,localhost") };
    });
}

fn redis_url() -> String {
    std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".to_string())
}

/// Return a limiter, or `None` if Redis is unreachable (test is skipped).
async fn try_redis_limiter(window_secs: u64) -> Option<Arc<dyn RateLimiter>> {
    RedisRateLimiter::new(&redis_url(), window_secs)
        .await
        .ok()
        .map(|rl| rl as Arc<dyn RateLimiter>)
}

// ── Mock upstream ─────────────────────────────────────────────────────────────

/// Spawn a mock upstream and return its base URL.
async fn spawn_mock_upstream(status: u16, content_type: &'static str, body: &'static str) -> String {
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

// ── Gateway builder ───────────────────────────────────────────────────────────

/// Build a gateway `Router` pointing to `upstream_base_url` for a model named
/// `model_name`, with the given RPM / TPM key limits and rate limiter.
async fn make_gateway_with_rl(
    upstream_base_url: &str,
    model_name: &str,
    key_rpm: Option<u32>,
    key_tpm: Option<u32>,
    rate_limiter: Arc<dyn RateLimiter>,
) -> Router {
    bypass_proxy();

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
        ModelCard::new(model_name),
        RoutingStrategy::Swrr,
        vec![binding],
    )
    .unwrap()
    .with_rate_limits(key_rpm, key_tpm, None, None);

    context.upstream_registry.reload_all(vec![group]);

    let state = Arc::new(GatewayState {
        router: Arc::new(GatewayRouter::new(&context).await.unwrap()),
        context,
        api_key_repo: CachedApiKeyRepository::new().into_shared(),
        auth_required: false,
        rate_limiter: Some(rate_limiter),
        quota_store: QuotaStore::new(),
    });

    build_app(state, 10 * 1024 * 1024, vec![], CorsLayer::new())
}

/// Build a two-tier gateway: primary (priority=0) and fallback (priority=1),
/// with `primary_capacity_rpm` set so the primary tier spills over to the
/// fallback once its RPM capacity is exhausted (no 429 — transparent to client).
async fn make_two_tier_gateway(
    primary_url: &str,
    fallback_url: &str,
    model_name: &str,
    primary_capacity_rpm: u32,
    rate_limiter: Arc<dyn RateLimiter>,
) -> Router {
    bypass_proxy();

    let context = Arc::new(
        AppContext::with_config(RouterConfig {
            request_timeout_secs: 5,
            ..RouterConfig::default()
        })
        .await
        .unwrap(),
    );

    let make_binding = |url: &str, priority: u8| {
        let node = UpstreamNode {
            profile: UpstreamProfile {
                base_url: url.to_string(),
                api_compatibility: ApiCompatibility::OpenAi,
                runtime_type: RuntimeType::External,
                upstream_model_name: None,
                credential: Arc::new(UpstreamCredential {
                    name: format!("mock-{}", priority),
                    api_key: None,
                    provider_type: ProviderType::Unknown,
                }),
            },
            healthy: Arc::new(AtomicBool::new(true)),
        };
        UpstreamBinding::new(node, true, RoutingStrategyConfig::Swrr { weight: 1 }, priority)
            .unwrap()
    };

    let group = UpstreamGroup::new(
        ModelCard::new(model_name),
        RoutingStrategy::Swrr,
        vec![make_binding(primary_url, 0), make_binding(fallback_url, 1)],
    )
    .unwrap()
    .with_primary_capacity(Some(primary_capacity_rpm), None);

    context.upstream_registry.reload_all(vec![group]);

    let state = Arc::new(GatewayState {
        router: Arc::new(GatewayRouter::new(&context).await.unwrap()),
        context,
        api_key_repo: CachedApiKeyRepository::new().into_shared(),
        auth_required: false,
        rate_limiter: Some(rate_limiter),
        quota_store: QuotaStore::new(),
    });

    build_app(state, 10 * 1024 * 1024, vec![], CorsLayer::new())
}

// ── Request helpers ───────────────────────────────────────────────────────────

/// Send a non-streaming chat POST for the given `model` name.
async fn chat_post(app: Router, model: &str) -> axum::response::Response {
    let body = format!(
        r#"{{"model":"{}","messages":[{{"role":"user","content":"hi"}}]}}"#,
        model
    );
    let req = Request::builder()
        .method("POST")
        .uri("/v1/chat/completions")
        .header("content-type", "application/json")
        .body(Body::from(body))
        .unwrap();
    app.oneshot(req).await.unwrap()
}

async fn body_text(resp: axum::response::Response) -> String {
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    String::from_utf8_lossy(&bytes).into_owned()
}

// ── Upstream fixtures ─────────────────────────────────────────────────────────

/// Non-streaming upstream response with token usage.
const NON_STREAM_BODY: &str = r#"{
  "id": "chatcmpl-test",
  "object": "chat.completion",
  "choices": [{"index": 0, "message": {"role": "assistant", "content": "Hi!"}, "finish_reason": "stop"}],
  "usage": {"prompt_tokens": 5, "completion_tokens": 7, "total_tokens": 12}
}"#;

// ── RPM tests ─────────────────────────────────────────────────────────────────

/// RPM limit of 2: the 3rd request in the same window must be denied.
#[tokio::test]
async fn redis_rpm_third_request_returns_429() {
    let Some(rl) = try_redis_limiter(60).await else { return };

    // Unique model name → unique Redis key → tests don't interfere with each other.
    let model = format!("rpm-test-{}", Uuid::new_v4());
    let upstream_url = spawn_mock_upstream(200, "application/json", NON_STREAM_BODY).await;
    let app = make_gateway_with_rl(&upstream_url, &model, Some(2), None, rl).await;

    let r1 = chat_post(app.clone(), &model).await;
    assert_eq!(r1.status(), StatusCode::OK, "1st request should succeed");

    let r2 = chat_post(app.clone(), &model).await;
    assert_eq!(r2.status(), StatusCode::OK, "2nd request should succeed");

    let r3 = chat_post(app.clone(), &model).await;
    assert_eq!(r3.status(), StatusCode::TOO_MANY_REQUESTS, "3rd request should be rate-limited");
}

/// The 429 response must include a `Retry-After` header.
#[tokio::test]
async fn redis_rpm_429_includes_retry_after_header() {
    let Some(rl) = try_redis_limiter(60).await else { return };

    let model = format!("rpm-retry-{}", Uuid::new_v4());
    let upstream_url = spawn_mock_upstream(200, "application/json", NON_STREAM_BODY).await;
    let app = make_gateway_with_rl(&upstream_url, &model, Some(1), None, rl).await;

    // First request fills the limit (limit=1 → 1 request allowed)
    let _ = chat_post(app.clone(), &model).await;

    let denied = chat_post(app.clone(), &model).await;
    assert_eq!(denied.status(), StatusCode::TOO_MANY_REQUESTS);
    assert!(
        denied.headers().contains_key("retry-after"),
        "Retry-After header missing from 429 response"
    );
}

/// The 429 error body must describe the violated limit.
#[tokio::test]
async fn redis_rpm_429_body_describes_limit() {
    let Some(rl) = try_redis_limiter(60).await else { return };

    let model = format!("rpm-body-{}", Uuid::new_v4());
    let upstream_url = spawn_mock_upstream(200, "application/json", NON_STREAM_BODY).await;
    let app = make_gateway_with_rl(&upstream_url, &model, Some(1), None, rl).await;

    let _ = chat_post(app.clone(), &model).await;
    let denied = chat_post(app.clone(), &model).await;

    let text = body_text(denied).await;
    assert!(text.contains("rate_limit"), "body should mention rate_limit: {text}");
    assert!(text.contains("key_rpm"), "body should identify the dimension (key_rpm): {text}");
}

/// Two different models have independent rate-limit buckets.
#[tokio::test]
async fn redis_rpm_different_models_are_independent() {
    let Some(rl) = try_redis_limiter(60).await else { return };

    let model_a = format!("rpm-indep-a-{}", Uuid::new_v4());
    let model_b = format!("rpm-indep-b-{}", Uuid::new_v4());
    let upstream_url = spawn_mock_upstream(200, "application/json", NON_STREAM_BODY).await;

    bypass_proxy();
    let context = Arc::new(
        AppContext::with_config(RouterConfig {
            request_timeout_secs: 5,
            ..RouterConfig::default()
        })
        .await
        .unwrap(),
    );

    // Register both models in a single reload_all call.
    let groups: Vec<UpstreamGroup> = [&model_a, &model_b]
        .iter()
        .map(|model_name| {
            let node = UpstreamNode {
                profile: UpstreamProfile {
                    base_url: upstream_url.clone(),
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
                UpstreamBinding::new(node, true, RoutingStrategyConfig::Swrr { weight: 1 }, 0)
                    .unwrap();
            UpstreamGroup::new(
                ModelCard::new(model_name.as_str()),
                RoutingStrategy::Swrr,
                vec![binding],
            )
            .unwrap()
            .with_rate_limits(Some(1), None, None, None)
        })
        .collect();
    context.upstream_registry.reload_all(groups);

    let state = Arc::new(GatewayState {
        router: Arc::new(GatewayRouter::new(&context).await.unwrap()),
        context,
        api_key_repo: CachedApiKeyRepository::new().into_shared(),
        auth_required: false,
        rate_limiter: Some(rl),
        quota_store: QuotaStore::new(),
    });
    let app = build_app(state, 10 * 1024 * 1024, vec![], CorsLayer::new());

    // Exhaust model_a's limit (limit=1, so 2nd request is denied)
    let _ = chat_post(app.clone(), &model_a).await;
    let denied_a = chat_post(app.clone(), &model_a).await;
    assert_eq!(denied_a.status(), StatusCode::TOO_MANY_REQUESTS);

    // model_b bucket is completely independent
    let allowed_b = chat_post(app.clone(), &model_b).await;
    assert_eq!(allowed_b.status(), StatusCode::OK);
}

// ── TPM tests ─────────────────────────────────────────────────────────────────

/// Each non-streaming response records `total_tokens = 12`.
/// With `key_tpm = 20`, two requests accumulate 24 tokens;
/// the third pre-flight check sees 24 ≥ 20 → 429.
#[tokio::test]
async fn redis_tpm_third_request_returns_429_after_two_token_records() {
    let Some(rl) = try_redis_limiter(60).await else { return };

    let model = format!("tpm-test-{}", Uuid::new_v4());
    let upstream_url = spawn_mock_upstream(200, "application/json", NON_STREAM_BODY).await;
    // NON_STREAM_BODY has total_tokens: 12; limit 20 → two requests (24 total) exhaust it.
    let app = make_gateway_with_rl(&upstream_url, &model, None, Some(20), rl).await;

    let r1 = chat_post(app.clone(), &model).await;
    assert_eq!(r1.status(), StatusCode::OK, "1st request should succeed");

    let r2 = chat_post(app.clone(), &model).await;
    assert_eq!(r2.status(), StatusCode::OK, "2nd request should succeed (10 < 20 pre-flight)");

    let r3 = chat_post(app.clone(), &model).await;
    assert_eq!(r3.status(), StatusCode::TOO_MANY_REQUESTS, "3rd request should be TPM-limited");
}

/// TPM 429 must identify the violated dimension.
#[tokio::test]
async fn redis_tpm_429_body_identifies_key_tpm() {
    let Some(rl) = try_redis_limiter(60).await else { return };

    let model = format!("tpm-body-{}", Uuid::new_v4());
    let upstream_url = spawn_mock_upstream(200, "application/json", NON_STREAM_BODY).await;
    let app = make_gateway_with_rl(&upstream_url, &model, None, Some(20), rl).await;

    let _ = chat_post(app.clone(), &model).await; // records 12
    let _ = chat_post(app.clone(), &model).await; // records 12 → total 24 ≥ 20

    let denied = chat_post(app.clone(), &model).await;
    let text = body_text(denied).await;
    assert!(text.contains("key_tpm"), "body should identify dimension (key_tpm): {text}");
}

// ── Primary-tier capacity spillover ──────────────────────────────────────────
//
// `primary_capacity_rpm` is a silent traffic-shaping limit: when the primary
// tier's RPM bucket is full, the gateway routes subsequent requests to the
// fallback tier and returns 200 — the client never sees a 429.
// This is the key behavioral difference from `key_rpm` / `model_rpm`.

const PRIMARY_BODY: &str = r#"{"id":"p","object":"chat.completion","choices":[{"index":0,"message":{"role":"assistant","content":"from-primary"},"finish_reason":"stop"}],"usage":{"prompt_tokens":1,"completion_tokens":1,"total_tokens":2}}"#;
const FALLBACK_BODY: &str = r#"{"id":"f","object":"chat.completion","choices":[{"index":0,"message":{"role":"assistant","content":"from-fallback"},"finish_reason":"stop"}],"usage":{"prompt_tokens":1,"completion_tokens":1,"total_tokens":2}}"#;

/// With `primary_capacity_rpm = 1`:
/// - Request 1 → primary tier (capacity not yet exhausted)
/// - Request 2 → fallback tier (primary at capacity; no 429, client gets 200)
#[tokio::test]
async fn redis_primary_capacity_rpm_spillover_routes_to_fallback() {
    let Some(rl) = try_redis_limiter(60).await else { return };

    let model = format!("spillover-rpm-{}", Uuid::new_v4());
    let primary_url = spawn_mock_upstream(200, "application/json", PRIMARY_BODY).await;
    let fallback_url = spawn_mock_upstream(200, "application/json", FALLBACK_BODY).await;

    let app = make_two_tier_gateway(&primary_url, &fallback_url, &model, 1, rl).await;

    // First request: primary capacity 0 → 1 (≤ 1) → primary tier selected.
    let r1 = chat_post(app.clone(), &model).await;
    let status1 = r1.status();
    let body1 = body_text(r1).await;
    assert_eq!(status1, StatusCode::OK, "r1 unexpected status; body: {body1}");
    assert!(body1.contains("from-primary"), "r1 should come from primary: {body1}");

    // Second request: primary capacity 1 → 2 (> 1) → spillover to fallback.
    // Critically: still 200, not 429.
    let r2 = chat_post(app.clone(), &model).await;
    let status2 = r2.status();
    let body2 = body_text(r2).await;
    assert_eq!(status2, StatusCode::OK, "r2 must be 200 (spillover, not client error); body: {body2}");
    assert!(body2.contains("from-fallback"), "r2 should come from fallback: {body2}");
}

/// `primary_capacity_rpm` and `key_rpm` are orthogonal:
/// - `primary_capacity_rpm` spills over silently (200)
/// - `key_rpm` enforces a hard client-facing limit (429)
///
/// With primary_capacity_rpm=1 and key_rpm=3:
/// - Requests 1–3 are all 200 (key limit not exceeded)
/// - Requests 2–3 go to the fallback (primary capacity exhausted after request 1)
#[tokio::test]
async fn redis_spillover_and_key_rpm_are_orthogonal() {
    let Some(rl) = try_redis_limiter(60).await else { return };

    let model = format!("spillover-orth-{}", Uuid::new_v4());
    let primary_url = spawn_mock_upstream(200, "application/json", PRIMARY_BODY).await;
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

    let make_binding = |url: &str, priority: u8| {
        let node = UpstreamNode {
            profile: UpstreamProfile {
                base_url: url.to_string(),
                api_compatibility: ApiCompatibility::OpenAi,
                runtime_type: RuntimeType::External,
                upstream_model_name: None,
                credential: Arc::new(UpstreamCredential {
                    name: format!("mock-{}", priority),
                    api_key: None,
                    provider_type: ProviderType::Unknown,
                }),
            },
            healthy: Arc::new(AtomicBool::new(true)),
        };
        UpstreamBinding::new(node, true, RoutingStrategyConfig::Swrr { weight: 1 }, priority).unwrap()
    };

    let group = UpstreamGroup::new(
        ModelCard::new(&model),
        RoutingStrategy::Swrr,
        vec![make_binding(&primary_url, 0), make_binding(&fallback_url, 1)],
    )
    .unwrap()
    .with_rate_limits(Some(3), None, None, None)
    .with_primary_capacity(Some(1), None);

    context.upstream_registry.reload_all(vec![group]);
    let state = Arc::new(GatewayState {
        router: Arc::new(GatewayRouter::new(&context).await.unwrap()),
        context,
        api_key_repo: CachedApiKeyRepository::new().into_shared(),
        auth_required: false,
        rate_limiter: Some(rl),
        quota_store: QuotaStore::new(),
    });
    let app = build_app(state, 10 * 1024 * 1024, vec![], CorsLayer::new());

    // r1 → primary (capacity 0→1, key_rpm 0→1)
    let r1 = chat_post(app.clone(), &model).await;
    assert_eq!(r1.status(), StatusCode::OK);
    assert!(body_text(r1).await.contains("from-primary"), "r1 should be primary");

    // r2 → fallback (capacity exhausted; key_rpm 1→2, still within 3)
    let r2 = chat_post(app.clone(), &model).await;
    assert_eq!(r2.status(), StatusCode::OK, "r2 must be 200 not 429 — spillover not rejection");
    assert!(body_text(r2).await.contains("from-fallback"), "r2 should be fallback");

    // r3 → fallback (capacity still exhausted; key_rpm 2→3, within limit)
    let r3 = chat_post(app.clone(), &model).await;
    assert_eq!(r3.status(), StatusCode::OK, "r3 must be 200");
    assert!(body_text(r3).await.contains("from-fallback"), "r3 should be fallback");

    // r4 → 429 (key_rpm 3→4, exceeds limit=3)
    let r4 = chat_post(app.clone(), &model).await;
    assert_eq!(r4.status(), StatusCode::TOO_MANY_REQUESTS, "r4 should be rejected by key_rpm");
}
