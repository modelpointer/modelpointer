use axum::http::HeaderValue;
use axum::{
    Router,
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use rustls::crypto::ring;
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::{signal, spawn};
use tower_http::cors::{AllowOrigin, Any, CorsLayer};

use crate::auth::{CachedApiKeyRepository, SqlApiKeyRepository};
use crate::auth_config::AuthConfigSource;
use crate::db::{
    ConfigResource, Database, load_all_quota_overrides, load_all_upstream_groups,
    load_config_version,
};
use crate::file_config::FileConfigSource;
use crate::quota_config::{QuotaConfigSource, QuotaStore};
use crate::rate_limit_memory::MemoryRateLimiter;
use crate::rate_limit_redis::RedisRateLimiter;
use crate::{
    log_sink::{self, AccessLogRecord, LogSink, LogWriter},
    middleware::{self, ApiKeyIdentity, RequestId},
    router::{GatewayRouter, RequestContext, RouterTrait},
};
use chrono::Utc;
use modelpointer_core::observability::metrics::metrics_labels::{
    ENDPOINT_CHAT, ENDPOINT_EMBEDDINGS, ENDPOINT_MESSAGES, ENDPOINT_RESPONSES,
};
use modelpointer_core::openai_protocol::{
    chat::ChatCompletionRequest, embedding::EmbeddingRequest, messages::CreateMessageRequest,
    validated::ValidatedJson,
};
use modelpointer_core::{
    app_context::AppContext,
    config::{AuthMode, ServerConfig},
    observability::{
        logging::{self, LoggingConfig},
        metrics, otel_trace,
    },
    rate_limit::{RateLimitCtx, RateLimitDecision, RateLimitKey, RateLimiter},
    storage::ApiKeyRepository,
};
use serde_json::json;
use tracing::{Level, info, warn};

#[derive(Clone)]
pub struct GatewayState {
    pub router: Arc<dyn RouterTrait>,
    pub context: Arc<AppContext>,
    pub api_key_repo: Arc<dyn ApiKeyRepository>,
    /// When false the auth middleware inserts an anonymous identity and skips token validation.
    pub auth_required: bool,
    /// Rate limiter (memory or Redis). None when rate limiting is not configured.
    pub rate_limiter: Option<Arc<dyn RateLimiter>>,
    /// Per-(api_key, model) quota overrides. Always present; empty when no quota file is configured.
    pub quota_store: Arc<QuotaStore>,
    /// Cheap-to-clone handle for writing access log records (rate-limit rejections, etc.).
    pub log_sink: LogSink,
}

fn emit_rl_log(sink: &LogSink, req_id: &str, api_key_id: &str, model: &str, endpoint: &str) {
    sink.try_send(AccessLogRecord {
        ts: Utc::now(),
        request_id: req_id.to_owned(),
        api_key_id: api_key_id.to_owned(),
        model: model.to_owned(),
        endpoint: endpoint.to_owned(),
        provider_url: String::new(),
        provider_node_id: String::new(),
        upstream_model: String::new(),
        status_code: 429,
        latency_ms: 0,
        streaming: false,
        ttft_ms: 0,
        tpot_ms: 0.0,
        prompt_tokens: 0,
        completion_tokens: 0,
        total_tokens: 0,
        finish_reason: String::new(),
    });
}

/// Check all rate limits for the given (api_key, model) pair.
///
/// Returns `(RateLimitCtx, min_priority)`:
/// - `RateLimitCtx` is `Some` when TPM recording is needed after the response.
/// - `min_priority` is `0` (use primary tier) or `1` (spill to fallback) based on
///   primary-tier capacity checks.
///
/// Returns an error `Response` (429) if any client-facing limit is exceeded.
/// Returns `Ok((None, 0))` when rate limiting is not configured or no limits are set.
async fn check_rate_limits(
    state: &GatewayState,
    api_key_id: &str,
    model_id: &str,
    protocol: &str,
) -> Result<(Option<RateLimitCtx>, u8), Response> {
    let Some(rl) = &state.rate_limiter else {
        return Ok((None, 0));
    };
    let (default_key_rpm, default_key_tpm, model_rpm, model_tpm) =
        state.context.upstream_registry.get_rate_limits(model_id);
    let (primary_cap_rpm, primary_cap_tpm) = state
        .context
        .upstream_registry
        .get_primary_capacity(model_id, protocol);

    // Per-(api_key, model) quota override: takes precedence over model defaults.
    let quota = state.quota_store.get(api_key_id, model_id);
    let key_rpm = quota.as_ref().and_then(|q| q.key_rpm).or(default_key_rpm);
    let key_tpm = quota.as_ref().and_then(|q| q.key_tpm).or(default_key_tpm);

    if key_rpm.is_none()
        && key_tpm.is_none()
        && model_rpm.is_none()
        && model_tpm.is_none()
        && primary_cap_rpm.is_none()
        && primary_cap_tpm.is_none()
    {
        return Ok((None, 0));
    }

    let model_key = RateLimitKey::Model {
        model_id: model_id.to_string(),
    };
    let key_model_key = RateLimitKey::KeyModel {
        api_key_id: api_key_id.to_string(),
        model_id: model_id.to_string(),
    };

    // ── Client-facing hard limits (return 429 on violation) ───────────────────
    //
    // Checks are ordered by side-effect severity to minimise wasted counter
    // increments when a later dimension rejects the request.
    //
    // 1. TPM checks first — check_tpm is purely read-only (token usage is
    //    recorded after a successful response via record_tokens, not here).
    //    A rejection at this stage wastes nothing.
    //
    // 2. key_rpm before model_rpm — check_rpm increments the sliding-window
    //    counter as part of the check. By checking the per-(key,model) bucket
    //    first, a rejection only affects the requester's own quota and does not
    //    consume any slot in the shared model-level bucket.  The reverse order
    //    would let an abusive key drain shared capacity through rejected requests.
    //
    // Known limitation: true atomicity across all dimensions would require a
    // Redis Lua script or a two-phase check/commit protocol.  Neither is
    // implemented here.  The ordering above is the best single-pass approximation:
    // it eliminates side effects entirely for TPM rejections and confines RPM
    // over-counting to the requester's own bucket for key_rpm rejections.

    // TPM (read-only — no counter side effects)
    if let Some(limit) = key_tpm {
        if let RateLimitDecision::Denied { retry_after_secs } =
            rl.check_tpm(&key_model_key, limit).await
        {
            return Err(rate_limit_response("key_tpm", limit, retry_after_secs));
        }
    }
    if let Some(limit) = model_tpm {
        if let RateLimitDecision::Denied { retry_after_secs } =
            rl.check_tpm(&model_key, limit).await
        {
            return Err(rate_limit_response("model_tpm", limit, retry_after_secs));
        }
    }

    // RPM (each call increments the counter — ordered from narrowest to broadest scope)
    if let Some(limit) = key_rpm {
        if let RateLimitDecision::Denied { retry_after_secs } =
            rl.check_rpm(&key_model_key, limit).await
        {
            return Err(rate_limit_response("key_rpm", limit, retry_after_secs));
        }
    }
    if let Some(limit) = model_rpm {
        if let RateLimitDecision::Denied { retry_after_secs } =
            rl.check_rpm(&model_key, limit).await
        {
            return Err(rate_limit_response("model_rpm", limit, retry_after_secs));
        }
    }

    // ── Primary-tier capacity (spill to fallback, no 429) ─────────────────────
    let primary_key = RateLimitKey::PrimaryTier {
        model_id: model_id.to_string(),
        protocol: protocol.to_string(),
    };
    let mut min_priority: u8 = 0;

    // Known limitation: check_rpm for the primary tier increments the sliding-
    // window counter as a side-effect of the check (that is how the limiter
    // drives the spillover decision). If check_rpm allows the request here but
    // routing subsequently falls to the fallback tier due to a circuit open or
    // health failure, the primary RPM counter will have been incremented even
    // though the request never reached a primary upstream. The effect is that
    // primary capacity drains slightly faster during primary-unhealthy periods,
    // causing capacity-based spillover to trigger sooner than strictly necessary.
    // Fixing this properly would require separating the "check" and "record"
    // phases of RPM counting, which is a deeper change deferred for now.
    if let Some(limit) = primary_cap_rpm {
        if let RateLimitDecision::Denied { .. } = rl.check_rpm(&primary_key, limit).await {
            min_priority = 1;
        }
    }
    if min_priority == 0 {
        if let Some(limit) = primary_cap_tpm {
            if let RateLimitDecision::Denied { .. } = rl.check_tpm(&primary_key, limit).await {
                min_priority = 1;
            }
        }
    }

    let record_key_tpm = key_tpm.is_some();
    let record_model_tpm = model_tpm.is_some();
    // Only record primary TPM when the request actually goes to the primary tier.
    let record_primary_tpm = primary_cap_tpm.is_some() && min_priority == 0;

    if record_key_tpm || record_model_tpm || record_primary_tpm {
        Ok((
            Some(RateLimitCtx {
                limiter: Arc::clone(rl),
                model_id: model_id.to_string(),
                protocol: protocol.to_string(),
                record_key_tpm,
                record_model_tpm,
                record_primary_tpm,
            }),
            min_priority,
        ))
    } else {
        Ok((None, min_priority))
    }
}

fn rate_limit_response(dimension: &str, limit: u32, retry_after_secs: u64) -> Response {
    use axum::http::header::RETRY_AFTER;
    let mut headers = axum::http::HeaderMap::new();
    if let Ok(v) = axum::http::HeaderValue::from_str(&retry_after_secs.to_string()) {
        headers.insert(RETRY_AFTER, v);
    }
    (
        StatusCode::TOO_MANY_REQUESTS,
        headers,
        axum::Json(serde_json::json!({
            "error": {
                "type": "rate_limit_exceeded",
                "code": format!("{}_limit_exceeded", dimension),
                "message": format!(
                    "{} limit of {} exceeded. Please retry after {} seconds.",
                    dimension.to_uppercase(), limit, retry_after_secs
                ),
            }
        })),
    )
        .into_response()
}

async fn health(_state: State<Arc<GatewayState>>) -> Response {
    (StatusCode::OK, "OK").into_response()
}

async fn v1_models(State(state): State<Arc<GatewayState>>) -> impl IntoResponse {
    let models: Vec<serde_json::Value> = state
        .context
        .upstream_registry
        .all_model_ids()
        .into_iter()
        .map(|id| json!({ "id": id, "object": "model", "created": 0, "owned_by": "system" }))
        .collect();

    axum::Json(json!({ "object": "list", "data": models }))
}

async fn v1_chat_completions(
    State(state): State<Arc<GatewayState>>,
    headers: HeaderMap,
    axum::extract::Extension(request_id): axum::extract::Extension<RequestId>,
    axum::extract::Extension(identity): axum::extract::Extension<ApiKeyIdentity>,
    ValidatedJson(body): ValidatedJson<ChatCompletionRequest>,
) -> Response {
    info!("Received chat completion request for model: {}", body.model);
    let (rate_limit, min_priority) =
        match check_rate_limits(&state, &identity.key_id, &body.model, "openai").await {
            Ok(result) => result,
            Err(resp) => {
                emit_rl_log(
                    &state.log_sink,
                    &request_id.0,
                    &identity.key_id,
                    &body.model,
                    ENDPOINT_CHAT,
                );
                return resp;
            }
        };
    let ctx = RequestContext {
        request_id: request_id.0,
        api_key_id: identity.key_id,
        rate_limit,
        min_priority,
    };
    state
        .router
        .route_chat(&body, Some(&body.model), &headers, &ctx)
        .await
}

async fn v1_messages(
    State(state): State<Arc<GatewayState>>,
    headers: HeaderMap,
    axum::extract::Extension(request_id): axum::extract::Extension<RequestId>,
    axum::extract::Extension(identity): axum::extract::Extension<ApiKeyIdentity>,
    ValidatedJson(body): ValidatedJson<CreateMessageRequest>,
) -> Response {
    let (rate_limit, min_priority) =
        match check_rate_limits(&state, &identity.key_id, &body.model, "anthropic").await {
            Ok(result) => result,
            Err(resp) => {
                emit_rl_log(
                    &state.log_sink,
                    &request_id.0,
                    &identity.key_id,
                    &body.model,
                    ENDPOINT_MESSAGES,
                );
                return resp;
            }
        };
    let ctx = RequestContext {
        request_id: request_id.0,
        api_key_id: identity.key_id,
        rate_limit,
        min_priority,
    };
    state
        .router
        .route_messages(&body, Some(&body.model), &headers, &ctx)
        .await
}

async fn v1_embeddings(
    State(state): State<Arc<GatewayState>>,
    headers: HeaderMap,
    axum::extract::Extension(request_id): axum::extract::Extension<RequestId>,
    axum::extract::Extension(identity): axum::extract::Extension<ApiKeyIdentity>,
    axum::Json(body): axum::Json<EmbeddingRequest>,
) -> Response {
    info!("Received embedding request for model: {}", body.model);
    let (rate_limit, min_priority) =
        match check_rate_limits(&state, &identity.key_id, &body.model, "openai").await {
            Ok(result) => result,
            Err(resp) => {
                emit_rl_log(
                    &state.log_sink,
                    &request_id.0,
                    &identity.key_id,
                    &body.model,
                    ENDPOINT_EMBEDDINGS,
                );
                return resp;
            }
        };
    let ctx = RequestContext {
        request_id: request_id.0,
        api_key_id: identity.key_id,
        rate_limit,
        min_priority,
    };
    state
        .router
        .route_embedding(&body, Some(&body.model), &headers, &ctx)
        .await
}

async fn v1_responses(
    State(state): State<Arc<GatewayState>>,
    headers: HeaderMap,
    axum::extract::Extension(request_id): axum::extract::Extension<RequestId>,
    axum::extract::Extension(identity): axum::extract::Extension<ApiKeyIdentity>,
    axum::Json(body): axum::Json<serde_json::Value>,
) -> Response {
    let model_id = body.get("model").and_then(|v| v.as_str()).unwrap_or("");
    let (rate_limit, min_priority) =
        match check_rate_limits(&state, &identity.key_id, model_id, "openai").await {
            Ok(result) => result,
            Err(resp) => {
                emit_rl_log(
                    &state.log_sink,
                    &request_id.0,
                    &identity.key_id,
                    model_id,
                    ENDPOINT_RESPONSES,
                );
                return resp;
            }
        };
    let ctx = RequestContext {
        request_id: request_id.0,
        api_key_id: identity.key_id,
        rate_limit,
        min_priority,
    };
    state.router.route_responses(body, &headers, &ctx).await
}

async fn v1_responses_retrieve(
    State(state): State<Arc<GatewayState>>,
    headers: HeaderMap,
    axum::extract::Extension(request_id): axum::extract::Extension<RequestId>,
    axum::extract::Extension(identity): axum::extract::Extension<ApiKeyIdentity>,
    Path(response_id): Path<String>,
) -> Response {
    let ctx = RequestContext {
        request_id: request_id.0,
        api_key_id: identity.key_id,
        rate_limit: None,
        min_priority: 0,
    };
    state
        .router
        .route_response_retrieve(&response_id, &headers, &ctx)
        .await
}

async fn v1_responses_delete(
    State(state): State<Arc<GatewayState>>,
    headers: HeaderMap,
    axum::extract::Extension(request_id): axum::extract::Extension<RequestId>,
    axum::extract::Extension(identity): axum::extract::Extension<ApiKeyIdentity>,
    Path(response_id): Path<String>,
) -> Response {
    let ctx = RequestContext {
        request_id: request_id.0,
        api_key_id: identity.key_id,
        rate_limit: None,
        min_priority: 0,
    };
    state
        .router
        .route_response_delete(&response_id, &headers, &ctx)
        .await
}

async fn v1_responses_input_items(
    State(state): State<Arc<GatewayState>>,
    headers: HeaderMap,
    axum::extract::Extension(request_id): axum::extract::Extension<RequestId>,
    axum::extract::Extension(identity): axum::extract::Extension<ApiKeyIdentity>,
    Path(response_id): Path<String>,
    Query(params): Query<std::collections::HashMap<String, String>>,
) -> Response {
    // Forward all query params verbatim to the upstream (limit, after, order, include, …).
    let upstream_qs: String = params
        .iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join("&");
    let ctx = RequestContext {
        request_id: request_id.0,
        api_key_id: identity.key_id,
        rate_limit: None,
        min_priority: 0,
    };
    state
        .router
        .route_response_input_items(&response_id, &upstream_qs, &headers, &ctx)
        .await
}

fn cors_layer(origins: &[String]) -> Result<CorsLayer, String> {
    if origins.is_empty() {
        return Ok(CorsLayer::new());
    }
    let list: Vec<HeaderValue> = origins
        .iter()
        .map(|s| s.parse().map_err(|_| format!("invalid CORS origin: {s:?}")))
        .collect::<Result<_, _>>()?;
    Ok(CorsLayer::new()
        .allow_headers(Any)
        .allow_methods(Any)
        .allow_origin(AllowOrigin::list(list))
        .expose_headers(Any))
}

pub fn build_app(
    app_state: Arc<GatewayState>,
    max_payload_size: usize,
    request_id_headers: Vec<String>,
    cors: CorsLayer,
) -> Router {
    let protected_gateway_routes = Router::new()
        .route("/v1/chat/completions", post(v1_chat_completions))
        .route("/v1/messages", post(v1_messages))
        .route("/v1/embeddings", post(v1_embeddings))
        .route("/v1/responses", post(v1_responses))
        .route(
            "/v1/responses/{response_id}",
            get(v1_responses_retrieve).delete(v1_responses_delete),
        )
        .route(
            "/v1/responses/{response_id}/input_items",
            get(v1_responses_input_items),
        )
        .route_layer(axum::middleware::from_fn_with_state(
            app_state.clone(),
            middleware::auth_middleware,
        ));

    let public_routes = Router::new()
        .route("/health", get(health))
        .route("/v1/models", get(v1_models));

    Router::new()
        .merge(protected_gateway_routes)
        .merge(public_routes)
        .layer(cors)
        .layer(axum::extract::DefaultBodyLimit::max(max_payload_size))
        .layer(tower_http::limit::RequestBodyLimitLayer::new(
            max_payload_size,
        ))
        .layer(middleware::create_logging_layer())
        .layer(middleware::RequestIdLayer::new(request_id_headers))
        .with_state(app_state)
}

pub async fn startup(config: ServerConfig) -> Result<(), Box<dyn std::error::Error>> {
    if let Some(trace_config) = &config.trace_config {
        otel_trace::otel_tracing_init(
            trace_config.enable_trace,
            trace_config.otlp_traces_endpoint.as_str(),
            trace_config.service_env.as_str(),
            trace_config.service_version.as_str(),
        )?;
    }

    static LOGGING_INITIALIZED: AtomicBool = AtomicBool::new(false);

    let _log_guard = if !LOGGING_INITIALIZED.swap(true, Ordering::SeqCst) {
        Some(logging::init_logging(
            LoggingConfig {
                level: config
                    .log_level
                    .as_deref()
                    .and_then(|s| match s.to_uppercase().parse::<Level>() {
                        Ok(l) => Some(l),
                        Err(_) => {
                            warn!("Invalid log level string: '{s}'. Defaulting to INFO.");
                            None
                        }
                    })
                    .unwrap_or(Level::INFO),
                json_format: config.json_log,
                log_dir: config.log_dir.clone(),
                colorize: true,
                log_file_name: "modelpointer".to_string(),
                log_targets: None,
            },
            config.trace_config.clone(),
        ))
    } else {
        None
    };

    if let Some(prometheus_config) = &config.prometheus_config {
        metrics::start_prometheus(prometheus_config.clone());
    }

    let app_context = Arc::new(AppContext::with_config(config.router_config.clone()).await?);

    // ── Routes: file or database ──────────────────────────────────────────────
    let database: Option<Database> = if config.route_file.is_some() {
        // File config mode: load routes from file, no DB needed for routes.
        let config_path = config.route_file.as_deref().unwrap();
        info!("Loading route config from file: {}", config_path);
        let output = FileConfigSource::new(config_path)
            .load()
            .map_err(|e| format!("Failed to load config file: {e}"))?;
        app_context.upstream_registry.reload_all(output.groups);
        info!("UpstreamRegistry loaded from config file");

        // Spawn file change poller for routes
        {
            let registry = app_context.upstream_registry.clone();
            let path = config_path.to_string();
            let interval = config.upstream_sync_interval_secs;
            tokio::spawn(async move {
                let mut last_mod = file_modified_time(&path);
                let mut tick = tokio::time::interval(Duration::from_secs(interval));
                tick.tick().await;
                loop {
                    tick.tick().await;
                    let current_mod = file_modified_time(&path);
                    if current_mod != last_mod {
                        last_mod = current_mod;
                        match FileConfigSource::new(&path).load() {
                            Ok(out) => {
                                registry.reload_all(out.groups);
                                info!("Route config reloaded from file");
                            }
                            Err(e) => tracing::warn!("Route config reload failed: {}", e),
                        }
                    }
                }
            });
        }

        None
    } else {
        // Database mode: connect and keep registry in sync with polling.
        let db = Database::connect(&config.database)
            .await
            .map_err(|e| format!("Failed to connect to database: {e}"))?;

        match load_all_upstream_groups(db.pool(), db.dialect()).await {
            Ok(groups) if groups.is_empty() => {
                tracing::warn!(
                    "No upstream groups found in database; all model requests will return 503 until routes are configured via the Admin API"
                );
            }
            Ok(groups) => {
                let count = groups.len();
                app_context.upstream_registry.reload_all(groups);
                info!("UpstreamRegistry loaded from database ({} groups)", count);
            }
            Err(e) => {
                return Err(format!("Failed to load upstream groups from database: {e}").into());
            }
        }

        {
            let registry = app_context.upstream_registry.clone();
            let poll_db = db.clone();
            let sync_interval = config.upstream_sync_interval_secs;
            let force_reload = config.force_reload_interval_secs;
            tokio::spawn(async move {
                let mut interval = tokio::time::interval(Duration::from_secs(sync_interval));
                interval.tick().await;
                // Start at None so the first tick always reloads once, closing the
                // window between the startup load and the first version read.
                let mut last_version: Option<i64> = None;
                let mut last_full = Instant::now();
                loop {
                    interval.tick().await;
                    let current = load_config_version(
                        poll_db.pool(),
                        poll_db.dialect(),
                        ConfigResource::Routes,
                    )
                    .await;
                    let forced = force_reload > 0 && last_full.elapsed().as_secs() >= force_reload;
                    // A real config change (version differs from what we last loaded)
                    // is worth an INFO line; a periodic forced reload without a version
                    // change is not.
                    let changed = current != last_version;
                    // Skip the heavy reload when the version is unchanged (and no
                    // forced reload is due). A missing version row (None) always
                    // reloads, as a safe fallback.
                    if !forced && current.is_some() && current == last_version {
                        continue;
                    }
                    match load_all_upstream_groups(poll_db.pool(), poll_db.dialect()).await {
                        Ok(groups) if groups.is_empty() => {
                            // DB returned 0 routes — honour the admin's intent and clear
                            // the registry. Network/DB errors surface as Err, not Ok([]),
                            // so this is a genuine "no routes" signal from the database.
                            registry.reload_all(groups);
                            last_version = current;
                            last_full = Instant::now();
                            tracing::warn!(
                                "DB poll returned 0 upstream groups — registry cleared, all model requests will return 503"
                            );
                        }
                        Ok(groups) => {
                            let count = groups.len();
                            registry.reload_all(groups);
                            last_version = current;
                            last_full = Instant::now();
                            if changed {
                                info!("UpstreamRegistry reloaded from database ({} groups)", count);
                            } else {
                                tracing::debug!(
                                    "UpstreamRegistry force-reloaded from database ({} groups)",
                                    count
                                );
                            }
                        }
                        // Leave last_version/last_full unchanged so the next tick retries.
                        Err(e) => tracing::warn!("UpstreamRegistry reload failed: {}", e),
                    }
                }
            });
        }

        Some(db)
    };

    // ── Auth: explicit no-auth, file, or database ────────────────────────────
    let (api_key_repo, auth_required): (Arc<dyn ApiKeyRepository>, bool) = if config.no_auth {
        // Explicit opt-out: accept all requests without a key.
        warn!("--no-auth is set: API key authentication is disabled, all requests accepted");
        let cached = CachedApiKeyRepository::new();
        (cached.into_shared(), false)
    } else if let Some(auth_path) = &config.auth_file {
        // Auth from file: load keys and watch for changes.
        info!("Loading auth config from file: {}", auth_path);
        let auth_output = AuthConfigSource::new(auth_path)
            .load()
            .map_err(|e| format!("Failed to load auth file: {e}"))?;

        let cached = CachedApiKeyRepository::new();
        cached.reload(auth_output.auth_keys);
        info!("API key auth loaded from file");

        let cached_clone = cached.clone();
        let path = auth_path.clone();
        let interval = config.upstream_sync_interval_secs;
        tokio::spawn(async move {
            let mut last_mod = file_modified_time(&path);
            let mut tick = tokio::time::interval(Duration::from_secs(interval));
            tick.tick().await;
            loop {
                tick.tick().await;
                let current_mod = file_modified_time(&path);
                if current_mod != last_mod {
                    last_mod = current_mod;
                    match AuthConfigSource::new(&path).load() {
                        Ok(out) => {
                            cached_clone.reload(out.auth_keys);
                            info!("Auth config reloaded from file");
                        }
                        Err(e) => tracing::warn!("Auth config reload failed: {}", e),
                    }
                }
            }
        });

        (cached.into_shared(), true)
    } else if config.route_file.is_some() {
        // File route config without --auth-file and without --no-auth: refuse to start.
        return Err(
            "File config mode requires either --auth-file <path> or --no-auth. \
                Pass --no-auth to run without API key authentication."
                .into(),
        );
    } else {
        // Database mode: auth from DB (realtime or cached).
        let db = database
            .as_ref()
            .expect("database must be connected in DB mode");
        let sql_key_repo = SqlApiKeyRepository::new(db.pool().clone(), db.dialect());

        let repo: Arc<dyn ApiKeyRepository> = match config.auth_mode {
            AuthMode::Realtime => {
                info!("API key auth mode: realtime (per-request DB lookup)");
                match sql_key_repo.find_all_active().await {
                    Ok(entries) if entries.is_empty() => {
                        warn!(
                            "No active API keys found in the database — \
                                all requests will be rejected with 401. \
                                Create an API key via the Admin panel first."
                        );
                    }
                    Err(e) => tracing::warn!("Initial API key check failed: {}", e),
                    _ => {}
                }
                sql_key_repo.into_shared()
            }
            AuthMode::Cached => {
                info!(
                    "API key auth mode: cached (version-checked every {}s)",
                    config.upstream_sync_interval_secs
                );
                let cached = CachedApiKeyRepository::new();

                match sql_key_repo.find_all_active().await {
                    Ok(entries) => {
                        if entries.is_empty() {
                            warn!(
                                "No active API keys found in the database — \
                                    all requests will be rejected with 401. \
                                    Create an API key via the Admin panel first."
                            );
                        }
                        cached.reload(entries);
                        info!("API key cache loaded");
                    }
                    Err(e) => tracing::warn!("Initial API key cache load failed: {}", e),
                }

                {
                    let cached_clone = cached.clone();
                    let reload_repo = SqlApiKeyRepository::new(db.pool().clone(), db.dialect());
                    let version_pool = db.pool().clone();
                    let version_dialect = db.dialect();
                    let sync_interval = config.upstream_sync_interval_secs;
                    let force_reload = config.force_reload_interval_secs;
                    tokio::spawn(async move {
                        // Version-gated: only reloads when an admin mutation
                        // bumps the api_keys version. This is correct only while
                        // the active-key set is purely mutation-driven — i.e.
                        // `api_keys.expires_at` stays NULL. If key expiry is ever
                        // introduced, natural expiry won't bump the version and an
                        // expired key would linger until the forced reload; make
                        // the cache expiry-aware then. See auth::find_all_active.
                        let mut interval =
                            tokio::time::interval(Duration::from_secs(sync_interval));
                        interval.tick().await;
                        let mut last_version: Option<i64> = None;
                        let mut last_full = Instant::now();
                        loop {
                            interval.tick().await;
                            let current = load_config_version(
                                &version_pool,
                                version_dialect,
                                ConfigResource::ApiKeys,
                            )
                            .await;
                            let forced =
                                force_reload > 0 && last_full.elapsed().as_secs() >= force_reload;
                            let changed = current != last_version;
                            if !forced && current.is_some() && current == last_version {
                                continue;
                            }
                            match reload_repo.find_all_active().await {
                                Ok(entries) => {
                                    let count = entries.len();
                                    cached_clone.reload(entries);
                                    last_version = current;
                                    last_full = Instant::now();
                                    if changed {
                                        info!("API key cache reloaded ({} active keys)", count);
                                    } else {
                                        tracing::debug!(
                                            "API key cache force-reloaded ({} active keys)",
                                            count
                                        );
                                    }
                                }
                                Err(e) => tracing::warn!("API key cache reload failed: {}", e),
                            }
                        }
                    });
                }

                cached.into_shared()
            }
        };
        (repo, true)
    };

    let (log_sink, log_writer) = match &database {
        Some(db) => match log_sink::start(db.pool().clone(), db.dialect()).await {
            Ok((sink, writer)) => {
                info!("Access log writer started (database)");
                (sink, writer)
            }
            Err(e) => {
                warn!(
                    "Failed to start access log writer: {e} — access logs will only go to stdout"
                );
                (LogSink::noop(), LogWriter::noop())
            }
        },
        None => (LogSink::noop(), LogWriter::noop()),
    };

    let router: Arc<dyn RouterTrait> =
        Arc::new(GatewayRouter::new(&app_context, log_sink.clone()).await?);

    let rate_limiter: Option<Arc<dyn RateLimiter>> = if let Some(rl_cfg) = &config.rate_limit {
        if let Some(redis_url) = &rl_cfg.redis_url {
            // Redis backend: distributed, suitable for multi-instance deployments.
            // Connection failure at startup is fatal — do not silently fall back.
            let rl = RedisRateLimiter::new(redis_url, rl_cfg.window_secs)
                .await
                .map_err(|e| format!("Failed to connect to Redis ({redis_url}): {e}"))?;
            info!(%redis_url, window_secs = rl_cfg.window_secs, "Rate limiter initialized (Redis)");
            Some(rl)
        } else {
            // Memory backend: in-process, no external dependency.
            let rl = MemoryRateLimiter::new(rl_cfg.window_secs);
            info!(
                window_secs = rl_cfg.window_secs,
                "Rate limiter initialized (memory)"
            );
            Some(rl)
        }
    } else {
        None
    };

    let quota_store = QuotaStore::new();
    if let Some(quota_path) = &config.quota_file {
        // File mode: load quota overrides from YAML and watch for changes.
        match QuotaConfigSource::new(quota_path).load() {
            Ok(entries) => {
                quota_store.reload(entries);
                info!("Quota config loaded from file: {}", quota_path);
            }
            Err(e) => tracing::warn!("Failed to load quota config: {}", e),
        }
        {
            let store = quota_store.clone();
            let path = quota_path.clone();
            let interval_secs = config.upstream_sync_interval_secs;
            tokio::spawn(async move {
                let mut last_mod = file_modified_time(&path);
                let mut tick = tokio::time::interval(Duration::from_secs(interval_secs));
                tick.tick().await;
                loop {
                    tick.tick().await;
                    let current_mod = file_modified_time(&path);
                    if current_mod != last_mod {
                        last_mod = current_mod;
                        match QuotaConfigSource::new(&path).load() {
                            Ok(entries) => {
                                store.reload(entries);
                                info!("Quota config reloaded from file");
                            }
                            Err(e) => tracing::warn!("Quota config reload failed: {}", e),
                        }
                    }
                }
            });
        }
    } else if let Some(ref db) = database {
        // Database mode: load quota overrides from DB and poll for changes.
        // Uses the same poll interval as the upstream registry sync.
        // ArcSwap::store() in QuotaStore::reload() is atomic — reads on the
        // hot path always see a consistent snapshot; no lock is held during the swap.
        match load_all_quota_overrides(db.pool(), db.dialect()).await {
            Ok(entries) => {
                let count = entries.len();
                quota_store.reload(entries);
                info!("Quota overrides loaded from database ({} entries)", count);
            }
            Err(e) => tracing::warn!("Failed to load quota overrides from database: {}", e),
        }
        {
            let store = quota_store.clone();
            let poll_db = db.clone();
            let sync_interval = config.upstream_sync_interval_secs;
            let force_reload = config.force_reload_interval_secs;
            tokio::spawn(async move {
                let mut interval = tokio::time::interval(Duration::from_secs(sync_interval));
                interval.tick().await;
                let mut last_version: Option<i64> = None;
                let mut last_full = Instant::now();
                loop {
                    interval.tick().await;
                    let current = load_config_version(
                        poll_db.pool(),
                        poll_db.dialect(),
                        ConfigResource::Quota,
                    )
                    .await;
                    let forced = force_reload > 0 && last_full.elapsed().as_secs() >= force_reload;
                    let changed = current != last_version;
                    if !forced && current.is_some() && current == last_version {
                        continue;
                    }
                    match load_all_quota_overrides(poll_db.pool(), poll_db.dialect()).await {
                        Ok(entries) => {
                            let count = entries.len();
                            store.reload(entries);
                            last_version = current;
                            last_full = Instant::now();
                            if changed {
                                info!("Quota overrides reloaded from database ({} entries)", count);
                            } else {
                                tracing::debug!(
                                    "Quota overrides force-reloaded from database ({} entries)",
                                    count
                                );
                            }
                        }
                        Err(e) => tracing::warn!("Quota overrides reload failed: {}", e),
                    }
                }
            });
        }
    }

    let app_state = Arc::new(GatewayState {
        router,
        context: app_context.clone(),
        api_key_repo: api_key_repo.clone(),
        auth_required,
        rate_limiter,
        quota_store,
        log_sink,
    });

    let request_id_headers = config.request_id_headers.clone().unwrap_or_else(|| {
        vec![
            "x-request-id".to_string(),
            "x-correlation-id".to_string(),
            "x-trace-id".to_string(),
            "request-id".to_string(),
        ]
    });

    let cors = cors_layer(&config.cors_allowed_origins)
        .map_err(|msg| std::io::Error::new(std::io::ErrorKind::InvalidInput, msg))?;
    let app = build_app(app_state, config.max_payload_size, request_id_headers, cors);
    let bind_addr = format!("{}:{}", config.host, config.port);

    println!();

    info!(
        "Starting gateway on {}:{} | max_payload: {}MB",
        config.host,
        config.port,
        config.max_payload_size / (1024 * 1024)
    );

    // Parse address and set up graceful shutdown (common to both TLS and non-TLS)
    let addr: std::net::SocketAddr = bind_addr
        .parse()
        .map_err(|e| format!("Invalid address: {e}"))?;

    let handle = axum_server::Handle::new();
    let handle_clone = handle.clone();
    let grace_period = Duration::from_secs(config.shutdown_grace_period_secs);
    spawn(async move {
        shutdown_signal().await;
        handle_clone.graceful_shutdown(Some(grace_period));
    });

    if let (Some(cert), Some(key)) = (
        &config.router_config.server_cert,
        &config.router_config.server_key,
    ) {
        info!("TLS enabled");
        ring::default_provider()
            .install_default()
            .map_err(|e| format!("Failed to install rustls ring provider: {e:?}"))?;

        let tls_config = axum_server::tls_rustls::RustlsConfig::from_pem(cert.clone(), key.clone())
            .await
            .map_err(|e| format!("Failed to create TLS config: {e}"))?;

        axum_server::bind_rustls(addr, tls_config)
            .handle(handle)
            .serve(app.into_make_service())
            .await
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error>)?;
    } else {
        axum_server::bind(addr)
            .handle(handle)
            .serve(app.into_make_service())
            .await
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error>)?;
    }

    // HTTP server has stopped accepting new requests.  Flush any access log
    // records that are still buffered in the background writer before exit.
    info!("Flushing access log writer...");
    log_writer.shutdown().await;

    Ok(())
}

fn file_modified_time(path: &str) -> Option<std::time::SystemTime> {
    std::fs::metadata(path).ok().and_then(|m| m.modified().ok())
}

async fn shutdown_signal() {
    let ctrl_c = async {
        signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("failed to install signal handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {
            info!("Received Ctrl+C, starting graceful shutdown");
        },
        _ = terminate => {
            info!("Received terminate signal, starting graceful shutdown");
        },
    }
}
