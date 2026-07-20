use serde::{Deserialize, Serialize};

use crate::observability::metrics;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TraceConfig {
    pub enable_trace: bool,
    pub otlp_traces_endpoint: String,
    pub service_env: String,
    pub service_version: String,
}

impl Default for TraceConfig {
    fn default() -> Self {
        Self {
            enable_trace: false,
            otlp_traces_endpoint: std::env::var("OTLP_ENDPOINT")
                .ok()
                .unwrap_or_else(|| "http://localhost:4318/v1/traces".to_string()),
            service_env: std::env::var("OTEL_SERVICE_ENV")
                .unwrap_or_else(|_| "development".to_string()),
            service_version: std::env::var("OTEL_SERVICE_VERSION")
                .unwrap_or_else(|_| env!("CARGO_PKG_VERSION").to_string()),
        }
    }
}

/// API key authentication mode.
#[derive(Debug, Clone, Default, PartialEq)]
pub enum AuthMode {
    /// Periodically load all active keys into memory; auth is a pure in-memory lookup.
    /// Key revocations take effect within one TTL interval.
    #[default]
    Cached,
    /// Query the database on every request. Always up-to-date but adds DB latency to the hot path.
    Realtime,
}

/// Rate limiter configuration.
///
/// When `redis_url` is absent an in-process sliding-window limiter is used —
/// no external service required, suitable for single-instance deployments.
/// Set `redis_url` to enable the Redis-backed distributed limiter.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RateLimitConfig {
    /// Redis connection URL, e.g. `redis://127.0.0.1:6379`.
    /// When absent, the in-process memory limiter is used instead.
    #[serde(default)]
    pub redis_url: Option<String>,
    /// Sliding window size in seconds (default: 60).
    #[serde(default = "default_rl_window_secs")]
    pub window_secs: u64,
}

fn default_rl_window_secs() -> u64 {
    60
}

pub struct ServerConfig {
    pub host: String,
    pub port: u16,
    pub log_dir: Option<String>,
    pub log_level: Option<String>,
    pub json_log: bool,
    pub request_id_headers: Option<Vec<String>>,
    pub shutdown_grace_period_secs: u64,
    pub upstream_sync_interval_secs: u64,
    /// How often (in seconds) each database-backed polling task performs a full
    /// reload regardless of the config version, as a safety net against a lost
    /// version bump. Set to 0 to disable and rely solely on version changes.
    pub force_reload_interval_secs: u64,
    pub trace_config: Option<TraceConfig>,
    pub prometheus_config: Option<metrics::PrometheusConfig>,
    pub router_config: RouterConfig,
    pub max_payload_size: usize,
    /// Empty: no CORS headers (default). Non-empty: permissive methods/headers with these `Access-Control-Allow-Origin` values.
    pub cors_allowed_origins: Vec<String>,
    pub database: DatabaseConfig,
    pub auth_mode: AuthMode,
    /// Path to a YAML route config file. When set, the gateway loads upstreams
    /// and routes from the file instead of connecting to a database.
    pub route_file: Option<String>,
    /// Path to a YAML auth key file. Required in file-config mode unless
    /// --no-auth is set. Watched for changes and reloaded automatically.
    pub auth_file: Option<String>,
    /// Disable API key authentication entirely. All requests are accepted without
    /// a key. Must be set explicitly; there is no implicit no-auth path.
    pub no_auth: bool,
    /// Rate limiter config. When absent, rate limiting is disabled.
    pub rate_limit: Option<RateLimitConfig>,
    /// Path to a YAML quota override file. When set, per-(api_key, model) limits
    /// are loaded from this file and watched for changes.
    pub quota_file: Option<String>,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            host: "127.0.0.1".to_string(),
            port: 8080,
            log_dir: None,
            log_level: Some("info".to_string()),
            json_log: false,
            request_id_headers: None,
            shutdown_grace_period_secs: 5,
            upstream_sync_interval_secs: 30,
            force_reload_interval_secs: 3600,
            trace_config: Some(TraceConfig::default()),
            prometheus_config: Some(metrics::PrometheusConfig::default()),
            router_config: RouterConfig::default(),
            max_payload_size: 512 * 1024 * 1024, // 512 MB
            cors_allowed_origins: Vec::new(),
            database: DatabaseConfig::default(),
            auth_mode: AuthMode::Cached,
            route_file: None,
            auth_file: None,
            no_auth: false,
            rate_limit: None,
            quota_file: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatabaseConfig {
    pub url: String,
    pub max_connections: u32,
    pub min_connections: u32,
    pub acquire_timeout_secs: u64,
}

impl Default for DatabaseConfig {
    fn default() -> Self {
        Self {
            url: std::env::var("DATABASE_URL")
                .ok()
                .unwrap_or_else(|| "sqlite://model_gateway.db?mode=rwc".to_string()),
            max_connections: 20,
            min_connections: 1,
            acquire_timeout_secs: 5,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RouterConfig {
    pub request_timeout_secs: u64,
    /// Combined certificate + key in PEM format, loaded from client_cert_path and client_key_path during config creation
    #[serde(skip)]
    pub client_identity: Option<Vec<u8>>,
    /// PEM format, loaded from ca_cert_paths during config creation
    #[serde(default)]
    pub ca_certificates: Vec<Vec<u8>>,
    /// Server TLS certificate (PEM)
    #[serde(skip)]
    pub server_cert: Option<Vec<u8>>,
    /// Server TLS private key (PEM)
    #[serde(skip)]
    pub server_key: Option<Vec<u8>>,
    pub retry_config: Option<RetryConfig>,
    /// If true, log the full request and response body in the access log.
    /// Warning: for streaming responses the entire body will be buffered in memory.
    #[serde(default)]
    pub log_request_body: bool,
    /// If true, automatically inject `stream_options: {include_usage: true}` into
    /// streaming chat requests that do not already specify stream_options.
    #[serde(default = "default_true")]
    pub inject_stream_usage: bool,
}

impl Default for RouterConfig {
    fn default() -> Self {
        Self {
            request_timeout_secs: 30,
            client_identity: None,
            ca_certificates: Vec::new(),
            server_cert: None,
            server_key: None,
            retry_config: None,
            log_request_body: false,
            inject_stream_usage: true,
        }
    }
}

impl RouterConfig {
    pub fn effective_retry_config(&self) -> RetryConfig {
        self.retry_config.clone().unwrap_or_default()
    }
}

/// Retry configuration for request handling
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetryConfig {
    pub max_retries: u32,
    pub initial_backoff_ms: u64,
    pub max_backoff_ms: u64,
    pub backoff_multiplier: f32,
    /// D' = D * (1 + U[-j, +j]) where j is jitter factor
    #[serde(default = "default_retry_jitter_factor")]
    pub jitter_factor: f32,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_retries: 5,
            initial_backoff_ms: 50,
            max_backoff_ms: 30000,
            backoff_multiplier: 1.5,
            jitter_factor: 0.2,
        }
    }
}

fn default_retry_jitter_factor() -> f32 {
    0.2
}

fn default_true() -> bool {
    true
}
