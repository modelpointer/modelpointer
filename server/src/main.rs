use std::error::Error;
use clap::{Args, Parser, Subcommand};
use modelpointer_core::{
    config::{AuthMode, DatabaseConfig, RetryConfig, RouterConfig, ServerConfig, TraceConfig},
    observability::{metrics::PrometheusConfig, otel_trace::{is_otel_enabled, shutdown_otel}},
    version,
};

mod auth;
mod auth_config;
mod db;
mod env_expand;
mod file_config;
mod key_cmd;
mod log_sink;
mod middleware;
mod quota_config;
mod rate_limit_memory;
mod rate_limit_redis;
mod router;
mod server;

use db::{Database, DatabaseDialect, access_log_store};

// ── Top-level CLI ─────────────────────────────────────────────────────────────

#[derive(Parser, Debug)]
#[command(name = "modelpointer", about = "ModelPointer Gateway")]
enum Cli {
    /// Start the gateway server
    Serve(ServeArgs),
    /// Manage API keys for file-based authentication
    Key {
        #[command(subcommand)]
        command: KeyCommand,
    },
    /// Delete access log records older than a given age
    Cleanup(CleanupArgs),
}

// ── Key subcommands ───────────────────────────────────────────────────────────

#[derive(Subcommand, Debug)]
enum KeyCommand {
    /// Generate a new API key
    Generate {
        /// Human-readable label for this key
        #[arg(long)]
        name: String,
        /// Append the new key directly to an auth.yaml file
        #[arg(long)]
        append: Option<String>,
    },
    /// Disable a key (gateway rejects it; entry is preserved for audit)
    Disable {
        /// Key ID to disable
        id: String,
        /// Path to the auth.yaml file
        file: String,
    },
    /// Re-enable a previously disabled key
    Enable {
        /// Key ID to enable
        id: String,
        /// Path to the auth.yaml file
        file: String,
    },
    /// List all keys in an auth.yaml file
    List {
        /// Path to the auth.yaml file
        file: String,
    },
}

// ── Cleanup args ──────────────────────────────────────────────────────────────

#[derive(Args, Debug)]
struct CleanupArgs {
    /// Retention window, e.g. "30d", "7d", "24h".
    /// Records (or partitions) older than this are deleted.
    #[arg(long)]
    older_than: String,

    /// Database URL (defaults to SQLite in the current directory)
    #[arg(long, env = "DATABASE_URL", default_value = "sqlite://model_gateway.db?mode=rwc")]
    database_url: String,
}

// ── Serve args ────────────────────────────────────────────────────────────────

#[derive(Args, Debug)]
struct ServeArgs {
    /// Host to listen on
    #[arg(long, default_value = "0.0.0.0")]
    host: String,

    /// Port to listen on
    #[arg(long, default_value_t = 8080)]
    port: u16,

    /// Directory for log files
    #[arg(long)]
    log_dir: Option<String>,

    /// Log level (trace, debug, info, warn, error)
    #[arg(long, default_value = "info")]
    log_level: String,

    /// Enable JSON logging
    #[arg(long, default_value_t = false)]
    json_log: bool,

    /// Graceful shutdown period in seconds
    #[arg(long, default_value_t = 5)]
    shutdown_grace_period_secs: u64,

    /// Upstream registry sync interval in seconds
    #[arg(long, default_value_t = 30)]
    upstream_sync_interval_secs: u64,

    /// Maximum request payload size in bytes
    #[arg(long, default_value_t = 512 * 1024 * 1024)]
    max_payload_size: usize,

    /// Allowed CORS origins; repeat flag for multiple
    #[arg(long)]
    cors_allowed_origins: Vec<String>,

    /// HTTP header names to read inbound request IDs from; repeat for multiple
    #[arg(long)]
    request_id_headers: Vec<String>,

    /// Enable distributed tracing
    #[arg(long, default_value_t = false)]
    enable_trace: bool,

    /// OTLP traces endpoint
    #[arg(long, env = "OTLP_ENDPOINT", default_value = "http://localhost:4318/v1/traces")]
    otlp_traces_endpoint: String,

    /// Database max connections
    #[arg(long, default_value_t = 20)]
    db_max_connections: u32,

    /// Database min connections
    #[arg(long, default_value_t = 1)]
    db_min_connections: u32,

    /// Database acquire timeout in seconds
    #[arg(long, default_value_t = 5)]
    db_acquire_timeout_secs: u64,

    /// Database URL
    #[arg(long, env = "DATABASE_URL", default_value = "sqlite://model_gateway.db?mode=rwc")]
    database_url: String,

    /// Prometheus metrics host
    #[arg(long, default_value = "0.0.0.0")]
    prometheus_host: String,

    /// Prometheus metrics port
    #[arg(long, default_value_t = 29000)]
    prometheus_port: u16,

    /// Request timeout in seconds
    #[arg(long, default_value_t = 30)]
    request_timeout_secs: u64,

    /// Path to TLS certificate file (PEM format)
    #[arg(long)]
    tls_cert_path: Option<String>,

    /// Path to TLS private key file (PEM format)
    #[arg(long)]
    tls_key_path: Option<String>,

    /// Maximum number of retries
    #[arg(long, default_value_t = 5)]
    retry_max_retries: u32,

    /// Initial backoff in milliseconds
    #[arg(long, default_value_t = 50)]
    retry_initial_backoff_ms: u64,

    /// Maximum backoff in milliseconds
    #[arg(long, default_value_t = 30000)]
    retry_max_backoff_ms: u64,

    /// Backoff multiplier
    #[arg(long, default_value_t = 1.5)]
    retry_backoff_multiplier: f32,

    /// Jitter factor for retry backoff
    #[arg(long, default_value_t = 0.2)]
    retry_jitter_factor: f32,

    /// API key authentication mode: "cached" or "realtime"
    #[arg(long, default_value = "cached")]
    auth_mode: String,

    /// How often (in seconds) the cached auth mode reloads active keys
    #[arg(long, default_value_t = 60)]
    auth_cache_ttl_secs: u64,

    /// Log full request and response bodies in the access log
    #[arg(long, default_value_t = false)]
    log_request_body: bool,

    /// Path to a YAML route config file. When set, upstreams and routes are loaded
    /// from the file and no database connection is made.
    #[arg(long)]
    route_file: Option<String>,

    /// Path to a YAML auth key file. When set, API key authentication is loaded
    /// from this file and watched for changes. If omitted while --config-file is
    /// used, the gateway runs without authentication.
    #[arg(long)]
    auth_file: Option<String>,

    /// Path to a YAML quota override file for per-(api_key, model) rate-limit overrides.
    #[arg(long)]
    quota_file: Option<String>,

    /// Rate-limit sliding window in seconds. Setting this enables rate limiting.
    /// Without --rl-redis-url an in-process memory limiter is used (single-instance only).
    #[arg(long, default_value_t = 60)]
    rl_window_secs: u64,

    /// Redis URL for distributed rate limiting, e.g. redis://:password@host:6379/0.
    /// When set, the Redis backend is used instead of the in-process memory limiter.
    /// Supports the standard redis:// and rediss:// (TLS) URL formats.
    #[arg(long, env = "RL_REDIS_URL")]
    rl_redis_url: Option<String>,
}

// ── Entry point ───────────────────────────────────────────────────────────────

fn main() -> Result<(), Box<dyn Error>> {
    // Handle --version before clap to print the custom version string.
    let raw_args: Vec<String> = std::env::args().collect();
    for arg in &raw_args {
        if arg == "--version" || arg == "-V" {
            println!("{}", version::get_version_string());
            return Ok(());
        }
    }

    match Cli::parse() {
        Cli::Serve(args)      => run_serve(args),
        Cli::Key { command }  => run_key(command),
        Cli::Cleanup(args)    => run_cleanup(args),
    }
}

// ── Serve ─────────────────────────────────────────────────────────────────────

fn run_serve(args: ServeArgs) -> Result<(), Box<dyn Error>> {
    println!("modelpointer starting...");

    let server_cert = args.tls_cert_path
        .map(|p| std::fs::read(&p).map_err(|e| format!("Failed to read TLS cert '{}': {}", p, e)))
        .transpose()?;
    let server_key = args.tls_key_path
        .map(|p| std::fs::read(&p).map_err(|e| format!("Failed to read TLS key '{}': {}", p, e)))
        .transpose()?;

    let server_config = ServerConfig {
        host: args.host,
        port: args.port,
        log_dir: args.log_dir,
        log_level: Some(args.log_level),
        json_log: args.json_log,
        request_id_headers: (!args.request_id_headers.is_empty())
            .then_some(args.request_id_headers),
        shutdown_grace_period_secs: args.shutdown_grace_period_secs,
        upstream_sync_interval_secs: args.upstream_sync_interval_secs,
        max_payload_size: args.max_payload_size,
        cors_allowed_origins: args.cors_allowed_origins,
        trace_config: Some(TraceConfig {
            enable_trace: args.enable_trace,
            otlp_traces_endpoint: args.otlp_traces_endpoint,
            ..TraceConfig::default()
        }),
        prometheus_config: Some(PrometheusConfig {
            host: args.prometheus_host,
            port: args.prometheus_port,
            duration_buckets: None,
            ttft_buckets: None,
        }),
        router_config: RouterConfig {
            request_timeout_secs: args.request_timeout_secs,
            server_cert,
            server_key,
            retry_config: Some(RetryConfig {
                max_retries: args.retry_max_retries,
                initial_backoff_ms: args.retry_initial_backoff_ms,
                max_backoff_ms: args.retry_max_backoff_ms,
                backoff_multiplier: args.retry_backoff_multiplier,
                jitter_factor: args.retry_jitter_factor,
            }),
            log_request_body: args.log_request_body,
            ..Default::default()
        },
        database: DatabaseConfig {
            url: args.database_url,
            max_connections: args.db_max_connections,
            min_connections: args.db_min_connections,
            acquire_timeout_secs: args.db_acquire_timeout_secs,
        },
        auth_mode: match args.auth_mode.to_ascii_lowercase().as_str() {
            "realtime" => AuthMode::Realtime,
            _ => AuthMode::Cached,
        },
        auth_cache_ttl_secs: args.auth_cache_ttl_secs,
        route_file: args.route_file,
        auth_file: args.auth_file,
        quota_file: args.quota_file,
        rate_limit: Some(modelpointer_core::config::RateLimitConfig {
            redis_url: args.rl_redis_url,
            window_secs: args.rl_window_secs,
        }),
    };

    let runtime = tokio::runtime::Runtime::new()?;
    runtime.block_on(async move { server::startup(server_config).await })?;
    if is_otel_enabled() {
        shutdown_otel();
    }
    Ok(())
}

// ── Cleanup ───────────────────────────────────────────────────────────────────

fn run_cleanup(args: CleanupArgs) -> Result<(), Box<dyn Error>> {
    let duration = parse_duration(&args.older_than)
        .ok_or_else(|| format!(
            "Invalid --older-than value {:?}. Use a number followed by 'd' (days) or 'h' (hours), e.g. \"30d\" or \"24h\".",
            args.older_than
        ))?;

    let cutoff = chrono::Utc::now() - duration;
    println!("Deleting access log records older than {} (cutoff: {})", args.older_than, cutoff.to_rfc3339());

    let runtime = tokio::runtime::Runtime::new()?;
    runtime.block_on(async move {
        let db_config = modelpointer_core::config::DatabaseConfig {
            url: args.database_url,
            max_connections: 2,
            min_connections: 1,
            acquire_timeout_secs: 5,
        };
        let db = Database::connect(&db_config).await
            .map_err(|e| format!("Failed to connect to database: {e}"))?;

        match db.dialect() {
            DatabaseDialect::Postgres => {
                let dropped = access_log_store::drop_pg_partitions_older_than(db.pool(), cutoff)
                    .await
                    .map_err(|e| format!("Cleanup failed: {e}"))?;
                println!("Dropped {dropped} partition(s).");
            }
            dialect => {
                let deleted = access_log_store::delete_older_than(db.pool(), dialect, cutoff)
                    .await
                    .map_err(|e| format!("Cleanup failed: {e}"))?;
                println!("Deleted {deleted} row(s).");
            }
        }

        Ok::<_, String>(())
    })?;

    Ok(())
}

/// Parse a human duration string into a `chrono::Duration`.
/// Supported suffixes: `d` (days), `h` (hours).
fn parse_duration(s: &str) -> Option<chrono::Duration> {
    let s = s.trim();
    if let Some(n) = s.strip_suffix('d') {
        let days: i64 = n.trim().parse().ok()?;
        return Some(chrono::Duration::days(days));
    }
    if let Some(n) = s.strip_suffix('h') {
        let hours: i64 = n.trim().parse().ok()?;
        return Some(chrono::Duration::hours(hours));
    }
    None
}

// ── Key management ────────────────────────────────────────────────────────────

fn run_key(command: KeyCommand) -> Result<(), Box<dyn Error>> {
    match command {
        KeyCommand::Generate { name, append } => {
            key_cmd::cmd_generate(name, append)?;
        }
        KeyCommand::Disable { id, file } => {
            key_cmd::cmd_disable(id, file)?;
        }
        KeyCommand::Enable { id, file } => {
            key_cmd::cmd_enable(id, file)?;
        }
        KeyCommand::List { file } => {
            key_cmd::cmd_list(file)?;
        }
    }
    Ok(())
}
