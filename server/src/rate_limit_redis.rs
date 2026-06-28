//! Redis-backed rate limiter using sliding-window counters.
//!
//! RPM: atomic check + increment (hard limit).
//! TPM: separate check and record (soft limit — bounded overshoot accepted).
//!
//! Both use a sorted-set (ZSET) keyed by `rl:{api_key_id}:{model_id}:{dim}`.
//! RPM members are opaque UUIDs; TPM members encode token count as `{uuid}:{tokens}`
//! so the window sum can be computed server-side in Lua.
//!
//! All Redis failures are **fail-open**: the request is allowed and a WARN is logged.

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use redis::{aio::ConnectionManager, Client, Script};
use tracing::warn;
use uuid::Uuid;

use modelpointer_core::rate_limit::{RateLimitDecision, RateLimitKey, RateLimiter};

// ── Lua scripts ───────────────────────────────────────────────────────────────

/// Atomic RPM sliding-window check + increment.
/// Returns 1 (allow) or 0 (deny).
const RPM_SCRIPT: &str = r#"
local now    = tonumber(ARGV[1])
local window = tonumber(ARGV[2])
local limit  = tonumber(ARGV[3])
local member = ARGV[4]
redis.call('ZREMRANGEBYSCORE', KEYS[1], 0, now - window)
local count = redis.call('ZCARD', KEYS[1])
if count >= limit then
    return 0
end
redis.call('ZADD', KEYS[1], now, member)
redis.call('PEXPIRE', KEYS[1], window + 1000)
return 1
"#;

/// Read-only TPM sliding-window sum.
/// Cleans up expired entries and returns the current total token count.
const TPM_CHECK_SCRIPT: &str = r#"
local now    = tonumber(ARGV[1])
local window = tonumber(ARGV[2])
redis.call('ZREMRANGEBYSCORE', KEYS[1], 0, now - window)
local entries = redis.call('ZRANGE', KEYS[1], 0, -1)
local total = 0
for _, v in ipairs(entries) do
    local sep = string.find(v, ':', 1, true)
    if sep then
        total = total + tonumber(string.sub(v, sep + 1))
    end
end
return total
"#;

/// Append a token-usage entry to the TPM window.
const TPM_RECORD_SCRIPT: &str = r#"
redis.call('ZADD', KEYS[1], tonumber(ARGV[1]), ARGV[2])
redis.call('PEXPIRE', KEYS[1], tonumber(ARGV[3]) + 1000)
return 1
"#;

// ── RedisRateLimiter ──────────────────────────────────────────────────────────

pub struct RedisRateLimiter {
    conn: ConnectionManager,
    window_ms: i64,
}

impl RedisRateLimiter {
    pub async fn new(redis_url: &str, window_secs: u64) -> Result<Arc<Self>, String> {
        let client =
            Client::open(redis_url).map_err(|e| format!("Redis client error: {}", e))?;
        let conn = ConnectionManager::new(client)
            .await
            .map_err(|e| format!("Redis connect error: {}", e))?;
        Ok(Arc::new(Self {
            conn,
            window_ms: (window_secs * 1000) as i64,
        }))
    }

    fn now_ms() -> i64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64
    }

    fn rpm_key(key: &RateLimitKey) -> String {
        match key {
            RateLimitKey::KeyModel { api_key_id, model_id } => {
                format!("rl:key:{}:model:{}:rpm", api_key_id, model_id)
            }
            RateLimitKey::Model { model_id } => {
                format!("rl:model:{}:rpm", model_id)
            }
            RateLimitKey::PrimaryTier { model_id, protocol } => {
                format!("rl:model:{}:proto:{}:primary:rpm", model_id, protocol)
            }
        }
    }

    fn tpm_key(key: &RateLimitKey) -> String {
        match key {
            RateLimitKey::KeyModel { api_key_id, model_id } => {
                format!("rl:key:{}:model:{}:tpm", api_key_id, model_id)
            }
            RateLimitKey::Model { model_id } => {
                format!("rl:model:{}:tpm", model_id)
            }
            RateLimitKey::PrimaryTier { model_id, protocol } => {
                format!("rl:model:{}:proto:{}:primary:tpm", model_id, protocol)
            }
        }
    }
}

#[async_trait]
impl RateLimiter for RedisRateLimiter {
    async fn check_rpm(&self, key: &RateLimitKey, limit: u32) -> RateLimitDecision {
        let rpm_key = Self::rpm_key(key);
        let now = Self::now_ms();
        let member = Uuid::new_v4().to_string();
        let mut conn = self.conn.clone();

        let allowed: i64 = match Script::new(RPM_SCRIPT)
            .key(&rpm_key)
            .arg(now)
            .arg(self.window_ms)
            .arg(limit as i64)
            .arg(&member)
            .invoke_async(&mut conn)
            .await
        {
            Ok(v) => v,
            Err(e) => {
                warn!(error = %e, "Redis RPM check failed, failing open");
                return RateLimitDecision::Allow;
            }
        };

        if allowed == 0 {
            RateLimitDecision::Denied {
                retry_after_secs: (self.window_ms / 1000) as u64,
            }
        } else {
            RateLimitDecision::Allow
        }
    }

    async fn check_tpm(&self, key: &RateLimitKey, limit: u32) -> RateLimitDecision {
        let tpm_key = Self::tpm_key(key);
        let now = Self::now_ms();
        let mut conn = self.conn.clone();

        let current_tokens: i64 = match Script::new(TPM_CHECK_SCRIPT)
            .key(&tpm_key)
            .arg(now)
            .arg(self.window_ms)
            .invoke_async(&mut conn)
            .await
        {
            Ok(v) => v,
            Err(e) => {
                warn!(error = %e, "Redis TPM check failed, failing open");
                return RateLimitDecision::Allow;
            }
        };

        if current_tokens >= limit as i64 {
            RateLimitDecision::Denied {
                retry_after_secs: (self.window_ms / 1000) as u64,
            }
        } else {
            RateLimitDecision::Allow
        }
    }

    async fn record_tokens(&self, key: &RateLimitKey, tokens: u32) {
        if tokens == 0 {
            return;
        }
        let tpm_key = Self::tpm_key(key);
        let now = Self::now_ms();
        let member = format!("{}:{}", Uuid::new_v4(), tokens);
        let mut conn = self.conn.clone();

        let result: Result<i64, _> = Script::new(TPM_RECORD_SCRIPT)
            .key(&tpm_key)
            .arg(now)
            .arg(&member)
            .arg(self.window_ms)
            .invoke_async(&mut conn)
            .await;

        if let Err(e) = result {
            // Non-fatal: the TPM window will slightly under-count for this request.
            warn!(error = %e, tokens, "Redis TPM record failed");
        }
    }
}
