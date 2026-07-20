use std::sync::Arc;

use async_trait::async_trait;

/// Identifies a rate-limit bucket.
/// Each variant maps to a distinct Redis key namespace.
pub enum RateLimitKey {
    /// Per (api_key, model) — the finest-grained bucket.
    /// Redis key: `rl:key:{api_key_id}:model:{model_id}:{dim}`
    KeyModel {
        api_key_id: String,
        model_id: String,
    },

    /// Per model across all API keys.
    /// Redis key: `rl:model:{model_id}:{dim}`
    Model { model_id: String },

    /// Primary-tier capacity for a (model, protocol) pair — controls spill-to-fallback routing.
    /// Checked pre-routing; exceeding this limit routes the request to the
    /// fallback tier instead of returning 429 to the client.
    /// Redis key: `rl:model:{model_id}:proto:{protocol}:primary:{dim}`
    PrimaryTier { model_id: String, protocol: String },
}

pub enum RateLimitDecision {
    Allow,
    Denied { retry_after_secs: u64 },
}

/// Passed through [`crate::router::RequestContext`] so the router can
/// post-record actual token usage after a successful response.
#[derive(Clone)]
pub struct RateLimitCtx {
    pub limiter: Arc<dyn RateLimiter>,
    pub model_id: String,
    /// Protocol ("openai" / "anthropic") — used to key the primary-tier TPM bucket.
    pub protocol: String,
    /// Record tokens to the per-(key, model) TPM bucket.
    pub record_key_tpm: bool,
    /// Record tokens to the per-model TPM bucket.
    pub record_model_tpm: bool,
    /// Record tokens to the primary-tier capacity TPM bucket.
    /// Starts as `primary_cap_tpm.is_some() && min_priority == 0` (set in
    /// `check_rate_limits`), then refined in the router after upstream selection
    /// to `&= upstream.priority() == 0` so that health-driven fallback requests
    /// (min_priority == 0 but routed to fallback by circuit open / health check)
    /// do not pollute the primary TPM bucket.
    pub record_primary_tpm: bool,
}

impl RateLimitCtx {
    /// Record actual token usage to all configured TPM buckets.
    /// Called after a successful response — non-streaming inline,
    /// streaming at the end of the SSE spawn.
    pub async fn record_tokens(&self, api_key_id: &str, tokens: u32) {
        if tokens == 0 {
            return;
        }
        if self.record_key_tpm {
            self.limiter
                .record_tokens(
                    &RateLimitKey::KeyModel {
                        api_key_id: api_key_id.to_string(),
                        model_id: self.model_id.clone(),
                    },
                    tokens,
                )
                .await;
        }
        if self.record_model_tpm {
            self.limiter
                .record_tokens(
                    &RateLimitKey::Model {
                        model_id: self.model_id.clone(),
                    },
                    tokens,
                )
                .await;
        }
        if self.record_primary_tpm {
            self.limiter
                .record_tokens(
                    &RateLimitKey::PrimaryTier {
                        model_id: self.model_id.clone(),
                        protocol: self.protocol.clone(),
                    },
                    tokens,
                )
                .await;
        }
    }
}

#[async_trait]
pub trait RateLimiter: Send + Sync {
    /// RPM: atomic sliding-window check + increment (hard limit).
    async fn check_rpm(&self, key: &RateLimitKey, limit: u32) -> RateLimitDecision;

    /// TPM: read-only sliding-window token-sum check (soft limit).
    async fn check_tpm(&self, key: &RateLimitKey, limit: u32) -> RateLimitDecision;

    /// Append actual token usage to a TPM bucket.
    async fn record_tokens(&self, key: &RateLimitKey, tokens: u32);
}
