//! In-process sliding-window rate limiter.
//!
//! All state is stored in memory; no external service is required.
//! Suitable for single-instance deployments.
//!
//! For multi-instance deployments that require cross-process coordination,
//! use [`crate::rate_limit_redis::RedisRateLimiter`] instead.
//!
//! RPM: atomic check + increment behind a per-key mutex (hard limit).
//! TPM: separate read-only check and append (soft limit — bounded overshoot accepted).
//!
//! All operations are fail-safe by construction (no I/O, no external dependencies).

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use dashmap::DashMap;

use modelpointer_core::rate_limit::{RateLimitDecision, RateLimitKey, RateLimiter};

// ── Type aliases ──────────────────────────────────────────────────────────────

/// Sliding window of request timestamps (milliseconds) for RPM tracking.
type RpmWindow = Arc<Mutex<VecDeque<i64>>>;

/// Sliding window of (timestamp_ms, token_count) pairs for TPM tracking.
type TpmWindow = Arc<Mutex<VecDeque<(i64, u32)>>>;

// ── MemoryRateLimiter ─────────────────────────────────────────────────────────

pub struct MemoryRateLimiter {
    window_ms: i64,
    rpm_windows: DashMap<String, RpmWindow>,
    tpm_windows: DashMap<String, TpmWindow>,
}

impl MemoryRateLimiter {
    pub fn new(window_secs: u64) -> Arc<Self> {
        let rl = Arc::new(Self {
            window_ms: (window_secs * 1000) as i64,
            rpm_windows: DashMap::new(),
            tpm_windows: DashMap::new(),
        });

        // Spawn a background task that evicts stale DashMap entries once per
        // window. Uses a weak reference so the task exits automatically when
        // the last `Arc<MemoryRateLimiter>` is dropped — no explicit cleanup
        // or AbortHandle needed.
        let weak = Arc::downgrade(&rl);
        let interval = Duration::from_secs(window_secs.max(1));
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(interval).await;
                match weak.upgrade() {
                    Some(rl) => rl.cleanup_stale(),
                    None => break,
                }
            }
        });

        rl
    }

    /// Remove DashMap entries whose sliding window has fully expired.
    ///
    /// Called by the background task; also `pub(crate)` for testing.
    ///
    /// # Known limitation (TOCTOU)
    ///
    /// There is a narrow race between a caller acquiring the `Arc` from the
    /// DashMap (in `rpm_window` / `tpm_window`) and subsequently locking the
    /// inner `Mutex`. If cleanup runs in that gap, `try_lock` succeeds on the
    /// now-empty deque and removes the DashMap entry. The caller then pushes
    /// its timestamp into the orphaned `Arc`, which is not observed by future
    /// callers — effectively losing one rate-limit increment.
    ///
    /// The consequence is that a single request may go uncounted at the exact
    /// moment a window expires and cleanup fires simultaneously. This is
    /// acceptable for a soft rate limiter: the window resets naturally on the
    /// next access, and the worst case is allowing one extra request per
    /// cleanup cycle per key.
    ///
    /// A proper fix requires collapsing the two-level lock (DashMap shard +
    /// inner Mutex) into one by storing `VecDeque` directly in the DashMap and
    /// using the `entry()` API so get-or-insert and modify share the same shard
    /// lock. That change is deferred in favour of keeping this implementation
    /// simple.
    pub(crate) fn cleanup_stale(&self) {
        let cutoff = Self::now_ms() - self.window_ms;

        self.rpm_windows.retain(|_, window| {
            match window.try_lock() {
                Ok(mut deque) => {
                    while deque.front().map(|&t| t <= cutoff).unwrap_or(false) {
                        deque.pop_front();
                    }
                    !deque.is_empty()
                }
                Err(_) => true, // actively in use — skip this cycle
            }
        });

        self.tpm_windows.retain(|_, window| {
            match window.try_lock() {
                Ok(mut deque) => {
                    while deque.front().map(|&(t, _)| t <= cutoff).unwrap_or(false) {
                        deque.pop_front();
                    }
                    !deque.is_empty()
                }
                Err(_) => true, // actively in use — skip this cycle
            }
        });
    }

    fn now_ms() -> i64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64
    }

    fn rpm_key(key: &RateLimitKey) -> String {
        match key {
            RateLimitKey::KeyModel { api_key_id, model_id } =>
                format!("rl:key:{}:model:{}:rpm", api_key_id, model_id),
            RateLimitKey::Model { model_id } =>
                format!("rl:model:{}:rpm", model_id),
            RateLimitKey::PrimaryTier { model_id, protocol } =>
                format!("rl:model:{}:proto:{}:primary:rpm", model_id, protocol),
        }
    }

    fn tpm_key(key: &RateLimitKey) -> String {
        match key {
            RateLimitKey::KeyModel { api_key_id, model_id } =>
                format!("rl:key:{}:model:{}:tpm", api_key_id, model_id),
            RateLimitKey::Model { model_id } =>
                format!("rl:model:{}:tpm", model_id),
            RateLimitKey::PrimaryTier { model_id, protocol } =>
                format!("rl:model:{}:proto:{}:primary:tpm", model_id, protocol),
        }
    }

    /// Get the RPM window for `key`, creating it on first access.
    /// The DashMap shard lock is released before the inner Mutex is acquired.
    fn rpm_window(&self, key: &str) -> RpmWindow {
        Arc::clone(
            &*self.rpm_windows
                .entry(key.to_string())
                .or_insert_with(|| Arc::new(Mutex::new(VecDeque::new()))),
        )
    }

    /// Get the TPM window for `key`, creating it on first access.
    fn tpm_window(&self, key: &str) -> TpmWindow {
        Arc::clone(
            &*self.tpm_windows
                .entry(key.to_string())
                .or_insert_with(|| Arc::new(Mutex::new(VecDeque::new()))),
        )
    }
}

#[async_trait]
impl RateLimiter for MemoryRateLimiter {
    async fn check_rpm(&self, key: &RateLimitKey, limit: u32) -> RateLimitDecision {
        let window = self.rpm_window(&Self::rpm_key(key));
        let mut deque = window.lock().unwrap_or_else(|e| e.into_inner());

        let now = Self::now_ms();
        let cutoff = now - self.window_ms;

        // Evict timestamps that have fallen outside the window.
        while deque.front().map(|&t| t <= cutoff).unwrap_or(false) {
            deque.pop_front();
        }

        if deque.len() as u32 >= limit {
            return RateLimitDecision::Denied {
                retry_after_secs: (self.window_ms / 1000) as u64,
            };
        }

        deque.push_back(now);
        RateLimitDecision::Allow
    }

    async fn check_tpm(&self, key: &RateLimitKey, limit: u32) -> RateLimitDecision {
        let window = self.tpm_window(&Self::tpm_key(key));
        let mut deque = window.lock().unwrap_or_else(|e| e.into_inner());

        let now = Self::now_ms();
        let cutoff = now - self.window_ms;

        // Evict entries outside the window.
        while deque.front().map(|&(t, _)| t <= cutoff).unwrap_or(false) {
            deque.pop_front();
        }

        let total: u64 = deque.iter().map(|&(_, tokens)| tokens as u64).sum();

        if total >= limit as u64 {
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
        let window = self.tpm_window(&Self::tpm_key(key));
        let mut deque = window.lock().unwrap_or_else(|e| e.into_inner());
        deque.push_back((Self::now_ms(), tokens));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use modelpointer_core::rate_limit::{RateLimitDecision, RateLimitKey, RateLimiter};

    fn key_model() -> RateLimitKey {
        RateLimitKey::KeyModel {
            api_key_id: "key-abc".to_string(),
            model_id: "gpt-4o".to_string(),
        }
    }

    fn is_allowed(d: &RateLimitDecision) -> bool {
        matches!(d, RateLimitDecision::Allow)
    }

    fn is_denied(d: &RateLimitDecision) -> bool {
        matches!(d, RateLimitDecision::Denied { .. })
    }

    // ── RPM ───────────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn rpm_within_limit_allows() {
        let rl = MemoryRateLimiter::new(60);
        let key = key_model();
        for _ in 0..5 {
            assert!(is_allowed(&rl.check_rpm(&key, 5).await));
        }
    }

    #[tokio::test]
    async fn rpm_over_limit_denies() {
        let rl = MemoryRateLimiter::new(60);
        let key = key_model();
        for _ in 0..5 {
            rl.check_rpm(&key, 5).await;
        }
        assert!(is_denied(&rl.check_rpm(&key, 5).await));
    }

    #[tokio::test]
    async fn rpm_denied_includes_retry_after_secs() {
        let rl = MemoryRateLimiter::new(60);
        let key = key_model();
        for _ in 0..3 {
            rl.check_rpm(&key, 3).await;
        }
        match rl.check_rpm(&key, 3).await {
            RateLimitDecision::Denied { retry_after_secs } => assert_eq!(retry_after_secs, 60),
            _ => panic!("expected Denied"),
        }
    }

    #[tokio::test]
    async fn rpm_different_api_keys_are_independent() {
        let rl = MemoryRateLimiter::new(60);
        let key_a = key_model();
        let key_b = RateLimitKey::KeyModel {
            api_key_id: "key-xyz".to_string(),
            model_id: "gpt-4o".to_string(),
        };
        for _ in 0..3 {
            rl.check_rpm(&key_a, 3).await;
        }
        assert!(is_denied(&rl.check_rpm(&key_a, 3).await));
        assert!(is_allowed(&rl.check_rpm(&key_b, 3).await));
    }

    #[tokio::test]
    async fn rpm_key_model_and_model_buckets_are_independent() {
        let rl = MemoryRateLimiter::new(60);
        let key_model_key = key_model();
        let model_only_key = RateLimitKey::Model { model_id: "gpt-4o".to_string() };
        for _ in 0..3 {
            rl.check_rpm(&key_model_key, 3).await;
        }
        assert!(is_denied(&rl.check_rpm(&key_model_key, 3).await));
        assert!(is_allowed(&rl.check_rpm(&model_only_key, 3).await));
    }

    #[tokio::test]
    async fn rpm_window_resets_after_expiry() {
        let rl = MemoryRateLimiter::new(1); // 1-second window
        let key = key_model();
        for _ in 0..3 {
            rl.check_rpm(&key, 3).await;
        }
        assert!(is_denied(&rl.check_rpm(&key, 3).await));

        tokio::time::sleep(std::time::Duration::from_millis(1100)).await;

        assert!(is_allowed(&rl.check_rpm(&key, 3).await));
    }

    // ── TPM ───────────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn tpm_within_limit_allows() {
        let rl = MemoryRateLimiter::new(60);
        let key = key_model();
        rl.record_tokens(&key, 100).await;
        assert!(is_allowed(&rl.check_tpm(&key, 200).await));
    }

    #[tokio::test]
    async fn tpm_at_limit_denies() {
        let rl = MemoryRateLimiter::new(60);
        let key = key_model();
        rl.record_tokens(&key, 200).await;
        assert!(is_denied(&rl.check_tpm(&key, 200).await));
    }

    #[tokio::test]
    async fn tpm_tokens_accumulate_across_multiple_records() {
        let rl = MemoryRateLimiter::new(60);
        let key = key_model();
        rl.record_tokens(&key, 80).await;
        rl.record_tokens(&key, 80).await;
        // 160 tokens total, limit 150 → denied
        assert!(is_denied(&rl.check_tpm(&key, 150).await));
    }

    #[tokio::test]
    async fn tpm_zero_tokens_are_not_recorded() {
        let rl = MemoryRateLimiter::new(60);
        let key = key_model();
        rl.record_tokens(&key, 0).await;
        // No tokens in window, even limit=1 should allow
        assert!(is_allowed(&rl.check_tpm(&key, 1).await));
    }

    #[tokio::test]
    async fn tpm_check_is_read_only() {
        let rl = MemoryRateLimiter::new(60);
        let key = key_model();
        // check_tpm should never add tokens to the window
        for _ in 0..100 {
            rl.check_tpm(&key, 1).await;
        }
        assert!(is_allowed(&rl.check_tpm(&key, 1).await));
    }

    #[tokio::test]
    async fn tpm_window_resets_after_expiry() {
        let rl = MemoryRateLimiter::new(1); // 1-second window
        let key = key_model();
        rl.record_tokens(&key, 500).await;
        assert!(is_denied(&rl.check_tpm(&key, 499).await));

        tokio::time::sleep(std::time::Duration::from_millis(1100)).await;

        assert!(is_allowed(&rl.check_tpm(&key, 499).await));
    }

    // ── Cleanup ───────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn cleanup_removes_fully_expired_entries() {
        let rl = MemoryRateLimiter::new(1); // 1-second window
        let key = key_model();

        // Populate both maps.
        rl.check_rpm(&key, 10).await;
        rl.record_tokens(&key, 100).await;
        assert_eq!(rl.rpm_windows.len(), 1);
        assert_eq!(rl.tpm_windows.len(), 1);

        // Wait for the window to expire, then trigger cleanup manually.
        tokio::time::sleep(std::time::Duration::from_millis(1100)).await;
        rl.cleanup_stale();

        assert_eq!(rl.rpm_windows.len(), 0, "rpm entry should be removed");
        assert_eq!(rl.tpm_windows.len(), 0, "tpm entry should be removed");
    }

    #[tokio::test]
    async fn cleanup_retains_active_entries() {
        let rl = MemoryRateLimiter::new(60); // 60-second window
        let key = key_model();

        rl.check_rpm(&key, 10).await;
        rl.record_tokens(&key, 100).await;

        // Cleanup should keep entries that are still within the window.
        rl.cleanup_stale();

        assert_eq!(rl.rpm_windows.len(), 1, "active rpm entry must be retained");
        assert_eq!(rl.tpm_windows.len(), 1, "active tpm entry must be retained");
    }

    // ── PrimaryTier protocol isolation ────────────────────────────────────────

    #[tokio::test]
    async fn primary_tier_counters_are_independent_per_protocol() {
        let rl = MemoryRateLimiter::new(60);
        let openai_key = RateLimitKey::PrimaryTier {
            model_id: "my-model".to_string(),
            protocol: "openai".to_string(),
        };
        let anthropic_key = RateLimitKey::PrimaryTier {
            model_id: "my-model".to_string(),
            protocol: "anthropic".to_string(),
        };

        // Fill up the openai bucket (limit = 3).
        for _ in 0..3 {
            rl.check_rpm(&openai_key, 3).await;
        }
        assert!(is_denied(&rl.check_rpm(&openai_key, 3).await), "openai bucket should be full");

        // Anthropic bucket is independent — still allows requests.
        assert!(is_allowed(&rl.check_rpm(&anthropic_key, 3).await), "anthropic bucket must be independent");
    }

    #[tokio::test]
    async fn cleanup_removes_only_expired_keys() {
        let rl = MemoryRateLimiter::new(1); // 1-second window
        let active_key = RateLimitKey::KeyModel {
            api_key_id: "active".to_string(),
            model_id: "gpt-4o".to_string(),
        };
        let stale_key = RateLimitKey::KeyModel {
            api_key_id: "stale".to_string(),
            model_id: "gpt-4o".to_string(),
        };

        // Both keys get requests.
        rl.check_rpm(&stale_key, 10).await;
        rl.check_rpm(&active_key, 10).await;

        // Let the window expire, then refresh the active key only.
        tokio::time::sleep(std::time::Duration::from_millis(1100)).await;
        rl.check_rpm(&active_key, 10).await; // adds a fresh timestamp

        rl.cleanup_stale();

        // active_key has a fresh timestamp → retained; stale_key has none → removed.
        assert_eq!(rl.rpm_windows.len(), 1, "only active key should remain");
        assert!(
            rl.rpm_windows.contains_key("rl:key:active:model:gpt-4o:rpm"),
            "active key must be present"
        );
    }
}
