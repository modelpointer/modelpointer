//! Integration tests for RedisRateLimiter.
//!
//! Requires a running Redis instance. The URL is read from the `REDIS_URL`
//! environment variable, defaulting to `redis://127.0.0.1:6379`.
//!
//! Tests are skipped automatically when Redis is unreachable.
//!
//! Run:
//!   cargo test --test redis_rate_limit

use modelpointer::rate_limit_redis::RedisRateLimiter;
use modelpointer_core::rate_limit::{RateLimitDecision, RateLimitKey, RateLimiter};
use uuid::Uuid;

fn redis_url() -> String {
    std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".to_string())
}

/// Build a limiter, returning None if Redis is unreachable (skips the test).
async fn make_limiter(window_secs: u64) -> Option<std::sync::Arc<RedisRateLimiter>> {
    RedisRateLimiter::new(&redis_url(), window_secs).await.ok()
}

/// Generate a unique KeyModel key so tests don't interfere with each other.
fn unique_key() -> RateLimitKey {
    RateLimitKey::KeyModel {
        api_key_id: Uuid::new_v4().to_string(),
        model_id: "test-model".to_string(),
    }
}

fn is_allowed(d: &RateLimitDecision) -> bool {
    matches!(d, RateLimitDecision::Allow)
}

fn is_denied(d: &RateLimitDecision) -> bool {
    matches!(d, RateLimitDecision::Denied { .. })
}

// ── RPM ───────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn rpm_within_limit_allows() {
    let Some(rl) = make_limiter(60).await else { return };
    let key = unique_key();
    for _ in 0..5 {
        assert!(is_allowed(&rl.check_rpm(&key, 5).await));
    }
}

#[tokio::test]
async fn rpm_over_limit_denies() {
    let Some(rl) = make_limiter(60).await else { return };
    let key = unique_key();
    for _ in 0..5 {
        rl.check_rpm(&key, 5).await;
    }
    assert!(is_denied(&rl.check_rpm(&key, 5).await));
}

#[tokio::test]
async fn rpm_denied_includes_retry_after_secs() {
    let Some(rl) = make_limiter(60).await else { return };
    let key = unique_key();
    for _ in 0..3 {
        rl.check_rpm(&key, 3).await;
    }
    match rl.check_rpm(&key, 3).await {
        RateLimitDecision::Denied { retry_after_secs } => assert_eq!(retry_after_secs, 60),
        _ => panic!("expected Denied"),
    }
}

#[tokio::test]
async fn rpm_different_keys_are_independent() {
    let Some(rl) = make_limiter(60).await else { return };
    let key_a = unique_key();
    let key_b = unique_key();
    for _ in 0..3 {
        rl.check_rpm(&key_a, 3).await;
    }
    assert!(is_denied(&rl.check_rpm(&key_a, 3).await));
    assert!(is_allowed(&rl.check_rpm(&key_b, 3).await));
}

/// Two separate limiter instances share state via Redis — the core advantage
/// over the in-memory implementation.
#[tokio::test]
async fn rpm_two_instances_share_state() {
    let Some(rl_a) = make_limiter(60).await else { return };
    let Some(rl_b) = make_limiter(60).await else { return };
    let key = unique_key();

    // Instance A consumes 3 slots
    for _ in 0..3 {
        rl_a.check_rpm(&key, 3).await;
    }
    // Instance B should see the limit as exhausted
    assert!(is_denied(&rl_b.check_rpm(&key, 3).await));
}

#[tokio::test]
async fn rpm_window_resets_after_expiry() {
    let Some(rl) = make_limiter(1).await else { return }; // 1-second window
    let key = unique_key();
    for _ in 0..3 {
        rl.check_rpm(&key, 3).await;
    }
    assert!(is_denied(&rl.check_rpm(&key, 3).await));

    tokio::time::sleep(std::time::Duration::from_millis(1100)).await;

    assert!(is_allowed(&rl.check_rpm(&key, 3).await));
}

// ── TPM ───────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn tpm_within_limit_allows() {
    let Some(rl) = make_limiter(60).await else { return };
    let key = unique_key();
    rl.record_tokens(&key, 100).await;
    assert!(is_allowed(&rl.check_tpm(&key, 200).await));
}

#[tokio::test]
async fn tpm_at_limit_denies() {
    let Some(rl) = make_limiter(60).await else { return };
    let key = unique_key();
    rl.record_tokens(&key, 200).await;
    assert!(is_denied(&rl.check_tpm(&key, 200).await));
}

#[tokio::test]
async fn tpm_tokens_accumulate_across_records() {
    let Some(rl) = make_limiter(60).await else { return };
    let key = unique_key();
    rl.record_tokens(&key, 80).await;
    rl.record_tokens(&key, 80).await;
    // 160 total, limit 150 → denied
    assert!(is_denied(&rl.check_tpm(&key, 150).await));
}

/// Two instances writing tokens to the same key should see a combined total.
#[tokio::test]
async fn tpm_two_instances_share_state() {
    let Some(rl_a) = make_limiter(60).await else { return };
    let Some(rl_b) = make_limiter(60).await else { return };
    let key = unique_key();

    rl_a.record_tokens(&key, 100).await;
    rl_b.record_tokens(&key, 100).await;

    // Instance A sees the combined 200 tokens
    assert!(is_denied(&rl_a.check_tpm(&key, 150).await));
    // Instance B sees the same
    assert!(is_denied(&rl_b.check_tpm(&key, 150).await));
}

#[tokio::test]
async fn tpm_zero_tokens_not_recorded() {
    let Some(rl) = make_limiter(60).await else { return };
    let key = unique_key();
    rl.record_tokens(&key, 0).await;
    assert!(is_allowed(&rl.check_tpm(&key, 1).await));
}

#[tokio::test]
async fn tpm_window_resets_after_expiry() {
    let Some(rl) = make_limiter(1).await else { return }; // 1-second window
    let key = unique_key();
    rl.record_tokens(&key, 500).await;
    assert!(is_denied(&rl.check_tpm(&key, 499).await));

    tokio::time::sleep(std::time::Duration::from_millis(1100)).await;

    assert!(is_allowed(&rl.check_tpm(&key, 499).await));
}
