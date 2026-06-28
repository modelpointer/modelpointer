//! Routing strategy types and selection algorithms.
//!
//! Provides two strategies:
//! - **SWRR** (Smooth Weighted Round Robin) — nginx-style, stateful per-group.
//! - **WeightedHash** — consistent hashing with weighted scores; good for session affinity.

use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Mutex,
};

use serde::{Deserialize, Serialize};

use super::node::{Upstream, UpstreamBinding};

/// Routing algorithm used by an upstream group.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum RoutingStrategy {
    /// Smooth Weighted Round Robin (default).
    #[default]
    Swrr,
    /// Consistent weighted hash — routes the same key to the same upstream.
    WeightedHash,
}

/// Per-upstream routing parameters under a given strategy.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(tag = "strategy", rename_all = "snake_case")]
pub enum RoutingStrategyConfig {
    Swrr { weight: u8 },
    WeightedHash { weight: u8 },
}

impl RoutingStrategyConfig {
    pub fn strategy(&self) -> RoutingStrategy {
        match self {
            Self::Swrr { .. } => RoutingStrategy::Swrr,
            Self::WeightedHash { .. } => RoutingStrategy::WeightedHash,
        }
    }

    pub fn weight(&self) -> u8 {
        match self {
            Self::Swrr { weight } | Self::WeightedHash { weight } => *weight,
        }
    }

    pub fn validate(&self) -> Result<(), String> {
        if self.weight() == 0 {
            Err(format!("{:?} upstream weight must be greater than 0", self.strategy()))
        } else {
            Ok(())
        }
    }
}

/// Smooth Weighted Round Robin (SWRR) — nginx-style implementation.
///
/// Each step:
/// 1. Add each candidate's configured weight to its `current_weight`.
/// 2. Pick the candidate with the highest `current_weight` (ties broken by index).
/// 3. Subtract the total weight of all candidates from the winner.
///
/// The caller-supplied mutex ensures the three-step update is atomic across
/// concurrent callers, so no goroutine observes a partially-updated state.
pub fn select_swrr(
    candidates: &[&Arc<UpstreamBinding>],
    lock: &Mutex<()>,
) -> Option<Arc<dyn Upstream>> {
    if candidates.is_empty() {
        return None;
    }

    let _guard = lock.lock().unwrap();

    let total_weight: i64 = candidates
        .iter()
        .map(|c| c.strategy_config.weight() as i64)
        .sum();

    for c in candidates {
        c.current_weight
            .fetch_add(c.strategy_config.weight() as i64, Ordering::Relaxed);
    }

    let best = candidates
        .iter()
        .enumerate()
        .fold((0usize, i64::MIN), |(best_idx, best_cw), (i, c)| {
            let cw = c.current_weight.load(Ordering::Relaxed);
            if cw > best_cw { (i, cw) } else { (best_idx, best_cw) }
        })
        .0;

    candidates[best]
        .current_weight
        .fetch_sub(total_weight, Ordering::Relaxed);

    Some(candidates[best].clone() as Arc<dyn Upstream>)
}

/// Consistent weighted hash selection.
///
/// Each candidate receives a score derived from `blake3(routing_key:base_url)` scaled
/// by its weight. The candidate with the lowest score wins, producing stable routing
/// for the same key while respecting weight ratios across the fleet.
///
/// When `routing_key` is `None` a monotonically increasing fallback key is generated
/// from `model_id` and `cursor`, effectively degrading to weighted random selection.
pub fn select_weighted_hash(
    candidates: &[&Arc<UpstreamBinding>],
    routing_key: Option<&str>,
    model_id: &str,
    cursor: &AtomicUsize,
) -> Option<Arc<dyn Upstream>> {
    if candidates.is_empty() {
        return None;
    }

    let computed_key = routing_key.map(str::to_owned).unwrap_or_else(|| {
        let n = cursor.fetch_add(1, Ordering::Relaxed);
        format!("{}:{}", model_id, n)
    });

    candidates
        .iter()
        .map(|binding| {
            let score = weighted_hash_score(&computed_key, binding.base_url(), binding.strategy_config.weight());
            ((*binding).clone() as Arc<dyn Upstream>, score)
        })
        .min_by(|(_, a), (_, b)| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal))
        .map(|(upstream, _)| upstream)
}

fn weighted_hash_score(routing_key: &str, upstream_identity: &str, weight: u8) -> f64 {
    let hash = blake3::hash(format!("{}:{}", routing_key, upstream_identity).as_bytes());
    let mut bytes = [0_u8; 8];
    bytes.copy_from_slice(&hash.as_bytes()[..8]);
    let raw = u64::from_le_bytes(bytes);
    let uniform = (raw as f64 + 1.0) / (u64::MAX as f64 + 1.0);
    -uniform.ln() / f64::from(weight)
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicBool;

    use super::*;
    use crate::{
        model::ModelCard,
        upstream::{
            circuit_breaker::CircuitBreaker,
            node::{
                ApiCompatibility, RuntimeType, UpstreamCredential, UpstreamNode, UpstreamProfile,
                ProviderType,
            },
        },
    };

    fn make_binding(url: &str, weight: u8) -> Arc<UpstreamBinding> {
        Arc::new(UpstreamBinding {
            node: UpstreamNode {
                profile: UpstreamProfile {
                    base_url: url.to_string(),
                    api_compatibility: ApiCompatibility::OpenAi,
                    runtime_type: RuntimeType::External,
                    upstream_model_name: None,
                    credential: Arc::new(UpstreamCredential {
                        name: "test".to_string(),
                        api_key: None,
                        provider_type: ProviderType::Unknown,
                    }),
                },
                healthy: Arc::new(AtomicBool::new(true)),
            },
            enabled: true,
            priority: 0,
            strategy_config: RoutingStrategyConfig::Swrr { weight },
            circuit_breaker: CircuitBreaker::default(),
            current_weight: std::sync::atomic::AtomicI64::new(0),
        })
    }

    #[test]
    fn swrr_alternates_with_equal_weights() {
        let a = make_binding("http://a", 1);
        let b = make_binding("http://b", 1);
        let candidates = vec![&a, &b];
        let lock = Mutex::new(());

        let r1 = select_swrr(&candidates, &lock).unwrap();
        let r2 = select_swrr(&candidates, &lock).unwrap();
        let r3 = select_swrr(&candidates, &lock).unwrap();

        assert_eq!(r1.base_url(), "http://a");
        assert_eq!(r2.base_url(), "http://b");
        assert_eq!(r3.base_url(), "http://a");
    }

    #[test]
    fn swrr_respects_2_to_1_weight() {
        let a = make_binding("http://a", 2);
        let b = make_binding("http://b", 1);
        let candidates = vec![&a, &b];
        let lock = Mutex::new(());

        let counts = (0..6).fold((0u32, 0u32), |(ca, cb), _| {
            match select_swrr(&candidates, &lock).unwrap().base_url() {
                "http://a" => (ca + 1, cb),
                _ => (ca, cb + 1),
            }
        });

        assert_eq!(counts, (4, 2));
    }

    #[test]
    fn swrr_returns_none_for_empty_candidates() {
        let lock = Mutex::new(());
        assert!(select_swrr(&[], &lock).is_none());
    }

    #[test]
    fn weighted_hash_same_key_returns_same_upstream() {
        let a = make_binding("http://a", 1);
        let b = make_binding("http://b", 1);
        let candidates = vec![&a, &b];
        let cursor = AtomicUsize::new(0);

        let first = select_weighted_hash(&candidates, Some("user-123"), "m", &cursor)
            .unwrap()
            .base_url()
            .to_string();
        let second = select_weighted_hash(&candidates, Some("user-123"), "m", &cursor)
            .unwrap()
            .base_url()
            .to_string();

        assert_eq!(first, second);
    }

    #[test]
    fn weighted_hash_returns_none_for_empty_candidates() {
        let cursor = AtomicUsize::new(0);
        assert!(select_weighted_hash(&[], None, "m", &cursor).is_none());
    }

    #[test]
    fn routing_strategy_config_validation() {
        assert!(RoutingStrategyConfig::Swrr { weight: 0 }.validate().is_err());
        assert!(RoutingStrategyConfig::Swrr { weight: 1 }.validate().is_ok());
        assert!(RoutingStrategyConfig::WeightedHash { weight: 0 }.validate().is_err());
        assert!(RoutingStrategyConfig::WeightedHash { weight: 1 }.validate().is_ok());
    }

    #[test]
    fn routing_strategy_config_returns_correct_strategy() {
        assert_eq!(RoutingStrategyConfig::Swrr { weight: 1 }.strategy(), RoutingStrategy::Swrr);
        assert_eq!(RoutingStrategyConfig::WeightedHash { weight: 1 }.strategy(), RoutingStrategy::WeightedHash);
    }

    #[test]
    fn model_card_is_used_for_fallback_key() {
        // Two calls with no routing_key should NOT be pinned — cursor increments
        let a = make_binding("http://a", 1);
        let candidates = vec![&a];
        let cursor = AtomicUsize::new(0);
        let model = ModelCard::new("test-model");

        // Both calls should still succeed (single candidate always wins)
        assert!(select_weighted_hash(&candidates, None, &model.id, &cursor).is_some());
        assert!(select_weighted_hash(&candidates, None, &model.id, &cursor).is_some());
        assert_eq!(cursor.load(Ordering::Relaxed), 2);
    }
}
