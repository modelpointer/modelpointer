use std::{
    fmt,
    sync::{
        atomic::{AtomicBool, AtomicI64, AtomicUsize, Ordering},
        Arc, Mutex,
    },
};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::model::ModelCard;
use super::{
    CircuitBreaker,
    routing::{self, RoutingStrategy, RoutingStrategyConfig},
};

/// Core abstraction for a backend upstream service.
#[async_trait]
pub trait Upstream: Send + Sync + fmt::Debug {
    /// Get the upstream's base URL
    fn base_url(&self) -> &str;
    /// Get the upstream's API key
    fn api_key(&self) -> &Option<String>;

    /// Check if the upstream is currently healthy
    fn is_healthy(&self) -> bool;

    /// Set the upstream's health status
    fn set_healthy(&self, healthy: bool);

    /// Get the circuit breaker for this upstream
    fn circuit_breaker(&self) -> &CircuitBreaker;

    /// Check if the upstream is available (healthy + circuit closed/half-open)
    fn is_available(&self) -> bool;

    /// Record the outcome of a request to this upstream
    fn record_outcome(&self, success: bool) {
        self.circuit_breaker().record_outcome(success);
    }

    /// Get runtime implementation type
    fn runtime_type(&self) -> &RuntimeType;

    /// Get the provider type for this upstream
    fn provider_type(&self) -> &ProviderType;

    /// Get the API compatibility mode this upstream expects
    fn api_compatibility(&self) -> &ApiCompatibility;

    /// Get the provider id for this upstream (credential name, e.g. "aliyun.beijing")
    fn provider_id(&self) -> &str;

    /// Get the model name to use when forwarding to this upstream.
    /// Returns `None` if the gateway model name should be forwarded unchanged.
    fn upstream_model_name(&self) -> Option<&str>;

    /// Routing priority tier this upstream belongs to.
    /// 0 = primary, 1 = fallback. Used to decide whether to record primary-tier
    /// TPM after a response, based on where the request *actually* landed.
    fn priority(&self) -> u8;
}

/// Inference provider type.
/// Identifies the vendor to determine request/response format for provider-specific APIs.
/// Represented as a lowercase string for config and serialization.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum ProviderType {
    /// Alibaba Cloud (Aliyun / DashScope)
    Aliyun,
    /// Volcengine (Doubao / Ark)
    Volcengine,
    /// OpenAI
    OpenAi,
    /// Anthropic
    Anthropic,
    /// Baidu Cloud (Qianfan)
    Baidu,
    /// Cohere
    Cohere,
    /// Unknown / self-hosted
    #[default]
    Unknown,
}

impl fmt::Display for ProviderType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ProviderType::Aliyun => write!(f, "aliyun"),
            ProviderType::Volcengine => write!(f, "volcengine"),
            ProviderType::OpenAi => write!(f, "openai"),
            ProviderType::Anthropic => write!(f, "anthropic"),
            ProviderType::Baidu => write!(f, "baidu"),
            ProviderType::Cohere => write!(f, "cohere"),
            ProviderType::Unknown => write!(f, "unknown"),
        }
    }
}

impl std::str::FromStr for ProviderType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "aliyun" => Ok(ProviderType::Aliyun),
            "volcengine" => Ok(ProviderType::Volcengine),
            "openai" => Ok(ProviderType::OpenAi),
            "anthropic" => Ok(ProviderType::Anthropic),
            "baidu" => Ok(ProviderType::Baidu),
            "cohere" => Ok(ProviderType::Cohere),
            "unknown" => Ok(ProviderType::Unknown),
            other => Err(format!("Unknown provider type: {}", other)),
        }
    }
}

/// Runtime implementation type for workers
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum RuntimeType {
    /// SGLang runtime (default)
    #[default]
    Sglang,
    /// vLLM runtime
    Vllm,
    /// External OpenAI-compatible API (not local inference)
    /// Used for routing to external providers like OpenAI, Azure OpenAI, xAI, etc.
    External,
}

impl fmt::Display for RuntimeType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RuntimeType::Sglang => write!(f, "sglang"),
            RuntimeType::Vllm => write!(f, "vllm"),
            RuntimeType::External => write!(f, "external"),
        }
    }
}

impl std::str::FromStr for RuntimeType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        // Use eq_ignore_ascii_case to avoid to_lowercase() allocation
        if s.eq_ignore_ascii_case("sglang") {
            Ok(RuntimeType::Sglang)
        } else if s.eq_ignore_ascii_case("vllm") {
            Ok(RuntimeType::Vllm)
        } else if s.eq_ignore_ascii_case("external") {
            Ok(RuntimeType::External)
        } else {
            Err(format!("Unknown runtime type: {}", s))
        }
    }
}

/// API surface compatibility expected by an upstream worker.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum ApiCompatibility {
    /// OpenAI-compatible HTTP API shape and headers
    #[default]
    OpenAi,
    /// Anthropic-compatible HTTP API shape and headers
    Anthropic,
    /// Provider-specific (native) API — not OpenAI or Anthropic compatible.
    /// Used for vendor-specific capabilities such as rerank, text-to-speech,
    /// image generation, and video generation.
    Native,
}

impl fmt::Display for ApiCompatibility {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ApiCompatibility::OpenAi => write!(f, "openai"),
            ApiCompatibility::Anthropic => write!(f, "anthropic"),
            ApiCompatibility::Native => write!(f, "native"),
        }
    }
}

impl std::str::FromStr for ApiCompatibility {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.eq_ignore_ascii_case("openai") {
            Ok(ApiCompatibility::OpenAi)
        } else if s.eq_ignore_ascii_case("anthropic") {
            Ok(ApiCompatibility::Anthropic)
        } else if s.eq_ignore_ascii_case("native") {
            Ok(ApiCompatibility::Native)
        } else {
            Err(format!("Unknown API compatibility: {}", s))
        }
    }
}

/// Upstream binding within an upstream group.
///
/// Carries the routing attributes (weight, circuit breaker) specific to one upstream
/// under a model's routing strategy.
///
/// The `priority` field implements tiered routing:
/// - `0` = primary tier (preferred; used first)
/// - `1` = fallback tier (used when all primary upstreams are unavailable)
/// Higher numbers are tried in ascending order; values should be contiguous.
#[derive(Debug)]
pub struct UpstreamBinding {
    pub node: UpstreamNode,
    pub enabled: bool,
    /// Routing priority tier. 0 = primary, 1 = fallback.
    pub priority: u8,
    pub strategy_config: RoutingStrategyConfig,
    pub circuit_breaker: CircuitBreaker,
    /// Running weight used by Smooth Weighted Round Robin. Starts at 0.
    pub current_weight: AtomicI64,
}

impl Clone for UpstreamBinding {
    fn clone(&self) -> Self {
        Self {
            node: self.node.clone(),
            enabled: self.enabled,
            priority: self.priority,
            strategy_config: self.strategy_config.clone(),
            circuit_breaker: self.circuit_breaker.clone(),
            current_weight: AtomicI64::new(0),
        }
    }
}

impl UpstreamBinding {
    pub fn new(
        node: UpstreamNode,
        enabled: bool,
        strategy_config: RoutingStrategyConfig,
        priority: u8,
    ) -> Result<Self, String> {
        strategy_config.validate()?;
        Ok(Self {
            node,
            enabled,
            priority,
            strategy_config,
            circuit_breaker: CircuitBreaker::default(),
            current_weight: AtomicI64::new(0),
        })
    }
}

#[async_trait]
impl Upstream for UpstreamBinding {
    fn base_url(&self) -> &str {
        &self.node.profile.base_url
    }

    fn api_key(&self) -> &Option<String> {
        &self.node.profile.credential.api_key
    }

    fn is_healthy(&self) -> bool {
        self.node.healthy.load(Ordering::Acquire)
    }

    fn set_healthy(&self, healthy: bool) {
        self.node.healthy.store(healthy, Ordering::Release);
    }

    fn circuit_breaker(&self) -> &CircuitBreaker {
        &self.circuit_breaker
    }

    fn runtime_type(&self) -> &RuntimeType {
        &self.node.profile.runtime_type
    }

    fn provider_type(&self) -> &ProviderType {
        &self.node.profile.credential.provider_type
    }

    fn api_compatibility(&self) -> &ApiCompatibility {
        &self.node.profile.api_compatibility
    }

    fn provider_id(&self) -> &str {
        &self.node.profile.credential.name
    }

    fn upstream_model_name(&self) -> Option<&str> {
        self.node.profile.upstream_model_name.as_deref()
    }

    fn is_available(&self) -> bool {
        self.enabled && self.is_healthy() && self.circuit_breaker().can_execute()
    }

    fn priority(&self) -> u8 {
        self.priority
    }
}

/// Model-scoped upstream group.
///
/// Owns the routing strategy and the set of candidate upstreams for a specific model.
/// This is the primary unit managed by the registry.
#[derive(Debug)]
pub struct UpstreamGroup {
    pub model: ModelCard,
    pub strategy: RoutingStrategy,
    pub upstreams: Vec<Arc<UpstreamBinding>>,
    /// Maximum requests per minute per (api_key, model) pair. None = unlimited.
    pub key_rpm_limit: Option<u32>,
    /// Maximum tokens per minute per (api_key, model) pair. None = unlimited.
    pub key_tpm_limit: Option<u32>,
    /// Maximum requests per minute across all API keys for this model. None = unlimited.
    pub model_rpm_limit: Option<u32>,
    /// Maximum tokens per minute across all API keys for this model. None = unlimited.
    pub model_tpm_limit: Option<u32>,
    /// Primary-tier capacity: max requests/min before spilling to the fallback tier.
    /// Exceeding this routes the request to fallback silently (no 429 to client).
    pub primary_capacity_rpm: Option<u32>,
    /// Primary-tier capacity: max tokens/min before spilling to the fallback tier.
    pub primary_capacity_tpm: Option<u32>,
    /// Serialises SWRR selection steps so current_weight updates are atomic as a group.
    swrr_lock: Mutex<()>,
    /// Monotonic counter used by WeightedHash to generate a fallback routing key.
    hash_cursor: AtomicUsize,
}

impl UpstreamGroup {
    pub fn new(
        model: ModelCard,
        strategy: RoutingStrategy,
        upstreams: Vec<UpstreamBinding>,
    ) -> Result<Self, String> {
        if upstreams.is_empty() {
            return Err(format!(
                "upstream group '{}' must contain at least one upstream",
                model.id
            ));
        }

        for binding in &upstreams {
            if binding.strategy_config.strategy() != strategy {
                return Err(format!(
                    "upstream '{}' uses {:?} but model '{}' is configured with {:?}",
                    binding.base_url(),
                    binding.strategy_config.strategy(),
                    model.id,
                    strategy
                ));
            }
        }

        Ok(Self {
            model,
            strategy,
            upstreams: upstreams.into_iter().map(Arc::new).collect(),
            key_rpm_limit: None,
            key_tpm_limit: None,
            model_rpm_limit: None,
            model_tpm_limit: None,
            primary_capacity_rpm: None,
            primary_capacity_tpm: None,
            swrr_lock: Mutex::new(()),
            hash_cursor: AtomicUsize::new(0),
        })
    }

    /// Set client-facing rate limits on an existing group (builder style).
    pub fn with_rate_limits(
        mut self,
        key_rpm: Option<u32>,
        key_tpm: Option<u32>,
        model_rpm: Option<u32>,
        model_tpm: Option<u32>,
    ) -> Self {
        self.key_rpm_limit = key_rpm;
        self.key_tpm_limit = key_tpm;
        self.model_rpm_limit = model_rpm;
        self.model_tpm_limit = model_tpm;
        self
    }

    /// Set primary-tier capacity limits (builder style).
    /// When the primary tier exceeds these limits, requests spill to the fallback
    /// tier transparently — no 429 is returned to the client.
    pub fn with_primary_capacity(mut self, rpm: Option<u32>, tpm: Option<u32>) -> Self {
        self.primary_capacity_rpm = rpm;
        self.primary_capacity_tpm = tpm;
        self
    }

    pub fn new_swrr(
        model: ModelCard,
        upstreams: Vec<(UpstreamNode, bool, u8)>,
    ) -> Result<Self, String> {
        let bindings = upstreams
            .into_iter()
            .map(|(node, enabled, weight)| {
                UpstreamBinding::new(node, enabled, RoutingStrategyConfig::Swrr { weight }, 0)
            })
            .collect::<Result<Vec<_>, _>>()?;

        Self::new(model, RoutingStrategy::Swrr, bindings)
    }

    pub fn new_weighted_hash(
        model: ModelCard,
        upstreams: Vec<(UpstreamNode, bool, u8)>,
    ) -> Result<Self, String> {
        let bindings = upstreams
            .into_iter()
            .map(|(node, enabled, weight)| {
                UpstreamBinding::new(
                    node,
                    enabled,
                    RoutingStrategyConfig::WeightedHash { weight },
                    0,
                )
            })
            .collect::<Result<Vec<_>, _>>()?;

        Self::new(model, RoutingStrategy::WeightedHash, bindings)
    }

    pub fn matches(&self, model_id: &str) -> bool {
        self.model.matches(model_id)
    }

    /// Select an upstream, preferring the lowest-numbered priority tier.
    /// Falls through to the next tier only when all upstreams in the current
    /// tier are unavailable (circuit open, unhealthy, or disabled).
    pub fn select(
        &self,
        routing_key: Option<&str>,
        runtime_type: Option<&RuntimeType>,
        api_compatibility: Option<&ApiCompatibility>,
    ) -> Option<Arc<dyn Upstream>> {
        self.select_with_min_priority(routing_key, runtime_type, api_compatibility, 0)
    }

    /// Same as [`select`] but skips all tiers below `min_priority`.
    /// Used to force selection from the fallback tier (e.g. after explicit spillover).
    pub fn select_with_min_priority(
        &self,
        routing_key: Option<&str>,
        runtime_type: Option<&RuntimeType>,
        api_compatibility: Option<&ApiCompatibility>,
        min_priority: u8,
    ) -> Option<Arc<dyn Upstream>> {
        // Collect distinct priority tiers >= min_priority, in ascending order.
        let mut tiers: Vec<u8> = self.upstreams.iter()
            .map(|b| b.priority)
            .filter(|&p| p >= min_priority)
            .collect();
        tiers.sort();
        tiers.dedup();

        for tier in tiers {
            let candidates: Vec<&Arc<UpstreamBinding>> = self.upstreams.iter()
                .filter(|b| {
                    b.priority == tier
                        && b.is_available()
                        && runtime_type.map(|rt| b.runtime_type() == rt).unwrap_or(true)
                        && api_compatibility.map(|ac| b.api_compatibility() == ac).unwrap_or(true)
                })
                .collect();

            if candidates.is_empty() {
                continue; // All upstreams in this tier unavailable; try next tier.
            }

            return match self.strategy {
                RoutingStrategy::Swrr => routing::select_swrr(&candidates, &self.swrr_lock),
                RoutingStrategy::WeightedHash => {
                    routing::select_weighted_hash(&candidates, routing_key, &self.model.id, &self.hash_cursor)
                }
            };
        }
        None
    }
}

/// Bundles an API key with its associated provider and a human-readable name.
///
/// A single `UpstreamCredential` may be shared by multiple `UpstreamProfile`s — for example,
/// several backend endpoints that all consume the same provider credential.
#[derive(Debug, Clone)]
pub struct UpstreamCredential {
    /// Human-readable label for this credential (e.g. "aliyun-prod", "volcengine-staging").
    pub name: String,
    /// Actual API key value sent to the upstream.
    pub api_key: Option<String>,
    /// Inference provider type.
    pub provider_type: ProviderType,
}

/// Metadata associated with an upstream node.
#[derive(Debug, Clone)]
pub struct UpstreamProfile {
    /// Upstream base URL. Endpoint paths are appended by the router.
    pub base_url: String,
    /// Which HTTP API contract this upstream expects.
    pub api_compatibility: ApiCompatibility,
    /// Runtime type of the inference backend.
    pub runtime_type: RuntimeType,
    /// Credential shared across upstreams using the same vendor key.
    pub credential: Arc<UpstreamCredential>,
    /// Model name to send to this upstream, if different from the gateway model name.
    /// `None` means forward the client's model name unchanged.
    pub upstream_model_name: Option<String>,
}

/// A concrete upstream node.
#[derive(Clone)]
pub struct UpstreamNode {
    pub profile: UpstreamProfile,
    pub healthy: Arc<AtomicBool>,
}

impl fmt::Debug for UpstreamNode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("UpstreamNode")
            .field("profile", &self.profile)
            .field("healthy", &self.healthy.load(Ordering::Relaxed))
            .finish()
    }
}

impl Default for UpstreamProfile {
    fn default() -> Self {
        Self {
            base_url: "http://localhost:8000/v1".to_string(),
            api_compatibility: ApiCompatibility::OpenAi,
            runtime_type: RuntimeType::External,
            credential: Arc::new(UpstreamCredential {
                name: "default".to_string(),
                api_key: None,
                provider_type: ProviderType::Unknown,
            }),
            upstream_model_name: None,
        }
    }
}

impl Default for UpstreamNode {
    fn default() -> Self {
        Self {
            profile: UpstreamProfile::default(),
            healthy: Arc::new(AtomicBool::new(true)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn build_node(url: &str) -> UpstreamNode {
        UpstreamNode {
            profile: UpstreamProfile {
                base_url: url.to_string(),
                api_compatibility: ApiCompatibility::OpenAi,
                runtime_type: RuntimeType::External,
                upstream_model_name: None,
                credential: Arc::new(UpstreamCredential {
                    name: "test".to_string(),
                    api_key: None,
                    provider_type: ProviderType::Aliyun,
                }),
            },
            healthy: Arc::new(AtomicBool::new(true)),
        }
    }

    #[test]
    fn binding_rejects_zero_weight() {
        assert!(UpstreamBinding::new(
            build_node("http://a"),
            true,
            RoutingStrategyConfig::Swrr { weight: 0 },
            0,
        ).is_err());

        assert!(UpstreamBinding::new(
            build_node("http://a"),
            true,
            RoutingStrategyConfig::WeightedHash { weight: 0 },
            0,
        ).is_err());
    }

    #[test]
    fn new_swrr_builds_correct_strategy() {
        let group = UpstreamGroup::new_swrr(
            ModelCard::new("test-model"),
            vec![
                (build_node("http://upstream-a"), true, 1),
                (build_node("http://upstream-b"), true, 1),
            ],
        )
        .unwrap();

        assert_eq!(group.strategy, RoutingStrategy::Swrr);
        assert!(group
            .upstreams
            .iter()
            .all(|b| matches!(b.strategy_config, RoutingStrategyConfig::Swrr { .. })));
    }

    #[test]
    fn group_rejects_empty_upstreams() {
        assert!(UpstreamGroup::new(
            ModelCard::new("test-model"),
            RoutingStrategy::Swrr,
            vec![],
        ).is_err());
    }

    #[test]
    fn group_rejects_mismatched_strategy() {
        let result = UpstreamGroup::new(
            ModelCard::new("test-model"),
            RoutingStrategy::Swrr,
            vec![
                UpstreamBinding::new(
                    build_node("http://a"),
                    true,
                    RoutingStrategyConfig::WeightedHash { weight: 1 },
                    0,
                ).unwrap(),
            ],
        );
        assert!(result.is_err());
    }

    // ── Tiered-routing helpers ────────────────────────────────────────────────

    /// Build a binding with an explicit health flag and priority tier.
    fn build_binding(url: &str, healthy: bool, priority: u8) -> UpstreamBinding {
        let node = UpstreamNode {
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
            healthy: Arc::new(AtomicBool::new(healthy)),
        };
        UpstreamBinding::new(node, true, RoutingStrategyConfig::Swrr { weight: 1 }, priority).unwrap()
    }

    fn build_group(bindings: Vec<UpstreamBinding>) -> UpstreamGroup {
        UpstreamGroup::new(ModelCard::new("m"), RoutingStrategy::Swrr, bindings).unwrap()
    }

    // ── select() fallback tests ───────────────────────────────────────────────

    #[test]
    fn select_prefers_primary_over_fallback_when_both_healthy() {
        let group = build_group(vec![
            build_binding("http://primary", true, 0),
            build_binding("http://fallback", true, 1),
        ]);
        // With a single primary candidate SWRR always picks it.
        let url = group.select(None, None, None).unwrap();
        assert_eq!(url.base_url(), "http://primary");
    }

    #[test]
    fn select_falls_through_to_fallback_when_primary_unhealthy() {
        let group = build_group(vec![
            build_binding("http://primary", false, 0), // unhealthy
            build_binding("http://fallback", true, 1),
        ]);
        let url = group.select(None, None, None).unwrap();
        assert_eq!(url.base_url(), "http://fallback");
    }

    #[test]
    fn select_falls_through_to_fallback_when_primary_disabled() {
        let mut primary = build_binding("http://primary", true, 0);
        primary.enabled = false;
        let group = build_group(vec![
            primary,
            build_binding("http://fallback", true, 1),
        ]);
        let url = group.select(None, None, None).unwrap();
        assert_eq!(url.base_url(), "http://fallback");
    }

    #[test]
    fn select_falls_through_to_fallback_when_primary_circuit_open() {
        let group = build_group(vec![
            build_binding("http://primary", true, 0),
            build_binding("http://fallback", true, 1),
        ]);
        group.upstreams[0].circuit_breaker().force_open();
        let url = group.select(None, None, None).unwrap();
        assert_eq!(url.base_url(), "http://fallback");
    }

    #[test]
    fn select_returns_none_when_all_tiers_unavailable() {
        let group = build_group(vec![
            build_binding("http://primary", false, 0),
            build_binding("http://fallback", false, 1),
        ]);
        assert!(group.select(None, None, None).is_none());
    }

    #[test]
    fn select_returns_none_when_no_upstreams_match_tier() {
        // Only a primary (tier 0); no fallback configured.
        let primary = build_binding("http://primary", false, 0);
        let group = build_group(vec![primary]);
        assert!(group.select(None, None, None).is_none());
    }

    // ── select_with_min_priority() tests ─────────────────────────────────────

    #[test]
    fn select_with_min_priority_1_skips_healthy_primary() {
        let group = build_group(vec![
            build_binding("http://primary", true, 0),
            build_binding("http://fallback", true, 1),
        ]);
        // min_priority=1 must skip tier 0 even though it is healthy.
        let url = group.select_with_min_priority(None, None, None, 1).unwrap();
        assert_eq!(url.base_url(), "http://fallback");
    }

    #[test]
    fn select_with_min_priority_higher_than_all_tiers_returns_none() {
        let group = build_group(vec![
            build_binding("http://primary", true, 0),
            build_binding("http://fallback", true, 1),
        ]);
        assert!(group.select_with_min_priority(None, None, None, 2).is_none());
    }

    #[test]
    fn select_with_min_priority_0_behaves_like_select() {
        let group = build_group(vec![
            build_binding("http://primary", true, 0),
            build_binding("http://fallback", true, 1),
        ]);
        // min_priority=0 is the same as select()
        let url = group.select_with_min_priority(None, None, None, 0).unwrap();
        assert_eq!(url.base_url(), "http://primary");
    }

    #[test]
    fn select_healthy_flag_can_be_flipped_at_runtime() {
        // Prove that the health flag is checked live via Arc<AtomicBool>.
        let healthy = Arc::new(AtomicBool::new(true));
        let node = UpstreamNode {
            profile: UpstreamProfile {
                base_url: "http://primary".to_string(),
                api_compatibility: ApiCompatibility::OpenAi,
                runtime_type: RuntimeType::External,
                upstream_model_name: None,
                credential: Arc::new(UpstreamCredential {
                    name: "test".to_string(),
                    api_key: None,
                    provider_type: ProviderType::Unknown,
                }),
            },
            healthy: healthy.clone(),
        };
        let primary = UpstreamBinding::new(node, true, RoutingStrategyConfig::Swrr { weight: 1 }, 0).unwrap();
        let group = build_group(vec![
            primary,
            build_binding("http://fallback", true, 1),
        ]);

        // Initially primary is selected.
        assert_eq!(group.select(None, None, None).unwrap().base_url(), "http://primary");

        // Flip the flag → fallback takes over without rebuilding the group.
        healthy.store(false, Ordering::Release);
        assert_eq!(group.select(None, None, None).unwrap().base_url(), "http://fallback");

        // Restore → primary selected again.
        healthy.store(true, Ordering::Release);
        assert_eq!(group.select(None, None, None).unwrap().base_url(), "http://primary");
    }
}