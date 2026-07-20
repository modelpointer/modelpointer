//! Upstream registry with per-(model, protocol) routing.
//!
//! Each `UpstreamGroup` serves a single protocol for a single model.
//! The registry key is `"{model_id}\x00{protocol}"` so that openai and
//! anthropic groups for the same model are stored and looked up independently.
//!
//! In addition to the model-keyed `groups` index, the registry maintains a
//! provider-keyed `providers` index for the /v1/responses retrieve / delete /
//! input_items endpoints. These operations are model-independent, so they use
//! a separate set of `UpstreamBinding` instances with their own circuit breakers,
//! preventing responses-side failures from affecting model routing health.

use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex},
};

use arc_swap::ArcSwap;

use crate::upstream::{
    Upstream,
    node::{ApiCompatibility, RuntimeType, UpstreamBinding, UpstreamGroup},
    routing::RoutingStrategyConfig,
};

/// Build the composite registry key from a model id and protocol string.
/// Uses a null byte as separator — neither model IDs nor protocol names contain one.
fn group_key(model_id: &str, protocol: &str) -> String {
    format!("{model_id}\x00{protocol}")
}

/// A consistent snapshot of both indexes.
///
/// Both maps are always updated together as a single `Arc`, so readers that
/// load the snapshot always see the `groups` and `providers` that were built
/// from the same set of upstream groups — never a mix of old and new.
#[derive(Debug)]
struct RegistrySnapshot {
    /// "{model_id}\x00{protocol}" / alias variant -> upstream group.
    groups: HashMap<String, Arc<UpstreamGroup>>,

    /// Canonical model IDs (no aliases, no protocol suffix) for `all_model_ids`.
    model_ids: HashSet<String>,

    /// Provider ID (credential name, e.g. "aliyun.beijing") -> upstream binding.
    ///
    /// Each entry is a **fresh** `UpstreamBinding` derived from the first upstream
    /// seen for that credential name. It shares the same `UpstreamNode` (connection
    /// config) but has its own circuit breaker, so retrieve / delete / input_items
    /// failures do not affect model routing health.
    providers: HashMap<String, Arc<UpstreamBinding>>,
}

impl RegistrySnapshot {
    fn empty() -> Arc<Self> {
        Arc::new(Self {
            groups: HashMap::new(),
            model_ids: HashSet::new(),
            providers: HashMap::new(),
        })
    }
}

/// Upstream registry with per-(model, protocol) indexing.
///
/// Both indexes are stored together in a single `ArcSwap<RegistrySnapshot>`.
/// Mutations build a new `RegistrySnapshot` and swap the pointer atomically,
/// so readers always see either the complete old state or the complete new
/// state — never a partial mix of old groups with new providers or vice versa.
#[derive(Debug)]
pub struct UpstreamRegistry {
    snapshot: ArcSwap<RegistrySnapshot>,
    /// Serialises all writers (`register` / `reload_all`).
    write_lock: Mutex<()>,
}

/// Infer the protocol string from a group's first binding.
/// Panics if the group has no bindings (which `UpstreamGroup::new` already prevents).
fn infer_protocol(group: &UpstreamGroup) -> String {
    group
        .upstreams
        .first()
        .map(|b| b.api_compatibility().to_string())
        .unwrap_or_else(|| "openai".to_string())
}

/// Build a provider map from a slice of upstream groups.
/// For each unique `credential.name` the first encountered `UpstreamNode` wins.
/// A fresh `UpstreamBinding` is created so the circuit breaker is independent.
fn build_provider_map(groups: &[Arc<UpstreamGroup>]) -> HashMap<String, Arc<UpstreamBinding>> {
    let mut map: HashMap<String, Arc<UpstreamBinding>> = HashMap::new();
    for group in groups {
        for binding in &group.upstreams {
            let pid = binding.provider_id().to_string();
            map.entry(pid).or_insert_with(|| {
                Arc::new(
                    UpstreamBinding::new(
                        binding.node.clone(),
                        true,
                        RoutingStrategyConfig::Swrr { weight: 1 },
                        0,
                    )
                    .expect("provider binding must be valid"),
                )
            });
        }
    }
    map
}

impl UpstreamRegistry {
    pub fn new() -> Self {
        Self {
            snapshot: ArcSwap::from(RegistrySnapshot::empty()),
            write_lock: Mutex::new(()),
        }
    }

    /// Register an upstream group.
    ///
    /// The group's protocol is inferred from its first binding. The group is
    /// stored under `"{model_id}\x00{protocol}"` (and alias variants).
    pub fn register(&self, group: UpstreamGroup) -> Result<(), String> {
        let protocol = infer_protocol(&group);
        let group = Arc::new(group);
        let _write_guard = self.write_lock.lock().unwrap_or_else(|e| e.into_inner());
        let current = self.snapshot.load();

        let mut new_groups = current.groups.clone();
        let mut new_model_ids = current.model_ids.clone();

        let primary_key = group_key(&group.model.id, &protocol);
        new_groups.insert(primary_key, group.clone());
        new_model_ids.insert(group.model.id.clone());
        for alias in &group.model.aliases {
            new_groups.insert(group_key(alias, &protocol), group.clone());
        }

        let mut new_providers = current.providers.clone();
        for binding in &group.upstreams {
            let pid = binding.provider_id().to_string();
            new_providers.entry(pid).or_insert_with(|| {
                Arc::new(
                    UpstreamBinding::new(
                        binding.node.clone(),
                        true,
                        RoutingStrategyConfig::Swrr { weight: 1 },
                        0,
                    )
                    .expect("provider binding must be valid"),
                )
            });
        }

        self.snapshot.store(Arc::new(RegistrySnapshot {
            groups: new_groups,
            model_ids: new_model_ids,
            providers: new_providers,
        }));

        Ok(())
    }

    /// Look up the group for a specific (model_id, protocol) pair.
    fn get_protocol_group(&self, model_id: &str, protocol: &str) -> Option<Arc<UpstreamGroup>> {
        self.snapshot
            .load()
            .groups
            .get(&group_key(model_id, protocol))
            .cloned()
    }

    /// Select an upstream for the given (model, protocol), respecting min_priority.
    pub fn select_with_min_priority(
        &self,
        model_id: &str,
        routing_key: Option<&str>,
        runtime_type: Option<&RuntimeType>,
        api_compatibility: Option<&ApiCompatibility>,
        min_priority: u8,
    ) -> Option<Arc<dyn Upstream>> {
        let protocol = api_compatibility
            .map(|ac| ac.to_string())
            .unwrap_or_else(|| "openai".to_string());
        self.get_protocol_group(model_id, &protocol)
            .and_then(|group| {
                group.select_with_min_priority(
                    routing_key,
                    runtime_type,
                    api_compatibility,
                    min_priority,
                )
            })
    }

    /// Find an upstream for the given model by exact provider_id match (credential name).
    /// Used exclusively by the /v1/responses POST endpoint.
    pub fn find_by_provider_id(
        &self,
        model_id: &str,
        provider_id: &str,
    ) -> Option<Arc<dyn Upstream>> {
        // Try openai group first, then anthropic.
        for protocol in ["openai", "anthropic"] {
            if let Some(group) = self.get_protocol_group(model_id, protocol) {
                if let Some(u) = group.upstreams.iter().find_map(|b| {
                    if b.is_available() && b.provider_id() == provider_id {
                        Some(Arc::clone(b) as Arc<dyn Upstream>)
                    } else {
                        None
                    }
                }) {
                    return Some(u);
                }
            }
        }
        None
    }

    /// Find an upstream for the /v1/responses retrieve / delete / input_items endpoints.
    pub fn find_for_responses(&self, provider_id: &str) -> Option<Arc<dyn Upstream>> {
        self.snapshot
            .load()
            .providers
            .get(provider_id)
            .filter(|b| b.is_available())
            .map(|b| Arc::clone(b) as Arc<dyn Upstream>)
    }

    /// Return the (key_rpm, key_tpm, model_rpm, model_tpm) configured for a model.
    /// Rate limits are model-level; any protocol group carries the same values.
    pub fn get_rate_limits(
        &self,
        model_id: &str,
    ) -> (Option<u32>, Option<u32>, Option<u32>, Option<u32>) {
        let snap = self.snapshot.load();
        for protocol in ["openai", "anthropic"] {
            if let Some(g) = snap.groups.get(&group_key(model_id, protocol)) {
                return (
                    g.key_rpm_limit,
                    g.key_tpm_limit,
                    g.model_rpm_limit,
                    g.model_tpm_limit,
                );
            }
        }
        (None, None, None, None)
    }

    /// Return the (primary_capacity_rpm, primary_capacity_tpm) for a specific protocol.
    pub fn get_primary_capacity(
        &self,
        model_id: &str,
        protocol: &str,
    ) -> (Option<u32>, Option<u32>) {
        self.get_protocol_group(model_id, protocol)
            .map(|g| (g.primary_capacity_rpm, g.primary_capacity_tpm))
            .unwrap_or((None, None))
    }

    /// Return all registered canonical model ids (no aliases, no protocol suffix) in sorted order.
    pub fn all_model_ids(&self) -> Vec<String> {
        let mut ids: Vec<String> = self.snapshot.load().model_ids.iter().cloned().collect();
        ids.sort();
        ids
    }

    /// Replace all registered upstream groups atomically.
    pub fn reload_all(&self, groups: Vec<UpstreamGroup>) {
        let _write_guard = self.write_lock.lock().unwrap_or_else(|e| e.into_inner());

        let mut new_groups: HashMap<String, Arc<UpstreamGroup>> = HashMap::new();
        let mut new_model_ids: HashSet<String> = HashSet::new();
        let mut arced: Vec<Arc<UpstreamGroup>> = Vec::with_capacity(groups.len());

        for group in groups {
            let protocol = infer_protocol(&group);
            let group = Arc::new(group);
            new_groups.insert(group_key(&group.model.id, &protocol), group.clone());
            new_model_ids.insert(group.model.id.clone());
            for alias in &group.model.aliases {
                new_groups.insert(group_key(alias, &protocol), group.clone());
            }
            arced.push(group);
        }

        self.snapshot.store(Arc::new(RegistrySnapshot {
            groups: new_groups,
            model_ids: new_model_ids,
            providers: build_provider_map(&arced),
        }));
    }
}

impl Default for UpstreamRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::AtomicBool;
    use std::thread;

    use super::*;
    use crate::model::ModelCard;
    use crate::upstream::node::{ProviderType, UpstreamCredential, UpstreamNode, UpstreamProfile};

    /// Creates an OpenAI-protocol group (uses `UpstreamNode::default()`, which is OpenAI).
    fn group(id: &str) -> UpstreamGroup {
        UpstreamGroup::new_swrr(ModelCard::new(id), vec![(UpstreamNode::default(), true, 1)])
            .expect("group must be valid")
    }

    fn openai_group(id: &str) -> UpstreamGroup {
        group(id)
    }

    fn anthropic_group(id: &str) -> UpstreamGroup {
        let node = UpstreamNode {
            profile: UpstreamProfile {
                base_url: "http://anthropic.example.com".to_string(),
                provider_node_id: String::new(),
                api_compatibility: ApiCompatibility::Anthropic,
                runtime_type: RuntimeType::External,
                credential: Arc::new(UpstreamCredential {
                    name: "anthropic-test".to_string(),
                    api_key: None,
                    provider_type: ProviderType::Unknown,
                }),
                upstream_model_name: None,
            },
            healthy: Arc::new(AtomicBool::new(true)),
        };
        UpstreamGroup::new_swrr(ModelCard::new(id), vec![(node, true, 1)])
            .expect("anthropic group must be valid")
    }

    #[test]
    fn concurrent_register_does_not_lose_updates() {
        const N: usize = 32;
        let registry = Arc::new(UpstreamRegistry::new());

        let handles: Vec<_> = (0..N)
            .map(|i| {
                let reg = Arc::clone(&registry);
                thread::spawn(move || {
                    reg.register(group(&format!("model-{i}"))).unwrap();
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }

        assert_eq!(registry.all_model_ids().len(), N);
    }

    #[test]
    fn concurrent_register_and_reload_all_stay_consistent() {
        let registry = Arc::new(UpstreamRegistry::new());

        let reg1 = Arc::clone(&registry);
        let t1 = thread::spawn(move || {
            for i in 0..200 {
                reg1.register(group(&format!("reg-{i}"))).unwrap();
            }
        });
        let reg2 = Arc::clone(&registry);
        let t2 = thread::spawn(move || {
            for _ in 0..200 {
                reg2.reload_all(vec![group("reload-only")]);
            }
        });
        t1.join().unwrap();
        t2.join().unwrap();

        let _ = registry.all_model_ids();
    }

    // ── Per-protocol isolation ────────────────────────────────────────────────

    #[test]
    fn openai_and_anthropic_groups_for_same_model_are_independent() {
        let registry = UpstreamRegistry::new();
        registry.register(openai_group("claude-3")).unwrap();
        registry.register(anthropic_group("claude-3")).unwrap();

        // Each protocol maps to its own distinct group.
        let og = registry
            .get_protocol_group("claude-3", "openai")
            .expect("openai group missing");
        let ag = registry
            .get_protocol_group("claude-3", "anthropic")
            .expect("anthropic group missing");

        // They are different Arc instances (distinct UpstreamGroups).
        assert!(
            !Arc::ptr_eq(&og, &ag),
            "openai and anthropic groups must be separate Arc instances"
        );

        // Protocol inferred from each group's binding matches the expected value.
        assert_eq!(infer_protocol(&og), "openai");
        assert_eq!(infer_protocol(&ag), "anthropic");
    }

    #[test]
    fn get_primary_capacity_is_per_protocol() {
        let registry = UpstreamRegistry::new();

        let og = openai_group("llama").with_primary_capacity(Some(100), Some(50_000));
        let ag = anthropic_group("llama").with_primary_capacity(Some(200), Some(80_000));
        registry.register(og).unwrap();
        registry.register(ag).unwrap();

        let (rpm_o, tpm_o) = registry.get_primary_capacity("llama", "openai");
        assert_eq!(rpm_o, Some(100));
        assert_eq!(tpm_o, Some(50_000));

        let (rpm_a, tpm_a) = registry.get_primary_capacity("llama", "anthropic");
        assert_eq!(rpm_a, Some(200));
        assert_eq!(tpm_a, Some(80_000));
    }

    #[test]
    fn all_model_ids_deduplicates_across_protocols() {
        let registry = UpstreamRegistry::new();
        registry.register(openai_group("dedup-model")).unwrap();
        registry.register(anthropic_group("dedup-model")).unwrap();

        let ids = registry.all_model_ids();
        assert_eq!(
            ids,
            vec!["dedup-model"],
            "model id should appear once even with two protocols"
        );
    }
}
