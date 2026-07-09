//! File-based gateway route configuration.
//!
//! Loads upstreams and routes from a YAML file, expanding `${ENV_VAR}` references
//! in all string values. The result is a set of [`UpstreamGroup`]s ready to be
//! loaded into the registry.
//!
//! Auth key configuration is handled separately by [`crate::auth_config::AuthConfigSource`].
//!
//! # Config structure
//!
//! ## Flat format (all upstreams are primary tier)
//! ```yaml
//! upstreams:
//!   openai:
//!     provider_type: openai
//!     api_key: "${OPENAI_API_KEY}"
//!     regions:
//!       default:
//!         openai:
//!           base_url: "https://api.openai.com/v1"
//!
//! routes:
//!   - model: "gpt-4o"
//!     openai:
//!       strategy: swrr
//!       upstreams:
//!         - provider: openai
//!           region: default
//!           weight: 1
//! ```
//!
//! ## Primary/fallback format
//! ```yaml
//! routes:
//!   - model: "qwen3-plus"
//!     openai:
//!       primary:
//!         strategy: swrr
//!         upstreams:
//!           - provider: internal-gpu
//!             region: cluster1
//!             weight: 1
//!       fallback:
//!         strategy: swrr
//!         upstreams:
//!           - provider: aliyun
//!             region: default
//!             weight: 1
//! ```

use std::{collections::HashMap, path::PathBuf, sync::Arc};
use std::sync::atomic::AtomicBool;

use serde::Deserialize;

use crate::env_expand::expand_env;
use modelpointer_core::{
    model::ModelCard,
    upstream::{
        node::{
            ApiCompatibility, ProviderType, RuntimeType, UpstreamBinding, UpstreamCredential,
            UpstreamGroup, UpstreamNode, UpstreamProfile,
        },
        routing::{RoutingStrategy, RoutingStrategyConfig},
    },
};

// ── YAML deserialization structs ──────────────────────────────────────────────

#[derive(Debug, Deserialize)]
struct RawConfig {
    #[serde(default)]
    upstreams: HashMap<String, RawProvider>,
    #[serde(default)]
    routes: Vec<RawRoute>,
}

#[derive(Debug, Deserialize)]
struct RawProvider {
    api_key: Option<String>,
    /// Provider type for native API dispatch. Optional; defaults to Unknown.
    #[serde(default)]
    provider_type: ProviderType,
    /// Region map. Optional: providers used only for native routes may omit this.
    #[serde(default)]
    regions: HashMap<String, RawRegion>,
}

#[derive(Debug, Deserialize)]
struct RawRegion {
    openai: Option<RawEndpoint>,
    anthropic: Option<RawEndpoint>,
}

#[derive(Debug, Deserialize)]
struct RawEndpoint {
    base_url: String,
}

#[derive(Debug, Deserialize)]
struct RawRoute {
    model: String,
    #[serde(default)]
    aliases: Vec<String>,
    /// Max requests per minute per (api_key, model) pair. None = unlimited.
    key_rpm: Option<u32>,
    /// Max tokens per minute per (api_key, model) pair. None = unlimited.
    key_tpm: Option<u32>,
    /// Max requests per minute across all API keys for this model. None = unlimited.
    model_rpm: Option<u32>,
    /// Max tokens per minute across all API keys for this model. None = unlimited.
    model_tpm: Option<u32>,
    openai: Option<RawProtocolRoute>,
    anthropic: Option<RawProtocolRoute>,
    /// Provider-specific (native) route — for rerank, TTS, image, video, etc.
    native: Option<RawNativeProtocolRoute>,
}

/// One tier (primary or fallback) in the primary/fallback config format.
#[derive(Debug, Deserialize)]
struct RawTierConfig {
    strategy: Option<RoutingStrategy>,
    /// Max requests/min for this tier before spilling to the fallback tier.
    /// Only meaningful on the primary tier; ignored on fallback.
    capacity_rpm: Option<u32>,
    /// Max tokens/min for this tier before spilling to the fallback tier.
    /// Only meaningful on the primary tier; ignored on fallback.
    capacity_tpm: Option<u32>,
    upstreams: Vec<RawUpstreamRef>,
}

#[derive(Debug, Deserialize)]
struct RawProtocolRoute {
    // ── Flat format (backward compat) ──────────────────────────────────────
    strategy: Option<RoutingStrategy>,
    #[serde(default)]
    upstreams: Vec<RawUpstreamRef>,
    // ── Primary / fallback format ───────────────────────────────────────────
    primary: Option<RawTierConfig>,
    fallback: Option<RawTierConfig>,
}

/// Upstream reference for openai/anthropic routes.
/// Identifies the upstream by provider name + region.
#[derive(Debug, Deserialize)]
struct RawUpstreamRef {
    provider: String,
    region: String,
    weight: Option<u8>,
    #[serde(default)]
    disabled: bool,
    /// Override the model name forwarded to this upstream.
    /// If absent, the gateway model name is forwarded unchanged.
    upstream_model: Option<String>,
}

/// Native (provider-specific) protocol route.
#[derive(Debug, Deserialize)]
struct RawNativeProtocolRoute {
    strategy: Option<RoutingStrategy>,
    #[serde(default)]
    upstreams: Vec<RawNativeUpstreamRef>,
}

/// Upstream reference for native routes.
/// Identifies the upstream by provider name (for API key lookup) + direct URL.
#[derive(Debug, Deserialize)]
struct RawNativeUpstreamRef {
    provider: String,
    url: String,
    weight: Option<u8>,
    #[serde(default)]
    disabled: bool,
    upstream_model: Option<String>,
}

// ── Output ────────────────────────────────────────────────────────────────────

/// Result of loading a file config.
#[derive(Debug)]
pub struct FileConfigOutput {
    /// Upstream groups to load into the registry.
    pub groups: Vec<UpstreamGroup>,
}

// ── Internal resolved endpoint ────────────────────────────────────────────────

struct ResolvedEndpoint {
    /// "provider.region" key, used as the upstream's credential name / provider_id.
    credential_name: String,
    base_url: String,
    api_key: Option<String>,
    compatibility: ApiCompatibility,
    provider_type: ProviderType,
}

/// Resolved provider info used by native routes (URL + credentials only).
struct ResolvedProvider {
    api_key: Option<String>,
    provider_type: ProviderType,
}

// ── FileConfigSource ──────────────────────────────────────────────────────────

pub struct FileConfigSource {
    path: PathBuf,
}

impl FileConfigSource {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }

    pub fn load(&self) -> Result<FileConfigOutput, String> {
        let content = std::fs::read_to_string(&self.path)
            .map_err(|e| format!("Failed to read config '{}': {}", self.path.display(), e))?;

        let raw: RawConfig = serde_yaml::from_str(&content)
            .map_err(|e| format!("Config parse error in '{}': {}", self.path.display(), e))?;

        let endpoint_table = resolve_endpoints(&raw.upstreams)?;
        let provider_table = resolve_providers(&raw.upstreams)?;
        let groups = build_groups(&raw.routes, &endpoint_table, &provider_table)?;

        Ok(FileConfigOutput { groups })
    }
}


// ── Build endpoint lookup table ───────────────────────────────────────────────

/// Resolves all provider regions into a flat lookup table keyed by "provider.region".
/// Env vars in api_key and base_url are expanded here.
fn resolve_endpoints(
    providers: &HashMap<String, RawProvider>,
) -> Result<HashMap<String, Vec<ResolvedEndpoint>>, String> {
    let mut table: HashMap<String, Vec<ResolvedEndpoint>> = HashMap::new();

    for (provider_name, provider) in providers {
        let api_key = provider.api_key.as_ref().map(|k| expand_env(k)).transpose()?;

        for (region_name, region) in &provider.regions {
            let key = format!("{}.{}", provider_name, region_name);
            let mut endpoints = Vec::new();

            if let Some(ep) = &region.openai {
                endpoints.push(ResolvedEndpoint {
                    credential_name: key.clone(),
                    base_url: expand_env(&ep.base_url)?,
                    api_key: api_key.clone(),
                    compatibility: ApiCompatibility::OpenAi,
                    provider_type: provider.provider_type.clone(),
                });
            }
            if let Some(ep) = &region.anthropic {
                endpoints.push(ResolvedEndpoint {
                    credential_name: key.clone(),
                    base_url: expand_env(&ep.base_url)?,
                    api_key: api_key.clone(),
                    compatibility: ApiCompatibility::Anthropic,
                    provider_type: provider.provider_type.clone(),
                });
            }

            if endpoints.is_empty() {
                return Err(format!(
                    "Upstream '{}.{}' has no endpoints (add 'openai' or 'anthropic' section)",
                    provider_name, region_name
                ));
            }

            table.insert(key, endpoints);
        }
    }

    Ok(table)
}

/// Builds a flat provider lookup table (keyed by provider name) for native route resolution.
fn resolve_providers(
    providers: &HashMap<String, RawProvider>,
) -> Result<HashMap<String, ResolvedProvider>, String> {
    let mut table: HashMap<String, ResolvedProvider> = HashMap::new();

    for (provider_name, provider) in providers {
        let api_key = provider.api_key.as_ref().map(|k| expand_env(k)).transpose()?;
        table.insert(provider_name.clone(), ResolvedProvider {
            api_key,
            provider_type: provider.provider_type.clone(),
        });
    }

    Ok(table)
}

// ── Build UpstreamGroups ──────────────────────────────────────────────────────

/// Returns the effective routing strategy for a protocol section.
/// In primary/fallback format, the primary tier's strategy is used for the whole group.
fn effective_strategy(proto: &RawProtocolRoute) -> RoutingStrategy {
    if let Some(primary) = &proto.primary {
        return primary.strategy.unwrap_or_default();
    }
    proto.strategy.unwrap_or_default()
}

/// Append bindings for one tier of one protocol section to `bindings`.
fn add_tier_bindings(
    upstreams: &[RawUpstreamRef],
    compat: ApiCompatibility,
    strategy: RoutingStrategy,
    priority: u8,
    table: &HashMap<String, Vec<ResolvedEndpoint>>,
    bindings: &mut Vec<UpstreamBinding>,
) -> Result<(), String> {
    for up_ref in upstreams {
        if up_ref.disabled { continue; }
        let weight = up_ref.weight.unwrap_or(1);
        let key = format!("{}.{}", up_ref.provider, up_ref.region);
        let eps = lookup_endpoints(&key, table, compat.clone())?;
        for ep in eps {
            bindings.push(make_binding(ep, strategy, weight, up_ref.upstream_model.clone(), priority)?);
        }
    }
    Ok(())
}

fn build_groups(
    routes: &[RawRoute],
    table: &HashMap<String, Vec<ResolvedEndpoint>>,
    provider_table: &HashMap<String, ResolvedProvider>,
) -> Result<Vec<UpstreamGroup>, String> {
    let mut groups = Vec::new();

    for route in routes {
        let mut any_bindings = false;

        // Build one independent UpstreamGroup per protocol section.
        if let Some(proto) = &route.openai {
            let strategy = effective_strategy(proto);
            let mut bindings: Vec<UpstreamBinding> = Vec::new();

            if proto.primary.is_some() || proto.fallback.is_some() {
                if let Some(tier) = &proto.primary {
                    add_tier_bindings(&tier.upstreams, ApiCompatibility::OpenAi, strategy, 0, table, &mut bindings)?;
                }
                if let Some(tier) = &proto.fallback {
                    add_tier_bindings(&tier.upstreams, ApiCompatibility::OpenAi, strategy, 1, table, &mut bindings)?;
                }
            } else {
                add_tier_bindings(&proto.upstreams, ApiCompatibility::OpenAi, strategy, 0, table, &mut bindings)?;
            }

            if !bindings.is_empty() {
                any_bindings = true;
                let capacity = proto.primary.as_ref()
                    .map(|t| (t.capacity_rpm, t.capacity_tpm))
                    .unwrap_or((None, None));
                let model_card = ModelCard::new(route.model.clone()).with_aliases(route.aliases.clone());
                let group = UpstreamGroup::new(model_card, strategy, bindings)?
                    .with_rate_limits(route.key_rpm, route.key_tpm, route.model_rpm, route.model_tpm)
                    .with_primary_capacity(capacity.0, capacity.1);
                groups.push(group);
            }
        }

        if let Some(proto) = &route.anthropic {
            let strategy = effective_strategy(proto);
            let mut bindings: Vec<UpstreamBinding> = Vec::new();

            if proto.primary.is_some() || proto.fallback.is_some() {
                if let Some(tier) = &proto.primary {
                    add_tier_bindings(&tier.upstreams, ApiCompatibility::Anthropic, strategy, 0, table, &mut bindings)?;
                }
                if let Some(tier) = &proto.fallback {
                    add_tier_bindings(&tier.upstreams, ApiCompatibility::Anthropic, strategy, 1, table, &mut bindings)?;
                }
            } else {
                add_tier_bindings(&proto.upstreams, ApiCompatibility::Anthropic, strategy, 0, table, &mut bindings)?;
            }

            if !bindings.is_empty() {
                any_bindings = true;
                let capacity = proto.primary.as_ref()
                    .map(|t| (t.capacity_rpm, t.capacity_tpm))
                    .unwrap_or((None, None));
                let model_card = ModelCard::new(route.model.clone()).with_aliases(route.aliases.clone());
                let group = UpstreamGroup::new(model_card, strategy, bindings)?
                    .with_rate_limits(route.key_rpm, route.key_tpm, route.model_rpm, route.model_tpm)
                    .with_primary_capacity(capacity.0, capacity.1);
                groups.push(group);
            }
        }

        if let Some(proto) = &route.native {
            let strategy = proto.strategy.unwrap_or_default();
            let mut bindings: Vec<UpstreamBinding> = Vec::new();

            for up_ref in &proto.upstreams {
                if up_ref.disabled { continue; }
                let weight = up_ref.weight.unwrap_or(1);

                let resolved_prov = provider_table.get(&up_ref.provider)
                    .ok_or_else(|| format!(
                        "Native route for '{}': provider '{}' not found — check your upstreams config",
                        route.model, up_ref.provider
                    ))?;

                let url = expand_env(&up_ref.url)?;
                let node = UpstreamNode {
                    profile: UpstreamProfile {
                        base_url: url,
                        provider_node_id: String::new(),
                        api_compatibility: ApiCompatibility::Native,
                        runtime_type: RuntimeType::External,
                        credential: Arc::new(UpstreamCredential {
                            name: up_ref.provider.clone(),
                            api_key: resolved_prov.api_key.clone(),
                            provider_type: resolved_prov.provider_type.clone(),
                        }),
                        upstream_model_name: up_ref.upstream_model.clone(),
                    },
                    healthy: Arc::new(AtomicBool::new(true)),
                };
                let strategy_config = match strategy {
                    RoutingStrategy::Swrr => RoutingStrategyConfig::Swrr { weight },
                    RoutingStrategy::WeightedHash => RoutingStrategyConfig::WeightedHash { weight },
                };
                bindings.push(UpstreamBinding::new(node, true, strategy_config, 0)?);
            }

            if !bindings.is_empty() {
                any_bindings = true;
                let model_card = ModelCard::new(route.model.clone()).with_aliases(route.aliases.clone());
                let group = UpstreamGroup::new(model_card, strategy, bindings)?
                    .with_rate_limits(route.key_rpm, route.key_tpm, route.model_rpm, route.model_tpm);
                groups.push(group);
            }
        }

        if !any_bindings {
            return Err(format!(
                "Route '{}' has no active upstreams (all may be disabled)",
                route.model
            ));
        }
    }

    Ok(groups)
}

/// Returns all endpoints for `"provider.region"` that match the given compatibility.
/// Errors if the ref doesn't exist or has no matching endpoint.
fn lookup_endpoints<'a>(
    name: &str,
    table: &'a HashMap<String, Vec<ResolvedEndpoint>>,
    compat: ApiCompatibility,
) -> Result<impl Iterator<Item = &'a ResolvedEndpoint>, String> {
    let all = table.get(name)
        .ok_or_else(|| format!("Upstream '{}' not found — check your upstreams config", name))?;

    let matched: Vec<&ResolvedEndpoint> = all.iter()
        .filter(|ep| ep.compatibility == compat)
        .collect();

    if matched.is_empty() {
        return Err(format!(
            "Upstream '{}' has no {} endpoint defined",
            name, compat
        ));
    }

    Ok(matched.into_iter())
}

fn make_binding(
    ep: &ResolvedEndpoint,
    strategy: RoutingStrategy,
    weight: u8,
    upstream_model: Option<String>,
    priority: u8,
) -> Result<UpstreamBinding, String> {
    let node = UpstreamNode {
        profile: UpstreamProfile {
            base_url: ep.base_url.clone(),
            provider_node_id: String::new(),
            api_compatibility: ep.compatibility.clone(),
            runtime_type: RuntimeType::External,
            credential: Arc::new(UpstreamCredential {
                name: ep.credential_name.clone(),
                api_key: ep.api_key.clone(),
                provider_type: ep.provider_type.clone(),
            }),
            upstream_model_name: upstream_model,
        },
        healthy: Arc::new(AtomicBool::new(true)),
    };

    let strategy_config = match strategy {
        RoutingStrategy::Swrr => RoutingStrategyConfig::Swrr { weight },
        RoutingStrategy::WeightedHash => RoutingStrategyConfig::WeightedHash { weight },
    };

    UpstreamBinding::new(node, true, strategy_config, priority)
}

#[cfg(test)]
mod tests {
    use super::*;
    use modelpointer_core::upstream::Upstream;
    use uuid::Uuid;

    // ── Helpers ───────────────────────────────────────────────────────────────

    /// Write YAML to a temp file, run `load()`, delete the file, return the result.
    fn load(yaml: &str) -> Result<FileConfigOutput, String> {
        let path = format!("/tmp/tp-cfg-test-{}.yaml", Uuid::new_v4());
        std::fs::write(&path, yaml).unwrap();
        let result = FileConfigSource::new(&path).load();
        let _ = std::fs::remove_file(&path);
        result
    }

    // Minimal upstream block shared across many tests.
    fn upstream_block(provider: &str, region: &str, base_url: &str) -> String {
        format!(
            "upstreams:\n  {provider}:\n    regions:\n      {region}:\n        openai:\n          base_url: \"{base_url}\"\n",
        )
    }

    // ── Flat format ───────────────────────────────────────────────────────────

    #[test]
    fn flat_format_single_openai_upstream() {
        let yaml = r#"
upstreams:
  prov:
    api_key: "sk-test"
    regions:
      default:
        openai:
          base_url: "http://upstream-a"

routes:
  - model: "gpt-4o"
    openai:
      strategy: swrr
      upstreams:
        - provider: prov
          region: default
          weight: 2
"#;
        let out = load(yaml).unwrap();
        assert_eq!(out.groups.len(), 1);
        let g = &out.groups[0];
        assert_eq!(g.model.id, "gpt-4o");
        assert_eq!(g.upstreams.len(), 1);
        let b = &g.upstreams[0];
        assert_eq!(b.base_url(), "http://upstream-a");
        assert_eq!(b.api_key(), &Some("sk-test".to_string()));
        assert_eq!(b.strategy_config.weight(), 2);
        assert_eq!(b.priority, 0);
        assert!(matches!(b.api_compatibility(), ApiCompatibility::OpenAi));
    }

    #[test]
    fn flat_format_weight_defaults_to_one() {
        let yaml = upstream_block("p", "r", "http://x") + r#"
routes:
  - model: "m"
    openai:
      upstreams:
        - provider: p
          region: r
"#;
        let out = load(&yaml).unwrap();
        assert_eq!(out.groups[0].upstreams[0].strategy_config.weight(), 1);
    }

    #[test]
    fn multiple_routes_produce_multiple_groups() {
        let yaml = r#"
upstreams:
  p:
    regions:
      r:
        openai:
          base_url: "http://x"

routes:
  - model: "m1"
    openai:
      upstreams: [{provider: p, region: r}]
  - model: "m2"
    openai:
      upstreams: [{provider: p, region: r}]
"#;
        let out = load(yaml).unwrap();
        assert_eq!(out.groups.len(), 2);
        let ids: Vec<_> = out.groups.iter().map(|g| g.model.id.as_str()).collect();
        assert!(ids.contains(&"m1"));
        assert!(ids.contains(&"m2"));
    }

    // ── Anthropic compatibility ───────────────────────────────────────────────

    #[test]
    fn anthropic_compatibility_is_loaded() {
        let yaml = r#"
upstreams:
  p:
    api_key: "key"
    regions:
      r:
        anthropic:
          base_url: "http://anthropic-upstream"

routes:
  - model: "claude"
    anthropic:
      upstreams:
        - provider: p
          region: r
"#;
        let out = load(yaml).unwrap();
        let b = &out.groups[0].upstreams[0];
        assert_eq!(b.base_url(), "http://anthropic-upstream");
        assert!(matches!(b.api_compatibility(), ApiCompatibility::Anthropic));
    }

    // ── Primary / fallback format ─────────────────────────────────────────────

    #[test]
    fn primary_fallback_format_sets_tier_priorities() {
        let yaml = r#"
upstreams:
  p:
    regions:
      r1:
        openai:
          base_url: "http://primary"
      r2:
        openai:
          base_url: "http://fallback"

routes:
  - model: "m"
    openai:
      primary:
        strategy: swrr
        upstreams: [{provider: p, region: r1}]
      fallback:
        strategy: swrr
        upstreams: [{provider: p, region: r2}]
"#;
        let out = load(yaml).unwrap();
        let g = &out.groups[0];
        assert_eq!(g.upstreams.len(), 2);
        let primary = g.upstreams.iter().find(|b| b.base_url() == "http://primary").unwrap();
        let fallback = g.upstreams.iter().find(|b| b.base_url() == "http://fallback").unwrap();
        assert_eq!(primary.priority, 0);
        assert_eq!(fallback.priority, 1);
    }

    #[test]
    fn primary_capacity_is_propagated_to_group() {
        let yaml = r#"
upstreams:
  p:
    regions:
      r1:
        openai:
          base_url: "http://primary"
      r2:
        openai:
          base_url: "http://fallback"

routes:
  - model: "m"
    openai:
      primary:
        strategy: swrr
        capacity_rpm: 500
        capacity_tpm: 100000
        upstreams: [{provider: p, region: r1}]
      fallback:
        strategy: swrr
        upstreams: [{provider: p, region: r2}]
"#;
        let out = load(yaml).unwrap();
        let g = &out.groups[0];
        assert_eq!(g.primary_capacity_rpm, Some(500));
        assert_eq!(g.primary_capacity_tpm, Some(100_000));
    }

    // ── Rate limits ───────────────────────────────────────────────────────────

    #[test]
    fn rate_limits_are_propagated_to_group() {
        let yaml = upstream_block("p", "r", "http://x") + r#"
routes:
  - model: "m"
    key_rpm: 100
    key_tpm: 50000
    model_rpm: 1000
    model_tpm: 500000
    openai:
      upstreams: [{provider: p, region: r}]
"#;
        let out = load(&yaml).unwrap();
        let g = &out.groups[0];
        assert_eq!(g.key_rpm_limit, Some(100));
        assert_eq!(g.key_tpm_limit, Some(50_000));
        assert_eq!(g.model_rpm_limit, Some(1_000));
        assert_eq!(g.model_tpm_limit, Some(500_000));
    }

    // ── Disabled upstream ─────────────────────────────────────────────────────

    #[test]
    fn disabled_upstream_is_skipped() {
        let yaml = r#"
upstreams:
  p:
    regions:
      r1:
        openai:
          base_url: "http://active"
      r2:
        openai:
          base_url: "http://disabled"

routes:
  - model: "m"
    openai:
      upstreams:
        - provider: p
          region: r1
        - provider: p
          region: r2
          disabled: true
"#;
        let out = load(yaml).unwrap();
        assert_eq!(out.groups[0].upstreams.len(), 1);
        assert_eq!(out.groups[0].upstreams[0].base_url(), "http://active");
    }

    // ── Env var expansion ─────────────────────────────────────────────────────

    #[test]
    fn env_var_expanded_in_api_key() {
        let var = format!("TP_TEST_KEY_{}", Uuid::new_v4().simple());
        unsafe { std::env::set_var(&var, "sk-secret") };

        let yaml = format!(
            "upstreams:\n  p:\n    api_key: \"${{{var}}}\"\n    regions:\n      r:\n        openai:\n          base_url: \"http://x\"\n\nroutes:\n  - model: m\n    openai:\n      upstreams: [{{provider: p, region: r}}]\n"
        );
        let out = load(&yaml).unwrap();
        assert_eq!(out.groups[0].upstreams[0].api_key(), &Some("sk-secret".to_string()));

        unsafe { std::env::remove_var(&var) };
    }

    #[test]
    fn env_var_expanded_in_base_url() {
        let var = format!("TP_TEST_URL_{}", Uuid::new_v4().simple());
        unsafe { std::env::set_var(&var, "http://dynamic-host") };

        let yaml = format!(
            "upstreams:\n  p:\n    regions:\n      r:\n        openai:\n          base_url: \"${{{var}}}/v1\"\n\nroutes:\n  - model: m\n    openai:\n      upstreams: [{{provider: p, region: r}}]\n"
        );
        let out = load(&yaml).unwrap();
        assert_eq!(out.groups[0].upstreams[0].base_url(), "http://dynamic-host/v1");

        unsafe { std::env::remove_var(&var) };
    }

    // ── Aliases ───────────────────────────────────────────────────────────────

    #[test]
    fn model_aliases_are_preserved() {
        let yaml = upstream_block("p", "r", "http://x") + r#"
routes:
  - model: "gpt-4o"
    aliases: ["gpt4o", "gpt-4-omni"]
    openai:
      upstreams: [{provider: p, region: r}]
"#;
        let out = load(&yaml).unwrap();
        let g = &out.groups[0];
        assert_eq!(g.model.id, "gpt-4o");
        assert!(g.model.aliases.contains(&"gpt4o".to_string()));
        assert!(g.model.aliases.contains(&"gpt-4-omni".to_string()));
    }

    // ── Upstream model name override ──────────────────────────────────────────

    #[test]
    fn upstream_model_name_override_is_loaded() {
        let yaml = upstream_block("p", "r", "http://x") + r#"
routes:
  - model: "qwen3-plus"
    openai:
      upstreams:
        - provider: p
          region: r
          upstream_model: "qwen-plus-latest"
"#;
        let out = load(&yaml).unwrap();
        let b = &out.groups[0].upstreams[0];
        assert_eq!(b.upstream_model_name(), Some("qwen-plus-latest"));
    }

    // ── Multiple regions / providers ──────────────────────────────────────────

    #[test]
    fn multiple_regions_produce_multiple_bindings() {
        let yaml = r#"
upstreams:
  p:
    regions:
      us:
        openai:
          base_url: "http://us"
      eu:
        openai:
          base_url: "http://eu"

routes:
  - model: "m"
    openai:
      upstreams:
        - provider: p
          region: us
        - provider: p
          region: eu
"#;
        let out = load(yaml).unwrap();
        let urls: Vec<_> = out.groups[0].upstreams.iter().map(|b| b.base_url()).collect();
        assert!(urls.contains(&"http://us"));
        assert!(urls.contains(&"http://eu"));
    }

    // ── Provider type ─────────────────────────────────────────────────────────

    #[test]
    fn provider_type_is_propagated() {
        let yaml = r#"
upstreams:
  aliyun:
    provider_type: aliyun
    api_key: "sk-test"
    regions:
      default:
        openai:
          base_url: "http://dashscope"

routes:
  - model: "qwen-turbo"
    openai:
      upstreams:
        - provider: aliyun
          region: default
"#;
        let out = load(yaml).unwrap();
        let b = &out.groups[0].upstreams[0];
        assert!(matches!(b.provider_type(), ProviderType::Aliyun));
    }

    // ── Native routes ─────────────────────────────────────────────────────────

    #[test]
    fn native_route_with_provider_and_url() {
        let yaml = r#"
upstreams:
  aliyun:
    provider_type: aliyun
    api_key: "sk-dashscope"

routes:
  - model: "gte-rerank"
    native:
      upstreams:
        - provider: aliyun
          url: "https://dashscope.aliyuncs.com/api/v1/services/rerank/text-rerank/text-rerank"
          weight: 1
"#;
        let out = load(yaml).unwrap();
        assert_eq!(out.groups.len(), 1);
        let g = &out.groups[0];
        assert_eq!(g.model.id, "gte-rerank");
        let b = &g.upstreams[0];
        assert_eq!(b.base_url(), "https://dashscope.aliyuncs.com/api/v1/services/rerank/text-rerank/text-rerank");
        assert_eq!(b.api_key(), &Some("sk-dashscope".to_string()));
        assert!(matches!(b.api_compatibility(), ApiCompatibility::Native));
        assert!(matches!(b.provider_type(), ProviderType::Aliyun));
    }

    #[test]
    fn native_route_no_regions_required() {
        // Provider used only for native routes — no `regions` configured.
        let yaml = r#"
upstreams:
  cohere:
    provider_type: cohere
    api_key: "co-key"

routes:
  - model: "rerank-english"
    native:
      upstreams:
        - provider: cohere
          url: "https://api.cohere.com/v2/rerank"
"#;
        let out = load(yaml).unwrap();
        assert_eq!(out.groups[0].model.id, "rerank-english");
        assert!(matches!(out.groups[0].upstreams[0].api_compatibility(), ApiCompatibility::Native));
    }

    #[test]
    fn native_and_openai_routes_for_same_provider() {
        // Same provider can serve both openai (chat) and native (rerank).
        let yaml = r#"
upstreams:
  aliyun:
    provider_type: aliyun
    api_key: "sk-dashscope"
    regions:
      default:
        openai:
          base_url: "https://dashscope.aliyuncs.com/compatible-mode/v1"

routes:
  - model: "qwen-turbo"
    openai:
      upstreams:
        - provider: aliyun
          region: default
  - model: "gte-rerank"
    native:
      upstreams:
        - provider: aliyun
          url: "https://dashscope.aliyuncs.com/api/v1/services/rerank/text-rerank/text-rerank"
"#;
        let out = load(yaml).unwrap();
        assert_eq!(out.groups.len(), 2);
        let chat = out.groups.iter().find(|g| g.model.id == "qwen-turbo").unwrap();
        let rerank = out.groups.iter().find(|g| g.model.id == "gte-rerank").unwrap();
        assert!(matches!(chat.upstreams[0].api_compatibility(), ApiCompatibility::OpenAi));
        assert!(matches!(rerank.upstreams[0].api_compatibility(), ApiCompatibility::Native));
    }

    // ── Error cases ───────────────────────────────────────────────────────────

    #[test]
    fn missing_file_returns_error() {
        let err = FileConfigSource::new("/tmp/tp-nonexistent-config.yaml").load().unwrap_err();
        assert!(err.contains("Failed to read config"), "unexpected error: {err}");
    }

    #[test]
    fn malformed_yaml_returns_error() {
        let err = load("{ not: valid: yaml:").unwrap_err();
        assert!(err.contains("Config parse error"), "unexpected error: {err}");
    }

    #[test]
    fn missing_env_var_returns_error() {
        let var = format!("TP_TEST_MISSING_{}", Uuid::new_v4().simple());
        let yaml = format!(
            "upstreams:\n  p:\n    api_key: \"${{{var}}}\"\n    regions:\n      r:\n        openai:\n          base_url: \"http://x\"\n\nroutes:\n  - model: m\n    openai:\n      upstreams: [{{provider: p, region: r}}]\n"
        );
        let err = load(&yaml).unwrap_err();
        assert!(err.contains("not set") || err.contains(&var), "unexpected error: {err}");
    }

    #[test]
    fn unknown_upstream_ref_returns_error() {
        let yaml = upstream_block("p", "r", "http://x") + r#"
routes:
  - model: "m"
    openai:
      upstreams: [{provider: p, region: nonexistent}]
"#;
        let err = load(&yaml).unwrap_err();
        assert!(err.contains("p.nonexistent") || err.contains("not found"), "unexpected error: {err}");
    }

    #[test]
    fn upstream_has_no_matching_compat_returns_error() {
        let yaml = r#"
upstreams:
  p:
    regions:
      r:
        anthropic:
          base_url: "http://anthropic-only"

routes:
  - model: "m"
    openai:
      upstreams: [{provider: p, region: r}]
"#;
        let err = load(yaml).unwrap_err();
        assert!(err.contains("no openai endpoint") || err.contains("p.r"), "unexpected error: {err}");
    }

    #[test]
    fn region_with_no_endpoints_returns_error() {
        let yaml = r#"
upstreams:
  p:
    regions:
      r:
        # intentionally empty

routes:
  - model: "m"
    openai:
      upstreams: [{provider: p, region: r}]
"#;
        let result = load(yaml);
        assert!(result.is_err(), "empty region should produce an error");
    }

    #[test]
    fn all_upstreams_disabled_returns_error() {
        let yaml = upstream_block("p", "r", "http://x") + r#"
routes:
  - model: "m"
    openai:
      upstreams:
        - provider: p
          region: r
          disabled: true
"#;
        let err = load(&yaml).unwrap_err();
        assert!(err.contains("no active upstreams") || err.contains("disabled"), "unexpected error: {err}");
    }

    #[test]
    fn independent_protocol_strategies_succeed() {
        let yaml = r#"
upstreams:
  p:
    regions:
      r:
        openai:
          base_url: "http://x"
        anthropic:
          base_url: "http://y"

routes:
  - model: "m"
    openai:
      strategy: swrr
      upstreams: [{provider: p, region: r}]
    anthropic:
      strategy: weighted_hash
      upstreams: [{provider: p, region: r}]
"#;
        let out = load(yaml).expect("different strategies per protocol must be valid");
        assert_eq!(out.groups.len(), 2, "one group per protocol");
    }

    #[test]
    fn native_unknown_provider_returns_error() {
        let yaml = r#"
upstreams:
  aliyun:
    provider_type: aliyun
    api_key: "sk-test"

routes:
  - model: "rerank"
    native:
      upstreams:
        - provider: nonexistent
          url: "https://example.com/rerank"
"#;
        let err = load(yaml).unwrap_err();
        assert!(err.contains("nonexistent") || err.contains("not found"), "unexpected error: {err}");
    }
}
