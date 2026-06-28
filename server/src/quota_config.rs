//! Per-(api_key, model) quota overrides.
//!
//! Loaded from a YAML file at startup and hot-reloaded on file changes.
//! When a quota entry exists for a (api_key_id, model_id) pair, its limits
//! override the model-level defaults from the route config.
//!
//! # Config format
//!
//! ```yaml
//! api_key_quotas:
//!   - api_key_id: "key_abc123"
//!     model_id: "gpt-4o"
//!     key_rpm: 1000
//!     key_tpm: 500000
//!
//!   - api_key_id: "key_def456"
//!     model_id: "gpt-4o"
//!     key_rpm: 200
//!     # key_tpm omitted: falls back to model default
//!
//! # user_quotas: reserved for a future release
//! ```

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use arc_swap::ArcSwap;
use serde::Deserialize;

// ── YAML deserialization structs ──────────────────────────────────────────────

#[derive(Debug, Deserialize)]
struct RawQuotaConfig {
    #[serde(default)]
    api_key_quotas: Vec<RawApiKeyQuota>,
    // user_quotas: reserved for a future release.
    // Unrecognised YAML keys are silently ignored by serde, so configs that
    // already contain a `user_quotas` section will continue to parse correctly
    // once that field is added here.
}

#[derive(Debug, Deserialize)]
pub(crate) struct RawApiKeyQuota {
    pub(crate) api_key_id: String,
    pub(crate) model_id: String,
    pub(crate) key_rpm: Option<u32>,
    pub(crate) key_tpm: Option<u32>,
}

// ── Public types ──────────────────────────────────────────────────────────────

/// Rate-limit override for one (api_key, model) pair.
/// Fields absent in the config fall back to the model-level defaults.
#[derive(Debug, Clone)]
pub struct KeyQuota {
    pub key_rpm: Option<u32>,
    pub key_tpm: Option<u32>,
}

// ── QuotaStore ────────────────────────────────────────────────────────────────

/// In-process store for quota overrides, hot-reloadable via `reload()`.
///
/// Reads use `ArcSwap` for lock-free access on the hot path.
pub struct QuotaStore {
    api_key_map: ArcSwap<HashMap<(String, String), KeyQuota>>,
}

impl QuotaStore {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            api_key_map: ArcSwap::from_pointee(HashMap::new()),
        })
    }

    /// Replace the quota map atomically. Called at startup and on file change.
    pub(crate) fn reload(&self, entries: Vec<RawApiKeyQuota>) {
        let map = entries
            .into_iter()
            .map(|q| {
                (
                    (q.api_key_id, q.model_id),
                    KeyQuota { key_rpm: q.key_rpm, key_tpm: q.key_tpm },
                )
            })
            .collect();
        self.api_key_map.store(Arc::new(map));
    }

    /// Look up the quota override for a (api_key_id, model_id) pair.
    /// Returns `None` when no override is configured — caller should use
    /// the model-level defaults.
    pub fn get(&self, api_key_id: &str, model_id: &str) -> Option<KeyQuota> {
        self.api_key_map
            .load()
            .get(&(api_key_id.to_string(), model_id.to_string()))
            .cloned()
    }
}

// ── QuotaConfigSource ─────────────────────────────────────────────────────────

pub struct QuotaConfigSource {
    path: PathBuf,
}

impl QuotaConfigSource {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }

    pub(crate) fn load(&self) -> Result<Vec<RawApiKeyQuota>, String> {
        let content = std::fs::read_to_string(&self.path).map_err(|e| {
            format!("Failed to read quota config '{}': {}", self.path.display(), e)
        })?;
        let raw: RawQuotaConfig = serde_yaml::from_str(&content)
            .map_err(|e| format!("Quota config parse error in '{}': {}", self.path.display(), e))?;
        Ok(raw.api_key_quotas)
    }
}
