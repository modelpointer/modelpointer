//! File-based auth key configuration.
//!
//! Loads API keys from a separate YAML file that is managed like a secret
//! (not committed to git). The file is watched for changes and reloaded
//! automatically without restarting the gateway.
//!
//! # Config structure
//!
//! ```yaml
//! mode: api_key   # none | api_key
//! keys:
//!   - id: "7f3a9c2b-41d4-4a71-b446-655440000000"
//!     name: "Service A Production"   # optional, human-readable label
//!     hash: "ba7816bf8f01cfea..."    # SHA-256 hex of the raw key
//!
//!   - id: "3d8f1a2b-5678-4c90-d123-456789012345"
//!     name: "Mobile App"
//!     plain: "${MOBILE_APP_KEY}"     # alternative: expand env var then hash
//!     disabled: true                 # key exists but is rejected by the gateway
//! ```
//!
//! `id` is written to the access log as `api_key_id` and must be unique.
//! Either `hash` or `plain` must be present for each key; `hash` takes priority.

use std::path::PathBuf;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use modelpointer_core::storage::ApiKeyLookupResult;
use crate::env_expand::expand_env;

// ── YAML structs (pub so key_cmd.rs can read/write the file) ─────────────────

#[derive(Debug, Deserialize, Serialize)]
pub struct RawAuthConfig {
    pub mode: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub keys: Vec<RawApiKey>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct RawApiKey {
    pub id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hash: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub plain: Option<String>,
    #[serde(default, skip_serializing_if = "is_false")]
    pub disabled: bool,
}

fn is_false(b: &bool) -> bool {
    !*b
}

// ── Output ────────────────────────────────────────────────────────────────────

pub struct AuthConfigOutput {
    pub auth_required: bool,
    pub auth_keys: Vec<(String, ApiKeyLookupResult)>,
}

// ── AuthConfigSource ──────────────────────────────────────────────────────────

pub struct AuthConfigSource {
    path: PathBuf,
}

impl AuthConfigSource {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }

    /// Load and process the auth config for runtime use.
    /// Disabled keys are excluded from the output.
    pub fn load(&self) -> Result<AuthConfigOutput, String> {
        let raw = self.load_raw()?;
        process_auth(raw)
    }

    /// Load the raw YAML structure for inspection or modification.
    pub fn load_raw(&self) -> Result<RawAuthConfig, String> {
        let content = std::fs::read_to_string(&self.path)
            .map_err(|e| format!("Failed to read auth config '{}': {}", self.path.display(), e))?;
        serde_yaml::from_str(&content)
            .map_err(|e| format!("Auth config parse error in '{}': {}", self.path.display(), e))
    }

    /// Write a raw config back to the file.
    pub fn save_raw(&self, config: &RawAuthConfig) -> Result<(), String> {
        let content = serde_yaml::to_string(config)
            .map_err(|e| format!("Failed to serialize auth config: {}", e))?;
        std::fs::write(&self.path, content)
            .map_err(|e| format!("Failed to write auth config '{}': {}", self.path.display(), e))
    }
}

// ── Auth processing ───────────────────────────────────────────────────────────

fn process_auth(raw: RawAuthConfig) -> Result<AuthConfigOutput, String> {
    match raw.mode.as_str() {
        "none" => Ok(AuthConfigOutput { auth_required: false, auth_keys: vec![] }),
        "api_key" => {
            if raw.keys.is_empty() {
                return Err("auth.mode is 'api_key' but no keys are defined".to_string());
            }

            let mut keys = Vec::with_capacity(raw.keys.len());
            for entry in raw.keys {
                if entry.disabled {
                    continue;
                }

                let key_hash = if let Some(h) = entry.hash {
                    h
                } else if let Some(plain) = entry.plain {
                    let expanded = expand_env(&plain)?;
                    sha256_hex(&expanded)
                } else {
                    return Err(format!(
                        "Key '{}' must have either 'hash' or 'plain'",
                        entry.id
                    ));
                };

                keys.push((key_hash, ApiKeyLookupResult {
                    id: entry.id.clone(),
                    uid: entry.id,
                    status: "active".to_string(),
                }));
            }

            Ok(AuthConfigOutput { auth_required: true, auth_keys: keys })
        }
        other => Err(format!("Unknown auth.mode '{}': expected 'none' or 'api_key'", other)),
    }
}

fn sha256_hex(input: &str) -> String {
    format!("{:x}", Sha256::digest(input.as_bytes()))
}
