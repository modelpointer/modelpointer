//! Model identity types used by the router.
//!
//! [`ModelCard`] carries the minimal model identity needed for routing:
//! a primary ID and optional aliases.

use serde::{Deserialize, Serialize};

use crate::openai_protocol::UNKNOWN_MODEL_ID;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModelCard {
    /// Primary model ID (e.g., "meta-llama/Llama-3.1-8B-Instruct")
    pub id: String,

    /// Alternative names/aliases for this model
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub aliases: Vec<String>,
}

impl ModelCard {
    /// Create a new model card with just an ID.
    pub fn new(id: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            aliases: Vec::new(),
        }
    }

    /// Add a single alias.
    pub fn with_alias(mut self, alias: impl Into<String>) -> Self {
        self.aliases.push(alias.into());
        self
    }

    /// Add multiple aliases.
    pub fn with_aliases(mut self, aliases: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.aliases.extend(aliases.into_iter().map(|a| a.into()));
        self
    }

    /// Check if this model matches the given ID (including aliases).
    pub fn matches(&self, model_id: &str) -> bool {
        self.id == model_id || self.aliases.iter().any(|a| a == model_id)
    }
}

impl Default for ModelCard {
    fn default() -> Self {
        Self::new(UNKNOWN_MODEL_ID)
    }
}

impl std::fmt::Display for ModelCard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.id)
    }
}
