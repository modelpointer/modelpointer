//! Upstream backend abstractions for the gateway router.
//!
//! This module contains the fundamental types and traits used throughout the router:
//! - `Upstream` trait and implementations
//! - Routing strategies (SWRR, WeightedHash)
//! - Circuit breaker for reliability
//! - Retry executor

// Re-export UNKNOWN_MODEL_ID from protocols for use throughout core
pub use crate::openai_protocol::UNKNOWN_MODEL_ID;

pub mod circuit_breaker;
pub mod node;
pub mod registry;
pub mod retry;
pub mod routing;

// Re-export commonly used types for convenience
pub use circuit_breaker::{CircuitBreaker, CircuitBreakerConfig};
pub use node::{
    ApiCompatibility, ProviderType, RuntimeType, Upstream, UpstreamBinding, UpstreamCredential,
    UpstreamGroup, UpstreamNode, UpstreamProfile,
};
pub use registry::UpstreamRegistry;
pub use retry::{RetryExecutor, is_retryable_status};
pub use routing::{RoutingStrategy, RoutingStrategyConfig};
