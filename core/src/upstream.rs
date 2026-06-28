//! Upstream backend abstractions for the gateway router.
//!
//! This module contains the fundamental types and traits used throughout the router:
//! - `Upstream` trait and implementations
//! - Routing strategies (SWRR, WeightedHash)
//! - Circuit breaker for reliability
//! - Retry executor

// Re-export UNKNOWN_MODEL_ID from protocols for use throughout core
pub use crate::openai_protocol::UNKNOWN_MODEL_ID;

pub mod retry;
pub mod node;
pub mod routing;
pub mod registry;
pub mod circuit_breaker;

// Re-export commonly used types for convenience
pub use circuit_breaker::{CircuitBreaker, CircuitBreakerConfig};
pub use retry::{is_retryable_status, RetryExecutor};
pub use node::{
    ApiCompatibility, Upstream, UpstreamBinding, UpstreamCredential,
    UpstreamGroup, UpstreamNode, UpstreamProfile, RuntimeType, ProviderType,
};
pub use routing::{RoutingStrategy, RoutingStrategyConfig};
pub use registry::UpstreamRegistry;