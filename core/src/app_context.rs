use std::{
    sync::{Arc},
    time::Duration,
};

use reqwest::Client;
use tracing::debug;

use crate::{
    config::RouterConfig,
    upstream::UpstreamRegistry,
};

#[derive(Clone)]
pub struct AppContext {
    pub client: Client,
    pub router_config: RouterConfig,
    pub upstream_registry: Arc<UpstreamRegistry>,
}

impl AppContext {
    pub async fn with_config(
        router_config: RouterConfig,
    ) -> Result<Self, String> {
        let client = Self::build_http_client(&router_config)?;
        let upstream_registry = Arc::new(UpstreamRegistry::new());

        Ok(Self {
            client,
            router_config,
            upstream_registry,
        })
    }

    /// Create HTTP client with TLS/mTLS configuration
    fn build_http_client(config: &RouterConfig) -> Result<Client, String> {
        // FIXME: Current implementation creates a single HTTP client for all workers.
        // This works well for single security domain deployments where all workers share
        // the same CA and can accept the same client certificate.
        //
        // For multi-domain deployments (e.g., different model families with different CAs),
        // this architecture needs significant refactoring:
        // 1. Move client creation into worker registration workflow (per-worker clients)
        // 2. Store client per worker in WorkerRegistry
        // 3. Add per-worker TLS spec in WorkerConfigRequest
        //
        // Current single-domain approach is sufficient for most deployments.
        //
        // Use rustls TLS backend when TLS/mTLS is configured (client cert or CA certs provided).
        // This ensures proper PKCS#8 key format support. For plain HTTP workers, use default
        // backend to avoid unnecessary TLS initialization overhead.
        let has_tls_config = config.client_identity.is_some() || !config.ca_certificates.is_empty();

        let mut client_builder = Client::builder()
            .pool_idle_timeout(Some(Duration::from_secs(50)))
            .pool_max_idle_per_host(500)
            .timeout(Duration::from_secs(config.request_timeout_secs))
            .connect_timeout(Duration::from_secs(10))
            .tcp_nodelay(true)
            .tcp_keepalive(Some(Duration::from_secs(30)));

        // Force rustls backend when TLS is configured
        if has_tls_config {
            client_builder = client_builder.use_rustls_tls();
            debug!("Using rustls TLS backend for TLS/mTLS connections");
        }

        // Configure mTLS client identity if provided (certificates already loaded during config creation)
        if let Some(identity_pem) = &config.client_identity {
            let identity = reqwest::Identity::from_pem(identity_pem)
                .map_err(|e| format!("Failed to create client identity: {}", e))?;
            client_builder = client_builder.identity(identity);
            debug!("mTLS client authentication enabled");
        }

        // Add CA certificates for verifying worker TLS (certificates already loaded during config creation)
        for ca_cert in &config.ca_certificates {
            let cert = reqwest::Certificate::from_pem(ca_cert)
                .map_err(|e| format!("Failed to add CA certificate: {}", e))?;
            client_builder = client_builder.add_root_certificate(cert);
        }
        if !config.ca_certificates.is_empty() {
            debug!(
                "Added {} CA certificate(s) for worker verification",
                config.ca_certificates.len()
            );
        }

        let client = client_builder
            .build()
            .map_err(|e| format!("Failed to create HTTP client: {}", e))?;

        Ok(client)
    }
}