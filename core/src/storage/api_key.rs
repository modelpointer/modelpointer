use async_trait::async_trait;

#[derive(Debug, Clone)]
pub struct ApiKeyLookupResult {
    pub id: String,
    pub uid: String,
    pub status: String,
}

#[async_trait]
pub trait ApiKeyRepository: Send + Sync {
    async fn find_active_by_hash(
        &self,
        key_hash: &str,
    ) -> Result<Option<ApiKeyLookupResult>, Box<dyn std::error::Error + Send + Sync>>;
}
