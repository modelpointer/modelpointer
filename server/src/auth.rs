//! SQL and cached implementations of [`ApiKeyRepository`].
//!
//! [`SqlApiKeyRepository`] performs a per-request database lookup.
//! [`CachedApiKeyRepository`] keeps an in-memory snapshot that a background
//! task refreshes on a configurable interval, eliminating DB round-trips on
//! the hot path at the cost of bounded staleness.

use std::{collections::HashMap, sync::Arc};

use arc_swap::ArcSwap;
use async_trait::async_trait;
use sqlx::{AnyPool, Row};

use modelpointer_core::storage::{ApiKeyLookupResult, ApiKeyRepository};
use crate::db::DatabaseDialect;

// =============================================================================
// SQL implementation
// =============================================================================

#[derive(Clone)]
pub struct SqlApiKeyRepository {
    pool: AnyPool,
    dialect: DatabaseDialect,
}

impl SqlApiKeyRepository {
    pub fn new(pool: AnyPool, dialect: DatabaseDialect) -> Self {
        Self { pool, dialect }
    }

    pub fn into_shared(self) -> Arc<dyn ApiKeyRepository> {
        Arc::new(self)
    }

    /// Fetch all currently active, non-expired keys for bulk cache loading.
    pub async fn find_all_active(&self) -> Result<Vec<(String, ApiKeyLookupResult)>, sqlx::Error> {
        let rows = sqlx::query(
            "SELECT id, uid, key_hash \
             FROM api_keys \
             WHERE status = 'active' \
               AND (expires_at IS NULL OR expires_at > CURRENT_TIMESTAMP)",
        )
        .fetch_all(&self.pool)
        .await?;

        Ok(rows
            .into_iter()
            .map(|row| {
                let key_hash = row.get::<String, _>("key_hash");
                let result = ApiKeyLookupResult {
                    id: row.get::<String, _>("id"),
                    uid: row.get::<String, _>("uid"),
                    status: "active".to_string(),
                };
                (key_hash, result)
            })
            .collect())
    }
}

#[async_trait]
impl ApiKeyRepository for SqlApiKeyRepository {
    async fn find_active_by_hash(
        &self,
        key_hash: &str,
    ) -> Result<Option<ApiKeyLookupResult>, Box<dyn std::error::Error + Send + Sync>> {
        let sql = match self.dialect {
            DatabaseDialect::Postgres => POSTGRES_LOOKUP_SQL,
            DatabaseDialect::MySql | DatabaseDialect::Sqlite => GENERIC_LOOKUP_SQL,
        };

        let row = sqlx::query(sql)
            .bind(key_hash)
            .fetch_optional(&self.pool)
            .await?;

        Ok(row.map(|row| ApiKeyLookupResult {
            id: row.get::<String, _>("id"),
            uid: row.get::<String, _>("uid"),
            status: row.get::<String, _>("status"),
        }))
    }
}

const POSTGRES_LOOKUP_SQL: &str = "\
SELECT id, uid, status \
FROM api_keys \
WHERE key_hash = $1 \
  AND status = 'active' \
  AND (expires_at IS NULL OR expires_at > CURRENT_TIMESTAMP) \
LIMIT 1";

const GENERIC_LOOKUP_SQL: &str = "\
SELECT id, uid, status \
FROM api_keys \
WHERE key_hash = ? \
  AND status = 'active' \
  AND (expires_at IS NULL OR expires_at > CURRENT_TIMESTAMP) \
LIMIT 1";

// =============================================================================
// Cached implementation
// =============================================================================

/// In-memory API key cache backed by an `ArcSwap<HashMap>`.
pub struct CachedApiKeyRepository {
    cache: ArcSwap<HashMap<String, ApiKeyLookupResult>>,
}

impl CachedApiKeyRepository {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            cache: ArcSwap::from_pointee(HashMap::new()),
        })
    }

    /// Atomically replace the entire cache with a fresh snapshot.
    pub fn reload(&self, entries: Vec<(String, ApiKeyLookupResult)>) {
        self.cache.store(Arc::new(entries.into_iter().collect()));
    }

    pub fn into_shared(self: Arc<Self>) -> Arc<dyn ApiKeyRepository> {
        self
    }
}

#[async_trait]
impl ApiKeyRepository for CachedApiKeyRepository {
    async fn find_active_by_hash(
        &self,
        key_hash: &str,
    ) -> Result<Option<ApiKeyLookupResult>, Box<dyn std::error::Error + Send + Sync>> {
        Ok(self.cache.load().get(key_hash).cloned())
    }
}
