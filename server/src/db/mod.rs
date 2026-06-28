use std::time::Duration;

use sqlx::{AnyPool, any::AnyPoolOptions};

use modelpointer_core::config::DatabaseConfig;

pub mod access_log_store;
pub mod quota_store;
pub mod route_store;
pub(crate) use quota_store::load_all_quota_overrides;
pub use route_store::load_all_upstream_groups;

#[derive(Clone, Copy, Debug)]
pub enum DatabaseDialect {
    Postgres,
    MySql,
    Sqlite,
}

#[derive(Clone)]
pub struct Database {
    pool: AnyPool,
    dialect: DatabaseDialect,
}

impl Database {
    pub async fn connect(config: &DatabaseConfig) -> Result<Self, sqlx::Error> {
        sqlx::any::install_default_drivers();

        let dialect = dialect_from_url(&config.url);
        let pool = AnyPoolOptions::new()
            .max_connections(config.max_connections)
            .min_connections(config.min_connections)
            .acquire_timeout(Duration::from_secs(config.acquire_timeout_secs))
            .connect(&config.url)
            .await?;

        Ok(Self { pool, dialect })
    }

    pub fn pool(&self) -> &AnyPool {
        &self.pool
    }

    pub fn dialect(&self) -> DatabaseDialect {
        self.dialect
    }
}

fn dialect_from_url(url: &str) -> DatabaseDialect {
    if url.starts_with("postgres://") || url.starts_with("postgresql://") {
        DatabaseDialect::Postgres
    } else if url.starts_with("mysql://") {
        DatabaseDialect::MySql
    } else {
        DatabaseDialect::Sqlite
    }
}
