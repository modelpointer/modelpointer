use std::time::Duration;

use sqlx::{AnyPool, any::AnyPoolOptions};

use crate::config::DatabaseConfig;

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
    /// Connect to the database without running migrations.
    /// Migrations are the responsibility of the admin crate.
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
