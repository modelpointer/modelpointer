//! Lightweight config-version reads for the polling tasks.
//!
//! Each database-backed config category (`routes`, `api_keys`, `quota`) has a
//! row in the `config_versions` table that the admin plane increments on every
//! mutation. The gateway pollers read this cheap single-row value each tick and
//! only run the expensive full reload when the version has changed.

use sqlx::{AnyPool, Row};

use super::DatabaseDialect;

/// A config category tracked in the `config_versions` table.
#[derive(Clone, Copy, Debug)]
pub enum ConfigResource {
    Routes,
    ApiKeys,
    Quota,
}

impl ConfigResource {
    pub fn as_str(self) -> &'static str {
        match self {
            ConfigResource::Routes => "routes",
            ConfigResource::ApiKeys => "api_keys",
            ConfigResource::Quota => "quota",
        }
    }
}

/// Read the current version for `resource`. Returns `None` if the row (or table)
/// is missing or the query fails, which callers treat as "reload to be safe".
pub async fn load_config_version(
    pool: &AnyPool,
    dialect: DatabaseDialect,
    resource: ConfigResource,
) -> Option<i64> {
    let sql = match dialect {
        DatabaseDialect::Postgres => "SELECT version FROM config_versions WHERE resource = $1",
        _ => "SELECT version FROM config_versions WHERE resource = ?",
    };
    sqlx::query(sql)
        .bind(resource.as_str())
        .fetch_optional(pool)
        .await
        .ok()
        .flatten()
        .and_then(|row| row.try_get::<i64, _>("version").ok())
}
