use sqlx::{AnyPool, Row};

use crate::quota_config::RawApiKeyQuota;

use super::DatabaseDialect;

/// Load all quota overrides from the database.
/// Called at startup and periodically by the polling task.
///
/// Returns `Vec<RawApiKeyQuota>` so the result can be passed directly to
/// `QuotaStore::reload()`, keeping the gateway quota hot-path unchanged.
pub(crate) async fn load_all_quota_overrides(
    pool: &AnyPool,
    _dialect: DatabaseDialect,
) -> Result<Vec<RawApiKeyQuota>, String> {
    let rows =
        sqlx::query("SELECT api_key_id, model_id, rpm, tpm FROM api_key_model_quota_overrides")
            .fetch_all(pool)
            .await
            .map_err(|e| format!("DB error loading quota overrides: {e}"))?;

    Ok(rows
        .into_iter()
        .map(|r| {
            let rpm: Option<i64> = r.try_get("rpm").ok().flatten();
            let tpm: Option<i64> = r.try_get("tpm").ok().flatten();
            RawApiKeyQuota {
                api_key_id: r.get("api_key_id"),
                model_id: r.get("model_id"),
                key_rpm: rpm.map(|v| v as u32),
                key_tpm: tpm.map(|v| v as u32),
            }
        })
        .collect())
}
