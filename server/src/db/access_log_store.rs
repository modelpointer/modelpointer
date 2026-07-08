use chrono::{DateTime, SecondsFormat, Utc};
use sqlx::{AnyPool, Row};

use super::DatabaseDialect;

// ── Record ────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct AccessLogRecord {
    pub ts:                DateTime<Utc>,
    pub request_id:        String,
    pub api_key_id:        String,
    pub model:             String,
    pub endpoint:          String,
    pub provider_url:      String,
    pub upstream_model:    String,
    pub status_code:       u16,
    pub latency_ms:        u64,
    pub streaming:         bool,
    pub ttft_ms:           u64,
    pub tpot_ms:           f64,
    pub prompt_tokens:     u32,
    pub completion_tokens: u32,
    pub total_tokens:      u32,
    pub finish_reason:     String,
}

// ── Migration ─────────────────────────────────────────────────────────────────

pub async fn migrate(pool: &AnyPool, dialect: DatabaseDialect) -> Result<(), sqlx::Error> {
    match dialect {
        DatabaseDialect::Postgres => migrate_pg(pool).await,
        _ => migrate_simple(pool, dialect).await,
    }
}

/// SQLite / MySQL: plain table with index on ts.
async fn migrate_simple(pool: &AnyPool, dialect: DatabaseDialect) -> Result<(), sqlx::Error> {
    let id_col = match dialect {
        DatabaseDialect::MySql  => "id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY",
        DatabaseDialect::Sqlite => "id INTEGER PRIMARY KEY AUTOINCREMENT",
        DatabaseDialect::Postgres => unreachable!(),
    };
    let ts_col = match dialect {
        DatabaseDialect::MySql  => "ts TIMESTAMP(6) NOT NULL",
        DatabaseDialect::Sqlite => "ts TEXT         NOT NULL",
        DatabaseDialect::Postgres => unreachable!(),
    };
    sqlx::query(&format!(
        "CREATE TABLE IF NOT EXISTS access_log (
            {id_col},
            {ts_col},
            request_id        TEXT    NOT NULL DEFAULT '',
            api_key_id        TEXT    NOT NULL DEFAULT '',
            model             TEXT    NOT NULL DEFAULT '',
            endpoint          TEXT    NOT NULL DEFAULT '',
            provider_url      TEXT    NOT NULL DEFAULT '',
            upstream_model    TEXT    NOT NULL DEFAULT '',
            status_code       INTEGER NOT NULL DEFAULT 0,
            latency_ms        INTEGER NOT NULL DEFAULT 0,
            streaming         BOOLEAN NOT NULL DEFAULT FALSE,
            ttft_ms           INTEGER NOT NULL DEFAULT 0,
            tpot_ms           REAL    NOT NULL DEFAULT 0,
            prompt_tokens     INTEGER NOT NULL DEFAULT 0,
            completion_tokens INTEGER NOT NULL DEFAULT 0,
            total_tokens      INTEGER NOT NULL DEFAULT 0,
            finish_reason     TEXT    NOT NULL DEFAULT ''
        )"
    ))
    .execute(pool)
    .await?;

    sqlx::query("CREATE INDEX IF NOT EXISTS idx_access_log_ts ON access_log (ts)")
        .execute(pool)
        .await?;

    // Add column to existing tables (ignore error if column already exists).
    let _ = sqlx::query("ALTER TABLE access_log ADD COLUMN upstream_model TEXT NOT NULL DEFAULT ''")
        .execute(pool)
        .await;

    Ok(())
}

/// PostgreSQL: range-partitioned table by day.
///
/// # Partition strategy
/// - Table is `PARTITION BY RANGE (ts)`.
/// - Each partition covers one calendar day: `access_log_YYYY_MM_DD`.
/// - `id` is sourced from a dedicated sequence so values are globally unique
///   across all partition tables.
/// - PRIMARY KEY is `(id, ts)` — the partition key must be part of every
///   unique constraint in PostgreSQL declarative partitioning.
/// - On startup we pre-create today and the next 3 days.
///   The background writer calls `ensure_pg_partitions` hourly so the
///   partition for the upcoming day is always ready.
async fn migrate_pg(pool: &AnyPool) -> Result<(), sqlx::Error> {
    // Dedicated sequence so id is unique across all partition child tables.
    sqlx::query("CREATE SEQUENCE IF NOT EXISTS access_log_id_seq")
        .execute(pool)
        .await?;

    // Parent partitioned table.  No data lives here directly.
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS access_log (
            id                BIGINT      NOT NULL DEFAULT nextval('access_log_id_seq'),
            ts                TIMESTAMPTZ NOT NULL,
            request_id        TEXT    NOT NULL DEFAULT '',
            api_key_id        TEXT    NOT NULL DEFAULT '',
            model             TEXT    NOT NULL DEFAULT '',
            endpoint          TEXT    NOT NULL DEFAULT '',
            provider_url      TEXT    NOT NULL DEFAULT '',
            upstream_model    TEXT    NOT NULL DEFAULT '',
            status_code       INTEGER NOT NULL DEFAULT 0,
            latency_ms        INTEGER NOT NULL DEFAULT 0,
            streaming         BOOLEAN NOT NULL DEFAULT FALSE,
            ttft_ms           INTEGER NOT NULL DEFAULT 0,
            tpot_ms           REAL    NOT NULL DEFAULT 0,
            prompt_tokens     INTEGER NOT NULL DEFAULT 0,
            completion_tokens INTEGER NOT NULL DEFAULT 0,
            total_tokens      INTEGER NOT NULL DEFAULT 0,
            finish_reason     TEXT    NOT NULL DEFAULT '',
            PRIMARY KEY (id, ts)
        ) PARTITION BY RANGE (ts)",
    )
    .execute(pool)
    .await?;

    // Add column to existing tables (IF NOT EXISTS avoids error on re-runs).
    sqlx::query(
        "ALTER TABLE access_log ADD COLUMN IF NOT EXISTS upstream_model TEXT NOT NULL DEFAULT ''",
    )
    .execute(pool)
    .await?;

    // Global index on ts (automatically inherited by child partitions).
    sqlx::query(
        "CREATE INDEX IF NOT EXISTS idx_access_log_ts ON access_log (ts)",
    )
    .execute(pool)
    .await?;

    // Pre-create today + 3 days ahead.
    ensure_pg_partitions(pool, 3).await?;

    Ok(())
}

// ── PostgreSQL partition management ───────────────────────────────────────────

/// Create daily partitions from today through `days_ahead` days in the future.
/// Idempotent — safe to call repeatedly.
pub async fn ensure_pg_partitions(pool: &AnyPool, days_ahead: u32) -> Result<(), sqlx::Error> {
    let today = Utc::now().date_naive();
    for offset in 0..=days_ahead {
        let date = today + chrono::Duration::days(offset as i64);
        create_pg_partition(pool, date).await?;
    }
    Ok(())
}

async fn create_pg_partition(
    pool: &AnyPool,
    date: chrono::NaiveDate,
) -> Result<(), sqlx::Error> {
    let name  = pg_partition_name(date);
    let start = date.format("%Y-%m-%d 00:00:00+00").to_string();
    let end   = (date + chrono::Duration::days(1)).format("%Y-%m-%d 00:00:00+00").to_string();

    // IF NOT EXISTS on CREATE TABLE ... PARTITION OF is available in PG 11+.
    sqlx::query(&format!(
        "CREATE TABLE IF NOT EXISTS {name} \
         PARTITION OF access_log \
         FOR VALUES FROM ('{start}'::timestamptz) TO ('{end}'::timestamptz)"
    ))
    .execute(pool)
    .await?;

    Ok(())
}

/// Drop all daily partitions whose day is entirely before `cutoff`.
/// Returns the number of partitions dropped.
pub async fn drop_pg_partitions_older_than(
    pool: &AnyPool,
    cutoff: chrono::DateTime<Utc>,
) -> Result<usize, sqlx::Error> {
    let rows = sqlx::query(
        "SELECT c.relname AS tablename
         FROM pg_class c
         JOIN pg_inherits i ON c.oid = i.inhrelid
         JOIN pg_class p    ON i.inhparent = p.oid
         WHERE p.relname = 'access_log'
           AND c.relname ~ '^access_log_[0-9]{4}_[0-9]{2}_[0-9]{2}$'",
    )
    .fetch_all(pool)
    .await?;

    let cutoff_date = cutoff.date_naive();
    let mut dropped = 0_usize;

    for row in rows {
        let name: String = row.try_get("tablename")?;
        if let Some(date) = parse_pg_partition_name(&name) {
            // The partition covers [date, date+1). Drop when date < cutoff_date.
            if date < cutoff_date {
                sqlx::query(&format!("DROP TABLE IF EXISTS {name}"))
                    .execute(pool)
                    .await?;
                tracing::info!("Dropped access_log partition: {name}");
                dropped += 1;
            }
        }
    }
    Ok(dropped)
}

// ── Helpers ───────────────────────────────────────────────────────────────────

fn pg_partition_name(date: chrono::NaiveDate) -> String {
    date.format("access_log_%Y_%m_%d").to_string()
}

/// Parse `access_log_YYYY_MM_DD` → `NaiveDate`.
fn parse_pg_partition_name(name: &str) -> Option<chrono::NaiveDate> {
    // "access_log_2026_06_24" — strip prefix and parse the date part.
    let date_part = name.strip_prefix("access_log_")?;
    // Replace underscores with hyphens to get "2026-06-24".
    let normalized = date_part.replace('_', "-");
    chrono::NaiveDate::parse_from_str(&normalized, "%Y-%m-%d").ok()
}

// ── Cleanup ───────────────────────────────────────────────────────────────────

/// Remove access log records older than `cutoff` from SQLite or MySQL.
/// PostgreSQL uses [`drop_pg_partitions_older_than`] instead.
pub async fn delete_older_than(
    pool: &AnyPool,
    dialect: DatabaseDialect,
    cutoff: chrono::DateTime<Utc>,
) -> Result<u64, sqlx::Error> {
    let cutoff_str = match dialect {
        DatabaseDialect::MySql => cutoff.format("%Y-%m-%d %H:%M:%S%.6f").to_string(),
        _                      => cutoff.to_rfc3339(),
    };
    let result = sqlx::query("DELETE FROM access_log WHERE ts < ?")
        .bind(cutoff_str)
        .execute(pool)
        .await?;
    Ok(result.rows_affected())
}

// ── Batch insert ──────────────────────────────────────────────────────────────

pub async fn insert_batch(pool: &AnyPool, dialect: DatabaseDialect, records: &[AccessLogRecord]) {
    if records.is_empty() {
        return;
    }

    const COLS: usize = 16;
    // PostgreSQL uses positional placeholders ($1, $2, …); MySQL and SQLite use ?.
    // For PostgreSQL the ts column is TIMESTAMPTZ: cast the text parameter explicitly
    // so PostgreSQL's extended query protocol accepts it without a type mismatch.
    let placeholders: String = if let DatabaseDialect::Postgres = dialect {
        records
            .iter()
            .enumerate()
            .map(|(i, _)| {
                let start = i * COLS + 1;
                let mut cols: Vec<String> = (start..start + COLS).map(|n| format!("${n}")).collect();
                cols[0] = format!("${}::timestamptz", start); // ts needs explicit cast
                format!("({})", cols.join(","))
            })
            .collect::<Vec<_>>()
            .join(",")
    } else {
        records
            .iter()
            .map(|_| "(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")
            .collect::<Vec<_>>()
            .join(",")
    };

    let sql = format!(
        "INSERT INTO access_log \
         (ts,request_id,api_key_id,model,endpoint,provider_url,upstream_model,\
          status_code,latency_ms,streaming,ttft_ms,tpot_ms,\
          prompt_tokens,completion_tokens,total_tokens,finish_reason) \
         VALUES {placeholders}"
    );

    let mut q = sqlx::query(&sql);
    for r in records {
        // AnyPool does not implement Encode for DateTime<Utc>; format as a
        // dialect-appropriate string instead.
        let ts_str = match dialect {
            DatabaseDialect::MySql =>
                r.ts.format("%Y-%m-%d %H:%M:%S%.6f").to_string(),
            // Fixed millisecond precision + "+00:00" offset so SQLite text values
            // sort lexicographically *and* match the query bounds exactly (the
            // admin side normalizes incoming bounds to the same canonical form).
            _ =>
                r.ts.to_rfc3339_opts(SecondsFormat::Millis, false),
        };
        q = q
            .bind(ts_str)
            .bind(&r.request_id)
            .bind(&r.api_key_id)
            .bind(&r.model)
            .bind(&r.endpoint)
            .bind(&r.provider_url)
            .bind(&r.upstream_model)
            .bind(r.status_code as i32)
            .bind(r.latency_ms as i64)
            .bind(r.streaming as i32)
            .bind(r.ttft_ms as i64)
            .bind(r.tpot_ms)
            .bind(r.prompt_tokens as i32)
            .bind(r.completion_tokens as i32)
            .bind(r.total_tokens as i32)
            .bind(&r.finish_reason);
    }

    if let Err(e) = q.execute(pool).await {
        tracing::error!("access_log batch insert failed: {e}");
    }
}
