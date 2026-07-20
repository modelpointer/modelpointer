use std::time::Duration;

use sqlx::AnyPool;
use tokio::sync::mpsc;

pub use crate::db::access_log_store::AccessLogRecord;
use crate::db::{DatabaseDialect, access_log_store};

const CHANNEL_SIZE: usize = 4096;
const BATCH_SIZE: usize = 100;
const FLUSH_INTERVAL_SECS: u64 = 1;
const PARTITION_CHECK_SECS: u64 = 3600; // hourly — ensure next day's partition exists

// ── LogSink ───────────────────────────────────────────────────────────────────

/// Cheap-to-clone handle for sending access log records to the background writer.
/// When `tx` is `None` (no database configured) all sends are no-ops.
#[derive(Clone)]
pub struct LogSink {
    tx: Option<mpsc::Sender<AccessLogRecord>>,
}

impl LogSink {
    pub fn noop() -> Self {
        Self { tx: None }
    }

    /// Drop the record silently if the channel is full — access log loss is
    /// acceptable; we must never slow down the request path.
    pub fn try_send(&self, record: AccessLogRecord) {
        if let Some(tx) = &self.tx {
            let _ = tx.try_send(record);
        }
    }
}

// ── LogWriter ─────────────────────────────────────────────────────────────────

/// Owned shutdown handle returned by [`start`].
///
/// The caller must call [`LogWriter::shutdown`] (or drop this value) after the
/// server stops accepting new requests.  Dropping without awaiting is safe but
/// may lose records still in flight; always prefer `shutdown().await` during a
/// clean exit.
pub struct LogWriter {
    /// One extra sender kept alive so the task does not exit before we call
    /// `shutdown`.  Dropping it closes the channel from our side.
    _tx: mpsc::Sender<AccessLogRecord>,
    join: tokio::task::JoinHandle<()>,
}

impl LogWriter {
    /// A no-op writer used when the database is not configured.
    pub fn noop() -> Self {
        // Spawn a task that does nothing so `join` is always valid.
        Self {
            _tx: mpsc::channel(1).0,
            join: tokio::spawn(async {}),
        }
    }

    /// Signal the writer to stop and wait for it to flush all remaining records.
    ///
    /// This drops the internal sender, which closes the channel.  The background
    /// task will drain its buffer and write any buffered records before exiting.
    pub async fn shutdown(self) {
        drop(self._tx); // signal EOF to the task
        let _ = self.join.await;
    }
}

// ── Background writer ─────────────────────────────────────────────────────────

/// Initialise the access log subsystem:
/// 1. Run table migration (creates the table / partitions).
/// 2. Spawn the background writer task.
/// 3. Return a `(LogSink, LogWriter)` pair.
///
/// `LogSink` is cheap to clone and used on the hot request path.
/// `LogWriter` must be kept alive until the server shuts down, then
/// `.shutdown().await` should be called to flush any remaining records.
pub async fn start(
    pool: AnyPool,
    dialect: DatabaseDialect,
) -> Result<(LogSink, LogWriter), sqlx::Error> {
    access_log_store::migrate(&pool, dialect).await?;

    let (tx, rx) = mpsc::channel(CHANNEL_SIZE);
    // Keep an extra sender in LogWriter so the task doesn't exit until
    // shutdown() is called, even if all LogSink clones are dropped first.
    let writer_tx = tx.clone();
    let join = tokio::spawn(writer_task(pool, dialect, rx));

    Ok((
        LogSink { tx: Some(tx) },
        LogWriter {
            _tx: writer_tx,
            join,
        },
    ))
}

async fn writer_task(
    pool: AnyPool,
    dialect: DatabaseDialect,
    mut rx: mpsc::Receiver<AccessLogRecord>,
) {
    let mut buf: Vec<AccessLogRecord> = Vec::with_capacity(BATCH_SIZE);

    let mut flush_interval = tokio::time::interval(Duration::from_secs(FLUSH_INTERVAL_SECS));
    flush_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    // For PostgreSQL: pre-create the next day's partition before it is needed.
    // Other dialects skip this entirely.
    let mut partition_interval = tokio::time::interval(Duration::from_secs(PARTITION_CHECK_SECS));
    partition_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            msg = rx.recv() => {
                match msg {
                    Some(record) => {
                        buf.push(record);
                        if buf.len() >= BATCH_SIZE {
                            access_log_store::insert_batch(&pool, dialect, &buf).await;
                            buf.clear();
                        }
                    }
                    None => break, // all senders dropped — drain and exit
                }
            }

            _ = flush_interval.tick() => {
                if !buf.is_empty() {
                    access_log_store::insert_batch(&pool, dialect, &buf).await;
                    buf.clear();
                }
            }

            _ = partition_interval.tick() => {
                if let DatabaseDialect::Postgres = dialect {
                    if let Err(e) = access_log_store::ensure_pg_partitions(&pool, 3).await {
                        tracing::warn!("Failed to ensure access_log partitions: {e}");
                    }
                }
            }
        }
    }

    // Flush any records buffered at the time of shutdown.
    if !buf.is_empty() {
        access_log_store::insert_batch(&pool, dialect, &buf).await;
    }

    tracing::info!("Access log writer flushed and stopped");
}
