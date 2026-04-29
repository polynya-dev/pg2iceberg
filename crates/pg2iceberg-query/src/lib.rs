//! Query-mode pipeline.
//!
//! pg2iceberg's second replication mode: instead of WAL-based logical
//! replication, periodically `SELECT` rows whose watermark column has
//! advanced past a stored cursor, dedup by PK in memory, and write directly
//! to the Iceberg materialized table via `TableWriter` + `Catalog`.
//!
//! Differences from logical mode:
//! - No coord, no staging Parquet, no replication slot. The watermark per
//!   table IS the cursor. Persistence (a future task) goes into the
//!   `_pg2iceberg.checkpoints` table just like logical mode's checkpoint.
//! - Every output row is treated as `Op::Insert`. PK dedup in [`Buffer`]
//!   collapses repeats; `promote_re_inserts` against `FileIndex` later
//!   demotes prior-existing PKs to `Update` so MoR readers see the
//!   replacement, not duplicates.
//! - Deletes can't be observed via watermark polling (no row is left with
//!   a "deletion" watermark). Query mode is upsert-only; users who need
//!   delete semantics use logical mode.
//!
//! Mirrors `query/poller.go` + `query/buffer.go` + `query/pipeline.go`
//! (the orchestrator part). Phase 10 first-cut deferrals: watermark
//! persistence, snapshot bootstrap (`pg_export_snapshot` for initial-load
//! consistency), reconnect/retry logic.

pub mod buffer;
pub mod pipeline;
pub mod runtime;

use async_trait::async_trait;
use pg2iceberg_core::{PgValue, Row, TableIdent};
use thiserror::Error;

pub use buffer::Buffer;
pub use pipeline::{QueryPipeline, QueryPipelineError};
pub use runtime::{run_query_lifecycle, QueryLifecycle, QueryLifecycleError};

#[derive(Debug, Error)]
pub enum QueryError {
    #[error("source: {0}")]
    Source(String),
    #[error("unsupported watermark type for column {col}: {value:?}")]
    UnsupportedWatermark { col: String, value: PgValue },
}

pub type Result<T> = std::result::Result<T, QueryError>;

/// What the query-mode poller needs from the source DB. Production wraps a
/// `tokio-postgres` connection issuing `SELECT * FROM t WHERE wm > $1
/// ORDER BY wm ASC LIMIT $2`. The sim impl on `SimPostgres` does the same
/// filter + sort + truncate in-memory.
///
/// `after` is `None` on the first poll (returns rows from the start).
/// `limit` caps the number of rows returned per call so the poller can
/// loop without OOM-ing on a freshly-bootstrapped large table. `None` =
/// no limit (caller asserts memory is fine).
#[async_trait]
pub trait WatermarkSource: Send + Sync {
    async fn read_after(
        &self,
        ident: &TableIdent,
        watermark_col: &str,
        after: Option<&PgValue>,
        limit: Option<usize>,
    ) -> Result<Vec<Row>>;
}

/// Total ordering on the subset of `PgValue` types we accept as watermark
/// columns. Mirrors the Go reference: numeric and timestamp/date columns
/// are valid; everything else is rejected at the source layer.
pub fn watermark_compare(
    a: &PgValue,
    b: &PgValue,
) -> std::result::Result<std::cmp::Ordering, QueryError> {
    match (a, b) {
        (PgValue::Int2(x), PgValue::Int2(y)) => Ok(x.cmp(y)),
        (PgValue::Int4(x), PgValue::Int4(y)) => Ok(x.cmp(y)),
        (PgValue::Int8(x), PgValue::Int8(y)) => Ok(x.cmp(y)),
        (PgValue::Date(x), PgValue::Date(y)) => Ok(x.0.cmp(&y.0)),
        (PgValue::Timestamp(x), PgValue::Timestamp(y)) => Ok(x.0.cmp(&y.0)),
        (PgValue::TimestampTz(x), PgValue::TimestampTz(y)) => Ok(x.0.cmp(&y.0)),
        // Distinct or unsupported types compare as Equal so the caller
        // doesn't accidentally advance the watermark across types.
        (a, b) if std::mem::discriminant(a) != std::mem::discriminant(b) => {
            Err(QueryError::UnsupportedWatermark {
                col: "<watermark>".into(),
                value: a.clone(),
            })
        }
        (a, _) => Err(QueryError::UnsupportedWatermark {
            col: "<watermark>".into(),
            value: a.clone(),
        }),
    }
}
