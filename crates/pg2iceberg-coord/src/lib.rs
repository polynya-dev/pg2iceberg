//! Coordinator trait: owns the `_pg2iceberg` schema in the source PG.
//!
//! Mirrors `stream/coordinator_pg.go`. This is the durability boundary —
//! `flushedLSN` may only advance after a successful [`Coordinator::claim_offsets`]
//! call, and the [`CoordCommitReceipt`] enforces that invariant in the type
//! system.
//!
//! ## The receipt invariant
//!
//! Pipeline supplies the LSN it wants to become flushable as part of the
//! [`CommitBatch`]. The coord doesn't validate the LSN — it just preserves it
//! across the PG-commit boundary into the [`CoordCommitReceipt`]. The receipt
//! is non-`Clone`, non-public-construct, and the only way to obtain one is via
//! a `Coordinator` impl in this crate (or a sim impl in `pg2iceberg-sim`,
//! which routes through [`receipt::mint`]). That makes "advance the slot
//! before the coord write commits" a compile error.

pub mod schema;
pub mod sql;

#[cfg(feature = "prod")]
pub mod prod;

use async_trait::async_trait;
use pg2iceberg_core::{Checkpoint, Lsn, TableIdent, WorkerId};
use serde::{Deserialize, Serialize};
use std::time::Duration;
use thiserror::Error;

#[derive(Clone, Debug, Error)]
pub enum CoordError {
    #[error("postgres: {0}")]
    Pg(String),
    #[error("conflict on {table}: {detail}")]
    Conflict { table: TableIdent, detail: String },
    #[error("not found: {0}")]
    NotFound(String),
    #[error("other: {0}")]
    Other(String),
}

pub type Result<T> = std::result::Result<T, CoordError>;

/// One staged Parquet object the pipeline wants registered in `log_index`.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OffsetClaim {
    pub table: TableIdent,
    pub record_count: u64,
    pub byte_size: u64,
    pub s3_path: String,
}

/// Atomic batch of claims plus the LSN that becomes flushable once the batch
/// commits in PG. The pipeline knows the LSN from the transactions it just
/// staged; the coord just carries it across the commit boundary.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CommitBatch {
    pub claims: Vec<OffsetClaim>,
    /// Highest LSN covered by the staged objects in this batch. After
    /// [`Coordinator::claim_offsets`] returns, the pipeline may advance
    /// `flushedLSN` to this value via [`set_flushed_lsn_with`].
    pub flushable_lsn: Lsn,
    /// Marker UUIDs observed in this flush's transactions, with their
    /// commit LSNs. Persisted atomically with the log_index rows so a
    /// crash between flush and marker-write can't drop them. The
    /// materializer reads these via
    /// [`Coordinator::pending_markers_for_table`] after each cycle and
    /// emits Iceberg meta-marker rows; see [`MarkerInfo`] for the
    /// blue-green replica-alignment design.
    #[serde(default)]
    pub markers: Vec<MarkerInfo>,
}

impl CommitBatch {
    /// Construct a marker-less batch — convenience for tests and
    /// callers that don't enable blue-green markers.
    pub fn without_markers(claims: Vec<OffsetClaim>, flushable_lsn: Lsn) -> Self {
        Self {
            claims,
            flushable_lsn,
            markers: Vec::new(),
        }
    }
}

/// One observation of a `_pg2iceberg.markers` INSERT in the source PG's
/// WAL. The pipeline extracts the `uuid` column from the INSERT and
/// pairs it with the containing transaction's commit LSN.
///
/// Markers are the blue-green replica-alignment primitive (see
/// `examples/blue-green/` in the Go reference). When both blue and
/// green pg2iceberg instances see the same marker UUID at equivalent
/// WAL points, each emits a row to its own Iceberg meta-marker table
/// recording the snapshot ID per tracked table at that moment.
/// External `iceberg-diff` then verifies blue/green equivalence at
/// the WAL point identified by the marker UUID.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MarkerInfo {
    pub uuid: String,
    pub commit_lsn: Lsn,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OffsetGrant {
    pub table: TableIdent,
    pub start_offset: u64,
    pub end_offset: u64,
    pub s3_path: String,
}

/// Proof that [`Coordinator::claim_offsets`] committed in PG. Construction is
/// crate-private; impls outside this crate cannot mint one.
#[derive(Debug)]
#[non_exhaustive]
pub struct CoordCommitReceipt {
    pub flushable_lsn: Lsn,
    pub grants: Vec<OffsetGrant>,
    _proof: ReceiptProof,
}

#[derive(Debug)]
struct ReceiptProof(());

impl CoordCommitReceipt {
    fn new(flushable_lsn: Lsn, grants: Vec<OffsetGrant>) -> Self {
        Self {
            flushable_lsn,
            grants,
            _proof: ReceiptProof(()),
        }
    }
}

/// Mint helper for `Coordinator` implementations. Production (PG) and sim
/// impls both route through here; outside callers cannot construct a receipt.
pub mod receipt {
    use super::{CoordCommitReceipt, Lsn, OffsetGrant};

    pub fn mint(flushable_lsn: Lsn, grants: Vec<OffsetGrant>) -> CoordCommitReceipt {
        CoordCommitReceipt::new(flushable_lsn, grants)
    }
}

/// Helper that callers use to advance their flushed-LSN holder. The receipt is
/// consumed (not borrowed) so it can't be reused across batches.
pub fn set_flushed_lsn_with<F>(receipt: CoordCommitReceipt, mut update: F)
where
    F: FnMut(Lsn),
{
    update(receipt.flushable_lsn)
}

/// One row in `log_index`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogEntry {
    pub table: TableIdent,
    pub start_offset: u64,
    pub end_offset: u64,
    pub s3_path: String,
    pub record_count: u64,
    pub byte_size: u64,
    /// Highest source-WAL LSN covered by the events in this entry's
    /// staged Parquet object. Lets the materializer compute "this
    /// cycle covered up to LSN X" so it can emit blue-green
    /// meta-marker rows for any pending marker with `commit_lsn <= X`.
    /// Default `Lsn::ZERO` for backward compat with old log_index
    /// rows that predate the field.
    #[serde(default)]
    pub flushable_lsn: Lsn,
}

#[async_trait]
pub trait Coordinator: Send + Sync {
    /// Atomic in PG: ensure-row → bump `log_seq` → insert per-claim
    /// `log_index` rows for every claim in `batch.claims`. Returns a
    /// [`CoordCommitReceipt`] only if the PG transaction committed.
    ///
    /// Empty batches return `Ok` with an empty grants vector and the supplied
    /// `flushable_lsn`. (Mirrors Go's no-op behavior for empty appends.)
    async fn claim_offsets(&self, batch: &CommitBatch) -> Result<CoordCommitReceipt>;

    async fn read_log(
        &self,
        table: &TableIdent,
        after_offset: u64,
        limit: usize,
    ) -> Result<Vec<LogEntry>>;

    /// Delete log_index rows with `end_offset <= before_offset`, returning
    /// their `s3_path`s so the caller can GC the staged objects.
    async fn truncate_log(&self, table: &TableIdent, before_offset: u64) -> Result<Vec<String>>;

    async fn ensure_cursor(&self, group: &str, table: &TableIdent) -> Result<()>;
    /// Returns `None` if the cursor row doesn't exist; some callers treat that
    /// distinctly from "exists but at -1". The Go impl returns `-1` in both
    /// cases; we surface the difference.
    async fn get_cursor(&self, group: &str, table: &TableIdent) -> Result<Option<i64>>;
    async fn set_cursor(&self, group: &str, table: &TableIdent, to_offset: i64) -> Result<()>;

    async fn register_consumer(&self, group: &str, worker: &WorkerId, ttl: Duration) -> Result<()>;
    async fn unregister_consumer(&self, group: &str, worker: &WorkerId) -> Result<()>;
    /// Returns workers with non-expired heartbeats, sorted by id (matches Go).
    async fn active_consumers(&self, group: &str) -> Result<Vec<WorkerId>>;

    async fn try_lock(&self, table: &TableIdent, worker: &WorkerId, ttl: Duration) -> Result<bool>;
    async fn renew_lock(
        &self,
        table: &TableIdent,
        worker: &WorkerId,
        ttl: Duration,
    ) -> Result<bool>;
    async fn release_lock(&self, table: &TableIdent, worker: &WorkerId) -> Result<()>;

    async fn load_checkpoint(&self) -> Result<Option<Checkpoint>>;
    async fn save_checkpoint(&self, cp: &Checkpoint) -> Result<()>;

    /// Read pending [`MarkerInfo`]s eligible for emission as
    /// meta-marker rows for `table`. A marker is *eligible* iff:
    ///
    /// 1. It exists in `pending_markers` (durable in coord).
    /// 2. It has not been emitted for this table (idempotence).
    /// 3. Every `log_index` entry for `table` with
    ///    `flushable_lsn <= marker.commit_lsn` has
    ///    `end_offset <= cursor` — i.e. the materializer has caught
    ///    up past every event for this table that committed at or
    ///    before the marker.
    ///
    /// (3) covers two cases cleanly: tables with no events at the
    /// marker's WAL point are eligible immediately; tables that
    /// were touched in (or before) the marker's tx are only
    /// eligible after the materializer commits those events.
    ///
    /// Default impl returns empty (marker mode disabled).
    async fn pending_markers_for_table(
        &self,
        table: &TableIdent,
        cursor: i64,
    ) -> Result<Vec<MarkerInfo>> {
        let _ = (table, cursor);
        Ok(Vec::new())
    }

    /// Record that the meta-marker row `(uuid, table)` has been
    /// written to Iceberg. Idempotent. Used by the materializer to
    /// dedup emissions across crashes/replays. Default no-op.
    async fn record_marker_emitted(
        &self,
        uuid: &str,
        table: &TableIdent,
    ) -> Result<()> {
        let _ = (uuid, table);
        Ok(())
    }
}
