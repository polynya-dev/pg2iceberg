//! Initial-snapshot phase: bootstrap a fresh pg2iceberg deployment by
//! reading every row from each tracked table at a known LSN, staging them
//! through the same pipeline as logical replication, then advancing the
//! slot past that LSN so live replication picks up exactly where the
//! snapshot left off.
//!
//! Mirrors `snapshot/` and `logical/logical.go:507-519` from the Go
//! reference, but skips the CTID-page-chunked parallel read (deferred to
//! Phase 11.5 — useful when source tables are huge, irrelevant for the sim).
//!
//! ## Why this exists
//!
//! pg2iceberg is a mirror, not a CDC tool. The Iceberg side must reflect
//! 100% of PG state, so we cannot start replication from "now and onwards"
//! — that would lose every row inserted before the slot was created. The
//! snapshot phase fills the gap.
//!
//! ## Handoff timing
//!
//! 1. Create publication + replication slot (slot's `restart_lsn` is now
//!    set to the LSN at slot-creation time, call it `K`).
//! 2. Read each table's current rows at the source's "current LSN" `N` —
//!    `N >= K` always.
//! 3. Stage the rows through the pipeline as a synthetic transaction with
//!    `commit_lsn = N`. The receipt-gated `flushed_lsn` advances to `N`.
//! 4. Caller acks the slot at `N` via `send_standby`. `restart_lsn` jumps
//!    to `N`, so future replication reads start at `N`.
//!
//! Any WAL events between `K` and `N` are *covered* by the snapshot (rows
//! read at `N` reflect their effects), so skipping them is correct, not
//! lossy.

use async_trait::async_trait;
use pg2iceberg_coord::Coordinator;
use pg2iceberg_core::{
    ChangeEvent, ColumnName, Lsn, Op, Row, TableIdent,
    TableSchema, Timestamp,
};
use pg2iceberg_iceberg::{pk_key, Catalog};
use pg2iceberg_logical::{Pipeline, PipelineError};
use pg2iceberg_pg::DecodedMessage;
use std::collections::BTreeMap;
use std::sync::Arc;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum SnapshotError {
    #[error("source: {0}")]
    Source(String),
    #[error("pipeline: {0}")]
    Pipeline(#[from] PipelineError),
}

pub type Result<T> = std::result::Result<T, SnapshotError>;

/// What the snapshotter needs from the source PG. Production wraps a
/// `tokio-postgres` connection inside a `BEGIN ISOLATION LEVEL REPEATABLE
/// READ; SELECT pg_export_snapshot()` transaction; the sim impl is on
/// `SimPostgres`.
///
/// Both `snapshot_lsn` and `read_chunk` should observe a consistent view —
/// any rows missed will be lost (the slot has already advanced past them
/// by the time live replication kicks in).
///
/// Chunked reads keep peak memory bounded for large tables. Implementations
/// must:
/// - Return rows sorted by canonical PK ASC.
/// - Be stable across calls (same snapshot view).
/// - Honor `after_pk_key` as a strict lower bound; rows whose canonical PK
///   key (JSON form, see `pg2iceberg_iceberg::pk_key`) is `<=` the bound
///   must be excluded.
/// - Cap output at `chunk_size`. Returning fewer rows is allowed only at
///   end-of-table.
///
/// `after_pk_key` is a string (rather than a `Row`) because resumable
/// snapshot persists the key into the checkpoint — it can't reconstruct a
/// `Row` without the schema, and a string is the canonical wire form.
#[async_trait]
pub trait SnapshotSource: Send + Sync {
    async fn snapshot_lsn(&self) -> Result<Lsn>;
    async fn read_chunk(
        &self,
        ident: &TableIdent,
        chunk_size: usize,
        after_pk_key: Option<&str>,
    ) -> Result<Vec<Row>>;
}

/// Pseudo-xid base for synthetic snapshot transactions. Real PG xids are
/// monotonic from 1; we reserve a high range so collisions are impossible.
/// Each table gets `BASE + table_index`, so all events for a table land in
/// the same buffer.
pub const SNAPSHOT_XID_BASE: u32 = 0xFFFF_FF00;

/// `_pg2iceberg.markers` is a **blue-green replica alignment**
/// feature in the Go reference (see `examples/blue-green/` in the Go
/// repo). It is *not* the snapshot↔CDC fence — that lives in
/// `pg2iceberg_validate::run_logical_lifecycle` and uses
/// `pg_current_wal_lsn()` directly.
///
/// ## How markers work
///
/// 1. Both blue and green PG clusters share the schema (including
///    a `_pg2iceberg.markers` table) and one is logically
///    replicating to the other. Both clusters' `pg2iceberg`
///    instances include `_pg2iceberg.markers` in their publication.
/// 2. An operator inserts a row `(uuid, ...)` into blue's
///    `_pg2iceberg.markers`. PG logical replication ships the
///    insert to green at the same WAL position.
/// 3. Each pg2iceberg instance, on observing a marker INSERT in
///    its WAL, flushes the containing transaction and triggers a
///    materializer cycle. After the cycle succeeds, it writes
///    `(marker_uuid, table_name, iceberg_snapshot_id)` rows into a
///    *separate* Iceberg-side meta-markers table (in
///    `meta_namespace`, e.g. `_pg2iceberg_blue.markers`).
/// 4. An external diff tool (`iceberg-diff`) joins the two
///    Iceberg-side meta-markers tables by `marker_uuid` and
///    compares blue's snapshot vs green's snapshot per table. If
///    EQUAL, the two replicas have produced byte-equal data at
///    that WAL point — safe to cut over.
///
/// ## Status in pg2iceberg-rust
///
/// Not implemented. Adding it requires:
/// - `sink.meta_namespace` config field.
/// - PG-side `_pg2iceberg.markers` table creation (in coord
///   migrate, only if marker mode enabled).
/// - Iceberg meta-markers table creation per-instance.
/// - Pipeline-side detection of marker INSERTs in the WAL stream.
/// - Materializer-side flush trigger + meta-markers row insertion
///   keyed by `(marker_uuid, table, snapshot_id)`.
pub const MARKER_TABLE_NAME: &str = "_pg2iceberg.markers";

/// Currently unused. Reserved for the marker feature above; Go's
/// version doesn't have a kind enum (markers are just UUIDs in PG),
/// so this is likely to be removed when the feature is properly
/// ported.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MarkerKind {
    Pre,
    Post,
}

/// Default chunk size for [`run_snapshot`]. Trades off peak memory against
/// coord-write amplification. ~1k rows per chunk is the same default as
/// the Go reference's `flush_rows`.
pub const DEFAULT_CHUNK_SIZE: usize = 1024;

/// Drive a fresh pipeline through the initial-snapshot phase using the
/// default chunk size.
///
/// Returns the LSN the caller should `send_standby` to so the replication
/// slot picks up live replication from the right point. The caller should
/// also drive the materializer at least once after this returns to flush
/// snapshot rows out to Iceberg.
pub async fn run_snapshot<S, C>(
    source: &S,
    coord: Arc<dyn Coordinator>,
    schemas: &[TableSchema],
    pipeline: &mut Pipeline<C>,
) -> Result<Lsn>
where
    S: SnapshotSource + ?Sized,
    C: Coordinator + ?Sized,
{
    run_snapshot_chunked(source, coord, schemas, pipeline, DEFAULT_CHUNK_SIZE).await
}

/// Like [`run_snapshot`] but with an explicit `chunk_size`. Each chunk
/// is staged + coord-committed independently; peak memory is bounded
/// by `chunk_size` rows. The materializer later coalesces all chunks
/// into one Iceberg snapshot per table.
///
/// Resumability: each chunk persists a per-table cursor in
/// `_pg2iceberg.snapshot_progress`, and a completed table writes a
/// row to `_pg2iceberg.tables`. A subsequent call against the same
/// coord will skip already-complete tables and resume mid-table from
/// the last saved cursor.
pub async fn run_snapshot_chunked<S, C>(
    source: &S,
    coord: Arc<dyn Coordinator>,
    schemas: &[TableSchema],
    pipeline: &mut Pipeline<C>,
    chunk_size: usize,
) -> Result<Lsn>
where
    S: SnapshotSource + ?Sized,
    C: Coordinator + ?Sized,
{
    Snapshotter::new(coord)
        .with_chunk_size(chunk_size)
        .run(source, schemas, pipeline)
        .await
}

/// Resumable snapshot driver.
///
/// On `run`, loads each table's per-row resume cursor from
/// `_pg2iceberg.snapshot_progress`; for each table, resumes reading
/// at PKs strictly greater than the saved key. After every chunk,
/// stamps the new cursor so a crash mid-snapshot resumes from the
/// right place. When a table finishes, the cursor row is dropped
/// and `mark_table_snapshot_complete` upserts the per-table flag in
/// `_pg2iceberg.tables`.
///
/// Replaces the old `Checkpoint`-driven design — see the module-level
/// docs for the rationale.
pub struct Snapshotter {
    coord: Arc<dyn Coordinator>,
    chunk_size: usize,
    /// Optional per-table `pg_class.oid`s captured at snapshot time.
    /// Stamped onto the `_pg2iceberg.tables` row when a table
    /// completes; used downstream by the `TableIdentityChanged`
    /// startup invariant. Empty in sim runs (oid 0 means "unknown";
    /// the invariant skips on 0).
    table_oids: BTreeMap<TableIdent, u32>,
}

impl Snapshotter {
    pub fn new(coord: Arc<dyn Coordinator>) -> Self {
        Self {
            coord,
            chunk_size: DEFAULT_CHUNK_SIZE,
            table_oids: BTreeMap::new(),
        }
    }

    pub fn with_chunk_size(mut self, n: usize) -> Self {
        assert!(n > 0, "chunk_size must be > 0");
        self.chunk_size = n;
        self
    }

    /// Stamp per-table `pg_class.oid`s. Callers that have access to
    /// PG (the lifecycle, `run_snapshot_phase`) supply this so each
    /// table's `_pg2iceberg.tables` row carries the oid for the
    /// `TableIdentityChanged` invariant. Tests that don't care can
    /// skip — defaults to empty.
    pub fn with_table_oids(mut self, oids: BTreeMap<TableIdent, u32>) -> Self {
        self.table_oids = oids;
        self
    }

    /// Run the snapshot to completion (all tables, all chunks).
    pub async fn run<S, C>(
        &self,
        source: &S,
        schemas: &[TableSchema],
        pipeline: &mut Pipeline<C>,
    ) -> Result<Lsn>
    where
        S: SnapshotSource + ?Sized,
        C: Coordinator + ?Sized,
    {
        self.run_chunks(source, schemas, pipeline, None).await
    }

    /// Run at most `max_chunks` chunks across all tables, then return.
    /// `None` = unlimited. Used by tests to simulate a mid-snapshot
    /// crash: after `run_chunks(.., Some(N))`, drop the pipeline +
    /// Snapshotter, recreate them, and call `run` to finish — the
    /// per-table progress rows drive correct resumption.
    pub async fn run_chunks<S, C>(
        &self,
        source: &S,
        schemas: &[TableSchema],
        pipeline: &mut Pipeline<C>,
        max_chunks: Option<usize>,
    ) -> Result<Lsn>
    where
        S: SnapshotSource + ?Sized,
        C: Coordinator + ?Sized,
    {
        let snap_lsn = source.snapshot_lsn().await?;

        // Per-table snapshot loop. For each schema, read its progress
        // cursor from coord, drive chunks until empty, stamp progress
        // after each, mark the table complete on finish. A
        // mid-snapshot fault leaves only the in-progress table's row
        // in `snapshot_progress`; on resume the inner loop picks up
        // at the saved PK.
        let mut total_chunks = 0usize;
        'tables: for (i, schema) in schemas.iter().enumerate() {
            let xid = SNAPSHOT_XID_BASE.wrapping_add(i as u32);
            let pk_cols: Vec<ColumnName> = schema
                .primary_key_columns()
                .map(|c| ColumnName(c.name.clone()))
                .collect();
            if pk_cols.is_empty() {
                return Err(SnapshotError::Source(format!(
                    "table {} has no primary key; pg2iceberg requires one",
                    schema.ident
                )));
            }

            // Skip tables already complete from a prior pass. Cheap
            // single-row read; avoids re-reading the source for
            // already-snapshotted data.
            if let Some(state) = self.coord.table_state(&schema.ident).await.map_err(
                |e| SnapshotError::Source(format!("table_state: {e}")),
            )? {
                if state.snapshot_complete {
                    continue;
                }
            }

            let mut last_pk_key: Option<String> = self
                .coord
                .snapshot_progress(&schema.ident)
                .await
                .map_err(|e| SnapshotError::Source(format!("snapshot_progress: {e}")))?;

            loop {
                if let Some(cap) = max_chunks {
                    if total_chunks >= cap {
                        break 'tables;
                    }
                }

                let chunk = source
                    .read_chunk(&schema.ident, self.chunk_size, last_pk_key.as_deref())
                    .await?;
                if chunk.is_empty() {
                    // Table done — drop the resume cursor and stamp
                    // the per-table completion row. Order matters: we
                    // clear progress *first* so a crash between the
                    // two writes leaves the table marked
                    // not-yet-complete (snapshotter retries cleanly)
                    // rather than complete-without-cursor (no-op
                    // resume; correct).
                    self.coord
                        .clear_snapshot_progress(&schema.ident)
                        .await
                        .map_err(|e| {
                            SnapshotError::Source(format!("clear_snapshot_progress: {e}"))
                        })?;
                    let oid = self.table_oids.get(&schema.ident).copied().unwrap_or(0);
                    self.coord
                        .mark_table_snapshot_complete(&schema.ident, oid, snap_lsn)
                        .await
                        .map_err(|e| {
                            SnapshotError::Source(format!(
                                "mark_table_snapshot_complete: {e}"
                            ))
                        })?;
                    break;
                }

                let last_in_chunk = chunk.last().unwrap();
                let new_key = pk_key(last_in_chunk, &pk_cols);

                if let Some(prev) = last_pk_key.as_deref() {
                    if new_key.as_str() <= prev {
                        return Err(SnapshotError::Source(format!(
                            "snapshot source did not advance past PK {prev} for table {}; got {new_key}",
                            schema.ident
                        )));
                    }
                }

                pipeline
                    .process(DecodedMessage::Begin {
                        final_lsn: snap_lsn,
                        xid,
                    })
                    .await?;
                for row in chunk {
                    pipeline
                        .process(DecodedMessage::Change(ChangeEvent {
                            table: schema.ident.clone(),
                            op: Op::Insert,
                            lsn: snap_lsn,
                            commit_ts: Timestamp(0),
                            xid: Some(xid),
                            before: None,
                            after: Some(row),
                            unchanged_cols: vec![],
                        }))
                        .await?;
                }
                pipeline
                    .process(DecodedMessage::Commit {
                        commit_lsn: snap_lsn,
                        xid,
                    })
                    .await?;
                pipeline.flush().await?;

                // Chunk fully durable in coord. Stamp the resume
                // cursor *after* the flush — if we crash before this
                // write, resume re-reads the chunk (idempotent: the
                // materializer's fold-by-PK absorbs the duplicate
                // staged events).
                last_pk_key = Some(new_key.clone());
                self.coord
                    .set_snapshot_progress(&schema.ident, &new_key)
                    .await
                    .map_err(|e| SnapshotError::Source(format!("set_snapshot_progress: {e}")))?;
                total_chunks += 1;
            }
        }
        Ok(snap_lsn)
    }
}


/// Outcome of [`run_snapshot_phase`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SnapshotPhaseOutcome {
    /// Snapshot was skipped (already complete in checkpoint, or every
    /// configured table is in `skip_idents`).
    Skipped,
    /// Snapshot ran (or resumed) and reached completion. The snapshot
    /// LSN is the value the caller should `send_standby` to.
    Completed { snapshot_lsn: Lsn },
}

/// **Production snapshot phase orchestration**, lifted from the binary
/// so the fault-injection DST can exercise the *exact* logic the
/// binary runs.
///
/// Steps:
/// 1. Load checkpoint; return `Skipped` if `snapshot_state ==
///    Complete`.
/// 2. Filter `schemas` by `skip_idents` (per-table `skip_snapshot`
///    opt-out).
/// 3. Run [`Snapshotter`] (resumable surface — progress persists into
///    `Checkpoint::snapshot_progress` after each chunk, so a
///    mid-snapshot crash + restart resumes at the next chunk).
/// 4. Defensive `pipeline.flush()` to drain.
/// 5. Persist `snapshot_state: Complete` + `flushed_lsn` +
///    `tracked_tables` + cleared progress map.
///
/// **What this function does not do** (caller's responsibility):
/// - `stream.send_standby(snap_lsn, snap_lsn)` to advance the slot.
/// - `materializer.cycle()` to publish snapshot rows to Iceberg.
/// These are tied to the caller's replication-stream and materializer
/// types, which the snapshot crate doesn't depend on.
///
/// **Resumability:** uses `Snapshotter` (not the free `run_snapshot`)
/// so a fault during chunk K leaves the checkpoint at progress K-1.
/// On restart, the next call re-loads progress and resumes — no
/// re-staging of completed chunks. The fault-DST proves this:
/// `binary_snapshot_phase_resumes_after_mid_chunk_blob_put_fault`.
#[allow(clippy::too_many_arguments)]
pub async fn run_snapshot_phase<S, C>(
    source: &S,
    coord: Arc<dyn Coordinator>,
    schemas: &[TableSchema],
    skip_idents: &std::collections::BTreeSet<TableIdent>,
    table_oids: &std::collections::BTreeMap<TableIdent, u32>,
    pipeline: &mut Pipeline<C>,
    chunk_size: usize,
) -> Result<SnapshotPhaseOutcome>
where
    S: SnapshotSource + ?Sized,
    C: Coordinator + ?Sized,
{
    // Filter out operator-supplied skip-list entries.
    let to_snapshot: Vec<TableSchema> = schemas
        .iter()
        .filter(|s| !skip_idents.contains(&s.ident))
        .cloned()
        .collect();
    if to_snapshot.is_empty() {
        return Ok(SnapshotPhaseOutcome::Skipped);
    }

    // Skip the phase entirely when every relevant table is already
    // marked complete in `_pg2iceberg.tables`. Per-table reads — one
    // round-trip per table at startup, cheap.
    let mut all_complete = true;
    for s in &to_snapshot {
        let state = coord
            .table_state(&s.ident)
            .await
            .map_err(|e| SnapshotError::Source(format!("table_state: {e}")))?;
        if !state.map(|t| t.snapshot_complete).unwrap_or(false) {
            all_complete = false;
            break;
        }
    }
    if all_complete {
        return Ok(SnapshotPhaseOutcome::Skipped);
    }

    // Resumable snapshot. The Snapshotter persists per-chunk progress
    // (one row per in-flight table in `_pg2iceberg.snapshot_progress`)
    // and stamps each table's completion row in `_pg2iceberg.tables`
    // when it finishes — so this function doesn't have to do any
    // post-pass coord writes itself. A mid-snapshot fault leaves the
    // world in a state where the next call picks up exactly where the
    // previous one left off.
    let snapshotter = Snapshotter::new(coord.clone())
        .with_chunk_size(chunk_size)
        .with_table_oids(table_oids.clone());
    let snap_lsn = snapshotter.run(source, &to_snapshot, pipeline).await?;

    // Defensive final flush — the Snapshotter::run loop already
    // flushes after each chunk, but a final pass is cheap insurance
    // that every staged chunk is durable in coord before the caller
    // acks the slot.
    pipeline.flush().await?;

    Ok(SnapshotPhaseOutcome::Completed {
        snapshot_lsn: snap_lsn,
    })
}

/// Type bound the snapshotter wants on the catalog. Currently unused
/// directly here — the pipeline carries its own catalog reference for the
/// materializer — but exposing it lets future versions of `run_snapshot`
/// validate that the catalog table exists before reading from PG.
#[doc(hidden)]
pub trait SnapshotCatalog: Catalog {}
impl<C: Catalog> SnapshotCatalog for C {}
