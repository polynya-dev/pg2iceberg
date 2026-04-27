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
    ChangeEvent, Checkpoint, ColumnName, Lsn, Mode, Op, Row, TableIdent,
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
    schemas: &[TableSchema],
    pipeline: &mut Pipeline<C>,
) -> Result<Lsn>
where
    S: SnapshotSource + ?Sized,
    C: Coordinator + ?Sized,
{
    run_snapshot_chunked(source, schemas, pipeline, DEFAULT_CHUNK_SIZE).await
}

/// Like [`run_snapshot`] but with an explicit `chunk_size`. Each chunk is
/// staged + coord-committed independently; peak memory is bounded by
/// `chunk_size` rows. The materializer later coalesces all chunks into one
/// Iceberg snapshot.
///
/// Crash recovery: if the process dies mid-snapshot, the pipeline restarts
/// from the slot's `restart_lsn` (still at the pre-snapshot point) and the
/// snapshot replays from chunk 0. Idempotent because chunked replay
/// produces the same staged Parquet for the same source rows. (Resumability
/// — skipping already-completed chunks — is a Phase 11.5 task that needs
/// per-table progress tracked in the checkpoint.)
pub async fn run_snapshot_chunked<S, C>(
    source: &S,
    schemas: &[TableSchema],
    pipeline: &mut Pipeline<C>,
    chunk_size: usize,
) -> Result<Lsn>
where
    S: SnapshotSource + ?Sized,
    C: Coordinator + ?Sized,
{
    // Non-resumable variant: just runs a single pass with empty
    // progress and no incremental saving.
    let snap_lsn = source.snapshot_lsn().await?;
    let progress = SnapshotProgressMap::default();
    let (_chunks, _end) = snapshot_one_pass(
        source, schemas, pipeline, chunk_size, snap_lsn, progress, None, None,
    )
    .await?;
    Ok(snap_lsn)
}

/// Per-table progress map (canonical PK key of the last staged row).
/// Progress map keyed by `"<namespace>.<name>"` (matches the
/// `Checkpoint::snapshot_progress` wire format). Centralized helper
/// `key_for(ident)` builds the lookup key.
type SnapshotProgressMap = BTreeMap<String, String>;

/// Build the canonical "namespace.name" key used in `snapshot_progress`
/// + `snapshoted_tables` maps. Mirrors `TableIdent::Display`.
fn key_for(ident: &TableIdent) -> String {
    ident.to_string()
}

/// Resumable snapshot driver.
///
/// On `run`, loads `Checkpoint::snapshot_progress` from the coord; for each
/// table, resumes reading at PKs strictly greater than the saved key. After
/// each chunk, persists updated progress so a crash mid-snapshot resumes
/// from the right place. When all tables complete, clears their progress
/// entries.
///
/// Phase 11 finish surface — the existing `run_snapshot` /
/// `run_snapshot_chunked` free functions stay for tests and uses that
/// don't care about resumability.
pub struct Snapshotter {
    coord: Arc<dyn Coordinator>,
    chunk_size: usize,
}

impl Snapshotter {
    pub fn new(coord: Arc<dyn Coordinator>) -> Self {
        Self {
            coord,
            chunk_size: DEFAULT_CHUNK_SIZE,
        }
    }

    pub fn with_chunk_size(mut self, n: usize) -> Self {
        assert!(n > 0, "chunk_size must be > 0");
        self.chunk_size = n;
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
    /// `None` = unlimited. Used by tests to simulate a mid-snapshot crash:
    /// after `run_chunks(.., Some(N))`, drop the pipeline + Snapshotter,
    /// recreate them, and call `run` to finish — checkpoint progress
    /// drives correct resumption.
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

        // Load existing checkpoint progress. Pass `0` for
        // connected_system_id — the snapshotter doesn't have a PG
        // client handle here; the lifecycle's startup validation is
        // what verifies cluster fingerprint before we get here.
        let cp = self
            .coord
            .load_checkpoint(0)
            .await
            .map_err(|e| SnapshotError::Source(format!("load_checkpoint: {e}")))?;
        let progress = cp
            .as_ref()
            .map(|c| c.snapshot_progress.clone())
            .unwrap_or_default();

        // The single mutable Checkpoint we thread through every save
        // in this pass. After each successful save, `seal` has bumped
        // its `revision` to match the on-disk row, so the next save's
        // OCC predicate matches. Cloning `base_cp` per-save (the
        // earlier shape) loses that revision and the second save
        // returns ConcurrentUpdate.
        let mut cp_state = cp.unwrap_or_else(|| Checkpoint::fresh(Mode::Logical));

        // Pass the coord through to snapshot_one_pass so progress is
        // persisted *after every successful chunk*, not just at
        // end-of-pass. Without this, a fault mid-chunk loses every
        // chunk's progress and the resume restarts from chunk 0 —
        // which is exactly the bug fault-DST surfaced.
        let pass = snapshot_one_pass(
            source,
            schemas,
            pipeline,
            self.chunk_size,
            snap_lsn,
            progress,
            max_chunks,
            Some((self.coord.as_ref(), &mut cp_state)),
        )
        .await;

        // The pass may have errored mid-chunk; in that case
        // snapshot_one_pass has already persisted progress through
        // the last successful chunk before propagating the error.
        // Just propagate.
        let (_chunks, end_progress) = pass?;

        // End-of-pass save: marks Complete iff every table this pass
        // had finished. Per-table tracking lets a future "add another
        // table" pass snapshot only the new one without losing
        // existing per-table state.
        cp_state.snapshot_progress = end_progress;
        for s in schemas {
            if !cp_state.snapshot_progress.contains_key(&key_for(&s.ident)) {
                cp_state
                    .snapshoted_tables
                    .insert(key_for(&s.ident), true);
            }
        }
        cp_state.snapshot_complete = cp_state.snapshot_progress.is_empty();
        self.coord
            .save_checkpoint(&mut cp_state)
            .await
            .map_err(|e| SnapshotError::Source(format!("save_checkpoint: {e}")))?;

        Ok(snap_lsn)
    }
}

/// Drive the actual snapshot loop. Shared by both the free-function
/// entry point and `Snapshotter`. When `incremental_saver` is `Some`,
/// the loop persists progress to the coord *after every successful
/// chunk*, so a fault between chunks doesn't lose previous progress.
/// The free `run_snapshot` passes `None` (non-resumable, single-pass).
///
/// On error, this function persists whatever progress was made before
/// the error (when an `incremental_saver` is provided) and *then*
/// propagates the error. The caller's resume picks up at the next
/// chunk.
async fn snapshot_one_pass<'a, S, C>(
    source: &S,
    schemas: &[TableSchema],
    pipeline: &mut Pipeline<C>,
    chunk_size: usize,
    snap_lsn: Lsn,
    progress: SnapshotProgressMap,
    max_chunks: Option<usize>,
    incremental_saver: Option<(&'a dyn Coordinator, &'a mut Checkpoint)>,
) -> Result<(usize, SnapshotProgressMap)>
where
    S: SnapshotSource + ?Sized,
    C: Coordinator + ?Sized,
{
    assert!(chunk_size > 0, "chunk_size must be > 0");

    let mut total_chunks = 0usize;
    let mut state = progress;
    // Split the saver up-front: `run_chunk_loop` needs a mutable
    // borrow of the Checkpoint, but the error-path save below also
    // needs one. We re-borrow through an Option that we replace each
    // pass.
    let (saver_coord, mut saver_cp): (Option<&dyn Coordinator>, Option<&mut Checkpoint>) =
        match incremental_saver {
            Some((c, cp)) => (Some(c), Some(cp)),
            None => (None, None),
        };

    let res = run_chunk_loop(
        source,
        schemas,
        pipeline,
        chunk_size,
        snap_lsn,
        &mut state,
        &mut total_chunks,
        max_chunks,
        saver_coord,
        saver_cp.as_deref_mut(),
    )
    .await;

    // On any error, save partial progress before propagating so the
    // next call resumes at the next chunk. Best-effort: a save failure
    // here gets swallowed (the original error is more interesting).
    if res.is_err() {
        if let (Some(coord), Some(cp)) = (saver_coord, saver_cp.as_deref_mut()) {
            cp.snapshot_progress = state.clone();
            cp.snapshot_complete = state.is_empty();
            let _ = coord.save_checkpoint(cp).await;
        }
    }

    res?;
    Ok((total_chunks, state))
}

#[allow(clippy::too_many_arguments)]
async fn run_chunk_loop<S, C>(
    source: &S,
    schemas: &[TableSchema],
    pipeline: &mut Pipeline<C>,
    chunk_size: usize,
    snap_lsn: Lsn,
    state: &mut SnapshotProgressMap,
    total_chunks: &mut usize,
    max_chunks: Option<usize>,
    saver_coord: Option<&dyn Coordinator>,
    mut saver_cp: Option<&mut Checkpoint>,
) -> Result<()>
where
    S: SnapshotSource + ?Sized,
    C: Coordinator + ?Sized,
{
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

        let key = key_for(&schema.ident);
        let mut last_pk_key: Option<String> = state.get(&key).cloned();

        loop {
            if let Some(cap) = max_chunks {
                if *total_chunks >= cap {
                    break 'tables;
                }
            }

            let chunk = source
                .read_chunk(&schema.ident, chunk_size, last_pk_key.as_deref())
                .await?;
            if chunk.is_empty() {
                state.remove(&key);
                // Persist the table-done state so a subsequent call
                // doesn't try to re-read this table from scratch.
                if let (Some(coord), Some(cp)) =
                    (saver_coord, saver_cp.as_deref_mut())
                {
                    cp.snapshot_progress = state.clone();
                    cp.snapshoted_tables.insert(key.clone(), true);
                    cp.snapshot_complete = state.is_empty();
                    coord
                        .save_checkpoint(cp)
                        .await
                        .map_err(|e| SnapshotError::Source(format!("save_checkpoint: {e}")))?;
                }
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

            // Chunk fully durable in coord. Update state + persist.
            last_pk_key = Some(new_key.clone());
            state.insert(key.clone(), new_key);
            *total_chunks += 1;

            if let (Some(coord), Some(cp)) =
                (saver_coord, saver_cp.as_deref_mut())
            {
                cp.snapshot_progress = state.clone();
                cp.snapshot_complete = false;
                coord
                    .save_checkpoint(cp)
                    .await
                    .map_err(|e| SnapshotError::Source(format!("save_checkpoint: {e}")))?;
            }
        }
    }
    Ok(())
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
    let cp_pre = coord
        .load_checkpoint(0)
        .await
        .map_err(|e| SnapshotError::Source(format!("load_checkpoint: {e}")))?;
    if cp_pre.as_ref().map(|c| c.snapshot_complete).unwrap_or(false) {
        return Ok(SnapshotPhaseOutcome::Skipped);
    }

    let to_snapshot: Vec<TableSchema> = schemas
        .iter()
        .filter(|s| !skip_idents.contains(&s.ident))
        .cloned()
        .collect();
    if to_snapshot.is_empty() {
        return Ok(SnapshotPhaseOutcome::Skipped);
    }

    // Resumable snapshot. Persists progress per chunk in
    // Checkpoint::snapshot_progress; a mid-snapshot fault + restart
    // resumes at the next chunk.
    let snapshotter = Snapshotter::new(coord.clone()).with_chunk_size(chunk_size);
    let snap_lsn = snapshotter
        .run(source, &to_snapshot, pipeline)
        .await?;

    // Defensive: ensure every staged chunk is durable in coord before
    // we ack the slot. The Snapshotter::run loop already calls
    // pipeline.flush() after each chunk, so this is usually a no-op,
    // but a final flush is cheap insurance.
    pipeline.flush().await?;

    // Persist completion. The Snapshotter just saved the checkpoint
    // multiple times during `run` so on-disk revision has advanced
    // past `cp_pre`'s. Re-load to pick up the current revision; that
    // way our OCC predicate matches and we don't bounce with
    // ConcurrentUpdate. Re-loading is safe because the Snapshotter
    // is the only other writer between `cp_pre` and here.
    let mut cp_save = coord
        .load_checkpoint(0)
        .await
        .map_err(|e| SnapshotError::Source(format!("load_checkpoint: {e}")))?
        .unwrap_or_else(|| Checkpoint::fresh(Mode::Logical));
    cp_save.snapshot_complete = true;
    cp_save.flushed_lsn = snap_lsn;
    for s in &to_snapshot {
        let key = key_for(&s.ident);
        cp_save.snapshoted_tables.insert(key.clone(), true);
        // Stamp `pg_class.oid` alongside the snapshot-done flag.
        // Startup invariant 11 compares this against the current oid
        // on subsequent runs to detect `DROP TABLE` + recreate.
        if let Some(oid) = table_oids.get(&s.ident).copied() {
            if oid != 0 {
                cp_save.snapshoted_table_oids.insert(key, oid);
            }
        }
    }
    cp_save.snapshot_progress.clear();
    coord
        .save_checkpoint(&mut cp_save)
        .await
        .map_err(|e| SnapshotError::Source(format!("save_checkpoint: {e}")))?;

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
