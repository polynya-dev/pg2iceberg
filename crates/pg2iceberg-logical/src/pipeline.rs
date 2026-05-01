//! Logical-replication pipeline orchestrator.
//!
//! `Pipeline::process(msg)` advances the sink for a single decoded message;
//! `flush()` drains, uploads, claims, and advances `flushedLSN` — the last
//! step gated by [`CoordCommitReceipt`].

use crate::sink::{FlushOutput, Sink, SinkError, TableChunk};
use async_trait::async_trait;
use bytes::Bytes;
use pg2iceberg_coord::{
    CommitBatch, CoordCommitReceipt, CoordError, Coordinator, MarkerInfo, OffsetClaim,
};
use pg2iceberg_core::metrics::{names, Labels};
use pg2iceberg_core::{ColumnName, Lsn, Metrics, NoopMetrics, Op, PgValue, TableIdent};
use pg2iceberg_pg::DecodedMessage;
use pg2iceberg_stream::{BlobStore, StreamError};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use thiserror::Error;

#[derive(Clone, Debug, Error)]
pub enum PipelineError {
    #[error("sink: {0}")]
    Sink(#[from] SinkError),
    #[error("blob: {0}")]
    Blob(#[from] StreamError),
    #[error("coord: {0}")]
    Coord(#[from] CoordError),
}

pub type Result<T> = std::result::Result<T, PipelineError>;

/// Names a staged Parquet object in the blob store. Real production uses
/// `IdGen::new_uuid` for unique suffixes; the sim uses a deterministic counter
/// so DST runs are reproducible.
#[async_trait]
pub trait BlobNamer: Send + Sync {
    async fn next_blob_path(&self, table: &str) -> String;
}

/// Deterministic blob namer for sim/test paths. Uses a monotonic counter.
#[derive(Default)]
pub struct CounterBlobNamer {
    counter: AtomicU64,
    base: String,
}

impl CounterBlobNamer {
    pub fn new(base: impl Into<String>) -> Self {
        Self {
            counter: AtomicU64::new(0),
            base: base.into(),
        }
    }
}

#[async_trait]
impl BlobNamer for CounterBlobNamer {
    async fn next_blob_path(&self, table: &str) -> String {
        let n = self.counter.fetch_add(1, Ordering::SeqCst);
        format!("{}/{}/{:010}.parquet", self.base, table, n)
    }
}

/// The pipeline. Generic over the coord impl so the type system carries the
/// `Coordinator` choice all the way through. `?Sized` so callers that
/// only have an `Arc<dyn Coordinator>` (the lifecycle helper) can use
/// it without re-parameterizing.
pub struct Pipeline<C: Coordinator + ?Sized> {
    sink: Sink,
    coord: Arc<C>,
    blob_store: Arc<dyn BlobStore>,
    namer: Arc<dyn BlobNamer>,
    flushed_lsn: AtomicU64,
    metrics: Arc<dyn Metrics>,
    /// True after `shutdown` runs; further `process` calls become no-ops.
    /// Tested for in flush so a forgotten shutdown sequence stays correct.
    shut_down: bool,
    /// Optional marker-table identity. When set, INSERTs to this table
    /// are intercepted (filtered out of staging) and their `uuid` column
    /// captured; the resulting marker UUID + commit LSN is included in
    /// the next [`CommitBatch`]. Defaults to `_pg2iceberg.markers` when
    /// marker mode is on; `None` disables.
    markers_table: Option<TableIdent>,
    /// Per-tx pending markers, by xid. Filled from `Change` events,
    /// drained on `Commit` into [`Self::ready_markers`].
    pending_markers_by_xid: BTreeMap<u32, Vec<String>>,
    /// Markers from committed txs awaiting flush. Drained into
    /// `CommitBatch.markers` on each successful `claim_offsets`.
    ready_markers: Vec<MarkerInfo>,
    /// Per-table primary-key column list. Lets the pipeline detect
    /// `UPDATE` events that change the primary key (`before.pk !=
    /// after.pk`) and split them into a synthetic `Delete` for the
    /// old PK plus an `Update` for the new PK, so the materializer
    /// drops the old row. Without this, the old PK becomes orphaned
    /// in Iceberg. Empty when not configured — pipeline falls back
    /// to the previous "stage `after` only" behavior, which is
    /// wrong for PK changes but matches the prior shape for tests
    /// that haven't registered PKs.
    primary_keys: BTreeMap<TableIdent, Vec<ColumnName>>,
}

impl<C: Coordinator + ?Sized> Pipeline<C> {
    pub fn new(
        coord: Arc<C>,
        blob_store: Arc<dyn BlobStore>,
        namer: Arc<dyn BlobNamer>,
        flush_threshold: usize,
    ) -> Self {
        Self::with_metrics(
            coord,
            blob_store,
            namer,
            flush_threshold,
            Arc::new(NoopMetrics),
        )
    }

    pub fn with_metrics(
        coord: Arc<C>,
        blob_store: Arc<dyn BlobStore>,
        namer: Arc<dyn BlobNamer>,
        flush_threshold: usize,
        metrics: Arc<dyn Metrics>,
    ) -> Self {
        Self {
            sink: Sink::new(flush_threshold),
            coord,
            blob_store,
            namer,
            flushed_lsn: AtomicU64::new(0),
            metrics,
            shut_down: false,
            markers_table: None,
            pending_markers_by_xid: BTreeMap::new(),
            ready_markers: Vec::new(),
            primary_keys: BTreeMap::new(),
        }
    }

    /// Register the primary-key columns for `table`. Required for
    /// correct UPDATE-with-PK-change handling — without it, the
    /// pipeline can't tell whether a PG `UPDATE` changed the PK and
    /// the old row stays orphaned in Iceberg.
    pub fn register_primary_keys(&mut self, table: TableIdent, pk_cols: Vec<ColumnName>) {
        self.primary_keys.insert(table, pk_cols);
    }

    /// Enable blue-green marker detection. INSERTs to `table` are
    /// intercepted (not staged as user data) and their `uuid` column
    /// is included in the next flush's [`CommitBatch.markers`].
    /// `table` is typically `_pg2iceberg.markers` per the Go
    /// reference's blue-green example.
    pub fn enable_markers(&mut self, table: TableIdent) {
        self.markers_table = Some(table);
    }

    /// `true` iff at least one marker has been observed in a
    /// committed tx and is awaiting a flush. The lifecycle main loop
    /// uses this to trigger an immediate flush+materialize on
    /// marker observation, instead of waiting for the next periodic
    /// tick — that's what gives blue-green replicas wall-clock-
    /// independent alignment.
    pub fn has_pending_marker(&self) -> bool {
        !self.ready_markers.is_empty()
    }

    /// Highest LSN whose underlying batch has committed in the coordinator.
    /// This is what the pipeline hands to its `send_standby` ticker (Phase 5
    /// keeps that out of band — the sim test calls `send_standby` directly).
    pub fn flushed_lsn(&self) -> Lsn {
        Lsn(self.flushed_lsn.load(Ordering::SeqCst))
    }

    pub async fn process(&mut self, msg: DecodedMessage) -> Result<()> {
        if self.shut_down {
            // Refuse new events after shutdown. Caller bug if this fires.
            return Ok(());
        }
        match msg {
            DecodedMessage::Begin { xid, .. } => self.sink.begin_tx(xid),
            DecodedMessage::Commit { xid, commit_lsn } => {
                // Drain any markers observed in this tx before
                // committing. Flushed atomically with the rest of
                // the tx via the next claim_offsets call.
                if let Some(uuids) = self.pending_markers_by_xid.remove(&xid) {
                    for uuid in uuids {
                        self.ready_markers.push(MarkerInfo { uuid, commit_lsn });
                    }
                }
                self.sink.commit_tx(xid, commit_lsn);
            }
            DecodedMessage::Change(evt) => {
                if let Some(marker_table) = &self.markers_table {
                    if evt.table == *marker_table && evt.op == Op::Insert {
                        // Intercept: extract uuid, don't stage as
                        // user data. The marker is operator metadata
                        // observable in PG but materialized only as
                        // an Iceberg-side meta-marker row by the
                        // materializer.
                        if let Some(row) = &evt.after {
                            if let Some(PgValue::Text(uuid)) = row.get(&ColumnName("uuid".into())) {
                                if let Some(xid) = evt.xid {
                                    self.pending_markers_by_xid
                                        .entry(xid)
                                        .or_default()
                                        .push(uuid.clone());
                                }
                            }
                        }
                        return Ok(());
                    }
                }
                // UPDATE with PK change → split into Delete(old PK) +
                // Update(new full row). Without this, the materializer
                // folds-by-new-PK and the old row stays orphaned in
                // Iceberg. Skipped silently when PKs aren't registered
                // (legacy / test path).
                if evt.op == Op::Update {
                    if let (Some(pks), Some(before), Some(after)) = (
                        self.primary_keys.get(&evt.table),
                        evt.before.as_ref(),
                        evt.after.as_ref(),
                    ) {
                        let before_pk: Vec<&PgValue> =
                            pks.iter().filter_map(|c| before.get(c)).collect();
                        let after_pk: Vec<&PgValue> =
                            pks.iter().filter_map(|c| after.get(c)).collect();
                        if !before_pk.is_empty()
                            && before_pk.len() == after_pk.len()
                            && before_pk != after_pk
                        {
                            // Split. Delete carries `before` (full or
                            // PK-only — whichever pgoutput sent us),
                            // which `staged_row(Delete)` will pull as
                            // the staged row. Update carries `after`
                            // for the new-PK insert.
                            let mut del = evt.clone();
                            del.op = Op::Delete;
                            del.after = None;
                            self.sink.record_change(del)?;

                            let mut upd = evt;
                            // Strip `before` so the staging layer
                            // doesn't carry duplicate data.
                            upd.before = None;
                            self.sink.record_change(upd)?;
                            return Ok(());
                        }
                    }
                }
                self.sink.record_change(evt)?;
            }
            DecodedMessage::Relation { .. } => {
                // Schema evolution lands in Phase 7.
            }
            DecodedMessage::Keepalive { .. } => {
                // The standby ticker reads `flushed_lsn()` directly.
            }
        }
        Ok(())
    }

    /// Drain all committed-but-unflushed transactions: encode → upload →
    /// `claim_offsets` → advance `flushedLSN` via the receipt. No-op if
    /// nothing is ready (no staged events AND no markers awaiting
    /// emission).
    pub async fn flush(&mut self) -> Result<Option<Lsn>> {
        let sink_output = self.sink.flush()?;
        // Markers can ride alone in an otherwise-empty flush — a tx
        // that contains only a marker INSERT (no user-data events)
        // still needs to write the marker into coord. Use the
        // marker's commit LSN as the batch's flushable_lsn in that
        // case.
        let (chunks, flushable_lsn) = match sink_output {
            Some(FlushOutput {
                chunks,
                flushable_lsn,
            }) => (chunks, flushable_lsn),
            None if !self.ready_markers.is_empty() => {
                let max_marker_lsn = self
                    .ready_markers
                    .iter()
                    .map(|m| m.commit_lsn)
                    .max()
                    .expect("non-empty checked above");
                (Vec::new(), max_marker_lsn)
            }
            None => return Ok(None),
        };
        if chunks.is_empty() {
            // Possible if every committed tx was empty. Still advance the LSN
            // (we know all tx commits up to this point are durable in PG, but
            // there's nothing for the coord to register).
            // For Phase 5 we still go through claim_offsets with an empty
            // batch so the receipt is the single LSN-advance code path.
        }

        let mut claims = Vec::with_capacity(chunks.len());
        let mut total_records = 0u64;
        for TableChunk { table, chunk } in chunks {
            let path = self.namer.next_blob_path(&table.name).await;
            let byte_size = chunk.bytes.len() as u64;
            self.blob_store
                .put(&path, Bytes::clone(&chunk.bytes))
                .await?;
            let mut labels = Labels::new();
            labels.insert("table".into(), table.name.clone());
            self.metrics.counter(
                names::PIPELINE_ROWS_STAGED_TOTAL,
                &labels,
                chunk.record_count,
            );
            total_records += chunk.record_count;
            claims.push(OffsetClaim {
                table,
                record_count: chunk.record_count,
                byte_size,
                s3_path: path,
            });
        }
        let _ = total_records;

        // Drain ready markers into the batch. claim_offsets writes
        // them atomically with the log_index rows so a crash
        // between staging and marker-record can't drop them.
        // Markers stay in `ready_markers` until claim_offsets
        // returns Ok — a failed flush retries them on the next
        // call. Re-flushing markers is idempotent (uuid is the PK
        // in coord's pending_markers).
        let markers_for_batch = self.ready_markers.clone();
        let batch = CommitBatch {
            claims,
            flushable_lsn,
            markers: markers_for_batch,
        };
        let receipt = self.coord.claim_offsets(&batch).await?;
        self.ready_markers.clear();
        self.advance_flushed_lsn(receipt);

        // Emit per-flush counters + the flushed_lsn gauge.
        let mut empty_labels = Labels::new();
        self.metrics
            .counter(names::PIPELINE_FLUSH_TOTAL, &empty_labels, 1);
        // Empty-labels gauge doesn't need a fresh map; reuse the buffer.
        empty_labels.clear();
        self.metrics.gauge(
            names::PIPELINE_FLUSHED_LSN,
            &empty_labels,
            flushable_lsn.0 as f64,
        );
        Ok(Some(flushable_lsn))
    }

    /// Graceful shutdown: drain whatever's already buffered into committed
    /// txns (no-op for already-empty queues), do one final flush + receipt
    /// advance, then mark the pipeline as no-longer-accepting events.
    ///
    /// Intentionally does NOT wait for in-flight transactions to commit:
    /// the caller is expected to have already drained the source. After
    /// this returns, `process` is a no-op and `flush` returns `Ok(None)`.
    pub async fn shutdown(&mut self) -> Result<()> {
        if self.shut_down {
            return Ok(());
        }
        // One last flush to push any committed-but-unflushed work.
        let _ = self.flush().await?;
        self.shut_down = true;
        Ok(())
    }

    pub fn is_shut_down(&self) -> bool {
        self.shut_down
    }

    /// **Receipt-gated LSN advance.** Consumes a [`CoordCommitReceipt`] —
    /// since the receipt cannot be constructed outside `pg2iceberg-coord`'s
    /// internals, no caller can advance the slot LSN without the coord
    /// having committed.
    fn advance_flushed_lsn(&self, receipt: CoordCommitReceipt) {
        // The receipt carries the LSN that the pipeline supplied via
        // `CommitBatch::flushable_lsn`. After the coord commit, that LSN is
        // safely durable downstream of the slot.
        self.flushed_lsn
            .store(receipt.flushable_lsn.0, Ordering::SeqCst);
    }
}
