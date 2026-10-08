//! Logical-replication pipeline orchestrator.
//!
//! `Pipeline::process(msg)` advances the sink for a single decoded message;
//! `flush()` drains, uploads, claims, and advances `flushedLSN` — the last
//! step gated by [`CoordCommitReceipt`].

use crate::relation_event;
use crate::sink::{FlushOutput, Sink, SinkError, TableChunk};
use async_trait::async_trait;
use pg2iceberg_coord::{
    CommitBatch, CoordCommitReceipt, CoordError, Coordinator, MarkerInfo, OffsetClaim,
};
use pg2iceberg_core::metrics::{labels, names, Labels};
use pg2iceberg_core::{
    ChangeEvent, ColumnName, Lsn, Metrics, NoopMetrics, Op, PgValue, TableIdent, Timestamp,
};
use pg2iceberg_pg::{ColumnDefaultSource, DecodedMessage, PgError};
use pg2iceberg_stream::codec::EncodedChunk;
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
    #[error("source: {0}")]
    Source(#[from] PgError),
}

pub type Result<T> = std::result::Result<T, PipelineError>;

/// Names a staged Parquet object in the blob store. Real production uses
/// `IdGen::new_uuid` for unique suffixes; the sim uses a deterministic counter
/// so DST runs are reproducible.
#[async_trait]
pub trait BlobNamer: Send + Sync {
    /// A new path for a chunk staged for `table`.
    async fn next_blob_path(&self, table: &TableIdent) -> String;
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
    async fn next_blob_path(&self, table: &TableIdent) -> String {
        let n = self.counter.fetch_add(1, Ordering::SeqCst);
        format!("{}/{}/{:010}.parquet", self.base, table.name, n)
    }
}

/// Where to start replication: past every transaction already staged
/// ([`Coordinator::replicated_lsn`]) and past the snapshot, if one just
/// ran. Postgres skips transactions that committed before it and sends
/// the rest, so at most the last staged transaction comes again,
/// directly after its first copy.
///
/// Starting at the slot's confirmed position instead replays every
/// transaction staged after the last slot ack — after a crash between
/// `claim_offsets` and the ack, several. Staged again, they follow newer
/// transactions in the log, and the materializer, applying the log in
/// order, commits their old rows over the newer ones until the replay
/// catches up.
pub async fn replication_start_lsn<C: Coordinator + ?Sized>(
    coord: &C,
    snapshot_lsn: Option<Lsn>,
) -> Result<Lsn> {
    let staged = coord.replicated_lsn().await?;
    Ok(staged.max(snapshot_lsn.unwrap_or(Lsn::ZERO)))
}

/// The pipeline. Generic over the coord impl so the type system carries the
/// `Coordinator` choice all the way through. `?Sized` so callers that
/// only have an `Arc<dyn Coordinator>` (the lifecycle helper) can use
/// it without re-parameterizing.
pub struct Pipeline<C: Coordinator + ?Sized> {
    sink: Sink,
    /// Change events the pipeline may hold in memory before it stages
    /// them (`sink.flush_rows`).
    flush_threshold: usize,
    /// Chunks of still-open transactions, staged but not yet claimed —
    /// invisible to the materializer until their transaction commits.
    spilled: BTreeMap<u32, Vec<OffsetClaim>>,
    /// Spilled chunks whose transaction has committed; claimed by the
    /// next flush, ahead of the sink's chunks.
    committed_spills: Vec<OffsetClaim>,
    coord: Arc<C>,
    blob_store: Arc<dyn BlobStore>,
    namer: Arc<dyn BlobNamer>,
    flushed_lsn: AtomicU64,
    /// `wal_end` of the latest keepalive received outside a
    /// transaction. Every transaction that arrived before it has
    /// committed (so it's buffered in the sink or already flushed), and
    /// the WAL after those commits carried nothing for the publication —
    /// so once the next flush lands, the slot can be acked up to here.
    /// Without this, WAL that only touched unpublished tables (or other
    /// databases) never moves `flushed_lsn`, and the slot pins it until
    /// the next published change.
    keepalive_lsn: Lsn,
    metrics: Arc<dyn Metrics>,
    /// Changes and transactions received since the last flush, counted
    /// into the metrics when it lands (see [`Received`]).
    received: Received,
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
    /// PG → Iceberg ident translation, keyed by the source-side ident
    /// the pgoutput stream emits in `ChangeEvent.table`. The lifecycle
    /// registers one entry per replicated table so the pipeline's
    /// internal state (staging blobs, log_index keys, coord cursors)
    /// uses the Iceberg-side ident the materializer expects.
    /// Tables with no entry pass through unchanged (legacy tests +
    /// the meta-marker `_pg2iceberg.markers` table both rely on the
    /// fall-through).
    table_translation: BTreeMap<TableIdent, TableIdent>,
    /// This pipeline consumes the replication stream (see
    /// [`Self::track_replication`]).
    replication: bool,
    /// The open transaction: xid and `Begin`'s LSN.
    open_tx: Option<(u32, Lsn)>,
    /// Each table's columns as last staged (see [`Self::stage_relation`]).
    relations: BTreeMap<TableIdent, relation_event::Columns>,
    /// Where relation events' column defaults come from (see
    /// [`Self::read_column_defaults`]).
    column_defaults: Option<Arc<dyn ColumnDefaultSource>>,
}

/// What the pipeline received from the replication stream since its last
/// flush. Counted per message, but recorded into the metrics once per
/// flush: a metric records with an allocated label map.
#[derive(Default)]
struct Received {
    /// Per table: inserts, updates, deletes, truncates.
    changes: BTreeMap<TableIdent, [u64; 4]>,
    transactions: u64,
}

impl Received {
    const OPS: [&'static str; 4] = ["insert", "update", "delete", "truncate"];

    fn change(&mut self, table: &TableIdent, op: Op) {
        let i = match op {
            Op::Insert => 0,
            Op::Update => 1,
            Op::Delete => 2,
            Op::Truncate => 3,
            Op::Relation => return,
        };
        match self.changes.get_mut(table) {
            Some(counts) => counts[i] += 1,
            None => {
                let mut counts = [0; 4];
                counts[i] = 1;
                self.changes.insert(table.clone(), counts);
            }
        }
    }

    fn record(self, metrics: &dyn Metrics) {
        for (table, counts) in self.changes {
            let table = table.to_string();
            for (op, n) in Self::OPS.iter().zip(counts) {
                if n > 0 {
                    let l = labels([("table", &table), ("op", op)]);
                    metrics.counter(names::PIPELINE_CHANGES_TOTAL, &l, n);
                }
            }
        }
        if self.transactions > 0 {
            metrics.counter(
                names::PIPELINE_TRANSACTIONS_TOTAL,
                &Labels::new(),
                self.transactions,
            );
        }
    }
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
            flush_threshold,
            spilled: BTreeMap::new(),
            committed_spills: Vec::new(),
            coord,
            blob_store,
            namer,
            flushed_lsn: AtomicU64::new(0),
            keepalive_lsn: Lsn::ZERO,
            metrics,
            received: Received::default(),
            shut_down: false,
            markers_table: None,
            pending_markers_by_xid: BTreeMap::new(),
            ready_markers: Vec::new(),
            primary_keys: BTreeMap::new(),
            table_translation: BTreeMap::new(),
            replication: false,
            open_tx: None,
            relations: BTreeMap::new(),
            column_defaults: None,
        }
    }

    /// Mark this as the pipeline consuming the replication stream: each
    /// flush records how far it has staged the stream
    /// ([`CommitBatch::replicated_lsn`]), which
    /// [`replication_start_lsn`] resumes from. A mid-stream table's
    /// backfill pipeline stays unmarked: its snapshot LSN covers that
    /// table only.
    pub fn track_replication(&mut self) {
        self.replication = true;
    }

    /// Stage each changed table's column defaults with its columns, read
    /// from `source`: the materializer fills a column added with a
    /// default Postgres stores for the rows that predate it — which
    /// carry no WAL for it — into those rows.
    pub fn read_column_defaults(&mut self, source: Arc<dyn ColumnDefaultSource>) {
        self.column_defaults = Some(source);
    }

    /// Register the primary-key columns for `table`. Required for
    /// correct UPDATE-with-PK-change handling — without it, the
    /// pipeline can't tell whether a PG `UPDATE` changed the PK and
    /// the old row stays orphaned in Iceberg.
    pub fn register_primary_keys(&mut self, table: TableIdent, pk_cols: Vec<ColumnName>) {
        self.primary_keys.insert(table, pk_cols);
    }

    /// Register a translation from the PG-side ident (as it appears
    /// in `ChangeEvent.table` from the pgoutput stream) to the
    /// Iceberg-side ident (where the table lives in the catalog).
    /// When `sink.namespace` differs from the PG schema, every
    /// incoming change must be retagged so the pipeline's staging
    /// blobs / log_index entries are keyed by the Iceberg ident the
    /// materializer reads.
    pub fn register_table_translation(&mut self, pg_ident: TableIdent, iceberg_ident: TableIdent) {
        self.table_translation.insert(pg_ident, iceberg_ident);
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
    /// This is what the pipeline hands to its `send_standby` ticker
    /// (kept out-of-band so callers in tests can drive `send_standby`
    /// directly).
    pub fn flushed_lsn(&self) -> Lsn {
        Lsn(self.flushed_lsn.load(Ordering::SeqCst))
    }

    /// Change events currently held in memory (see [`Sink::buffered_rows`]).
    pub fn buffered_rows(&self) -> usize {
        self.sink.buffered_rows()
    }

    pub async fn process(&mut self, msg: DecodedMessage) -> Result<()> {
        if self.shut_down {
            // Refuse new events after shutdown. Caller bug if this fires.
            return Ok(());
        }
        match msg {
            DecodedMessage::Begin { xid, final_lsn } => {
                self.open_tx = Some((xid, final_lsn));
                self.sink.begin_tx(xid);
            }
            DecodedMessage::Commit { xid, commit_lsn } => {
                self.open_tx = None;
                self.received.transactions += 1;
                // Drain any markers observed in this tx before
                // committing. Flushed atomically with the rest of
                // the tx via the next claim_offsets call.
                if let Some(uuids) = self.pending_markers_by_xid.remove(&xid) {
                    for uuid in uuids {
                        self.ready_markers.push(MarkerInfo { uuid, commit_lsn });
                    }
                }
                self.sink.commit_tx(xid, commit_lsn);
                // A transaction staged in chunks is claimed as soon as it
                // commits, keeping the log in commit order (see
                // `spill_if_full`). Otherwise flush once enough rows are
                // waiting, so memory stays bounded however fast
                // transactions arrive.
                if let Some(chunks) = self.spilled.remove(&xid) {
                    self.committed_spills.extend(chunks);
                    self.flush().await?;
                } else if self.sink.committed_rows() >= self.flush_threshold {
                    self.flush().await?;
                }
            }
            DecodedMessage::Change(mut evt) => {
                let xid = evt.xid;
                // Translate the PG-side ident from the pgoutput stream
                // into the Iceberg-side ident the rest of the
                // pipeline (staging path, log_index keys, coord
                // cursors) expects. Fall through unchanged when no
                // mapping is registered — keeps test fixtures and
                // the meta-marker table working.
                if let Some(iceberg_ident) = self.table_translation.get(&evt.table) {
                    evt.table = iceberg_ident.clone();
                }
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
                self.received.change(&evt.table, evt.op);
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
                            // A delete has no TOASTed values to resolve.
                            del.unchanged_cols.clear();
                            self.sink.record_change(del)?;

                            let mut upd = evt;
                            let old = upd.before.take().unwrap_or_default();
                            // The new key has no committed row to
                            // resolve unchanged TOAST columns from. Take
                            // their values from the old tuple when it
                            // has them (REPLICA IDENTITY FULL; a TOASTed
                            // value is never NULL, so NULL is a key-only
                            // tuple's filler). Otherwise stage the old
                            // key as the update's `before`, for the
                            // materializer to resolve them from.
                            if let Some(after) = upd.after.as_mut() {
                                upd.unchanged_cols.retain(|c| match old.get(c) {
                                    Some(v) if *v != PgValue::Null => {
                                        after.insert(c.clone(), v.clone());
                                        false
                                    }
                                    _ => true,
                                });
                            }
                            if !upd.unchanged_cols.is_empty() {
                                upd.before = Some(
                                    pks.iter()
                                        .filter_map(|c| old.get(c).map(|v| (c.clone(), v.clone())))
                                        .collect(),
                                );
                            }
                            self.sink.record_change(upd)?;
                            self.spill_if_full(xid).await?;
                            return Ok(());
                        }
                    }
                }
                // A staged UPDATE carries `before` only for a key change
                // (above), where it is the old key.
                let mut evt = evt;
                if evt.op == Op::Update {
                    evt.before = None;
                }
                self.sink.record_change(evt)?;
                self.spill_if_full(xid).await?;
            }
            DecodedMessage::Relation {
                rel_id,
                ident,
                columns,
            } => {
                self.stage_relation(rel_id, ident, &columns).await?;
            }
            DecodedMessage::Keepalive { wal_end, .. } => {
                // Only trustworthy between transactions: pgoutput sends a
                // transaction once its commit is decoded, so a keepalive
                // arriving mid-transaction can carry a `wal_end` past that
                // not-yet-flushed commit.
                if !self.sink.has_open_tx() && wal_end > self.keepalive_lsn {
                    self.keepalive_lsn = wal_end;
                }
            }
        }
        Ok(())
    }

    /// Stage the table's columns, if they changed, as a relation event
    /// where the stream put the Relation message — before the
    /// transaction's changes to the table — so the materializer applies
    /// the schema change between the rows staged before and after it.
    /// Applied when the message arrived instead, ahead of rows already
    /// staged under the old schema, a dropped and re-added column would
    /// take those rows' values for the dropped one.
    ///
    /// The columns' defaults come from Postgres's catalog as it is now,
    /// which may be past the change the message reports.
    async fn stage_relation(
        &mut self,
        rel_id: u32,
        ident: TableIdent,
        columns: &[pg2iceberg_pg::RelationColumn],
    ) -> Result<()> {
        let table = self.table_translation.get(&ident).cloned().unwrap_or(ident);
        if self.markers_table.as_ref() == Some(&table) {
            return Ok(());
        }
        let columns: relation_event::Columns =
            columns.iter().map(|c| (c.name.clone(), c.ty)).collect();
        // pgoutput resends a table's Relation, unchanged, at every new
        // session and cache invalidation; those say nothing new.
        if self.relations.get(&table) == Some(&columns) {
            return Ok(());
        }
        let mut defaults = relation_event::Defaults::new();
        if let Some(source) = &self.column_defaults {
            let catalog: BTreeMap<String, pg2iceberg_pg::ColumnDefault> = source
                .column_defaults(rel_id)
                .await?
                .into_iter()
                .map(|d| (d.name.clone(), d))
                .collect();
            for (name, _) in &columns {
                let value = match catalog.get(name) {
                    None => relation_event::DefaultValue::ColumnGone,
                    // A value the log can't hold is as good as not stored.
                    Some(d) => match d
                        .stored
                        .clone()
                        .filter(|v| relation_event::value_to_json(v).is_some())
                    {
                        Some(v) => relation_event::DefaultValue::Stored(v),
                        None if d.has_default => relation_event::DefaultValue::NotStored,
                        None => continue,
                    },
                };
                defaults.insert(name.clone(), value);
            }
        }
        let (xid, lsn) = match self.open_tx {
            Some((xid, lsn)) => (Some(xid), lsn),
            None => (None, Lsn::ZERO),
        };
        self.sink.record_change(ChangeEvent {
            table: table.clone(),
            op: Op::Relation,
            lsn,
            commit_ts: Timestamp(0),
            xid,
            before: None,
            after: Some(relation_event::encode(&relation_event::Relation {
                columns: columns.clone(),
                defaults,
            })),
            unchanged_cols: Vec::new(),
        })?;
        self.relations.insert(table, columns);
        Ok(())
    }

    /// Drain all committed-but-unflushed transactions: encode → upload →
    /// `claim_offsets` → advance `flushedLSN` via the receipt. No-op if
    /// nothing is ready (no staged events, no markers awaiting emission,
    /// and no keepalive past `flushedLSN`).
    pub async fn flush(&mut self) -> Result<Option<Lsn>> {
        let sink_output = self.sink.flush()?;
        // Markers can ride alone in an otherwise-empty flush — a tx
        // that contains only a marker INSERT (no user-data events)
        // still needs to write the marker into coord. Use the
        // marker's commit LSN as the batch's flushable_lsn in that
        // case.
        let (chunks, data_lsn) = match sink_output {
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
            // Nothing to stage, but a keepalive showed the WAL moved past
            // what we've flushed without anything for us: flush an empty
            // batch so the receipt advances `flushedLSN` over it.
            None if self.keepalive_lsn > self.flushed_lsn() => (Vec::new(), Lsn::ZERO),
            None => return Ok(None),
        };
        // Every transaction that arrived before the last out-of-transaction
        // keepalive is in this batch or already flushed, so the batch also
        // covers the WAL up to that keepalive's `wal_end`.
        let flushable_lsn = data_lsn.max(self.keepalive_lsn);
        if chunks.is_empty() {
            // Possible if every committed tx was empty. Still advance the LSN
            // (we know all tx commits up to this point are durable in PG, but
            // there's nothing for the coord to register).
            // We still go through claim_offsets with an empty batch
            // so the receipt is the single LSN-advance code path.
        }

        // A committed transaction's spilled chunks precede its remainder.
        let mut claims = self.committed_spills.clone();
        for TableChunk { table, chunk } in chunks {
            claims.push(self.stage(table, chunk).await?);
        }

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
            replicated_lsn: self.replication.then_some(flushable_lsn),
        };
        let receipt = self.coord.claim_offsets(&batch).await?;
        self.ready_markers.clear();
        self.committed_spills.clear();
        self.advance_flushed_lsn(receipt);

        // Emit per-flush counters + the flushed_lsn gauge.
        let no_labels = Labels::new();
        self.metrics
            .counter(names::PIPELINE_FLUSH_TOTAL, &no_labels, 1);
        self.metrics.gauge(
            names::PIPELINE_FLUSHED_LSN,
            &no_labels,
            flushable_lsn.0 as f64,
        );
        std::mem::take(&mut self.received).record(self.metrics.as_ref());
        Ok(Some(flushable_lsn))
    }

    /// Stage the open transaction `xid` in chunks once it buffers
    /// `flush_threshold` rows, so transaction size never bounds memory.
    /// The chunks stay unclaimed — invisible to the materializer — until
    /// the transaction commits.
    ///
    /// Relies on pgoutput (protocol v1) delivering transactions whole and
    /// in commit order: at most one is open, and nothing else commits
    /// before it does. Claiming everything already committed first
    /// therefore keeps the log in commit order. A crash before the commit
    /// orphans the chunks; the slot replays the whole transaction.
    async fn spill_if_full(&mut self, xid: Option<u32>) -> Result<()> {
        let Some(xid) = xid else {
            return Ok(());
        };
        if self.sink.open_tx_rows(xid) < self.flush_threshold {
            return Ok(());
        }
        if self.sink.has_committed() {
            self.flush().await?;
        }
        for TableChunk { table, chunk } in self.sink.spill_open_tx(xid)? {
            let claim = self.stage(table, chunk).await?;
            self.spilled.entry(xid).or_default().push(claim);
        }
        Ok(())
    }

    /// Upload one encoded chunk; returns the claim that registers it.
    async fn stage(&self, table: TableIdent, chunk: EncodedChunk) -> Result<OffsetClaim> {
        let path = self.namer.next_blob_path(&table).await;
        let byte_size = chunk.bytes.len() as u64;
        self.blob_store.put(&path, chunk.bytes).await?;
        let table_labels = labels([("table", &table.to_string())]);
        self.metrics.counter(
            names::PIPELINE_ROWS_STAGED_TOTAL,
            &table_labels,
            chunk.row_count,
        );
        self.metrics
            .counter(names::PIPELINE_STAGED_BYTES_TOTAL, &table_labels, byte_size);
        Ok(OffsetClaim {
            table,
            record_count: chunk.record_count,
            byte_size,
            s3_path: path,
        })
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

    /// Forget the replication session, for a new stream that starts at
    /// [`replication_start_lsn`]: what arrived since the last flush, the
    /// open transaction (its spilled chunks become orphans, as after a
    /// crash), and the relations the old stream sent. The new stream
    /// sends all of it again — keeping the relations would drop a schema
    /// change that was buffered but not yet staged, as a duplicate.
    /// Configuration and `flushed_lsn` stay: the slot was acked there,
    /// and the new stream starts at or past it.
    pub fn reset_session(&mut self) {
        // Destructured so a new field can't be skipped: it is either
        // reset here or listed as kept.
        let Self {
            sink,
            flush_threshold,
            spilled,
            committed_spills,
            keepalive_lsn,
            pending_markers_by_xid,
            ready_markers,
            open_tx,
            relations,
            coord: _,
            blob_store: _,
            namer: _,
            flushed_lsn: _,
            metrics: _,
            // Counted as received; a reopened stream sends them again.
            received: _,
            shut_down: _,
            markers_table: _,
            primary_keys: _,
            table_translation: _,
            replication: _,
            column_defaults: _,
        } = self;
        *sink = Sink::new(*flush_threshold);
        spilled.clear();
        committed_spills.clear();
        *keepalive_lsn = Lsn::ZERO;
        pending_markers_by_xid.clear();
        ready_markers.clear();
        *open_tx = None;
        relations.clear();
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
