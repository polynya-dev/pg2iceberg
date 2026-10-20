//! Initial-snapshot phase: bootstrap a fresh pg2iceberg deployment by
//! reading every row from each tracked table at a known LSN, writing them
//! to the tables, then advancing the slot past that LSN so live
//! replication picks up exactly where the snapshot left off. A table
//! backfilled mid-stream, while its changes stream, has its rows staged
//! instead, through the same pipeline as logical replication
//! ([`SnapshotSink`]).
//!
//! Each table is read in primary key order, a chunk at a time; several
//! tables can be read at once ([`Snapshotter::run_concurrently`]).
//! Splitting one large table's reads is a tracked follow-up.
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
//! 3. Write the rows to the tables, each as one update, as of `N`.
//! 4. Caller acks the slot at `N` via `send_standby`. `restart_lsn` jumps
//!    to `N`, so future replication reads start at `N`.
//!
//! Any WAL events between `K` and `N` are *covered* by the snapshot (rows
//! read at `N` reflect their effects), so skipping them is correct, not
//! lossy.

use async_trait::async_trait;
use futures::channel::oneshot;
use futures::stream::{FuturesUnordered, StreamExt as _};
use pg2iceberg_coord::Coordinator;
use pg2iceberg_core::{ChangeEvent, ColumnName, Lsn, Op, Row, TableIdent, TableSchema, Timestamp};
use pg2iceberg_iceberg::{pk_key, Catalog};
use pg2iceberg_logical::materializer::MaterializerError;
use pg2iceberg_logical::pipeline::Spawn;
use pg2iceberg_logical::work_span;
use pg2iceberg_logical::{Materializer, Pipeline, PipelineError};
use pg2iceberg_pg::DecodedMessage;
use std::collections::{BTreeMap, VecDeque};
use std::sync::Arc;
use thiserror::Error;
use tracing::Instrument as _;

#[derive(Debug, Error)]
pub enum SnapshotError {
    #[error("source: {0}")]
    Source(String),
    #[error("pipeline: {0}")]
    Pipeline(#[from] PipelineError),
    #[error("materializer: {0}")]
    Materializer(#[from] MaterializerError),
    /// A table's task was dropped before it finished.
    #[error("snapshot task abandoned")]
    Abandoned,
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
/// - Return rows sorted by their primary key ASC, in an order of the
///   source's choosing.
/// - Be stable across calls (same snapshot view).
/// - Honor `after_pk_key` as a strict lower bound: it is [`Self::cursor`]
///   of the last row of a chunk, and rows up to it in that order must be
///   excluded.
/// - Cap output at `chunk_size`. Returning fewer rows is allowed only at
///   end-of-table.
///
/// `after_pk_key` is a string (rather than a `Row`) because resumable
/// snapshot persists the key into the checkpoint — it can't reconstruct a
/// `Row` without the schema, and a string is the canonical wire form.
/// A cursor persisted by an earlier version must still resume where it
/// left off.
#[async_trait]
pub trait SnapshotSource: Send + Sync {
    async fn snapshot_lsn(&self) -> Result<Lsn>;
    async fn read_chunk(
        &self,
        ident: &TableIdent,
        chunk_size: usize,
        after_pk_key: Option<&str>,
    ) -> Result<Vec<Row>>;

    /// Split `ident` into ranges of its keys to read apart
    /// ([`Self::read_range`]), in order, together covering it: at most
    /// `parts`, each of about `min_rows` rows or more. None — or one — if
    /// it can't, or needn't. Default: none.
    async fn split(
        &self,
        _ident: &TableIdent,
        _parts: usize,
        _min_rows: usize,
    ) -> Result<Vec<KeyRange>> {
        Ok(Vec::new())
    }

    /// [`Self::read_chunk`], of `range` of the table only — one
    /// [`Self::split`] gave.
    async fn read_range(
        &self,
        ident: &TableIdent,
        _range: &KeyRange,
        _chunk_size: usize,
        _after_pk_key: Option<&str>,
    ) -> Result<Vec<Row>> {
        Err(SnapshotError::Source(format!(
            "this source reads {ident} whole"
        )))
    }

    /// The `after_pk_key` that reads on past `last`, the last row of the
    /// chunk of `ident` read after `after`; `pk_cols` is the table's
    /// primary key. Default: `last`'s canonical PK key (JSON form, see
    /// `pg2iceberg_iceberg::pk_key`).
    fn cursor(
        &self,
        _ident: &TableIdent,
        _after: Option<&str>,
        last: &Row,
        pk_cols: &[ColumnName],
    ) -> String {
        pk_key(last, pk_cols)
    }
}

/// A range of a table's keys, read apart from the rest
/// ([`SnapshotSource::split`]): from `from`, inclusive, to `to`, exclusive,
/// each in the source's own encoding; `None` is the table's start, or end.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct KeyRange {
    pub from: Option<String>,
    pub to: Option<String>,
}

pub use pg2iceberg_core::SNAPSHOT_XID_BASE;

/// `_pg2iceberg.markers` is the **blue-green replica alignment**
/// table. It is *not* the snapshot↔CDC fence — that lives in
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
/// coord-write amplification — ~1k rows per chunk is a reasonable
/// balance for the typical OLTP-row-size workload. pg2iceberg itself
/// reads `flush_rows` a chunk: the pipeline holds that many changes
/// anyway, and each chunk staged is a file and a claim.
pub const DEFAULT_CHUNK_SIZE: usize = 1024;

/// Most tables pg2iceberg's snapshot reads at once by default
/// ([`Snapshotter::with_concurrency`]): each holds a Postgres connection,
/// and up to two chunks in memory.
pub const DEFAULT_TABLE_CONCURRENCY: usize = 4;

/// Where a [`Snapshotter`] puts the tables' rows: written to the tables
/// ([`Materializer`]), or staged ([`Pipeline`]) — for a table backfilled
/// mid-stream, whose changes stage meanwhile, and are applied after its
/// rows.
#[async_trait]
pub trait SnapshotSink: Send + Sized + 'static {
    /// Put `rows`, the next chunk of `table`'s snapshot, read at `lsn`,
    /// as a transaction `xid`. Whether every chunk of the table's put so
    /// far is durable, so a restart can resume past them.
    async fn put(&mut self, table: &TableIdent, xid: u32, lsn: Lsn, rows: Vec<Row>)
        -> Result<bool>;

    /// Make every chunk of `table`'s put durable: its snapshot is read.
    async fn finish(&mut self, table: &TableIdent) -> Result<()>;

    /// Forget the chunks of `table`'s put but not durable: its snapshot
    /// failed.
    fn abandon(&mut self, table: &TableIdent);

    /// Whether tables can be put alongside each other
    /// ([`Self::split_off`]).
    fn splits(&self) -> bool;

    /// A sink for `table` alone, to put it alongside this one.
    fn split_off(&mut self, table: &TableIdent) -> Result<Self>;

    /// Take back a sink [`Self::split_off`] made, its table done or
    /// abandoned.
    fn rejoin(&mut self, part: Self);

    /// A sink for one of `ranges` key ranges of `table`, read apart from
    /// the others: what's put to it joins the table's
    /// ([`Self::rejoin_range`]), durable with it once [`Self::finish`]ed.
    /// `None` if this sink puts a table only whole, or `table` holds rows
    /// already. Default: `None`.
    fn split_range(&mut self, _table: &TableIdent, _ranges: usize) -> Option<Self> {
        None
    }

    /// Ready what's been put of `table`'s range to join it: its own work,
    /// done on its own task. Default: nothing to do.
    async fn seal(&mut self, _table: &TableIdent) -> Result<()> {
        Ok(())
    }

    /// Take back a sink [`Self::split_range`] made, sealed: what's been
    /// put to it joins `table`'s.
    fn rejoin_range(&mut self, _table: &TableIdent, _part: Self) {}
}

/// Each chunk is staged and claimed as it's put: durable at once.
#[async_trait]
impl<C: Coordinator + ?Sized + 'static> SnapshotSink for Pipeline<C> {
    async fn put(
        &mut self,
        table: &TableIdent,
        xid: u32,
        lsn: Lsn,
        rows: Vec<Row>,
    ) -> Result<bool> {
        self.process(DecodedMessage::Begin {
            final_lsn: lsn,
            xid,
        })
        .await?;
        for row in rows {
            self.process(DecodedMessage::Change(ChangeEvent {
                table: table.clone(),
                op: Op::Insert,
                lsn,
                commit_ts: Timestamp(0),
                xid: Some(xid),
                before: None,
                after: Some(row),
                unchanged_cols: vec![],
            }))
            .await?;
        }
        self.process(DecodedMessage::Commit {
            commit_lsn: lsn,
            xid,
        })
        .await?;
        self.flush().await?;
        Ok(true)
    }

    async fn finish(&mut self, _table: &TableIdent) -> Result<()> {
        Ok(())
    }

    fn abandon(&mut self, _table: &TableIdent) {
        // What a failed flush left; the chunk is read again.
        self.reset_session();
    }

    fn splits(&self) -> bool {
        true
    }

    fn split_off(&mut self, _table: &TableIdent) -> Result<Self> {
        Ok(Pipeline::split_off(self))
    }

    fn rejoin(&mut self, part: Self) {
        Pipeline::rejoin(self, part)
    }
}

/// The chunks are written to the table as they're put, and committed when
/// it's read, as one update ([`Materializer::apply_snapshot_chunk`]):
/// durable only then. A restart reads the table again.
#[async_trait]
impl<Cat: Catalog + 'static> SnapshotSink for Materializer<Cat> {
    async fn put(
        &mut self,
        table: &TableIdent,
        xid: u32,
        lsn: Lsn,
        rows: Vec<Row>,
    ) -> Result<bool> {
        self.apply_snapshot_chunk(table, xid, lsn, rows).await?;
        Ok(false)
    }

    async fn finish(&mut self, table: &TableIdent) -> Result<()> {
        self.commit_snapshot(table).await?;
        Ok(())
    }

    fn abandon(&mut self, table: &TableIdent) {
        self.abandon_snapshot(table);
    }

    fn splits(&self) -> bool {
        Materializer::splits(self)
    }

    fn split_off(&mut self, table: &TableIdent) -> Result<Self> {
        Materializer::split_off(self, table)
            .ok_or_else(|| MaterializerError::UnknownTable(table.clone()).into())
    }

    fn rejoin(&mut self, part: Self) {
        Materializer::rejoin(self, part)
    }

    fn split_range(&mut self, table: &TableIdent, ranges: usize) -> Option<Self> {
        Materializer::split_range(self, table, ranges)
    }

    async fn seal(&mut self, table: &TableIdent) -> Result<()> {
        self.seal_snapshot(table).await?;
        Ok(())
    }

    fn rejoin_range(&mut self, table: &TableIdent, part: Self) {
        Materializer::rejoin_range(self, table, part)
    }
}

/// Drive a fresh pipeline through the initial-snapshot phase using the
/// default chunk size.
///
/// Returns the LSN the caller should `send_standby` to so the replication
/// slot picks up live replication from the right point. The caller should
/// also drive the materializer at least once after this returns to flush
/// snapshot rows out to Iceberg.
pub async fn run_snapshot<S, K>(
    source: &S,
    coord: Arc<dyn Coordinator>,
    schemas: &[TableSchema],
    sink: &mut K,
) -> Result<Lsn>
where
    S: SnapshotSource + ?Sized,
    K: SnapshotSink,
{
    run_snapshot_chunked(source, coord, schemas, sink, DEFAULT_CHUNK_SIZE).await
}

/// Like [`run_snapshot`] but with an explicit `chunk_size`: peak memory
/// is bounded by `chunk_size` rows, two chunks a table.
///
/// Resumability: a completed table writes a row to
/// `_pg2iceberg.tables`, and a table's chunks, once durable
/// ([`SnapshotSink::put`]), its progress to
/// `_pg2iceberg.snapshot_progress`. A subsequent call against the same
/// coord will skip already-complete tables and resume the others from
/// their progress.
pub async fn run_snapshot_chunked<S, K>(
    source: &S,
    coord: Arc<dyn Coordinator>,
    schemas: &[TableSchema],
    sink: &mut K,
    chunk_size: usize,
) -> Result<Lsn>
where
    S: SnapshotSource + ?Sized,
    K: SnapshotSink,
{
    Snapshotter::new(coord)
        .with_chunk_size(chunk_size)
        .run(source, schemas, sink)
        .await
}

/// Resumable snapshot driver.
///
/// On `run`, loads each table's per-row resume cursor from
/// `_pg2iceberg.snapshot_progress`; for each table, resumes reading
/// at PKs strictly greater than the saved key. Once chunks are durable,
/// stamps the new cursor so a crash mid-snapshot resumes from the
/// right place. When a table finishes, the cursor row is dropped
/// and `mark_table_snapshot_complete` upserts the per-table flag in
/// `_pg2iceberg.tables`.
///
/// Replaces the old `Checkpoint`-driven design — see the module-level
/// docs for the rationale.
#[derive(Clone)]
pub struct Snapshotter {
    coord: Arc<dyn Coordinator>,
    chunk_size: usize,
    /// Optional per-table `pg_class.oid`s captured at snapshot time.
    /// Stamped onto the `_pg2iceberg.tables` row when a table
    /// completes; used downstream by the `TableIdentityChanged`
    /// startup invariant. Empty in sim runs (oid 0 means "unknown";
    /// the invariant skips on 0).
    table_oids: BTreeMap<TableIdent, u32>,
    /// How many tables [`Self::run_concurrently`] reads at once.
    concurrency: Option<Concurrency>,
}

/// Up to `tables` tables snapshotted at once, each on a task of its own
/// that `spawn` starts ([`Snapshotter::run_concurrently`]).
#[derive(Clone)]
pub struct Concurrency {
    pub tables: usize,
    pub spawn: Spawn,
}

impl Snapshotter {
    pub fn new(coord: Arc<dyn Coordinator>) -> Self {
        Self {
            coord,
            chunk_size: DEFAULT_CHUNK_SIZE,
            table_oids: BTreeMap::new(),
            concurrency: None,
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

    /// Read up to `concurrency.tables` tables at once in
    /// [`Self::run_concurrently`].
    pub fn with_concurrency(mut self, concurrency: Concurrency) -> Self {
        assert!(concurrency.tables > 0, "tables must be > 0");
        self.concurrency = Some(concurrency);
        self
    }

    /// Run the snapshot to completion (all tables, all chunks), one
    /// table at a time.
    pub async fn run<S, K>(&self, source: &S, schemas: &[TableSchema], sink: &mut K) -> Result<Lsn>
    where
        S: SnapshotSource + ?Sized,
        K: SnapshotSink,
    {
        self.run_chunks(source, schemas, sink, None).await
    }

    /// Run at most `max_chunks` chunks across all tables, then return.
    /// `None` = unlimited. Used by tests to simulate a mid-snapshot
    /// crash: after `run_chunks(.., Some(N))`, drop the pipeline +
    /// Snapshotter, recreate them, and call `run` to finish — the
    /// per-table progress rows drive correct resumption.
    pub async fn run_chunks<S, K>(
        &self,
        source: &S,
        schemas: &[TableSchema],
        sink: &mut K,
        max_chunks: Option<usize>,
    ) -> Result<Lsn>
    where
        S: SnapshotSource + ?Sized,
        K: SnapshotSink,
    {
        let snap_lsn = source.snapshot_lsn().await?;
        let mut budget = max_chunks;
        for (i, schema) in schemas.iter().enumerate() {
            let table = TableRun::new(schema, i, snap_lsn)?;
            if !self.table(source, &table, sink, &mut budget).await? {
                break;
            }
        }
        Ok(snap_lsn)
    }

    /// Like [`Self::run`], but up to [`Self::with_concurrency`] tasks at
    /// once, each reading a table — or a range of a large table's keys —
    /// and putting it to a sink of its own ([`SnapshotSink::split_off`],
    /// [`SnapshotSink::split_range`]): reads and puts are CPU and round
    /// trips that don't wait on one another. `source` serves them all at
    /// once. A table whose snapshot fails, or a range of it, stops the
    /// snapshot once the tasks running finish. One at a time, if the sink
    /// can't split.
    pub async fn run_concurrently<S, K>(
        &self,
        source: Arc<S>,
        schemas: &[TableSchema],
        sink: &mut K,
    ) -> Result<Lsn>
    where
        S: SnapshotSource + ?Sized + 'static,
        K: SnapshotSink,
    {
        let concurrency = self.concurrency.clone().filter(|_| sink.splits());
        let Some(Concurrency {
            tables: tasks,
            spawn,
        }) = concurrency
        else {
            return self.run(source.as_ref(), schemas, sink).await;
        };
        let snap_lsn = source.snapshot_lsn().await?;
        let mut tables = schemas.iter().cloned().enumerate();
        let mut queue: VecDeque<Work<K>> = VecDeque::new();
        // Tables read in ranges, not yet complete: their ranges yet to land.
        let mut ranged: BTreeMap<TableIdent, usize> = BTreeMap::new();
        let mut running = FuturesUnordered::new();
        let mut failed = None;
        loop {
            while failed.is_none() && running.len() < tasks {
                let work = match queue.pop_front() {
                    Some(work) => work,
                    None => {
                        let Some((i, schema)) = tables.next() else {
                            break;
                        };
                        match self.plan(source.as_ref(), sink, i, schema, tasks).await {
                            Ok(works) => {
                                if let Some(Work::Range { schema, .. }) = works.first() {
                                    ranged.insert(schema.ident.clone(), works.len());
                                }
                                queue.extend(works);
                            }
                            Err(e) => failed = Some(e),
                        }
                        continue;
                    }
                };
                let this = self.clone();
                let source = source.clone();
                let (done, landed) = oneshot::channel();
                match work {
                    Work::Table { i, schema } => {
                        let mut part = match sink.split_off(&schema.ident) {
                            Ok(part) => part,
                            Err(e) => {
                                failed = Some(e);
                                break;
                            }
                        };
                        spawn(Box::pin(async move {
                            let out = match TableRun::new(&schema, i, snap_lsn) {
                                Ok(table) => this
                                    .table(source.as_ref(), &table, &mut part, &mut None)
                                    .await
                                    .map(|_| ()),
                                Err(e) => Err(e),
                            };
                            // Unheard if the snapshot is gone.
                            let _ = done.send(Done::Table { part, out });
                        }));
                    }
                    Work::Range {
                        i,
                        schema,
                        range,
                        mut part,
                    } => {
                        spawn(Box::pin(async move {
                            let out = async {
                                let table = TableRun::new(&schema, i, snap_lsn)?;
                                this.read_range(source.as_ref(), &table, &range, &mut part)
                                    .await?;
                                part.seal(&schema.ident).await
                            }
                            .await;
                            // Unheard if the snapshot is gone. Failed, the
                            // part is dropped: what it put, with it.
                            let _ = done.send(Done::Range { schema, part, out });
                        }));
                    }
                }
                running.push(landed);
            }
            match running.next().await {
                Some(Ok(Done::Table { part, out })) => {
                    // Its table done or abandoned, either way.
                    sink.rejoin(part);
                    if let Err(e) = out {
                        failed.get_or_insert(e);
                    }
                }
                Some(Ok(Done::Range { schema, part, out })) => {
                    if let Err(e) = out {
                        failed.get_or_insert(e);
                        continue;
                    }
                    let ident = &schema.ident;
                    sink.rejoin_range(ident, part);
                    let left = ranged.get_mut(ident).expect("a ranged table");
                    *left -= 1;
                    if *left > 0 {
                        continue;
                    }
                    // Its last range landed: the table is whole, committed
                    // on a task of its own, as one read whole is — even
                    // with another table failed, like one read whole that
                    // was running.
                    ranged.remove(ident);
                    let mut part = match sink.split_off(ident) {
                        Ok(part) => part,
                        Err(e) => {
                            sink.abandon(ident);
                            failed = Some(e);
                            continue;
                        }
                    };
                    let this = self.clone();
                    let (done, landed) = oneshot::channel();
                    spawn(Box::pin(async move {
                        let out = this.complete(&schema, snap_lsn, &mut part).await;
                        if out.is_err() {
                            part.abandon(&schema.ident);
                        }
                        // Unheard if the snapshot is gone.
                        let _ = done.send(Done::Table { part, out });
                    }));
                    running.push(landed);
                }
                Some(Err(oneshot::Canceled)) => {
                    failed.get_or_insert(SnapshotError::Abandoned);
                }
                None => break,
            }
        }
        match failed {
            Some(e) => {
                // What the ranges of a table not complete put is abandoned,
                // and those not read yet are dropped.
                for ident in ranged.keys() {
                    sink.abandon(ident);
                }
                Err(e)
            }
            None => Ok(snap_lsn),
        }
    }

    /// How `schema`'s table, the `i`th, is read: whole, or — read from its
    /// start, of `tasks` more than one, the sink and the source able — in
    /// ranges of its keys ([`SnapshotSource::split`]), each a task's, to a
    /// sink of its own. Nothing, if complete.
    async fn plan<S, K>(
        &self,
        source: &S,
        sink: &mut K,
        i: usize,
        schema: TableSchema,
        tasks: usize,
    ) -> Result<Vec<Work<K>>>
    where
        S: SnapshotSource + ?Sized,
        K: SnapshotSink,
    {
        match self.resume(&schema).await? {
            Resume::Complete => return Ok(Vec::new()),
            // Read on from its progress.
            Resume::From(Some(_)) => return Ok(vec![Work::Table { i, schema }]),
            Resume::From(None) => {}
        }
        if tasks < 2 || sink.split_range(&schema.ident, 2).is_none() {
            return Ok(vec![Work::Table { i, schema }]);
        }
        let ranges = source
            .split(
                &schema.pg_ident(),
                MAX_RANGES,
                self.chunk_size.saturating_mul(RANGE_CHUNKS),
            )
            .await?;
        if ranges.len() < 2 {
            return Ok(vec![Work::Table { i, schema }]);
        }
        // Each now: once one joins the table, the table holds rows.
        let n = ranges.len();
        ranges
            .into_iter()
            .map(|range| {
                let part = sink.split_range(&schema.ident, n).ok_or_else(|| {
                    SnapshotError::Source(format!("{} can't be read in ranges", schema.ident))
                })?;
                Ok(Work::Range {
                    i,
                    schema: schema.clone(),
                    range,
                    part,
                })
            })
            .collect()
    }

    /// Put the chunks of `range` of `table` to `sink`: no progress recorded,
    /// nor the table complete — that's for once every range of it landed
    /// ([`Self::complete`]).
    async fn read_range<S, K>(
        &self,
        source: &S,
        table: &TableRun<'_>,
        range: &KeyRange,
        sink: &mut K,
    ) -> Result<()>
    where
        S: SnapshotSource + ?Sized,
        K: SnapshotSink,
    {
        let pg_ident = table.schema.pg_ident();
        let mut last_pk_key: Option<String> = None;
        let mut rows = self.read(source, &pg_ident, Some(range), None).await?;
        loop {
            let span = work_span!(
                parent: None,
                "snapshot.chunk",
                table = %table.schema.ident,
                rows = tracing::field::Empty,
            );
            let chunk = self
                .chunk(
                    source,
                    table,
                    sink,
                    last_pk_key.as_deref(),
                    rows,
                    Some(range),
                )
                .instrument(span.clone())
                .await;
            pg2iceberg_logical::spans::record_outcome(&span, &chunk);
            match chunk? {
                Chunk::TableDone => return Ok(()),
                Chunk::Staged {
                    last_pk_key: key,
                    next,
                } => {
                    last_pk_key = Some(key);
                    rows = next;
                }
            }
        }
    }

    /// The chunk of `ident` after `after`, of `range` if given.
    async fn read<S: SnapshotSource + ?Sized>(
        &self,
        source: &S,
        ident: &TableIdent,
        range: Option<&KeyRange>,
        after: Option<&str>,
    ) -> Result<Vec<Row>> {
        match range {
            None => source.read_chunk(ident, self.chunk_size, after).await,
            Some(range) => {
                source
                    .read_range(ident, range, self.chunk_size, after)
                    .await
            }
        }
    }

    /// `schema`'s table is read: make what's been put of it durable, and
    /// mark it complete.
    async fn complete<K: SnapshotSink>(
        &self,
        schema: &TableSchema,
        snap_lsn: Lsn,
        sink: &mut K,
    ) -> Result<()> {
        sink.finish(&schema.ident).await?;
        // Drop the resume cursor and stamp the per-table completion row.
        // Order matters: we clear progress *first* so a crash between the
        // two writes leaves the table marked not-yet-complete (snapshotter
        // retries cleanly) rather than complete-without-cursor (no-op
        // resume; correct).
        self.coord
            .clear_snapshot_progress(&schema.ident)
            .await
            .map_err(|e| SnapshotError::Source(format!("clear_snapshot_progress: {e}")))?;
        let oid = self.table_oids.get(&schema.ident).copied().unwrap_or(0);
        self.coord
            .mark_table_snapshot_complete(&schema.ident, oid, snap_lsn)
            .await
            .map_err(|e| SnapshotError::Source(format!("mark_table_snapshot_complete: {e}")))?;
        Ok(())
    }

    /// Snapshot `table`: resume it, then put its chunks until it's read
    /// to the end and marked complete, or until `budget` chunks are put.
    /// Whether the budget lasted. A mid-snapshot fault leaves the table's
    /// progress row in `snapshot_progress`, past the chunks durable: a
    /// resumed pass picks up there. What isn't durable is abandoned.
    async fn table<S, K>(
        &self,
        source: &S,
        table: &TableRun<'_>,
        sink: &mut K,
        budget: &mut Option<usize>,
    ) -> Result<bool>
    where
        S: SnapshotSource + ?Sized,
        K: SnapshotSink,
    {
        let out = self.read_table(source, table, sink, budget).await;
        if out.is_err() {
            sink.abandon(&table.schema.ident);
        }
        out
    }

    async fn read_table<S, K>(
        &self,
        source: &S,
        table: &TableRun<'_>,
        sink: &mut K,
        budget: &mut Option<usize>,
    ) -> Result<bool>
    where
        S: SnapshotSource + ?Sized,
        K: SnapshotSink,
    {
        let schema = table.schema;
        let resume = {
            // Closed here, before the chunks: a span is exported once closed.
            let span = work_span!(parent: None, "snapshot.table", table = %schema.ident);
            let resume = self.resume(schema).instrument(span.clone()).await;
            pg2iceberg_logical::spans::record_outcome(&span, &resume);
            resume
        };
        let Resume::From(mut last_pk_key) = resume? else {
            return Ok(true);
        };
        // The table's first chunk; each next one is read while the one
        // before it is put ([`Self::chunk`]).
        let mut rows = self
            .read(source, &schema.pg_ident(), None, last_pk_key.as_deref())
            .await?;
        loop {
            if *budget == Some(0) {
                return Ok(false);
            }
            // A table can take hours: each chunk is a trace of its own
            // (`snapshot.chunk`), not part of one for the table.
            let span = work_span!(
                parent: None,
                "snapshot.chunk",
                table = %schema.ident,
                rows = tracing::field::Empty,
            );
            let chunk = self
                .chunk(source, table, sink, last_pk_key.as_deref(), rows, None)
                .instrument(span.clone())
                .await;
            pg2iceberg_logical::spans::record_outcome(&span, &chunk);
            match chunk? {
                Chunk::TableDone => return Ok(true),
                Chunk::Staged {
                    last_pk_key: key,
                    next,
                } => {
                    last_pk_key = Some(key);
                    rows = next;
                    if let Some(left) = budget {
                        *left -= 1;
                    }
                }
            }
        }
    }

    /// Where `schema`'s snapshot resumes: after the last chunk a prior
    /// pass made durable, if any — or nowhere, the table being complete.
    async fn resume(&self, schema: &TableSchema) -> Result<Resume> {
        // Cheap single-row read; avoids re-reading the source for
        // already-snapshotted data.
        if let Some(state) = self
            .coord
            .table_state(&schema.ident)
            .await
            .map_err(|e| SnapshotError::Source(format!("table_state: {e}")))?
        {
            if state.snapshot_complete {
                return Ok(Resume::Complete);
            }
        }
        let last_pk_key = self
            .coord
            .snapshot_progress(&schema.ident)
            .await
            .map_err(|e| SnapshotError::Source(format!("snapshot_progress: {e}")))?;
        Ok(Resume::From(last_pk_key))
    }

    /// Put `chunk`, the rows of `table` after `last_pk_key`, of `range` if
    /// given — or, with nothing left to read of the whole table, make its
    /// chunks durable and mark it complete. The next chunk is read from
    /// Postgres meanwhile: a read and a put are each a round trip or more,
    /// and neither needs the other.
    async fn chunk<S, K>(
        &self,
        source: &S,
        table: &TableRun<'_>,
        sink: &mut K,
        last_pk_key: Option<&str>,
        chunk: Vec<Row>,
        range: Option<&KeyRange>,
    ) -> Result<Chunk>
    where
        S: SnapshotSource + ?Sized,
        K: SnapshotSink,
    {
        let TableRun {
            schema,
            ref pk_cols,
            snap_lsn,
            ..
        } = *table;
        // PG-side ident — `read_chunk` issues a SELECT against
        // the source PG table, so namespace must be the PG
        // schema (not the Iceberg `sink.namespace`).
        let pg_ident = schema.pg_ident();
        tracing::Span::current().record("rows", chunk.len());
        if chunk.is_empty() {
            if range.is_none() {
                self.complete(schema, snap_lsn, sink).await?;
            }
            return Ok(Chunk::TableDone);
        }

        let last_in_chunk = chunk.last().unwrap();
        let new_key = source.cursor(&pg_ident, last_pk_key, last_in_chunk, pk_cols);

        if let Some(prev) = last_pk_key {
            if new_key.as_str() == prev {
                return Err(SnapshotError::Source(format!(
                    "snapshot source did not advance past PK {prev} for table {}; got {new_key}",
                    schema.ident
                )));
            }
        }

        // A range's progress isn't the table's: none is recorded.
        let progress = range.is_none().then_some(new_key.as_str());
        let (put, next) = futures::join!(
            self.put(table, sink, chunk, progress),
            self.read(source, &pg_ident, range, Some(&new_key)),
        );
        put?;
        Ok(Chunk::Staged {
            last_pk_key: new_key,
            next: next?,
        })
    }

    /// Put `chunk`, `table`'s through `new_key`; once the chunks put are
    /// durable, stamp the resume cursor there, if given.
    async fn put<K: SnapshotSink>(
        &self,
        table: &TableRun<'_>,
        sink: &mut K,
        chunk: Vec<Row>,
        new_key: Option<&str>,
    ) -> Result<()> {
        let TableRun {
            schema,
            xid,
            snap_lsn,
            ..
        } = *table;
        let durable = sink.put(&schema.ident, xid, snap_lsn, chunk).await?;
        let Some(new_key) = new_key.filter(|_| durable) else {
            return Ok(());
        };
        // Stamp the resume cursor *after* the chunk's durable — if we
        // crash before this write, resume re-reads the chunk (idempotent:
        // the materializer's fold-by-PK absorbs the duplicate rows).
        self.coord
            .set_snapshot_progress(&schema.ident, new_key)
            .await
            .map_err(|e| SnapshotError::Source(format!("set_snapshot_progress: {e}")))?;
        Ok(())
    }
}

/// Most ranges a table is read in ([`SnapshotSource::split`]).
const MAX_RANGES: usize = 64;
/// Fewest chunks a range of a table holds, about: each is a task, and a
/// part of the table's index of its own.
const RANGE_CHUNKS: usize = 10;

/// A [`Snapshotter::run_concurrently`] task's work.
enum Work<K> {
    /// The `i`th table, read whole.
    Table { i: usize, schema: TableSchema },
    /// A range of the `i`th table's keys, put to `part`.
    Range {
        i: usize,
        schema: TableSchema,
        range: KeyRange,
        part: K,
    },
}

/// A [`Work`] done, its sink to take back.
enum Done<K> {
    Table {
        part: K,
        out: Result<()>,
    },
    Range {
        schema: TableSchema,
        part: K,
        out: Result<()>,
    },
}

/// One table of a [`Snapshotter`] run.
struct TableRun<'a> {
    schema: &'a TableSchema,
    /// The synthetic transaction id its chunks are put under.
    xid: u32,
    pk_cols: Vec<ColumnName>,
    snap_lsn: Lsn,
}

impl<'a> TableRun<'a> {
    /// The `i`th table of a run.
    fn new(schema: &'a TableSchema, i: usize, snap_lsn: Lsn) -> Result<Self> {
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
        Ok(Self {
            schema,
            xid: SNAPSHOT_XID_BASE.wrapping_add(i as u32),
            pk_cols,
            snap_lsn,
        })
    }
}

/// Where a table's snapshot resumes.
enum Resume {
    Complete,
    /// After this primary key, or from the start.
    From(Option<String>),
}

/// What [`Snapshotter::chunk`] did.
enum Chunk {
    /// Nothing left: the table is marked complete.
    TableDone,
    /// Put, through the row with this primary key; the chunk after it,
    /// read meanwhile.
    Staged { last_pk_key: String, next: Vec<Row> },
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
/// 3. Run [`Snapshotter`] into `sink`, up to `concurrency` tables at
///    once. Progress persists into `snapshot_progress` as chunks are
///    durable, so a crash + restart resumes past them.
///
/// **What this function does not do** (caller's responsibility):
/// - `stream.send_standby(snap_lsn, snap_lsn)` to advance the slot.
/// - With a staging sink, `materializer.cycle()` to publish snapshot
///   rows to Iceberg.
///
/// **Resumability:** a fault leaves each table's progress past its
/// durable chunks — with a staging sink, every chunk before the one that
/// failed; with a writing one, none until the table's committed. On
/// restart, the next call re-loads progress and resumes there. The
/// fault-DSTs prove this:
/// `binary_snapshot_phase_resumes_after_mid_chunk_blob_put_fault`,
/// `a_snapshot_retried_after_a_failed_chunk_starts_its_table_over`.
#[allow(clippy::too_many_arguments)]
pub async fn run_snapshot_phase<S, K>(
    source: Arc<S>,
    coord: Arc<dyn Coordinator>,
    schemas: &[TableSchema],
    skip_idents: &std::collections::BTreeSet<TableIdent>,
    table_oids: &std::collections::BTreeMap<TableIdent, u32>,
    sink: &mut K,
    chunk_size: usize,
    concurrency: Option<Concurrency>,
) -> Result<SnapshotPhaseOutcome>
where
    S: SnapshotSource + ?Sized + 'static,
    K: SnapshotSink,
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
    let mut snapshotter = Snapshotter::new(coord.clone())
        .with_chunk_size(chunk_size)
        .with_table_oids(table_oids.clone());
    if let Some(concurrency) = concurrency {
        snapshotter = snapshotter.with_concurrency(concurrency);
    }
    let snap_lsn = snapshotter
        .run_concurrently(source, &to_snapshot, sink)
        .await?;

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
