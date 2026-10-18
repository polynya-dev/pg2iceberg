//! pg2iceberg against stores a round trip away, as a real object store
//! and catalog are: requests that don't depend on each other overlap,
//! and at most a fixed number run at once.
//!
//! Every store call takes `LATENCY` of tokio's paused time, so a test
//! measures round trips exactly and runs instantly.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use pg2iceberg_coord::schema::CoordSchema;
use pg2iceberg_coord::{
    CommitBatch, CoordCommitReceipt, Coordinator, LogEntry, MarkerInfo, Result as CoordResult,
    TableSnapshotState,
};
use pg2iceberg_core::{
    ColumnName, ColumnSchema, IcebergType, Lsn, Namespace, PgValue, Row, TableIdent, TableSchema,
    Timestamp, WorkerId,
};
use pg2iceberg_iceberg::read_materialized_state;
use pg2iceberg_logical::materializer::{CounterMaterializerNamer, Materializer};
use pg2iceberg_logical::pipeline::{CounterBlobNamer, Pipeline};
use pg2iceberg_logical::staged_log::READ_AHEAD;
use pg2iceberg_sim::blob::MemoryBlobStore;
use pg2iceberg_sim::catalog::MemoryCatalog;
use pg2iceberg_sim::clock::TestClock;
use pg2iceberg_sim::coord::MemoryCoordinator;
use pg2iceberg_sim::postgres::{SimPostgres, SimReplicationStream};
use pg2iceberg_stream::{BlobInfo, BlobStore, Result as BlobResult};
use tokio::sync::{Mutex, RwLock};
use tokio::time::Instant;

const SLOT: &str = "slot-concurrency";
const PUB: &str = "pub-concurrency";
const LATENCY: Duration = Duration::from_millis(100);
/// Longer than any cycle here takes: one that hasn't finished by then
/// never will.
const STALLED: Duration = Duration::from_secs(3600);

fn ident() -> TableIdent {
    TableIdent {
        namespace: Namespace(vec!["public".into()]),
        name: "orders".into(),
    }
}

fn column(name: &str, field_id: i32, ty: IcebergType, pk: bool) -> ColumnSchema {
    ColumnSchema {
        name: name.into(),
        field_id,
        ty,
        nullable: !pk,
        is_primary_key: pk,
    }
}

fn schema() -> TableSchema {
    TableSchema {
        ident: ident(),
        columns: vec![
            column("id", 1, IcebergType::Int, true),
            column("qty", 2, IcebergType::Int, false),
            column("note", 3, IcebergType::String, false),
        ],
        partition_spec: vec![],
        pg_schema: None,
    }
}

fn row(id: i32, qty: i32) -> Row {
    Row::from([
        (ColumnName("id".into()), PgValue::Int4(id)),
        (ColumnName("qty".into()), PgValue::Int4(qty)),
        (
            ColumnName("note".into()),
            PgValue::Text(format!("note {id}")),
        ),
    ])
}

/// Reads in flight now, and the most there have been at once.
#[derive(Default)]
struct InFlight {
    now: AtomicUsize,
    peak: AtomicUsize,
}

impl InFlight {
    fn peak(&self) -> usize {
        self.peak.load(Ordering::SeqCst)
    }

    fn reset_peak(&self) {
        self.peak.store(0, Ordering::SeqCst);
    }
}

/// Counts a read in flight until dropped: completed or abandoned.
struct Reading<'a>(&'a InFlight);

impl<'a> Reading<'a> {
    fn start(reads: &'a InFlight) -> Self {
        let now = reads.now.fetch_add(1, Ordering::SeqCst) + 1;
        reads.peak.fetch_max(now, Ordering::SeqCst);
        Self(reads)
    }
}

impl Drop for Reading<'_> {
    fn drop(&mut self) {
        self.0.now.fetch_sub(1, Ordering::SeqCst);
    }
}

/// The stores a test's materializer works against.
#[derive(Clone, Copy)]
struct Stores {
    batch_rows: usize,
    /// The most events a cycle reads before it stops.
    cycle_rows: usize,
    /// How long the `n`th read takes.
    read_latency: fn(usize) -> Duration,
    /// The pieces a read's bytes arrive in, each only once the last is
    /// taken — a large file streams in over many round trips, and only
    /// while its reader reads it.
    read_pieces: u32,
    /// The object store request, by number, that finds the credentials
    /// expired, and refreshes them.
    expire_at: Option<usize>,
    /// Whether the coordinator takes one request at a time, a round trip
    /// each, as `PostgresCoordinator` on its one connection does.
    one_connection: bool,
}

impl Stores {
    fn new(batch_rows: usize) -> Self {
        Self {
            batch_rows,
            cycle_rows: usize::MAX,
            read_latency: |_| LATENCY,
            read_pieces: 1,
            expire_at: None,
            one_connection: false,
        }
    }
}

/// `MemoryBlobStore` a round trip away: every call takes `LATENCY`, but
/// the `n`th read `read_latency(n)`.
///
/// Every request first checks its credentials, under a lock that the
/// request finding them expired holds while it refreshes them from the
/// catalog — `VendedBlobStoreRouter`'s.
struct SlowBlobStore {
    inner: Arc<MemoryBlobStore>,
    stores: Stores,
    reads: InFlight,
    gets: AtomicUsize,
    requests: AtomicUsize,
    credentials: RwLock<()>,
}

impl SlowBlobStore {
    async fn authorize(&self) {
        let n = self.requests.fetch_add(1, Ordering::SeqCst);
        if self.stores.expire_at == Some(n) {
            let _refreshing = self.credentials.write().await;
            tokio::time::sleep(10 * LATENCY).await;
        } else {
            let _checking = self.credentials.read().await;
        }
    }
}

#[async_trait]
impl BlobStore for SlowBlobStore {
    async fn put(&self, path: &str, bytes: Bytes) -> BlobResult<()> {
        self.authorize().await;
        tokio::time::sleep(LATENCY).await;
        self.inner.put(path, bytes).await
    }

    async fn get(&self, path: &str) -> BlobResult<Bytes> {
        let _reading = Reading::start(&self.reads);
        self.authorize().await;
        let n = self.gets.fetch_add(1, Ordering::SeqCst);
        let pieces = self.stores.read_pieces;
        for _ in 0..pieces {
            tokio::time::sleep((self.stores.read_latency)(n) / pieces).await;
        }
        self.inner.get(path).await
    }

    async fn list(&self, prefix: &str) -> BlobResult<Vec<BlobInfo>> {
        self.authorize().await;
        tokio::time::sleep(LATENCY).await;
        self.inner.list(prefix).await
    }

    async fn delete(&self, path: &str) -> BlobResult<()> {
        self.authorize().await;
        tokio::time::sleep(LATENCY).await;
        self.inner.delete(path).await
    }
}

/// `MemoryCoordinator` on one connection a round trip away: a request
/// holds it until answered, and the others wait their turn.
struct OneConnection {
    inner: Arc<MemoryCoordinator>,
    connection: Mutex<()>,
}

impl OneConnection {
    async fn request<T>(&self, call: impl std::future::Future<Output = T>) -> T {
        let _connection = self.connection.lock().await;
        tokio::time::sleep(LATENCY).await;
        call.await
    }
}

#[async_trait]
impl Coordinator for OneConnection {
    async fn claim_offsets(&self, batch: &CommitBatch) -> CoordResult<CoordCommitReceipt> {
        self.request(self.inner.claim_offsets(batch)).await
    }
    async fn read_log(
        &self,
        table: &TableIdent,
        after_offset: u64,
        limit: usize,
    ) -> CoordResult<Vec<LogEntry>> {
        self.request(self.inner.read_log(table, after_offset, limit))
            .await
    }
    async fn truncate_log(
        &self,
        table: &TableIdent,
        before_offset: u64,
    ) -> CoordResult<Vec<String>> {
        self.request(self.inner.truncate_log(table, before_offset))
            .await
    }
    async fn ensure_cursor(&self, group: &str, table: &TableIdent) -> CoordResult<()> {
        self.request(self.inner.ensure_cursor(group, table)).await
    }
    async fn get_cursor(&self, group: &str, table: &TableIdent) -> CoordResult<Option<i64>> {
        self.request(self.inner.get_cursor(group, table)).await
    }
    async fn set_cursor(&self, group: &str, table: &TableIdent, to_offset: i64) -> CoordResult<()> {
        self.request(self.inner.set_cursor(group, table, to_offset))
            .await
    }
    async fn register_consumer(
        &self,
        group: &str,
        worker: &WorkerId,
        ttl: Duration,
    ) -> CoordResult<()> {
        self.request(self.inner.register_consumer(group, worker, ttl))
            .await
    }
    async fn unregister_consumer(&self, group: &str, worker: &WorkerId) -> CoordResult<()> {
        self.request(self.inner.unregister_consumer(group, worker))
            .await
    }
    async fn active_consumers(&self, group: &str) -> CoordResult<Vec<WorkerId>> {
        self.request(self.inner.active_consumers(group)).await
    }
    async fn try_lock(
        &self,
        table: &TableIdent,
        worker: &WorkerId,
        ttl: Duration,
    ) -> CoordResult<bool> {
        self.request(self.inner.try_lock(table, worker, ttl)).await
    }
    async fn renew_lock(
        &self,
        table: &TableIdent,
        worker: &WorkerId,
        ttl: Duration,
    ) -> CoordResult<bool> {
        self.request(self.inner.renew_lock(table, worker, ttl))
            .await
    }
    async fn release_lock(&self, table: &TableIdent, worker: &WorkerId) -> CoordResult<()> {
        self.request(self.inner.release_lock(table, worker)).await
    }
    async fn pipeline_system_identifier(&self) -> CoordResult<u64> {
        self.request(self.inner.pipeline_system_identifier()).await
    }
    async fn set_pipeline_system_identifier(&self, sysid: u64) -> CoordResult<()> {
        self.request(self.inner.set_pipeline_system_identifier(sysid))
            .await
    }
    async fn flushed_lsn(&self) -> CoordResult<Lsn> {
        self.request(self.inner.flushed_lsn()).await
    }
    async fn set_flushed_lsn(&self, lsn: Lsn) -> CoordResult<()> {
        self.request(self.inner.set_flushed_lsn(lsn)).await
    }
    async fn replicated_lsn(&self) -> CoordResult<Lsn> {
        self.request(self.inner.replicated_lsn()).await
    }
    async fn table_state(&self, ident: &TableIdent) -> CoordResult<Option<TableSnapshotState>> {
        self.request(self.inner.table_state(ident)).await
    }
    async fn mark_table_snapshot_complete(
        &self,
        ident: &TableIdent,
        pg_oid: u32,
        snapshot_lsn: Lsn,
    ) -> CoordResult<()> {
        self.request(
            self.inner
                .mark_table_snapshot_complete(ident, pg_oid, snapshot_lsn),
        )
        .await
    }
    async fn bump_table_epoch(&self, table: &TableIdent) -> CoordResult<i64> {
        self.request(self.inner.bump_table_epoch(table)).await
    }
    async fn table_epochs(&self, tables: &[TableIdent]) -> CoordResult<BTreeMap<TableIdent, i64>> {
        self.request(self.inner.table_epochs(tables)).await
    }
    async fn snapshot_progress(&self, ident: &TableIdent) -> CoordResult<Option<String>> {
        self.request(self.inner.snapshot_progress(ident)).await
    }
    async fn set_snapshot_progress(
        &self,
        ident: &TableIdent,
        last_pk_key: &str,
    ) -> CoordResult<()> {
        self.request(self.inner.set_snapshot_progress(ident, last_pk_key))
            .await
    }
    async fn clear_snapshot_progress(&self, ident: &TableIdent) -> CoordResult<()> {
        self.request(self.inner.clear_snapshot_progress(ident))
            .await
    }
    async fn pending_markers_for_table(
        &self,
        table: &TableIdent,
        cursor: i64,
    ) -> CoordResult<Vec<MarkerInfo>> {
        self.request(self.inner.pending_markers_for_table(table, cursor))
            .await
    }
    async fn record_marker_emitted(&self, uuid: &str, table: &TableIdent) -> CoordResult<()> {
        self.request(self.inner.record_marker_emitted(uuid, table))
            .await
    }
}

/// A pipeline staging into memory, and a materializer reading what it
/// staged from slow stores.
struct Harness {
    db: SimPostgres,
    stream: SimReplicationStream,
    pipeline: Pipeline<MemoryCoordinator>,
    materializer: Materializer<MemoryCatalog>,
    blob: Arc<SlowBlobStore>,
    catalog: Arc<MemoryCatalog>,
}

impl Harness {
    async fn boot(stores: Stores) -> Self {
        let db = SimPostgres::new();
        db.create_table(schema()).unwrap();
        db.create_publication(PUB, &[ident()]).unwrap();
        db.create_slot(SLOT, PUB).unwrap();
        let stream = db.start_replication(SLOT).unwrap();

        let memory_coord = Arc::new(MemoryCoordinator::new(
            CoordSchema::default_name(),
            Arc::new(TestClock::at(0)),
        ));
        let coord: Arc<dyn Coordinator> = if stores.one_connection {
            Arc::new(OneConnection {
                inner: memory_coord.clone(),
                connection: Mutex::new(()),
            })
        } else {
            memory_coord.clone()
        };
        let memory = Arc::new(MemoryBlobStore::new());
        let blob = Arc::new(SlowBlobStore {
            inner: memory.clone(),
            stores,
            reads: InFlight::default(),
            gets: AtomicUsize::new(0),
            requests: AtomicUsize::new(0),
            credentials: RwLock::new(()),
        });
        let catalog = Arc::new(MemoryCatalog::new());
        let mut pipeline = Pipeline::new(
            memory_coord,
            memory,
            Arc::new(CounterBlobNamer::new("s3://stage")),
            64,
        );
        pipeline.register_primary_keys(ident(), vec![ColumnName("id".into())]);
        let mut materializer = Materializer::new(
            coord,
            blob.clone(),
            catalog.clone(),
            Arc::new(CounterMaterializerNamer::new("s3://table")),
            "default",
            stores.batch_rows,
        )
        .with_cycle_rows(stores.cycle_rows);
        materializer.register_table(schema()).await.unwrap();
        Self {
            db,
            stream,
            pipeline,
            materializer,
            blob,
            catalog,
        }
    }

    /// Commit `tx` in the source and stage it on its own: one log entry.
    async fn stage(&mut self, tx: impl FnOnce(&mut pg2iceberg_sim::postgres::TxHandle)) {
        let mut handle = self.db.begin_tx();
        tx(&mut handle);
        handle.commit(Timestamp(0)).unwrap();
        while let Some(msg) = self.stream.recv() {
            self.pipeline.process(msg).await.unwrap();
        }
        self.pipeline.flush().await.unwrap();
    }

    /// `rows` transactions inserting a row each: a log entry each.
    async fn stage_inserts(&mut self, rows: usize) {
        for i in 0..rows as i32 {
            self.stage(|tx| {
                tx.insert(&ident(), row(i, i));
            })
            .await;
        }
    }

    /// A materializer cycle; it fails the test if it stalls.
    async fn cycle(&mut self) -> usize {
        tokio::time::timeout(STALLED, self.materializer.cycle())
            .await
            .expect("the cycle stalled")
            .unwrap()
    }

    /// A materializer cycle, and how long it took.
    async fn timed_cycle(&mut self) -> Duration {
        let start = Instant::now();
        self.cycle().await;
        start.elapsed()
    }

    async fn assert_iceberg_matches_pg(&self) {
        let id = |r: &Row| match r.get(&ColumnName("id".into())) {
            Some(PgValue::Int4(v)) => *v,
            _ => 0,
        };
        let mut iceberg = read_materialized_state(
            self.catalog.as_ref(),
            self.blob.inner.as_ref(),
            &ident(),
            &schema(),
            &[ColumnName("id".into())],
        )
        .await
        .unwrap();
        let mut pg = self.db.read_table(&ident()).unwrap();
        iceberg.sort_by_key(id);
        pg.sort_by_key(id);
        assert_eq!(iceberg, pg);
    }
}

/// A cycle reads the staged chunks ahead of the one it's applying, so a
/// table's backlog costs a round trip per `READ_AHEAD` chunks, not one per
/// chunk — and no more than `READ_AHEAD` reads are in flight.
#[tokio::test(start_paused = true)]
async fn a_cycle_reads_staged_chunks_ahead() {
    // More than a page of the log.
    const STAGED: usize = 100;
    let mut h = Harness::boot(Stores::new(1024)).await;
    h.stage_inserts(STAGED).await;

    let took = h.timed_cycle().await;

    assert_eq!(h.blob.reads.peak(), READ_AHEAD, "reads in flight at once");
    // Then one more, to upload the step's data file.
    let round_trips = STAGED.div_ceil(READ_AHEAD) as u32 + 1;
    assert!(
        took <= LATENCY * round_trips,
        "{STAGED} staged chunks took {took:?}, {} round trips",
        took.as_millis() / LATENCY.as_millis(),
    );
    h.assert_iceberg_matches_pg().await;
}

/// Chunks read ahead are applied in log order, however their reads
/// finish: here every read finishes before every read issued before it.
/// Every chunk inserts a row and updates one they all share, and commits
/// on its own: a chunk applied out of order, or skipped, shows.
#[tokio::test(start_paused = true)]
async fn chunks_read_ahead_apply_in_order() {
    const CHUNKS: i32 = 32;
    // Two round trips sooner than the read before: sooner than any read
    // issued in the commits since, too.
    let sooner = |n: usize| 2 * LATENCY * (64 - n as u32);
    let mut h = Harness::boot(Stores {
        read_latency: sooner,
        ..Stores::new(2)
    })
    .await;
    h.stage(|tx| {
        tx.insert(&ident(), row(0, 0));
    })
    .await;
    for id in 1..CHUNKS {
        h.stage(|tx| {
            tx.insert(&ident(), row(id, id));
            tx.update(&ident(), row(0, id));
        })
        .await;
    }

    h.cycle().await;

    h.assert_iceberg_matches_pg().await;
}

/// The reads ahead go on while the cycle commits what it has read: here
/// each staged chunk takes four round trips to stream in, and each is its
/// own transaction, committed with an upload of one. Read only between
/// commits, the chunks would fall behind them; read during them, they
/// keep up, and the cycle takes a round trip per commit.
#[tokio::test(start_paused = true)]
async fn reads_ahead_go_on_while_the_cycle_commits() {
    const STAGED: usize = 64;
    let mut h = Harness::boot(Stores {
        read_latency: |_| 4 * LATENCY,
        read_pieces: 64,
        ..Stores::new(1)
    })
    .await;
    h.stage_inserts(STAGED).await;

    let took = h.timed_cycle().await;

    // The first chunks' reads, then the commits.
    let round_trips = 4 + STAGED as u32 + 1;
    assert!(
        took <= LATENCY * round_trips,
        "{STAGED} commits took {took:?}, {} round trips",
        took.as_millis() / LATENCY.as_millis(),
    );
    h.assert_iceberg_matches_pg().await;
}

/// What's read ahead is bounded, not just what's in flight: a cycle
/// committing one transaction at a time, its reads ahead each done in a
/// round trip, has read at most `READ_AHEAD` chunks past where it stops.
#[tokio::test(start_paused = true)]
async fn a_cycle_reads_at_most_read_ahead_past_where_it_stops() {
    let mut h = Harness::boot(Stores {
        cycle_rows: 4,
        ..Stores::new(1)
    })
    .await;
    h.stage_inserts(64).await;

    let applied = h.cycle().await;

    // And the entry it stopped at, read but left for the next cycle.
    let read = h.blob.gets.load(Ordering::SeqCst);
    assert!(
        read <= applied + 1 + READ_AHEAD,
        "read {read} chunks, applied {applied}"
    );
}

/// A read ahead that refreshes the object store's credentials holds the
/// lock every other request waits on until it's done: the cycle's own
/// uploads wait for it, so it has to go on meanwhile.
#[tokio::test(start_paused = true)]
async fn a_read_ahead_refreshing_credentials_doesnt_stall_the_cycle() {
    let mut h = Harness::boot(Stores {
        // The cycle's first reads all go out before it uploads anything:
        // the fourth is read ahead.
        expire_at: Some(3),
        ..Stores::new(1)
    })
    .await;
    h.stage_inserts(32).await;

    h.cycle().await;

    h.assert_iceberg_matches_pg().await;
}

/// On `PostgresCoordinator`'s one connection, listing the next page of
/// the log ahead holds the connection the cycle's own coordinator
/// requests wait for: it has to go on meanwhile.
#[tokio::test(start_paused = true)]
async fn listing_the_log_ahead_doesnt_stall_the_cycle() {
    let mut h = Harness::boot(Stores {
        one_connection: true,
        ..Stores::new(1)
    })
    .await;
    // Several pages of the log.
    h.stage_inserts(200).await;

    h.cycle().await;

    h.assert_iceberg_matches_pg().await;
}

/// A step whose updates leave TOASTed columns out reads the data files
/// holding their prior values together, not one after another.
#[tokio::test(start_paused = true)]
async fn a_step_reads_prior_rows_together() {
    const FILES: usize = 16;
    let mut h = Harness::boot(Stores::new(1024)).await;
    // One data file per row.
    for i in 0..FILES as i32 {
        h.stage(|tx| {
            tx.insert(&ident(), row(i, i));
        })
        .await;
        h.cycle().await;
    }
    // Each row updated, its note TOASTed: unchanged, so not sent.
    h.stage(|tx| {
        for i in 0..FILES as i32 {
            tx.update_with_unchanged(&ident(), row(i, i + 100), vec![ColumnName("note".into())]);
        }
    })
    .await;
    h.blob.reads.reset_peak();

    h.cycle().await;

    assert_eq!(h.blob.reads.peak(), READ_AHEAD, "reads in flight at once");
    h.assert_iceberg_matches_pg().await;
}
