//! Deterministic Simulation Test (DST) harness for the logical-replication
//! pipeline + materializer.
//!
//! For each randomly-generated workload, runs:
//!
//!   SimPostgres → SimReplicationStream → Pipeline.process → Sink → codec
//!     → MemoryBlobStore → MemoryCoordinator → CoordCommitReceipt → flushed_lsn
//!     → Materializer.cycle → fold → resolve_unchanged_cols → promote_re_inserts
//!     → TableWriter.prepare → MemoryCatalog.commit_snapshot → set_cursor
//!
//! Then asserts the plan §9 invariants:
//!
//! 1. Every `log_index.s3_path` resolves in the blob store.
//! 2. Per-table offsets are contiguous (start_i == end_{i-1}, first start = 0).
//! 3. `pipeline.flushed_lsn ≤ slot.confirmed_flush_lsn` after every ack.
//! 4. Staged events == committed WAL events (filtered to publication, sorted
//!    by LSN). This is the "no lost commits / no phantom commits" check.
//! 5. **PG ground truth == Iceberg materialized state at quiescence.** This
//!    is the headline correctness property of pg2iceberg: after a workload
//!    runs through the entire stack, `read_table(SimPostgres)` and
//!    `read_materialized_state(MemoryCatalog)` must be byte-equal (sorted
//!    by PK).
//! 6. **No WAL retention at quiescence:** `slot.confirmed_flush_lsn` ==
//!    end of WAL, even when the tail only touched unpublished tables.
//!
//! Workload generator interleaves `MaterializerCycle` with the pipeline
//! steps so the proptest exercises pipeline/materializer ordering, and
//! `CrashAndRestart` models pipeline-process crashes between flushes.
//!
//! `RestartMaterializer` crashes the materializer too: a new instance
//! rebuilds its FileIndex from catalog history, and must never reuse a
//! file path a committed snapshot references (the sim blob store refuses
//! overwrites).

use pg2iceberg_coord::schema::CoordSchema;
use pg2iceberg_coord::Coordinator;
use pg2iceberg_core::typemap::IcebergType;
use pg2iceberg_core::{
    ColumnName, ColumnSchema, Namespace, Op, PgValue, Row, TableIdent, TableSchema, Timestamp,
};
use pg2iceberg_iceberg::read_materialized_state;
use pg2iceberg_iceberg::{
    Catalog, PreparedCommit, PreparedCompaction, SchemaChange, Snapshot, TableMetadata,
};
use pg2iceberg_logical::materializer::{MaterializerNamer, UuidMaterializerNamer};
use pg2iceberg_logical::pipeline::CounterBlobNamer;
use pg2iceberg_logical::{Materializer, Pipeline};
use pg2iceberg_sim::blob::MemoryBlobStore;
use pg2iceberg_sim::catalog::MemoryCatalog;
use pg2iceberg_sim::clock::TestClock;
use pg2iceberg_sim::coord::MemoryCoordinator;
use pg2iceberg_sim::id::SeqIdGen;
use pg2iceberg_sim::postgres::{SimPostgres, SimReplicationStream};
use pg2iceberg_snapshot::Snapshotter;
use pg2iceberg_stream::codec::decode_chunk;
use pg2iceberg_stream::BlobStore;
use pollster::block_on;
use proptest::prelude::*;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};

const TABLE_NAME: &str = "orders";
const PUB: &str = "pub1";
const SLOT: &str = "slot1";
/// Rows the pipeline may buffer before it must stage them. Tiny so
/// ordinary workloads produce transactions that span many chunks.
const FLUSH_ROWS: usize = 3;
/// Bound on change events held in memory, and on rows per staged
/// object: a not-yet-staged transaction tail plus committed rows
/// awaiting a flush, each below `FLUSH_ROWS`.
const MAX_BUFFERED_ROWS: usize = 2 * FLUSH_ROWS;
/// Materializer batch limit. Tiny so a large transaction spans several
/// batches — it must still become visible in one step (invariant 10).
const MAT_BATCH: usize = 2;

fn ident() -> TableIdent {
    TableIdent {
        namespace: Namespace(vec!["public".into()]),
        name: TABLE_NAME.into(),
    }
}

fn schema() -> TableSchema {
    TableSchema {
        ident: ident(),
        columns: vec![
            ColumnSchema {
                name: "id".into(),
                field_id: 1,
                ty: IcebergType::Int,
                nullable: false,
                is_primary_key: true,
            },
            ColumnSchema {
                name: "qty".into(),
                field_id: 2,
                ty: IcebergType::Int,
                nullable: false,
                is_primary_key: false,
            },
        ],
        partition_spec: Vec::new(),
        pg_schema: None,
    }
}

fn row(id: i32, qty: i32) -> Row {
    let mut r = BTreeMap::new();
    r.insert(ColumnName("id".into()), PgValue::Int4(id));
    r.insert(ColumnName("qty".into()), PgValue::Int4(qty));
    r
}

/// A table outside the publication. Its WAL never reaches the
/// pipeline, so it models every write the slot must not stay pinned
/// behind: other app tables, other databases on the cluster, and
/// pg2iceberg's own coord writes.
fn noise_ident() -> TableIdent {
    TableIdent {
        namespace: Namespace(vec!["public".into()]),
        name: "noise".into(),
    }
}

fn noise_schema() -> TableSchema {
    TableSchema {
        ident: noise_ident(),
        ..schema()
    }
}

fn pk_only(id: i32) -> Row {
    let mut r = BTreeMap::new();
    r.insert(ColumnName("id".into()), PgValue::Int4(id));
    r
}

// ---------- workload model ----------

#[derive(Clone, Debug)]
enum Step {
    /// `BEGIN; INSERT id, qty; COMMIT` — skipped if `id` already exists.
    Insert { id: i32, qty: i32 },
    /// `BEGIN; UPDATE id SET qty=N; COMMIT` — skipped if `id` is missing.
    Update { id: i32, qty: i32 },
    /// `BEGIN; DELETE id; COMMIT` — skipped if `id` is missing.
    Delete { id: i32 },
    /// `BEGIN; INSERT id, qty; ROLLBACK`. Exercises the rollback path.
    RollbackInsert { id: i32, qty: i32 },
    /// `BEGIN; INSERT INTO noise ...; COMMIT` — WAL for a table outside
    /// the publication. pgoutput skips the whole transaction, so only a
    /// keepalive tells the pipeline it can ack past it.
    UnpublishedWrite { qty: i32 },
    /// One transaction that updates every live row and inserts `inserts`
    /// fresh ones — routinely bigger than `FLUSH_ROWS`, so it must be
    /// staged in chunks.
    BigTx { inserts: usize, qty: i32 },
    /// Process at most `n` replication messages; may stop mid-transaction.
    DrivePartial { n: usize },
    /// A flush tick + ack without draining the stream first.
    FlushTick,
    /// Hard crash: no drain, flush, or ack. Pipeline memory and any
    /// staged-but-unclaimed objects are lost; the slot replays from
    /// `restart_lsn`.
    CrashMidStream,
    /// Drive replication + flush + ack: a complete pipeline cycle.
    DriveFlush,
    /// Run one materializer cycle for every registered table.
    MaterializerCycle,
    /// `DriveFlush` followed by dropping the pipeline + stream and rebuilding
    /// from the slot's `restart_lsn`. The coord, blob_store, catalog, and
    /// materializer (with its in-memory FileIndex) survive — those represent
    /// durable storage and the parallel materializer worker process. Phase
    /// 8.5 will extend this to also crash the materializer.
    CrashAndRestart,
    /// Materializer-process crash: a new materializer — fresh file namer,
    /// FileIndex rebuilt from the catalog — over the same durable coord,
    /// catalog, and blob store.
    RestartMaterializer,
}

fn step_strategy() -> impl Strategy<Value = Step> {
    // Small id space so collisions / valid Update / valid Delete are common.
    let id = 1i32..=6;
    let qty = 0i32..=100;
    prop_oneof![
        5 => (id.clone(), qty.clone()).prop_map(|(id, qty)| Step::Insert { id, qty }),
        3 => (id.clone(), qty.clone()).prop_map(|(id, qty)| Step::Update { id, qty }),
        2 => id.clone().prop_map(|id| Step::Delete { id }),
        1 => (id.clone(), qty.clone()).prop_map(|(id, qty)| Step::RollbackInsert { id, qty }),
        3 => qty.clone().prop_map(|qty| Step::UnpublishedWrite { qty }),
        2 => (1usize..=8, qty.clone()).prop_map(|(inserts, qty)| Step::BigTx { inserts, qty }),
        2 => (1usize..=6).prop_map(|n| Step::DrivePartial { n }),
        1 => Just(Step::FlushTick),
        1 => Just(Step::CrashMidStream),
        3 => Just(Step::DriveFlush),
        2 => Just(Step::MaterializerCycle),
        1 => Just(Step::CrashAndRestart),
        1 => Just(Step::RestartMaterializer),
    ]
}

fn workload() -> impl Strategy<Value = Vec<Step>> {
    prop::collection::vec(step_strategy(), 1..=24)
}

// ---------- harness ----------

/// The production file namer: a fresh instance per materializer
/// incarnation, drawing from the shared UUID sequence.
fn mat_namer(id_gen: &Arc<SeqIdGen>) -> Arc<dyn MaterializerNamer> {
    Arc::new(UuidMaterializerNamer::new(id_gen.clone(), "s3://table"))
}

struct DstHarness {
    db: SimPostgres,
    coord: Arc<MemoryCoordinator>,
    blob_store: Arc<MemoryBlobStore>,
    catalog: Arc<MemoryCatalog>,
    namer: Arc<CounterBlobNamer>,
    pipeline: Pipeline<MemoryCoordinator>,
    materializer: Materializer<AuditedCatalog>,
    /// The materializer's catalog: checks invariant 10 after every commit.
    audited: Arc<AuditedCatalog>,
    stream: SimReplicationStream,
    /// Mirror of which PK ids are currently live in the source DB. Used by
    /// the workload runner to pre-filter ops the proptest generator can't
    /// know about (state-dependent validity).
    live: BTreeSet<i32>,
    /// Next PK for `noise` inserts (always fresh, so they never conflict).
    noise_next_id: i32,
    /// Last PK handed out by `BigTx` inserts (kept clear of `1..=6`).
    next_bulk_id: i32,
    /// UUID source for materializer file names, shared by every
    /// materializer incarnation — like real UUIDs, never repeating.
    id_gen: Arc<SeqIdGen>,
}

impl DstHarness {
    /// Boot with a set of pre-existing rows. The seeds are committed BEFORE
    /// the publication + slot are created, so logical replication won't see
    /// them — they have to come in via the snapshot phase.
    fn boot_with_seeds(seeds: &[(i32, i32)]) -> Self {
        let db = SimPostgres::new();
        db.create_table(schema()).unwrap();
        db.create_table(noise_schema()).unwrap();

        if !seeds.is_empty() {
            let mut tx = db.begin_tx();
            for (id, qty) in seeds {
                tx.insert(&ident(), row(*id, *qty));
            }
            tx.commit(Timestamp(0)).unwrap();
        }

        db.create_publication(PUB, &[ident()]).unwrap();
        db.create_slot(SLOT, PUB).unwrap();

        let clock = TestClock::at(0);
        let arc_clock: Arc<dyn pg2iceberg_core::Clock> = Arc::new(clock);
        let coord = Arc::new(MemoryCoordinator::new(
            CoordSchema::default_name(),
            arc_clock,
        ));
        let blob_store = Arc::new(MemoryBlobStore::new());
        let catalog = Arc::new(MemoryCatalog::new());
        let namer = Arc::new(CounterBlobNamer::new("s3://stage"));
        let pipeline = Pipeline::new(coord.clone(), blob_store.clone(), namer.clone(), FLUSH_ROWS);

        let id_gen = Arc::new(SeqIdGen::new());
        let mat_namer = mat_namer(&id_gen);
        let audited = Arc::new(AuditedCatalog {
            inner: catalog.clone(),
            blob: blob_store.clone(),
            db: db.clone(),
            violations: Mutex::new(Vec::new()),
            fail_next_commit: Default::default(),
        });
        let mut materializer = Materializer::new(
            coord.clone() as Arc<dyn Coordinator>,
            blob_store.clone(),
            audited.clone(),
            mat_namer,
            "default",
            MAT_BATCH,
        );
        block_on(materializer.register_table(schema())).unwrap();

        let stream = db.start_replication(SLOT).unwrap();

        Self {
            db,
            coord,
            blob_store,
            catalog,
            namer,
            pipeline,
            materializer,
            audited,
            stream,
            live: seeds.iter().map(|(id, _)| *id).collect(),
            noise_next_id: 0,
            next_bulk_id: 1000,
            id_gen,
        }
    }

    fn boot() -> Self {
        let db = SimPostgres::new();
        db.create_table(schema()).unwrap();
        db.create_table(noise_schema()).unwrap();
        db.create_publication(PUB, &[ident()]).unwrap();
        db.create_slot(SLOT, PUB).unwrap();

        let clock = TestClock::at(0);
        let arc_clock: Arc<dyn pg2iceberg_core::Clock> = Arc::new(clock);
        let coord = Arc::new(MemoryCoordinator::new(
            CoordSchema::default_name(),
            arc_clock,
        ));
        let blob_store = Arc::new(MemoryBlobStore::new());
        let catalog = Arc::new(MemoryCatalog::new());
        let namer = Arc::new(CounterBlobNamer::new("s3://stage"));
        let pipeline = Pipeline::new(coord.clone(), blob_store.clone(), namer.clone(), FLUSH_ROWS);

        let id_gen = Arc::new(SeqIdGen::new());
        let mat_namer = mat_namer(&id_gen);
        let audited = Arc::new(AuditedCatalog {
            inner: catalog.clone(),
            blob: blob_store.clone(),
            db: db.clone(),
            violations: Mutex::new(Vec::new()),
            fail_next_commit: Default::default(),
        });
        let mut materializer = Materializer::new(
            coord.clone() as Arc<dyn Coordinator>,
            blob_store.clone(),
            audited.clone(),
            mat_namer,
            "default",
            MAT_BATCH,
        );
        block_on(materializer.register_table(schema())).unwrap();

        let stream = db.start_replication(SLOT).unwrap();

        Self {
            db,
            coord,
            blob_store,
            catalog,
            namer,
            pipeline,
            materializer,
            audited,
            stream,
            live: BTreeSet::new(),
            noise_next_id: 0,
            next_bulk_id: 1000,
            id_gen,
        }
    }

    fn drive(&mut self) {
        while let Some(msg) = self.stream.recv() {
            block_on(self.pipeline.process(msg)).unwrap();
        }
    }

    /// Process at most `n` messages — may stop mid-transaction.
    fn drive_partial(&mut self, n: usize) {
        for _ in 0..n {
            match self.stream.recv() {
                Some(msg) => block_on(self.pipeline.process(msg)).unwrap(),
                None => break,
            }
        }
    }

    fn flush_and_ack(&mut self) {
        block_on(self.pipeline.flush()).unwrap();
        self.stream.send_standby(self.pipeline.flushed_lsn());
    }

    fn materialize(&mut self) -> usize {
        block_on(self.materializer.cycle()).unwrap()
    }

    /// Run a Snapshotter-driven snapshot phase against the harness's source.
    /// Acks the slot to the snapshot LSN so live replication picks up
    /// strictly-later events.
    fn run_snapshot(&mut self) {
        let s = Snapshotter::new(self.coord.clone() as Arc<dyn Coordinator>);
        let snap_lsn = block_on(s.run(&self.db, &[schema()], &mut self.pipeline)).unwrap();
        self.stream.send_standby(snap_lsn);
    }

    /// Pipeline-process crash. Slot, coord, and blob store survive (durable
    /// storage); pipeline state and replication-stream cursor are lost.
    fn restart_materializer(&mut self) {
        let mut materializer = Materializer::new(
            self.coord.clone() as Arc<dyn Coordinator>,
            self.blob_store.clone(),
            self.audited.clone(),
            mat_namer(&self.id_gen),
            "default",
            MAT_BATCH,
        );
        block_on(materializer.register_table(schema())).unwrap();
        self.materializer = materializer;
    }

    fn crash_and_restart(&mut self) {
        // Drain + ack first so we model "graceful crash after a flush" — the
        // simpler case. Mid-flush crashes (orphan blobs from PUT-without-claim)
        // are a tracked follow-up.
        self.drive();
        self.flush_and_ack();
        self.crash_mid_stream();
    }

    /// Drop the pipeline + stream as-is and rebuild from the slot.
    fn crash_mid_stream(&mut self) {
        self.pipeline = Pipeline::new(
            self.coord.clone(),
            self.blob_store.clone(),
            self.namer.clone(),
            FLUSH_ROWS,
        );
        self.stream = self.db.start_replication(SLOT).unwrap();
    }

    fn run_step(&mut self, step: &Step) {
        match step {
            Step::Insert { id, qty } => {
                if !self.live.contains(id) {
                    let mut tx = self.db.begin_tx();
                    tx.insert(&ident(), row(*id, *qty));
                    if tx.commit(Timestamp(0)).is_ok() {
                        self.live.insert(*id);
                    }
                }
            }
            Step::Update { id, qty } => {
                if self.live.contains(id) {
                    let mut tx = self.db.begin_tx();
                    tx.update(&ident(), row(*id, *qty));
                    let _ = tx.commit(Timestamp(0));
                }
            }
            Step::Delete { id } => {
                if self.live.contains(id) {
                    let mut tx = self.db.begin_tx();
                    tx.delete(&ident(), pk_only(*id));
                    if tx.commit(Timestamp(0)).is_ok() {
                        self.live.remove(id);
                    }
                }
            }
            Step::RollbackInsert { id, qty } => {
                let mut tx = self.db.begin_tx();
                tx.insert(&ident(), row(*id, *qty));
                tx.rollback();
            }
            Step::UnpublishedWrite { qty } => {
                self.noise_next_id += 1;
                let mut tx = self.db.begin_tx();
                tx.insert(&noise_ident(), row(self.noise_next_id, *qty));
                tx.commit(Timestamp(0)).unwrap();
            }
            Step::DriveFlush => {
                self.drive();
                self.flush_and_ack();
            }
            Step::MaterializerCycle => {
                let _ = self.materialize();
            }
            Step::CrashAndRestart => self.crash_and_restart(),
            Step::BigTx { inserts, qty } => {
                let mut tx = self.db.begin_tx();
                for id in &self.live {
                    tx.update(&ident(), row(*id, *qty));
                }
                let mut fresh = Vec::with_capacity(*inserts);
                for _ in 0..*inserts {
                    self.next_bulk_id += 1;
                    tx.insert(&ident(), row(self.next_bulk_id, *qty));
                    fresh.push(self.next_bulk_id);
                }
                // Touch every row again, now in a later chunk — including
                // the ones this transaction just inserted: the materializer
                // must hide their first version within the same atomic
                // commit.
                for id in self.live.iter().chain(&fresh) {
                    tx.update(&ident(), row(*id, *qty + 1));
                }
                tx.commit(Timestamp(0)).unwrap();
                self.live.extend(fresh);
            }
            Step::DrivePartial { n } => self.drive_partial(*n),
            Step::FlushTick => self.flush_and_ack(),
            Step::CrashMidStream => self.crash_mid_stream(),
            Step::RestartMaterializer => self.restart_materializer(),
        }
    }
}

// ---------- invariant checks ----------

/// Invariants that must hold after *every* step, not just at quiescence.
fn check_step_invariants(h: &DstHarness) -> Result<(), String> {
    // 7. Bounded memory: no transaction, however large, makes the
    //    pipeline hold more than MAX_BUFFERED_ROWS change events.
    let buffered = h.pipeline.buffered_rows();
    if buffered > MAX_BUFFERED_ROWS {
        return Err(format!(
            "invariant 7 (bounded memory): pipeline buffers {buffered} rows > {MAX_BUFFERED_ROWS}"
        ));
    }

    let entries = block_on(h.coord.read_log(&ident(), 0, 1_000_000))
        .map_err(|e| format!("read_log failed: {e}"))?;

    // 8. Bounded staged objects, so the materializer never has to load
    //    one huge file either.
    if let Some(e) = entries
        .iter()
        .find(|e| e.record_count as usize > MAX_BUFFERED_ROWS)
    {
        return Err(format!(
            "invariant 8 (bounded staged object): {} holds {} rows > {MAX_BUFFERED_ROWS}",
            e.s3_path, e.record_count
        ));
    }

    // 9. No torn transactions: each transaction is either entirely in the
    //    claimed log or absent. Staged-but-unclaimed objects don't count —
    //    only claims are visible to the materializer.
    let mut staged = BTreeSet::new();
    for entry in &entries {
        let bytes = block_on(h.blob_store.get(&entry.s3_path))
            .map_err(|e| format!("blob_store.get({}): {e}", entry.s3_path))?;
        let chunk =
            decode_chunk(&bytes).map_err(|e| format!("decode_chunk({}): {e}", entry.s3_path))?;
        staged.extend(chunk.into_iter().map(|m| m.lsn));
    }
    let mut by_xid: BTreeMap<u32, Vec<_>> = BTreeMap::new();
    for c in
        h.db.dump_change_events(PUB)
            .map_err(|e| format!("dump_change_events: {e}"))?
    {
        by_xid.entry(c.xid.unwrap_or(0)).or_default().push(c.lsn);
    }
    for (xid, lsns) in &by_xid {
        let claimed = lsns.iter().filter(|l| staged.contains(*l)).count();
        if claimed != 0 && claimed != lsns.len() {
            return Err(format!(
                "invariant 9 (torn transaction): xid {xid} has {claimed} of {} events claimed",
                lsns.len()
            ));
        }
    }

    // 10. Atomic visibility per table — now, and at every commit made
    //     since the last check (a cycle may commit several times).
    block_on(atomic_visibility(&h.catalog, &h.blob_store, &h.db))?;
    if let Some(v) = h.audited.violations.lock().unwrap().first() {
        return Err(v.clone());
    }
    Ok(())
}

/// Invariant 10: Iceberg matches PG as of some transaction boundary.
/// Lagging behind is fine; a partly applied transaction is not.
async fn atomic_visibility(
    catalog: &MemoryCatalog,
    blob: &MemoryBlobStore,
    db: &SimPostgres,
) -> Result<(), String> {
    let mut iceberg = read_materialized_state(
        catalog,
        blob,
        &ident(),
        &schema(),
        &[ColumnName("id".into())],
    )
    .await
    .map_err(|e| format!("read_materialized_state: {e}"))?;
    sort_by_pk(&mut iceberg);
    let events = db
        .dump_change_events(PUB)
        .map_err(|e| format!("dump_change_events: {e}"))?;
    let pk = |r: &Row| match r.get(&ColumnName("id".into())) {
        Some(PgValue::Int4(n)) => *n,
        _ => i32::MAX,
    };
    let mut state: BTreeMap<i32, Row> = BTreeMap::new();
    let mut boundaries: Vec<Vec<Row>> = vec![Vec::new()];
    let mut i = 0;
    while i < events.len() {
        let xid = events[i].xid;
        while i < events.len() && events[i].xid == xid {
            let e = &events[i];
            match e.op {
                Op::Insert | Op::Update => {
                    let after = e.after.clone().expect("insert/update carries after");
                    state.insert(pk(&after), after);
                }
                Op::Delete => {
                    state.remove(&pk(e.before.as_ref().expect("delete carries before")));
                }
                _ => state.clear(),
            }
            i += 1;
        }
        boundaries.push(state.values().cloned().collect());
    }
    if !boundaries.contains(&iceberg) {
        return Err(format!(
            "invariant 10 (atomic visibility): Iceberg state matches no transaction boundary: {iceberg:?}"
        ));
    }
    Ok(())
}

/// The materializer's catalog in the DST: delegates to the in-memory
/// catalog and checks invariant 10 after every commit — the moments a
/// reader could observe the table — since one materializer cycle can
/// commit several times between two DST steps.
struct AuditedCatalog {
    inner: Arc<MemoryCatalog>,
    blob: Arc<MemoryBlobStore>,
    db: SimPostgres,
    violations: Mutex<Vec<String>>,
    /// When set, the next multi-step commit fails without committing.
    fail_next_commit: std::sync::atomic::AtomicBool,
}

impl AuditedCatalog {
    async fn audit(&self) {
        if let Err(e) = atomic_visibility(&self.inner, &self.blob, &self.db).await {
            self.violations
                .lock()
                .unwrap()
                .push(format!("after a commit: {e}"));
        }
    }
}

#[async_trait::async_trait]
impl Catalog for AuditedCatalog {
    async fn ensure_namespace(&self, ns: &Namespace) -> pg2iceberg_iceberg::Result<()> {
        self.inner.ensure_namespace(ns).await
    }
    async fn load_table(
        &self,
        ident: &TableIdent,
    ) -> pg2iceberg_iceberg::Result<Option<TableMetadata>> {
        self.inner.load_table(ident).await
    }
    async fn create_table(
        &self,
        schema: &TableSchema,
    ) -> pg2iceberg_iceberg::Result<TableMetadata> {
        self.inner.create_table(schema).await
    }
    async fn commit_snapshot(
        &self,
        prepared: PreparedCommit,
    ) -> pg2iceberg_iceberg::Result<TableMetadata> {
        let meta = self.inner.commit_snapshot(prepared).await?;
        self.audit().await;
        Ok(meta)
    }
    async fn commit_snapshots(
        &self,
        steps: Vec<PreparedCommit>,
    ) -> pg2iceberg_iceberg::Result<TableMetadata> {
        if self
            .fail_next_commit
            .swap(false, std::sync::atomic::Ordering::SeqCst)
        {
            return Err(pg2iceberg_iceberg::IcebergError::Other(
                "injected: commit_snapshots".into(),
            ));
        }
        let meta = self.inner.commit_snapshots(steps).await?;
        self.audit().await;
        Ok(meta)
    }
    async fn commit_compaction(
        &self,
        prepared: PreparedCompaction,
    ) -> pg2iceberg_iceberg::Result<TableMetadata> {
        let meta = self.inner.commit_compaction(prepared).await?;
        self.audit().await;
        Ok(meta)
    }
    async fn evolve_schema(
        &self,
        ident: &TableIdent,
        changes: Vec<SchemaChange>,
    ) -> pg2iceberg_iceberg::Result<TableMetadata> {
        self.inner.evolve_schema(ident, changes).await
    }
    async fn expire_snapshots(
        &self,
        ident: &TableIdent,
        retention_ms: i64,
    ) -> pg2iceberg_iceberg::Result<usize> {
        self.inner.expire_snapshots(ident, retention_ms).await
    }
    async fn snapshots(&self, ident: &TableIdent) -> pg2iceberg_iceberg::Result<Vec<Snapshot>> {
        self.inner.snapshots(ident).await
    }
}

fn check_invariants(h: &mut DstHarness) -> Result<(), String> {
    // Reach quiescence: drain WAL, flush, ack, then materialize until idle.
    // Loop because a flush may produce events the materializer hasn't seen.
    h.drive();
    h.flush_and_ack();
    // Drain materializer; safety bound to catch infinite loops.
    for _ in 0..1000 {
        if h.materialize() == 0 {
            break;
        }
    }

    // 6. No WAL retention at quiescence: the slot is acked up to the end
    //    of WAL, including WAL that only touched unpublished tables.
    //    Otherwise PG can't recycle it until the next published change.
    let slot =
        h.db.slot_state(SLOT)
            .map_err(|e| format!("slot_state failed: {e}"))?;
    let wal_end = h.db.current_lsn();
    if slot.confirmed_flush_lsn != wal_end {
        return Err(format!(
            "invariant 6 (WAL retention): slot confirmed_flush_lsn {:?} != WAL end {wal_end:?} at quiescence",
            slot.confirmed_flush_lsn
        ));
    }

    let entries = block_on(h.coord.read_log(&ident(), 0, 1_000_000))
        .map_err(|e| format!("read_log failed: {e}"))?;

    // 1. Every log_index s3_path resolves in blob_store.
    let blob_paths: BTreeSet<String> = h.blob_store.paths().into_iter().collect();
    for entry in &entries {
        if !blob_paths.contains(&entry.s3_path) {
            return Err(format!(
                "invariant 1 (blob completeness) violated: log_index references {} but blob_store doesn't have it",
                entry.s3_path
            ));
        }
    }

    // 2. Contiguous offsets per table.
    let mut prev_end = 0u64;
    for entry in &entries {
        if entry.start_offset != prev_end {
            return Err(format!(
                "invariant 2 (contiguous offsets) violated: prev_end={} but next start={}",
                prev_end, entry.start_offset
            ));
        }
        if entry.end_offset != entry.start_offset + entry.record_count {
            return Err(format!(
                "invariant 2 (offset arithmetic): start={}, end={}, record_count={}",
                entry.start_offset, entry.end_offset, entry.record_count
            ));
        }
        prev_end = entry.end_offset;
    }

    // 3. pipeline.flushed_lsn <= slot.confirmed_flush_lsn (after ack).
    let slot =
        h.db.slot_state(SLOT)
            .map_err(|e| format!("slot_state: {e}"))?;
    if h.pipeline.flushed_lsn() > slot.confirmed_flush_lsn {
        return Err(format!(
            "invariant 3 (LSN ordering): pipeline.flushed_lsn={} > slot.confirmed_flush_lsn={}",
            h.pipeline.flushed_lsn(),
            slot.confirmed_flush_lsn
        ));
    }

    // 4. Sorted staged events == sorted committed WAL events.
    //    (Read only blobs that are in coord — orphans from a hypothetical
    //    mid-flush crash would be ignored. We don't currently produce orphans
    //    in this harness, but this keeps the check robust to future expansion.)
    let mut staged_events = Vec::new();
    for entry in &entries {
        let bytes = block_on(h.blob_store.get(&entry.s3_path))
            .map_err(|e| format!("blob_store.get({}): {e}", entry.s3_path))?;
        let mut chunk =
            decode_chunk(&bytes).map_err(|e| format!("decode_chunk({}): {e}", entry.s3_path))?;
        staged_events.append(&mut chunk);
    }
    // A crash between a claim and the slot ack replays the transaction,
    // so it's staged twice: staging is at-least-once and the fold absorbs
    // the repeat (invariant 5). Compare distinct events; a repeat that
    // differs from the original still shows up as a mismatch.
    staged_events.sort_by_key(|m| m.lsn);
    staged_events.dedup_by(|a, b| a.lsn == b.lsn && a.op == b.op && a.row == b.row);

    let mut wal_events =
        h.db.dump_change_events(PUB)
            .map_err(|e| format!("dump_change_events: {e}"))?;
    wal_events.sort_by_key(|c| c.lsn);

    if staged_events.len() != wal_events.len() {
        return Err(format!(
            "invariant 4 (WAL == staged) count: staged={}, wal={}",
            staged_events.len(),
            wal_events.len()
        ));
    }

    for (m, c) in staged_events.iter().zip(wal_events.iter()) {
        if m.lsn != c.lsn {
            return Err(format!(
                "invariant 4: lsn mismatch staged={}, wal={}",
                m.lsn, c.lsn
            ));
        }
        if m.op != c.op {
            return Err(format!(
                "invariant 4: op mismatch at lsn={}: staged={:?}, wal={:?}",
                m.lsn, m.op, c.op
            ));
        }
        let expected_row = match c.op {
            Op::Insert | Op::Update => c.after.as_ref(),
            Op::Delete => c.before.as_ref(),
            _ => return Err(format!("invariant 4: non-DML op in WAL: {:?}", c.op)),
        };
        let expected = expected_row
            .ok_or_else(|| format!("invariant 4: WAL event at lsn={} has no payload row", c.lsn))?;
        if &m.row != expected {
            return Err(format!(
                "invariant 4: row mismatch at lsn={}: staged={:?}, wal={:?}",
                m.lsn, m.row, expected
            ));
        }
    }

    // 5. Iceberg materialized state == PG ground truth.
    let mut iceberg_rows = block_on(read_materialized_state(
        h.catalog.as_ref(),
        h.blob_store.as_ref(),
        &ident(),
        &schema(),
        &[ColumnName("id".into())],
    ))
    .map_err(|e| format!("read_materialized_state: {e}"))?;
    sort_by_pk(&mut iceberg_rows);

    let mut pg_rows =
        h.db.read_table(&ident())
            .map_err(|e| format!("read_table: {e}"))?;
    sort_by_pk(&mut pg_rows);

    if iceberg_rows != pg_rows {
        return Err(format!(
            "invariant 5 (PG == Iceberg) violated:\n  pg={pg_rows:?}\n  iceberg={iceberg_rows:?}"
        ));
    }

    Ok(())
}

fn sort_by_pk(rows: &mut [Row]) {
    rows.sort_by_key(|r| match r.get(&ColumnName("id".into())) {
        Some(PgValue::Int4(n)) => *n,
        _ => i32::MAX,
    });
}

/// Relaxed invariant set for snapshot-aware tests.
///
/// Invariant 4 (`staged events == WAL events sorted by LSN`) doesn't apply
/// once the snapshot phase is in play: snapshot stages all rows at
/// `snap_lsn`, while their WAL counterparts have varying earlier LSNs. The
/// LSN-sorted comparison would falsely flag this as a mismatch.
///
/// We keep invariants 1, 2, 3, and 5 — the headline correctness property
/// (PG == Iceberg at quiescence) still holds, which is what matters.
fn check_invariants_with_snapshot(h: &mut DstHarness) -> Result<(), String> {
    h.drive();
    h.flush_and_ack();
    for _ in 0..16 {
        if h.materialize() == 0 {
            break;
        }
    }

    let entries = block_on(h.coord.read_log(&ident(), 0, 1_000_000))
        .map_err(|e| format!("read_log failed: {e}"))?;
    let blob_paths: BTreeSet<String> = h.blob_store.paths().into_iter().collect();
    for entry in &entries {
        if !blob_paths.contains(&entry.s3_path) {
            return Err(format!(
                "invariant 1: log_index references {} but blob_store doesn't have it",
                entry.s3_path
            ));
        }
    }

    let mut prev_end = 0u64;
    for entry in &entries {
        if entry.start_offset != prev_end {
            return Err(format!(
                "invariant 2: prev_end={} but next start={}",
                prev_end, entry.start_offset
            ));
        }
        prev_end = entry.end_offset;
    }

    let slot =
        h.db.slot_state(SLOT)
            .map_err(|e| format!("slot_state: {e}"))?;
    if h.pipeline.flushed_lsn() > slot.confirmed_flush_lsn {
        return Err(format!(
            "invariant 3: pipeline.flushed_lsn={} > slot.confirmed_flush_lsn={}",
            h.pipeline.flushed_lsn(),
            slot.confirmed_flush_lsn
        ));
    }

    // Invariant 5: PG == Iceberg at quiescence.
    let mut iceberg_rows = block_on(read_materialized_state(
        h.catalog.as_ref(),
        h.blob_store.as_ref(),
        &ident(),
        &schema(),
        &[ColumnName("id".into())],
    ))
    .map_err(|e| format!("read_materialized_state: {e}"))?;
    sort_by_pk(&mut iceberg_rows);
    let mut pg_rows =
        h.db.read_table(&ident())
            .map_err(|e| format!("read_table: {e}"))?;
    sort_by_pk(&mut pg_rows);
    if iceberg_rows != pg_rows {
        return Err(format!(
            "invariant 5: pg={pg_rows:?}\n  iceberg={iceberg_rows:?}"
        ));
    }
    Ok(())
}

// ---------- proptest ----------

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    /// Random workloads (including rollbacks and pipeline crashes) preserve
    /// every checked invariant at quiescence.
    #[test]
    fn pipeline_preserves_invariants_under_random_workload(steps in workload()) {
        let mut h = DstHarness::boot();
        for (i, step) in steps.iter().enumerate() {
            h.run_step(step);
            if let Err(e) = check_step_invariants(&h) {
                panic!("workload {:?}\nfailed after step {i}: {}", steps, e);
            }
        }
        if let Err(e) = check_invariants(&mut h) {
            // proptest will shrink and re-print this as needed.
            panic!("workload {:?}\nfailed: {}", steps, e);
        }
    }
}

// ---------- pinned regressions ----------
//
// As DST surfaces failing seeds we pin them here as deterministic tests so the
// regression doesn't reappear silently. None yet.

/// A materializer restart must never write over a file a committed
/// snapshot still references (the sim blob store refuses overwrites).
#[test]
fn materializer_restart_never_overwrites_committed_files() {
    let mut h = DstHarness::boot();
    h.run_step(&Step::Insert { id: 1, qty: 10 });
    h.run_step(&Step::DriveFlush);
    h.run_step(&Step::MaterializerCycle);
    h.run_step(&Step::RestartMaterializer);
    h.run_step(&Step::Insert { id: 2, qty: 20 });
    h.run_step(&Step::DriveFlush);
    h.run_step(&Step::MaterializerCycle);
    check_invariants(&mut h).unwrap();
}

#[test]
fn happy_path_one_insert_one_flush() {
    let mut h = DstHarness::boot();
    h.run_step(&Step::Insert { id: 1, qty: 10 });
    h.run_step(&Step::DriveFlush);
    check_invariants(&mut h).unwrap();
}

/// One transaction far bigger than `FLUSH_ROWS`: staged in bounded
/// chunks, never partly visible while it streams in (invariants 7-9),
/// and intact once it lands.
#[test]
fn large_transaction_is_staged_in_bounded_chunks() {
    let mut h = DstHarness::boot();
    h.run_step(&Step::BigTx {
        inserts: 20,
        qty: 1,
    });
    for _ in 0..8 {
        h.run_step(&Step::DrivePartial { n: 3 });
        check_step_invariants(&h).unwrap();
        h.run_step(&Step::FlushTick);
        check_step_invariants(&h).unwrap();
    }
    check_invariants(&mut h).unwrap();
}

/// A transaction spanning many staged chunks, materialized in batches
/// far smaller than it: readers must still see all of it or none of it.
#[test]
fn large_transaction_becomes_visible_atomically() {
    let mut h = DstHarness::boot();
    h.run_step(&Step::Insert { id: 1, qty: 1 });
    h.run_step(&Step::BigTx {
        inserts: 20,
        qty: 2,
    });
    h.run_step(&Step::DriveFlush);
    for _ in 0..100 {
        h.run_step(&Step::MaterializerCycle);
        check_step_invariants(&h).unwrap();
    }
    check_invariants(&mut h).unwrap();
}

/// A transaction that inserts rows, truncates, then inserts more, spread
/// over several materializer steps. The TRUNCATE becomes deletes for
/// every row the materializer knows about — which must include rows this
/// same, not-yet-committed unit wrote in earlier steps.
#[test]
fn truncate_inside_large_transaction_hides_its_earlier_inserts() {
    let mut h = DstHarness::boot();
    h.run_step(&Step::Insert { id: 1, qty: 1 });
    h.run_step(&Step::DriveFlush);
    h.run_step(&Step::MaterializerCycle);
    let mut tx = h.db.begin_tx();
    for id in 100..106 {
        tx.insert(&ident(), row(id, 1));
    }
    tx.truncate(&ident());
    for id in 200..202 {
        tx.insert(&ident(), row(id, 2));
    }
    tx.commit(Timestamp(0)).unwrap();
    h.run_step(&Step::DriveFlush);
    for _ in 0..100 {
        h.run_step(&Step::MaterializerCycle);
        check_step_invariants(&h).unwrap();
    }
    let mut iceberg = block_on(read_materialized_state(
        h.catalog.as_ref(),
        h.blob_store.as_ref(),
        &ident(),
        &schema(),
        &[ColumnName("id".into())],
    ))
    .unwrap();
    sort_by_pk(&mut iceberg);
    let mut pg = h.db.read_table(&ident()).unwrap();
    sort_by_pk(&mut pg);
    assert_eq!(iceberg, pg);
}

/// The atomic commit of a multi-step transaction fails: nothing may
/// become visible, and the next cycle must land the whole transaction
/// intact — despite FileIndex having been updated for the lost steps.
#[test]
fn failed_multi_step_commit_leaves_nothing_and_retry_lands_it() {
    let mut h = DstHarness::boot();
    h.run_step(&Step::Insert { id: 1, qty: 1 });
    h.run_step(&Step::DriveFlush);
    h.run_step(&Step::MaterializerCycle);
    h.run_step(&Step::BigTx {
        inserts: 20,
        qty: 2,
    });
    h.run_step(&Step::DriveFlush);
    h.audited
        .fail_next_commit
        .store(true, std::sync::atomic::Ordering::SeqCst);
    assert!(block_on(h.materializer.cycle()).is_err());
    check_step_invariants(&h).unwrap();
    for _ in 0..100 {
        h.run_step(&Step::MaterializerCycle);
        check_step_invariants(&h).unwrap();
    }
    check_invariants(&mut h).unwrap();
}

/// A flush that runs mid-transaction — here on a keepalive received just
/// before the transaction began — must not claim the chunks already
/// staged for it.
#[test]
fn flush_mid_transaction_never_claims_its_chunks() {
    let mut h = DstHarness::boot();
    h.run_step(&Step::UnpublishedWrite { qty: 0 });
    h.run_step(&Step::DrivePartial { n: 1 }); // just the caught-up keepalive
    h.run_step(&Step::BigTx {
        inserts: 20,
        qty: 1,
    });
    h.run_step(&Step::DrivePartial { n: 10 }); // spills, stays open
    h.run_step(&Step::FlushTick);
    check_step_invariants(&h).unwrap();
    check_invariants(&mut h).unwrap();
}

/// Crash partway through a large transaction: chunks staged so far are
/// never claimed, the slot replays the whole transaction, and nothing is
/// lost or torn.
#[test]
fn crash_mid_large_transaction_replays_cleanly() {
    let mut h = DstHarness::boot();
    h.run_step(&Step::BigTx {
        inserts: 20,
        qty: 1,
    });
    h.run_step(&Step::DrivePartial { n: 10 });
    check_step_invariants(&h).unwrap();
    h.run_step(&Step::CrashMidStream);
    h.run_step(&Step::BigTx { inserts: 5, qty: 2 });
    h.run_step(&Step::DriveFlush);
    check_step_invariants(&h).unwrap();
    check_invariants(&mut h).unwrap();
}

/// Writes only to tables outside the publication after the last
/// published change: pgoutput sends nothing for them, so the slot must
/// advance from the caught-up keepalive alone (invariant 6).
#[test]
fn unpublished_writes_do_not_pin_the_slot() {
    let mut h = DstHarness::boot();
    h.run_step(&Step::Insert { id: 1, qty: 10 });
    h.run_step(&Step::DriveFlush);
    for qty in 0..3 {
        h.run_step(&Step::UnpublishedWrite { qty });
    }
    check_invariants(&mut h).unwrap();
}

#[test]
fn crash_after_some_inserts_then_more_inserts() {
    let mut h = DstHarness::boot();
    h.run_step(&Step::Insert { id: 1, qty: 10 });
    h.run_step(&Step::Insert { id: 2, qty: 20 });
    h.run_step(&Step::DriveFlush);
    h.run_step(&Step::CrashAndRestart);
    h.run_step(&Step::Insert { id: 3, qty: 30 });
    h.run_step(&Step::DriveFlush);
    check_invariants(&mut h).unwrap();
}

#[test]
fn rollback_does_not_appear_in_staged_or_coord() {
    let mut h = DstHarness::boot();
    h.run_step(&Step::RollbackInsert { id: 99, qty: 1 });
    h.run_step(&Step::Insert { id: 1, qty: 10 });
    h.run_step(&Step::DriveFlush);
    check_invariants(&mut h).unwrap();

    let entries = block_on(h.coord.read_log(&ident(), 0, 100)).unwrap();
    let total: u64 = entries.iter().map(|e| e.record_count).sum();
    assert_eq!(total, 1, "only the committed insert should be staged");
}

#[test]
fn update_then_delete_round_trips_to_staged() {
    let mut h = DstHarness::boot();
    h.run_step(&Step::Insert { id: 1, qty: 10 });
    h.run_step(&Step::Update { id: 1, qty: 99 });
    h.run_step(&Step::Delete { id: 1 });
    h.run_step(&Step::DriveFlush);
    check_invariants(&mut h).unwrap();

    let entries = block_on(h.coord.read_log(&ident(), 0, 100)).unwrap();
    let total: u64 = entries.iter().map(|e| e.record_count).sum();
    assert_eq!(total, 3, "I + U + D");
}

#[test]
fn materializer_runs_between_writes_keeps_iceberg_in_sync() {
    let mut h = DstHarness::boot();
    h.run_step(&Step::Insert { id: 1, qty: 10 });
    h.run_step(&Step::DriveFlush);
    h.run_step(&Step::MaterializerCycle);
    h.run_step(&Step::Insert { id: 2, qty: 20 });
    h.run_step(&Step::Update { id: 1, qty: 99 });
    h.run_step(&Step::DriveFlush);
    h.run_step(&Step::MaterializerCycle);
    check_invariants(&mut h).unwrap();
}

#[test]
fn materializer_idempotent_when_run_extra_times() {
    let mut h = DstHarness::boot();
    h.run_step(&Step::Insert { id: 1, qty: 10 });
    h.run_step(&Step::DriveFlush);
    h.run_step(&Step::MaterializerCycle);
    h.run_step(&Step::MaterializerCycle);
    h.run_step(&Step::MaterializerCycle);
    check_invariants(&mut h).unwrap();
}

// ---------- snapshot integration ----------

#[test]
fn snapshot_then_workload_keeps_all_invariants() {
    // Pre-seed 10 rows. Snapshot bootstraps Iceberg with them. Then run
    // mixed live-replication workload on top. All 5 invariants must hold.
    let seeds: Vec<(i32, i32)> = (1..=10).map(|i| (i, i * 10)).collect();
    let mut h = DstHarness::boot_with_seeds(&seeds);
    h.run_snapshot();
    h.materialize();

    // Now run live workload that mutates the snapshot rows.
    h.run_step(&Step::Update { id: 1, qty: 999 });
    h.run_step(&Step::Delete { id: 2 });
    h.run_step(&Step::Insert { id: 11, qty: 110 });
    h.run_step(&Step::DriveFlush);
    h.run_step(&Step::MaterializerCycle);
    check_invariants_with_snapshot(&mut h).unwrap();
}

#[test]
fn snapshot_resume_keeps_invariants() {
    // Partial snapshot (chunk_size=3, max_chunks=2 → 6 of 10 rows staged).
    // Pipeline + Snapshotter dropped; rebuild and resume to completion.
    // Invariants must hold across the resume boundary.
    let seeds: Vec<(i32, i32)> = (1..=10).map(|i| (i, i * 10)).collect();
    let mut h = DstHarness::boot_with_seeds(&seeds);
    {
        let s = Snapshotter::new(h.coord.clone() as Arc<dyn Coordinator>).with_chunk_size(3);
        block_on(s.run_chunks(&h.db, &[schema()], &mut h.pipeline, Some(2))).unwrap();
    }
    // Crash the pipeline (replay from slot's restart_lsn) so subsequent
    // snapshot run rebuilds state cleanly.
    h.crash_and_restart();

    // Resume the snapshot.
    {
        let s = Snapshotter::new(h.coord.clone() as Arc<dyn Coordinator>).with_chunk_size(3);
        let snap_lsn = block_on(s.run(&h.db, &[schema()], &mut h.pipeline)).unwrap();
        h.stream.send_standby(snap_lsn);
    }
    h.materialize();
    check_invariants_with_snapshot(&mut h).unwrap();
}

#[test]
fn pipeline_crash_then_materializer_catches_up() {
    let mut h = DstHarness::boot();
    h.run_step(&Step::Insert { id: 1, qty: 10 });
    h.run_step(&Step::DriveFlush);
    h.run_step(&Step::CrashAndRestart);
    h.run_step(&Step::Insert { id: 2, qty: 20 });
    h.run_step(&Step::DriveFlush);
    h.run_step(&Step::MaterializerCycle);
    check_invariants(&mut h).unwrap();
}
