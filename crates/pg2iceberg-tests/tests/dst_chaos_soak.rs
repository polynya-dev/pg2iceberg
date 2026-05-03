//! Chaos soak DST.
//!
//! Drives the full prod lifecycle (`run_logical_lifecycle` →
//! `run_logical_main_loop`) under proptest-driven workloads with
//! deterministic-but-paused tokio time. Each seed exercises:
//!
//! - Multi-table snapshot bootstrap on first lifecycle run.
//! - Steady-state CDC via the slot, Standby acks, periodic Materialize.
//! - Schedule-driven Compact (lifecycle's `compaction` config wires it
//!   into the Materialize tick).
//! - "Very large value" (TOAST-class) writes on a wide-typed schema.
//! - Deterministic fault injection across blob / coord / catalog ops.
//! - Restart (lifecycle stop + restart with same schemas).
//! - Live AddTable: stop lifecycle, extend YAML, restart — the
//!   lifecycle's reconcile + background-snapshot task does the rest.
//!
//! The headline invariant after each seed is the one that matters:
//! **for every registered table, `read_table(SimPostgres) ==
//! read_materialized_state(MemoryCatalog)`** at quiescence. No data
//! loss, no phantom rows, no torn types.
//!
//! Why a paused tokio runtime: `run_logical_main_loop` schedules
//! Standby / Flush / Materialize via `tokio::time::sleep`. With
//! `start_paused = true` + explicit `tokio::time::advance`,
//! virtual time only moves when the workload says so — so the same
//! proptest seed produces bit-identical execution.
//!
//! Compared to the old harness that called `pipeline.process` /
//! `flush` / `materializer.cycle` directly, this version exposes a
//! second-order class of bugs: anything where a *separately-scheduled*
//! Standby tick reads stale-or-speculative pipeline state and acks
//! the slot ahead of durable storage. (See the "DST PROBE" experiment
//! in the conversation history.)

use pg2iceberg_coord::schema::CoordSchema;
use pg2iceberg_coord::Coordinator;
use pg2iceberg_core::typemap::IcebergType;
use pg2iceberg_core::value::TimestampMicros;
use pg2iceberg_core::{
    Clock, ColumnName, ColumnSchema, IdGen, Mode, Namespace, PgValue, Row, TableIdent, TableSchema,
    Timestamp, WorkerId,
};
use pg2iceberg_iceberg::read_materialized_state;
use pg2iceberg_logical::pipeline::CounterBlobNamer;
use pg2iceberg_logical::runner::Schedule;
use pg2iceberg_logical::CounterMaterializerNamer;
use pg2iceberg_sim::blob::MemoryBlobStore;
use pg2iceberg_sim::catalog::MemoryCatalog;
use pg2iceberg_sim::clock::TestClock;
use pg2iceberg_sim::coord::MemoryCoordinator;
use pg2iceberg_sim::fault::{ops, FaultPlan, FaultyBlobStore, FaultyCatalog, FaultyCoordinator};
use pg2iceberg_sim::postgres::{SimPgClient, SimPostgres};
use pg2iceberg_validate::{LogicalLifecycle, SnapshotSourceFactory};
use proptest::prelude::*;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

// ── constants ──────────────────────────────────────────────────────

const NAMESPACE: &str = "public";
const PUB: &str = "chaos_pub";
const SLOT: &str = "chaos_slot";
const INITIAL_TABLES: usize = 2;
const SEED_ROWS_PER_TABLE: usize = 50;
const MAX_TABLES: usize = 5;

/// Tick durations short enough that a single `Step::Tick { ms: 200 }`
/// reliably fires every handler at least once. Watcher is rarer
/// (and validates slot health), so it gets a slightly longer cadence.
fn chaos_schedule() -> Schedule {
    Schedule {
        flush: Duration::from_millis(50),
        standby: Duration::from_millis(60),
        materialize: Duration::from_millis(70),
        watcher: Duration::from_millis(200),
    }
}

// ── wide schema ────────────────────────────────────────────────────

fn ident_for(idx: usize) -> TableIdent {
    TableIdent {
        namespace: Namespace(vec![NAMESPACE.into()]),
        name: format!("chaos_t{idx}"),
    }
}

fn wide_schema(idx: usize) -> TableSchema {
    TableSchema {
        ident: ident_for(idx),
        columns: vec![
            ColumnSchema {
                name: "id".into(),
                field_id: 1,
                ty: IcebergType::Int,
                nullable: false,
                is_primary_key: true,
            },
            ColumnSchema {
                name: "qty_l".into(),
                field_id: 2,
                ty: IcebergType::Long,
                nullable: true,
                is_primary_key: false,
            },
            ColumnSchema {
                name: "score".into(),
                field_id: 3,
                ty: IcebergType::Double,
                nullable: true,
                is_primary_key: false,
            },
            ColumnSchema {
                name: "active".into(),
                field_id: 4,
                ty: IcebergType::Boolean,
                nullable: true,
                is_primary_key: false,
            },
            ColumnSchema {
                name: "label".into(),
                field_id: 5,
                ty: IcebergType::String,
                nullable: true,
                is_primary_key: false,
            },
            ColumnSchema {
                name: "payload".into(),
                field_id: 6,
                ty: IcebergType::Binary,
                nullable: true,
                is_primary_key: false,
            },
            ColumnSchema {
                name: "ts".into(),
                field_id: 7,
                ty: IcebergType::Timestamp,
                nullable: true,
                is_primary_key: false,
            },
        ],
        partition_spec: Vec::new(),
        pg_schema: None,
    }
}

// ── deterministic value generator ──────────────────────────────────

fn mix(id: i32, gen: u64) -> u64 {
    let mut x = (id as u64).wrapping_mul(0x9E3779B97F4A7C15) ^ gen.wrapping_mul(0xBF58476D1CE4E5B9);
    x ^= x >> 30;
    x = x.wrapping_mul(0xBF58476D1CE4E5B9);
    x ^= x >> 27;
    x
}

fn wide_row(id: i32, gen: u64, payload_size: usize) -> Row {
    let h = mix(id, gen);
    let payload: Vec<u8> = (0..payload_size)
        .map(|i| (h.wrapping_add(i as u64) & 0xFF) as u8)
        .collect();
    let mut r = BTreeMap::new();
    r.insert(ColumnName("id".into()), PgValue::Int4(id));
    r.insert(ColumnName("qty_l".into()), PgValue::Int8(h as i64));
    // Float8 with values small enough that the staged-event JSON
    // round-trip preserves them bit-exactly. Anything above ~2^53 is
    // one-ULP-fragile; we keep the value in [-1e6, 1e6]. Avoids NaN
    // by construction (no `f64::from_bits`).
    let score_int = (h as i32) % 1_000_000;
    r.insert(
        ColumnName("score".into()),
        PgValue::Float8(score_int as f64 / 100.0),
    );
    r.insert(ColumnName("active".into()), PgValue::Bool(h & 1 == 0));
    r.insert(
        ColumnName("label".into()),
        PgValue::Text(format!("row-{id}-g{gen}")),
    );
    r.insert(ColumnName("payload".into()), PgValue::Bytea(payload));
    r.insert(
        ColumnName("ts".into()),
        PgValue::Timestamp(TimestampMicros(h as i64 & 0x7FFF_FFFF_FFFF_FFFF)),
    );
    r
}

fn pk_only(id: i32) -> Row {
    let mut r = BTreeMap::new();
    r.insert(ColumnName("id".into()), PgValue::Int4(id));
    r
}

// ── workload model ─────────────────────────────────────────────────

/// Fault ops the workload can schedule. Limited to ops where
/// injection usefully exercises a recovery path.
const FAULTABLE_OPS: &[&str] = &[
    ops::BLOB_PUT,
    ops::BLOB_GET,
    ops::COORD_CLAIM,
    ops::COORD_SAVE_CP,
    ops::CAT_COMMIT_SNAPSHOT,
];

#[derive(Clone, Debug)]
enum Step {
    Insert {
        table_idx: usize,
        id: i32,
    },
    Update {
        table_idx: usize,
        id: i32,
    },
    Delete {
        table_idx: usize,
        id: i32,
    },
    /// Insert with a ~32 KiB Bytea — exercises chunking + large-value path.
    InsertToast {
        table_idx: usize,
        id: i32,
    },
    /// Advance virtual time by `ms` milliseconds. main_loop's
    /// `tokio::time::sleep` arm fires; due `Handler::*` ticks run
    /// (Flush, Standby, Materialize, Watcher) with `compact_cycle`
    /// folded into the Materialize handler via `compaction` config.
    Tick {
        ms: u64,
    },
    /// Schedule the next `count` calls to `op` to fail.
    InjectFault {
        op: &'static str,
        count: u64,
    },
    /// Stop the running lifecycle, then start a fresh one with the
    /// same schemas. Models a process restart.
    Restart,
    /// Stop, create a new table in PG + extend YAML, restart. The
    /// lifecycle's reconcile path + `register_table_pending` +
    /// background snapshot task do the rest.
    AddTable,
}

fn step_strategy() -> impl Strategy<Value = Step> {
    let id = 1i32..=12;
    let table_idx = 0usize..MAX_TABLES;
    prop_oneof![
        // Steady-state DML — biased high.
        12 => (table_idx.clone(), id.clone())
            .prop_map(|(t, i)| Step::Insert { table_idx: t, id: i }),
        8 => (table_idx.clone(), id.clone())
            .prop_map(|(t, i)| Step::Update { table_idx: t, id: i }),
        5 => (table_idx.clone(), id.clone())
            .prop_map(|(t, i)| Step::Delete { table_idx: t, id: i }),
        2 => (table_idx.clone(), id.clone())
            .prop_map(|(t, i)| Step::InsertToast { table_idx: t, id: i }),
        // Time advances let main_loop tick. Mix short and long so the
        // distribution covers both "barely fires Flush" and "fires
        // every handler several times".
        10 => (50u64..=500).prop_map(|ms| Step::Tick { ms }),
        // Chaos.
        2 => Just(Step::Restart),
        2 => (prop::sample::select(FAULTABLE_OPS), 1u64..=3)
            .prop_map(|(op, count)| Step::InjectFault { op, count }),
        // Topology evolution.
        1 => Just(Step::AddTable),
    ]
}

fn workload() -> impl Strategy<Value = Vec<Step>> {
    prop::collection::vec(step_strategy(), 16..=80)
}

// ── deterministic IdGen for the lifecycle ──────────────────────────

struct ZeroIdGen;
impl IdGen for ZeroIdGen {
    fn new_uuid(&self) -> [u8; 16] {
        [0u8; 16]
    }
    fn worker_id(&self) -> WorkerId {
        WorkerId("dst-chaos".into())
    }
}

// ── harness ────────────────────────────────────────────────────────

struct ChaosHarness {
    db: SimPostgres,
    coord_inner: Arc<MemoryCoordinator>,
    blob_inner: Arc<MemoryBlobStore>,
    catalog_inner: Arc<MemoryCatalog>,
    plan: FaultPlan,
    /// Concrete `TestClock` so the harness can advance it in lockstep
    /// with tokio's virtual clock. Lifecycle's ticker reads
    /// `lc.clock.now()` to decide which handlers are due — if we
    /// only advanced tokio time, the lifecycle's `tokio::time::sleep`
    /// would wake but `ticker.fire_due` would still see now=0 and
    /// fire nothing.
    test_clock: Arc<TestClock>,
    clock: Arc<dyn Clock>,
    /// Long-lived blob namer — kept across lifecycle restarts so the
    /// monotonic counter doesn't reset and collide with paths from
    /// the previous run.
    blob_namer: Arc<CounterBlobNamer>,
    materializer_namer: Arc<CounterMaterializerNamer>,
    schemas: Vec<TableSchema>,
    live: Vec<BTreeSet<i32>>,
    next_gen: u64,
    /// Currently-running lifecycle. `None` between restarts.
    running: Option<RunningLifecycle>,
}

struct RunningLifecycle {
    /// `spawn_local` (rather than `tokio::spawn`) because
    /// `run_logical_lifecycle`'s future isn't `Send` — tracing
    /// macros embed `Arguments<'_>`/`dyn Value` across awaits. The
    /// chaos soak's whole runtime is single-threaded
    /// (`current_thread` + `LocalSet`), so non-`Send` is fine.
    handle: tokio::task::JoinHandle<Result<(), pg2iceberg_validate::LifecycleError>>,
    shutdown_tx: tokio::sync::oneshot::Sender<()>,
}

impl ChaosHarness {
    async fn boot() -> Self {
        let db = SimPostgres::new();
        let mut schemas = Vec::new();
        let mut live = Vec::new();
        for i in 0..INITIAL_TABLES {
            let s = wide_schema(i);
            db.create_table(s.clone()).unwrap();
            schemas.push(s);
            live.push(BTreeSet::new());
        }
        // Pre-seed rows BEFORE any publication exists. Lifecycle's
        // first run takes the fresh-slot path: creates publication +
        // slot, runs the synchronous snapshot phase, then enters CDC.
        let mut next_gen = 0u64;
        for (i, schema) in schemas.iter().enumerate() {
            let mut tx = db.begin_tx();
            for k in 0..SEED_ROWS_PER_TABLE {
                let id = 1000 + (i * SEED_ROWS_PER_TABLE + k) as i32;
                tx.insert(&schema.ident, wide_row(id, next_gen, 64));
                live[i].insert(id);
                next_gen += 1;
            }
            tx.commit(Timestamp(0)).unwrap();
        }

        let test_clock = Arc::new(TestClock::at(0));
        let clock: Arc<dyn Clock> = test_clock.clone();
        let coord_inner = Arc::new(MemoryCoordinator::new(
            CoordSchema::default_name(),
            Arc::clone(&clock),
        ));
        let blob_inner = Arc::new(MemoryBlobStore::new());
        let catalog_inner = Arc::new(MemoryCatalog::new());

        let mut h = Self {
            db,
            coord_inner,
            blob_inner,
            catalog_inner,
            plan: FaultPlan::new(),
            test_clock,
            clock,
            blob_namer: Arc::new(CounterBlobNamer::new("s3://stage")),
            materializer_namer: Arc::new(CounterMaterializerNamer::new("s3://table")),
            schemas,
            live,
            next_gen,
            running: None,
        };

        h.start_lifecycle().await;
        // Initial run needs enough virtual time to complete snapshot
        // phase + drain the post-snapshot CDC stream + run a few
        // materializer cycles. Snapshot is synchronous in lifecycle's
        // fresh-slot path, so this is bounded by chunk count.
        h.advance_time(2_000).await;
        h
    }

    fn build_lifecycle(&self) -> LogicalLifecycle<FaultyCatalog> {
        let coord = Arc::new(FaultyCoordinator::new(
            Arc::clone(&self.coord_inner),
            self.plan.clone(),
        ));
        let blob = Arc::new(FaultyBlobStore::new(
            Arc::clone(&self.blob_inner),
            self.plan.clone(),
        ));
        let catalog = Arc::new(FaultyCatalog::new(
            Arc::clone(&self.catalog_inner),
            self.plan.clone(),
        ));
        let pg_client = Arc::new(SimPgClient::new(self.db.clone()));

        let snapshot_db = self.db.clone();
        let snapshot_factory: SnapshotSourceFactory = Box::new(move |_| {
            Box::pin(async move {
                Ok::<
                    Box<dyn pg2iceberg_snapshot::SnapshotSource>,
                    pg2iceberg_validate::LifecycleError,
                >(Box::new(snapshot_db))
            })
        });

        LogicalLifecycle {
            pg: pg_client.clone() as Arc<dyn pg2iceberg_pg::PgClient>,
            slot_monitor: pg_client as Arc<dyn pg2iceberg_pg::SlotMonitor>,
            coord: coord as Arc<dyn Coordinator>,
            catalog,
            blob: blob as Arc<dyn pg2iceberg_stream::BlobStore>,
            clock: Arc::clone(&self.clock),
            id_gen: Arc::new(ZeroIdGen) as Arc<dyn IdGen>,
            schemas: self.schemas.clone(),
            skip_snapshot_idents: BTreeSet::new(),
            slot_name: SLOT.into(),
            publication_name: PUB.into(),
            group: "default".into(),
            schedule: chaos_schedule(),
            // Compaction folded into Materialize tick. Thresholds
            // lowered so the workload reliably triggers rewrites.
            compaction: Some(pg2iceberg_iceberg::CompactionConfig {
                data_file_threshold: 2,
                delete_file_threshold: 1,
                target_size_bytes: 4 * 1024 * 1024,
            }),
            flush_rows: 64,
            mat_cycle_limit: 128,
            consumer_ttl: Duration::from_secs(60),
            snapshot_source_factory: snapshot_factory,
            materializer_namer: Arc::clone(&self.materializer_namer)
                as Arc<dyn pg2iceberg_logical::materializer::MaterializerNamer>,
            blob_namer: Arc::clone(&self.blob_namer)
                as Arc<dyn pg2iceberg_logical::pipeline::BlobNamer>,
            metrics: Arc::new(pg2iceberg_core::InMemoryMetrics::new()),
            mode: Mode::Logical,
            meta_namespace: None,
        }
    }

    async fn start_lifecycle(&mut self) {
        if self.running.is_some() {
            return;
        }
        let lifecycle = self.build_lifecycle();
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        let handle = tokio::task::spawn_local(async move {
            let shutdown = Box::pin(async move {
                let _ = shutdown_rx.await;
            });
            pg2iceberg_validate::run_logical_lifecycle(lifecycle, shutdown).await
        });
        self.running = Some(RunningLifecycle {
            handle,
            shutdown_tx,
        });
        // Yield repeatedly so the spawned task runs through its
        // synchronous setup (validate → slot setup → register
        // tables → snapshot phase → start_replication → enter
        // main_loop). Setup has many `.await` points; need plenty
        // of yields. Snapshot phase in particular pumps chunks
        // through pipeline → coord, each of which awaits. We then
        // tick the test clock + tokio clock so any time-driven
        // setup (e.g. snapshot's first ticker pass) sees nonzero
        // time.
        for _ in 0..256 {
            tokio::task::yield_now().await;
        }
        self.test_clock.advance(Duration::from_millis(50));
        tokio::time::advance(Duration::from_millis(50)).await;
        for _ in 0..256 {
            tokio::task::yield_now().await;
        }
    }

    async fn stop_lifecycle(&mut self) {
        if let Some(r) = self.running.take() {
            let _ = r.shutdown_tx.send(());
            // Bump both clocks so main_loop's `tokio::time::sleep`
            // arm wakes and `ticker.fire_due` sees nonzero now.
            self.test_clock.advance(Duration::from_millis(500));
            tokio::time::advance(Duration::from_millis(500)).await;
            // Then yield until the task actually exits. Bound the
            // loop so a hung shutdown can't block the test.
            for _ in 0..256 {
                if r.handle.is_finished() {
                    break;
                }
                tokio::task::yield_now().await;
            }
            // Swallow lifecycle's `Result` and the join error; the
            // chaos test treats *any* exit as "process ended; if a
            // subsequent step needs the lifecycle alive,
            // `ensure_lifecycle_alive` will respawn it." Reach for
            // `eprintln!` here when investigating a specific failing
            // seed.
            let _ = r.handle.await;
        }
    }

    async fn advance_time(&mut self, ms: u64) {
        // Yield BEFORE advancing virtual time. `tokio::time::advance`
        // is a no-op for tasks that haven't reached a `sleep().await`
        // yet — without this pre-yield, a spawned task that's still
        // executing synchronous setup work (validate, register
        // tables, snapshot phase) won't see the time bump. After the
        // pre-yields the spawned task is parked on its main_loop
        // `select!`'s sleep arm, ready to wake on advance.
        //
        // Then advance in small slices, yielding between each, so
        // every fired tick gets a chance to make a full pass through
        // main_loop's event-process / dispatch / heartbeat sequence
        // before the next tick fires.
        for _ in 0..16 {
            tokio::task::yield_now().await;
        }
        let slice_ms = 50u64.min(ms);
        let mut remaining = ms;
        while remaining > 0 {
            let step = remaining.min(slice_ms);
            // Advance BOTH clocks: tokio's virtual clock to wake
            // `tokio::time::sleep` arms, and the harness's `TestClock`
            // (passed to the lifecycle as `lc.clock`) so
            // `ticker.fire_due(loop_state.clock.now())` actually
            // returns due handlers. Without the latter, sleep wakes
            // but no Flush/Standby/Materialize ticks fire — events
            // sit in pipeline.sink forever.
            self.test_clock.advance(Duration::from_millis(step));
            tokio::time::advance(Duration::from_millis(step)).await;
            for _ in 0..32 {
                tokio::task::yield_now().await;
            }
            remaining -= step;
        }
        self.ensure_lifecycle_alive().await;
    }

    /// If the spawned lifecycle exited (because main_loop returned
    /// `Err` on a fault-injected flush, or for any other reason),
    /// restart it. Models the operator's "process crashed → bring
    /// it back up" loop. Without this, a single mid-workload fault
    /// would silently strand subsequent DML in PG's WAL with
    /// nothing materializing it.
    async fn ensure_lifecycle_alive(&mut self) {
        let needs_restart = match &self.running {
            Some(r) => r.handle.is_finished(),
            None => true,
        };
        if needs_restart {
            // Stop_lifecycle awaits the (already-finished) handle
            // and clears `self.running`.
            self.stop_lifecycle().await;
            // Clear faults across crash-restart so the new
            // lifecycle's startup validation isn't permanently
            // wedged on a scheduled fault from before the crash.
            // The workload's `InjectFault` model is "schedule the
            // next N calls"; an in-flight schedule that already
            // tripped main_loop's flush should be considered
            // consumed by the implicit restart it caused.
            self.plan.clear_failures();
            self.start_lifecycle().await;
        }
    }

    async fn run_step(&mut self, step: &Step) {
        // Implicit operator-restart between every step: if a fault
        // tripped main_loop's flush mid-workload, the lifecycle
        // task exited with Err. Subsequent DML would land in PG's
        // WAL with no consumer. Bring the lifecycle back up before
        // doing anything else.
        self.ensure_lifecycle_alive().await;

        match step {
            Step::Insert { table_idx, id } => self.do_insert(*table_idx, *id, 64),
            Step::InsertToast { table_idx, id } => self.do_insert(*table_idx, *id, 32 * 1024),
            Step::Update { table_idx, id } => self.do_update(*table_idx, *id),
            Step::Delete { table_idx, id } => self.do_delete(*table_idx, *id),
            Step::Tick { ms } => self.advance_time(*ms).await,
            Step::InjectFault { op, count } => {
                let base = self.plan.counter(op);
                self.plan.fail(op, base..(base + *count));
            }
            Step::Restart => {
                self.stop_lifecycle().await;
                // Clear faults across restart so the next lifecycle's
                // startup validation isn't permanently blocked by a
                // scheduled fault on a coord op it must succeed at.
                self.plan.clear_failures();
                self.start_lifecycle().await;
            }
            Step::AddTable => self.add_table().await,
        }
    }

    async fn add_table(&mut self) {
        if self.schemas.len() >= MAX_TABLES {
            return;
        }
        self.stop_lifecycle().await;
        self.plan.clear_failures();
        let new_idx = self.schemas.len();
        let new_schema = wide_schema(new_idx);
        if self.db.create_table(new_schema.clone()).is_err() {
            self.start_lifecycle().await;
            return;
        }
        self.schemas.push(new_schema);
        self.live.push(BTreeSet::new());
        self.start_lifecycle().await;
        // Backfill snapshot for the new table runs in the background
        // task spawned by the lifecycle's reconcile path. Give it
        // virtual time to complete before subsequent workload steps
        // expect the table to be CDC-ready.
        self.advance_time(1_000).await;
    }

    fn do_insert(&mut self, table_idx: usize, id: i32, payload: usize) {
        if table_idx >= self.schemas.len() {
            return;
        }
        if self.live[table_idx].contains(&id) {
            return;
        }
        let ident = self.schemas[table_idx].ident.clone();
        let mut tx = self.db.begin_tx();
        tx.insert(&ident, wide_row(id, self.next_gen, payload));
        if tx.commit(Timestamp(0)).is_ok() {
            self.live[table_idx].insert(id);
            self.next_gen += 1;
        }
    }

    fn do_update(&mut self, table_idx: usize, id: i32) {
        if table_idx >= self.schemas.len() {
            return;
        }
        if !self.live[table_idx].contains(&id) {
            return;
        }
        let ident = self.schemas[table_idx].ident.clone();
        let mut tx = self.db.begin_tx();
        tx.update(&ident, wide_row(id, self.next_gen, 64));
        if tx.commit(Timestamp(0)).is_ok() {
            self.next_gen += 1;
        }
    }

    fn do_delete(&mut self, table_idx: usize, id: i32) {
        if table_idx >= self.schemas.len() {
            return;
        }
        if !self.live[table_idx].contains(&id) {
            return;
        }
        let ident = self.schemas[table_idx].ident.clone();
        let mut tx = self.db.begin_tx();
        tx.delete(&ident, pk_only(id));
        if tx.commit(Timestamp(0)).is_ok() {
            self.live[table_idx].remove(&id);
        }
    }
}

// ── invariant check ────────────────────────────────────────────────

fn pk_of(row: &Row) -> Option<i32> {
    match row.get(&ColumnName("id".into())) {
        Some(PgValue::Int4(n)) => Some(*n),
        _ => None,
    }
}

fn first_row_diff(pg: &[Row], ice: &[Row]) -> String {
    let n = pg.len().min(ice.len());
    for i in 0..n {
        if pg[i] != ice[i] {
            let mut col_diff = String::from("(no column-level diff?)");
            for (col, pv) in &pg[i] {
                match ice[i].get(col) {
                    Some(iv) if iv == pv => continue,
                    Some(iv) => {
                        col_diff = format!("col `{}`: pg={:?} ice={:?}", col.0, pv, iv);
                        break;
                    }
                    None => {
                        col_diff = format!("col `{}`: pg={:?} ice=<missing>", col.0, pv);
                        break;
                    }
                }
            }
            return format!("pk={:?} {}", pk_of(&pg[i]), col_diff);
        }
    }
    if pg.len() != ice.len() {
        let extra_in_pg: Vec<i32> = pg.iter().skip(n).filter_map(pk_of).collect();
        let extra_in_ice: Vec<i32> = ice.iter().skip(n).filter_map(pk_of).collect();
        return format!(
            "length differ: pg_extra={:?} ice_extra={:?}",
            extra_in_pg, extra_in_ice
        );
    }
    "rows equal? (shouldn't reach)".into()
}

fn sort_by_pk(rows: &mut [Row]) {
    rows.sort_by_key(|r| match r.get(&ColumnName("id".into())) {
        Some(PgValue::Int4(n)) => *n,
        _ => i32::MAX,
    });
}

/// Drain to quiescence and assert PG ↔ Iceberg parity per table.
///
/// The path:
/// 1. Clear any scheduled faults so background work can finish.
/// 2. Advance virtual time generously (5 seconds) so every
///    Flush/Standby/Materialize/Compact tick fires repeatedly and
///    materializes any pending log_index entries.
/// 3. Stop the lifecycle gracefully — `drain_and_shutdown` runs a
///    final flush + materialize cycle on the way out, so the
///    materializer reflects every durably-staged event.
/// 4. Read PG ground truth and Iceberg materialized state via the
///    long-lived `*_inner` Arcs (FaultyXxx wrappers are dropped with
///    the lifecycle, but the underlying state survives).
async fn assert_no_data_loss(h: &mut ChaosHarness) -> Result<(), String> {
    h.plan.clear_failures();
    // Recovery loop: each iteration restarts the lifecycle if it
    // exited (because a fault tripped main_loop's flush) and gives
    // it real virtual time to drain pending events. Multiple
    // iterations because the fresh lifecycle's startup validation
    // briefly holds the slot before main_loop starts pulling — a
    // single advance may not be enough if the slot's WAL backlog
    // is large.
    for _ in 0..6 {
        h.ensure_lifecycle_alive().await;
        h.advance_time(2_000).await;
    }
    h.stop_lifecycle().await;

    for schema in &h.schemas {
        let ident = &schema.ident;
        let mut pg_rows =
            h.db.read_table(ident)
                .map_err(|e| format!("read_table({ident}): {e}"))?;
        let mut ice_rows = read_materialized_state(
            h.catalog_inner.as_ref(),
            h.blob_inner.as_ref(),
            ident,
            schema,
            &[ColumnName("id".into())],
        )
        .await
        .map_err(|e| format!("read_materialized_state({ident}): {e}"))?;
        sort_by_pk(&mut pg_rows);
        sort_by_pk(&mut ice_rows);
        if pg_rows != ice_rows {
            let first_diff = first_row_diff(&pg_rows, &ice_rows);
            return Err(format!(
                "PG ↔ Iceberg mismatch for {ident}: pg.len={} ice.len={}\n  first_diff: {first_diff}",
                pg_rows.len(),
                ice_rows.len(),
            ));
        }
    }
    Ok(())
}

// ── runtime helpers ────────────────────────────────────────────────

/// Build a fresh single-threaded tokio runtime with paused virtual
/// time. One per proptest case keeps state isolated.
fn paused_runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .start_paused(true)
        .build()
        .unwrap()
}

/// Run an async test body inside a `LocalSet` on a paused-time
/// runtime. The lifecycle's future isn't `Send`, so spawned tasks
/// must stay on the local thread (`spawn_local`).
fn run_chaos_test<Fut: std::future::Future<Output = ()>>(body: impl FnOnce() -> Fut) {
    let runtime = paused_runtime();
    let local = tokio::task::LocalSet::new();
    local.block_on(&runtime, body());
}

// ── proptest ───────────────────────────────────────────────────────

proptest! {
    #![proptest_config(ProptestConfig {
        cases: 32,
        ..ProptestConfig::default()
    })]

    #[test]
    fn chaos_soak_preserves_data_integrity(steps in workload()) {
        run_chaos_test(|| async move {
            let mut h = ChaosHarness::boot().await;
            for step in &steps {
                h.run_step(step).await;
            }
            if let Err(e) = assert_no_data_loss(&mut h).await {
                panic!("workload {:?}\nfailed: {}", steps, e);
            }
        });
    }
}

// ── extended soak (opt-in) ─────────────────────────────────────────
//
// Case-count tunable via `CHAOS_SOAK_CASES` env var (default 4096).
// Local: `cargo test --release chaos_soak_extended_seeds --
// --ignored`. Nightly CI sets `CHAOS_SOAK_CASES=50000`.

#[test]
#[ignore]
fn chaos_soak_extended_seeds() {
    let cases: u32 = std::env::var("CHAOS_SOAK_CASES")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(4096);
    let mut runner = proptest::test_runner::TestRunner::new(proptest::test_runner::Config {
        cases,
        ..proptest::test_runner::Config::default()
    });
    runner
        .run(&workload(), |steps| {
            let runtime = paused_runtime();
            let local = tokio::task::LocalSet::new();
            let result: Result<(), proptest::test_runner::TestCaseError> =
                local.block_on(&runtime, async {
                    let mut h = ChaosHarness::boot().await;
                    for step in &steps {
                        h.run_step(step).await;
                    }
                    assert_no_data_loss(&mut h).await.map_err(|e| {
                        proptest::test_runner::TestCaseError::fail(format!(
                            "workload {:?}\nfailed: {}",
                            steps, e
                        ))
                    })
                });
            result?;
            Ok(())
        })
        .unwrap();
}

// ── pinned regressions ─────────────────────────────────────────────

#[test]
fn smoke_boot_and_quiesce_without_workload() {
    run_chaos_test(|| async move {
        let mut h = ChaosHarness::boot().await;
        assert_no_data_loss(&mut h).await.unwrap();
    });
}

#[test]
fn add_table_then_workload_then_quiesce() {
    run_chaos_test(|| async move {
        let mut h = ChaosHarness::boot().await;
        h.run_step(&Step::AddTable).await;
        h.run_step(&Step::Insert {
            table_idx: 2,
            id: 7,
        })
        .await;
        h.run_step(&Step::Insert {
            table_idx: 0,
            id: 3,
        })
        .await;
        h.run_step(&Step::Tick { ms: 300 }).await;
        h.run_step(&Step::Update {
            table_idx: 0,
            id: 3,
        })
        .await;
        h.run_step(&Step::Tick { ms: 300 }).await;
        assert_no_data_loss(&mut h).await.unwrap();
    });
}

/// Regression: `Restart` after CDC progress used to trip
/// `SlotAheadOfCheckpoint` because the validation invariant compared
/// `slot.restart_lsn` against the per-table one-shot
/// `snapshot_lsn` (instead of the moving `coord_flushed_lsn`).
/// Fixed in `crates/pg2iceberg-validate/src/lib.rs` invariant 5.
#[test]
fn regression_restart_after_cdc_passes_validation() {
    run_chaos_test(|| async move {
        let mut h = ChaosHarness::boot().await;
        h.run_step(&Step::Insert {
            table_idx: 0,
            id: 1,
        })
        .await;
        h.run_step(&Step::Tick { ms: 200 }).await;
        // CDC has advanced slot past snapshot_lsn. Restart must
        // validate cleanly.
        h.run_step(&Step::Restart).await;
        h.run_step(&Step::Insert {
            table_idx: 0,
            id: 2,
        })
        .await;
        h.run_step(&Step::Tick { ms: 500 }).await;
        assert_no_data_loss(&mut h).await.unwrap();
    });
}

/// Regression: an empty table added via `AddTable` would later trip
/// `SnapshotCompleteButTableNoSnapshot` on Restart — the invariant
/// fired even though "snapshot of an empty source produced no
/// catalog snapshot" is the correct behavior. Invariant removed
/// from `crates/pg2iceberg-validate/src/lib.rs` (was invariant 7).
#[test]
fn regression_add_empty_table_then_restart_validates() {
    run_chaos_test(|| async move {
        let mut h = ChaosHarness::boot().await;
        h.run_step(&Step::AddTable).await;
        // Don't insert anything into the new table. Restart must
        // not reject just because chaos_t2 has no Iceberg snapshot.
        h.run_step(&Step::Restart).await;
        h.run_step(&Step::Tick { ms: 200 }).await;
        assert_no_data_loss(&mut h).await.unwrap();
    });
}

#[test]
fn fault_blob_put_then_recovery_keeps_parity() {
    run_chaos_test(|| async move {
        let mut h = ChaosHarness::boot().await;
        h.run_step(&Step::Insert {
            table_idx: 0,
            id: 1,
        })
        .await;
        h.run_step(&Step::Insert {
            table_idx: 1,
            id: 2,
        })
        .await;
        h.run_step(&Step::InjectFault {
            op: ops::BLOB_PUT,
            count: 2,
        })
        .await;
        h.run_step(&Step::Tick { ms: 500 }).await;
        h.run_step(&Step::Restart).await;
        h.run_step(&Step::Tick { ms: 500 }).await;
        assert_no_data_loss(&mut h).await.unwrap();
    });
}
