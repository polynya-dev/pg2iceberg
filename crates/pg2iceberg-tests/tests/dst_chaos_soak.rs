//! Chaos soak DST.
//!
//! A single proptest seed in this file exercises *most* of the system at
//! once: a multi-table snapshot bootstrap, steady-state CDC over a
//! wide-typed schema, occasional "very large value" (TOAST-class)
//! writes, randomly-injected faults across blob / coord / catalog,
//! pipeline crash-and-restart, runtime additions of new tables via
//! `ALTER PUBLICATION ADD TABLE` (slot stays alive — the operator
//! action that extends an existing pg2iceberg deployment), and
//! maintenance (compact / expire / orphan-cleanup) all interleaved.
//!
//! The headline invariant after each seed is the one that matters:
//! **for every registered table, `read_table(SimPostgres) ==
//! read_materialized_state(MemoryCatalog)`** at quiescence. No data
//! loss, no phantom rows, no torn types.
//!
//! Scale knobs intentionally stay small enough that the in-memory sim
//! finishes in well under a second per seed. Realistic byte-scale
//! belongs in a separate testcontainers chaos suite — the failure
//! surface here is correctness under interleaving, not throughput.
//!
//! What this file *doesn't* model directly:
//!
//! - Real PG-process crashes — `SimPostgres` doesn't simulate WAL replay
//!   from a torn file. We simulate the operator-visible failure surface
//!   instead (slot disappearance, publication change, oid bump). Real
//!   crashes belong in the testcontainers suite.
//! - The materializer's FileIndex rebuild from catalog — same gap as
//!   the rest of the DST suite. `crash_and_restart` only crashes the
//!   pipeline.

use pg2iceberg_coord::schema::CoordSchema;
use pg2iceberg_coord::Coordinator;
use pg2iceberg_core::typemap::IcebergType;
use pg2iceberg_core::value::TimestampMicros;
use pg2iceberg_core::{
    ColumnName, ColumnSchema, Namespace, PgValue, Row, TableIdent, TableSchema, Timestamp,
};
use pg2iceberg_iceberg::read_materialized_state;
use pg2iceberg_logical::pipeline::CounterBlobNamer;
use pg2iceberg_logical::{CounterMaterializerNamer, Materializer, Pipeline};
use pg2iceberg_sim::blob::MemoryBlobStore;
use pg2iceberg_sim::catalog::MemoryCatalog;
use pg2iceberg_sim::clock::TestClock;
use pg2iceberg_sim::coord::MemoryCoordinator;
use pg2iceberg_sim::fault::{ops, FaultPlan, FaultyBlobStore, FaultyCatalog, FaultyCoordinator};
use pg2iceberg_sim::postgres::{SimPostgres, SimReplicationStream};
use pg2iceberg_snapshot::Snapshotter;
use pollster::block_on;
use proptest::prelude::*;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

// ── constants ──────────────────────────────────────────────────────

const NAMESPACE: &str = "public";
const PUB: &str = "chaos_pub";
const SLOT: &str = "chaos_slot";
const INITIAL_TABLES: usize = 2;
const SEED_ROWS_PER_TABLE: usize = 50;
const MAX_TABLES: usize = 5;

// ── wide schema ────────────────────────────────────────────────────
//
// One schema reused per table, covering the type categories that
// actually flow through the pipeline today: int, long, double, bool,
// string, binary, timestamp. Each `field_id` is stable across runs so
// the catalog roundtrip is deterministic.

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
//
// Workload is generated by proptest (deterministic from seed). Each
// row's payload also has to be deterministic so re-running the same
// seed produces byte-identical Iceberg state. We don't care about the
// *content* — just that PG and Iceberg agree on it after replication.

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
    // Float8 with values small enough that the JSON round-trip in
    // the staged-event codec (`serde_json::to_string` →
    // `serde_json::from_str`) preserves them bit-exactly. Anything
    // above ~2^53 is one-ULP-fragile; we keep the value in [-1e6,
    // 1e6] and use a simple deterministic mapping. Avoids NaN by
    // construction (no `f64::from_bits`).
    let score_int = (h as i32) % 1_000_000; // bounded, two's-complement OK
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

/// Faults the workload may inject. Limited to ops where injection
/// usefully exercises a recovery path. Materializer-side ops
/// (cat.commit_snapshot) cause the materializer cycle to error and the
/// rows to remain unmaterialized until the next clean cycle.
const FAULTABLE_OPS: &[&str] = &[
    ops::BLOB_PUT,
    ops::BLOB_GET,
    ops::COORD_CLAIM,
    ops::COORD_SAVE_CP,
    ops::CAT_COMMIT_SNAPSHOT,
];

#[derive(Clone, Debug)]
enum Step {
    /// Insert a wide row with a normal-sized payload (~64 bytes).
    Insert { table_idx: usize, id: i32 },
    /// Update a row, regenerating every column. Skipped at run time if
    /// the PK doesn't currently exist.
    Update { table_idx: usize, id: i32 },
    /// Delete by PK. Skipped if the PK isn't live.
    Delete { table_idx: usize, id: i32 },
    /// Insert with a ~32 KiB Bytea — exercises chunking and the
    /// large-value path. Doesn't actually trip PG's TOAST machinery
    /// (sim doesn't simulate it), but it stresses the wire codec the
    /// same way TOAST-class rows would.
    InsertToast { table_idx: usize, id: i32 },
    /// Drive replication → flush → ack. The complete pipeline cycle.
    DriveFlush,
    /// One materializer cycle for every registered table.
    MaterializerCycle,
    /// Maintenance: compact every table whose live-file count crosses
    /// the (lowered for tests) threshold.
    Compact,
    /// Maintenance: expire snapshots older than retention.
    Expire,
    // (Orphan cleanup is intentionally NOT a workload step. With
    // `grace=0` and the sim's monotonic millisecond counter, mid-
    // chaos cleanup nukes legitimately-recent staging blobs that the
    // catalog hasn't yet committed — the cleanup would race the
    // commit. We exercise the orphan-cleanup code path *once* in
    // `assert_no_data_loss`, where the system is quiescent.)
    /// Schedule the next `count` calls to `op` to fail. The fault
    /// counter advances on every call, so we don't pin to absolute
    /// indices — we just say "the next N invocations" by observing
    /// the current counter.
    InjectFault { op: &'static str, count: u64 },
    /// Pipeline-process crash. Drains, acks, then drops the pipeline
    /// and reopens replication from `restart_lsn`.
    CrashAndRestart,
    /// Runtime table addition: drop slot+publication, recreate with
    /// one extra table, pre-seed rows in PG, snapshot the new table.
    /// Models "operator changes config and redeploys."
    AddTable,
}

fn step_strategy() -> impl Strategy<Value = Step> {
    let id = 1i32..=12;
    let table_idx = 0usize..MAX_TABLES;
    prop_oneof![
        // Steady-state DML — biased high so chaos doesn't dominate.
        12 => (table_idx.clone(), id.clone())
            .prop_map(|(t, i)| Step::Insert { table_idx: t, id: i }),
        8 => (table_idx.clone(), id.clone())
            .prop_map(|(t, i)| Step::Update { table_idx: t, id: i }),
        5 => (table_idx.clone(), id.clone())
            .prop_map(|(t, i)| Step::Delete { table_idx: t, id: i }),
        2 => (table_idx.clone(), id.clone())
            .prop_map(|(t, i)| Step::InsertToast { table_idx: t, id: i }),
        // Pipeline + materializer driving.
        6 => Just(Step::DriveFlush),
        4 => Just(Step::MaterializerCycle),
        // Maintenance.
        2 => Just(Step::Compact),
        1 => Just(Step::Expire),
        // Chaos.
        2 => Just(Step::CrashAndRestart),
        2 => (prop::sample::select(FAULTABLE_OPS), 1u64..=3)
            .prop_map(|(op, count)| Step::InjectFault { op, count }),
        // Topology evolution.
        1 => Just(Step::AddTable),
    ]
}

fn workload() -> impl Strategy<Value = Vec<Step>> {
    prop::collection::vec(step_strategy(), 16..=80)
}

// ── harness ────────────────────────────────────────────────────────

struct ChaosHarness {
    db: SimPostgres,
    /// The non-faulty coordinator. Held so future invariant checks
    /// can read coord state without going through the faulty wrapper
    /// (which would tick fault counters mid-assertion).
    #[allow(dead_code)]
    coord_inner: Arc<MemoryCoordinator>,
    blob_inner: Arc<MemoryBlobStore>,
    catalog_inner: Arc<MemoryCatalog>,
    plan: FaultPlan,
    coord: Arc<FaultyCoordinator>,
    blob: Arc<FaultyBlobStore>,
    /// The faulty catalog. The materializer owns its `Arc` clone, so
    /// nothing currently reads this field directly — but holding it
    /// here lets future tests script catalog-level faults without
    /// rebuilding the harness.
    #[allow(dead_code)]
    catalog: Arc<FaultyCatalog>,
    namer: Arc<CounterBlobNamer>,
    pipeline: Pipeline<FaultyCoordinator>,
    materializer: Materializer<FaultyCatalog>,
    stream: SimReplicationStream,
    /// Currently-registered tables, by (logical) index. `tables[i].ident`
    /// is the table the workload's `table_idx == i` operates on.
    tables: Vec<TableSchema>,
    /// Per-table: which PKs are currently live in PG. Used to filter
    /// state-dependent ops (Update / Delete on a missing PK is a no-op).
    live: Vec<BTreeSet<i32>>,
    /// Monotonic generation counter for the value generator. Bumped on
    /// every Insert/Update so re-generated rows are byte-distinct.
    next_gen: u64,
    /// The pipeline can sit in a hosed state after a flush error: the
    /// sink already drained its in-memory `committed` buffer past the
    /// failed PUT, so those events are gone. Recovery model is "crash
    /// and restart"; we set this flag on flush/materialize errors and
    /// the next step that touches the pipeline drains it via
    /// `crash_and_restart` before doing real work.
    pipeline_dirty: bool,
}

impl ChaosHarness {
    fn boot() -> Self {
        let db = SimPostgres::new();

        // Create initial tables and seed pre-existing rows so the
        // snapshot phase has something to do. Rows are committed
        // BEFORE the publication exists, so logical replication won't
        // see them — they have to come in via Snapshotter.
        let mut tables = Vec::new();
        for i in 0..INITIAL_TABLES {
            let s = wide_schema(i);
            db.create_table(s.clone()).unwrap();
            tables.push(s);
        }
        let mut live: Vec<BTreeSet<i32>> = vec![BTreeSet::new(); INITIAL_TABLES];
        let mut gen: u64 = 0;
        for (i, schema) in tables.iter().enumerate() {
            let mut tx = db.begin_tx();
            for k in 0..SEED_ROWS_PER_TABLE {
                // PK domain for seeds is 1000+; workload uses 1..=12 so
                // they don't collide.
                let id = 1000 + (i * SEED_ROWS_PER_TABLE + k) as i32;
                tx.insert(&schema.ident, wide_row(id, gen, 64));
                live[i].insert(id);
                gen += 1;
            }
            tx.commit(Timestamp(0)).unwrap();
        }

        let publish_idents: Vec<TableIdent> = tables.iter().map(|s| s.ident.clone()).collect();
        db.create_publication(PUB, &publish_idents).unwrap();
        db.create_slot(SLOT, PUB).unwrap();

        let clock = TestClock::at(0);
        let arc_clock: Arc<dyn pg2iceberg_core::Clock> = Arc::new(clock);
        let coord_inner = Arc::new(MemoryCoordinator::new(
            CoordSchema::default_name(),
            arc_clock,
        ));
        let blob_inner = Arc::new(MemoryBlobStore::new());
        let catalog_inner = Arc::new(MemoryCatalog::new());
        let plan = FaultPlan::new();
        let coord = Arc::new(FaultyCoordinator::new(coord_inner.clone(), plan.clone()));
        let blob = Arc::new(FaultyBlobStore::new(blob_inner.clone(), plan.clone()));
        let catalog = Arc::new(FaultyCatalog::new(catalog_inner.clone(), plan.clone()));

        let namer = Arc::new(CounterBlobNamer::new("s3://stage"));
        let pipeline = Pipeline::new(coord.clone(), blob.clone(), namer.clone(), 64);

        let mat_namer = Arc::new(CounterMaterializerNamer::new("s3://table"));
        let mut materializer = Materializer::new(
            coord.clone() as Arc<dyn Coordinator>,
            blob.clone(),
            catalog.clone(),
            mat_namer,
            NAMESPACE,
            128,
        );
        for s in &tables {
            block_on(materializer.register_table(s.clone())).unwrap();
        }

        let stream = db.start_replication(SLOT).unwrap();

        let mut h = Self {
            db,
            coord_inner,
            blob_inner,
            catalog_inner,
            plan,
            coord,
            blob,
            catalog,
            namer,
            pipeline,
            materializer,
            stream,
            tables,
            live,
            next_gen: gen,
            pipeline_dirty: false,
        };

        // Run the snapshot phase up-front so the seeds become Iceberg
        // rows. Subsequent CDC mutates them in place. The snapshotter
        // can't fail at boot (no faults are scheduled yet), but if a
        // future seed causes one we'd fall through to the recovery
        // loop in `assert_no_data_loss`.
        let schemas = h.tables.clone();
        let s = Snapshotter::new(h.coord.clone() as Arc<dyn Coordinator>);
        let snap_lsn = block_on(s.run(&h.db, &schemas, &mut h.pipeline)).expect("initial snapshot");
        h.stream.send_standby(snap_lsn);
        block_on(h.materializer.cycle()).unwrap();

        h
    }

    // ── pipeline driving ─────────────────────────────────────────

    fn drive(&mut self) {
        while let Some(msg) = self.stream.recv() {
            // process() can fail under fault injection too — treat any
            // error as "the pipeline is hosed, fall through to crash".
            if block_on(self.pipeline.process(msg)).is_err() {
                self.pipeline_dirty = true;
                return;
            }
        }
    }

    fn try_flush(&mut self) {
        // `flush()` returns `Ok(Option<Lsn>)` — `None` if there was
        // nothing to flush, `Some(lsn)` for the newly-durable LSN. We
        // ack `flushed_lsn()` either way; that's the canonical
        // "what's durable" value the standby should advance to.
        match block_on(self.pipeline.flush()) {
            Ok(_) => self.stream.send_standby(self.pipeline.flushed_lsn()),
            Err(_) => self.pipeline_dirty = true,
        }
    }

    fn try_materialize(&mut self) {
        if block_on(self.materializer.cycle()).is_err() {
            // Materializer errors don't dirty the pipeline; the
            // pipeline's flushed_lsn is already durable in coord. The
            // next clean cycle picks up where this one left off.
        }
    }

    fn crash_and_restart(&mut self) {
        // Drain whatever's in the stream so we don't strand events in
        // the closing reader (mirrors dst_fault.rs).
        self.drive();
        let _ = block_on(self.pipeline.flush());
        // Whatever the result, we're rebuilding the pipeline. The
        // sink's in-memory committed buffer is dropped; the next
        // start_replication will replay from the slot's restart_lsn.
        let pipeline = Pipeline::new(
            self.coord.clone(),
            self.blob.clone(),
            self.namer.clone(),
            64,
        );
        let stream = self.db.start_replication(SLOT).expect("start_replication");
        self.pipeline = pipeline;
        self.stream = stream;
        self.pipeline_dirty = false;
    }

    /// Recover the pipeline if a previous step left it dirty. Called
    /// at the top of every step; safe to call when clean.
    fn ensure_clean_pipeline(&mut self) {
        if self.pipeline_dirty {
            self.crash_and_restart();
        }
    }

    // ── runtime table addition ───────────────────────────────────

    fn add_table(&mut self) {
        if self.tables.len() >= MAX_TABLES {
            return;
        }
        // Drain + ack so the slot is caught up before we extend the
        // publication. The slot stays alive across the addition.
        self.drive();
        self.try_flush();
        if self.pipeline_dirty {
            self.crash_and_restart();
            return;
        }

        let new_idx = self.tables.len();
        let new_schema = wide_schema(new_idx);

        // Models the operator's `ALTER PUBLICATION pub ADD TABLE t`:
        // create the table in PG, then attach it to the existing
        // publication. `SimReplicationStream::recv` re-reads the
        // publication's table set on every call, so the slot starts
        // seeing future events for the new table on the next recv —
        // no slot drop needed.
        //
        // We deliberately do NOT pre-seed rows for runtime-added
        // tables. Pre-existing rows in a runtime-added table would
        // require a coordinated snapshot/CDC fence that prod's
        // `run_logical_lifecycle` doesn't expose for mid-stream
        // additions today (snapshot phase is gated on
        // `slot_was_fresh`). Empty start + CDC fill is the honest
        // model for an `ALTER PUBLICATION ADD TABLE` workflow.
        if self.db.create_table(new_schema.clone()).is_err() {
            return;
        }
        let new_ident = new_schema.ident.clone();

        // Attach to the live publication. Sim mirrors PG's
        // `ALTER PUBLICATION ADD TABLE` semantics — slot stays open,
        // future events flow.
        if self.db.add_table_to_publication(PUB, &new_ident).is_err() {
            return;
        }

        // Register on the materializer (catalog table created here)
        // and on the harness's bookkeeping. The Pipeline learns the
        // ident lazily, via Relation/Change messages from the live
        // slot — same path prod takes for any new table that appears
        // in the WAL stream.
        if block_on(self.materializer.register_table(new_schema.clone())).is_err() {
            return;
        }
        self.tables.push(new_schema);
        self.live.push(BTreeSet::new());
    }

    // ── step dispatch ────────────────────────────────────────────

    fn run_step(&mut self, step: &Step) {
        // Always heal first. Most steps assume the pipeline can take work.
        self.ensure_clean_pipeline();

        match step {
            Step::Insert { table_idx, id } => self.do_insert(*table_idx, *id, 64),
            Step::InsertToast { table_idx, id } => self.do_insert(*table_idx, *id, 32 * 1024),
            Step::Update { table_idx, id } => self.do_update(*table_idx, *id),
            Step::Delete { table_idx, id } => self.do_delete(*table_idx, *id),
            Step::DriveFlush => {
                self.drive();
                self.try_flush();
            }
            Step::MaterializerCycle => {
                self.try_materialize();
            }
            Step::Compact => {
                let cfg = pg2iceberg_iceberg::CompactionConfig {
                    data_file_threshold: 2,
                    delete_file_threshold: 1,
                    target_size_bytes: 4 * 1024 * 1024,
                };
                let _ = block_on(self.materializer.compact_cycle(&cfg));
            }
            Step::Expire => {
                // Sim-fidelity caveat: `MemoryCatalog` stores
                // `data_files` per-snapshot as a delta, while real
                // Iceberg keeps data files reachable via shared
                // manifests across snapshots. In real Iceberg,
                // dropping a snapshot is non-destructive; in the sim,
                // it hides that snapshot's delta files from
                // `read_materialized_state`. We still want the
                // expire-cycle call path under test, so use a huge
                // retention that keeps every snapshot — exercise the
                // code without provoking the sim gap.
                let _ = block_on(self.materializer.expire_cycle(i64::MAX / 4));
            }
            Step::InjectFault { op, count } => {
                let base = self.plan.counter(op);
                self.plan.fail(op, base..(base + *count));
            }
            Step::CrashAndRestart => self.crash_and_restart(),
            Step::AddTable => self.add_table(),
        }
    }

    fn do_insert(&mut self, table_idx: usize, id: i32, payload: usize) {
        if table_idx >= self.tables.len() {
            return;
        }
        if self.live[table_idx].contains(&id) {
            return;
        }
        let ident = self.tables[table_idx].ident.clone();
        let mut tx = self.db.begin_tx();
        tx.insert(&ident, wide_row(id, self.next_gen, payload));
        if tx.commit(Timestamp(0)).is_ok() {
            self.live[table_idx].insert(id);
            self.next_gen += 1;
        }
    }

    fn do_update(&mut self, table_idx: usize, id: i32) {
        if table_idx >= self.tables.len() {
            return;
        }
        if !self.live[table_idx].contains(&id) {
            return;
        }
        let ident = self.tables[table_idx].ident.clone();
        let mut tx = self.db.begin_tx();
        tx.update(&ident, wide_row(id, self.next_gen, 64));
        if tx.commit(Timestamp(0)).is_ok() {
            self.next_gen += 1;
        }
    }

    fn do_delete(&mut self, table_idx: usize, id: i32) {
        if table_idx >= self.tables.len() {
            return;
        }
        if !self.live[table_idx].contains(&id) {
            return;
        }
        let ident = self.tables[table_idx].ident.clone();
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

/// Render a one-line description of the first place two row vectors
/// disagree. Both vectors must already be sorted by PK.
fn first_row_diff(pg: &[Row], ice: &[Row]) -> String {
    let n = pg.len().min(ice.len());
    for i in 0..n {
        if pg[i] != ice[i] {
            // Pinpoint the first column that differs so the message
            // doesn't dump the full wide row to the panic output.
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

/// After a chaotic workload, drive the system back to a quiescent
/// state with all faults cleared and assert PG ↔ Iceberg parity for
/// every registered table.
///
/// The recovery loop has to make multiple passes because:
/// - A flush failure may have left the pipeline dirty (drained-past-
///   the-failed-PUT events lost from memory) → we crash-restart and
///   replay from `restart_lsn`.
/// - A materializer failure may have left log_index entries
///   un-applied → next clean cycle picks them up.
/// - Compaction / expiry running mid-workload doesn't change row
///   identity, but rebuilds FileIndex; the next cycle has to settle.
fn assert_no_data_loss(h: &mut ChaosHarness) -> Result<(), String> {
    h.plan.clear_failures();
    // Always crash-and-restart entering recovery: the simplest model
    // matches what an operator does after seeing the pipeline error.
    h.crash_and_restart();

    // Drive to quiescence. Bound the loop so a regression can't hang.
    //
    // Note: we deliberately do NOT re-run `Snapshotter::run` here.
    // Prod's restart path (validate::runtime) gates the snapshot
    // phase on `slot_was_fresh = !slot_exists()` — on a normal
    // restart with an existing slot, snapshot is skipped entirely.
    // To stay honest about what we're testing, recovery in the sim
    // matches that: we just heal the pipeline and drive CDC to
    // quiescence. `add_table` is responsible for ensuring its own
    // snapshot completes before returning (it retries internally on
    // fault), since the only path for the seed rows to reach Iceberg
    // is through the snapshotter — they're not in WAL after the
    // slot was created.
    let mut last_progress = 0usize;
    for _ in 0..32 {
        h.drive();
        h.try_flush();
        if h.pipeline_dirty {
            h.crash_and_restart();
            continue;
        }
        let n =
            block_on(h.materializer.cycle()).map_err(|e| format!("recovery materialize: {e}"))?;
        if n == 0 && last_progress == 0 {
            break;
        }
        last_progress = n;
    }

    // Now that the system is quiescent, exercise orphan cleanup.
    // Pick a `now_ms` past every blob's last_modified_ms so the
    // grace check considers everything "old enough", but keep
    // `grace=0` so anything still referenced by catalog is safe by
    // virtue of being referenced (orphan cleanup checks reference,
    // not freshness, for protection). Errors here are logged-and-
    // swallowed by `cleanup_orphans_cycle` itself; the parity check
    // below is what matters.
    let _ = block_on(
        h.materializer
            .cleanup_orphans_cycle("s3://table/", i64::MAX, 0),
    );

    // Per-table parity. Ordering by PK matters because Iceberg's
    // materialized state isn't insertion-ordered.
    for schema in &h.tables {
        let ident = &schema.ident;
        let mut pg_rows =
            h.db.read_table(ident)
                .map_err(|e| format!("read_table({ident}): {e}"))?;
        let mut ice_rows = block_on(read_materialized_state(
            h.catalog_inner.as_ref(),
            h.blob_inner.as_ref(),
            ident,
            schema,
            &[ColumnName("id".into())],
        ))
        .map_err(|e| format!("read_materialized_state({ident}): {e}"))?;
        sort_by_pk(&mut pg_rows);
        sort_by_pk(&mut ice_rows);
        if pg_rows != ice_rows {
            // Surface the first PK whose row content differs (or
            // count mismatch). Saves a debug cycle compared to just
            // dumping both PK lists.
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

// ── proptest ───────────────────────────────────────────────────────

proptest! {
    #![proptest_config(ProptestConfig {
        // Small `cases` keeps `cargo test` snappy; the headline soak
        // run is `cargo test --release dst_chaos_soak -- --ignored
        // --nocapture` (see `chaos_soak_extended_seeds` below) once
        // CI is willing to spend a minute on it.
        cases: 32,
        ..ProptestConfig::default()
    })]

    #[test]
    fn chaos_soak_preserves_data_integrity(steps in workload()) {
        let mut h = ChaosHarness::boot();
        for step in &steps {
            h.run_step(step);
        }
        if let Err(e) = assert_no_data_loss(&mut h) {
            // proptest will shrink and re-print; the error message
            // includes per-table row counts and PK lists for triage.
            panic!("workload {:?}\nfailed: {}", steps, e);
        }
    }
}

// ── extended soak (opt-in) ─────────────────────────────────────────
//
// The proptest above runs 32 cases per `cargo test` invocation. That's
// enough to surface regressions during PR CI but often misses rare
// interleavings that need 200+ seeds to manifest. The ignored test
// below cranks the case count up; run with `cargo test --release
// chaos_soak_extended_seeds -- --ignored --nocapture` when you want
// the longer soak (a few minutes locally).

#[test]
#[ignore]
fn chaos_soak_extended_seeds() {
    let mut runner = proptest::test_runner::TestRunner::new(proptest::test_runner::Config {
        cases: 256,
        ..proptest::test_runner::Config::default()
    });
    runner
        .run(&workload(), |steps| {
            let mut h = ChaosHarness::boot();
            for step in &steps {
                h.run_step(step);
            }
            assert_no_data_loss(&mut h).map_err(|e| {
                proptest::test_runner::TestCaseError::fail(format!(
                    "workload {:?}\nfailed: {}",
                    steps, e
                ))
            })?;
            Ok(())
        })
        .unwrap();
}

// ── pinned regressions ─────────────────────────────────────────────
//
// Failing seeds the chaos soak surfaces get pinned here as
// deterministic tests, so the regression doesn't reappear silently.
// Empty until the first interesting failure lands.

#[test]
fn smoke_boot_and_quiesce_without_workload() {
    // The simplest possible exercise: snapshot 100 seeded rows across
    // 2 tables, then assert parity. If this fails the harness itself
    // is broken, not the code under test.
    let mut h = ChaosHarness::boot();
    assert_no_data_loss(&mut h).unwrap();
}

#[test]
fn add_table_then_workload_then_quiesce() {
    let mut h = ChaosHarness::boot();
    h.run_step(&Step::AddTable);
    h.run_step(&Step::Insert {
        table_idx: 2,
        id: 7,
    });
    h.run_step(&Step::Insert {
        table_idx: 0,
        id: 3,
    });
    h.run_step(&Step::DriveFlush);
    h.run_step(&Step::MaterializerCycle);
    h.run_step(&Step::Update {
        table_idx: 0,
        id: 3,
    });
    h.run_step(&Step::DriveFlush);
    h.run_step(&Step::MaterializerCycle);
    assert_no_data_loss(&mut h).unwrap();
}

#[test]
fn fault_blob_put_then_recovery_keeps_parity() {
    let mut h = ChaosHarness::boot();
    h.run_step(&Step::Insert {
        table_idx: 0,
        id: 1,
    });
    h.run_step(&Step::Insert {
        table_idx: 1,
        id: 2,
    });
    h.run_step(&Step::InjectFault {
        op: ops::BLOB_PUT,
        count: 2,
    });
    h.run_step(&Step::DriveFlush);
    h.run_step(&Step::DriveFlush);
    assert_no_data_loss(&mut h).unwrap();
}

#[test]
fn maintenance_runs_during_steady_state() {
    let mut h = ChaosHarness::boot();
    for i in 0..5 {
        h.run_step(&Step::Insert {
            table_idx: 0,
            id: i,
        });
        h.run_step(&Step::DriveFlush);
        h.run_step(&Step::MaterializerCycle);
    }
    h.run_step(&Step::Compact);
    h.run_step(&Step::Expire);
    // Orphan cleanup is run by `assert_no_data_loss` in its
    // quiescent recovery phase, not as a workload step — see the
    // comment on `Step` for why.
    assert_no_data_loss(&mut h).unwrap();
}
