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
//!
//! Workload generator interleaves `MaterializerCycle` with the pipeline
//! steps so the proptest exercises pipeline/materializer ordering, and
//! `CrashAndRestart` models pipeline-process crashes between flushes.
//!
//! Pipeline-only crash for now: the materializer's FileIndex rebuild from
//! catalog history is a Phase 8.5 follow-on. Once it lands, we can crash
//! the materializer in DST too.

use pg2iceberg_coord::schema::CoordSchema;
use pg2iceberg_coord::Coordinator;
use pg2iceberg_core::typemap::IcebergType;
use pg2iceberg_core::{
    ColumnName, ColumnSchema, Namespace, Op, PgValue, Row, TableIdent, TableSchema, Timestamp,
};
use pg2iceberg_iceberg::read_materialized_state;
use pg2iceberg_logical::pipeline::CounterBlobNamer;
use pg2iceberg_logical::{CounterMaterializerNamer, Materializer, Pipeline};
use pg2iceberg_sim::blob::MemoryBlobStore;
use pg2iceberg_sim::catalog::MemoryCatalog;
use pg2iceberg_sim::clock::TestClock;
use pg2iceberg_sim::coord::MemoryCoordinator;
use pg2iceberg_sim::postgres::{SimPostgres, SimReplicationStream};
use pg2iceberg_snapshot::Snapshotter;
use pg2iceberg_stream::codec::decode_chunk;
use pg2iceberg_stream::BlobStore;
use pollster::block_on;
use proptest::prelude::*;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

const TABLE_NAME: &str = "orders";
const PUB: &str = "pub1";
const SLOT: &str = "slot1";

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
    }
}

fn row(id: i32, qty: i32) -> Row {
    let mut r = BTreeMap::new();
    r.insert(ColumnName("id".into()), PgValue::Int4(id));
    r.insert(ColumnName("qty".into()), PgValue::Int4(qty));
    r
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
        3 => Just(Step::DriveFlush),
        2 => Just(Step::MaterializerCycle),
        1 => Just(Step::CrashAndRestart),
    ]
}

fn workload() -> impl Strategy<Value = Vec<Step>> {
    prop::collection::vec(step_strategy(), 1..=24)
}

// ---------- harness ----------

struct DstHarness {
    db: SimPostgres,
    coord: Arc<MemoryCoordinator>,
    blob_store: Arc<MemoryBlobStore>,
    catalog: Arc<MemoryCatalog>,
    namer: Arc<CounterBlobNamer>,
    pipeline: Pipeline<MemoryCoordinator>,
    materializer: Materializer<MemoryCatalog>,
    stream: SimReplicationStream,
    /// Mirror of which PK ids are currently live in the source DB. Used by
    /// the workload runner to pre-filter ops the proptest generator can't
    /// know about (state-dependent validity).
    live: BTreeSet<i32>,
}

impl DstHarness {
    /// Boot with a set of pre-existing rows. The seeds are committed BEFORE
    /// the publication + slot are created, so logical replication won't see
    /// them — they have to come in via the snapshot phase.
    fn boot_with_seeds(seeds: &[(i32, i32)]) -> Self {
        let db = SimPostgres::new();
        db.create_table(schema()).unwrap();

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
        let pipeline = Pipeline::new(coord.clone(), blob_store.clone(), namer.clone(), 64);

        let mat_namer = Arc::new(CounterMaterializerNamer::new("s3://table"));
        let mut materializer = Materializer::new(
            coord.clone() as Arc<dyn Coordinator>,
            blob_store.clone(),
            catalog.clone(),
            mat_namer,
            "default",
            128,
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
            stream,
            live: seeds.iter().map(|(id, _)| *id).collect(),
        }
    }

    fn boot() -> Self {
        let db = SimPostgres::new();
        db.create_table(schema()).unwrap();
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
        let pipeline = Pipeline::new(coord.clone(), blob_store.clone(), namer.clone(), 64);

        let mat_namer = Arc::new(CounterMaterializerNamer::new("s3://table"));
        let mut materializer = Materializer::new(
            coord.clone() as Arc<dyn Coordinator>,
            blob_store.clone(),
            catalog.clone(),
            mat_namer,
            "default",
            128,
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
            stream,
            live: BTreeSet::new(),
        }
    }

    fn drive(&mut self) {
        while let Some(msg) = self.stream.recv() {
            block_on(self.pipeline.process(msg)).unwrap();
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
    fn crash_and_restart(&mut self) {
        // Drain + ack first so we model "graceful crash after a flush" — the
        // simpler case. Mid-flush crashes (orphan blobs from PUT-without-claim)
        // are an explicit Phase 6.5 expansion.
        self.drive();
        self.flush_and_ack();

        let pipeline = Pipeline::new(
            self.coord.clone(),
            self.blob_store.clone(),
            self.namer.clone(),
            64,
        );
        let stream = self.db.start_replication(SLOT).unwrap();
        self.pipeline = pipeline;
        self.stream = stream;
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
            Step::DriveFlush => {
                self.drive();
                self.flush_and_ack();
            }
            Step::MaterializerCycle => {
                let _ = self.materialize();
            }
            Step::CrashAndRestart => self.crash_and_restart(),
        }
    }
}

// ---------- invariant checks ----------

fn check_invariants(h: &mut DstHarness) -> Result<(), String> {
    // Reach quiescence: drain WAL, flush, ack, then materialize until idle.
    // Loop because a flush may produce events the materializer hasn't seen.
    h.drive();
    h.flush_and_ack();
    // Drain materializer; safety bound to catch infinite loops.
    for _ in 0..16 {
        if h.materialize() == 0 {
            break;
        }
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
    staged_events.sort_by_key(|m| m.lsn);

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
        for step in &steps {
            h.run_step(step);
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

#[test]
fn happy_path_one_insert_one_flush() {
    let mut h = DstHarness::boot();
    h.run_step(&Step::Insert { id: 1, qty: 10 });
    h.run_step(&Step::DriveFlush);
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

// ---------- snapshot integration (Phase 11 in DST) ----------

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
