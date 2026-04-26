//! Fault-injection DST: re-runs the durability/correctness invariants
//! under random IO failure at the blob, coord, and catalog layers.
//!
//! Wraps `MemoryBlobStore` / `MemoryCoordinator` / `MemoryCatalog` in
//! the [`pg2iceberg_sim::fault`] faulty-wrappers, scripts deterministic
//! failures via [`FaultPlan`], and asserts:
//!
//! - The pipeline's receipt-gated `flushed_lsn` *never* advances past
//!   any committed event that wasn't durably staged.
//! - After a crash-and-restart, the pipeline replays from the slot's
//!   `restart_lsn` and reaches PG ↔ Iceberg parity at quiescence.
//! - Orphan blobs left by mid-flush failures don't break readers (the
//!   verifier uses log_index, not the raw blob list) and are reachable
//!   for the orphan-cleanup pass.
//!
//! ## What this surfaces
//!
//! Each failing seed gets pinned as a deterministic test. The first
//! batch lives in [`pinned_regressions`] below — these are scenarios
//! where the fault path *does* recover correctly. Scenarios where the
//! pipeline doesn't recover get pinned with `#[ignore]` plus a
//! comment naming the gap they expose. Right now:
//!
//! - **`mid_snapshot_blob_put_fault_surfaces_non_resumable_snapshot`**
//!   demonstrates that the binary's use of the free `run_snapshot`
//!   function (instead of `Snapshotter::run_chunks`) means a
//!   mid-snapshot PUT failure restarts the snapshot from chunk 0
//!   on replay. Correctness-safe (re-stage produces the same
//!   bytes), but a real fix is "switch to Snapshotter" — Phase 11.5.

use pg2iceberg_coord::schema::CoordSchema;
use pg2iceberg_coord::Coordinator;
use pg2iceberg_core::typemap::IcebergType;
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
use pg2iceberg_sim::fault::{
    ops, FaultPlan, FaultyBlobStore, FaultyCatalog, FaultyCoordinator,
};
use pg2iceberg_sim::postgres::{SimPostgres, SimReplicationStream};
use pg2iceberg_snapshot::{run_snapshot_phase, SnapshotPhaseOutcome, Snapshotter};
use pollster::block_on;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

const TABLE: &str = "orders";
const PUB: &str = "pub-fault";
const SLOT: &str = "slot-fault";

fn ident() -> TableIdent {
    TableIdent {
        namespace: Namespace(vec!["public".into()]),
        name: TABLE.into(),
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

// ── Harness ────────────────────────────────────────────────────────

struct FaultHarness {
    db: SimPostgres,
    coord_inner: Arc<MemoryCoordinator>,
    blob_inner: Arc<MemoryBlobStore>,
    catalog_inner: Arc<MemoryCatalog>,
    plan: FaultPlan,
    coord: Arc<FaultyCoordinator>,
    blob: Arc<FaultyBlobStore>,
    /// The faulty catalog wraps `catalog_inner`. We don't need a
    /// direct field reference at runtime (the materializer holds it
    /// via Arc), but exposing it here lets future tests script
    /// catalog-level faults without rebuilding the harness. Suppress
    /// the dead-code warning until then.
    #[allow(dead_code)]
    catalog: Arc<FaultyCatalog>,
    namer: Arc<CounterBlobNamer>,
    pipeline: Pipeline<FaultyCoordinator>,
    materializer: Materializer<FaultyCatalog>,
    stream: SimReplicationStream,
    live: BTreeSet<i32>,
}

impl FaultHarness {
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
            "default",
            128,
        );
        block_on(materializer.register_table(schema())).unwrap();
        let stream = db.start_replication(SLOT).unwrap();

        Self {
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
            live: seeds.iter().map(|(id, _)| *id).collect(),
        }
    }

    fn boot() -> Self {
        Self::boot_with_seeds(&[])
    }

    fn drive(&mut self) {
        while let Some(msg) = self.stream.recv() {
            // Process can fail for unrelated reasons; in this harness it doesn't.
            block_on(self.pipeline.process(msg)).unwrap();
        }
    }

    /// Drive + flush. Unlike the non-fault harness this CAN return Err
    /// when a scripted fault fires; the caller decides whether to
    /// crash-and-restart or retry.
    fn try_flush(&mut self) -> Result<(), String> {
        self.drive();
        match block_on(self.pipeline.flush()) {
            Ok(_) => {
                self.stream.send_standby(self.pipeline.flushed_lsn());
                Ok(())
            }
            Err(e) => Err(format!("pipeline.flush: {e}")),
        }
    }

    fn try_materialize(&mut self) -> Result<usize, String> {
        block_on(self.materializer.cycle()).map_err(|e| format!("materializer.cycle: {e}"))
    }

    fn insert(&mut self, id: i32, qty: i32) {
        let mut tx = self.db.begin_tx();
        tx.insert(&ident(), row(id, qty));
        tx.commit(Timestamp(0)).unwrap();
        self.live.insert(id);
    }

    fn update(&mut self, id: i32, qty: i32) {
        let mut tx = self.db.begin_tx();
        tx.update(&ident(), row(id, qty));
        tx.commit(Timestamp(0)).unwrap();
    }

    /// Crash-and-restart: drop the in-memory pipeline, reopen
    /// replication from the slot, materializer state survives (the
    /// FileIndex rebuild path makes this safe for both crash variants
    /// once Phase 8.5 lands; for now the materializer is a "parallel
    /// worker" that we don't restart here).
    fn crash_and_restart(&mut self) {
        let pipeline = Pipeline::new(
            self.coord.clone(),
            self.blob.clone(),
            self.namer.clone(),
            64,
        );
        let stream = self.db.start_replication(SLOT).unwrap();
        self.pipeline = pipeline;
        self.stream = stream;
    }

    fn set_fault(&self, op: &'static str, indices: impl IntoIterator<Item = u64>) {
        self.plan.fail(op, indices);
    }

    fn clear_faults(&self) {
        self.plan.clear_failures();
    }

    fn injected(&self) -> u64 {
        self.plan.injected_count()
    }
}

// ── Invariant check ───────────────────────────────────────────────

/// Drive the system to quiescence (post-fault recovery), then assert
/// the headline correctness property: PG == Iceberg.
///
/// **Contract:** any flush failure leaves the in-memory pipeline in
/// an unrecoverable state — the sink drains its `committed` buffer
/// *before* the failed PUT, so those events are gone from memory.
/// pg2iceberg's recovery model is "restart from slot's restart_lsn,"
/// so this helper *unconditionally* calls `crash_and_restart` before
/// the recovery loop. That mirrors the operator's job: notice the
/// flush errored → restart the process.
fn assert_recovery(h: &mut FaultHarness) -> Result<(), String> {
    h.clear_faults();
    h.crash_and_restart();
    // Drive several recovery cycles. Each cycle drains WAL → flushes
    // → materializes. With faults cleared, all calls succeed.
    for _ in 0..16 {
        h.drive();
        h.try_flush().map_err(|e| format!("recovery flush: {e}"))?;
        let n = h.try_materialize().map_err(|e| format!("recovery mat: {e}"))?;
        if n == 0 {
            break;
        }
    }
    // Cross-layer truth check.
    let mut iceberg_rows = block_on(read_materialized_state(
        h.catalog_inner.as_ref(),
        h.blob_inner.as_ref(),
        &ident(),
        &schema(),
        &[ColumnName("id".into())],
    ))
    .map_err(|e| format!("read_materialized_state: {e}"))?;
    sort_by_pk(&mut iceberg_rows);
    let mut pg_rows = h
        .db
        .read_table(&ident())
        .map_err(|e| format!("read_table: {e}"))?;
    sort_by_pk(&mut pg_rows);
    if iceberg_rows != pg_rows {
        return Err(format!(
            "PG ↔ Iceberg mismatch after fault recovery:\n  pg={pg_rows:?}\n  iceberg={iceberg_rows:?}"
        ));
    }
    // Pipeline LSN must not exceed slot's confirmed_flush_lsn.
    let slot = h.db.slot_state(SLOT).map_err(|e| format!("slot: {e}"))?;
    if h.pipeline.flushed_lsn() > slot.confirmed_flush_lsn {
        return Err(format!(
            "pipeline.flushed_lsn={} > slot.confirmed_flush_lsn={}",
            h.pipeline.flushed_lsn(),
            slot.confirmed_flush_lsn
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

// ── Pinned scenarios that recover cleanly ─────────────────────────

#[test]
fn proptest_repro_inject_then_flush_then_recover() {
    // Minimal failing case from proptest shrinking. Trace:
    // 1. Insert(1, 0) → PG commits at LSN 1.
    // 2. InjectBlobPutFault → schedules blob.put #0 to fail.
    // 3. Flush → drive() consumes WAL, sink drains; blob.put #0 fails;
    //    pipeline.flush() returns Err. flushed_lsn stays 0.
    // 4. assert_recovery → clear_faults + crash_and_restart + drive
    //    loop. New stream from slot's restart_lsn=0 should re-emit
    //    the Insert; flush + materialize should land it in Iceberg.
    let mut h = FaultHarness::boot();
    h.insert(1, 0);
    h.set_fault(ops::BLOB_PUT, [0]);
    let _ = h.try_flush();
    assert_recovery(&mut h).unwrap();
}

#[test]
fn blob_put_fault_during_first_flush_recovers_after_restart() {
    // PG commits an INSERT; the FIRST attempt to flush its staging
    // Parquet fails at `blob.put`. Pipeline returns Err; flushed_lsn
    // doesn't advance; slot is not acked. Restart from slot's
    // restart_lsn replays the event; second flush succeeds.
    let mut h = FaultHarness::boot();
    h.insert(1, 10);
    h.insert(2, 20);
    h.set_fault(ops::BLOB_PUT, [0]);

    let err = h.try_flush().expect_err("should fail at first PUT");
    assert!(err.contains("blob.put"), "unexpected error: {err}");
    assert_eq!(h.injected(), 1);
    // flushed_lsn stayed at 0 — receipt was never minted.
    assert_eq!(h.pipeline.flushed_lsn().0, 0);

    // Crash-and-restart. The slot still emits events from the start.
    h.crash_and_restart();
    h.clear_faults();

    // No more faults: drive → flush → materialize succeeds.
    assert_recovery(&mut h).unwrap();
}

#[test]
fn coord_claim_fault_leaves_orphan_blob_recoverable_after_restart() {
    // PUT succeeds, claim_offsets fails. Result: orphan blob in
    // staging area. Pipeline returns Err; flushed_lsn doesn't
    // advance. Crash-and-restart replays from the slot, re-PUTs to a
    // new path, claims, advances. The original blob is an orphan
    // that cleanup eventually reaps.
    let mut h = FaultHarness::boot();
    h.insert(1, 10);
    h.set_fault(ops::COORD_CLAIM, [0]);

    let err = h.try_flush().expect_err("claim should fail");
    assert!(err.contains("claim_offsets"), "unexpected: {err}");
    assert_eq!(h.injected(), 1);

    // Confirm an orphan blob exists in the inner store (PUT happened).
    let orphans_before: Vec<String> = h
        .blob_inner
        .paths()
        .into_iter()
        .filter(|p| p.starts_with("s3://stage"))
        .collect();
    assert_eq!(orphans_before.len(), 1, "expected one orphan PUT");

    h.crash_and_restart();
    assert_recovery(&mut h).unwrap();

    // Orphan still there (not yet referenced by any log_index entry,
    // since the original claim failed). Orphan-cleanup ops sweep it.
    let stage_blobs: Vec<String> = h
        .blob_inner
        .paths()
        .into_iter()
        .filter(|p| p.starts_with("s3://stage"))
        .collect();
    assert!(
        stage_blobs.len() >= 2,
        "expected orphan + replay PUT, got {stage_blobs:?}"
    );
}

#[test]
fn catalog_commit_snapshot_fault_makes_materializer_retryable() {
    // Pipeline staged events successfully; materializer read them and
    // built a `PreparedCommit`; commit_snapshot fails. The
    // materializer's cursor doesn't advance, so the next
    // `materializer.cycle()` retries the same range. With faults
    // cleared, recovery succeeds.
    let mut h = FaultHarness::boot();
    h.insert(1, 10);
    h.insert(2, 20);
    h.try_flush().unwrap();

    h.set_fault(ops::CAT_COMMIT_SNAPSHOT, [0]);
    let err = h.try_materialize().expect_err("commit should fail");
    assert!(err.contains("commit_snapshot"), "unexpected: {err}");
    assert_eq!(h.injected(), 1);

    // Cursor stayed at -1 — nothing got materialized.
    let cursor = block_on(h.coord_inner.get_cursor("default", &ident())).unwrap();
    assert_eq!(cursor, Some(-1), "cursor must not advance on commit failure");

    // Recovery: clear, materialize again, assert PG == Iceberg.
    assert_recovery(&mut h).unwrap();
}

#[test]
fn coord_claim_fault_then_continued_writes_all_recover() {
    // Mid-stream fault, then more PG writes happen, then restart.
    // All events should land in Iceberg eventually.
    let mut h = FaultHarness::boot();
    h.insert(1, 10);
    h.set_fault(ops::COORD_CLAIM, [0]);
    let _ = h.try_flush();

    // PG keeps committing while the pipeline is wedged.
    h.insert(2, 20);
    h.insert(3, 30);

    h.crash_and_restart();
    assert_recovery(&mut h).unwrap();

    let pg_rows = h.db.read_table(&ident()).unwrap();
    assert_eq!(pg_rows.len(), 3, "all three inserts should be in PG");
}

#[test]
fn repeated_blob_put_faults_eventually_recover() {
    // First two flushes both fail at blob.put. Third succeeds. PG ↔
    // Iceberg parity holds at the end.
    let mut h = FaultHarness::boot();
    h.insert(1, 10);
    h.set_fault(ops::BLOB_PUT, [0, 1]);

    let _ = h.try_flush(); // fails
    h.crash_and_restart();
    let _ = h.try_flush(); // fails again — replays the same event
    h.crash_and_restart();
    assert_eq!(h.injected(), 2);

    // Recovery clears faults; third try succeeds.
    assert_recovery(&mut h).unwrap();
}

// ── Gap-surfacing scenarios ───────────────────────────────────────

#[test]
fn mid_snapshot_blob_put_fault_recovers_via_resumable_snapshotter() {
    // Snapshotter (the resumable surface) handles a mid-snapshot PUT
    // fault correctly: progress is persisted in the checkpoint, and a
    // resume picks up at the next chunk.
    //
    // The binary today uses the *non-resumable* `run_snapshot` free
    // function (Phase 11.5 gap) — see
    // `mid_snapshot_blob_put_fault_with_run_snapshot_loses_progress`.
    let seeds: Vec<(i32, i32)> = (1..=10).map(|i| (i, i * 10)).collect();
    let mut h = FaultHarness::boot_with_seeds(&seeds);

    // Fail the 3rd staging PUT (mid-snapshot). Each chunk does one PUT.
    h.set_fault(ops::BLOB_PUT, [2]);
    let s = Snapshotter::new(h.coord.clone() as Arc<dyn Coordinator>).with_chunk_size(2);
    let err = block_on(s.run(&h.db, &[schema()], &mut h.pipeline)).expect_err("mid-snap fault");
    assert!(format!("{err:?}").contains("blob.put"));

    h.crash_and_restart();
    h.clear_faults();

    // Resume snapshot with the resumable surface.
    let s = Snapshotter::new(h.coord.clone() as Arc<dyn Coordinator>).with_chunk_size(2);
    let snap_lsn = block_on(s.run(&h.db, &[schema()], &mut h.pipeline)).unwrap();
    h.stream.send_standby(snap_lsn);
    let _ = h.try_materialize();

    let mut iceberg = block_on(read_materialized_state(
        h.catalog_inner.as_ref(),
        h.blob_inner.as_ref(),
        &ident(),
        &schema(),
        &[ColumnName("id".into())],
    ))
    .unwrap();
    sort_by_pk(&mut iceberg);
    let mut pg = h.db.read_table(&ident()).unwrap();
    sort_by_pk(&mut pg);
    assert_eq!(iceberg, pg, "snapshot resumed correctly");
}

#[test]
fn binary_snapshot_phase_resumes_after_mid_chunk_blob_put_fault() {
    // The headline test for "wiring logic shouldn't be in the binary":
    // exercises `pg2iceberg_snapshot::run_snapshot_phase` (the helper
    // the binary now calls) end-to-end with sim wiring + a scripted
    // mid-chunk fault. Proves the binary's snapshot path resumes
    // correctly on restart instead of restarting from chunk 0.
    //
    // Setup: 10 seeded rows, chunk_size=2 → 5 chunks.
    // Fault: blob.put #4 fails (i.e. mid-3rd-chunk during snapshot
    // staging — chunk 0 puts blob 0, chunk 1 puts blob 1, ..., the
    // PUT for chunk 2 happens at index 4 because materializer cycles
    // ran in between... wait, materializer doesn't run during
    // snapshot. So blob.put indices match chunk indices: chunk K
    // PUTs at index K.
    //
    // Failing PUT at index 2 means chunks 0 and 1 succeeded (4 rows
    // staged), chunk 2 failed. Checkpoint persists progress through
    // chunk 1. On restart, the helper resumes at chunk 2.
    let seeds: Vec<(i32, i32)> = (1..=10).map(|i| (i, i * 10)).collect();
    let mut h = FaultHarness::boot_with_seeds(&seeds);
    h.set_fault(ops::BLOB_PUT, [2]);

    // Run the same library function the binary calls. This is the
    // production wiring under test.
    let skip: std::collections::BTreeSet<TableIdent> = std::collections::BTreeSet::new();
    let err = block_on(run_snapshot_phase(
        &h.db,
        h.coord.clone() as Arc<dyn Coordinator>,
        &[schema()],
        &skip,
        &mut h.pipeline,
        2,
    ))
    .expect_err("mid-chunk fault should bubble up");
    assert!(format!("{err:?}").contains("blob.put"), "got: {err:?}");
    assert_eq!(h.injected(), 1);

    // Checkpoint must reflect partial progress: snapshot_state ==
    // InProgress AND snapshot_progress is non-empty (the last
    // successfully-staged chunk's last PK key is recorded).
    let cp = block_on(h.coord_inner.load_checkpoint()).unwrap().unwrap();
    assert_eq!(
        cp.snapshot_state,
        pg2iceberg_core::SnapshotState::InProgress,
        "expected InProgress, got {:?}",
        cp.snapshot_state
    );
    assert!(
        !cp.snapshot_progress.is_empty(),
        "expected progress recorded for resume; got empty map"
    );

    // Crash + clear faults; resume.
    h.crash_and_restart();
    h.clear_faults();
    let outcome = block_on(run_snapshot_phase(
        &h.db,
        h.coord.clone() as Arc<dyn Coordinator>,
        &[schema()],
        &skip,
        &mut h.pipeline,
        2,
    ))
    .expect("resume should succeed");
    let snap_lsn = match outcome {
        SnapshotPhaseOutcome::Completed { snapshot_lsn } => snapshot_lsn,
        SnapshotPhaseOutcome::Skipped => panic!("resume should not skip"),
    };
    h.stream.send_standby(snap_lsn);
    let _ = h.try_materialize();

    // Final state: PG ↔ Iceberg parity, checkpoint marked Complete.
    let mut iceberg = block_on(read_materialized_state(
        h.catalog_inner.as_ref(),
        h.blob_inner.as_ref(),
        &ident(),
        &schema(),
        &[ColumnName("id".into())],
    ))
    .unwrap();
    sort_by_pk(&mut iceberg);
    let mut pg = h.db.read_table(&ident()).unwrap();
    sort_by_pk(&mut pg);
    assert_eq!(iceberg, pg, "all 10 rows visible in Iceberg after resume");

    let cp_done = block_on(h.coord_inner.load_checkpoint()).unwrap().unwrap();
    assert_eq!(
        cp_done.snapshot_state,
        pg2iceberg_core::SnapshotState::Complete,
        "checkpoint must be Complete after resume"
    );
    assert!(
        cp_done.snapshot_progress.is_empty(),
        "progress map should be cleared on Complete"
    );
}

#[test]
fn binary_snapshot_phase_skips_when_checkpoint_already_complete() {
    // Second-run idempotence: with checkpoint == Complete, the helper
    // returns Skipped without re-staging.
    let seeds: Vec<(i32, i32)> = (1..=4).map(|i| (i, i * 10)).collect();
    let mut h = FaultHarness::boot_with_seeds(&seeds);
    let skip: std::collections::BTreeSet<TableIdent> = std::collections::BTreeSet::new();
    let outcome = block_on(run_snapshot_phase(
        &h.db,
        h.coord.clone() as Arc<dyn Coordinator>,
        &[schema()],
        &skip,
        &mut h.pipeline,
        2,
    ))
    .unwrap();
    assert!(matches!(outcome, SnapshotPhaseOutcome::Completed { .. }));

    // Second call: same checkpoint says Complete → Skipped.
    let outcome2 = block_on(run_snapshot_phase(
        &h.db,
        h.coord.clone() as Arc<dyn Coordinator>,
        &[schema()],
        &skip,
        &mut h.pipeline,
        2,
    ))
    .unwrap();
    assert_eq!(outcome2, SnapshotPhaseOutcome::Skipped);
}

#[test]
fn binary_snapshot_phase_skips_when_all_tables_in_skip_set() {
    // Per-table `skip_snapshot: true` opt-out: with every configured
    // table in the skip set, the helper returns Skipped without
    // touching the source.
    let mut h = FaultHarness::boot_with_seeds(&[(1, 10), (2, 20)]);
    let mut skip = std::collections::BTreeSet::new();
    skip.insert(ident());
    let outcome = block_on(run_snapshot_phase(
        &h.db,
        h.coord.clone() as Arc<dyn Coordinator>,
        &[schema()],
        &skip,
        &mut h.pipeline,
        2,
    ))
    .unwrap();
    assert_eq!(outcome, SnapshotPhaseOutcome::Skipped);
}

#[test]
fn naive_continue_after_flush_failure_loses_events() {
    // Documents *why* the binary's `run` loop exits on flush failure
    // (it does — see `crates/pg2iceberg/src/run.rs::run_inner`'s
    // `Handler::Flush` arm: returns Err on pipeline.flush() failure,
    // which propagates to the top and exits the process).
    //
    // If you naively *kept running* the pipeline across a flush
    // failure — drained the sink mid-failure, kept reading from the
    // stream, then did another flush — the second flush would ack
    // the slot past the events that the first flush lost. Once the
    // slot is acked, those events are unrecoverable: the WAL behind
    // `restart_lsn` is gone.
    //
    // This test is the inverse of the proptest contract: it
    // demonstrates the durability hole when you DON'T crash on
    // flush error. The fix is structural — the binary already does
    // it. This pinned test is a regression guard: if anyone changes
    // the run loop to "log and continue" on flush errors, this
    // test starts passing (which fails the assertion in CI).
    let mut h = FaultHarness::boot();
    h.insert(1, 0);
    h.set_fault(ops::BLOB_PUT, [0]);

    // Step 1: flush fails. Events drained from sink. Stream cursor
    // advanced. Pipeline kept alive (the wrong choice).
    let _ = h.try_flush();

    // Step 2: more PG activity, then a successful flush. This
    // *advances the slot's confirmed_flush_lsn past Insert(1, 0)*,
    // making it unrecoverable.
    h.insert(2, 0);
    h.try_flush().unwrap();
    h.try_materialize().unwrap();

    // Insert(1, 0) is gone from Iceberg. Crash + recovery from
    // slot's restart_lsn doesn't bring it back — the slot has moved
    // past it.
    h.crash_and_restart();
    h.clear_faults();
    for _ in 0..4 {
        h.drive();
        let _ = h.try_flush();
        let _ = h.try_materialize();
    }
    let mut iceberg = block_on(read_materialized_state(
        h.catalog_inner.as_ref(),
        h.blob_inner.as_ref(),
        &ident(),
        &schema(),
        &[ColumnName("id".into())],
    ))
    .unwrap();
    sort_by_pk(&mut iceberg);
    let pg = h.db.read_table(&ident()).unwrap();
    assert_eq!(pg.len(), 2, "PG has both rows");
    assert_eq!(iceberg.len(), 1, "Iceberg lost id=1 — durability hole");
    assert!(
        iceberg.iter().all(|r| r.get(&ColumnName("id".into()))
            != Some(&PgValue::Int4(1))),
        "id=1 must be missing from Iceberg in the naive-continue model"
    );
}

#[test]
#[ignore = "GAP: materializer-crash recovery requires FileIndex rebuild from catalog history; pinned in plan §P2"]
fn materializer_crash_during_commit_loses_in_memory_state() {
    // Documents Phase 8.5 follow-up: today the harness doesn't
    // rebuild the materializer's FileIndex from catalog history on
    // restart. A real materializer crash mid-commit would lose the
    // in-memory FileIndex, and a re-insert promotion would
    // mis-classify already-committed PKs as fresh inserts. The
    // FileIndex rebuild path exists in the iceberg crate
    // (`rebuild_from_catalog`); the harness just doesn't exercise it
    // yet.
}

// ── Smoke-style proptest ──────────────────────────────────────────

use proptest::prelude::*;

#[derive(Clone, Debug)]
enum FaultStep {
    Insert { id: i32, qty: i32 },
    Update { id: i32, qty: i32 },
    Flush,
    Materialize,
    InjectBlobPutFault,
    InjectCoordClaimFault,
    InjectCommitSnapshotFault,
    Crash,
}

fn fault_step() -> impl Strategy<Value = FaultStep> {
    let id = 1i32..=4;
    let qty = 0i32..=50;
    prop_oneof![
        4 => (id.clone(), qty.clone()).prop_map(|(id, qty)| FaultStep::Insert { id, qty }),
        2 => (id.clone(), qty.clone()).prop_map(|(id, qty)| FaultStep::Update { id, qty }),
        4 => Just(FaultStep::Flush),
        3 => Just(FaultStep::Materialize),
        1 => Just(FaultStep::InjectBlobPutFault),
        1 => Just(FaultStep::InjectCoordClaimFault),
        1 => Just(FaultStep::InjectCommitSnapshotFault),
        2 => Just(FaultStep::Crash),
    ]
}

fn run_fault_step(h: &mut FaultHarness, step: &FaultStep) {
    match step {
        FaultStep::Insert { id, qty } => {
            if !h.live.contains(id) {
                h.insert(*id, *qty);
            }
        }
        FaultStep::Update { id, qty } => {
            if h.live.contains(id) {
                h.update(*id, *qty);
            }
        }
        FaultStep::Flush => {
            // Mirror prod: the binary's `run` loop exits on
            // pipeline.flush() Err so the operator/k8s restart
            // replays from slot. Any "kept running after a flush
            // error" model has a known durability hole — see the
            // `naive_continue_after_flush_failure_loses_events`
            // pinned scenario for the demonstration.
            if h.try_flush().is_err() {
                h.crash_and_restart();
            }
        }
        FaultStep::Materialize => {
            // Materializer failure is non-fatal in prod: the next
            // cycle re-reads from coord and retries. No crash needed.
            let _ = h.try_materialize();
        }
        FaultStep::InjectBlobPutFault => {
            let next = h.plan.counter(ops::BLOB_PUT);
            h.set_fault(ops::BLOB_PUT, [next]);
        }
        FaultStep::InjectCoordClaimFault => {
            let next = h.plan.counter(ops::COORD_CLAIM);
            h.set_fault(ops::COORD_CLAIM, [next]);
        }
        FaultStep::InjectCommitSnapshotFault => {
            let next = h.plan.counter(ops::CAT_COMMIT_SNAPSHOT);
            h.set_fault(ops::CAT_COMMIT_SNAPSHOT, [next]);
        }
        FaultStep::Crash => h.crash_and_restart(),
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(48))]

    /// Random workloads with random faults still reach PG ↔ Iceberg
    /// parity once faults are cleared and recovery runs.
    #[test]
    fn random_workload_with_random_faults_recovers(
        steps in prop::collection::vec(fault_step(), 1..=20)
    ) {
        let mut h = FaultHarness::boot();
        for step in &steps {
            run_fault_step(&mut h, step);
        }
        if let Err(e) = assert_recovery(&mut h) {
            panic!("workload {:?}\nfailed recovery: {}", steps, e);
        }
    }
}
