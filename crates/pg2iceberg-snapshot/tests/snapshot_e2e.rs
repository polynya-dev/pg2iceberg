//! End-to-end snapshot test: pre-populate `SimPostgres`, bootstrap the
//! pipeline + materializer through `run_snapshot`, then chain into normal
//! live replication and verify Iceberg state == PG ground truth at every
//! point.

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
use pg2iceberg_sim::postgres::{SimPostgres, SimReplicationStream};
use pg2iceberg_snapshot::{run_snapshot, run_snapshot_chunked, Snapshotter};
use pollster::block_on;
use std::collections::BTreeMap;
use std::sync::Arc;

const TABLE: &str = "orders";
const PUB: &str = "pub1";
const SLOT: &str = "slot1";

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

fn col(n: &str) -> ColumnName {
    ColumnName(n.into())
}

fn row(id: i32, qty: i32) -> Row {
    let mut r = BTreeMap::new();
    r.insert(col("id"), PgValue::Int4(id));
    r.insert(col("qty"), PgValue::Int4(qty));
    r
}

fn pk_only(id: i32) -> Row {
    let mut r = BTreeMap::new();
    r.insert(col("id"), PgValue::Int4(id));
    r
}

struct Harness {
    db: SimPostgres,
    coord: Arc<MemoryCoordinator>,
    blob_store: Arc<MemoryBlobStore>,
    catalog: Arc<MemoryCatalog>,
    pipeline: Pipeline<MemoryCoordinator>,
    materializer: Materializer<MemoryCatalog>,
    stream: SimReplicationStream,
}

/// Boot order:
/// 1. Pre-populate PG with `seed_rows` BEFORE creating publication/slot.
///    These are the rows the snapshot phase must capture.
/// 2. Create publication + slot. Slot's `restart_lsn` is now set to the
///    LSN at this moment.
/// 3. Wire the pipeline + materializer + replication stream.
fn boot(seed_rows: &[(i32, i32)]) -> Harness {
    let db = SimPostgres::new();
    db.create_table(schema()).unwrap();

    if !seed_rows.is_empty() {
        let mut tx = db.begin_tx();
        for (id, qty) in seed_rows {
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
    let stage_namer = Arc::new(CounterBlobNamer::new("s3://stage"));
    let pipeline = Pipeline::new(coord.clone(), blob_store.clone(), stage_namer, 64);

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

    Harness {
        db,
        coord,
        blob_store,
        catalog,
        pipeline,
        materializer,
        stream,
    }
}

fn drive_pipeline(h: &mut Harness) {
    while let Some(msg) = h.stream.recv() {
        block_on(h.pipeline.process(msg)).unwrap();
    }
    block_on(h.pipeline.flush()).unwrap();
    h.stream.send_standby(h.pipeline.flushed_lsn());
}

fn read_iceberg(h: &Harness) -> Vec<Row> {
    let mut rows = block_on(read_materialized_state(
        h.catalog.as_ref(),
        h.blob_store.as_ref(),
        &ident(),
        &schema(),
        &[col("id")],
    ))
    .unwrap();
    rows.sort_by_key(pk_int);
    rows
}

fn read_pg(h: &Harness) -> Vec<Row> {
    let mut rows = h.db.read_table(&ident()).unwrap();
    rows.sort_by_key(pk_int);
    rows
}

fn pk_int(r: &Row) -> i32 {
    match r.get(&col("id")) {
        Some(PgValue::Int4(n)) => *n,
        _ => i32::MAX,
    }
}

// ---------- tests ----------

#[test]
fn empty_table_snapshot_produces_no_iceberg_data() {
    let mut h = boot(&[]);
    let snap_lsn = block_on(run_snapshot(&h.db, h.coord.clone() as Arc<dyn Coordinator>, &[schema()], &mut h.pipeline)).unwrap();
    h.stream.send_standby(snap_lsn);
    block_on(h.materializer.cycle()).unwrap();
    assert!(read_iceberg(&h).is_empty());
    assert!(read_pg(&h).is_empty());
}

#[test]
fn snapshot_bootstraps_existing_pg_data() {
    let mut h = boot(&[(1, 10), (2, 20), (3, 30)]);

    let snap_lsn = block_on(run_snapshot(&h.db, h.coord.clone() as Arc<dyn Coordinator>, &[schema()], &mut h.pipeline)).unwrap();
    assert_eq!(h.pipeline.flushed_lsn(), snap_lsn);
    h.stream.send_standby(snap_lsn);

    block_on(h.materializer.cycle()).unwrap();

    let iceberg = read_iceberg(&h);
    assert_eq!(iceberg.len(), 3);
    assert_eq!(iceberg, read_pg(&h));
}

#[test]
fn snapshot_then_live_replication_picks_up_seamlessly() {
    let mut h = boot(&[(1, 10), (2, 20)]);

    // Phase 1: snapshot.
    let snap_lsn = block_on(run_snapshot(&h.db, h.coord.clone() as Arc<dyn Coordinator>, &[schema()], &mut h.pipeline)).unwrap();
    h.stream.send_standby(snap_lsn);
    block_on(h.materializer.cycle()).unwrap();
    assert_eq!(read_iceberg(&h), read_pg(&h));

    // Phase 2: live replication picks up new writes.
    let mut tx = h.db.begin_tx();
    tx.insert(&ident(), row(3, 30));
    tx.update(&ident(), row(1, 99));
    tx.delete(&ident(), pk_only(2));
    tx.commit(Timestamp(0)).unwrap();

    drive_pipeline(&mut h);
    block_on(h.materializer.cycle()).unwrap();

    let iceberg = read_iceberg(&h);
    assert_eq!(iceberg, read_pg(&h));
    // Sanity: post-snapshot state is {(1,99), (3,30)}.
    assert_eq!(iceberg, vec![row(1, 99), row(3, 30)]);
}

#[test]
fn snapshot_then_more_writes_in_pg_before_replication_starts_no_data_lost() {
    // The handoff: pre-snapshot rows are captured by snapshot; mid-snapshot
    // writes (after snapshot_lsn but before send_standby) flow through live
    // replication after the slot advances. Since SimPostgres serializes
    // operations and our test injects writes between phases, the ordering
    // is: seed → snapshot → write → drive → materialize.
    let mut h = boot(&[(1, 10)]);

    let snap_lsn = block_on(run_snapshot(&h.db, h.coord.clone() as Arc<dyn Coordinator>, &[schema()], &mut h.pipeline)).unwrap();

    // After snapshot reads but before slot ack: a new insert. Its LSN is
    // strictly greater than snap_lsn, so the slot (still at restart_lsn) will
    // serve it once we drive.
    let mut tx = h.db.begin_tx();
    tx.insert(&ident(), row(2, 20));
    tx.commit(Timestamp(0)).unwrap();

    h.stream.send_standby(snap_lsn);

    drive_pipeline(&mut h);
    block_on(h.materializer.cycle()).unwrap();

    let iceberg = read_iceberg(&h);
    assert_eq!(iceberg, read_pg(&h));
    assert_eq!(iceberg, vec![row(1, 10), row(2, 20)]);
}

// ---------- chunked snapshot (Phase 11 finish) ----------

#[test]
fn chunk_size_one_materializes_every_row_exactly_once() {
    let seeds: Vec<(i32, i32)> = (1..=25).map(|i| (i, i * 10)).collect();
    let mut h = boot(&seeds);

    let snap_lsn = block_on(run_snapshot_chunked(&h.db, h.coord.clone() as Arc<dyn Coordinator>, &[schema()], &mut h.pipeline, 1)).unwrap();
    h.stream.send_standby(snap_lsn);
    block_on(h.materializer.cycle()).unwrap();

    let iceberg = read_iceberg(&h);
    assert_eq!(iceberg.len(), 25, "every row materialized exactly once");
    assert_eq!(iceberg, read_pg(&h));
}

#[test]
fn chunk_size_does_not_change_final_state() {
    // Same seed, different chunk sizes — final Iceberg state must match.
    fn run_with_chunk_size(chunk_size: usize, n_rows: i32) -> Vec<Row> {
        let seeds: Vec<(i32, i32)> = (1..=n_rows).map(|i| (i, i * 10)).collect();
        let mut h = boot(&seeds);
        let snap_lsn = block_on(run_snapshot_chunked(
            &h.db,
            h.coord.clone() as Arc<dyn Coordinator>,
            &[schema()],
            &mut h.pipeline,
            chunk_size,
        ))
        .unwrap();
        h.stream.send_standby(snap_lsn);
        block_on(h.materializer.cycle()).unwrap();
        read_iceberg(&h)
    }
    let baseline = run_with_chunk_size(1024, 12);
    for cs in [1, 2, 5, 10, 100] {
        let got = run_with_chunk_size(cs, 12);
        assert_eq!(got, baseline, "chunk_size={cs} differs from baseline");
    }
}

#[test]
fn chunked_snapshot_produces_one_log_index_entry_per_chunk() {
    // Indirect proof of memory bounding: each chunk is its own coord
    // commit. If `run_snapshot_chunked` ignored chunk_size and read
    // everything at once, we'd see exactly one log_index entry; with
    // chunk_size=3 over 10 rows we expect ceil(10/3) = 4 entries.
    use pg2iceberg_coord::Coordinator;

    let seeds: Vec<(i32, i32)> = (1..=10).map(|i| (i, i * 10)).collect();
    let mut h = boot(&seeds);

    block_on(run_snapshot_chunked(&h.db, h.coord.clone() as Arc<dyn Coordinator>, &[schema()], &mut h.pipeline, 3)).unwrap();

    let entries = block_on(<MemoryCoordinator as Coordinator>::read_log(
        h.coord.as_ref(),
        &ident(),
        0,
        100,
    ))
    .unwrap();
    assert_eq!(entries.len(), 4, "ceil(10/3) = 4 chunks");
}

// ---------- resumable snapshot (Phase 11 finish) ----------

#[test]
fn snapshotter_run_to_completion_matches_run_snapshot() {
    let seeds: Vec<(i32, i32)> = (1..=20).map(|i| (i, i * 10)).collect();
    let mut h = boot(&seeds);

    let s = Snapshotter::new(h.coord.clone() as Arc<dyn Coordinator>).with_chunk_size(7);
    let snap_lsn = block_on(s.run(&h.db, &[schema()], &mut h.pipeline)).unwrap();
    h.stream.send_standby(snap_lsn);
    block_on(h.materializer.cycle()).unwrap();

    assert_eq!(read_iceberg(&h).len(), 20);
    assert_eq!(read_iceberg(&h), read_pg(&h));
}

#[test]
fn snapshotter_clears_progress_when_table_completes() {
    let seeds: Vec<(i32, i32)> = (1..=5).map(|i| (i, i * 10)).collect();
    let mut h = boot(&seeds);

    let s = Snapshotter::new(h.coord.clone() as Arc<dyn Coordinator>);
    block_on(s.run(&h.db, &[schema()], &mut h.pipeline)).unwrap();

    let progress = block_on(h.coord.snapshot_progress(&ident())).unwrap();
    assert!(
        progress.is_none(),
        "completed snapshot must clear its progress entry; got {progress:?}"
    );
}

#[test]
fn snapshotter_persists_progress_after_partial_run() {
    let seeds: Vec<(i32, i32)> = (1..=20).map(|i| (i, i * 10)).collect();
    let mut h = boot(&seeds);

    let s = Snapshotter::new(h.coord.clone() as Arc<dyn Coordinator>).with_chunk_size(5);
    // Process only 2 chunks (10 rows) then return.
    block_on(s.run_chunks(&h.db, &[schema()], &mut h.pipeline, Some(2))).unwrap();

    let progress = block_on(h.coord.snapshot_progress(&ident())).unwrap();
    assert!(
        progress.is_some(),
        "partial run must record progress; got None"
    );
}

#[test]
fn crash_mid_snapshot_then_resume_completes_correctly() {
    // The headline resumability test:
    // 1. Start snapshot, run 2 chunks (10 of 30 rows).
    // 2. "Crash" — drop the pipeline + materializer + Snapshotter handle.
    //    Coord, blob_store, catalog all survive (durable).
    // 3. Boot a fresh Harness pointed at the same coord. Snapshotter loads
    //    the saved progress and resumes.
    // 4. Verify all 30 rows materialize exactly once, no duplicates.
    let seeds: Vec<(i32, i32)> = (1..=30).map(|i| (i, i * 10)).collect();

    // Build harness with shared coord/blob/catalog/namer so reboot is realistic.
    let db = SimPostgres::new();
    db.create_table(schema()).unwrap();
    let mut tx = db.begin_tx();
    for (id, qty) in &seeds {
        tx.insert(&ident(), row(*id, *qty));
    }
    tx.commit(Timestamp(0)).unwrap();
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
    let stage_namer = Arc::new(CounterBlobNamer::new("s3://stage"));
    let mat_namer = Arc::new(CounterMaterializerNamer::new("s3://table"));

    fn build(
        db: &SimPostgres,
        coord: &Arc<MemoryCoordinator>,
        catalog: &Arc<MemoryCatalog>,
        blob_store: &Arc<MemoryBlobStore>,
        stage_namer: &Arc<CounterBlobNamer>,
        mat_namer: &Arc<CounterMaterializerNamer>,
    ) -> (
        Pipeline<MemoryCoordinator>,
        Materializer<MemoryCatalog>,
        SimReplicationStream,
    ) {
        let pipeline = Pipeline::new(coord.clone(), blob_store.clone(), stage_namer.clone(), 64);
        let mut materializer = Materializer::new(
            coord.clone() as Arc<dyn Coordinator>,
            blob_store.clone(),
            catalog.clone(),
            mat_namer.clone(),
            "default",
            128,
        );
        block_on(materializer.register_table(schema())).unwrap();
        let stream = db.start_replication(SLOT).unwrap();
        (pipeline, materializer, stream)
    }

    // Phase 1: partial snapshot.
    let snap_lsn = {
        let (mut pipeline, _materializer, _stream) =
            build(&db, &coord, &catalog, &blob_store, &stage_namer, &mat_namer);
        let s = Snapshotter::new(coord.clone() as Arc<dyn Coordinator>).with_chunk_size(5);
        block_on(s.run_chunks(&db, &[schema()], &mut pipeline, Some(2))).unwrap()
        // pipeline + Snapshotter dropped here — simulates a crash.
    };
    let _ = snap_lsn;

    let progress = block_on(coord.snapshot_progress(&ident())).unwrap();
    assert!(
        progress.is_some(),
        "after partial run, progress must be persisted"
    );

    // Phase 2: a fresh process resumes.
    let (mut pipeline, mut materializer, mut stream) =
        build(&db, &coord, &catalog, &blob_store, &stage_namer, &mat_namer);
    let s = Snapshotter::new(coord.clone() as Arc<dyn Coordinator>).with_chunk_size(5);
    let snap_lsn = block_on(s.run(&db, &[schema()], &mut pipeline)).unwrap();
    stream.send_standby(snap_lsn);
    block_on(materializer.cycle()).unwrap();

    // Phase 3: verify.
    let mut iceberg = block_on(read_materialized_state(
        catalog.as_ref(),
        blob_store.as_ref(),
        &ident(),
        &schema(),
        &[col("id")],
    ))
    .unwrap();
    iceberg.sort_by_key(pk_int);

    assert_eq!(iceberg.len(), 30, "all 30 rows materialized exactly once");
    let mut pg = db.read_table(&ident()).unwrap();
    pg.sort_by_key(pk_int);
    assert_eq!(iceberg, pg);

    // Per-table progress row should be cleared (snapshot complete).
    let progress = block_on(coord.snapshot_progress(&ident())).unwrap();
    assert!(
        progress.is_none(),
        "completed resume must clear progress; got {progress:?}"
    );
}

#[test]
fn chunked_snapshot_with_zero_chunk_size_panics() {
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let mut h = boot(&[(1, 10)]);
        block_on(run_snapshot_chunked(&h.db, h.coord.clone() as Arc<dyn Coordinator>, &[schema()], &mut h.pipeline, 0)).unwrap();
    }));
    assert!(result.is_err(), "chunk_size=0 must panic");
}

#[test]
fn snapshot_xid_does_not_collide_with_real_pg_xids() {
    // Run snapshot, then commit many real txs. The synthetic snapshot xid
    // (SNAPSHOT_XID_BASE) must never overlap with real ones — sim xids start
    // at 1 and increment. This test isn't a tight bound (we can't realistically
    // exhaust 0xFFFF_FF00 xids) but proves the design isn't visibly broken.
    let mut h = boot(&[(1, 10)]);
    let snap_lsn = block_on(run_snapshot(&h.db, h.coord.clone() as Arc<dyn Coordinator>, &[schema()], &mut h.pipeline)).unwrap();
    h.stream.send_standby(snap_lsn);
    block_on(h.materializer.cycle()).unwrap();

    for i in 2..=20 {
        let mut tx = h.db.begin_tx();
        tx.insert(&ident(), row(i, i * 10));
        // Each tx gets a fresh sim xid; we don't assert on the value but
        // confirm no panics or weird interaction with the snapshot's xid.
        tx.commit(Timestamp(0)).unwrap();
    }

    drive_pipeline(&mut h);
    block_on(h.materializer.cycle()).unwrap();
    assert_eq!(read_iceberg(&h), read_pg(&h));
    assert_eq!(read_iceberg(&h).len(), 20);
}
