//! End-to-end materializer test: full pipeline (PG → coord → blob → catalog
//! → verifier).
//!
//!   SimPostgres → SimReplicationStream → Pipeline → Sink → MemoryBlobStore
//!     → MemoryCoordinator → Materializer.cycle → MemoryCatalog
//!     → read_materialized_state → assert == PG ground truth

use pg2iceberg_coord::schema::CoordSchema;
use pg2iceberg_coord::Coordinator;
use pg2iceberg_core::typemap::IcebergType;
use pg2iceberg_core::{
    ColumnName, ColumnSchema, Lsn, Namespace, PgValue, Row, TableIdent, TableSchema, Timestamp,
};
use pg2iceberg_iceberg::{read_materialized_state, Catalog};
use pg2iceberg_logical::pipeline::CounterBlobNamer;
use pg2iceberg_logical::{CounterMaterializerNamer, Materializer, Pipeline};
use pg2iceberg_sim::blob::MemoryBlobStore;
use pg2iceberg_sim::catalog::MemoryCatalog;
use pg2iceberg_sim::clock::TestClock;
use pg2iceberg_sim::coord::MemoryCoordinator;
use pg2iceberg_sim::postgres::{SimPostgres, SimReplicationStream};
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

fn boot() -> Harness {
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

fn run_materializer(h: &mut Harness) -> usize {
    block_on(h.materializer.cycle()).unwrap()
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
    rows.sort_by_key(|r| match r.get(&col("id")) {
        Some(PgValue::Int4(n)) => *n,
        _ => i32::MAX,
    });
    rows
}

fn read_pg(h: &Harness) -> Vec<Row> {
    let mut rows = h.db.read_table(&ident()).unwrap();
    rows.sort_by_key(|r| match r.get(&col("id")) {
        Some(PgValue::Int4(n)) => *n,
        _ => i32::MAX,
    });
    rows
}

// ---------- tests ----------

#[test]
fn single_insert_materializes_to_iceberg_matching_pg() {
    let mut h = boot();
    let mut tx = h.db.begin_tx();
    tx.insert(&ident(), row(1, 10));
    tx.commit(Timestamp(0)).unwrap();

    drive_pipeline(&mut h);
    let n = run_materializer(&mut h);
    assert_eq!(n, 1);

    assert_eq!(read_iceberg(&h), read_pg(&h));
}

#[test]
fn update_replaces_prior_row_in_iceberg() {
    let mut h = boot();
    let mut tx = h.db.begin_tx();
    tx.insert(&ident(), row(1, 10));
    tx.commit(Timestamp(0)).unwrap();

    drive_pipeline(&mut h);
    run_materializer(&mut h);

    let mut tx = h.db.begin_tx();
    tx.update(&ident(), row(1, 99));
    tx.commit(Timestamp(0)).unwrap();

    drive_pipeline(&mut h);
    run_materializer(&mut h);

    assert_eq!(read_iceberg(&h), vec![row(1, 99)]);
    assert_eq!(read_pg(&h), vec![row(1, 99)]);
}

#[test]
fn delete_removes_row_from_iceberg() {
    let mut h = boot();
    let mut tx = h.db.begin_tx();
    tx.insert(&ident(), row(1, 10));
    tx.insert(&ident(), row(2, 20));
    tx.commit(Timestamp(0)).unwrap();

    drive_pipeline(&mut h);
    run_materializer(&mut h);

    let mut tx = h.db.begin_tx();
    tx.delete(&ident(), pk_only(1));
    tx.commit(Timestamp(0)).unwrap();

    drive_pipeline(&mut h);
    run_materializer(&mut h);

    assert_eq!(read_iceberg(&h), vec![row(2, 20)]);
    assert_eq!(read_pg(&h), vec![row(2, 20)]);
}

#[test]
fn re_insert_after_delete_correct_via_promotion() {
    let mut h = boot();
    let mut tx = h.db.begin_tx();
    tx.insert(&ident(), row(1, 10));
    tx.commit(Timestamp(0)).unwrap();
    drive_pipeline(&mut h);
    run_materializer(&mut h);

    // Delete then re-insert the same PK in the *same* materializer cycle.
    // The fold collapses to a single Insert. promote_re_inserts notices the
    // PK is in FileIndex (from the first cycle's data file) and promotes to
    // Update — TableWriter then emits an equality delete on PK 1, voiding
    // the prior row.
    let mut tx = h.db.begin_tx();
    tx.delete(&ident(), pk_only(1));
    tx.commit(Timestamp(0)).unwrap();
    let mut tx = h.db.begin_tx();
    tx.insert(&ident(), row(1, 200));
    tx.commit(Timestamp(0)).unwrap();

    drive_pipeline(&mut h);
    run_materializer(&mut h);

    assert_eq!(read_iceberg(&h), vec![row(1, 200)]);
    assert_eq!(read_pg(&h), vec![row(1, 200)]);
}

#[test]
fn idempotent_when_run_twice_with_no_new_events() {
    let mut h = boot();
    let mut tx = h.db.begin_tx();
    tx.insert(&ident(), row(1, 10));
    tx.commit(Timestamp(0)).unwrap();

    drive_pipeline(&mut h);
    let first = run_materializer(&mut h);
    assert_eq!(first, 1);

    let second = run_materializer(&mut h);
    assert_eq!(second, 0, "second cycle should be a no-op");

    let snaps_after = block_on(h.catalog.snapshots(&ident())).unwrap();
    assert_eq!(
        snaps_after.len(),
        1,
        "no extra snapshot from the no-op cycle"
    );
}

#[test]
fn flushed_lsn_and_cursor_advance_independently_but_correctly() {
    let mut h = boot();
    let mut tx = h.db.begin_tx();
    tx.insert(&ident(), row(1, 10));
    let commit = tx.commit(Timestamp(0)).unwrap();

    drive_pipeline(&mut h);
    assert_eq!(h.pipeline.flushed_lsn(), commit);

    // Cursor is still -1 — the materializer hasn't run yet.
    let cur_before = block_on(h.coord.get_cursor("default", &ident())).unwrap();
    assert_eq!(cur_before, Some(-1));

    run_materializer(&mut h);

    // Cursor now points past the only log entry.
    let cur_after = block_on(h.coord.get_cursor("default", &ident())).unwrap();
    assert_eq!(cur_after, Some(1));
}

#[test]
fn many_inserts_materialize_in_one_cycle() {
    let mut h = boot();
    for i in 1..=10 {
        let mut tx = h.db.begin_tx();
        tx.insert(&ident(), row(i, i * 10));
        tx.commit(Timestamp(0)).unwrap();
    }
    drive_pipeline(&mut h);
    let n = run_materializer(&mut h);
    assert_eq!(n, 10);

    let iceberg = read_iceberg(&h);
    assert_eq!(iceberg.len(), 10);
    assert_eq!(iceberg, read_pg(&h));
}

#[test]
fn sequential_pipeline_then_materialize_cycles_keep_state_in_sync() {
    let mut h = boot();
    for i in 1..=4 {
        let mut tx = h.db.begin_tx();
        tx.insert(&ident(), row(i, i * 10));
        tx.commit(Timestamp(0)).unwrap();
        drive_pipeline(&mut h);
        run_materializer(&mut h);
        assert_eq!(read_iceberg(&h), read_pg(&h), "after cycle {i}");
    }

    // Mutate.
    let mut tx = h.db.begin_tx();
    tx.update(&ident(), row(2, 999));
    tx.delete(&ident(), pk_only(3));
    tx.commit(Timestamp(0)).unwrap();
    drive_pipeline(&mut h);
    run_materializer(&mut h);

    assert_eq!(read_iceberg(&h), read_pg(&h));
}

#[test]
fn pipeline_holds_data_until_materializer_runs() {
    // The materializer cursor is only advanced AFTER the catalog commits.
    // This test confirms the temporal ordering: pipeline can advance the
    // *coord log* (and thus flushed_lsn) without the materializer having
    // touched anything.
    let mut h = boot();
    let mut tx = h.db.begin_tx();
    tx.insert(&ident(), row(1, 10));
    tx.commit(Timestamp(0)).unwrap();
    drive_pipeline(&mut h);

    // Coord log has the entry; catalog has nothing.
    let log = block_on(h.coord.read_log(&ident(), 0, 100)).unwrap();
    assert_eq!(log.len(), 1);
    let snaps = block_on(h.catalog.snapshots(&ident())).unwrap();
    assert!(snaps.is_empty());

    run_materializer(&mut h);
    let snaps = block_on(h.catalog.snapshots(&ident())).unwrap();
    assert_eq!(snaps.len(), 1);
}
