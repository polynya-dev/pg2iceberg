//! End-to-end query-mode test: poll SimPostgres on a watermark column,
//! upsert into the Buffer, flush to Iceberg via TableWriter + Catalog,
//! and verify Iceberg state matches PG.

use pg2iceberg_coord::schema::CoordSchema;
use pg2iceberg_coord::Coordinator;
use pg2iceberg_core::typemap::IcebergType;
use pg2iceberg_core::{
    ColumnName, ColumnSchema, Namespace, PgValue, Row, TableIdent, TableSchema, Timestamp,
};
use pg2iceberg_iceberg::read_materialized_state;
use pg2iceberg_logical::CounterMaterializerNamer;
use pg2iceberg_query::QueryPipeline;
use pg2iceberg_sim::blob::MemoryBlobStore;
use pg2iceberg_sim::catalog::MemoryCatalog;
use pg2iceberg_sim::clock::TestClock;
use pg2iceberg_sim::coord::MemoryCoordinator;
use pg2iceberg_sim::postgres::SimPostgres;
use pollster::block_on;
use std::collections::BTreeMap;
use std::sync::Arc;

const TABLE: &str = "events";
const WATERMARK: &str = "version";

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
            ColumnSchema {
                name: WATERMARK.into(),
                field_id: 3,
                ty: IcebergType::Long,
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

fn row(id: i32, qty: i32, version: i64) -> Row {
    let mut r = BTreeMap::new();
    r.insert(col("id"), PgValue::Int4(id));
    r.insert(col("qty"), PgValue::Int4(qty));
    r.insert(col(WATERMARK), PgValue::Int8(version));
    r
}

struct Harness {
    db: SimPostgres,
    coord: Arc<MemoryCoordinator>,
    blob_store: Arc<MemoryBlobStore>,
    catalog: Arc<MemoryCatalog>,
    qp: QueryPipeline<MemoryCatalog>,
}

fn boot_with_coord(
    db: SimPostgres,
    coord: Arc<MemoryCoordinator>,
    catalog: Arc<MemoryCatalog>,
    blob_store: Arc<MemoryBlobStore>,
    namer: Arc<CounterMaterializerNamer>,
) -> Harness {
    let mut qp = QueryPipeline::new(
        coord.clone() as Arc<dyn Coordinator>,
        catalog.clone(),
        blob_store.clone(),
        namer,
    );
    block_on(qp.register_table(schema(), WATERMARK)).unwrap();
    Harness {
        db,
        coord,
        blob_store,
        catalog,
        qp,
    }
}

fn boot() -> Harness {
    let db = SimPostgres::new();
    db.create_table(schema()).unwrap();

    let clock = TestClock::at(0);
    let arc_clock: Arc<dyn pg2iceberg_core::Clock> = Arc::new(clock);
    let coord = Arc::new(MemoryCoordinator::new(
        CoordSchema::default_name(),
        arc_clock,
    ));
    let blob_store = Arc::new(MemoryBlobStore::new());
    let catalog = Arc::new(MemoryCatalog::new());
    let namer = Arc::new(CounterMaterializerNamer::new("s3://table"));
    boot_with_coord(db, coord, catalog, blob_store, namer)
}

fn poll_and_flush(h: &mut Harness) {
    block_on(h.qp.poll(&h.db)).unwrap();
    block_on(h.qp.flush()).unwrap();
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

fn insert(db: &SimPostgres, id: i32, qty: i32, version: i64) {
    let mut tx = db.begin_tx();
    tx.insert(&ident(), row(id, qty, version));
    tx.commit(Timestamp(0)).unwrap();
}

fn update(db: &SimPostgres, id: i32, qty: i32, version: i64) {
    let mut tx = db.begin_tx();
    tx.update(&ident(), row(id, qty, version));
    tx.commit(Timestamp(0)).unwrap();
}

// ---------- tests ----------

#[test]
fn empty_table_poll_is_a_noop() {
    let mut h = boot();
    poll_and_flush(&mut h);
    assert!(read_iceberg(&h).is_empty());
    assert!(h.qp.watermark(&ident()).is_none());
}

#[test]
fn single_insert_flows_to_iceberg() {
    let mut h = boot();
    insert(&h.db, 1, 10, 1);
    poll_and_flush(&mut h);

    assert_eq!(read_iceberg(&h), read_pg(&h));
    assert_eq!(h.qp.watermark(&ident()), Some(&PgValue::Int8(1)));
}

#[test]
fn watermark_advances_to_max_value_observed() {
    let mut h = boot();
    insert(&h.db, 1, 10, 5);
    insert(&h.db, 2, 20, 3);
    insert(&h.db, 3, 30, 9);
    poll_and_flush(&mut h);
    assert_eq!(h.qp.watermark(&ident()), Some(&PgValue::Int8(9)));
}

#[test]
fn second_poll_only_returns_rows_with_higher_watermark() {
    let mut h = boot();
    insert(&h.db, 1, 10, 1);
    poll_and_flush(&mut h);
    let snaps_after_first = block_on(<MemoryCatalog as pg2iceberg_iceberg::Catalog>::snapshots(
        h.catalog.as_ref(),
        &ident(),
    ))
    .unwrap()
    .len();
    assert_eq!(snaps_after_first, 1);

    // No new rows: poll is a no-op, no snapshot bumped.
    poll_and_flush(&mut h);
    let snaps_after_noop = block_on(<MemoryCatalog as pg2iceberg_iceberg::Catalog>::snapshots(
        h.catalog.as_ref(),
        &ident(),
    ))
    .unwrap()
    .len();
    assert_eq!(snaps_after_noop, 1, "no-op poll must not commit a snapshot");

    // New row at higher watermark: gets picked up.
    insert(&h.db, 2, 20, 2);
    poll_and_flush(&mut h);
    let snaps_after_third = block_on(<MemoryCatalog as pg2iceberg_iceberg::Catalog>::snapshots(
        h.catalog.as_ref(),
        &ident(),
    ))
    .unwrap()
    .len();
    assert_eq!(snaps_after_third, 2);

    assert_eq!(read_iceberg(&h), read_pg(&h));
}

#[test]
fn update_with_higher_watermark_promotes_to_iceberg_replacement() {
    let mut h = boot();
    insert(&h.db, 1, 10, 1);
    poll_and_flush(&mut h);

    // Update bumps the version; next poll picks up the new row, FileIndex
    // sees the PK already exists, promotes Insert → Update, and the writer
    // emits an equality delete that voids the prior data row.
    update(&h.db, 1, 99, 2);
    poll_and_flush(&mut h);

    assert_eq!(read_iceberg(&h), vec![row(1, 99, 2)]);
    assert_eq!(read_iceberg(&h), read_pg(&h));
}

#[test]
fn buffer_dedups_within_a_single_poll() {
    // Both rows have the same PK but different versions. The poll returns
    // them in version-ASC order; the buffer keeps the latest.
    let mut h = boot();
    insert(&h.db, 1, 10, 1);
    update(&h.db, 1, 99, 2);
    update(&h.db, 1, 50, 3);
    poll_and_flush(&mut h);

    assert_eq!(read_iceberg(&h), vec![row(1, 50, 3)]);
}

#[test]
fn many_rows_round_trip() {
    let mut h = boot();
    for i in 1..=10 {
        insert(&h.db, i, i * 10, i as i64);
    }
    poll_and_flush(&mut h);
    assert_eq!(read_iceberg(&h).len(), 10);
    assert_eq!(read_iceberg(&h), read_pg(&h));
}

#[test]
fn poll_only_no_flush_does_not_appear_in_iceberg() {
    // Confirms the durability boundary: rows are buffered but no catalog
    // commit happens until flush.
    let mut h = boot();
    insert(&h.db, 1, 10, 1);
    block_on(h.qp.poll(&h.db)).unwrap();
    assert!(read_iceberg(&h).is_empty());

    block_on(h.qp.flush()).unwrap();
    assert_eq!(read_iceberg(&h).len(), 1);
}

// ---------- watermark persistence (Phase 10 finish) ----------

#[test]
fn watermark_persists_across_pipeline_restart() {
    // Phase 1: a fresh process polls + flushes; watermark gets saved to coord.
    let db = SimPostgres::new();
    db.create_table(schema()).unwrap();

    let clock = TestClock::at(0);
    let arc_clock: Arc<dyn pg2iceberg_core::Clock> = Arc::new(clock);
    let coord = Arc::new(MemoryCoordinator::new(
        CoordSchema::default_name(),
        arc_clock,
    ));
    let blob_store = Arc::new(MemoryBlobStore::new());
    let catalog = Arc::new(MemoryCatalog::new());
    let namer = Arc::new(CounterMaterializerNamer::new("s3://table"));

    {
        let mut h = boot_with_coord(
            db.clone(),
            coord.clone(),
            catalog.clone(),
            blob_store.clone(),
            namer.clone(),
        );
        insert(&h.db, 1, 10, 5);
        insert(&h.db, 2, 20, 7);
        poll_and_flush(&mut h);
        assert_eq!(h.qp.watermark(&ident()), Some(&PgValue::Int8(7)));
    }
    // Pipeline dropped. Coord, catalog, blob_store survive (durable).

    // Phase 2: a new process boots with the same coord; should restore wm=7.
    let mut h = boot_with_coord(db, coord, catalog, blob_store, namer);
    assert_eq!(
        h.qp.watermark(&ident()),
        Some(&PgValue::Int8(7)),
        "watermark must be restored from checkpoint"
    );

    // Polling now skips the already-materialized rows. Source returns 0
    // because no row has version > 7.
    let polled = block_on(h.qp.poll(&h.db)).unwrap();
    assert_eq!(polled, 0, "no re-poll of rows already past watermark");
}

#[test]
fn restart_then_new_writes_pick_up_correctly() {
    // Boot, materialize, restart, new writes appear in Iceberg without
    // duplicating the prior rows.
    let db = SimPostgres::new();
    db.create_table(schema()).unwrap();
    let clock = TestClock::at(0);
    let arc_clock: Arc<dyn pg2iceberg_core::Clock> = Arc::new(clock);
    let coord = Arc::new(MemoryCoordinator::new(
        CoordSchema::default_name(),
        arc_clock,
    ));
    let blob_store = Arc::new(MemoryBlobStore::new());
    let catalog = Arc::new(MemoryCatalog::new());
    let namer = Arc::new(CounterMaterializerNamer::new("s3://table"));

    {
        let mut h = boot_with_coord(
            db.clone(),
            coord.clone(),
            catalog.clone(),
            blob_store.clone(),
            namer.clone(),
        );
        insert(&h.db, 1, 10, 1);
        poll_and_flush(&mut h);
    }

    // Restart + new write at higher version.
    {
        let mut h = boot_with_coord(
            db.clone(),
            coord.clone(),
            catalog.clone(),
            blob_store.clone(),
            namer.clone(),
        );
        insert(&h.db, 2, 20, 2);
        poll_and_flush(&mut h);
    }

    // Read after second cycle. Must reflect both rows (no dup, no missing).
    let h = boot_with_coord(db, coord, catalog, blob_store, namer);
    let mut iceberg = read_iceberg(&h);
    iceberg.sort_by_key(pk_int);
    assert_eq!(iceberg, vec![row(1, 10, 1), row(2, 20, 2)]);
}

#[test]
fn save_checkpoint_without_flush_still_persists() {
    let mut h = boot();
    insert(&h.db, 1, 10, 5);
    block_on(h.qp.poll(&h.db)).unwrap();
    // Watermark is in memory but not yet committed because flush wasn't called.
    block_on(h.qp.save_checkpoint()).unwrap();

    let cp = block_on(h.coord.load_checkpoint()).unwrap().unwrap();
    assert_eq!(cp.query_watermarks.get(&ident()), Some(&PgValue::Int8(5)));
}

#[test]
fn empty_pipeline_save_yields_empty_watermark_map() {
    let h = boot();
    block_on(h.qp.save_checkpoint()).unwrap();
    let cp = block_on(h.coord.load_checkpoint()).unwrap().unwrap();
    assert!(cp.query_watermarks.is_empty());
    assert_eq!(cp.tracked_tables, vec![ident()]);
}

// ---------- chunked polling (Phase 10 finish) ----------

fn boot_with_poll_chunk_size(n: usize) -> Harness {
    let db = SimPostgres::new();
    db.create_table(schema()).unwrap();
    let clock = TestClock::at(0);
    let arc_clock: Arc<dyn pg2iceberg_core::Clock> = Arc::new(clock);
    let coord = Arc::new(MemoryCoordinator::new(
        CoordSchema::default_name(),
        arc_clock,
    ));
    let blob_store = Arc::new(MemoryBlobStore::new());
    let catalog = Arc::new(MemoryCatalog::new());
    let namer = Arc::new(CounterMaterializerNamer::new("s3://table"));
    let mut qp = QueryPipeline::new(
        coord.clone() as Arc<dyn Coordinator>,
        catalog.clone(),
        blob_store.clone(),
        namer,
    )
    .with_poll_chunk_size(n);
    block_on(qp.register_table(schema(), WATERMARK)).unwrap();
    Harness {
        db,
        coord,
        blob_store,
        catalog,
        qp,
    }
}

#[test]
fn chunk_size_1_polls_every_row() {
    let mut h = boot_with_poll_chunk_size(1);
    for i in 1..=20 {
        insert(&h.db, i, i * 10, i as i64);
    }
    poll_and_flush(&mut h);
    assert_eq!(read_iceberg(&h).len(), 20);
    assert_eq!(read_iceberg(&h), read_pg(&h));
    assert_eq!(h.qp.watermark(&ident()), Some(&PgValue::Int8(20)));
}

#[test]
fn chunk_size_5_advances_watermark_across_iterations() {
    let mut h = boot_with_poll_chunk_size(5);
    for i in 1..=100 {
        insert(&h.db, i, i * 10, i as i64);
    }
    let total = block_on(h.qp.poll(&h.db)).unwrap();
    assert_eq!(total, 100, "all 100 rows fetched across loop iterations");
    block_on(h.qp.flush()).unwrap();
    assert_eq!(read_iceberg(&h).len(), 100);
}

#[test]
fn chunk_size_does_not_change_final_state() {
    fn count_with(chunk: usize) -> usize {
        let mut h = boot_with_poll_chunk_size(chunk);
        for i in 1..=37 {
            insert(&h.db, i, i * 10, i as i64);
        }
        poll_and_flush(&mut h);
        read_iceberg(&h).len()
    }
    let baseline = count_with(1024);
    assert_eq!(baseline, 37);
    for cs in [1, 2, 7, 100] {
        assert_eq!(
            count_with(cs),
            baseline,
            "chunk_size={cs} produced wrong row count"
        );
    }
}

// ---------- FileIndex rebuild on restart ----------

#[test]
fn re_insert_after_restart_correctly_promotes_via_rebuilt_file_index() {
    // Phase 1: insert id=1 v=1, materialize. Phase 1 dies.
    // Phase 2: a new process boots; rebuilds FileIndex from catalog so it
    //          knows pk=1 is "live" in a prior data file.
    //          PG side: row(1) gets a new value at v=2 (UPDATE bumps wm).
    //          Poll picks it up. Promote_re_inserts must fire even though
    //          phase-2's in-memory FileIndex started empty pre-rebuild.
    // Phase 3: read Iceberg → must show {(1, 99, 2)}, NOT duplicates of (1, *).

    let db = SimPostgres::new();
    db.create_table(schema()).unwrap();
    let clock = TestClock::at(0);
    let arc_clock: Arc<dyn pg2iceberg_core::Clock> = Arc::new(clock);
    let coord = Arc::new(MemoryCoordinator::new(
        CoordSchema::default_name(),
        arc_clock,
    ));
    let blob_store = Arc::new(MemoryBlobStore::new());
    let catalog = Arc::new(MemoryCatalog::new());
    let namer = Arc::new(CounterMaterializerNamer::new("s3://table"));

    {
        let mut h = boot_with_coord(
            db.clone(),
            coord.clone(),
            catalog.clone(),
            blob_store.clone(),
            namer.clone(),
        );
        insert(&h.db, 1, 10, 1);
        poll_and_flush(&mut h);
    }
    // Process death.

    // Phase 2: bump the row.
    update(&db, 1, 99, 2);

    let mut h = boot_with_coord(db, coord, catalog, blob_store, namer);
    // FileIndex now contains pk=1 → prior file path. Confirm by polling +
    // flushing and asserting only one row remains visible.
    poll_and_flush(&mut h);
    let iceberg = read_iceberg(&h);
    assert_eq!(
        iceberg,
        vec![row(1, 99, 2)],
        "re-insert after restart must replace prior row, not duplicate"
    );
}

#[test]
fn short_batch_terminates_loop() {
    // Confirm that returning fewer than chunk_size rows ends the loop —
    // the next poll should be a no-op, not another loop iteration.
    let mut h = boot_with_poll_chunk_size(10);
    for i in 1..=3 {
        insert(&h.db, i, i * 10, i as i64);
    }
    let total = block_on(h.qp.poll(&h.db)).unwrap();
    assert_eq!(total, 3);
    // No new rows: next poll fetches 0 rows, exits without looping.
    let total2 = block_on(h.qp.poll(&h.db)).unwrap();
    assert_eq!(total2, 0);
}
