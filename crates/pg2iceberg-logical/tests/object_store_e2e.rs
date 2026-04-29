//! Differential test: Pipeline + Materializer drive identical end state
//! through `MemoryBlobStore` (sim) vs `ObjectStoreBlobStore` (prod, backed
//! by `object_store::memory::InMemory`).
//!
//! This catches divergences in the prod-impl wrapper — path encoding,
//! byte-level put/get fidelity, error mapping. The "production" leg uses
//! the real `object_store::ObjectStore` trait via the prod wrapper, so
//! anything that breaks here would also break against real S3.

use object_store::memory::InMemory;
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
use pg2iceberg_stream::{BlobStore, ObjectStoreBlobStore};
use pollster::block_on;
use std::collections::BTreeMap;
use std::sync::Arc;

fn ident() -> TableIdent {
    TableIdent {
        namespace: Namespace(vec!["public".into()]),
        name: "orders".into(),
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

fn pk_int(r: &Row) -> i32 {
    match r.get(&col("id")) {
        Some(PgValue::Int4(n)) => *n,
        _ => i32::MAX,
    }
}

/// Run a fixed workload (insert/update/delete) against the supplied blob
/// store, materialize, and return Iceberg state sorted by PK.
fn run_workload_and_read(blob_store: Arc<dyn BlobStore>) -> Vec<Row> {
    let db = pg2iceberg_sim::postgres::SimPostgres::new();
    db.create_table(schema()).unwrap();
    db.create_publication("pub1", &[ident()]).unwrap();
    db.create_slot("slot1", "pub1").unwrap();

    let clock = TestClock::at(0);
    let arc_clock: Arc<dyn pg2iceberg_core::Clock> = Arc::new(clock);
    let coord = Arc::new(MemoryCoordinator::new(
        CoordSchema::default_name(),
        arc_clock,
    ));
    let catalog = Arc::new(MemoryCatalog::new());

    // Use simple relative paths — object_store's `Path::parse` requires
    // them. The `s3://...` prefix is a binary-config concern, not a
    // pipeline concern.
    let stage_namer = Arc::new(CounterBlobNamer::new("stage"));
    let mut pipeline = Pipeline::new(coord.clone(), blob_store.clone(), stage_namer, 64);

    let mat_namer = Arc::new(CounterMaterializerNamer::new("table"));
    let mut materializer = Materializer::new(
        coord.clone() as Arc<dyn Coordinator>,
        blob_store.clone(),
        catalog.clone(),
        mat_namer,
        "default",
        128,
    );
    block_on(materializer.register_table(schema())).unwrap();

    let mut stream = db.start_replication("slot1").unwrap();

    // Workload: insert 1, 2, 3; update 2; delete 3.
    {
        let mut tx = db.begin_tx();
        tx.insert(&ident(), row(1, 10));
        tx.insert(&ident(), row(2, 20));
        tx.insert(&ident(), row(3, 30));
        tx.commit(Timestamp(0)).unwrap();
    }
    {
        let mut tx = db.begin_tx();
        tx.update(&ident(), row(2, 999));
        tx.commit(Timestamp(0)).unwrap();
    }
    {
        let mut tx = db.begin_tx();
        let mut pk = BTreeMap::new();
        pk.insert(col("id"), PgValue::Int4(3));
        tx.delete(&ident(), pk);
        tx.commit(Timestamp(0)).unwrap();
    }

    while let Some(msg) = stream.recv() {
        block_on(pipeline.process(msg)).unwrap();
    }
    block_on(pipeline.flush()).unwrap();
    stream.send_standby(pipeline.flushed_lsn());
    block_on(materializer.cycle()).unwrap();

    let mut rows = block_on(read_materialized_state(
        catalog.as_ref(),
        blob_store.as_ref(),
        &ident(),
        &schema(),
        &[col("id")],
    ))
    .unwrap();
    rows.sort_by_key(pk_int);
    rows
}

#[test]
fn memory_and_object_store_blob_produce_identical_iceberg_state() {
    let mem = run_workload_and_read(Arc::new(MemoryBlobStore::new()));
    let object_store_blob: Arc<dyn BlobStore> =
        Arc::new(ObjectStoreBlobStore::new(Arc::new(InMemory::new())));
    let prod = run_workload_and_read(object_store_blob);

    assert_eq!(mem.len(), 2, "expected 2 rows after I/U/D workload");
    assert_eq!(
        mem, prod,
        "MemoryBlobStore and ObjectStoreBlobStore diverged: mem={mem:?}, prod={prod:?}"
    );
}
