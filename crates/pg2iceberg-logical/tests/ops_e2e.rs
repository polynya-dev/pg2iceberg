//! Phase 13 ops tests: pipeline metrics + materializer metrics + graceful
//! shutdown lose nothing in the durable state.

use pg2iceberg_coord::schema::CoordSchema;
use pg2iceberg_coord::Coordinator;
use pg2iceberg_core::metrics::{names, Labels};
use pg2iceberg_core::typemap::IcebergType;
use pg2iceberg_core::{
    ColumnName, ColumnSchema, InMemoryMetrics, Metrics, Namespace, PgValue, Row, TableIdent,
    TableSchema, Timestamp,
};
use pg2iceberg_iceberg::read_materialized_state;
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

#[allow(dead_code)]
struct Harness {
    db: SimPostgres,
    coord: Arc<MemoryCoordinator>,
    blob_store: Arc<MemoryBlobStore>,
    catalog: Arc<MemoryCatalog>,
    pipeline: Pipeline<MemoryCoordinator>,
    materializer: Materializer<MemoryCatalog>,
    stream: SimReplicationStream,
    metrics: Arc<InMemoryMetrics>,
}

fn boot() -> Harness {
    let db = SimPostgres::new();
    db.create_table(schema()).unwrap();
    db.create_publication("pub1", &[ident()]).unwrap();
    db.create_slot("slot1", "pub1").unwrap();

    let clock = TestClock::at(0);
    let arc_clock: Arc<dyn pg2iceberg_core::Clock> = Arc::new(clock);
    let coord = Arc::new(MemoryCoordinator::new(
        CoordSchema::default_name(),
        arc_clock,
    ));
    let blob_store = Arc::new(MemoryBlobStore::new());
    let catalog = Arc::new(MemoryCatalog::new());
    let metrics: Arc<InMemoryMetrics> = Arc::new(InMemoryMetrics::new());
    let metrics_dyn: Arc<dyn Metrics> = metrics.clone();

    let stage_namer = Arc::new(CounterBlobNamer::new("s3://stage"));
    let pipeline = Pipeline::with_metrics(
        coord.clone(),
        blob_store.clone(),
        stage_namer,
        64,
        metrics_dyn.clone(),
    );

    let mat_namer = Arc::new(CounterMaterializerNamer::new("s3://table"));
    let mut materializer = Materializer::with_metrics(
        coord.clone() as Arc<dyn Coordinator>,
        blob_store.clone(),
        catalog.clone(),
        mat_namer,
        "default",
        128,
        metrics_dyn,
    );
    block_on(materializer.register_table(schema())).unwrap();

    let stream = db.start_replication("slot1").unwrap();
    Harness {
        db,
        coord,
        blob_store,
        catalog,
        pipeline,
        materializer,
        stream,
        metrics,
    }
}

fn drive(h: &mut Harness) {
    while let Some(msg) = h.stream.recv() {
        block_on(h.pipeline.process(msg)).unwrap();
    }
    block_on(h.pipeline.flush()).unwrap();
    h.stream.send_standby(h.pipeline.flushed_lsn());
}

// ---------- metrics ----------

#[test]
fn pipeline_flush_emits_counter_and_gauge() {
    let mut h = boot();
    let mut tx = h.db.begin_tx();
    tx.insert(&ident(), row(1, 10));
    tx.insert(&ident(), row(2, 20));
    let commit = tx.commit(Timestamp(0)).unwrap();
    drive(&mut h);

    let no_labels = Labels::new();
    let mut table_labels = Labels::new();
    table_labels.insert("table".into(), "orders".into());

    assert_eq!(
        h.metrics
            .counter_value(names::PIPELINE_FLUSH_TOTAL, &no_labels),
        1
    );
    assert_eq!(
        h.metrics
            .counter_value(names::PIPELINE_ROWS_STAGED_TOTAL, &table_labels),
        2
    );
    assert_eq!(
        h.metrics
            .gauge_value(names::PIPELINE_FLUSHED_LSN, &no_labels),
        Some(commit.0 as f64)
    );
}

#[test]
fn materializer_cycle_emits_counters_per_table() {
    let mut h = boot();
    for i in 1..=3 {
        let mut tx = h.db.begin_tx();
        tx.insert(&ident(), row(i, i * 10));
        tx.commit(Timestamp(0)).unwrap();
    }
    drive(&mut h);
    block_on(h.materializer.cycle()).unwrap();

    let mut table_labels = Labels::new();
    table_labels.insert("table".into(), "orders".into());

    assert_eq!(
        h.metrics
            .counter_value(names::MATERIALIZER_CYCLE_TOTAL, &table_labels),
        1
    );
    assert_eq!(
        h.metrics
            .counter_value(names::MATERIALIZER_ROWS_TOTAL, &table_labels),
        3
    );
}

#[test]
fn metrics_accumulate_across_multiple_flushes() {
    let mut h = boot();
    for batch in 0..3 {
        let mut tx = h.db.begin_tx();
        tx.insert(&ident(), row(batch + 1, (batch + 1) * 10));
        tx.commit(Timestamp(0)).unwrap();
        drive(&mut h);
    }

    let no_labels = Labels::new();
    assert_eq!(
        h.metrics
            .counter_value(names::PIPELINE_FLUSH_TOTAL, &no_labels),
        3
    );
}

// ---------- graceful shutdown ----------

#[test]
fn shutdown_flushes_buffered_tx_then_refuses_new_events() {
    let mut h = boot();
    let mut tx = h.db.begin_tx();
    tx.insert(&ident(), row(1, 10));
    let commit = tx.commit(Timestamp(0)).unwrap();

    // Drain replication into the pipeline buffer but don't manually flush.
    while let Some(msg) = h.stream.recv() {
        block_on(h.pipeline.process(msg)).unwrap();
    }

    // Shutdown does the final flush.
    block_on(h.pipeline.shutdown()).unwrap();
    assert_eq!(h.pipeline.flushed_lsn(), commit);
    assert!(h.pipeline.is_shut_down());

    // Materializer should now see the staged data.
    block_on(h.materializer.cycle()).unwrap();
    let rows = block_on(read_materialized_state(
        h.catalog.as_ref(),
        h.blob_store.as_ref(),
        &ident(),
        &schema(),
        &[col("id")],
    ))
    .unwrap();
    assert_eq!(rows.len(), 1);

    // Subsequent process calls are no-ops.
    let pre = h.pipeline.flushed_lsn();
    let mut tx = h.db.begin_tx();
    tx.insert(&ident(), row(2, 20));
    tx.commit(Timestamp(0)).unwrap();
    while let Some(msg) = h.stream.recv() {
        block_on(h.pipeline.process(msg)).unwrap();
    }
    block_on(h.pipeline.flush()).unwrap();
    assert_eq!(
        h.pipeline.flushed_lsn(),
        pre,
        "post-shutdown work must not advance flushed_lsn"
    );
}

#[test]
fn shutdown_is_idempotent() {
    let mut h = boot();
    block_on(h.pipeline.shutdown()).unwrap();
    block_on(h.pipeline.shutdown()).unwrap();
    block_on(h.pipeline.shutdown()).unwrap();
    assert!(h.pipeline.is_shut_down());
}

#[test]
fn shutdown_with_no_buffered_work_is_a_noop_flush() {
    let mut h = boot();
    block_on(h.pipeline.shutdown()).unwrap();

    // No flush ever happened → counter stays at 0.
    let no_labels = Labels::new();
    assert_eq!(
        h.metrics
            .counter_value(names::PIPELINE_FLUSH_TOTAL, &no_labels),
        0
    );
}
