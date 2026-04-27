//! End-to-end test that the `Ticker` correctly drives Pipeline +
//! Materializer + InvariantWatcher through multiple cycles, mirroring
//! what the production binary's main loop does.
//!
//! Uses TestClock so we can advance time deterministically without
//! sleeping. Production runs the same loop under tokio::time::sleep —
//! the Ticker logic itself is identical.

use pg2iceberg_coord::schema::CoordSchema;
use pg2iceberg_coord::Coordinator;
use pg2iceberg_core::metrics::names;
use pg2iceberg_core::typemap::IcebergType;
use pg2iceberg_core::{
    Clock, ColumnName, ColumnSchema, InMemoryMetrics, Labels, Metrics, Namespace, PgValue, Row,
    TableIdent, TableSchema, Timestamp,
};
use pg2iceberg_iceberg::read_materialized_state;
use pg2iceberg_logical::pipeline::CounterBlobNamer;
use pg2iceberg_logical::{
    CounterMaterializerNamer, Handler, Materializer, Pipeline, Schedule, Ticker,
};
use pg2iceberg_sim::blob::MemoryBlobStore;
use pg2iceberg_sim::catalog::MemoryCatalog;
use pg2iceberg_sim::clock::TestClock;
use pg2iceberg_sim::coord::MemoryCoordinator;
use pg2iceberg_sim::postgres::{SimPostgres, SimReplicationStream};
use pg2iceberg_validate::{InvariantWatcher, WatcherInputs};
use pollster::block_on;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

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

#[allow(dead_code)]
struct Harness {
    db: SimPostgres,
    coord: Arc<MemoryCoordinator>,
    blob_store: Arc<MemoryBlobStore>,
    catalog: Arc<MemoryCatalog>,
    pipeline: Pipeline<MemoryCoordinator>,
    materializer: Materializer<MemoryCatalog>,
    stream: SimReplicationStream,
    watcher: InvariantWatcher,
    metrics: Arc<InMemoryMetrics>,
    clock: TestClock,
    ticker: Ticker,
}

fn boot(schedule: Schedule) -> Harness {
    let db = SimPostgres::new();
    db.create_table(schema()).unwrap();
    db.create_publication("pub1", &[ident()]).unwrap();
    db.create_slot("slot1", "pub1").unwrap();

    let clock = TestClock::at(0);
    let arc_clock: Arc<dyn Clock> = Arc::new(clock.clone());
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
        metrics_dyn.clone(),
    );
    block_on(materializer.register_table(schema())).unwrap();

    let stream = db.start_replication("slot1").unwrap();
    let watcher = InvariantWatcher::new(coord.clone() as Arc<dyn Coordinator>, metrics_dyn);
    let ticker = Ticker::new(clock.now(), schedule);

    Harness {
        db,
        coord,
        blob_store,
        catalog,
        pipeline,
        materializer,
        stream,
        watcher,
        metrics,
        clock,
        ticker,
    }
}

/// Drain replication stream into pipeline, then run all due handlers at
/// the current clock time. Mirrors the production binary's per-iteration
/// step.
fn pump(h: &mut Harness) {
    while let Some(msg) = h.stream.recv() {
        block_on(h.pipeline.process(msg)).unwrap();
    }
    let now = h.clock.now();
    for handler in h.ticker.fire_due(now) {
        match handler {
            Handler::Flush => {
                block_on(h.pipeline.flush()).unwrap();
            }
            Handler::Standby => {
                h.stream.send_standby(h.pipeline.flushed_lsn());
            }
            Handler::Materialize => {
                block_on(h.materializer.cycle()).unwrap();
            }
            Handler::Watcher => {
                let slot = h.db.slot_state("slot1").unwrap();
                let inputs = WatcherInputs {
                    pipeline_flushed_lsn: h.pipeline.flushed_lsn(),
                    slot_confirmed_flush_lsn: slot.confirmed_flush_lsn,
                    group: "default".into(),
                    watched_tables: vec![ident()],
                    ..Default::default()
                };
                let violations = block_on(h.watcher.check(&inputs));
                assert!(
                    violations.is_empty(),
                    "watcher caught violations: {violations:?}"
                );
            }
        }
    }
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
fn no_writes_no_handler_fire_at_t_zero() {
    let mut h = boot(Schedule::default());
    pump(&mut h);
    let no_labels = Labels::new();
    assert_eq!(
        h.metrics
            .counter_value(names::PIPELINE_FLUSH_TOTAL, &no_labels),
        0,
        "no handler should fire at t=0"
    );
}

#[test]
fn ticker_drives_full_cycle_after_interval_elapses() {
    // 5s flush interval, everything else 5s too so they all fire together.
    let s = Schedule {
        flush: Duration::from_secs(5),
        standby: Duration::from_secs(5),
        materialize: Duration::from_secs(5),
        watcher: Duration::from_secs(5),
    };
    let mut h = boot(s);

    let mut tx = h.db.begin_tx();
    tx.insert(&ident(), row(1, 10));
    tx.insert(&ident(), row(2, 20));
    tx.commit(Timestamp(0)).unwrap();

    // t=0: nothing due yet — pipeline still drains and processes events,
    // but no flush, no cycle.
    pump(&mut h);
    assert!(read_iceberg(&h).is_empty());

    // Advance to t=5: every interval has elapsed, all four handlers fire.
    h.clock.advance(Duration::from_secs(5));
    pump(&mut h);

    // After flush + materialize, Iceberg should mirror PG.
    assert_eq!(read_iceberg(&h), read_pg(&h));
    assert_eq!(read_iceberg(&h).len(), 2);

    // Slot ack should match pipeline flushed LSN (standby fired).
    let slot = h.db.slot_state("slot1").unwrap();
    assert_eq!(slot.confirmed_flush_lsn, h.pipeline.flushed_lsn());
}

#[test]
fn ticker_handles_multiple_cycles_with_intervening_writes() {
    let s = Schedule {
        flush: Duration::from_secs(5),
        standby: Duration::from_secs(5),
        materialize: Duration::from_secs(5),
        watcher: Duration::from_secs(5),
    };
    let mut h = boot(s);

    // Cycle 1: insert row 1, advance time, pump.
    let mut tx = h.db.begin_tx();
    tx.insert(&ident(), row(1, 10));
    tx.commit(Timestamp(0)).unwrap();
    h.clock.advance(Duration::from_secs(5));
    pump(&mut h);

    // Cycle 2: insert row 2, advance time, pump.
    let mut tx = h.db.begin_tx();
    tx.insert(&ident(), row(2, 20));
    tx.commit(Timestamp(0)).unwrap();
    h.clock.advance(Duration::from_secs(5));
    pump(&mut h);

    // Cycle 3: update row 1, advance time, pump.
    let mut tx = h.db.begin_tx();
    tx.update(&ident(), row(1, 999));
    tx.commit(Timestamp(0)).unwrap();
    h.clock.advance(Duration::from_secs(5));
    pump(&mut h);

    assert_eq!(read_iceberg(&h), read_pg(&h));
    assert_eq!(read_iceberg(&h), vec![row(1, 999), row(2, 20)]);
}

#[test]
fn flush_fires_more_often_than_materialize_when_intervals_differ() {
    // 5s flush, 15s materialize: at t=15 we should have 3 flushes and 1
    // materialize cycle.
    let s = Schedule {
        flush: Duration::from_secs(5),
        standby: Duration::from_secs(5),
        materialize: Duration::from_secs(15),
        watcher: Duration::from_secs(60),
    };
    let mut h = boot(s);

    // Add a single row so flush has work to do.
    let mut tx = h.db.begin_tx();
    tx.insert(&ident(), row(1, 10));
    tx.commit(Timestamp(0)).unwrap();

    // 3 pumps spaced 5s apart.
    for _ in 0..3 {
        h.clock.advance(Duration::from_secs(5));
        pump(&mut h);
    }

    let no_labels = Labels::new();
    let mut table_labels = Labels::new();
    table_labels.insert("table".into(), "orders".into());

    // Ticker fires Handler::Flush at t=5, t=10, t=15 — but the metrics
    // counter only ticks when there's actual work (Sink.flush returns
    // Some). Only the first cycle has data; subsequent cycles return
    // early. So flush_total = 1, not 3.
    assert_eq!(
        h.metrics
            .counter_value(names::PIPELINE_FLUSH_TOTAL, &no_labels),
        1,
        "only the first flush had buffered work"
    );

    // Materialize fired at t=15 (interval = 15s).
    assert_eq!(
        h.metrics
            .counter_value(names::MATERIALIZER_CYCLE_TOTAL, &table_labels),
        1
    );
}

#[test]
fn next_due_advances_correctly_after_partial_fire() {
    let s = Schedule {
        flush: Duration::from_secs(5),
        standby: Duration::from_secs(10),
        materialize: Duration::from_secs(15),
        watcher: Duration::from_secs(30),
    };
    let mut h = boot(s);
    // At t=0, earliest is flush at t=5.
    assert_eq!(h.ticker.next_due(), Timestamp(5_000_000));

    h.clock.advance(Duration::from_secs(5));
    pump(&mut h);
    // Now last_flush = 5; next flush at t=10. Standby still due at t=10.
    // Next due = min(10, 10, 15, 30) = 10.
    assert_eq!(h.ticker.next_due(), Timestamp(10_000_000));
}

#[test]
fn ticker_with_no_drift_overslept_oversleep() {
    // Sleeping past several intervals shouldn't queue up a backlog.
    let s = Schedule {
        flush: Duration::from_secs(5),
        standby: Duration::from_secs(5),
        materialize: Duration::from_secs(5),
        watcher: Duration::from_secs(30),
    };
    let mut h = boot(s);

    // Add work so flush has data.
    let mut tx = h.db.begin_tx();
    tx.insert(&ident(), row(1, 10));
    tx.commit(Timestamp(0)).unwrap();

    // Sleep for an hour.
    h.clock.advance(Duration::from_secs(3600));
    pump(&mut h);

    let no_labels = Labels::new();
    // Flush fires once even though 720 intervals elapsed.
    assert_eq!(
        h.metrics
            .counter_value(names::PIPELINE_FLUSH_TOTAL, &no_labels),
        1
    );
}
