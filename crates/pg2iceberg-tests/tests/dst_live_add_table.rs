//! Lifecycle DST: a new table appears in YAML on a non-fresh slot.
//!
//! End-to-end exercise of Phases A–D from the "background snapshot
//! during live CDC" plan:
//!
//! Phase 1 of this test pre-onboards `table_a` by running it through
//! the same `Snapshotter` + `Materializer` plumbing pg2iceberg uses
//! in prod. After Phase 1, coord, catalog, and blob are in a real
//! "post-onboarded" state — the same state pg2iceberg would leave
//! behind after a normal first deploy.
//!
//! Phase 2 spins up a fresh `Materializer` + `LogicalLifecycle`
//! against the same coord/catalog/blob, but with YAML now also
//! configuring `table_b`. The lifecycle must:
//!
//! 1. Reconcile the publication: `ALTER PUBLICATION ADD TABLE` for
//!    `table_b`.
//! 2. Register `table_b` on the materializer with
//!    `register_table_pending` (its CDC events stay gated until
//!    backfill finishes).
//! 3. Spawn a background snapshot task that pumps `table_b`'s rows
//!    through a task-local pipeline into coord/blob.
//! 4. On per-table completion, the main loop's pending-snapshot
//!    select arm calls `materializer.mark_snapshot_complete`.
//!
//! Final invariants:
//! - Publication contains both tables.
//! - `table_b`'s coord row shows `snapshot_complete = true`.
//! - All seed rows for `table_b` are visible in Iceberg.
//! - `table_a`'s state is undisturbed.

use pg2iceberg_coord::schema::CoordSchema;
use pg2iceberg_coord::Coordinator;
use pg2iceberg_core::typemap::IcebergType;
use pg2iceberg_core::{
    ColumnName, ColumnSchema, IdGen, Mode, Namespace, PgValue, Row, TableIdent, TableSchema,
    Timestamp, WorkerId,
};
use pg2iceberg_logical::pipeline::CounterBlobNamer;
use pg2iceberg_logical::{CounterMaterializerNamer, Materializer, Pipeline, Schedule};
use pg2iceberg_sim::clock::TestClock;
use pg2iceberg_sim::postgres::{SimPgClient, SimPostgres};
use pg2iceberg_validate::{LogicalLifecycle, SnapshotSourceFactory};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

const NAMESPACE: &str = "public";
const PUB: &str = "live_add_pub";
const SLOT: &str = "live_add_slot";

fn ident_a() -> TableIdent {
    TableIdent {
        namespace: Namespace(vec![NAMESPACE.into()]),
        name: "table_a".into(),
    }
}

fn ident_b() -> TableIdent {
    TableIdent {
        namespace: Namespace(vec![NAMESPACE.into()]),
        name: "table_b".into(),
    }
}

fn schema_for(ident: TableIdent) -> TableSchema {
    TableSchema {
        ident,
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
        pg_schema: None,
    }
}

fn row(id: i32, qty: i32) -> Row {
    let mut r = BTreeMap::new();
    r.insert(ColumnName("id".into()), PgValue::Int4(id));
    r.insert(ColumnName("qty".into()), PgValue::Int4(qty));
    r
}

#[tokio::test]
async fn live_add_table_backfills_via_background_snapshot() {
    // ── 0. Source PG with both tables. table_a is initially in
    //      the publication; table_b is the "new" table the operator
    //      added on this restart.
    let db = SimPostgres::new();
    db.create_table(schema_for(ident_a())).unwrap();
    db.create_table(schema_for(ident_b())).unwrap();
    // Publication starts with table_a only — modeling the "earlier
    // deploy" snapshot.
    db.create_publication(PUB, &[ident_a()]).unwrap();
    db.create_slot(SLOT, PUB).unwrap();
    // Seed table_a (via slot's view: post-create_slot inserts get
    // captured as CDC). 5 rows so Phase 1's snapshot has work to do.
    {
        let mut tx = db.begin_tx();
        for i in 1..=5 {
            tx.insert(&ident_a(), row(i, i * 10));
        }
        tx.commit(Timestamp(0)).unwrap();
    }
    // Seed table_b. These pre-exist when the operator decides to
    // add it to YAML; the backfill snapshot is what makes them
    // reach Iceberg.
    {
        let mut tx = db.begin_tx();
        for i in 1..=10 {
            tx.insert(&ident_b(), row(i, i * 100));
        }
        tx.commit(Timestamp(0)).unwrap();
    }

    // ── 1. Shared coord + catalog + blob. Both phases of the test
    //      operate against these.
    let clock_arc: Arc<dyn pg2iceberg_core::Clock> = Arc::new(TestClock::at(0));
    let coord = Arc::new(pg2iceberg_sim::coord::MemoryCoordinator::new(
        CoordSchema::default_name(),
        Arc::clone(&clock_arc),
    ));
    let blob = Arc::new(pg2iceberg_sim::blob::MemoryBlobStore::new());
    let catalog = Arc::new(pg2iceberg_sim::catalog::MemoryCatalog::new());

    // ── Phase 1: pre-onboard table_a through the same Snapshotter
    //    + Materializer pipeline pg2iceberg uses in prod. After this,
    //    coord shows table_a as snapshot_complete=true with a real
    //    snapshot LSN, catalog has at least one snapshot for A with
    //    a real (parseable) parquet data file in blob, and blob has
    //    that file's bytes.
    {
        let blob_namer = Arc::new(CounterBlobNamer::new("s3://stage"));
        let mat_namer = Arc::new(CounterMaterializerNamer::new("s3://table"));
        let mut pipeline = Pipeline::new(
            coord.clone() as Arc<dyn Coordinator>,
            blob.clone() as Arc<dyn pg2iceberg_stream::BlobStore>,
            blob_namer,
            64,
        );
        pipeline.register_primary_keys(ident_a(), vec![ColumnName("id".into())]);
        let mut materializer: Materializer<pg2iceberg_sim::catalog::MemoryCatalog> =
            Materializer::new(
                coord.clone() as Arc<dyn Coordinator>,
                blob.clone() as Arc<dyn pg2iceberg_stream::BlobStore>,
                catalog.clone(),
                mat_namer,
                "default",
                128,
            );
        materializer
            .register_table(schema_for(ident_a()))
            .await
            .unwrap();

        let snapshotter =
            pg2iceberg_snapshot::Snapshotter::new(coord.clone() as Arc<dyn Coordinator>);
        let snap_lsn = snapshotter
            .run(&db, &[schema_for(ident_a())], &mut pipeline)
            .await
            .unwrap();
        // Drive any post-snapshot CDC events through pipeline so
        // they're durable, then materialize.
        let stream = db.start_replication(SLOT).unwrap();
        let mut stream = stream;
        while let Some(msg) = stream.recv() {
            pipeline.process(msg).await.unwrap();
        }
        pipeline.flush().await.unwrap();
        stream.send_standby(snap_lsn);
        materializer.cycle().await.unwrap();
    }

    // Snapshot the slot's confirmed_flush_lsn into coord so the
    // tamper-detection invariant ("slot advanced externally") sees
    // them aligned. Phase 1's standby ack pushed the slot's
    // confirmed_flush_lsn forward; mirror that into coord.
    {
        let slot_lsn = db.slot_state(SLOT).unwrap().confirmed_flush_lsn;
        coord.set_flushed_lsn(slot_lsn).await.unwrap();
    }

    // ── Phase 2: lifecycle restart with table_b added to YAML.
    let pg_client = Arc::new(SimPgClient::new(db.clone()));
    let pg: Arc<dyn pg2iceberg_pg::PgClient> = pg_client.clone();
    let slot_monitor: Arc<dyn pg2iceberg_pg::SlotMonitor> = pg_client.clone();

    struct ZeroIdGen;
    impl IdGen for ZeroIdGen {
        fn new_uuid(&self) -> [u8; 16] {
            [0u8; 16]
        }
        fn worker_id(&self) -> WorkerId {
            WorkerId("dst-live-add".into())
        }
    }
    let id_gen: Arc<dyn IdGen> = Arc::new(ZeroIdGen);

    let snapshot_db = db.clone();
    let snapshot_factory: SnapshotSourceFactory = Box::new(move |_| {
        Box::pin(async move {
            Ok::<Box<dyn pg2iceberg_snapshot::SnapshotSource>, pg2iceberg_validate::LifecycleError>(
                Box::new(snapshot_db),
            )
        })
    });

    let lifecycle = LogicalLifecycle {
        pg,
        slot_monitor,
        coord: coord.clone() as Arc<dyn Coordinator>,
        catalog: catalog.clone(),
        blob: blob.clone() as Arc<dyn pg2iceberg_stream::BlobStore>,
        clock: clock_arc,
        id_gen,
        schemas: vec![schema_for(ident_a()), schema_for(ident_b())],
        skip_snapshot_idents: BTreeSet::new(),
        slot_name: SLOT.into(),
        publication_name: PUB.into(),
        group: "default".into(),
        schedule: Schedule::default(),
        compaction: None,
        flush_rows: 64,
        mat_cycle_limit: 128,
        consumer_ttl: Duration::from_secs(60),
        snapshot_source_factory: snapshot_factory,
        materializer_namer: Arc::new(CounterMaterializerNamer::new("s3://table")),
        blob_namer: Arc::new(CounterBlobNamer::new("s3://stage")),
        metrics: Arc::new(pg2iceberg_core::InMemoryMetrics::new()),
        mode: Mode::Logical,
        meta_namespace: None,
    };

    // Run the lifecycle. Shutdown after a bounded duration — enough
    // for the background snapshot task to finish 10 rows of backfill
    // and for the main loop's pending-snapshot select arm to receive
    // the per-table completion message.
    let shutdown = Box::pin(tokio::time::sleep(Duration::from_millis(500)));
    let result = pg2iceberg_validate::run_logical_lifecycle(lifecycle, shutdown).await;
    result.expect("lifecycle should complete cleanly");

    // ── Invariants.
    // (a) Publication now contains both tables. The lifecycle's
    //     reconciler called ALTER PUBLICATION ADD TABLE table_b.
    let pub_tables: BTreeSet<TableIdent> = db.publication_tables(PUB).into_iter().collect();
    assert!(pub_tables.contains(&ident_a()), "table_a still in pub");
    assert!(
        pub_tables.contains(&ident_b()),
        "ALTER PUBLICATION ADD TABLE table_b should have run during lifecycle reconcile"
    );

    // (b) coord shows table_b as snapshot-complete. This is the
    //     marker the background snapshot task stamps on success
    //     (via Snapshotter::run → mark_table_snapshot_complete).
    let state_b = coord
        .table_state(&ident_b())
        .await
        .expect("read coord state for table_b")
        .expect("table_b should have a coord row after backfill");
    assert!(
        state_b.snapshot_complete,
        "table_b coord row should have snapshot_complete=true after background task"
    );

    // (c) Iceberg has all 10 of table_b's seed rows. They reached
    //     materialization via the spawned task's task-local
    //     pipeline → coord → main_loop's materializer cycles after
    //     the gate was lifted.
    let materialized_b = pg2iceberg_iceberg::read_materialized_state(
        catalog.as_ref(),
        blob.as_ref(),
        &ident_b(),
        &schema_for(ident_b()),
        &[ColumnName("id".into())],
    )
    .await
    .expect("read materialized table_b");
    assert_eq!(
        materialized_b.len(),
        10,
        "all 10 backfill rows should land in Iceberg; got {}",
        materialized_b.len()
    );
    let mut pks: Vec<i32> = materialized_b
        .iter()
        .filter_map(|r| match r.get(&ColumnName("id".into())) {
            Some(PgValue::Int4(n)) => Some(*n),
            _ => None,
        })
        .collect();
    pks.sort();
    assert_eq!(pks, (1..=10).collect::<Vec<i32>>());

    // (d) table_a's row count is unchanged — backfill of B didn't
    //     disturb A's already-materialized state.
    let materialized_a = pg2iceberg_iceberg::read_materialized_state(
        catalog.as_ref(),
        blob.as_ref(),
        &ident_a(),
        &schema_for(ident_a()),
        &[ColumnName("id".into())],
    )
    .await
    .expect("read materialized table_a");
    assert_eq!(
        materialized_a.len(),
        5,
        "table_a should still have its 5 pre-existing rows; got {}",
        materialized_a.len()
    );
}
