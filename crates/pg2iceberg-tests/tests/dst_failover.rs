//! Failover slots: the lifecycle makes its replication slot one where
//! the source has them (PG 17+) unless told not, and keeps an existing
//! slot in line with that.

use pg2iceberg_coord::schema::CoordSchema;
use pg2iceberg_core::typemap::IcebergType;
use pg2iceberg_core::{
    ColumnName, ColumnSchema, IdGen, InMemoryMetrics, Labels, Namespace, PgValue, Row, TableIdent,
    TableSchema, Timestamp, WorkerId,
};
use pg2iceberg_iceberg::read_materialized_state;
use pg2iceberg_logical::pipeline::CounterBlobNamer;
use pg2iceberg_logical::CounterMaterializerNamer;
use pg2iceberg_sim::blob::MemoryBlobStore;
use pg2iceberg_sim::catalog::MemoryCatalog;
use pg2iceberg_sim::coord::MemoryCoordinator;
use pg2iceberg_sim::postgres::{SimPgClient, SimPostgres};
use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

const SLOT: &str = "failover_slot";
const PUB: &str = "failover_pub";
const PG16: i32 = 160_004;
const PG17: i32 = 170_002;

fn ident() -> TableIdent {
    TableIdent {
        namespace: Namespace(vec!["public".into()]),
        name: "orders".into(),
    }
}

fn schema() -> TableSchema {
    let column = |name: &str, field_id: i32, is_primary_key: bool| ColumnSchema {
        name: name.into(),
        field_id,
        ty: IcebergType::Int,
        nullable: !is_primary_key,
        is_primary_key,
    };
    TableSchema {
        ident: ident(),
        columns: vec![column("id", 1, true), column("qty", 2, false)],
        partition_spec: Vec::new(),
        pg_schema: None,
    }
}

fn row(id: i32, qty: i32) -> Row {
    let mut r = Row::new();
    r.insert(ColumnName("id".into()), PgValue::Int4(id));
    r.insert(ColumnName("qty".into()), PgValue::Int4(qty));
    r
}

fn insert(db: &SimPostgres, id: i32) {
    let mut tx = db.begin_tx();
    tx.insert(&ident(), row(id, id * 10));
    tx.commit(Timestamp(0)).unwrap();
}

/// Time as tokio's clock tells it, so a paused-time test's main loop
/// fires its handlers as virtual time passes.
struct TokioClock(tokio::time::Instant);

#[async_trait::async_trait]
impl pg2iceberg_core::Clock for TokioClock {
    fn now(&self) -> Timestamp {
        Timestamp(self.0.elapsed().as_micros() as i64)
    }

    async fn sleep(&self, d: Duration) {
        tokio::time::sleep(d).await
    }
}

struct ZeroIdGen;
impl IdGen for ZeroIdGen {
    fn new_uuid(&self) -> [u8; 16] {
        [0u8; 16]
    }
    fn worker_id(&self) -> WorkerId {
        WorkerId("dst-failover".into())
    }
}

/// One pipeline's state, kept across the processes that run it.
struct Pipeline {
    db: SimPostgres,
    coord: Arc<MemoryCoordinator>,
    catalog: Arc<MemoryCatalog>,
    blob: Arc<MemoryBlobStore>,
    blob_namer: Arc<CounterBlobNamer>,
    materializer_namer: Arc<CounterMaterializerNamer>,
}

impl Pipeline {
    fn new(db: &SimPostgres) -> Self {
        let clock: Arc<dyn pg2iceberg_core::Clock> =
            Arc::new(TokioClock(tokio::time::Instant::now()));
        Self {
            db: db.clone(),
            coord: Arc::new(MemoryCoordinator::new(CoordSchema::default_name(), clock)),
            catalog: Arc::new(MemoryCatalog::new()),
            blob: Arc::new(MemoryBlobStore::new()),
            blob_namer: Arc::new(CounterBlobNamer::new("s3://stage")),
            materializer_namer: Arc::new(CounterMaterializerNamer::new("s3://table")),
        }
    }

    /// Run a process told `slot_failover` for `secs` of virtual time,
    /// `during` running alongside it; returns how it ended.
    async fn run(
        &self,
        slot_failover: bool,
        metrics: Arc<InMemoryMetrics>,
        secs: u64,
        during: impl FnOnce() + Send + 'static,
    ) -> Result<(), pg2iceberg_validate::LifecycleError> {
        let pg_client = Arc::new(SimPgClient::new(self.db.clone()));
        let snapshot_db = self.db.clone();
        let second = Duration::from_secs(1);
        let lifecycle = pg2iceberg_validate::LogicalLifecycle {
            pg: pg_client.clone(),
            slot_monitor: pg_client,
            coord: self.coord.clone(),
            catalog: self.catalog.clone(),
            blob: self.blob.clone(),
            clock: Arc::new(TokioClock(tokio::time::Instant::now())),
            id_gen: Arc::new(ZeroIdGen),
            schemas: vec![schema()],
            skip_snapshot_idents: BTreeSet::new(),
            slot_name: SLOT.into(),
            publication_name: PUB.into(),
            slot_failover,
            group: "default".into(),
            schedule: pg2iceberg_logical::Schedule {
                flush: second,
                materialize: second,
                standby: second,
                watcher: second,
            },
            compaction: None,
            maintenance: Default::default(),
            flush_rows: 64,
            mat_batch_rows: 128,
            mat_table_concurrency: pg2iceberg_logical::materializer::DEFAULT_TABLE_CONCURRENCY,
            pk_index_memory: pg2iceberg_logical::materializer::DEFAULT_PK_INDEX_MEMORY,
            snapshot_concurrency: pg2iceberg_snapshot::DEFAULT_TABLE_CONCURRENCY,
            snapshot_source_factory: Box::new(move |_| {
                Box::pin(async move {
                    Ok::<
                        Box<dyn pg2iceberg_snapshot::SnapshotSource>,
                        pg2iceberg_validate::LifecycleError,
                    >(Box::new(snapshot_db))
                })
            }),
            materializer_namer: self.materializer_namer.clone(),
            blob_namer: self.blob_namer.clone(),
            metrics,
            meta_namespace: None,
        };
        let shutdown = Box::pin(async move {
            tokio::time::sleep(Duration::from_millis(500)).await;
            during();
            tokio::time::sleep(Duration::from_secs(secs)).await;
        });
        pg2iceberg_validate::run_logical_lifecycle(lifecycle, shutdown).await
    }

    /// The ids the Iceberg table holds.
    async fn iceberg_ids(&self) -> BTreeSet<i32> {
        read_materialized_state(
            self.catalog.as_ref(),
            self.blob.as_ref(),
            &ident(),
            &schema(),
            &[ColumnName("id".into())],
        )
        .await
        .unwrap()
        .iter()
        .map(|r| match r.get(&ColumnName("id".into())) {
            Some(PgValue::Int4(id)) => *id,
            other => panic!("id {other:?}"),
        })
        .collect()
    }

    fn slot_failover(&self) -> bool {
        self.db.slot_state(SLOT).unwrap().failover
    }
}

/// A source with one table holding rows 1 and 2, as `version`.
fn source(version: i32) -> SimPostgres {
    let db = SimPostgres::new();
    db.set_server_version_num(version);
    db.create_table(schema()).unwrap();
    insert(&db, 1);
    insert(&db, 2);
    db
}

fn metrics() -> Arc<InMemoryMetrics> {
    Arc::new(InMemoryMetrics::new())
}

/// On PG 17 the lifecycle creates a failover slot, and its gauge says
/// so.
#[tokio::test(start_paused = true)]
async fn the_slot_is_a_failover_slot_on_pg17() {
    let p = Pipeline::new(&source(PG17));
    let m = metrics();
    p.run(true, m.clone(), 3, || {}).await.unwrap();
    assert!(p.slot_failover());
    assert_eq!(
        m.gauge_value(
            pg2iceberg_core::metrics::names::SLOT_FAILOVER,
            &Labels::new()
        ),
        Some(1.0)
    );
    assert_eq!(p.iceberg_ids().await, BTreeSet::from([1, 2]));
}

/// Before PG 17 there are no failover slots: the default (failover on)
/// makes a plain one instead of a `FAILOVER` option PG would refuse, and
/// no gauge.
#[tokio::test(start_paused = true)]
async fn before_pg17_the_slot_is_a_plain_one() {
    let db = source(PG16);
    let p = Pipeline::new(&db);
    let m = metrics();
    let during = db.clone();
    p.run(true, m.clone(), 3, move || insert(&during, 3))
        .await
        .unwrap();
    assert!(!p.slot_failover());
    assert_eq!(
        m.gauge_value(
            pg2iceberg_core::metrics::names::SLOT_FAILOVER,
            &Labels::new()
        ),
        None
    );
    assert_eq!(p.iceberg_ids().await, BTreeSet::from([1, 2, 3]));
}

/// Told not to, the lifecycle makes a plain slot on PG 17 too.
#[tokio::test(start_paused = true)]
async fn told_not_to_the_slot_is_a_plain_one() {
    let p = Pipeline::new(&source(PG17));
    let m = metrics();
    p.run(false, m.clone(), 3, || {}).await.unwrap();
    assert!(!p.slot_failover());
    assert_eq!(
        m.gauge_value(
            pg2iceberg_core::metrics::names::SLOT_FAILOVER,
            &Labels::new()
        ),
        Some(0.0)
    );
}

/// A slot that exists — made before pg2iceberg made failover slots, or
/// under the other setting — is altered to match at the next start, and
/// back again.
#[tokio::test(start_paused = true)]
async fn an_existing_slot_is_altered_to_match() {
    let db = source(PG17);
    let p = Pipeline::new(&db);
    p.run(false, metrics(), 3, || {}).await.unwrap();
    assert!(!p.slot_failover());

    let during = db.clone();
    p.run(true, metrics(), 3, move || insert(&during, 3))
        .await
        .unwrap();
    assert!(p.slot_failover());

    let during = db.clone();
    p.run(false, metrics(), 3, move || insert(&during, 4))
        .await
        .unwrap();
    assert!(!p.slot_failover());
    assert_eq!(p.iceberg_ids().await, BTreeSet::from([1, 2, 3, 4]));
}

/// A slot the lifecycle can't alter — a dead connection's walsender
/// still holds it — doesn't stop the pipeline: it replicates as before,
/// and the next start alters it.
#[tokio::test(start_paused = true)]
async fn a_slot_that_cant_be_altered_still_replicates() {
    let db = source(PG17);
    let p = Pipeline::new(&db);
    p.run(false, metrics(), 3, || {}).await.unwrap();

    // Held for one acquisition: the alter's. Replication starts after.
    db.hold_slot(SLOT, 1);
    let during = db.clone();
    p.run(true, metrics(), 3, move || insert(&during, 3))
        .await
        .unwrap();
    assert!(!p.slot_failover());
    assert_eq!(p.iceberg_ids().await, BTreeSet::from([1, 2, 3]));

    p.run(true, metrics(), 3, || {}).await.unwrap();
    assert!(p.slot_failover());
}
