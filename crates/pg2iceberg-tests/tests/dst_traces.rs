//! The spans a lifecycle run makes, as an OpenTelemetry exporter sends
//! them (see `pg2iceberg_logical::spans`).
//!
//! A test binary of its own: `tracing` caches whether anything is
//! interested in a span site process-wide, and tests on other threads
//! without a subscriber can cache "nothing" while this one installs its
//! thread-local recorder.

use pg2iceberg_coord::schema::CoordSchema;
use pg2iceberg_core::typemap::IcebergType;
use pg2iceberg_core::{
    ColumnName, ColumnSchema, Namespace, PgValue, Row, TableIdent, TableSchema, Timestamp,
};
use pg2iceberg_iceberg::CompactionConfig;
use pg2iceberg_logical::pipeline::CounterBlobNamer;
use pg2iceberg_logical::CounterMaterializerNamer;
use pg2iceberg_sim::blob::MemoryBlobStore;
use pg2iceberg_sim::catalog::MemoryCatalog;
use pg2iceberg_sim::coord::MemoryCoordinator;
use pg2iceberg_sim::fault::{ops, FaultPlan, FaultyCoordinator};
use pg2iceberg_sim::postgres::{SimPgClient, SimPostgres};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

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
    let mut r = BTreeMap::new();
    r.insert(ColumnName("id".into()), PgValue::Int4(id));
    r.insert(ColumnName("qty".into()), PgValue::Int4(qty));
    r
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

/// The full lifecycle over `db` — fresh slot, so the snapshot phase runs
/// first — every handler due each second, its coordinator failing as
/// `plan` says.
fn lifecycle(
    db: &SimPostgres,
    plan: FaultPlan,
) -> pg2iceberg_validate::LogicalLifecycle<MemoryCatalog> {
    use pg2iceberg_core::{IdGen, InMemoryMetrics, WorkerId};

    struct ZeroIdGen;
    impl IdGen for ZeroIdGen {
        fn new_uuid(&self) -> [u8; 16] {
            [0u8; 16]
        }
        fn worker_id(&self) -> WorkerId {
            WorkerId("dst-traces".into())
        }
    }

    let clock: Arc<dyn pg2iceberg_core::Clock> = Arc::new(TokioClock(tokio::time::Instant::now()));
    let coord = Arc::new(MemoryCoordinator::new(
        CoordSchema::default_name(),
        clock.clone(),
    ));
    let pg_client = Arc::new(SimPgClient::new(db.clone()));
    let snapshot_db = db.clone();
    let second = Duration::from_secs(1);
    pg2iceberg_validate::LogicalLifecycle {
        pg: pg_client.clone(),
        slot_monitor: pg_client,
        coord: Arc::new(FaultyCoordinator::new(coord, plan)),
        catalog: Arc::new(MemoryCatalog::new()),
        blob: Arc::new(MemoryBlobStore::new()),
        clock,
        id_gen: Arc::new(ZeroIdGen),
        schemas: vec![schema()],
        skip_snapshot_idents: BTreeSet::new(),
        slot_name: "traces-slot".into(),
        publication_name: "traces-pub".into(),
        group: "default".into(),
        schedule: pg2iceberg_logical::Schedule {
            flush: second,
            materialize: second,
            standby: second,
            watcher: second,
        },
        compaction: Some(CompactionConfig::default()),
        maintenance: Default::default(),
        flush_rows: 64,
        mat_batch_rows: 128,
        snapshot_source_factory: Box::new(move |_| {
            Box::pin(async move {
                Ok::<
                    Box<dyn pg2iceberg_snapshot::SnapshotSource>,
                    pg2iceberg_validate::LifecycleError,
                >(Box::new(snapshot_db))
            })
        }),
        materializer_namer: Arc::new(CounterMaterializerNamer::new("s3://table")),
        blob_namer: Arc::new(CounterBlobNamer::new("s3://stage")),
        metrics: Arc::new(InMemoryMetrics::new()),
        meta_namespace: None,
    }
}

/// Spans as a tracing layer sees them, in the order they began.
#[derive(Clone, Default)]
struct SpanRecorder(Arc<std::sync::Mutex<Vec<RecordedSpan>>>);

#[derive(Clone, Debug)]
struct RecordedSpan {
    name: &'static str,
    fields: BTreeMap<String, String>,
    /// Index of the span it's a child of.
    parent: Option<usize>,
}

struct FieldsVisitor<'a>(&'a mut BTreeMap<String, String>);

impl tracing::field::Visit for FieldsVisitor<'_> {
    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        self.0.insert(field.name().into(), value.into());
    }

    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        self.0.insert(field.name().into(), format!("{value:?}"));
    }
}

impl<S> tracing_subscriber::Layer<S> for SpanRecorder
where
    S: tracing::Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a>,
{
    fn on_new_span(
        &self,
        attrs: &tracing::span::Attributes<'_>,
        id: &tracing::span::Id,
        ctx: tracing_subscriber::layer::Context<'_, S>,
    ) {
        let span = ctx.span(id).expect("the span being created");
        let parent = span
            .parent()
            .and_then(|p| p.extensions().get::<usize>().copied());
        let mut fields = BTreeMap::new();
        attrs.record(&mut FieldsVisitor(&mut fields));
        let mut spans = self.0.lock().unwrap();
        spans.push(RecordedSpan {
            name: attrs.metadata().name(),
            fields,
            parent,
        });
        span.extensions_mut().insert(spans.len() - 1);
    }

    fn on_record(
        &self,
        id: &tracing::span::Id,
        values: &tracing::span::Record<'_>,
        ctx: tracing_subscriber::layer::Context<'_, S>,
    ) {
        let span = ctx.span(id).expect("a recorded span");
        let Some(&i) = span.extensions().get::<usize>() else {
            return;
        };
        values.record(&mut FieldsVisitor(&mut self.0.lock().unwrap()[i].fields));
    }
}

impl SpanRecorder {
    fn spans(&self) -> Vec<RecordedSpan> {
        self.0.lock().unwrap().clone()
    }
}

/// The spans a run makes, as an OpenTelemetry exporter sends them: each
/// unit of work a trace — a flush, a materializer cycle, a snapshot chunk
/// — and every request to the catalog, object store and coordinator
/// within the one that made it, never a trace of its own.
#[tokio::test(start_paused = true)]
async fn every_request_is_traced_within_its_unit_of_work() {
    use tracing_subscriber::layer::SubscriberExt as _;

    let recorder = SpanRecorder::default();
    let _subscriber =
        tracing::subscriber::set_default(tracing_subscriber::registry().with(recorder.clone()));

    // Four rows before the slot: the snapshot's.
    let db = SimPostgres::new();
    db.create_table(schema()).unwrap();
    let mut tx = db.begin_tx();
    for i in 1..=4 {
        tx.insert(&ident(), row(i, i * 10));
    }
    tx.commit(Timestamp(0)).unwrap();
    // The first ack's stamp fails (the two before are the start's).
    let plan = FaultPlan::new();
    plan.fail(ops::COORD_SET_FLUSHED_LSN, 2..3);
    let lifecycle = lifecycle(&db, plan);
    let script = db.clone();
    let shutdown = Box::pin(async move {
        tokio::time::sleep(Duration::from_millis(1500)).await;
        let mut tx = script.begin_tx();
        tx.insert(&ident(), row(5, 50));
        tx.update(&ident(), row(1, 11));
        tx.commit(Timestamp(0)).unwrap();
        tokio::time::sleep(Duration::from_secs(3)).await;
        // Past `flush_rows` (64): staged in chunks before it commits.
        let mut tx = script.begin_tx();
        for i in 100..300 {
            tx.insert(&ident(), row(i, i));
        }
        tx.commit(Timestamp(0)).unwrap();
        tokio::time::sleep(Duration::from_secs(5)).await;
    });
    pg2iceberg_validate::run_logical_lifecycle(lifecycle, shutdown)
        .await
        .unwrap();

    let spans = recorder.spans();
    let parent = |s: &RecordedSpan| s.parent.map(|p| &spans[p]);
    let named = |name: &'static str| spans.iter().filter(move |s| s.name == name);
    let field = |s: &RecordedSpan, f: &str| s.fields.get(f).cloned();

    // Every request belongs to a unit of work.
    let requests: Vec<&RecordedSpan> = named("request").collect();
    assert!(requests.len() > 20, "{} requests", requests.len());
    for r in &requests {
        let within = parent(r).map(|p| p.name);
        assert!(
            within.is_some(),
            "a request traced on its own: {:?}",
            r.fields
        );
        assert_ne!(within, Some("request"), "{:?}", r.fields);
    }
    // Which are the traces.
    let roots: BTreeSet<&str> = spans
        .iter()
        .filter(|s| s.parent.is_none())
        .map(|s| s.name)
        .collect();
    let expected: BTreeSet<&str> = [
        "startup",
        "snapshot.table",
        "snapshot.chunk",
        "pipeline.flush",
        "pipeline.spill",
        "slot.ack",
        "materializer.cycle",
        "compaction.cycle",
        "watcher.check",
        "shutdown",
    ]
    .into_iter()
    .collect();
    assert_eq!(roots, expected);

    // A commit: within its table's materialization, within the cycle.
    let commit = named("materializer.commit")
        .find(|c| c.fields.get("table") == Some(&ident().to_string()))
        .expect("a commit");
    assert_eq!(parent(commit).map(|p| p.name), Some("materializer.table"));
    assert_eq!(
        parent(commit).and_then(|t| t.parent).map(|c| spans[c].name),
        Some("materializer.cycle")
    );
    for f in ["rows", "data_files", "delete_files", "bytes", "snapshot"] {
        assert!(
            field(commit, f).is_some(),
            "commit's {f}: {:?}",
            commit.fields
        );
    }
    let in_commit: BTreeSet<String> = requests
        .iter()
        .filter(|r| {
            r.parent
                .map(|p| &spans[p])
                .is_some_and(|p| std::ptr::eq(p, commit))
        })
        .filter_map(|r| field(r, "op"))
        .collect();
    assert!(in_commit.contains("commit_snapshots"), "{in_commit:?}");
    assert!(in_commit.contains("set_cursor"), "{in_commit:?}");

    // A flush stages and claims, and says how much and how far.
    let flush = named("pipeline.flush")
        .find(|f| parent(f).is_none() && field(f, "rows").is_some_and(|r| r != "0"))
        .expect("a flush of the changes");
    assert!(field(flush, "lsn").is_some());
    let ops_in = |unit: &RecordedSpan| -> BTreeSet<(String, String)> {
        requests
            .iter()
            .filter(|r| r.parent.is_some_and(|p| std::ptr::eq(&spans[p], unit)))
            .map(|r| (field(r, "store").unwrap(), field(r, "op").unwrap()))
            .collect()
    };
    let flushed = ops_in(flush);
    assert!(
        flushed.contains(&("object_store".into(), "put".into())),
        "{flushed:?}"
    );
    assert!(
        flushed.contains(&("coordinator".into(), "claim_offsets".into())),
        "{flushed:?}"
    );
    // The big transaction staged in chunks as it arrived.
    let spill = named("pipeline.spill").next().expect("a spill");
    assert!(ops_in(spill).contains(&("object_store".into(), "put".into())));

    // The snapshot: a trace per chunk, the last one finding nothing.
    let chunk_rows: Vec<String> = named("snapshot.chunk")
        .filter_map(|c| field(c, "rows"))
        .collect();
    assert_eq!(chunk_rows, ["4", "0"]);

    // The failed stamp, and the ack it was part of.
    let failed = requests
        .iter()
        .find(|r| field(r, "otel.status_description").is_some())
        .expect("the failed request");
    assert_eq!(field(failed, "op").as_deref(), Some("set_flushed_lsn"));
    assert_eq!(parent(failed).map(|p| p.name), Some("slot.ack"));
    // Exported under the store and the operation.
    assert_eq!(
        field(failed, "otel.name").as_deref(),
        Some("coordinator.set_flushed_lsn")
    );
}
