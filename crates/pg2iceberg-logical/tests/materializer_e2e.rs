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
    ColumnName, ColumnSchema, Namespace, PgValue, Row, TableIdent, TableSchema, Timestamp,
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
        partition_spec: Vec::new(),
        pg_schema: None,
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
    // Past the commit: to the WAL end the caught-up keepalive reported.
    assert!(h.pipeline.flushed_lsn() > commit);

    // Cursor is still -1 — the materializer hasn't run yet.
    let cur_before = block_on(h.coord.get_cursor("default", &ident())).unwrap();
    assert_eq!(cur_before, Some(-1));

    run_materializer(&mut h);

    // Cursor now points past the only log entry: the table's columns
    // and the row.
    let cur_after = block_on(h.coord.get_cursor("default", &ident())).unwrap();
    assert_eq!(cur_after, Some(2));
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

// ── compaction wiring ──────────────────────────────────────────────────

#[test]
fn compact_cycle_merges_small_files_after_many_materialize_cycles() {
    let mut h = boot();
    // 5 separate transactions → 5 separate snapshots after materialize.
    for i in 1..=5 {
        let mut tx = h.db.begin_tx();
        tx.insert(&ident(), row(i, i * 10));
        tx.commit(Timestamp(0)).unwrap();
        drive_pipeline(&mut h);
        run_materializer(&mut h);
    }
    let snaps_before = block_on(h.catalog.snapshots(&ident())).unwrap();
    assert_eq!(snaps_before.len(), 5, "5 materialize cycles → 5 snapshots");

    let cfg = pg2iceberg_iceberg::CompactionConfig {
        data_file_threshold: 3,
        delete_file_threshold: 1,
        target_size_bytes: 1024 * 1024 * 1024,
        ..Default::default()
    };
    let outcomes = block_on(h.materializer.compact_cycle(&cfg)).unwrap();
    assert_eq!(outcomes.len(), 1, "one table compacted");
    let (_ident, outcome) = &outcomes[0];
    assert_eq!(outcome.input_data_files, 5);
    assert_eq!(outcome.output_data_files, 1);
    assert_eq!(outcome.rows_rewritten, 5);

    // Iceberg state still matches PG ground truth post-compaction.
    assert_eq!(read_iceberg(&h), read_pg(&h));
}

#[test]
fn compact_cycle_below_threshold_returns_empty() {
    let mut h = boot();
    let mut tx = h.db.begin_tx();
    tx.insert(&ident(), row(1, 10));
    tx.commit(Timestamp(0)).unwrap();
    drive_pipeline(&mut h);
    run_materializer(&mut h);

    let cfg = pg2iceberg_iceberg::CompactionConfig {
        data_file_threshold: 8,
        delete_file_threshold: 4,
        target_size_bytes: 1024 * 1024 * 1024,
        ..Default::default()
    };
    let outcomes = block_on(h.materializer.compact_cycle(&cfg)).unwrap();
    assert!(outcomes.is_empty(), "below threshold: nothing compacted");
}

#[test]
fn compact_cycle_then_subsequent_materialize_cycle_handles_new_inserts() {
    // After compaction, the materializer's FileIndex is rebuilt. A fresh
    // re-insert of an already-compacted PK must trigger
    // promote_re_inserts so we emit an equality delete on the
    // post-compaction file. End-to-end correctness through the
    // FileIndex rebuild seam.
    let mut h = boot();
    for i in 1..=4 {
        let mut tx = h.db.begin_tx();
        tx.insert(&ident(), row(i, i * 10));
        tx.commit(Timestamp(0)).unwrap();
        drive_pipeline(&mut h);
        run_materializer(&mut h);
    }
    let cfg = pg2iceberg_iceberg::CompactionConfig {
        data_file_threshold: 3,
        delete_file_threshold: 1,
        target_size_bytes: 1024 * 1024 * 1024,
        ..Default::default()
    };
    block_on(h.materializer.compact_cycle(&cfg)).unwrap();

    // Re-insert PK 1 with a new value. PG ground truth replaces the row.
    let mut tx = h.db.begin_tx();
    tx.update(&ident(), row(1, 999));
    tx.commit(Timestamp(0)).unwrap();
    drive_pipeline(&mut h);
    run_materializer(&mut h);

    // Iceberg should match PG: PK 1 → qty 999, others unchanged.
    assert_eq!(read_iceberg(&h), read_pg(&h));
}

/// What Postgres's catalog says of the table's columns: `note` was added
/// with a constant default, stored for the rows that predate it.
struct NoteDefault;

#[async_trait::async_trait]
impl pg2iceberg_pg::ColumnDefaultSource for NoteDefault {
    async fn column_defaults(
        &self,
        _rel_id: u32,
    ) -> pg2iceberg_pg::Result<Vec<pg2iceberg_pg::ColumnDefault>> {
        let column = |name: &str, stored: Option<&str>| pg2iceberg_pg::ColumnDefault {
            name: name.into(),
            stored: stored.map(|v| PgValue::Text(v.into())),
            has_default: stored.is_some(),
        };
        Ok(vec![
            column("id", None),
            column("qty", None),
            column("note", Some("x")),
            column("flag", None),
        ])
    }
}

/// `BEGIN; INSERT 4; ALTER TABLE ADD note DEFAULT 'x'; UPDATE 1 SET note
/// = 'y'; ALTER TABLE ADD flag; INSERT 3; COMMIT`, as pgoutput sends it
/// (the sim runs no DDL inside a transaction). Rows that predate `note`
/// read the default — row 4 too, written before the ALTER — and row 1
/// keeps the value it got after it: the second schema change in the same
/// transaction doesn't fill `note` in again.
#[test]
fn a_default_added_mid_transaction_is_filled_once() {
    use pg2iceberg_core::{ChangeEvent, Lsn, Op};
    use pg2iceberg_pg::{DecodedMessage, RelationColumn};

    let mut h = boot();
    h.pipeline.read_column_defaults(Arc::new(NoteDefault));
    let relation = |extra: &[&str]| DecodedMessage::Relation {
        rel_id: 16384,
        ident: ident(),
        columns: [
            ("id", IcebergType::Int, true),
            ("qty", IcebergType::Int, false),
        ]
        .into_iter()
        .chain(extra.iter().map(|c| (*c, IcebergType::String, false)))
        .map(|(name, ty, key)| RelationColumn {
            name: name.into(),
            ty,
            is_primary_key: key,
            nullable: !key,
        })
        .collect(),
    };
    let change = |op: Op, xid: u32, lsn: u64, values: &[(&str, PgValue)]| {
        DecodedMessage::Change(ChangeEvent {
            table: ident(),
            op,
            lsn: Lsn(lsn),
            commit_ts: Timestamp(0),
            xid: Some(xid),
            before: None,
            after: Some(values.iter().map(|(c, v)| (col(c), v.clone())).collect()),
            unchanged_cols: Vec::new(),
        })
    };
    let (int, text) = (PgValue::Int4, |s: &str| PgValue::Text(s.into()));
    let tx1 = vec![
        DecodedMessage::Begin {
            final_lsn: Lsn(13),
            xid: 1,
        },
        relation(&[]),
        change(Op::Insert, 1, 11, &[("id", int(1)), ("qty", int(10))]),
        change(Op::Insert, 1, 12, &[("id", int(2)), ("qty", int(20))]),
        DecodedMessage::Commit {
            commit_lsn: Lsn(13),
            xid: 1,
        },
    ];
    let tx2 = vec![
        DecodedMessage::Begin {
            final_lsn: Lsn(25),
            xid: 2,
        },
        change(Op::Insert, 2, 21, &[("id", int(4)), ("qty", int(40))]),
        relation(&["note"]),
        change(
            Op::Update,
            2,
            22,
            &[("id", int(1)), ("qty", int(11)), ("note", text("y"))],
        ),
        relation(&["note", "flag"]),
        change(
            Op::Insert,
            2,
            23,
            &[
                ("id", int(3)),
                ("qty", int(30)),
                ("note", text("x")),
                ("flag", text("f")),
            ],
        ),
        DecodedMessage::Commit {
            commit_lsn: Lsn(25),
            xid: 2,
        },
    ];
    for tx in [tx1, tx2] {
        for msg in tx {
            block_on(h.pipeline.process(msg)).unwrap();
        }
        block_on(h.pipeline.flush()).unwrap();
        run_materializer(&mut h);
    }

    let schema = block_on(h.catalog.load_table(&ident()))
        .unwrap()
        .unwrap()
        .schema;
    let rows = block_on(read_materialized_state(
        h.catalog.as_ref(),
        h.blob_store.as_ref(),
        &ident(),
        &schema,
        &[col("id")],
    ))
    .unwrap();
    let notes: BTreeMap<i32, (PgValue, PgValue)> = rows
        .iter()
        .map(|r| {
            let id = match r[&col("id")] {
                PgValue::Int4(n) => n,
                ref v => panic!("id {v:?}"),
            };
            (id, (r[&col("note")].clone(), r[&col("flag")].clone()))
        })
        .collect();
    assert_eq!(
        notes,
        BTreeMap::from([
            (1, (text("y"), PgValue::Null)),
            (2, (text("x"), PgValue::Null)),
            (3, (text("x"), text("f"))),
            (4, (text("x"), PgValue::Null)),
        ])
    );
    // The fill's mark is gone with its commit.
    let meta = block_on(h.catalog.load_table(&ident())).unwrap().unwrap();
    assert!(
        !meta
            .properties
            .keys()
            .any(|k| k.starts_with("pg2iceberg.fill.")),
        "{:?}",
        meta.properties
    );
}
