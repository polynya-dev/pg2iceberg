//! Tier-1 DST coverage: DML scenarios that today silently diverge
//! between PG and Iceberg.
//!
//! Each scenario follows the same pattern:
//!   1. Seed PG via the sim with N rows.
//!   2. Apply some DML through the sim that real PG would replicate.
//!   3. Drive the lifecycle (decode WAL → stage → flush → materialize).
//!   4. Assert PG ground truth equals Iceberg materialized state.
//!
//! These tests exist to **lock in correctness** before we wire fixes.
//! When a test fails today, that's the bug; once the fix lands, the
//! test goes green and stays green.

use std::sync::Arc;

use bytes::Bytes;
use pg2iceberg_coord::schema::CoordSchema;
use pg2iceberg_coord::Coordinator;
use pg2iceberg_core::{
    ColumnName, ColumnSchema, IcebergType, Lsn, Namespace, Op, PgValue, Row, TableIdent,
    TableSchema, Timestamp,
};
use pg2iceberg_iceberg::read_materialized_state;
use pg2iceberg_logical::materializer::{CounterMaterializerNamer, Materializer};
use pg2iceberg_logical::pipeline::{CounterBlobNamer, Pipeline};
use pg2iceberg_pg::DecodedMessage;
use pg2iceberg_sim::blob::MemoryBlobStore;
use pg2iceberg_sim::catalog::MemoryCatalog;
use pg2iceberg_sim::clock::TestClock;
use pg2iceberg_sim::coord::MemoryCoordinator;
use pg2iceberg_sim::postgres::{SimPostgres, SimReplicationStream};
use pg2iceberg_stream::BlobStore;
use pollster::block_on;
use std::collections::BTreeMap;

const SLOT: &str = "p2i-slot";
const PUB: &str = "p2i-pub";

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
        partition_spec: vec![],
    }
}

fn row(id: i32, qty: i32) -> Row {
    let mut r = BTreeMap::new();
    r.insert(ColumnName("id".into()), PgValue::Int4(id));
    r.insert(ColumnName("qty".into()), PgValue::Int4(qty));
    r
}

struct Harness {
    db: SimPostgres,
    coord: Arc<MemoryCoordinator>,
    blob: Arc<MemoryBlobStore>,
    catalog: Arc<MemoryCatalog>,
    pipeline: Pipeline<MemoryCoordinator>,
    materializer: Materializer<MemoryCatalog>,
    stream: SimReplicationStream,
}

impl Harness {
    fn boot() -> Self {
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
        let blob = Arc::new(MemoryBlobStore::new());
        let catalog = Arc::new(MemoryCatalog::new());

        let mut pipeline = Pipeline::new(
            coord.clone(),
            blob.clone(),
            Arc::new(CounterBlobNamer::new("s3://stage")),
            64,
        );
        // Match what the lifecycle does — without PK registration,
        // UPDATE-with-PK-change splitting is silently disabled.
        pipeline.register_primary_keys(ident(), vec![ColumnName("id".into())]);
        let mut materializer = Materializer::new(
            coord.clone() as Arc<dyn Coordinator>,
            blob.clone(),
            catalog.clone(),
            Arc::new(CounterMaterializerNamer::new("s3://table")),
            "default",
            128,
        );
        block_on(materializer.register_table(schema())).unwrap();
        let stream = db.start_replication(SLOT).unwrap();

        Self {
            db,
            coord,
            blob,
            catalog,
            pipeline,
            materializer,
            stream,
        }
    }

    fn drive_then_materialize(&mut self) {
        // Drain replication stream → pipeline → flush → materialize.
        // SimReplicationStream::recv is sync and returns None when
        // there are no more events to deliver. Mirrors the
        // lifecycle's main loop: Relation messages drive schema
        // evolution via `materializer.apply_relation` BEFORE the
        // pipeline forwards them onward.
        while let Some(msg) = self.stream.recv() {
            if let DecodedMessage::Relation { ident, columns } = &msg {
                block_on(self.materializer.apply_relation(ident, columns)).unwrap();
            }
            block_on(self.pipeline.process(msg)).unwrap();
        }
        block_on(self.pipeline.flush()).unwrap();
        block_on(self.materializer.cycle()).unwrap();
    }

    fn iceberg_rows(&self) -> Vec<Row> {
        let mut rows = block_on(read_materialized_state(
            self.catalog.as_ref(),
            self.blob.as_ref(),
            &ident(),
            &schema(),
            &[ColumnName("id".into())],
        ))
        .unwrap();
        rows.sort_by_key(|r| match r.get(&ColumnName("id".into())) {
            Some(PgValue::Int4(v)) => *v,
            _ => 0,
        });
        rows
    }

    fn pg_rows(&self) -> Vec<Row> {
        let mut rows = self.db.read_table(&ident()).unwrap();
        rows.sort_by_key(|r| match r.get(&ColumnName("id".into())) {
            Some(PgValue::Int4(v)) => *v,
            _ => 0,
        });
        rows
    }
}

// ── 1. TRUNCATE ───────────────────────────────────────────────────

#[test]
fn truncate_in_pg_propagates_to_iceberg_as_full_delete() {
    // Seed: 5 rows, materialize. Then TRUNCATE. After re-materialize,
    // Iceberg must show zero rows. Today this fails — pgoutput's
    // Truncate event is silently dropped at the staging layer, so
    // Iceberg keeps every seed row.
    let mut h = Harness::boot();

    let mut tx = h.db.begin_tx();
    for i in 1..=5 {
        tx.insert(&ident(), row(i, i * 10));
    }
    tx.commit(Timestamp(0)).unwrap();
    h.drive_then_materialize();
    assert_eq!(h.iceberg_rows().len(), 5, "seed materialized");

    // Operator runs TRUNCATE.
    let mut tx = h.db.begin_tx();
    tx.truncate(&ident());
    tx.commit(Timestamp(0)).unwrap();
    h.drive_then_materialize();

    // Critical assertion: Iceberg state matches PG (both empty).
    let pg = h.pg_rows();
    let ice = h.iceberg_rows();
    assert!(pg.is_empty(), "PG should be empty after TRUNCATE");
    assert_eq!(
        ice, pg,
        "Iceberg must match PG after TRUNCATE (got Iceberg={ice:?})"
    );
}

#[test]
fn insert_after_truncate_in_same_tx_keeps_post_truncate_rows() {
    // Stronger correctness check: a tx that does TRUNCATE + INSERT
    // produces "table contains only the post-truncate rows" — same
    // as PG itself.
    let mut h = Harness::boot();

    let mut tx = h.db.begin_tx();
    for i in 1..=3 {
        tx.insert(&ident(), row(i, i * 10));
    }
    tx.commit(Timestamp(0)).unwrap();
    h.drive_then_materialize();

    let mut tx = h.db.begin_tx();
    tx.truncate(&ident());
    tx.insert(&ident(), row(99, 990));
    tx.commit(Timestamp(0)).unwrap();
    h.drive_then_materialize();

    let pg = h.pg_rows();
    let ice = h.iceberg_rows();
    assert_eq!(pg.len(), 1, "PG should have only the post-truncate row");
    assert_eq!(ice, pg, "Iceberg must match PG");
}

// ── 2. UPDATE that changes the primary key ────────────────────────

#[test]
fn update_with_pk_change_deletes_old_pk_in_iceberg() {
    // PG accepts `UPDATE t SET id = 99 WHERE id = 1`. With REPLICA
    // IDENTITY FULL, pgoutput emits an Update with before={id=1}
    // and after={id=99}. After materialize, Iceberg must contain
    // only the row at id=99, NOT both id=1 and id=99. Today this
    // fails — the materializer's fold-by-PK keys on `after`'s PK
    // and never emits a delete for the old PK.
    let mut h = Harness::boot();

    let mut tx = h.db.begin_tx();
    tx.insert(&ident(), row(1, 10));
    tx.insert(&ident(), row(2, 20));
    tx.commit(Timestamp(0)).unwrap();
    h.drive_then_materialize();
    assert_eq!(h.iceberg_rows().len(), 2);

    // UPDATE id=1 → id=99 (PK change). Real PG with REPLICA IDENTITY
    // FULL sends the full old row in `before`.
    let mut tx = h.db.begin_tx();
    tx.update_with_pk_change(&ident(), row(1, 10), row(99, 999));
    tx.commit(Timestamp(0)).unwrap();
    h.drive_then_materialize();

    let pg = h.pg_rows();
    let ice = h.iceberg_rows();
    assert_eq!(pg.len(), 2, "PG: id=2 + id=99");
    assert_eq!(
        ice, pg,
        "Iceberg must match PG after PK change \
         (old PK 1 should be deleted, new PK 99 should be inserted; got Iceberg={ice:?})"
    );
}

// ── 3. ALTER TABLE ADD COLUMN mid-stream ──────────────────────────

#[test]
fn alter_table_add_column_mid_stream_propagates_to_iceberg() {
    // Operator runs `ALTER TABLE orders ADD COLUMN note TEXT`,
    // followed by an INSERT carrying the new column.
    // Goes through `materializer.apply_relation` →
    // `Catalog::evolve_schema` → updated `entry.schema` + writer
    // rebuild. The post-evolve INSERT must persist the new
    // column's value in Iceberg.
    let mut h = Harness::boot();

    // Seed pre-evolution data.
    let mut tx = h.db.begin_tx();
    tx.insert(&ident(), row(1, 10));
    tx.insert(&ident(), row(2, 20));
    tx.commit(Timestamp(0)).unwrap();
    h.drive_then_materialize();

    // ALTER TABLE ADD COLUMN note TEXT (nullable).
    h.db
        .alter_add_column(
            &ident(),
            ColumnSchema {
                name: "note".into(),
                field_id: 0, // sim re-allocates monotonically
                ty: IcebergType::String,
                nullable: true,
                is_primary_key: false,
            },
        )
        .unwrap();

    // INSERT a row that uses the new column.
    let mut tx = h.db.begin_tx();
    let mut row3 = row(3, 30);
    row3.insert(
        ColumnName("note".into()),
        PgValue::Text("hello".into()),
    );
    tx.insert(&ident(), row3);
    tx.commit(Timestamp(0)).unwrap();
    h.drive_then_materialize();

    // Iceberg-side schema should now have `note`. Build a schema
    // matching the post-evolution shape so `read_materialized_state`
    // pulls every column the new data files carry.
    let new_schema = TableSchema {
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
                name: "note".into(),
                field_id: 3,
                ty: IcebergType::String,
                nullable: true,
                is_primary_key: false,
            },
        ],
        partition_spec: vec![],
    };
    let mut visible = block_on(read_materialized_state(
        h.catalog.as_ref(),
        h.blob.as_ref(),
        &ident(),
        &new_schema,
        &[ColumnName("id".into())],
    ))
    .unwrap();
    visible.sort_by_key(|r| match r.get(&ColumnName("id".into())) {
        Some(PgValue::Int4(v)) => *v,
        _ => 0,
    });

    assert_eq!(visible.len(), 3, "all 3 rows should materialize");
    // Pre-evolution rows: `note` absent (or null).
    let r1 = &visible[0];
    assert_eq!(r1.get(&ColumnName("id".into())), Some(&PgValue::Int4(1)));
    // The new column on row 3 must carry "hello".
    let r3 = &visible[2];
    assert_eq!(r3.get(&ColumnName("id".into())), Some(&PgValue::Int4(3)));
    assert_eq!(
        r3.get(&ColumnName("note".into())),
        Some(&PgValue::Text("hello".into())),
        "new column value must round-trip through Iceberg"
    );
}

#[test]
fn alter_table_drop_column_is_soft_drop() {
    // DROP COLUMN doesn't physically remove data — Iceberg
    // soft-drops by keeping the column nullable. Pre-drop rows
    // still have the column populated; post-drop rows write null
    // (or the column is just absent from new data files).
    let mut h = Harness::boot();

    let mut tx = h.db.begin_tx();
    tx.insert(&ident(), row(1, 10));
    tx.commit(Timestamp(0)).unwrap();
    h.drive_then_materialize();

    h.db.alter_drop_column(&ident(), "qty").unwrap();

    // Insert a row WITHOUT the dropped column.
    let mut row_after = BTreeMap::new();
    row_after.insert(ColumnName("id".into()), PgValue::Int4(2));
    let mut tx = h.db.begin_tx();
    tx.insert(&ident(), row_after);
    tx.commit(Timestamp(0)).unwrap();
    h.drive_then_materialize();

    // Iceberg side: schema still has `qty` (soft-drop), nullable=true.
    // Read with the original schema — pre-drop row keeps qty=10,
    // post-drop row has qty=null.
    let visible = block_on(read_materialized_state(
        h.catalog.as_ref(),
        h.blob.as_ref(),
        &ident(),
        &schema(),
        &[ColumnName("id".into())],
    ))
    .unwrap();
    assert_eq!(visible.len(), 2);
}
