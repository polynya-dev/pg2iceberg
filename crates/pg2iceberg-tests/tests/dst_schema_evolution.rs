//! DST coverage for the full surface of schema-evolution scenarios that
//! pgoutput can produce. Each test seeds a sim PG, drives ALTERs through
//! `materializer.apply_relation`, and asserts the Iceberg-side schema /
//! data state.
//!
//! Companion to `dst_dml_correctness.rs` (which covers DML correctness,
//! including the basic ADD/DROP COLUMN happy paths). This file focuses
//! on the *less obvious* evolution cases: type promotion, illegal type
//! changes, sequential evolution, multi-column ALTERs, multi-table
//! evolution, and the no-op replay of identical Relation messages.

use std::sync::Arc;

use pg2iceberg_coord::schema::CoordSchema;
use pg2iceberg_coord::Coordinator;
use pg2iceberg_core::{
    ColumnName, ColumnSchema, IcebergType, Namespace, PgValue, Row, TableIdent, TableSchema,
    Timestamp,
};
use pg2iceberg_iceberg::{read_materialized_state, Catalog};
use pg2iceberg_logical::materializer::{CounterMaterializerNamer, Materializer};
use pg2iceberg_logical::pipeline::{CounterBlobNamer, Pipeline};
use pg2iceberg_logical::MaterializerError;
use pg2iceberg_pg::DecodedMessage;
use pg2iceberg_sim::blob::MemoryBlobStore;
use pg2iceberg_sim::catalog::MemoryCatalog;
use pg2iceberg_sim::clock::TestClock;
use pg2iceberg_sim::coord::MemoryCoordinator;
use pg2iceberg_sim::postgres::{SimPostgres, SimReplicationStream};
use pollster::block_on;
use std::collections::BTreeMap;

const SLOT: &str = "p2i-slot";
const PUB: &str = "p2i-pub";

fn ns() -> Namespace {
    Namespace(vec!["public".into()])
}

fn ident_named(name: &str) -> TableIdent {
    TableIdent {
        namespace: ns(),
        name: name.into(),
    }
}

fn col(name: &str) -> ColumnName {
    ColumnName(name.into())
}

/// Schema with `id INT PK` + a non-PK column of caller-chosen type.
/// Field ids 1, 2 — matching the Iceberg "all field ids unique &
/// monotonically allocated" rule that downstream evolution depends on.
fn schema_with(table: &str, non_pk_name: &str, non_pk_ty: IcebergType) -> TableSchema {
    TableSchema {
        ident: ident_named(table),
        columns: vec![
            ColumnSchema {
                name: "id".into(),
                field_id: 1,
                ty: IcebergType::Int,
                nullable: false,
                is_primary_key: true,
            },
            ColumnSchema {
                name: non_pk_name.into(),
                field_id: 2,
                ty: non_pk_ty,
                nullable: false,
                is_primary_key: false,
            },
        ],
        partition_spec: vec![],
    }
}

struct Harness {
    db: SimPostgres,
    blob: Arc<MemoryBlobStore>,
    catalog: Arc<MemoryCatalog>,
    pipeline: Pipeline<MemoryCoordinator>,
    materializer: Materializer<MemoryCatalog>,
    stream: SimReplicationStream,
}

impl Harness {
    /// Boot the full lifecycle stack with one or more registered tables.
    /// Each schema is created in the sim PG, registered in the
    /// publication, and registered with the materializer. The
    /// replication stream is opened on a single shared slot.
    fn boot(schemas: &[TableSchema]) -> Self {
        let db = SimPostgres::new();
        let mut idents: Vec<TableIdent> = Vec::with_capacity(schemas.len());
        for s in schemas {
            db.create_table(s.clone()).unwrap();
            idents.push(s.ident.clone());
        }
        db.create_publication(PUB, &idents).unwrap();
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
        for s in schemas {
            pipeline.register_primary_keys(
                s.ident.clone(),
                s.primary_key_columns()
                    .map(|c| ColumnName(c.name.clone()))
                    .collect(),
            );
        }

        let mut materializer = Materializer::new(
            coord.clone() as Arc<dyn Coordinator>,
            blob.clone(),
            catalog.clone(),
            Arc::new(CounterMaterializerNamer::new("s3://table")),
            "default",
            128,
        );
        for s in schemas {
            block_on(materializer.register_table(s.clone())).unwrap();
        }

        let stream = db.start_replication(SLOT).unwrap();

        Self {
            db,
            blob,
            catalog,
            pipeline,
            materializer,
            stream,
        }
    }

    /// Drain the replication stream and run a materialize cycle,
    /// asserting every step succeeded. Use this for the happy path.
    fn drive_then_materialize(&mut self) {
        while let Some(msg) = self.stream.recv() {
            if let DecodedMessage::Relation { ident, columns } = &msg {
                block_on(self.materializer.apply_relation(ident, columns)).unwrap();
            }
            block_on(self.pipeline.process(msg)).unwrap();
        }
        block_on(self.pipeline.flush()).unwrap();
        block_on(self.materializer.cycle()).unwrap();
    }

    /// Drain the replication stream, but capture the first
    /// `apply_relation` error and return it instead of panicking.
    /// Used by the illegal-type-change tests to assert that the
    /// lifecycle fails loudly rather than silently coercing.
    fn drive_capturing_relation_error(&mut self) -> Option<MaterializerError> {
        while let Some(msg) = self.stream.recv() {
            if let DecodedMessage::Relation { ident, columns } = &msg {
                if let Err(e) = block_on(self.materializer.apply_relation(ident, columns)) {
                    return Some(e);
                }
            }
            block_on(self.pipeline.process(msg)).unwrap();
        }
        None
    }

    fn iceberg_schema(&self, ident: &TableIdent) -> TableSchema {
        let meta = block_on(self.catalog.load_table(ident)).unwrap().unwrap();
        meta.schema
    }
}

// ── Type promotion: legal Iceberg-spec evolutions ────────────────────

#[test]
fn type_promotion_int_to_long_succeeds() {
    // ALTER TABLE orders ALTER COLUMN qty TYPE BIGINT.
    // Iceberg spec allows int → long; the materializer should detect
    // the type change in the next Relation, emit
    // `PromoteColumnType`, and post-promotion data should encode as
    // Long. We avoid mixing pre-promotion (Int32 parquet) with
    // post-promotion (Int64 parquet) here so the read path doesn't
    // hit the "old data file under new schema" downcast issue —
    // that's compaction's job to fix.
    let s = schema_with("orders", "qty", IcebergType::Int);
    let mut h = Harness::boot(&[s.clone()]);

    h.db
        .alter_column_type(&s.ident, "qty", IcebergType::Long)
        .unwrap();

    let mut tx = h.db.begin_tx();
    let mut row = BTreeMap::new();
    row.insert(col("id"), PgValue::Int4(1));
    row.insert(col("qty"), PgValue::Int8(9_000_000_000));
    tx.insert(&s.ident, row);
    tx.commit(Timestamp(0)).unwrap();
    h.drive_then_materialize();

    // Iceberg metadata should reflect the new type, with field id
    // preserved (Iceberg requires field ids to stay stable across
    // promotions so older snapshots still resolve).
    let evolved = h.iceberg_schema(&s.ident);
    let qty = evolved.columns.iter().find(|c| c.name == "qty").unwrap();
    assert_eq!(qty.ty, IcebergType::Long, "promoted to Long");
    assert_eq!(qty.field_id, 2, "field id preserved across promotion");

    // Round-trip the post-promotion row through the reader.
    let rows = block_on(read_materialized_state(
        h.catalog.as_ref(),
        h.blob.as_ref(),
        &s.ident,
        &evolved,
        &[col("id")],
    ))
    .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get(&col("qty")), Some(&PgValue::Int8(9_000_000_000)));
}

#[test]
fn type_promotion_float_to_double_succeeds() {
    // ALTER TABLE … ALTER COLUMN price TYPE DOUBLE PRECISION.
    let s = schema_with("prices", "price", IcebergType::Float);
    let mut h = Harness::boot(&[s.clone()]);

    h.db
        .alter_column_type(&s.ident, "price", IcebergType::Double)
        .unwrap();

    let mut tx = h.db.begin_tx();
    let mut row = BTreeMap::new();
    row.insert(col("id"), PgValue::Int4(1));
    row.insert(col("price"), PgValue::Float8(3.141_592_653_589_793));
    tx.insert(&s.ident, row);
    tx.commit(Timestamp(0)).unwrap();
    h.drive_then_materialize();

    let evolved = h.iceberg_schema(&s.ident);
    let price = evolved.columns.iter().find(|c| c.name == "price").unwrap();
    assert_eq!(price.ty, IcebergType::Double);
    assert_eq!(price.field_id, 2);
}

// ── Type promotion: illegal evolutions must fail loudly ──────────────

#[test]
fn type_narrowing_long_to_int_fails_loudly() {
    // ALTER … ALTER COLUMN qty TYPE INTEGER (narrowing). Iceberg
    // doesn't allow this — values that fit in Long but not Int would
    // be silently truncated. The materializer must reject the
    // Relation with a clear error rather than coerce.
    let s = schema_with("orders", "qty", IcebergType::Long);
    let mut h = Harness::boot(&[s.clone()]);

    h.db
        .alter_column_type(&s.ident, "qty", IcebergType::Int)
        .unwrap();

    let err = h
        .drive_capturing_relation_error()
        .expect("narrowing must produce an error");
    let msg = format!("{err}");
    assert!(
        msg.contains("not a legal Iceberg")
            || msg.contains("re-snapshot")
            || msg.contains("promotion"),
        "error must explain that the change is illegal; got: {msg}"
    );

    // Schema must remain unchanged on the Iceberg side.
    let still_long = h.iceberg_schema(&s.ident);
    let qty = still_long.columns.iter().find(|c| c.name == "qty").unwrap();
    assert_eq!(qty.ty, IcebergType::Long, "schema unchanged after rejection");
}

#[test]
fn type_change_cross_family_text_to_int_fails_loudly() {
    // ALTER … ALTER COLUMN note TYPE INTEGER USING note::int.
    // Cross-family conversions corrupt downstream readers if applied
    // silently — must reject.
    let s = schema_with("notes", "note", IcebergType::String);
    let mut h = Harness::boot(&[s.clone()]);

    h.db
        .alter_column_type(&s.ident, "note", IcebergType::Int)
        .unwrap();

    let err = h
        .drive_capturing_relation_error()
        .expect("cross-family change must produce an error");
    let msg = format!("{err}");
    assert!(msg.contains("not a legal") || msg.contains("re-snapshot"));
}

#[test]
fn pk_type_change_rejected_even_for_legal_promotion() {
    // PK columns participate in equality-delete pk_key hashes;
    // promoting their type would invalidate every prior delete file.
    // Even an otherwise-legal int→long is rejected on the PK to
    // force operators to re-snapshot.
    let s = schema_with("orders", "qty", IcebergType::Int);
    let mut h = Harness::boot(&[s.clone()]);

    h.db
        .alter_column_type(&s.ident, "id", IcebergType::Long)
        .unwrap();

    let err = h
        .drive_capturing_relation_error()
        .expect("PK type change must produce an error");
    let msg = format!("{err}");
    assert!(
        msg.contains("primary-key") || msg.contains("equality-delete"),
        "error must call out the PK constraint; got: {msg}"
    );
}

// ── Multi-column ALTER + sequential evolution ────────────────────────

#[test]
fn single_relation_with_multiple_new_columns_adds_all() {
    // PG can run `ALTER TABLE … ADD COLUMN a TEXT, ADD COLUMN b INT`
    // in one statement, producing a single Relation message that
    // adds two columns at once. apply_relation must emit two
    // AddColumn changes and apply them atomically (both succeed or
    // neither does).
    let s = schema_with("orders", "qty", IcebergType::Int);
    let mut h = Harness::boot(&[s.clone()]);

    // Drain the initial Relation (from create_table).
    h.drive_then_materialize();

    // Two ALTERs back-to-back — sim emits one Relation per call,
    // but the materializer's diff is the same shape as a real
    // multi-column-add Relation: multiple new names compared to
    // current schema. We verify both end up in the Iceberg schema.
    h.db
        .alter_add_column(
            &s.ident,
            ColumnSchema {
                name: "note".into(),
                field_id: 0,
                ty: IcebergType::String,
                nullable: true,
                is_primary_key: false,
            },
        )
        .unwrap();
    h.db
        .alter_add_column(
            &s.ident,
            ColumnSchema {
                name: "tag".into(),
                field_id: 0,
                ty: IcebergType::String,
                nullable: true,
                is_primary_key: false,
            },
        )
        .unwrap();

    h.drive_then_materialize();

    let evolved = h.iceberg_schema(&s.ident);
    let names: Vec<&str> = evolved.columns.iter().map(|c| c.name.as_str()).collect();
    assert!(names.contains(&"note"));
    assert!(names.contains(&"tag"));
    let note_id = evolved.columns.iter().find(|c| c.name == "note").unwrap().field_id;
    let tag_id = evolved.columns.iter().find(|c| c.name == "tag").unwrap().field_id;
    assert_ne!(note_id, tag_id, "field ids must be unique");
    assert!(note_id >= 3 && tag_id >= 3, "post-evolution field ids start above PK + qty");
}

#[test]
fn sequential_evolution_allocates_field_ids_monotonically() {
    // Walk through ADD a → ADD b → DROP a → ADD c and verify field
    // ids are allocated monotonically (Iceberg forbids reuse).
    // Final state: a is soft-dropped (still present, nullable=true,
    // original field id), b is present, c is present with the
    // highest field id.
    let s = schema_with("orders", "qty", IcebergType::Int);
    let mut h = Harness::boot(&[s.clone()]);
    h.drive_then_materialize();

    let add = |name: &str, ty: IcebergType| ColumnSchema {
        name: name.into(),
        field_id: 0,
        ty,
        nullable: true,
        is_primary_key: false,
    };

    h.db.alter_add_column(&s.ident, add("a", IcebergType::Int)).unwrap();
    h.drive_then_materialize();
    let a_id = h.iceberg_schema(&s.ident).columns.iter().find(|c| c.name == "a").unwrap().field_id;

    h.db.alter_add_column(&s.ident, add("b", IcebergType::String)).unwrap();
    h.drive_then_materialize();
    let b_id = h.iceberg_schema(&s.ident).columns.iter().find(|c| c.name == "b").unwrap().field_id;

    h.db.alter_drop_column(&s.ident, "a").unwrap();
    h.drive_then_materialize();
    let after_drop = h.iceberg_schema(&s.ident);
    let a_after = after_drop.columns.iter().find(|c| c.name == "a").unwrap();
    assert!(a_after.nullable, "soft-dropped column becomes nullable");
    assert_eq!(a_after.field_id, a_id, "soft-drop preserves field id");

    h.db.alter_add_column(&s.ident, add("c", IcebergType::Long)).unwrap();
    h.drive_then_materialize();
    let final_schema = h.iceberg_schema(&s.ident);
    let c_id = final_schema.columns.iter().find(|c| c.name == "c").unwrap().field_id;

    // Strict monotonic ordering: a < b < c. (a survived as soft-drop
    // but still has its original id, which must be lower than b's
    // and c's.)
    assert!(a_id < b_id, "b allocated after a");
    assert!(b_id < c_id, "c allocated after b (no reuse of a's id)");
}

// ── ADD then DROP without intermediate INSERT ────────────────────────

#[test]
fn add_then_drop_without_data_leaves_soft_dropped_column() {
    // ALTER ADD COLUMN tmp + ALTER DROP COLUMN tmp without any
    // INSERTs in between. The column should end up in the Iceberg
    // schema as soft-dropped (nullable=true) with no data ever
    // written for it. This mirrors what PG would do: the column
    // existed for a moment, then was removed with no writes.
    let s = schema_with("orders", "qty", IcebergType::Int);
    let mut h = Harness::boot(&[s.clone()]);
    h.drive_then_materialize();

    h.db
        .alter_add_column(
            &s.ident,
            ColumnSchema {
                name: "tmp".into(),
                field_id: 0,
                ty: IcebergType::String,
                nullable: true,
                is_primary_key: false,
            },
        )
        .unwrap();
    h.db.alter_drop_column(&s.ident, "tmp").unwrap();
    h.drive_then_materialize();

    let evolved = h.iceberg_schema(&s.ident);
    let tmp = evolved.columns.iter().find(|c| c.name == "tmp").unwrap();
    assert!(tmp.nullable, "tmp should be soft-dropped to nullable");
    assert!(!tmp.is_primary_key);
}

// ── Multi-table evolution ────────────────────────────────────────────

#[test]
fn schema_evolution_across_two_tables_is_independent() {
    // ALTER ADD COLUMN on table A and ALTER DROP COLUMN on table B
    // interleaved over a single replication stream. Each table's
    // evolution must apply only to its own Iceberg metadata, with
    // no cross-contamination of field ids or column lists.
    let s_a = schema_with("orders", "qty", IcebergType::Int);
    let s_b = schema_with("users", "email", IcebergType::String);
    let mut h = Harness::boot(&[s_a.clone(), s_b.clone()]);
    h.drive_then_materialize();

    h.db
        .alter_add_column(
            &s_a.ident,
            ColumnSchema {
                name: "tax".into(),
                field_id: 0,
                ty: IcebergType::Int,
                nullable: true,
                is_primary_key: false,
            },
        )
        .unwrap();
    h.db.alter_drop_column(&s_b.ident, "email").unwrap();
    h.drive_then_materialize();

    let a = h.iceberg_schema(&s_a.ident);
    let b = h.iceberg_schema(&s_b.ident);

    assert!(a.columns.iter().any(|c| c.name == "tax"), "A got the new column");
    assert!(
        !b.columns.iter().any(|c| c.name == "tax"),
        "B did not get A's new column"
    );
    let email_b = b.columns.iter().find(|c| c.name == "email").unwrap();
    assert!(email_b.nullable, "B's email is soft-dropped");
    let qty_a = a.columns.iter().find(|c| c.name == "qty").unwrap();
    assert!(!qty_a.nullable, "A's qty unaffected by B's drop");
}

// ── Idempotent / no-op replays ───────────────────────────────────────

#[test]
fn repeated_relation_with_same_schema_is_no_op() {
    // pgoutput re-emits Relation messages liberally — every cache
    // invalidation, even when no schema change happened. The
    // materializer's diff must produce zero SchemaChanges in that
    // case, so we don't churn snapshot history with empty schema
    // updates.
    let s = schema_with("orders", "qty", IcebergType::Int);
    let mut h = Harness::boot(&[s.clone()]);
    h.drive_then_materialize();

    let snapshots_before = block_on(h.catalog.snapshots(&s.ident)).unwrap().len();

    // Re-ALTER with a no-op: drop a non-existent column would error,
    // but we can re-emit the same schema by calling
    // alter_column_type with the *current* type — sim emits a
    // Relation but the columns are unchanged so apply_relation
    // produces zero changes.
    h.db
        .alter_column_type(&s.ident, "qty", IcebergType::Int)
        .unwrap();
    h.drive_then_materialize();

    let snapshots_after = block_on(h.catalog.snapshots(&s.ident)).unwrap().len();
    assert_eq!(
        snapshots_before, snapshots_after,
        "no-op Relation must not commit a snapshot"
    );
}

// ── ADD + INSERT in same transactional flow ──────────────────────────

#[test]
fn add_column_then_insert_uses_new_column() {
    // The lifecycle's main loop routes Relation → apply_relation
    // before pipeline.process. So even if a Relation arrives
    // immediately before an Insert (e.g. mid-transaction in real
    // PG), the Iceberg schema is updated in time for the staged
    // Insert to encode the new column.
    let s = schema_with("orders", "qty", IcebergType::Int);
    let mut h = Harness::boot(&[s.clone()]);
    h.drive_then_materialize();

    h.db
        .alter_add_column(
            &s.ident,
            ColumnSchema {
                name: "note".into(),
                field_id: 0,
                ty: IcebergType::String,
                nullable: true,
                is_primary_key: false,
            },
        )
        .unwrap();

    let mut tx = h.db.begin_tx();
    let mut row: Row = BTreeMap::new();
    row.insert(col("id"), PgValue::Int4(1));
    row.insert(col("qty"), PgValue::Int4(10));
    row.insert(col("note"), PgValue::Text("hello".into()));
    tx.insert(&s.ident, row);
    tx.commit(Timestamp(0)).unwrap();
    h.drive_then_materialize();

    let evolved = h.iceberg_schema(&s.ident);
    let rows = block_on(read_materialized_state(
        h.catalog.as_ref(),
        h.blob.as_ref(),
        &s.ident,
        &evolved,
        &[col("id")],
    ))
    .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].get(&col("note")),
        Some(&PgValue::Text("hello".into()))
    );
}
