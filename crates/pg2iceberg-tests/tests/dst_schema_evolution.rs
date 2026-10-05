//! DST coverage for the full surface of schema-evolution scenarios that
//! pgoutput can produce. Each test seeds a sim PG, runs ALTERs, streams
//! the resulting Relation messages through the pipeline (which stages
//! them in the log) and the materializer (which applies them in order),
//! and asserts the Iceberg-side schema / data state.
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
use pg2iceberg_sim::blob::MemoryBlobStore;
use pg2iceberg_sim::catalog::MemoryCatalog;
use pg2iceberg_sim::clock::TestClock;
use pg2iceberg_sim::coord::MemoryCoordinator;
use pg2iceberg_sim::postgres::{SimPostgres, SimReplicationStream};
use pollster::block_on;
use std::collections::{BTreeMap, BTreeSet};

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
        pg_schema: None,
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
            block_on(self.pipeline.process(msg)).unwrap();
        }
        block_on(self.pipeline.flush()).unwrap();
        block_on(self.materializer.cycle()).unwrap();
    }

    /// Drain the replication stream and run a materialize cycle,
    /// returning the cycle's error: an illegal schema change fails the
    /// cycle that reaches it, rather than silently coercing.
    fn drive_capturing_relation_error(&mut self) -> Option<MaterializerError> {
        while let Some(msg) = self.stream.recv() {
            block_on(self.pipeline.process(msg)).unwrap();
        }
        block_on(self.pipeline.flush()).unwrap();
        block_on(self.materializer.cycle()).err()
    }

    /// Insert a row into `ident`. pgoutput sends a table's Relation only
    /// before a change to it, so an ALTER reaches pg2iceberg with the
    /// table's next write, never on its own.
    fn write(&self, ident: &TableIdent, cols: &[(&str, PgValue)]) {
        let mut tx = self.db.begin_tx();
        tx.insert(
            ident,
            cols.iter().map(|(c, v)| (col(c), v.clone())).collect(),
        );
        tx.commit(Timestamp(0)).unwrap();
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
    let mut h = Harness::boot(std::slice::from_ref(&s));

    h.db.alter_column_type(&s.ident, "qty", IcebergType::Long)
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
    assert_eq!(
        rows[0].get(&col("qty")),
        Some(&PgValue::Int8(9_000_000_000))
    );
}

#[test]
fn type_promotion_float_to_double_succeeds() {
    // ALTER TABLE … ALTER COLUMN price TYPE DOUBLE PRECISION.
    let s = schema_with("prices", "price", IcebergType::Float);
    let mut h = Harness::boot(std::slice::from_ref(&s));

    h.db.alter_column_type(&s.ident, "price", IcebergType::Double)
        .unwrap();

    let mut tx = h.db.begin_tx();
    let mut row = BTreeMap::new();
    row.insert(col("id"), PgValue::Int4(1));
    row.insert(col("price"), PgValue::Float8(std::f64::consts::PI));
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
    let mut h = Harness::boot(std::slice::from_ref(&s));

    h.db.alter_column_type(&s.ident, "qty", IcebergType::Int)
        .unwrap();
    h.write(
        &s.ident,
        &[("id", PgValue::Int4(1)), ("qty", PgValue::Int4(1))],
    );

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
    assert_eq!(
        qty.ty,
        IcebergType::Long,
        "schema unchanged after rejection"
    );
}

#[test]
fn type_change_cross_family_text_to_int_fails_loudly() {
    // ALTER … ALTER COLUMN note TYPE INTEGER USING note::int.
    // Cross-family conversions corrupt downstream readers if applied
    // silently — must reject.
    let s = schema_with("notes", "note", IcebergType::String);
    let mut h = Harness::boot(std::slice::from_ref(&s));

    h.db.alter_column_type(&s.ident, "note", IcebergType::Int)
        .unwrap();
    h.write(
        &s.ident,
        &[("id", PgValue::Int4(1)), ("note", PgValue::Int4(1))],
    );

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
    let mut h = Harness::boot(std::slice::from_ref(&s));

    h.db.alter_column_type(&s.ident, "id", IcebergType::Long)
        .unwrap();
    h.write(
        &s.ident,
        &[("id", PgValue::Int8(1)), ("qty", PgValue::Int4(1))],
    );

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
    // adds two columns at once. The materializer must emit two
    // AddColumn changes and apply them atomically (both succeed or
    // neither does).
    let s = schema_with("orders", "qty", IcebergType::Int);
    let mut h = Harness::boot(std::slice::from_ref(&s));

    // Drain the initial Relation (from create_table).
    h.drive_then_materialize();

    // Two ALTERs back-to-back, then a write: like pgoutput, the sim
    // sends one Relation, before the write, carrying both new columns.
    h.db.alter_add_column(
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
    h.db.alter_add_column(
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
    h.write(
        &s.ident,
        &[("id", PgValue::Int4(1)), ("qty", PgValue::Int4(1))],
    );

    h.drive_then_materialize();

    let evolved = h.iceberg_schema(&s.ident);
    let names: Vec<&str> = evolved.columns.iter().map(|c| c.name.as_str()).collect();
    assert!(names.contains(&"note"));
    assert!(names.contains(&"tag"));
    let note_id = evolved
        .columns
        .iter()
        .find(|c| c.name == "note")
        .unwrap()
        .field_id;
    let tag_id = evolved
        .columns
        .iter()
        .find(|c| c.name == "tag")
        .unwrap()
        .field_id;
    assert_ne!(note_id, tag_id, "field ids must be unique");
    assert!(
        note_id >= 3 && tag_id >= 3,
        "post-evolution field ids start above PK + qty"
    );
}

#[test]
fn sequential_evolution_allocates_field_ids_monotonically() {
    // Walk through ADD a → ADD b → DROP a → ADD c and verify field
    // ids are allocated monotonically (Iceberg forbids reuse).
    // Final state: a is soft-dropped (still present, nullable=true,
    // original field id), b is present, c is present with the
    // highest field id.
    let s = schema_with("orders", "qty", IcebergType::Int);
    let mut h = Harness::boot(std::slice::from_ref(&s));
    h.drive_then_materialize();

    let add = |name: &str, ty: IcebergType| ColumnSchema {
        name: name.into(),
        field_id: 0,
        ty,
        nullable: true,
        is_primary_key: false,
    };

    h.db.alter_add_column(&s.ident, add("a", IcebergType::Int))
        .unwrap();
    h.write(
        &s.ident,
        &[("id", PgValue::Int4(1)), ("qty", PgValue::Int4(1))],
    );
    h.drive_then_materialize();
    let a_id = h
        .iceberg_schema(&s.ident)
        .columns
        .iter()
        .find(|c| c.name == "a")
        .unwrap()
        .field_id;

    h.db.alter_add_column(&s.ident, add("b", IcebergType::String))
        .unwrap();
    h.write(
        &s.ident,
        &[("id", PgValue::Int4(2)), ("qty", PgValue::Int4(2))],
    );
    h.drive_then_materialize();
    let b_id = h
        .iceberg_schema(&s.ident)
        .columns
        .iter()
        .find(|c| c.name == "b")
        .unwrap()
        .field_id;

    h.db.alter_drop_column(&s.ident, "a").unwrap();
    h.write(
        &s.ident,
        &[("id", PgValue::Int4(3)), ("qty", PgValue::Int4(3))],
    );
    h.drive_then_materialize();
    let after_drop = h.iceberg_schema(&s.ident);
    let a_after = after_drop.columns.iter().find(|c| c.name == "a").unwrap();
    assert!(a_after.nullable, "soft-dropped column becomes nullable");
    assert_eq!(a_after.field_id, a_id, "soft-drop preserves field id");

    h.db.alter_add_column(&s.ident, add("c", IcebergType::Long))
        .unwrap();
    h.write(
        &s.ident,
        &[("id", PgValue::Int4(4)), ("qty", PgValue::Int4(4))],
    );
    h.drive_then_materialize();
    let final_schema = h.iceberg_schema(&s.ident);
    let c_id = final_schema
        .columns
        .iter()
        .find(|c| c.name == "c")
        .unwrap()
        .field_id;

    // Strict monotonic ordering: a < b < c. (a survived as soft-drop
    // but still has its original id, which must be lower than b's
    // and c's.)
    assert!(a_id < b_id, "b allocated after a");
    assert!(b_id < c_id, "c allocated after b (no reuse of a's id)");
}

// ── ADD then DROP without values ─────────────────────────────────────

fn tmp_column() -> ColumnSchema {
    ColumnSchema {
        name: "tmp".into(),
        field_id: 0,
        ty: IcebergType::String,
        nullable: true,
        is_primary_key: false,
    }
}

#[test]
fn add_then_drop_with_no_values_leaves_soft_dropped_column() {
    // ALTER ADD COLUMN tmp, a write that leaves it NULL, ALTER DROP
    // COLUMN tmp, another write. The write after the ADD brings a
    // Relation with tmp, the one after the DROP a Relation without it:
    // tmp ends up soft-dropped (nullable=true) with no value ever
    // written for it.
    let s = schema_with("orders", "qty", IcebergType::Int);
    let mut h = Harness::boot(std::slice::from_ref(&s));
    h.drive_then_materialize();

    h.db.alter_add_column(&s.ident, tmp_column()).unwrap();
    h.write(
        &s.ident,
        &[("id", PgValue::Int4(1)), ("qty", PgValue::Int4(1))],
    );
    h.drive_then_materialize();
    h.db.alter_drop_column(&s.ident, "tmp").unwrap();
    h.write(
        &s.ident,
        &[("id", PgValue::Int4(2)), ("qty", PgValue::Int4(2))],
    );
    h.drive_then_materialize();

    let evolved = h.iceberg_schema(&s.ident);
    let tmp = evolved.columns.iter().find(|c| c.name == "tmp").unwrap();
    assert!(tmp.nullable, "tmp should be soft-dropped to nullable");
    assert!(!tmp.is_primary_key);
}

#[test]
fn add_then_drop_with_no_write_between_never_reaches_iceberg() {
    // ALTER ADD COLUMN tmp then ALTER DROP COLUMN tmp with no change to
    // the table in between: pgoutput never sends a Relation with tmp,
    // so the Iceberg schema never has it.
    let s = schema_with("orders", "qty", IcebergType::Int);
    let mut h = Harness::boot(std::slice::from_ref(&s));
    h.drive_then_materialize();

    h.db.alter_add_column(&s.ident, tmp_column()).unwrap();
    h.db.alter_drop_column(&s.ident, "tmp").unwrap();
    h.write(
        &s.ident,
        &[("id", PgValue::Int4(1)), ("qty", PgValue::Int4(1))],
    );
    h.drive_then_materialize();

    let evolved = h.iceberg_schema(&s.ident);
    assert!(evolved.columns.iter().all(|c| c.name != "tmp"));
}

#[test]
fn rows_before_a_re_add_keep_their_values_out_of_the_new_column() {
    // A row written, `note` dropped (seen: a write follows), re-added
    // (seen: a write follows), all materialized in one cycle. The first
    // row's value belongs to the dropped column, now renamed; the new
    // `note` is empty for it.
    let s = schema_with("orders", "note", IcebergType::String);
    let mut h = Harness::boot(std::slice::from_ref(&s));
    h.write(
        &s.ident,
        &[
            ("id", PgValue::Int4(1)),
            ("note", PgValue::Text("a".into())),
        ],
    );
    h.db.alter_drop_column(&s.ident, "note").unwrap();
    h.write(&s.ident, &[("id", PgValue::Int4(2))]);
    h.db.alter_add_column(
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
    h.write(
        &s.ident,
        &[
            ("id", PgValue::Int4(3)),
            ("note", PgValue::Text("c".into())),
        ],
    );
    h.drive_then_materialize();

    let schema = h.iceberg_schema(&s.ident);
    let mut rows = block_on(read_materialized_state(
        h.catalog.as_ref(),
        h.blob.as_ref(),
        &s.ident,
        &schema,
        &[col("id")],
    ))
    .unwrap();
    rows.sort_by_key(|r| format!("{:?}", r[&col("id")]));
    let notes: Vec<(PgValue, PgValue)> = rows
        .iter()
        .map(|r| (r[&col("note__dropped_2")].clone(), r[&col("note")].clone()))
        .collect();
    let text = |v: &str| PgValue::Text(v.into());
    assert_eq!(
        notes,
        vec![
            (text("a"), PgValue::Null),
            (PgValue::Null, PgValue::Null),
            (PgValue::Null, text("c")),
        ]
    );
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

    h.db.alter_add_column(
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
    h.write(
        &s_a.ident,
        &[("id", PgValue::Int4(1)), ("qty", PgValue::Int4(1))],
    );
    h.write(&s_b.ident, &[("id", PgValue::Int4(1))]);
    h.drive_then_materialize();

    let a = h.iceberg_schema(&s_a.ident);
    let b = h.iceberg_schema(&s_b.ident);

    assert!(
        a.columns.iter().any(|c| c.name == "tax"),
        "A got the new column"
    );
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
    // pgoutput re-emits Relation messages liberally — after every
    // cache invalidation (CREATE INDEX, ANALYZE, …), even when no
    // schema change happened. The materializer's diff must produce
    // zero SchemaChanges in that case, so we don't churn snapshot
    // history with empty schema updates.
    let s = schema_with("orders", "qty", IcebergType::Int);
    let mut h = Harness::boot(std::slice::from_ref(&s));
    h.write(
        &s.ident,
        &[("id", PgValue::Int4(1)), ("qty", PgValue::Int4(1))],
    );
    h.drive_then_materialize();

    let snapshots_before = block_on(h.catalog.snapshots(&s.ident)).unwrap().len();

    // The write after the invalidation brings an unchanged Relation.
    h.db.invalidate_relation(&s.ident).unwrap();
    h.write(
        &s.ident,
        &[("id", PgValue::Int4(2)), ("qty", PgValue::Int4(2))],
    );
    h.drive_then_materialize();

    let snapshots_after = block_on(h.catalog.snapshots(&s.ident)).unwrap().len();
    assert_eq!(
        snapshots_after,
        snapshots_before + 1,
        "only the write commits a snapshot, not the no-op Relation"
    );
}

// ── ADD + INSERT in same transactional flow ──────────────────────────

#[test]
fn add_column_then_insert_uses_new_column() {
    // The pipeline stages a Relation in order with the rows, so a
    // Relation arriving immediately before an Insert (e.g.
    // mid-transaction in real PG) is applied in time for that Insert
    // to encode the new column.
    let s = schema_with("orders", "qty", IcebergType::Int);
    let mut h = Harness::boot(std::slice::from_ref(&s));
    h.drive_then_materialize();

    h.db.alter_add_column(
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

// ── Long-running evolution: simulate "months" of ALTERs ────────────────
//
// Drives a deterministic mix of `ALTER ADD COLUMN`, `ALTER COLUMN TYPE`,
// and `ALTER DROP COLUMN` against a single table over many iterations.
// After every step we assert PG and Iceberg agree on the schema:
//
// - Every column currently in PG must exist in Iceberg with the same
//   `IcebergType`. (Promotions are applied to both sides simultaneously,
//   so they should match exactly, not just be promotion-compatible.)
// - Every Iceberg column NOT in PG must be a soft-drop (we tracked the
//   drop, and Iceberg keeps it as `nullable = true` for backward
//   compatibility with prior data files that still carry it).
// - The PK column `id` is never dropped or retyped — its `field_id`
//   must stay stable across the whole run.
//
// We also re-insert one row per iteration so the materializer actually
// flushes after every evolution. That exercises the "old staged data
// promoted under new schema" path the writer's int→long / float→double
// widening just landed for.

/// Tiny SplitMix64 — deterministic PRNG, no external dep. Same constants
/// as Vigna's reference. Used to pick ops + columns + types so failures
/// reproduce on any host.
struct SplitMix64(u64);

impl SplitMix64 {
    fn new(seed: u64) -> Self {
        SplitMix64(seed)
    }
    fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E3779B97F4A7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D049BB133111EB);
        z ^ (z >> 31)
    }
    fn pick_in(&mut self, n: usize) -> usize {
        debug_assert!(n > 0);
        (self.next_u64() % (n as u64)) as usize
    }
}

#[derive(Debug)]
enum EvoOp {
    Add { name: String, ty: IcebergType },
    Promote { name: String, new_ty: IcebergType },
    Drop { name: String },
}

fn legal_promotion(ty: IcebergType) -> Option<IcebergType> {
    match ty {
        IcebergType::Int => Some(IcebergType::Long),
        IcebergType::Float => Some(IcebergType::Double),
        _ => None,
    }
}

fn pick_add_type(rng: &mut SplitMix64) -> IcebergType {
    // Mix of promotable + non-promotable types so later iterations have
    // both kinds in `pg_cols` to choose from.
    const CHOICES: &[IcebergType] = &[
        IcebergType::Int,
        IcebergType::Long,
        IcebergType::Float,
        IcebergType::Double,
        IcebergType::String,
        IcebergType::Boolean,
    ];
    CHOICES[rng.pick_in(CHOICES.len())]
}

fn default_value_for(ty: IcebergType, seed: i64) -> PgValue {
    match ty {
        IcebergType::Int => PgValue::Int4(seed as i32),
        IcebergType::Long => PgValue::Int8(seed),
        IcebergType::Float => PgValue::Float4(seed as f32 + 0.5),
        IcebergType::Double => PgValue::Float8(seed as f64 + 0.25),
        IcebergType::String => PgValue::Text(format!("v{seed}")),
        IcebergType::Boolean => PgValue::Bool(seed % 2 == 0),
        // Other types aren't reached because we only ADD from
        // `pick_add_type`'s closed set.
        other => panic!("default_value_for: unsupported type {other:?}"),
    }
}

#[test]
fn long_running_evolution_keeps_pg_and_iceberg_in_sync() {
    let initial_ident = ident_named("evo");
    let initial_schema = TableSchema {
        ident: initial_ident.clone(),
        columns: vec![
            ColumnSchema {
                name: "id".into(),
                field_id: 1,
                ty: IcebergType::Int,
                nullable: false,
                is_primary_key: true,
            },
            ColumnSchema {
                name: "c001".into(),
                field_id: 2,
                ty: IcebergType::Int,
                nullable: true,
                is_primary_key: false,
            },
        ],
        partition_spec: Vec::new(),
        pg_schema: None,
    };
    let mut h = Harness::boot(std::slice::from_ref(&initial_schema));

    // Tracked PG view — source of truth for what columns currently
    // exist in PG. Mirrors what `sim PG.alter_*` does.
    let mut pg_cols: BTreeMap<String, IcebergType> = initial_schema
        .columns
        .iter()
        .map(|c| (c.name.clone(), c.ty))
        .collect();
    // Names that have ever been dropped. Used to confirm Iceberg-side
    // soft-drops are accounted for.
    let mut dropped_cols: BTreeSet<String> = BTreeSet::new();
    // Field id of `id`; should never change across the run.
    let id_field_id = 1;

    let mut next_seq: u32 = 1; // c001 already taken
    let mut rng = SplitMix64::new(0x517A1C0DEC0FFEE);

    // 120 ≈ "monthly evolution for 10 years" if you squint. Enough
    // mutation density to surface field-id drift, soft-drop bookkeeping,
    // and writer/widening interactions without ballooning test runtime.
    let n_steps = 120;
    for step in 0..n_steps {
        // ── Pick op ──
        let non_pk_names: Vec<String> = pg_cols
            .keys()
            .filter(|n| n.as_str() != "id")
            .cloned()
            .collect();
        let promotable_names: Vec<String> = non_pk_names
            .iter()
            .filter(|n| legal_promotion(pg_cols[*n]).is_some())
            .cloned()
            .collect();

        // 50% Add, 25% Promote, 25% Drop — fall back to Add when the
        // chosen op has no eligible target.
        let dice = rng.pick_in(4);
        let op = match dice {
            0 | 1 => {
                next_seq += 1;
                let name = format!("c{next_seq:03}");
                EvoOp::Add {
                    name,
                    ty: pick_add_type(&mut rng),
                }
            }
            2 if !promotable_names.is_empty() => {
                let n = promotable_names[rng.pick_in(promotable_names.len())].clone();
                let new_ty = legal_promotion(pg_cols[&n]).unwrap();
                EvoOp::Promote { name: n, new_ty }
            }
            3 if !non_pk_names.is_empty() => {
                let n = non_pk_names[rng.pick_in(non_pk_names.len())].clone();
                EvoOp::Drop { name: n }
            }
            _ => {
                next_seq += 1;
                EvoOp::Add {
                    name: format!("c{next_seq:03}"),
                    ty: pick_add_type(&mut rng),
                }
            }
        };

        // ── Apply on sim PG ──
        match &op {
            EvoOp::Add { name, ty } => {
                h.db.alter_add_column(
                    &initial_ident,
                    ColumnSchema {
                        name: name.clone(),
                        field_id: 0,
                        ty: *ty,
                        nullable: true,
                        is_primary_key: false,
                    },
                )
                .unwrap();
                pg_cols.insert(name.clone(), *ty);
            }
            EvoOp::Promote { name, new_ty } => {
                h.db.alter_column_type(&initial_ident, name, *new_ty)
                    .unwrap();
                pg_cols.insert(name.clone(), *new_ty);
            }
            EvoOp::Drop { name } => {
                h.db.alter_drop_column(&initial_ident, name).unwrap();
                pg_cols.remove(name);
                dropped_cols.insert(name.clone());
            }
        }

        // ── Insert one row with the current PG schema ──
        let mut row: Row = BTreeMap::new();
        row.insert(col("id"), PgValue::Int4(step + 1));
        for (cname, cty) in &pg_cols {
            if cname == "id" {
                continue;
            }
            row.insert(col(cname), default_value_for(*cty, step as i64));
        }
        let mut tx = h.db.begin_tx();
        tx.insert(&initial_ident, row);
        tx.commit(Timestamp(step as i64)).unwrap();

        h.drive_then_materialize();

        // ── Parity check ──
        let ice = h.iceberg_schema(&initial_ident);
        let ice_by_name: BTreeMap<&str, &ColumnSchema> =
            ice.columns.iter().map(|c| (c.name.as_str(), c)).collect();

        // PK invariant: `id` is present, non-nullable, field_id stable.
        let id_col = ice_by_name
            .get("id")
            .unwrap_or_else(|| panic!("step {step} ({op:?}): id column gone from Iceberg"));
        assert!(
            id_col.is_primary_key && !id_col.nullable,
            "step {step} ({op:?}): id should remain non-null PK, got {id_col:?}"
        );
        assert_eq!(
            id_col.field_id, id_field_id,
            "step {step} ({op:?}): id field_id drifted"
        );

        // Every PG column → Iceberg has it with matching type.
        for (name, ty) in &pg_cols {
            let ice_col = ice_by_name.get(name.as_str()).unwrap_or_else(|| {
                panic!(
                    "step {step} ({op:?}): pg has {name} but Iceberg doesn't. \
                     pg_cols={pg_cols:?} ice={:?}",
                    ice_by_name.keys().collect::<Vec<_>>()
                )
            });
            assert_eq!(
                ice_col.ty, *ty,
                "step {step} ({op:?}): {name} type mismatch (pg={ty:?}, ice={:?})",
                ice_col.ty
            );
        }

        // Every Iceberg column not currently in PG must be a tracked
        // soft-drop AND must be nullable. Anything else means schema
        // bookkeeping has drifted.
        for (name, ic) in &ice_by_name {
            if pg_cols.contains_key(*name) {
                continue;
            }
            assert!(
                dropped_cols.contains(*name),
                "step {step} ({op:?}): Iceberg has unknown column {name}, \
                 not in pg_cols and never dropped"
            );
            assert!(
                ic.nullable,
                "step {step} ({op:?}): soft-dropped {name} should be nullable"
            );
        }
    }

    // Sanity: the final Iceberg schema should have at least all of
    // `id` + every currently-live PG column + every soft-dropped name
    // we ever saw. (The per-step asserts above already prove this; we
    // restate it post-loop for grep-ability when this test fails on
    // refactor.)
    let final_schema = h.iceberg_schema(&initial_ident);
    let final_names: BTreeSet<String> = final_schema
        .columns
        .iter()
        .map(|c| c.name.clone())
        .collect();
    assert!(final_names.contains("id"));
    for n in pg_cols.keys() {
        assert!(final_names.contains(n), "final schema missing live col {n}");
    }
    for n in &dropped_cols {
        assert!(
            final_names.contains(n),
            "final schema missing soft-dropped col {n}"
        );
    }

    // Read-back of materialized data is intentionally NOT asserted
    // here: data files written before a `PromoteColumnType` carry
    // narrow Arrow types (Int32, Float32) and the current reader
    // requires the on-disk type to match the schema's. That's the
    // known "old data file under new schema" issue — see the existing
    // `type_promotion_int_to_long_succeeds` test for context. The
    // promote-on-read path is compaction's job; this DST is about the
    // schema-bookkeeping invariant only.
}
