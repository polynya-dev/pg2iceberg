//! DST coverage for the Decimal write path.
//!
//! Two correctness invariants that the unit tests in `writer.rs`
//! pin in isolation, here exercised end-to-end through the fold +
//! materializer + reader round-trip:
//!
//! 1. **Lossless on equal scales** — a `NUMERIC(p, s)` column round-
//!    trips through stage→fold→write→read with identical unscaled
//!    bytes when the source and column scales match.
//!
//! 2. **Refuse lossy downscale** — when the source value carries
//!    more fractional digits than the column allows, the writer
//!    must error rather than silently truncate. This is the data-
//!    loss guard the user explicitly asked for.

use std::sync::Arc;

use pg2iceberg_coord::schema::CoordSchema;
use pg2iceberg_coord::Coordinator;
use pg2iceberg_core::value::Decimal;
use pg2iceberg_core::{
    ColumnName, ColumnSchema, IcebergType, Namespace, PgValue, Row, TableIdent, TableSchema,
    Timestamp,
};
use pg2iceberg_iceberg::read_materialized_state;
use pg2iceberg_logical::materializer::{CounterMaterializerNamer, Materializer};
use pg2iceberg_logical::pipeline::{CounterBlobNamer, Pipeline};
use pg2iceberg_sim::blob::MemoryBlobStore;
use pg2iceberg_sim::catalog::MemoryCatalog;
use pg2iceberg_sim::clock::TestClock;
use pg2iceberg_sim::coord::MemoryCoordinator;
use pg2iceberg_sim::postgres::{SimPostgres, SimReplicationStream};
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

fn schema_with_decimal(precision: u8, scale: u8) -> TableSchema {
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
                name: "amount".into(),
                field_id: 2,
                ty: IcebergType::Decimal { precision, scale },
                nullable: false,
                is_primary_key: false,
            },
        ],
        partition_spec: vec![],
        pg_schema: None,
    }
}

/// Build a `Decimal` whose `unscaled_be_bytes` use the **minimal**
/// big-endian two's-complement representation — which is what real
/// PG sends through pgoutput, and what the reader emits after
/// trimming. Tests can then assert exact `Decimal` equality.
fn dec(unscaled: i128, scale: u8) -> Decimal {
    let mut bytes = unscaled.to_be_bytes().to_vec();
    while bytes.len() > 1 {
        let head = bytes[0];
        let next = bytes[1];
        let head_is_redundant =
            (head == 0x00 && (next & 0x80) == 0) || (head == 0xFF && (next & 0x80) != 0);
        if !head_is_redundant {
            break;
        }
        bytes.remove(0);
    }
    Decimal {
        unscaled_be_bytes: bytes,
        scale,
    }
}

fn row(id: i32, d: Decimal) -> Row {
    let mut r = BTreeMap::new();
    r.insert(ColumnName("id".into()), PgValue::Int4(id));
    r.insert(ColumnName("amount".into()), PgValue::Numeric(d));
    r
}

#[allow(dead_code)]
struct Harness {
    db: SimPostgres,
    blob: Arc<MemoryBlobStore>,
    catalog: Arc<MemoryCatalog>,
    pipeline: Pipeline<MemoryCoordinator>,
    materializer: Materializer<MemoryCatalog>,
    stream: SimReplicationStream,
    schema: TableSchema,
}

impl Harness {
    fn boot(schema: TableSchema) -> Self {
        let db = SimPostgres::new();
        db.create_table(schema.clone()).unwrap();
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
        pipeline.register_primary_keys(ident(), vec![ColumnName("id".into())]);
        let mut materializer = Materializer::new(
            coord.clone() as Arc<dyn Coordinator>,
            blob.clone(),
            catalog.clone(),
            Arc::new(CounterMaterializerNamer::new("s3://table")),
            "default",
            128,
        );
        block_on(materializer.register_table(schema.clone())).unwrap();
        let stream = db.start_replication(SLOT).unwrap();

        Self {
            db,
            blob,
            catalog,
            pipeline,
            materializer,
            stream,
            schema,
        }
    }

    fn drive_then_materialize(&mut self) {
        while let Some(msg) = self.stream.recv() {
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
            &self.schema,
            &[ColumnName("id".into())],
        ))
        .unwrap();
        rows.sort_by_key(|r| match r.get(&ColumnName("id".into())) {
            Some(PgValue::Int4(v)) => *v,
            _ => 0,
        });
        rows
    }
}

#[test]
fn decimal_same_scale_round_trips_unchanged() {
    // NUMERIC(10,2) → Decimal{precision:10, scale:2}. Source values
    // already at scale=2 must round-trip with identical unscaled
    // bytes — no scaling, no precision drift.
    let mut h = Harness::boot(schema_with_decimal(10, 2));

    let mut tx = h.db.begin_tx();
    tx.insert(&ident(), row(1, dec(10050, 2))); // 100.50
    tx.insert(&ident(), row(2, dec(20075, 2))); // 200.75
    tx.insert(&ident(), row(3, dec(30000, 2))); // 300.00
    tx.commit(Timestamp(0)).unwrap();
    h.drive_then_materialize();

    let rows = h.iceberg_rows();
    assert_eq!(rows.len(), 3);
    let amount_of = |r: &Row| match r.get(&ColumnName("amount".into())) {
        Some(PgValue::Numeric(d)) => d.clone(),
        other => panic!("expected Numeric, got {:?}", other),
    };
    assert_eq!(amount_of(&rows[0]), dec(10050, 2));
    assert_eq!(amount_of(&rows[1]), dec(20075, 2));
    assert_eq!(amount_of(&rows[2]), dec(30000, 2));
}

#[test]
fn decimal_upscale_is_lossless() {
    // Source carries scale=0 (integer-shaped), column is NUMERIC(10,2).
    // Writer must upscale 100 → 100.00 (i.e. unscaled 10000 at scale 2).
    let mut h = Harness::boot(schema_with_decimal(10, 2));

    let mut tx = h.db.begin_tx();
    tx.insert(&ident(), row(1, dec(100, 0))); // 100
    tx.commit(Timestamp(0)).unwrap();
    h.drive_then_materialize();

    let rows = h.iceberg_rows();
    assert_eq!(rows.len(), 1);
    let got = match rows[0].get(&ColumnName("amount".into())) {
        Some(PgValue::Numeric(d)) => d.clone(),
        _ => panic!("expected Numeric"),
    };
    assert_eq!(got, dec(10000, 2));
}

#[test]
fn decimal_lossy_downscale_refuses_with_error() {
    // Source has scale=4, column has scale=2. Source value 1.2345
    // can't fit in scale=2 without dropping the 0.0045 — writer must
    // refuse rather than silently truncate. This is the data-loss
    // guard.
    let mut h = Harness::boot(schema_with_decimal(10, 2));

    let mut tx = h.db.begin_tx();
    tx.insert(&ident(), row(1, dec(12345, 4))); // 1.2345
    tx.commit(Timestamp(0)).unwrap();

    while let Some(msg) = h.stream.recv() {
        block_on(h.pipeline.process(msg)).unwrap();
    }
    block_on(h.pipeline.flush()).unwrap();
    let result = block_on(h.materializer.cycle());
    assert!(
        result.is_err(),
        "lossy downscale must error rather than truncate; got {:?}",
        result
    );
    let msg = format!("{}", result.unwrap_err());
    assert!(
        msg.contains("Decimal128") || msg.contains("decimal"),
        "error must reference the decimal column; got {}",
        msg
    );
}

#[test]
fn decimal_negative_values_round_trip() {
    // Sign handling is the most fragile part of the i128 path —
    // sign-extension on short byte representations, two's complement
    // through scaling. Round-trip a few negatives end-to-end.
    let mut h = Harness::boot(schema_with_decimal(10, 2));

    let mut tx = h.db.begin_tx();
    tx.insert(&ident(), row(1, dec(-12345, 2))); // -123.45
    tx.insert(&ident(), row(2, dec(-1, 2))); //   -0.01
    tx.commit(Timestamp(0)).unwrap();
    h.drive_then_materialize();

    let rows = h.iceberg_rows();
    let amount_of = |r: &Row| match r.get(&ColumnName("amount".into())) {
        Some(PgValue::Numeric(d)) => d.clone(),
        _ => panic!("expected Numeric"),
    };
    assert_eq!(rows.len(), 2);
    assert_eq!(amount_of(&rows[0]), dec(-12345, 2));
    assert_eq!(amount_of(&rows[1]), dec(-1, 2));
}
