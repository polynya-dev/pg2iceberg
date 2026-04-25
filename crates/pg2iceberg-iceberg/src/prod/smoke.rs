//! Smoke test against `iceberg-rust 0.6` primitive types.
//!
//! Confirms the type surface we'd need to translate is shaped the way the
//! gap audit assumes. Doesn't yet exercise a Catalog backend — every
//! published 0.6 catalog crate (REST, Glue, SQL) wants a real backing
//! service, and `iceberg-catalog-memory` exists only in the upstream
//! `apache/iceberg-rust` git source (not on crates.io). Wiring those up
//! is a Phase 13.5 task.

use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
use iceberg::{NamespaceIdent, TableIdent};

#[test]
fn schema_builder_accepts_our_field_id_shape() {
    let s = Schema::builder()
        .with_schema_id(0)
        .with_fields(vec![
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
            NestedField::required(2, "qty", Type::Primitive(PrimitiveType::Int)).into(),
        ])
        .build()
        .unwrap();

    assert_eq!(s.schema_id(), 0);
    let id = s.field_by_id(1).unwrap();
    assert_eq!(id.name, "id");
    let qty = s.field_by_id(2).unwrap();
    assert_eq!(qty.name, "qty");
}

#[test]
fn schema_supports_all_iceberg_types_we_emit() {
    // Mirrors the column types `pg2iceberg-iceberg::writer` knows how to
    // emit. If this stops compiling against a future iceberg release,
    // the writer's `IcebergType` → upstream-Type mapping needs an
    // update.
    let s = Schema::builder()
        .with_schema_id(0)
        .with_fields(vec![
            NestedField::required(1, "boolean", Type::Primitive(PrimitiveType::Boolean)).into(),
            NestedField::required(2, "int", Type::Primitive(PrimitiveType::Int)).into(),
            NestedField::required(3, "long", Type::Primitive(PrimitiveType::Long)).into(),
            NestedField::required(4, "float", Type::Primitive(PrimitiveType::Float)).into(),
            NestedField::required(5, "double", Type::Primitive(PrimitiveType::Double)).into(),
            NestedField::required(6, "string", Type::Primitive(PrimitiveType::String)).into(),
            NestedField::required(7, "date", Type::Primitive(PrimitiveType::Date)).into(),
            NestedField::required(8, "timestamp", Type::Primitive(PrimitiveType::Timestamp)).into(),
            NestedField::required(
                9,
                "timestamptz",
                Type::Primitive(PrimitiveType::Timestamptz),
            )
            .into(),
        ])
        .build()
        .unwrap();
    assert_eq!(s.as_struct().fields().len(), 9);
}

#[test]
fn nullable_fields_round_trip() {
    let s = Schema::builder()
        .with_schema_id(0)
        .with_fields(vec![
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
            NestedField::optional(2, "note", Type::Primitive(PrimitiveType::String)).into(),
        ])
        .build()
        .unwrap();
    assert!(s.field_by_id(1).unwrap().required);
    assert!(!s.field_by_id(2).unwrap().required);
}

#[test]
fn table_ident_round_trips() {
    let ns = NamespaceIdent::from_strs(["public"]).unwrap();
    let id = TableIdent::new(ns.clone(), "orders".into());
    assert_eq!(id.namespace(), &ns);
    assert_eq!(id.name(), "orders");
}

#[test]
fn nested_namespace_supported() {
    let ns = NamespaceIdent::from_strs(["my_db", "schema"]).unwrap();
    assert_eq!(ns.as_ref().len(), 2);
}
