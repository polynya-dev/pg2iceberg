//! Smoke tests that exercise `iceberg-rust 0.9` end-to-end against the
//! in-process `MemoryCatalog`. These confirm the upstream surface our
//! `IcebergRustCatalog` wrapper relies on:
//!
//! - Schema build with field ids + identifier-field marking.
//! - `MemoryCatalogBuilder` → `Catalog::create_namespace` / `create_table`.
//! - `Transaction::fast_append().add_data_files(...)` round-trip.
//! - Reading snapshots / manifests back from a loaded `Table`.
//!
//! The fast-append path is data-files-only by upstream design — equality
//! deletes and position deletes have no public action surface in 0.9.
//! See `gap_audit` for the remaining gaps.

use std::collections::HashMap;

use iceberg::spec::{
    DataContentType, DataFile, DataFileBuilder, DataFileFormat, NestedField, PrimitiveType, Schema,
    Struct, Type,
};
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::{
    memory::{MemoryCatalogBuilder, MEMORY_CATALOG_WAREHOUSE},
    Catalog, CatalogBuilder, NamespaceIdent, TableCreation, TableIdent,
};

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

/// Helper: build a `MemoryCatalog` rooted at `memory:///warehouse`. The
/// upstream memory catalog wires its own `MemoryStorageFactory`; manifests
/// live entirely in-process.
async fn make_memory_catalog() -> impl Catalog {
    MemoryCatalogBuilder::default()
        .load(
            "test",
            HashMap::from([(
                MEMORY_CATALOG_WAREHOUSE.to_string(),
                "memory:///warehouse".to_string(),
            )]),
        )
        .await
        .unwrap()
}

fn orders_schema() -> Schema {
    Schema::builder()
        .with_schema_id(0)
        // id-column field-id == 1; PG-side mapper produces this from the PK
        // column in `pg2iceberg_core::TableSchema`.
        .with_identifier_field_ids([1])
        .with_fields(vec![
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
            NestedField::required(2, "qty", Type::Primitive(PrimitiveType::Long)).into(),
            NestedField::optional(3, "note", Type::Primitive(PrimitiveType::String)).into(),
        ])
        .build()
        .unwrap()
}

#[tokio::test]
async fn create_namespace_and_table_round_trip_through_memory_catalog() {
    let catalog = make_memory_catalog().await;
    let ns = NamespaceIdent::from_strs(["public"]).unwrap();
    catalog.create_namespace(&ns, HashMap::new()).await.unwrap();
    assert!(catalog.namespace_exists(&ns).await.unwrap());

    let creation = TableCreation::builder()
        .name("orders".to_string())
        .schema(orders_schema())
        .build();
    let table = catalog.create_table(&ns, creation).await.unwrap();
    assert_eq!(table.identifier().name(), "orders");
    assert!(table.metadata().current_snapshot().is_none());

    let loaded = catalog
        .load_table(&TableIdent::new(ns, "orders".into()))
        .await
        .unwrap();
    assert_eq!(loaded.metadata().uuid(), table.metadata().uuid());
}

/// End-to-end fast-append commit: namespace → table → fast_append → load.
/// Confirms that we can build a `DataFile` from our side (path, size,
/// record_count, partition_spec_id) and have the manifest writer accept
/// it without ever opening the actual data file.
///
/// This proves the **append-only** write path. Equality-delete commits
/// have no equivalent action in 0.9 — `FastAppendAction` rejects content
/// types other than `Data`, and `TableCommit::builder` is `pub(crate)` so
/// we can't bypass it from outside the crate. Tracked in `gap_audit`.
#[tokio::test]
async fn fast_append_round_trip_creates_snapshot_with_data_file() {
    let catalog = make_memory_catalog().await;
    let ns = NamespaceIdent::from_strs(["public"]).unwrap();
    catalog.create_namespace(&ns, HashMap::new()).await.unwrap();
    let table = catalog
        .create_table(
            &ns,
            TableCreation::builder()
                .name("orders".to_string())
                .schema(orders_schema())
                .build(),
        )
        .await
        .unwrap();

    let data_file: DataFile = DataFileBuilder::default()
        .content(DataContentType::Data)
        .file_path(format!("{}/data/0001.parquet", table.metadata().location()))
        .file_format(DataFileFormat::Parquet)
        .file_size_in_bytes(2048)
        .record_count(7)
        .partition(Struct::empty())
        .partition_spec_id(table.metadata().default_partition_spec_id())
        .build()
        .unwrap();

    let tx = Transaction::new(&table);
    let action = tx
        .fast_append()
        .with_check_duplicate(false)
        .add_data_files(vec![data_file]);
    let tx = action.apply(tx).unwrap();
    let table = tx.commit(&catalog).await.unwrap();

    let snap = table
        .metadata()
        .current_snapshot()
        .expect("fast append must produce a snapshot");
    assert_eq!(snap.summary().operation, iceberg::spec::Operation::Append);

    let manifest_list = snap
        .load_manifest_list(table.file_io(), table.metadata())
        .await
        .unwrap();
    assert_eq!(manifest_list.entries().len(), 1);
    let manifest = manifest_list.entries()[0]
        .load_manifest(table.file_io())
        .await
        .unwrap();
    assert_eq!(manifest.entries().len(), 1);
    assert_eq!(manifest.entries()[0].data_file().record_count(), 7);
    assert_eq!(manifest.entries()[0].data_file().file_size_in_bytes(), 2048);
}

/// Fast append rejects `EqualityDeletes` — explicit upstream check we
/// rely on to know the equality-delete path needs a different surface.
/// If this test ever stops failing, iceberg-rust has shipped delete-aware
/// actions and we should revisit the `commit_snapshot` impl.
#[tokio::test]
async fn fast_append_rejects_equality_delete_files() {
    let catalog = make_memory_catalog().await;
    let ns = NamespaceIdent::from_strs(["public"]).unwrap();
    catalog.create_namespace(&ns, HashMap::new()).await.unwrap();
    let table = catalog
        .create_table(
            &ns,
            TableCreation::builder()
                .name("orders".to_string())
                .schema(orders_schema())
                .build(),
        )
        .await
        .unwrap();

    let delete_file: DataFile = DataFileBuilder::default()
        .content(DataContentType::EqualityDeletes)
        .file_path(format!(
            "{}/deletes/0001.parquet",
            table.metadata().location()
        ))
        .file_format(DataFileFormat::Parquet)
        .file_size_in_bytes(64)
        .record_count(1)
        .equality_ids(Some(vec![1]))
        .partition(Struct::empty())
        .partition_spec_id(table.metadata().default_partition_spec_id())
        .build()
        .unwrap();

    let tx = Transaction::new(&table);
    let action = tx
        .fast_append()
        .with_check_duplicate(false)
        .add_data_files(vec![delete_file]);
    let tx = action.apply(tx).unwrap();
    let err = tx.commit(&catalog).await.unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("Only data content type is allowed"),
        "expected upstream rejection, got: {msg}"
    );
}
