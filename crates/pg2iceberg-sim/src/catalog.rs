//! In-memory `Catalog` impl for tests and the DST harness.
//!
//! Stores append-only snapshot history per table. `commit_snapshot` allocates
//! a monotonic snapshot id (= `prev_id + 1`); the verifier uses these ids to
//! apply Iceberg MoR semantics (delete file at snap N applies only to data
//! files at snap < N).

use async_trait::async_trait;
use pg2iceberg_core::{Namespace, TableIdent, TableSchema};
use pg2iceberg_iceberg::PreparedCommit;
use pg2iceberg_iceberg::{
    apply_schema_changes, Catalog, IcebergError, Result, SchemaChange, Snapshot, TableMetadata,
};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};

#[derive(Default)]
struct State {
    namespaces: BTreeSet<Namespace>,
    tables: BTreeMap<TableIdent, MemTable>,
}

struct MemTable {
    metadata: TableMetadata,
    snapshots: Vec<Snapshot>,
    next_snapshot_id: i64,
}

#[derive(Default, Clone)]
pub struct MemoryCatalog {
    state: Arc<Mutex<State>>,
}

impl MemoryCatalog {
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl Catalog for MemoryCatalog {
    async fn ensure_namespace(&self, ns: &Namespace) -> Result<()> {
        self.state.lock().unwrap().namespaces.insert(ns.clone());
        Ok(())
    }

    async fn load_table(&self, ident: &TableIdent) -> Result<Option<TableMetadata>> {
        Ok(self
            .state
            .lock()
            .unwrap()
            .tables
            .get(ident)
            .map(|t| t.metadata.clone()))
    }

    async fn create_table(&self, schema: &TableSchema) -> Result<TableMetadata> {
        let mut s = self.state.lock().unwrap();
        let ident = schema.ident.clone();
        if s.tables.contains_key(&ident) {
            return Err(IcebergError::Conflict(format!(
                "table already exists: {ident}"
            )));
        }
        if !s.namespaces.contains(&ident.namespace) {
            return Err(IcebergError::NotFound(format!(
                "namespace not registered: {}",
                ident.namespace
            )));
        }
        let metadata = TableMetadata {
            ident: ident.clone(),
            schema: schema.clone(),
            current_snapshot_id: None,
            config: BTreeMap::new(),
        };
        s.tables.insert(
            ident,
            MemTable {
                metadata: metadata.clone(),
                snapshots: Vec::new(),
                next_snapshot_id: 1,
            },
        );
        Ok(metadata)
    }

    async fn commit_snapshot(&self, prepared: PreparedCommit) -> Result<TableMetadata> {
        let mut s = self.state.lock().unwrap();
        let table = s
            .tables
            .get_mut(&prepared.ident)
            .ok_or_else(|| IcebergError::NotFound(format!("table: {}", prepared.ident)))?;

        // Skip empty commits (no data, no deletes). This matches the materializer's
        // behavior of not bumping the snapshot id when there's nothing to write.
        if prepared.data_files.is_empty() && prepared.equality_deletes.is_empty() {
            return Ok(table.metadata.clone());
        }

        let id = table.next_snapshot_id;
        table.next_snapshot_id += 1;
        table.snapshots.push(Snapshot {
            id,
            data_files: prepared.data_files,
            delete_files: prepared.equality_deletes,
        });
        table.metadata.current_snapshot_id = Some(id);
        Ok(table.metadata.clone())
    }

    async fn evolve_schema(
        &self,
        ident: &TableIdent,
        changes: Vec<SchemaChange>,
    ) -> Result<TableMetadata> {
        let mut s = self.state.lock().unwrap();
        let table = s
            .tables
            .get_mut(ident)
            .ok_or_else(|| IcebergError::NotFound(format!("table: {ident}")))?;
        if changes.is_empty() {
            return Ok(table.metadata.clone());
        }
        // Use the same helper the prod catalog uses, so both backends apply
        // identical field-id allocation and soft-drop semantics. The sim
        // does not produce snapshots for schema-only commits — it just
        // updates the schema in metadata.
        apply_schema_changes(&mut table.metadata.schema, &changes)?;
        Ok(table.metadata.clone())
    }

    async fn snapshots(&self, ident: &TableIdent) -> Result<Vec<Snapshot>> {
        let s = self.state.lock().unwrap();
        Ok(s.tables
            .get(ident)
            .map(|t| t.snapshots.clone())
            .unwrap_or_default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pg2iceberg_core::typemap::IcebergType;
    use pg2iceberg_core::ColumnSchema;
    use pg2iceberg_iceberg::DataFile;
    use pollster::block_on;

    fn ident() -> TableIdent {
        TableIdent {
            namespace: Namespace(vec!["public".into()]),
            name: "t".into(),
        }
    }

    fn schema() -> TableSchema {
        TableSchema {
            ident: ident(),
            columns: vec![ColumnSchema {
                name: "id".into(),
                field_id: 1,
                ty: IcebergType::Int,
                nullable: false,
                is_primary_key: true,
            }],
            partition_spec: Vec::new(),
        }
    }

    #[test]
    fn create_then_load_round_trips() {
        let c = MemoryCatalog::new();
        block_on(c.ensure_namespace(&ident().namespace)).unwrap();
        assert!(block_on(c.load_table(&ident())).unwrap().is_none());
        let meta = block_on(c.create_table(&schema())).unwrap();
        assert_eq!(meta.ident, ident());
        assert!(block_on(c.load_table(&ident())).unwrap().is_some());
    }

    #[test]
    fn create_table_in_unregistered_namespace_errors() {
        let c = MemoryCatalog::new();
        let err = block_on(c.create_table(&schema())).unwrap_err();
        assert!(matches!(err, IcebergError::NotFound(_)));
    }

    #[test]
    fn commit_appends_snapshots_with_increasing_ids() {
        let c = MemoryCatalog::new();
        block_on(c.ensure_namespace(&ident().namespace)).unwrap();
        block_on(c.create_table(&schema())).unwrap();

        for i in 0..3 {
            block_on(c.commit_snapshot(PreparedCommit {
                ident: ident(),
                data_files: vec![DataFile {
                    path: format!("s3://t/data-{i}.parquet"),
                    record_count: 1,
                    byte_size: 100,
                    equality_field_ids: vec![],
                }],
                equality_deletes: vec![],
            }))
            .unwrap();
        }
        let snaps = block_on(c.snapshots(&ident())).unwrap();
        assert_eq!(
            snaps.iter().map(|s| s.id).collect::<Vec<_>>(),
            vec![1, 2, 3]
        );
    }

    #[test]
    fn empty_prepared_commit_is_a_noop() {
        let c = MemoryCatalog::new();
        block_on(c.ensure_namespace(&ident().namespace)).unwrap();
        block_on(c.create_table(&schema())).unwrap();
        block_on(c.commit_snapshot(PreparedCommit {
            ident: ident(),
            data_files: vec![],
            equality_deletes: vec![],
        }))
        .unwrap();
        assert!(block_on(c.snapshots(&ident())).unwrap().is_empty());
    }

    #[test]
    fn evolve_schema_add_column_appends_with_fresh_field_id() {
        let c = MemoryCatalog::new();
        block_on(c.ensure_namespace(&ident().namespace)).unwrap();
        block_on(c.create_table(&schema())).unwrap();
        let meta = block_on(c.evolve_schema(
            &ident(),
            vec![pg2iceberg_iceberg::SchemaChange::AddColumn {
                name: "qty".into(),
                ty: IcebergType::Long,
                nullable: true,
            }],
        ))
        .unwrap();
        assert_eq!(meta.schema.columns.len(), 2);
        let qty = meta
            .schema
            .columns
            .iter()
            .find(|c| c.name == "qty")
            .unwrap();
        assert_eq!(qty.field_id, 2);
        assert!(qty.nullable);
        assert!(!qty.is_primary_key);
    }

    #[test]
    fn evolve_schema_drop_column_is_soft_drop() {
        let c = MemoryCatalog::new();
        block_on(c.ensure_namespace(&ident().namespace)).unwrap();
        // Create a table where column 2 is non-nullable so we can observe
        // the soft-drop flipping it.
        let s = TableSchema {
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
                    ty: IcebergType::Long,
                    nullable: false,
                    is_primary_key: false,
                },
            ],
            partition_spec: Vec::new(),
        };
        block_on(c.create_table(&s)).unwrap();
        let meta = block_on(c.evolve_schema(
            &ident(),
            vec![pg2iceberg_iceberg::SchemaChange::DropColumn { name: "qty".into() }],
        ))
        .unwrap();
        assert_eq!(meta.schema.columns.len(), 2);
        let qty = meta
            .schema
            .columns
            .iter()
            .find(|c| c.name == "qty")
            .unwrap();
        assert!(qty.nullable);
    }

    #[test]
    fn evolve_schema_on_missing_table_errors() {
        let c = MemoryCatalog::new();
        let err = block_on(c.evolve_schema(
            &ident(),
            vec![pg2iceberg_iceberg::SchemaChange::AddColumn {
                name: "x".into(),
                ty: IcebergType::Int,
                nullable: true,
            }],
        ))
        .unwrap_err();
        assert!(matches!(err, IcebergError::NotFound(_)));
    }
}
