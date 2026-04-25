//! `IcebergRustCatalog`: wraps any `iceberg::Catalog` (Memory, REST, Glue,
//! SQL, ...) behind our [`crate::Catalog`] trait so the materializer can
//! drive a real Iceberg backend.
//!
//! Translation rules:
//!
//! - **Append-only commits.** `commit_snapshot` uses
//!   [`iceberg::transaction::Transaction::fast_append`]. This handles the
//!   bulk of pg2iceberg's I/O: every initial-snapshot batch and every
//!   no-update-only WAL flush is append-only.
//! - **Equality-delete commits.** Returns
//!   [`crate::IcebergError::Other`] with a clear "blocked on upstream"
//!   message. iceberg-rust 0.9's only public commit action is
//!   `FastAppendAction`, which rejects `DataContentType::EqualityDeletes`,
//!   and `TableCommit::builder` is `pub(crate)` so we cannot bypass it.
//!   See `gap_audit` for the upstream tracking.
//! - **Schema evolution.** Returns `Other("not yet supported")`. The
//!   transaction surface for `UpdateSchema` exists in `iceberg-rust` but
//!   the materializer doesn't drive it yet — wiring is a follow-on task.
//! - **`load_table` not-found.** Maps `ErrorKind::TableNotFound` →
//!   `Ok(None)` (the materializer treats not-found distinctly from
//!   transient errors).

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use async_trait::async_trait;
use iceberg::spec::{
    DataContentType, DataFile as IcebergDataFile, DataFileBuilder, DataFileFormat, NestedField,
    PrimitiveType, Schema as IcebergSchema, Struct, Type,
};
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::{
    Catalog as IcebergCatalogTrait, ErrorKind, NamespaceIdent, TableCreation,
    TableIdent as IcebergTableIdent,
};
use pg2iceberg_core::{typemap::IcebergType, ColumnSchema, Namespace, TableIdent, TableSchema};

use crate::{
    Catalog, DataFile, IcebergError, PreparedCommit, Result, SchemaChange, Snapshot, TableMetadata,
};

/// Wraps an `iceberg::Catalog` (e.g. `MemoryCatalog`, `RestCatalog`,
/// `GlueCatalog`) and exposes it as our [`Catalog`] trait.
pub struct IcebergRustCatalog<C: IcebergCatalogTrait> {
    inner: Arc<C>,
}

impl<C: IcebergCatalogTrait> IcebergRustCatalog<C> {
    pub fn new(inner: Arc<C>) -> Self {
        Self { inner }
    }
}

impl<C: IcebergCatalogTrait> std::fmt::Debug for IcebergRustCatalog<C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IcebergRustCatalog").finish()
    }
}

#[async_trait]
impl<C: IcebergCatalogTrait + Send + Sync + 'static> Catalog for IcebergRustCatalog<C> {
    async fn ensure_namespace(&self, ns: &Namespace) -> Result<()> {
        let ident = to_iceberg_namespace(ns)?;
        if self
            .inner
            .namespace_exists(&ident)
            .await
            .map_err(map_iceberg_err)?
        {
            return Ok(());
        }
        match self.inner.create_namespace(&ident, HashMap::new()).await {
            Ok(_) => Ok(()),
            // Concurrent create races against our exists-check.
            Err(e) if e.kind() == ErrorKind::NamespaceAlreadyExists => Ok(()),
            Err(e) => Err(map_iceberg_err(e)),
        }
    }

    async fn load_table(&self, ident: &TableIdent) -> Result<Option<TableMetadata>> {
        let it = to_iceberg_table_ident(ident)?;
        match self.inner.load_table(&it).await {
            Ok(table) => Ok(Some(metadata_from_table(ident, &table)?)),
            // Either a missing table or a missing namespace means "no such
            // table" from the materializer's perspective.
            Err(e)
                if e.kind() == ErrorKind::TableNotFound
                    || e.kind() == ErrorKind::NamespaceNotFound =>
            {
                Ok(None)
            }
            Err(e) => Err(map_iceberg_err(e)),
        }
    }

    async fn create_table(&self, schema: &TableSchema) -> Result<TableMetadata> {
        let ns = to_iceberg_namespace(&schema.ident.namespace)?;
        let ice_schema = to_iceberg_schema(schema)?;
        let creation = TableCreation::builder()
            .name(schema.ident.name.clone())
            .schema(ice_schema)
            .build();
        let table = self
            .inner
            .create_table(&ns, creation)
            .await
            .map_err(map_iceberg_err)?;
        metadata_from_table(&schema.ident, &table)
    }

    async fn commit_snapshot(&self, prepared: PreparedCommit) -> Result<TableMetadata> {
        if !prepared.equality_deletes.is_empty() {
            // FastAppendAction validates `content_type == Data`. Our only other
            // public path would be `TableCommit::builder`, which is `pub(crate)`
            // in 0.9. Tracked in `gap_audit`.
            return Err(IcebergError::Other(
                "equality-delete commits are not yet supported by the iceberg-rust prod \
                 backend (FastAppendAction rejects non-Data content; TableCommit::builder \
                 is pub(crate) in 0.9). Use the sim catalog or wait for upstream to ship a \
                 delete-aware action."
                    .to_string(),
            ));
        }
        if prepared.data_files.is_empty() {
            // No work — match the sim-catalog noop semantics so the materializer
            // can flush "no data, no deletes" without a snapshot bump.
            let it = to_iceberg_table_ident(&prepared.ident)?;
            let table = self.inner.load_table(&it).await.map_err(map_iceberg_err)?;
            return metadata_from_table(&prepared.ident, &table);
        }

        let it = to_iceberg_table_ident(&prepared.ident)?;
        let table = self.inner.load_table(&it).await.map_err(map_iceberg_err)?;
        let spec_id = table.metadata().default_partition_spec_id();

        let mut data_files: Vec<IcebergDataFile> = Vec::with_capacity(prepared.data_files.len());
        for df in &prepared.data_files {
            data_files.push(
                DataFileBuilder::default()
                    .content(DataContentType::Data)
                    .file_path(df.path.clone())
                    .file_format(DataFileFormat::Parquet)
                    .file_size_in_bytes(df.byte_size)
                    .record_count(df.record_count)
                    .partition(Struct::empty())
                    .partition_spec_id(spec_id)
                    .build()
                    .map_err(|e| IcebergError::Other(format!("data file build: {e}")))?,
            );
        }

        let tx = Transaction::new(&table);
        let action = tx
            .fast_append()
            // The materializer guarantees unique paths via `RollingWriter`'s
            // counter-based namer; skip iceberg-rust's path-dedup which would
            // otherwise scan the full manifest list on each commit.
            .with_check_duplicate(false)
            .add_data_files(data_files);
        let tx = action.apply(tx).map_err(map_iceberg_err)?;
        let updated = tx
            .commit(self.inner.as_ref())
            .await
            .map_err(map_iceberg_err)?;
        metadata_from_table(&prepared.ident, &updated)
    }

    async fn evolve_schema(
        &self,
        _ident: &TableIdent,
        _changes: Vec<SchemaChange>,
    ) -> Result<TableMetadata> {
        // The materializer's `apply_schema_change` path doesn't drive this
        // through the prod catalog yet. Wire UpdateSchema action when we
        // start mirroring DDL through Iceberg.
        Err(IcebergError::Other(
            "schema evolution against iceberg-rust prod catalog is not yet wired".into(),
        ))
    }

    async fn snapshots(&self, ident: &TableIdent) -> Result<Vec<Snapshot>> {
        let it = to_iceberg_table_ident(ident)?;
        let table = match self.inner.load_table(&it).await {
            Ok(t) => t,
            Err(e)
                if e.kind() == ErrorKind::TableNotFound
                    || e.kind() == ErrorKind::NamespaceNotFound =>
            {
                return Ok(Vec::new());
            }
            Err(e) => return Err(map_iceberg_err(e)),
        };
        let mut out: Vec<Snapshot> = Vec::new();
        let snaps: Vec<_> = table.metadata().snapshots().cloned().collect();
        for snap in snaps {
            // Use the iceberg snapshot_id for manifest filtering (matches the
            // `added_snapshot_id` field stored in manifest entries), but report
            // `sequence_number` as our `Snapshot.id` so MoR ordering
            // (`delete.id > data.id`) stays monotonic. iceberg-rust generates
            // `snapshot_id` as a random 63-bit value — comparing those would
            // break the verifier and FileIndex.
            let snap_id = snap.snapshot_id();
            let seq_num = snap.sequence_number();
            let manifest_list = snap
                .load_manifest_list(table.file_io(), table.metadata())
                .await
                .map_err(map_iceberg_err)?;
            let mut data_files: Vec<DataFile> = Vec::new();
            let mut delete_files: Vec<DataFile> = Vec::new();
            for entry in manifest_list.entries() {
                // Iceberg snapshots inherit prior manifests by reference;
                // restrict to the ones first introduced by *this* snapshot
                // so our `Snapshot.data_files` matches the sim catalog's
                // "files added in this commit" semantics.
                if entry.added_snapshot_id != snap_id {
                    continue;
                }
                let manifest = entry
                    .load_manifest(table.file_io())
                    .await
                    .map_err(map_iceberg_err)?;
                for me in manifest.entries() {
                    let df = me.data_file();
                    let our = DataFile {
                        path: df.file_path().to_string(),
                        record_count: df.record_count(),
                        byte_size: df.file_size_in_bytes(),
                        equality_field_ids: df.equality_ids().unwrap_or_default(),
                    };
                    match df.content_type() {
                        DataContentType::Data => data_files.push(our),
                        DataContentType::EqualityDeletes | DataContentType::PositionDeletes => {
                            delete_files.push(our)
                        }
                    }
                }
            }
            out.push(Snapshot {
                id: seq_num,
                data_files,
                delete_files,
            });
        }
        out.sort_by_key(|s| s.id);
        Ok(out)
    }
}

// ───── translation helpers ───────────────────────────────────────────────

fn to_iceberg_namespace(ns: &Namespace) -> Result<NamespaceIdent> {
    NamespaceIdent::from_strs(ns.0.iter().map(|s| s.as_str()))
        .map_err(|e| IcebergError::Other(format!("namespace ident: {e}")))
}

fn to_iceberg_table_ident(t: &TableIdent) -> Result<IcebergTableIdent> {
    Ok(IcebergTableIdent::new(
        to_iceberg_namespace(&t.namespace)?,
        t.name.clone(),
    ))
}

fn to_iceberg_type(ty: IcebergType) -> Type {
    use IcebergType::*;
    let p = match ty {
        Boolean => PrimitiveType::Boolean,
        Int => PrimitiveType::Int,
        Long => PrimitiveType::Long,
        Float => PrimitiveType::Float,
        Double => PrimitiveType::Double,
        Decimal { precision, scale } => PrimitiveType::Decimal {
            precision: precision as u32,
            scale: scale as u32,
        },
        String => PrimitiveType::String,
        Binary => PrimitiveType::Binary,
        Date => PrimitiveType::Date,
        Time => PrimitiveType::Time,
        Timestamp => PrimitiveType::Timestamp,
        TimestampTz => PrimitiveType::Timestamptz,
        Uuid => PrimitiveType::Uuid,
    };
    Type::Primitive(p)
}

fn from_iceberg_type(ty: &Type) -> Result<IcebergType> {
    let p = match ty {
        Type::Primitive(p) => p,
        Type::Struct(_) | Type::List(_) | Type::Map(_) => {
            return Err(IcebergError::Other(format!(
                "non-primitive iceberg type encountered: {ty:?}"
            )));
        }
    };
    Ok(match p {
        PrimitiveType::Boolean => IcebergType::Boolean,
        PrimitiveType::Int => IcebergType::Int,
        PrimitiveType::Long => IcebergType::Long,
        PrimitiveType::Float => IcebergType::Float,
        PrimitiveType::Double => IcebergType::Double,
        PrimitiveType::Decimal { precision, scale } => IcebergType::Decimal {
            precision: *precision as u8,
            scale: *scale as u8,
        },
        PrimitiveType::String => IcebergType::String,
        PrimitiveType::Binary => IcebergType::Binary,
        PrimitiveType::Date => IcebergType::Date,
        PrimitiveType::Time => IcebergType::Time,
        PrimitiveType::Timestamp => IcebergType::Timestamp,
        PrimitiveType::Timestamptz => IcebergType::TimestampTz,
        PrimitiveType::Uuid => IcebergType::Uuid,
        // Nanosecond timestamps + Fixed are not in our Postgres mapping;
        // surface them as an error rather than silently coercing.
        other => {
            return Err(IcebergError::Other(format!(
                "unsupported iceberg primitive: {other:?}"
            )));
        }
    })
}

fn to_iceberg_schema(schema: &TableSchema) -> Result<IcebergSchema> {
    let pk_ids: Vec<i32> = schema
        .columns
        .iter()
        .filter(|c| c.is_primary_key)
        .map(|c| c.field_id)
        .collect();
    let fields: Vec<_> = schema
        .columns
        .iter()
        .map(|c| {
            let ty = to_iceberg_type(c.ty);
            let nf = if c.nullable {
                NestedField::optional(c.field_id, &c.name, ty)
            } else {
                NestedField::required(c.field_id, &c.name, ty)
            };
            nf.into()
        })
        .collect();
    let mut b = IcebergSchema::builder()
        .with_schema_id(0)
        .with_fields(fields);
    if !pk_ids.is_empty() {
        b = b.with_identifier_field_ids(pk_ids);
    }
    b.build()
        .map_err(|e| IcebergError::Other(format!("schema build: {e}")))
}

fn from_iceberg_schema(ident: &TableIdent, schema: &IcebergSchema) -> Result<TableSchema> {
    let pk_set: std::collections::BTreeSet<i32> = schema.identifier_field_ids().collect();
    let mut columns: Vec<ColumnSchema> = Vec::new();
    for f in schema.as_struct().fields().iter() {
        columns.push(ColumnSchema {
            name: f.name.clone(),
            field_id: f.id,
            ty: from_iceberg_type(&f.field_type)?,
            nullable: !f.required,
            is_primary_key: pk_set.contains(&f.id),
        });
    }
    Ok(TableSchema {
        ident: ident.clone(),
        columns,
    })
}

fn metadata_from_table(ident: &TableIdent, table: &iceberg::table::Table) -> Result<TableMetadata> {
    Ok(TableMetadata {
        ident: ident.clone(),
        schema: from_iceberg_schema(ident, table.metadata().current_schema())?,
        // We surface `sequence_number` rather than the random 63-bit
        // `snapshot_id`, matching `Snapshot.id` in `snapshots()` so callers
        // get consistent monotonic IDs across both surfaces.
        current_snapshot_id: table
            .metadata()
            .current_snapshot()
            .map(|s| s.sequence_number()),
        // `iceberg-rust` does not yet vend per-table credential config back
        // through the Catalog trait surface; populate empty for now. The
        // vended-credentials S3 router runs separately for now.
        config: BTreeMap::new(),
    })
}

fn map_iceberg_err(e: iceberg::Error) -> IcebergError {
    match e.kind() {
        ErrorKind::TableNotFound | ErrorKind::NamespaceNotFound => {
            IcebergError::NotFound(e.to_string())
        }
        ErrorKind::TableAlreadyExists
        | ErrorKind::NamespaceAlreadyExists
        | ErrorKind::CatalogCommitConflicts => IcebergError::Conflict(e.to_string()),
        _ => IcebergError::Other(e.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use iceberg::memory::{MemoryCatalogBuilder, MEMORY_CATALOG_WAREHOUSE};
    use iceberg::CatalogBuilder;
    use pg2iceberg_core::ColumnSchema;

    async fn fresh() -> IcebergRustCatalog<iceberg::memory::MemoryCatalog> {
        let inner = MemoryCatalogBuilder::default()
            .load(
                "test",
                HashMap::from([(
                    MEMORY_CATALOG_WAREHOUSE.to_string(),
                    "memory:///warehouse".to_string(),
                )]),
            )
            .await
            .unwrap();
        IcebergRustCatalog::new(Arc::new(inner))
    }

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
                    ty: IcebergType::Long,
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
        }
    }

    #[tokio::test]
    async fn ensure_namespace_is_idempotent() {
        let c = fresh().await;
        c.ensure_namespace(&ident().namespace).await.unwrap();
        c.ensure_namespace(&ident().namespace).await.unwrap();
    }

    #[tokio::test]
    async fn create_then_load_round_trips_schema() {
        let c = fresh().await;
        c.ensure_namespace(&ident().namespace).await.unwrap();
        let meta = c.create_table(&schema()).await.unwrap();
        // The translated schema should preserve our field ids, types,
        // nullability and PK marking.
        assert_eq!(meta.ident, ident());
        assert_eq!(meta.schema.columns.len(), 3);
        let id = &meta.schema.columns[0];
        assert_eq!(id.field_id, 1);
        assert_eq!(id.ty, IcebergType::Int);
        assert!(id.is_primary_key);
        assert!(!id.nullable);
        let note = &meta.schema.columns[2];
        assert!(note.nullable);
        assert!(!note.is_primary_key);

        let loaded = c.load_table(&ident()).await.unwrap().unwrap();
        assert_eq!(loaded.schema, meta.schema);
        assert!(loaded.current_snapshot_id.is_none());
    }

    #[tokio::test]
    async fn load_table_returns_none_when_missing() {
        let c = fresh().await;
        let got = c.load_table(&ident()).await.unwrap();
        assert!(got.is_none());
    }

    #[tokio::test]
    async fn commit_snapshot_appends_data_file_and_snapshot_history_grows() {
        let c = fresh().await;
        c.ensure_namespace(&ident().namespace).await.unwrap();
        c.create_table(&schema()).await.unwrap();

        for i in 0..3 {
            let meta = c
                .commit_snapshot(PreparedCommit {
                    ident: ident(),
                    data_files: vec![DataFile {
                        path: format!("memory:///warehouse/public/orders/data-{i}.parquet"),
                        record_count: 10 + i,
                        byte_size: 1024 + i * 100,
                        equality_field_ids: vec![],
                    }],
                    equality_deletes: vec![],
                })
                .await
                .unwrap();
            assert!(meta.current_snapshot_id.is_some());
        }

        let snaps = c.snapshots(&ident()).await.unwrap();
        assert_eq!(snaps.len(), 3);
        // Snapshots are returned in ascending id order.
        for w in snaps.windows(2) {
            assert!(w[0].id < w[1].id);
        }
        assert_eq!(snaps[0].data_files.len(), 1);
        assert_eq!(snaps[0].data_files[0].record_count, 10);
        assert_eq!(snaps[2].data_files[0].record_count, 12);
        for s in &snaps {
            assert!(s.delete_files.is_empty());
        }
    }

    #[tokio::test]
    async fn empty_prepared_commit_is_a_noop() {
        let c = fresh().await;
        c.ensure_namespace(&ident().namespace).await.unwrap();
        c.create_table(&schema()).await.unwrap();
        let meta = c
            .commit_snapshot(PreparedCommit {
                ident: ident(),
                data_files: vec![],
                equality_deletes: vec![],
            })
            .await
            .unwrap();
        assert!(meta.current_snapshot_id.is_none());
        assert!(c.snapshots(&ident()).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn equality_delete_commit_returns_clear_unsupported_error() {
        let c = fresh().await;
        c.ensure_namespace(&ident().namespace).await.unwrap();
        c.create_table(&schema()).await.unwrap();
        let err = c
            .commit_snapshot(PreparedCommit {
                ident: ident(),
                data_files: vec![],
                equality_deletes: vec![DataFile {
                    path: "memory:///warehouse/public/orders/eq-deletes-0.parquet".to_string(),
                    record_count: 1,
                    byte_size: 64,
                    equality_field_ids: vec![1],
                }],
            })
            .await
            .unwrap_err();
        assert!(matches!(err, IcebergError::Other(_)));
        let msg = err.to_string();
        assert!(
            msg.contains("equality-delete"),
            "expected message about equality-delete blocker, got: {msg}"
        );
    }

    #[tokio::test]
    async fn snapshots_on_missing_table_returns_empty() {
        let c = fresh().await;
        let snaps = c.snapshots(&ident()).await.unwrap();
        assert!(snaps.is_empty());
    }

    #[tokio::test]
    async fn evolve_schema_returns_not_yet_wired() {
        let c = fresh().await;
        c.ensure_namespace(&ident().namespace).await.unwrap();
        c.create_table(&schema()).await.unwrap();
        let err = c
            .evolve_schema(
                &ident(),
                vec![SchemaChange::AddColumn {
                    name: "new_col".into(),
                    ty: IcebergType::String,
                    nullable: true,
                }],
            )
            .await
            .unwrap_err();
        assert!(matches!(err, IcebergError::Other(_)));
    }

    #[tokio::test]
    async fn nested_namespace_ensure_works() {
        let c = fresh().await;
        let ns = Namespace(vec!["root".into(), "child".into()]);
        // Iceberg's MemoryCatalog needs the parent first.
        c.ensure_namespace(&Namespace(vec!["root".into()]))
            .await
            .unwrap();
        c.ensure_namespace(&ns).await.unwrap();
        c.ensure_namespace(&ns).await.unwrap();
    }

    #[test]
    fn type_round_trip_covers_full_postgres_subset() {
        for ty in [
            IcebergType::Boolean,
            IcebergType::Int,
            IcebergType::Long,
            IcebergType::Float,
            IcebergType::Double,
            IcebergType::Decimal {
                precision: 10,
                scale: 2,
            },
            IcebergType::String,
            IcebergType::Binary,
            IcebergType::Date,
            IcebergType::Time,
            IcebergType::Timestamp,
            IcebergType::TimestampTz,
            IcebergType::Uuid,
        ] {
            let back = from_iceberg_type(&to_iceberg_type(ty)).unwrap();
            assert_eq!(back, ty);
        }
    }
}
