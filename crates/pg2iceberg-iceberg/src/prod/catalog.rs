//! `IcebergRustCatalog`: wraps any `iceberg::Catalog` (Memory, REST, Glue,
//! SQL, ...) behind our [`crate::Catalog`] trait so the materializer can
//! drive a real Iceberg backend.
//!
//! Translation rules:
//!
//! - **Mixed data + equality-delete commits.** `commit_snapshot` builds a
//!   single `Vec<DataFile>` from `prepared.data_files` (content = Data)
//!   and `prepared.equality_deletes` (content = EqualityDeletes), then
//!   submits via [`iceberg::transaction::Transaction::fast_append`]. The
//!   forked `FastAppendAction` routes by `content_type()` into separate
//!   data and delete manifests at commit time.
//! - **Schema evolution.** Translates our `Vec<SchemaChange>` to a target
//!   `iceberg::Schema` (via [`crate::apply_schema_changes`]) and submits
//!   via the forked `Transaction::update_schema()`.
//! - **`load_table` not-found.** Maps `ErrorKind::TableNotFound` and
//!   `NamespaceNotFound` → `Ok(None)` (the materializer treats not-found
//!   distinctly from transient errors).
//!
//! See [`super::gap_audit`] for the full method-by-method status and the
//! list of fork patches we depend on.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use async_trait::async_trait;
use iceberg::spec::{
    DataContentType, DataFile as IcebergDataFile, DataFileBuilder, DataFileFormat, Literal,
    NestedField, PrimitiveLiteral, PrimitiveType, Schema as IcebergSchema, Struct, Type,
};
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::{
    Catalog as IcebergCatalogTrait, ErrorKind, NamespaceIdent, TableCreation,
    TableIdent as IcebergTableIdent,
};
use pg2iceberg_core::{
    typemap::IcebergType, ColumnSchema, Namespace, PartitionLiteral, TableIdent, TableSchema,
};

use crate::{
    apply_schema_changes, Catalog, DataFile, IcebergError, PreparedCommit, Result, SchemaChange,
    Snapshot, TableMetadata,
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
        // TypedBuilder switches type-state when `partition_spec()` is
        // called, so we have to choose at compile time which arm to
        // build. The `clone()` on `ice_schema` is the cost of avoiding
        // a more elaborate dynamic-build dance.
        let creation = if schema.partition_spec.is_empty() {
            TableCreation::builder()
                .name(schema.ident.name.clone())
                .schema(ice_schema)
                .build()
        } else {
            let unbound = to_iceberg_unbound_partition_spec(schema)?;
            TableCreation::builder()
                .name(schema.ident.name.clone())
                .schema(ice_schema)
                .partition_spec(unbound)
                .build()
        };
        let table = self
            .inner
            .create_table(&ns, creation)
            .await
            .map_err(map_iceberg_err)?;
        metadata_from_table(&schema.ident, &table)
    }

    async fn commit_snapshot(&self, prepared: PreparedCommit) -> Result<TableMetadata> {
        if prepared.data_files.is_empty() && prepared.equality_deletes.is_empty() {
            // No work — match the sim-catalog noop semantics so the materializer
            // can flush "no data, no deletes" without a snapshot bump.
            let it = to_iceberg_table_ident(&prepared.ident)?;
            let table = self.inner.load_table(&it).await.map_err(map_iceberg_err)?;
            return metadata_from_table(&prepared.ident, &table);
        }

        let it = to_iceberg_table_ident(&prepared.ident)?;
        let table = self.inner.load_table(&it).await.map_err(map_iceberg_err)?;
        let spec_id = table.metadata().default_partition_spec_id();
        let part_field_count = table.metadata().default_partition_spec().fields().len();

        let mut all_files: Vec<IcebergDataFile> =
            Vec::with_capacity(prepared.data_files.len() + prepared.equality_deletes.len());
        for df in &prepared.data_files {
            let partition =
                build_partition_struct(&df.partition_values, part_field_count, &df.path)?;
            all_files.push(
                DataFileBuilder::default()
                    .content(DataContentType::Data)
                    .file_path(df.path.clone())
                    .file_format(DataFileFormat::Parquet)
                    .file_size_in_bytes(df.byte_size)
                    .record_count(df.record_count)
                    .partition(partition)
                    .partition_spec_id(spec_id)
                    .build()
                    .map_err(|e| IcebergError::Other(format!("data file build: {e}")))?,
            );
        }
        for df in &prepared.equality_deletes {
            if df.equality_field_ids.is_empty() {
                return Err(IcebergError::Other(format!(
                    "equality-delete file {} has empty equality_field_ids; refusing to \
                     commit a delete that wouldn't match any rows",
                    df.path
                )));
            }
            let partition =
                build_partition_struct(&df.partition_values, part_field_count, &df.path)?;
            all_files.push(
                DataFileBuilder::default()
                    .content(DataContentType::EqualityDeletes)
                    .file_path(df.path.clone())
                    .file_format(DataFileFormat::Parquet)
                    .file_size_in_bytes(df.byte_size)
                    .record_count(df.record_count)
                    .equality_ids(Some(df.equality_field_ids.clone()))
                    .partition(partition)
                    .partition_spec_id(spec_id)
                    .build()
                    .map_err(|e| IcebergError::Other(format!("delete file build: {e}")))?,
            );
        }

        let tx = Transaction::new(&table);
        let action = tx
            .fast_append()
            // The materializer guarantees unique paths via `RollingWriter`'s
            // counter-based namer; skip iceberg-rust's path-dedup which would
            // otherwise scan the full manifest list on each commit.
            .with_check_duplicate(false)
            // FastAppendAction (forked) routes by `content_type()` into
            // separate data and delete manifests at commit time.
            .add_data_files(all_files);
        let tx = action.apply(tx).map_err(map_iceberg_err)?;
        let updated = tx
            .commit(self.inner.as_ref())
            .await
            .map_err(map_iceberg_err)?;
        metadata_from_table(&prepared.ident, &updated)
    }

    async fn evolve_schema(
        &self,
        ident: &TableIdent,
        changes: Vec<SchemaChange>,
    ) -> Result<TableMetadata> {
        if changes.is_empty() {
            // Match the sim semantics: a no-op evolve still returns current
            // metadata rather than erroring.
            let it = to_iceberg_table_ident(ident)?;
            let table = self.inner.load_table(&it).await.map_err(map_iceberg_err)?;
            return metadata_from_table(ident, &table);
        }

        let it = to_iceberg_table_ident(ident)?;
        let table = self.inner.load_table(&it).await.map_err(map_iceberg_err)?;

        // Translate iceberg schema → our shape, apply changes, translate back.
        // Keeping the round-trip in our type domain centralizes field-id
        // allocation rules (next id = current highest + 1) and the soft-drop
        // semantics for `DropColumn`.
        let part_spec = table.metadata().default_partition_spec();
        let mut our_schema =
            from_iceberg_schema(ident, table.metadata().current_schema(), part_spec.as_ref())?;
        apply_schema_changes(&mut our_schema, &changes)?;
        let new_iceberg_schema = to_iceberg_schema(&our_schema)?;

        let tx = Transaction::new(&table);
        let action = tx.update_schema().set_schema(new_iceberg_schema);
        let tx = action.apply(tx).map_err(map_iceberg_err)?;
        let updated = tx
            .commit(self.inner.as_ref())
            .await
            .map_err(map_iceberg_err)?;
        metadata_from_table(ident, &updated)
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
                    let partition_values = iceberg_struct_to_partition_literals(df.partition());
                    let our = DataFile {
                        path: df.file_path().to_string(),
                        record_count: df.record_count(),
                        byte_size: df.file_size_in_bytes(),
                        equality_field_ids: df.equality_ids().unwrap_or_default(),
                        partition_values,
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

fn from_iceberg_schema(
    ident: &TableIdent,
    schema: &IcebergSchema,
    partition_spec: &iceberg::spec::PartitionSpec,
) -> Result<TableSchema> {
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

    // Build a `field_id -> column_name` index so we can resolve
    // PartitionField source IDs back to column names.
    let id_to_name: std::collections::HashMap<i32, String> = columns
        .iter()
        .map(|c| (c.field_id, c.name.clone()))
        .collect();
    let partition_fields = partition_spec
        .fields()
        .iter()
        .map(|f| {
            let source_column = id_to_name.get(&f.source_id).cloned().ok_or_else(|| {
                IcebergError::Other(format!(
                    "partition field {} references unknown source_id {}",
                    f.name, f.source_id
                ))
            })?;
            let transform = from_iceberg_transform(&f.transform)?;
            Ok(pg2iceberg_core::PartitionField {
                source_column,
                name: f.name.clone(),
                transform,
            })
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(TableSchema {
        ident: ident.clone(),
        columns,
        partition_spec: partition_fields,
    })
}

fn from_iceberg_transform(t: &iceberg::spec::Transform) -> Result<pg2iceberg_core::Transform> {
    use iceberg::spec::Transform as IT;
    use pg2iceberg_core::Transform as OT;
    Ok(match t {
        IT::Identity => OT::Identity,
        IT::Year => OT::Year,
        IT::Month => OT::Month,
        IT::Day => OT::Day,
        IT::Hour => OT::Hour,
        IT::Bucket(n) => OT::Bucket(*n),
        IT::Truncate(n) => OT::Truncate(*n),
        other => {
            return Err(IcebergError::Other(format!(
                "iceberg partition transform {other:?} is not supported by pg2iceberg"
            )))
        }
    })
}

fn to_iceberg_transform(t: pg2iceberg_core::Transform) -> iceberg::spec::Transform {
    use iceberg::spec::Transform as IT;
    use pg2iceberg_core::Transform as OT;
    match t {
        OT::Identity => IT::Identity,
        OT::Year => IT::Year,
        OT::Month => IT::Month,
        OT::Day => IT::Day,
        OT::Hour => IT::Hour,
        OT::Bucket(n) => IT::Bucket(n),
        OT::Truncate(n) => IT::Truncate(n),
    }
}

/// Build an `iceberg::spec::UnboundPartitionSpec` from our schema's
/// `partition_spec`. We use the *unbound* variant because at
/// `create_table` time the iceberg schema doesn't yet have stable
/// field ids for the partition fields; `TableMetadataBuilder` binds
/// them when the table metadata is constructed.
fn to_iceberg_unbound_partition_spec(
    schema: &TableSchema,
) -> Result<iceberg::spec::UnboundPartitionSpec> {
    let mut builder = iceberg::spec::UnboundPartitionSpec::builder();
    for f in &schema.partition_spec {
        let source_id = schema.field_id_for(&f.source_column).ok_or_else(|| {
            IcebergError::Other(format!(
                "partition source column {} not in schema",
                f.source_column
            ))
        })?;
        builder = builder
            .add_partition_field(source_id, f.name.clone(), to_iceberg_transform(f.transform))
            .map_err(|e| IcebergError::Other(format!("add partition field {}: {e}", f.name)))?;
    }
    Ok(builder.build())
}

/// Translate our per-file `partition_values` to an iceberg `Struct`. Length
/// must match the table's partition spec; we error rather than pad/truncate
/// to keep upstream bugs visible.
fn build_partition_struct(
    values: &[PartitionLiteral],
    expected_field_count: usize,
    file_path: &str,
) -> Result<Struct> {
    if values.len() != expected_field_count {
        return Err(IcebergError::Other(format!(
            "file {} carries {} partition values but the table's default spec has {}; \
             writer and catalog disagree on partition arity",
            file_path,
            values.len(),
            expected_field_count
        )));
    }
    if expected_field_count == 0 {
        return Ok(Struct::empty());
    }
    let lits: Vec<Option<Literal>> = values.iter().map(partition_literal_to_iceberg).collect();
    Ok(Struct::from_iter(lits))
}

/// Inverse of [`partition_literal_to_iceberg`]. Used by `snapshots()` to
/// surface partition values back through our `DataFile`.
fn iceberg_struct_to_partition_literals(s: &Struct) -> Vec<PartitionLiteral> {
    s.iter().map(iceberg_literal_to_partition).collect()
}

fn iceberg_literal_to_partition(lit: Option<&Literal>) -> PartitionLiteral {
    use pg2iceberg_core::partition::{f32_no_nan::F32, f64_no_nan::F64};
    let Some(Literal::Primitive(p)) = lit else {
        return PartitionLiteral::Null;
    };
    match p {
        PrimitiveLiteral::Boolean(b) => PartitionLiteral::Boolean(*b),
        PrimitiveLiteral::Int(n) => PartitionLiteral::Int(*n),
        PrimitiveLiteral::Long(n) => PartitionLiteral::Long(*n),
        PrimitiveLiteral::Float(f) => PartitionLiteral::Float(F32(f.0)),
        PrimitiveLiteral::Double(f) => PartitionLiteral::Double(F64(f.0)),
        PrimitiveLiteral::String(s) => PartitionLiteral::String(s.clone()),
        PrimitiveLiteral::Binary(b) => PartitionLiteral::Binary(b.clone()),
        // UUID partition values were translated as 16-byte BE; round-trip them
        // back as raw binary so the writer's identity-on-UUID-source stays
        // self-consistent.
        PrimitiveLiteral::UInt128(u) => PartitionLiteral::Binary(u.to_be_bytes().to_vec()),
        // Decimal partition values: iceberg stores the unscaled `i128`. We
        // can't recover scale from the literal alone (it lives in the
        // partition spec field type) so we report 0 — verifier filtering
        // works on unscaled equality, which is what iceberg actually
        // compares on. Operators displaying partition values for human
        // consumption should consult the schema for the scale.
        PrimitiveLiteral::Int128(n) => PartitionLiteral::Decimal {
            unscaled: *n,
            scale: 0,
        },
        // AboveMax / BelowMin: surface as Null rather than panic so reads
        // of foreign-written partition values don't crash the verifier.
        _ => PartitionLiteral::Null,
    }
}

fn partition_literal_to_iceberg(lit: &PartitionLiteral) -> Option<Literal> {
    use pg2iceberg_core::partition::{f32_no_nan::F32, f64_no_nan::F64};
    match lit {
        PartitionLiteral::Null => None,
        PartitionLiteral::Boolean(b) => Some(Literal::bool(*b)),
        PartitionLiteral::Int(n) => Some(Literal::int(*n)),
        PartitionLiteral::Long(n) => Some(Literal::long(*n)),
        PartitionLiteral::Float(F32(f)) => Some(Literal::float(*f)),
        PartitionLiteral::Double(F64(f)) => Some(Literal::double(*f)),
        PartitionLiteral::String(s) => Some(Literal::string(s)),
        // For now Binary covers raw bytes and UUID identity-partition values.
        // iceberg-rust's UUID literal stores `UInt128`; if that becomes a
        // real validation issue we'll add a `PartitionLiteral::Uuid` variant
        // and translate it here. Today's writer rejects UUID identity
        // partitioning before it reaches the catalog (see `apply_transform`
        // for `PgValue::Uuid`).
        PartitionLiteral::Binary(b) => {
            if b.len() == 16 {
                let arr: [u8; 16] = b.as_slice().try_into().expect("len-checked");
                Some(Literal::Primitive(PrimitiveLiteral::UInt128(
                    u128::from_be_bytes(arr),
                )))
            } else {
                Some(Literal::binary(b.clone()))
            }
        }
        PartitionLiteral::Decimal { unscaled, .. } => Some(Literal::decimal(*unscaled)),
    }
}

fn metadata_from_table(ident: &TableIdent, table: &iceberg::table::Table) -> Result<TableMetadata> {
    let part_spec = table.metadata().default_partition_spec();
    Ok(TableMetadata {
        ident: ident.clone(),
        schema: from_iceberg_schema(ident, table.metadata().current_schema(), part_spec.as_ref())?,
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
            partition_spec: Vec::new(),
        }
    }

    /// `orders` schema with a `created_at` timestamp column, ready to
    /// be partitioned by day.
    fn schema_with_timestamp() -> TableSchema {
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
                    name: "created_at".into(),
                    field_id: 2,
                    ty: IcebergType::TimestampTz,
                    nullable: false,
                    is_primary_key: false,
                },
            ],
            partition_spec: Vec::new(),
        }
    }

    #[tokio::test]
    async fn create_table_with_identity_partition_spec_round_trips() {
        use pg2iceberg_core::Transform;
        let c = fresh().await;
        c.ensure_namespace(&ident().namespace).await.unwrap();
        let mut s = schema();
        s.partition_spec = vec![pg2iceberg_core::PartitionField {
            source_column: "qty".into(),
            name: "qty".into(),
            transform: Transform::Identity,
        }];
        let meta = c.create_table(&s).await.unwrap();
        assert_eq!(meta.schema.partition_spec.len(), 1);
        assert_eq!(meta.schema.partition_spec[0].source_column, "qty");
        assert_eq!(meta.schema.partition_spec[0].transform, Transform::Identity);

        // Reload via load_table — confirms the partition spec round-trips
        // through the Iceberg metadata read path.
        let reloaded = c.load_table(&ident()).await.unwrap().unwrap();
        assert_eq!(reloaded.schema.partition_spec, meta.schema.partition_spec);
    }

    #[tokio::test]
    async fn create_table_with_day_transform_round_trips() {
        use pg2iceberg_core::Transform;
        let c = fresh().await;
        c.ensure_namespace(&ident().namespace).await.unwrap();
        let mut s = schema_with_timestamp();
        s.partition_spec = vec![pg2iceberg_core::PartitionField {
            source_column: "created_at".into(),
            name: "created_at_day".into(),
            transform: Transform::Day,
        }];
        let meta = c.create_table(&s).await.unwrap();
        assert_eq!(meta.schema.partition_spec.len(), 1);
        assert_eq!(meta.schema.partition_spec[0].source_column, "created_at");
        assert_eq!(meta.schema.partition_spec[0].transform, Transform::Day);
        assert_eq!(meta.schema.partition_spec[0].name, "created_at_day");
    }

    #[tokio::test]
    async fn commit_to_partitioned_table_with_partition_values_round_trips() {
        use pg2iceberg_core::{PartitionLiteral, Transform};
        let c = fresh().await;
        c.ensure_namespace(&ident().namespace).await.unwrap();
        let mut s = schema();
        s.partition_spec = vec![pg2iceberg_core::PartitionField {
            source_column: "qty".into(),
            name: "qty".into(),
            transform: Transform::Identity,
        }];
        c.create_table(&s).await.unwrap();
        // Two distinct partition values land in the same snapshot as two
        // separate data files.
        let meta = c
            .commit_snapshot(PreparedCommit {
                ident: ident(),
                data_files: vec![
                    DataFile {
                        path: "memory:///warehouse/public/orders/data-qty-1.parquet".into(),
                        record_count: 3,
                        byte_size: 256,
                        equality_field_ids: vec![],
                        partition_values: vec![PartitionLiteral::Long(1)],
                    },
                    DataFile {
                        path: "memory:///warehouse/public/orders/data-qty-2.parquet".into(),
                        record_count: 5,
                        byte_size: 384,
                        equality_field_ids: vec![],
                        partition_values: vec![PartitionLiteral::Long(2)],
                    },
                ],
                equality_deletes: vec![],
            })
            .await
            .unwrap();
        assert!(meta.current_snapshot_id.is_some());
        let snaps = c.snapshots(&ident()).await.unwrap();
        assert_eq!(snaps.len(), 1);
        assert_eq!(snaps[0].data_files.len(), 2);
        let mut by_qty: std::collections::BTreeMap<i64, u64> = std::collections::BTreeMap::new();
        for df in &snaps[0].data_files {
            assert_eq!(df.partition_values.len(), 1);
            if let PartitionLiteral::Long(q) = &df.partition_values[0] {
                by_qty.insert(*q, df.record_count);
            } else {
                panic!(
                    "expected Long partition value, got {:?}",
                    df.partition_values[0]
                );
            }
        }
        assert_eq!(by_qty.get(&1), Some(&3));
        assert_eq!(by_qty.get(&2), Some(&5));
    }

    #[tokio::test]
    async fn writer_to_catalog_roundtrips_partition_values_for_identity_partitioned_table() {
        use crate::TableWriter;
        use pg2iceberg_core::value::PgValue;
        use pg2iceberg_core::{ColumnName, Op, PartitionLiteral, Transform};
        use std::collections::BTreeMap;

        let c = fresh().await;
        c.ensure_namespace(&ident().namespace).await.unwrap();
        let mut s = schema();
        // Partition by `note` (string identity). Force `note` non-nullable
        // for this test so the writer doesn't have to handle null partitions.
        s.columns[2].nullable = false;
        s.partition_spec = vec![pg2iceberg_core::PartitionField {
            source_column: "note".into(),
            name: "note".into(),
            transform: Transform::Identity,
        }];
        c.create_table(&s).await.unwrap();

        // Prepare via TableWriter: 2 inserts into "us", 1 into "eu" → two
        // data chunks tagged with the right partition tuples.
        let w = TableWriter::new(s.clone());
        let mut row_a = BTreeMap::new();
        row_a.insert(ColumnName("id".into()), PgValue::Int4(1));
        row_a.insert(ColumnName("qty".into()), PgValue::Int8(10));
        row_a.insert(ColumnName("note".into()), PgValue::Text("us".into()));
        let mut row_b = BTreeMap::new();
        row_b.insert(ColumnName("id".into()), PgValue::Int4(2));
        row_b.insert(ColumnName("qty".into()), PgValue::Int8(20));
        row_b.insert(ColumnName("note".into()), PgValue::Text("us".into()));
        let mut row_c = BTreeMap::new();
        row_c.insert(ColumnName("id".into()), PgValue::Int4(3));
        row_c.insert(ColumnName("qty".into()), PgValue::Int8(30));
        row_c.insert(ColumnName("note".into()), PgValue::Text("eu".into()));
        let prepared = w
            .prepare(
                &[
                    crate::MaterializedRow {
                        op: Op::Insert,
                        row: row_a,
                        unchanged_cols: vec![],
                    },
                    crate::MaterializedRow {
                        op: Op::Insert,
                        row: row_b,
                        unchanged_cols: vec![],
                    },
                    crate::MaterializedRow {
                        op: Op::Insert,
                        row: row_c,
                        unchanged_cols: vec![],
                    },
                ],
                &crate::FileIndex::new(),
            )
            .unwrap();
        assert_eq!(prepared.data.len(), 2);

        // Commit each chunk as a separate DataFile carrying its
        // partition_values. This is the same shape the materializer
        // produces.
        let mut data_files: Vec<DataFile> = Vec::new();
        for (i, chunk) in prepared.data.into_iter().enumerate() {
            data_files.push(DataFile {
                path: format!("memory:///warehouse/public/orders/data-{i}.parquet"),
                record_count: chunk.chunk.record_count,
                byte_size: chunk.chunk.bytes.len() as u64,
                equality_field_ids: vec![],
                partition_values: chunk.partition_values,
            });
        }
        c.commit_snapshot(PreparedCommit {
            ident: ident(),
            data_files,
            equality_deletes: vec![],
        })
        .await
        .unwrap();

        // Read back: snapshots() must surface the same partition values we
        // wrote. This also covers `iceberg_struct_to_partition_literals`.
        let snaps = c.snapshots(&ident()).await.unwrap();
        assert_eq!(snaps.len(), 1);
        assert_eq!(snaps[0].data_files.len(), 2);
        let mut by_region: BTreeMap<String, u64> = BTreeMap::new();
        for df in &snaps[0].data_files {
            assert_eq!(df.partition_values.len(), 1);
            if let PartitionLiteral::String(r) = &df.partition_values[0] {
                by_region.insert(r.clone(), df.record_count);
            } else {
                panic!(
                    "expected String partition value, got {:?}",
                    df.partition_values[0]
                );
            }
        }
        assert_eq!(by_region.get("us"), Some(&2));
        assert_eq!(by_region.get("eu"), Some(&1));
    }

    #[tokio::test]
    async fn commit_to_partitioned_table_with_arity_mismatch_errors() {
        use pg2iceberg_core::Transform;
        let c = fresh().await;
        c.ensure_namespace(&ident().namespace).await.unwrap();
        let mut s = schema();
        s.partition_spec = vec![pg2iceberg_core::PartitionField {
            source_column: "qty".into(),
            name: "qty".into(),
            transform: Transform::Identity,
        }];
        c.create_table(&s).await.unwrap();
        // Empty partition_values for a partitioned table should error with a
        // clear arity mismatch — not silently land at `Struct::empty()`.
        let err = c
            .commit_snapshot(PreparedCommit {
                ident: ident(),
                data_files: vec![DataFile {
                    path: "memory:///warehouse/public/orders/data-bogus.parquet".into(),
                    record_count: 1,
                    byte_size: 64,
                    equality_field_ids: vec![],
                    partition_values: Vec::new(),
                }],
                equality_deletes: vec![],
            })
            .await
            .unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("partition arity") || msg.contains("partition values"),
            "expected arity-mismatch error, got: {msg}"
        );
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
                        partition_values: Vec::new(),
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
    async fn commit_snapshot_with_only_equality_deletes_produces_a_snapshot() {
        // After the fork patch, equality-delete commits flow through the same
        // FastAppendAction path as data commits. A delete-only commit should
        // still produce a snapshot whose delete_files list is non-empty.
        let c = fresh().await;
        c.ensure_namespace(&ident().namespace).await.unwrap();
        c.create_table(&schema()).await.unwrap();
        let meta = c
            .commit_snapshot(PreparedCommit {
                ident: ident(),
                data_files: vec![],
                equality_deletes: vec![DataFile {
                    path: "memory:///warehouse/public/orders/eq-deletes-0.parquet".into(),
                    record_count: 1,
                    byte_size: 64,
                    equality_field_ids: vec![1],
                    partition_values: Vec::new(),
                }],
            })
            .await
            .unwrap();
        assert!(meta.current_snapshot_id.is_some());

        let snaps = c.snapshots(&ident()).await.unwrap();
        assert_eq!(snaps.len(), 1);
        assert!(snaps[0].data_files.is_empty());
        assert_eq!(snaps[0].delete_files.len(), 1);
        assert_eq!(snaps[0].delete_files[0].equality_field_ids, vec![1]);
    }

    #[tokio::test]
    async fn commit_snapshot_with_data_plus_equality_deletes_lands_in_one_snapshot() {
        let c = fresh().await;
        c.ensure_namespace(&ident().namespace).await.unwrap();
        c.create_table(&schema()).await.unwrap();
        let meta = c
            .commit_snapshot(PreparedCommit {
                ident: ident(),
                data_files: vec![DataFile {
                    path: "memory:///warehouse/public/orders/data-0.parquet".into(),
                    record_count: 5,
                    byte_size: 1024,
                    equality_field_ids: vec![],
                    partition_values: Vec::new(),
                }],
                equality_deletes: vec![DataFile {
                    path: "memory:///warehouse/public/orders/eq-deletes-0.parquet".into(),
                    record_count: 2,
                    byte_size: 128,
                    equality_field_ids: vec![1],
                    partition_values: Vec::new(),
                }],
            })
            .await
            .unwrap();
        assert!(meta.current_snapshot_id.is_some());

        let snaps = c.snapshots(&ident()).await.unwrap();
        // Both files belong to the same snapshot — not two.
        assert_eq!(snaps.len(), 1);
        assert_eq!(snaps[0].data_files.len(), 1);
        assert_eq!(snaps[0].delete_files.len(), 1);
        assert_eq!(snaps[0].data_files[0].record_count, 5);
        assert_eq!(snaps[0].delete_files[0].record_count, 2);
    }

    #[tokio::test]
    async fn commit_snapshot_rejects_delete_file_with_empty_equality_field_ids() {
        // An equality-delete file with no field-id list would match no rows
        // (or every row, depending on reader). Refuse rather than silently
        // commit a meaningless delete.
        let c = fresh().await;
        c.ensure_namespace(&ident().namespace).await.unwrap();
        c.create_table(&schema()).await.unwrap();
        let err = c
            .commit_snapshot(PreparedCommit {
                ident: ident(),
                data_files: vec![],
                equality_deletes: vec![DataFile {
                    path: "memory:///warehouse/public/orders/eq-deletes-bad.parquet".into(),
                    record_count: 1,
                    byte_size: 64,
                    equality_field_ids: vec![],
                    partition_values: Vec::new(),
                }],
            })
            .await
            .unwrap_err();
        assert!(matches!(err, IcebergError::Other(_)));
        assert!(err.to_string().contains("empty equality_field_ids"));
    }

    #[tokio::test]
    async fn snapshots_on_missing_table_returns_empty() {
        let c = fresh().await;
        let snaps = c.snapshots(&ident()).await.unwrap();
        assert!(snaps.is_empty());
    }

    #[tokio::test]
    async fn evolve_schema_add_column_appends_to_schema_with_fresh_field_id() {
        let c = fresh().await;
        c.ensure_namespace(&ident().namespace).await.unwrap();
        c.create_table(&schema()).await.unwrap();
        let meta = c
            .evolve_schema(
                &ident(),
                vec![SchemaChange::AddColumn {
                    name: "new_col".into(),
                    ty: IcebergType::String,
                    nullable: true,
                }],
            )
            .await
            .unwrap();
        assert_eq!(meta.schema.columns.len(), 4);
        let new_col = meta
            .schema
            .columns
            .iter()
            .find(|c| c.name == "new_col")
            .expect("new_col must be in schema after evolve");
        // Original schema had ids 1,2,3 — the new column should get 4.
        assert_eq!(new_col.field_id, 4);
        assert!(new_col.nullable);
        assert!(!new_col.is_primary_key);

        // Re-loading via load_table sees the same evolved schema.
        let reloaded = c.load_table(&ident()).await.unwrap().unwrap();
        assert_eq!(reloaded.schema, meta.schema);
    }

    #[tokio::test]
    async fn evolve_schema_drop_column_is_soft_drop_makes_column_nullable() {
        let c = fresh().await;
        c.ensure_namespace(&ident().namespace).await.unwrap();
        c.create_table(&schema()).await.unwrap();
        // `qty` (column 2) is non-nullable in the test schema.
        let pre = c.load_table(&ident()).await.unwrap().unwrap();
        assert!(!pre.schema.columns[1].nullable);

        let meta = c
            .evolve_schema(
                &ident(),
                vec![SchemaChange::DropColumn { name: "qty".into() }],
            )
            .await
            .unwrap();
        // Column count unchanged — soft-drop preserves it.
        assert_eq!(meta.schema.columns.len(), 3);
        let qty = meta
            .schema
            .columns
            .iter()
            .find(|c| c.name == "qty")
            .unwrap();
        assert!(qty.nullable, "soft-drop should mark column nullable");
        // field_id is preserved across the evolve.
        assert_eq!(qty.field_id, 2);
    }

    #[tokio::test]
    async fn evolve_schema_empty_changes_is_a_noop() {
        let c = fresh().await;
        c.ensure_namespace(&ident().namespace).await.unwrap();
        let original = c.create_table(&schema()).await.unwrap();
        let after = c.evolve_schema(&ident(), vec![]).await.unwrap();
        assert_eq!(original.schema, after.schema);
    }

    #[tokio::test]
    async fn evolve_schema_add_existing_column_errors() {
        let c = fresh().await;
        c.ensure_namespace(&ident().namespace).await.unwrap();
        c.create_table(&schema()).await.unwrap();
        let err = c
            .evolve_schema(
                &ident(),
                vec![SchemaChange::AddColumn {
                    name: "qty".into(),
                    ty: IcebergType::Long,
                    nullable: true,
                }],
            )
            .await
            .unwrap_err();
        assert!(matches!(err, IcebergError::Conflict(_)));
    }

    #[tokio::test]
    async fn evolve_schema_drop_unknown_column_errors() {
        let c = fresh().await;
        c.ensure_namespace(&ident().namespace).await.unwrap();
        c.create_table(&schema()).await.unwrap();
        let err = c
            .evolve_schema(
                &ident(),
                vec![SchemaChange::DropColumn {
                    name: "ghost".into(),
                }],
            )
            .await
            .unwrap_err();
        assert!(matches!(err, IcebergError::NotFound(_)));
    }

    #[tokio::test]
    async fn evolve_schema_then_commit_snapshot_uses_new_schema_id() {
        // After an evolve, subsequent commits should target the new schema
        // version. We can't observe the schema id directly through our
        // metadata surface (we only carry sequence_number for current_snapshot_id),
        // but we can confirm the round-trip stays self-consistent: evolve,
        // commit a data file, reload, and assert the evolved column is
        // still present.
        let c = fresh().await;
        c.ensure_namespace(&ident().namespace).await.unwrap();
        c.create_table(&schema()).await.unwrap();
        c.evolve_schema(
            &ident(),
            vec![SchemaChange::AddColumn {
                name: "added".into(),
                ty: IcebergType::Int,
                nullable: true,
            }],
        )
        .await
        .unwrap();
        c.commit_snapshot(PreparedCommit {
            ident: ident(),
            data_files: vec![DataFile {
                path: "memory:///warehouse/public/orders/data-0.parquet".into(),
                record_count: 1,
                byte_size: 256,
                equality_field_ids: vec![],
                partition_values: Vec::new(),
            }],
            equality_deletes: vec![],
        })
        .await
        .unwrap();

        let reloaded = c.load_table(&ident()).await.unwrap().unwrap();
        assert!(reloaded.schema.columns.iter().any(|c| c.name == "added"));
        assert!(reloaded.current_snapshot_id.is_some());
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
