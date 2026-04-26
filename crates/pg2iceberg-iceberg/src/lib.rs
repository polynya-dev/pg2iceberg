//! Catalog and TableWriter trait surface.
//!
//! Wraps `iceberg-rust` in production. The sim impl in `pg2iceberg-sim` keeps
//! metadata in memory.

pub mod compact;
pub mod file_index;
pub mod fold;
pub mod materialize;
#[cfg(feature = "prod")]
pub mod prod;
pub mod reader;
pub mod verify;
pub mod writer;

pub use compact::{compact_table, CompactError, CompactionConfig, CompactionOutcome};
pub use file_index::{rebuild_from_catalog, FileIndex};
pub use fold::{fold_events, pk_key, MaterializedRow};
pub use materialize::{promote_re_inserts, resolve_unchanged_cols};
pub use reader::read_data_file;
pub use verify::read_materialized_state;
pub use writer::{DataChunk, PreparedChunk, PreparedFiles, TableWriter, WriterError};

use async_trait::async_trait;
use pg2iceberg_core::{Namespace, PartitionLiteral, TableIdent, TableSchema};
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Clone, Debug, Error)]
pub enum IcebergError {
    #[error("not found: {0}")]
    NotFound(String),
    #[error("conflict: {0}")]
    Conflict(String),
    #[error("other: {0}")]
    Other(String),
}

pub type Result<T> = std::result::Result<T, IcebergError>;

/// Opaque token returned by the catalog representing the current table state.
/// Carry it through the prepare/commit cycle so we can detect concurrent
/// modifications.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TableMetadata {
    pub ident: TableIdent,
    pub schema: TableSchema,
    pub current_snapshot_id: Option<i64>,
    /// Catalog-vended config (e.g. `s3.access-key-id` etc.). Used by the
    /// vended-credentials S3 router.
    pub config: std::collections::BTreeMap<String, String>,
}

/// Built by the materializer (combining `TableWriter::prepare` output with
/// blob-store paths) and consumed by [`Catalog::commit_snapshot`].
#[derive(Clone, Debug)]
pub struct PreparedCommit {
    pub ident: TableIdent,
    pub data_files: Vec<DataFile>,
    pub equality_deletes: Vec<DataFile>,
}

/// Built by [`crate::compact::compact_table`] and consumed by
/// [`Catalog::commit_compaction`]. Produces an `Operation::Replace`
/// snapshot: drop everything in `removed_paths`, add everything in
/// `added_data_files` atomically.
#[derive(Clone, Debug)]
pub struct PreparedCompaction {
    pub ident: TableIdent,
    /// Newly written compacted data files (and any equality-delete files
    /// the rewrite leaves in place — usually empty since compaction
    /// applies pending deletes inline).
    pub added_data_files: Vec<DataFile>,
    /// File paths that this compaction supersedes. Iceberg will drop them
    /// from the new snapshot's manifest list; readers see the compacted
    /// output instead.
    pub removed_paths: Vec<String>,
}

#[derive(Clone, Debug)]
pub struct DataFile {
    pub path: String,
    pub record_count: u64,
    pub byte_size: u64,
    /// For equality-delete files, the field IDs that participate in the
    /// equality predicate (typically the PK columns). Empty for data files.
    pub equality_field_ids: Vec<i32>,
    /// One literal per partition spec field, in `TableSchema.partition_spec`
    /// order. Empty for unpartitioned tables. The catalog translates this to
    /// an `iceberg::spec::Struct` at commit time.
    pub partition_values: Vec<PartitionLiteral>,
}

/// One Iceberg snapshot. Snapshots are append-only and ordered by `id`.
/// Iceberg MoR semantics: a delete file at snapshot `N` applies only to data
/// files at snapshots `< N` (data and deletes from the same snapshot are
/// kept consistent by the materializer).
///
/// Compaction snapshots populate `removed_paths` with the paths of data /
/// delete files that were live in prior snapshots but are no longer
/// referenced by this snapshot's manifest list. Verifier and FileIndex
/// rebuild paths skip any data/delete file whose path appears in a
/// compaction's `removed_paths` set.
#[derive(Clone, Debug)]
pub struct Snapshot {
    pub id: i64,
    pub data_files: Vec<DataFile>,
    pub delete_files: Vec<DataFile>,
    /// File paths superseded by this snapshot. Empty for non-compaction
    /// snapshots.
    pub removed_paths: Vec<String>,
    /// Wall-clock timestamp in milliseconds since epoch. Used by
    /// snapshot expiry — `expire_snapshots(retention)` drops snapshots
    /// older than `now - retention`. Surfaced from
    /// `iceberg::spec::Snapshot::timestamp_ms()` in the prod path; the
    /// sim catalog populates it from its `Clock` source.
    pub timestamp_ms: i64,
}

#[derive(Clone, Debug)]
pub enum SchemaChange {
    AddColumn {
        name: String,
        ty: pg2iceberg_core::IcebergType,
        nullable: bool,
    },
    /// Soft-drop: column is retained as nullable in Iceberg.
    DropColumn { name: String },
}

/// Apply [`SchemaChange`] variants in-place to a [`TableSchema`]. Shared
/// between the sim and prod catalogs so they handle field-id allocation
/// and soft-drop semantics identically.
///
/// - `AddColumn` appends a new non-PK column. Field id = current highest +
///   1 (Iceberg's metadata builder forbids id reuse, so monotonic allocation
///   is the only safe choice).
/// - `DropColumn` is a soft drop — the column stays in the schema but
///   becomes nullable. Preserves read compatibility for older data files
///   that still carry the column.
///
/// Errors on duplicate adds and drops of unknown columns so the caller
/// doesn't silently no-op.
pub fn apply_schema_changes(
    schema: &mut pg2iceberg_core::TableSchema,
    changes: &[SchemaChange],
) -> Result<()> {
    for change in changes {
        match change {
            SchemaChange::AddColumn { name, ty, nullable } => {
                if schema.columns.iter().any(|c| c.name == *name) {
                    return Err(IcebergError::Conflict(format!(
                        "AddColumn: column {name} already exists"
                    )));
                }
                let next_id = schema.columns.iter().map(|c| c.field_id).max().unwrap_or(0) + 1;
                schema.columns.push(pg2iceberg_core::ColumnSchema {
                    name: name.clone(),
                    field_id: next_id,
                    ty: *ty,
                    nullable: *nullable,
                    is_primary_key: false,
                });
            }
            SchemaChange::DropColumn { name } => {
                let col = schema
                    .columns
                    .iter_mut()
                    .find(|c| c.name == *name)
                    .ok_or_else(|| {
                        IcebergError::NotFound(format!("DropColumn: column {name} not in schema"))
                    })?;
                col.nullable = true;
            }
        }
    }
    Ok(())
}

#[async_trait]
pub trait Catalog: Send + Sync {
    async fn ensure_namespace(&self, ns: &Namespace) -> Result<()>;
    /// `Ok(None)` for "table doesn't exist yet" — the materializer treats
    /// this distinctly from a transient catalog error.
    async fn load_table(&self, ident: &TableIdent) -> Result<Option<TableMetadata>>;
    async fn create_table(&self, schema: &TableSchema) -> Result<TableMetadata>;
    async fn commit_snapshot(&self, prepared: PreparedCommit) -> Result<TableMetadata>;
    /// Commit a compaction snapshot (Operation::Replace): drop the listed
    /// files, add the new ones, atomically. Default impl errors so impls
    /// that don't yet support compaction surface a clear error rather
    /// than silently no-op.
    async fn commit_compaction(&self, prepared: PreparedCompaction) -> Result<TableMetadata> {
        let _ = prepared;
        Err(IcebergError::Other(
            "commit_compaction not implemented for this Catalog impl".into(),
        ))
    }
    async fn evolve_schema(
        &self,
        ident: &TableIdent,
        changes: Vec<SchemaChange>,
    ) -> Result<TableMetadata>;
    /// Drop snapshots older than `retention` (in milliseconds since
    /// the most recent snapshot — *not* wall clock — so tests with
    /// `TestClock` work deterministically). Never drops the current
    /// snapshot. Returns the number of expired snapshots.
    ///
    /// Default impl errors so impls that don't yet support expiry
    /// surface a clear error rather than silently no-op.
    async fn expire_snapshots(&self, ident: &TableIdent, retention_ms: i64) -> Result<usize> {
        let _ = (ident, retention_ms);
        Err(IcebergError::Other(
            "expire_snapshots not implemented for this Catalog impl".into(),
        ))
    }
    /// Append-only snapshot history for `ident`. Used by the verifier (MoR
    /// reads) and by materializer restart code (FileIndex rebuild). Ordered
    /// by snapshot id ascending.
    async fn snapshots(&self, ident: &TableIdent) -> Result<Vec<Snapshot>>;
}
