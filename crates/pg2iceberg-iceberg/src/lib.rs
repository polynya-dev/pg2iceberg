//! Catalog and TableWriter trait surface.
//!
//! Wraps `iceberg-rust` in production. The sim impl in `pg2iceberg-sim` keeps
//! metadata in memory.

pub mod file_index;
pub mod fold;
pub mod materialize;
#[cfg(feature = "prod")]
pub mod prod;
pub mod reader;
pub mod verify;
pub mod writer;

pub use file_index::{rebuild_from_catalog, FileIndex};
pub use fold::{fold_events, pk_key, MaterializedRow};
pub use materialize::{promote_re_inserts, resolve_unchanged_cols};
pub use reader::read_data_file;
pub use verify::read_materialized_state;
pub use writer::{DataChunk, PreparedFiles, TableWriter, WriterError};

use async_trait::async_trait;
use pg2iceberg_core::{Namespace, TableIdent, TableSchema};
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

#[derive(Clone, Debug)]
pub struct DataFile {
    pub path: String,
    pub record_count: u64,
    pub byte_size: u64,
    /// For equality-delete files, the field IDs that participate in the
    /// equality predicate (typically the PK columns). Empty for data files.
    pub equality_field_ids: Vec<i32>,
}

/// One Iceberg snapshot. Snapshots are append-only and ordered by `id`.
/// Iceberg MoR semantics: a delete file at snapshot `N` applies only to data
/// files at snapshots `< N` (data and deletes from the same snapshot are
/// kept consistent by the materializer).
#[derive(Clone, Debug)]
pub struct Snapshot {
    pub id: i64,
    pub data_files: Vec<DataFile>,
    pub delete_files: Vec<DataFile>,
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

#[async_trait]
pub trait Catalog: Send + Sync {
    async fn ensure_namespace(&self, ns: &Namespace) -> Result<()>;
    /// `Ok(None)` for "table doesn't exist yet" — the materializer treats
    /// this distinctly from a transient catalog error.
    async fn load_table(&self, ident: &TableIdent) -> Result<Option<TableMetadata>>;
    async fn create_table(&self, schema: &TableSchema) -> Result<TableMetadata>;
    async fn commit_snapshot(&self, prepared: PreparedCommit) -> Result<TableMetadata>;
    async fn evolve_schema(
        &self,
        ident: &TableIdent,
        changes: Vec<SchemaChange>,
    ) -> Result<TableMetadata>;
    /// Append-only snapshot history for `ident`. Used by the verifier (MoR
    /// reads) and by materializer restart code (FileIndex rebuild). Ordered
    /// by snapshot id ascending.
    async fn snapshots(&self, ident: &TableIdent) -> Result<Vec<Snapshot>>;
}
