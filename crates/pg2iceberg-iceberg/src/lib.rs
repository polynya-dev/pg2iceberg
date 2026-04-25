//! Catalog and TableWriter trait surface.
//!
//! Wraps `iceberg-rust` in production. The sim impl in `pg2iceberg-sim` keeps
//! metadata in memory.

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

/// Built by [`TableWriter::prepare`] and consumed by
/// [`Catalog::commit_snapshot`]. The exact contents (data files, equality
/// deletes, manifest list updates) are filled in during Phase 7.
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
    async fn load_table(&self, ident: &TableIdent) -> Result<TableMetadata>;
    async fn create_table(&self, schema: &TableSchema) -> Result<TableMetadata>;
    async fn commit_snapshot(&self, prepared: PreparedCommit) -> Result<TableMetadata>;
    async fn evolve_schema(
        &self,
        ident: &TableIdent,
        changes: Vec<SchemaChange>,
    ) -> Result<TableMetadata>;
}
