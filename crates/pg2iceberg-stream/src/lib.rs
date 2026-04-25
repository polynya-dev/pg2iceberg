//! Staged-event Parquet writer/reader.
//!
//! The fixed staged Parquet schema is the reason logical mode can keep
//! slot-advance off the Iceberg catalog path. The codec in [`codec`] is sync
//! and pure-compute; the [`StagedWriter`]/[`StagedReader`] traits add async IO
//! on top once a `pg2iceberg-coord` impl is ready to consume the chunks.

use async_trait::async_trait;
use pg2iceberg_core::{ChangeEvent, ColumnName, Lsn, Op, Row, TableIdent, Timestamp};
use serde::{Deserialize, Serialize};
use thiserror::Error;

pub mod blob;
pub mod codec;
pub mod prod;
pub mod rolling;

pub use blob::BlobStore;
pub use prod::ObjectStoreBlobStore;

#[derive(Clone, Debug, Error)]
pub enum StreamError {
    #[error("io: {0}")]
    Io(String),
    #[error("encode: {0}")]
    Encode(String),
    #[error("decode: {0}")]
    Decode(String),
}

pub type Result<T> = std::result::Result<T, StreamError>;

#[derive(Clone, Eq, PartialEq, Debug, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ObjectPath(pub String);

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StagedObject {
    pub table: TableIdent,
    pub path: ObjectPath,
    pub record_count: u64,
    pub byte_size: u64,
    /// Highest LSN included; passed straight into `OffsetClaim::max_lsn`.
    pub max_lsn: Lsn,
}

/// Pre-decoded form used by the materializer hot path. Avoids re-parsing JSON
/// per row on each cycle. Mirrors the fields actually persisted in a staged
/// Parquet row — note this is a flat struct, not a wrapper around
/// `ChangeEvent`, since `before`/`after` collapse into a single `row` at write
/// time.
#[derive(Clone, Debug, PartialEq)]
pub struct MatEvent {
    pub op: Op,
    pub lsn: Lsn,
    pub commit_ts: Timestamp,
    pub xid: Option<u32>,
    pub unchanged_cols: Vec<ColumnName>,
    pub row: Row,
}

#[async_trait]
pub trait StagedWriter: Send {
    async fn append(&mut self, evt: &ChangeEvent) -> Result<()>;
    /// Closes the current rolling object and returns metadata for the
    /// coordinator claim. Must be a no-op (returning `None`) if no rows are
    /// buffered.
    async fn flush(&mut self) -> Result<Option<StagedObject>>;
}

#[async_trait]
pub trait StagedReader: Send + Sync {
    async fn read(&self, path: &ObjectPath) -> Result<Vec<MatEvent>>;
}
