//! Staged-event Parquet writer/reader trait surface.
//!
//! The fixed staged schema is the reason logical mode can keep slot-advance
//! off the Iceberg catalog path. Phase 2 will fill in concrete impls.

use async_trait::async_trait;
use pg2iceberg_core::{ChangeEvent, Lsn, TableIdent};
use serde::{Deserialize, Serialize};
use thiserror::Error;

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
    /// Highest LSN included; passed straight into [`OffsetClaim::max_lsn`].
    pub max_lsn: Lsn,
}

/// Pre-decoded form used by the materializer hot path. Avoids re-parsing JSON
/// per row on each cycle.
#[derive(Clone, Debug)]
pub struct MatEvent {
    pub event: ChangeEvent,
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
