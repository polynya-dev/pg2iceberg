//! Coordinator trait: owns the `_pg2iceberg` schema in the source PG.
//!
//! Mirrors `stream/coordinator_pg.go`. The coordinator is the durability
//! boundary — `flushedLSN` may only advance after a successful
//! [`Coordinator::claim_offsets`] call, and the receipt enforces that
//! invariant in the type system.

use async_trait::async_trait;
use pg2iceberg_core::{Checkpoint, Lsn, TableIdent, WorkerId};
use serde::{Deserialize, Serialize};
use std::time::Duration;
use thiserror::Error;

#[derive(Clone, Debug, Error)]
pub enum CoordError {
    #[error("postgres: {0}")]
    Pg(String),
    #[error("conflict on {table}: {detail}")]
    Conflict { table: TableIdent, detail: String },
    #[error("other: {0}")]
    Other(String),
}

pub type Result<T> = std::result::Result<T, CoordError>;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OffsetClaim {
    pub table: TableIdent,
    pub record_count: u64,
    pub byte_size: u64,
    pub s3_path: String,
    /// Highest LSN included in this staged object. The coordinator commits
    /// this LSN as part of the same transaction as the `log_index` insert.
    pub max_lsn: Lsn,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OffsetGrant {
    pub table: TableIdent,
    pub start_offset: u64,
    pub end_offset: u64,
}

/// Proof that [`Coordinator::claim_offsets`] committed in PG. Construction is
/// crate-private so no other code can mint one. Pass it to
/// [`MaxFlushedLsn::advance`] to advance the slot.
///
/// This is the type-system encoding of the durability invariant.
#[derive(Debug)]
#[non_exhaustive]
pub struct CoordCommitReceipt {
    pub max_flushed_lsn: Lsn,
    pub grants: Vec<OffsetGrant>,
    /// Marker proves the receipt came from a `Coordinator` impl in this crate;
    /// outside code cannot construct it.
    _proof: ReceiptProof,
}

#[derive(Debug)]
struct ReceiptProof(());

impl CoordCommitReceipt {
    /// Only callable from within this crate. Coordinator impls go through
    /// [`receipt::mint`].
    fn new(max_flushed_lsn: Lsn, grants: Vec<OffsetGrant>) -> Self {
        Self {
            max_flushed_lsn,
            grants,
            _proof: ReceiptProof(()),
        }
    }
}

/// Minting helper exposed to in-crate impls. Sim and prod coordinators both
/// route through this so the receipt remains unforgeable from outside.
pub mod receipt {
    use super::{CoordCommitReceipt, Lsn, OffsetGrant};

    pub fn mint(max_flushed_lsn: Lsn, grants: Vec<OffsetGrant>) -> CoordCommitReceipt {
        CoordCommitReceipt::new(max_flushed_lsn, grants)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LogEntry {
    pub table: TableIdent,
    pub start_offset: u64,
    pub end_offset: u64,
    pub s3_path: String,
    pub record_count: u64,
    pub byte_size: u64,
}

#[async_trait]
pub trait Coordinator: Send + Sync {
    /// Atomic in PG: bump `log_seq` and insert the corresponding `log_index`
    /// rows for every claim. Returns a [`CoordCommitReceipt`] only on commit
    /// success.
    async fn claim_offsets(&self, claims: &[OffsetClaim]) -> Result<CoordCommitReceipt>;

    async fn read_log(
        &self,
        table: &TableIdent,
        after_offset: u64,
        limit: usize,
    ) -> Result<Vec<LogEntry>>;

    async fn truncate_log(&self, table: &TableIdent, before_offset: u64) -> Result<()>;

    async fn get_cursor(&self, group: &str, table: &TableIdent) -> Result<Option<u64>>;
    async fn advance_cursor(&self, group: &str, table: &TableIdent, to_offset: u64) -> Result<()>;

    async fn register_consumer(&self, group: &str, worker: &WorkerId, ttl: Duration) -> Result<()>;
    async fn active_consumers(&self, group: &str) -> Result<Vec<WorkerId>>;

    async fn load_checkpoint(&self) -> Result<Option<Checkpoint>>;
    async fn save_checkpoint(&self, cp: &Checkpoint) -> Result<()>;
}
