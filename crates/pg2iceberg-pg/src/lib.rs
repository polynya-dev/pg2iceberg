//! Postgres client + replication trait surface.
//!
//! Production impls live further down the workspace and depend on
//! `tokio-postgres`. This crate is sim-friendly: it has no network deps.

use async_trait::async_trait;
use pg2iceberg_core::{ChangeEvent, Lsn, TableIdent};
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Clone, Eq, PartialEq, Debug, Serialize, Deserialize)]
#[serde(transparent)]
pub struct SnapshotId(pub String);

#[derive(Clone, Debug, Error)]
pub enum PgError {
    #[error("connection error: {0}")]
    Connection(String),
    #[error("protocol error: {0}")]
    Protocol(String),
    #[error("replication slot {0} not found")]
    SlotNotFound(String),
    #[error("publication {0} not found")]
    PublicationNotFound(String),
    #[error("other: {0}")]
    Other(String),
}

pub type Result<T> = std::result::Result<T, PgError>;

/// Decoded pgoutput message. Matches the variants in `logical/decode.go:41-58`.
#[derive(Clone, Debug)]
pub enum DecodedMessage {
    Begin {
        final_lsn: Lsn,
    },
    Commit {
        commit_lsn: Lsn,
    },
    /// Schema for a relation. Sent before any change events for that relation.
    Relation {
        ident: TableIdent,
        // Columns omitted from this skeleton — Phase 5 wires them through.
    },
    Change(ChangeEvent),
    /// Periodic primary keepalive; carries the server's WAL end position.
    Keepalive {
        wal_end: Lsn,
        reply_requested: bool,
    },
}

#[async_trait]
pub trait ReplicationStream: Send {
    async fn recv(&mut self) -> Result<DecodedMessage>;

    /// Send standby status. `flushed` is the LSN we've durably committed via
    /// the coordinator; PG can recycle WAL up to this point.
    async fn send_standby(&mut self, flushed: Lsn, applied: Lsn) -> Result<()>;
}

#[async_trait]
pub trait PgClient: Send + Sync {
    async fn create_publication(&self, name: &str, tables: &[TableIdent]) -> Result<()>;
    async fn create_slot(&self, slot: &str) -> Result<Lsn>;
    async fn slot_exists(&self, slot: &str) -> Result<bool>;
    async fn slot_restart_lsn(&self, slot: &str) -> Result<Option<Lsn>>;

    async fn export_snapshot(&self) -> Result<SnapshotId>;

    async fn start_replication(
        &self,
        slot: &str,
        start: Lsn,
        publication: &str,
    ) -> Result<Box<dyn ReplicationStream>>;
}
