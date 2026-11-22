//! The standby a source fails over to, and what pg2iceberg does outside
//! its coordinator waiting for it.
//!
//! The coordinator lives in the source database, so a failover to an
//! asynchronous standby rolls it back to what the standby had received.
//! What pg2iceberg did elsewhere on a write the standby lacked — acked
//! the slot, committed to Iceberg, deleted staged files — the failover
//! doesn't undo: afterwards the coordinator would remember less than the
//! slot, Iceberg and the object store show. So none of those happen until
//! the standby has every coordinator write made before them
//! ([`Standby::settle`]). A failover then loses only writes nothing acted
//! on, as a crash just before them would.

use async_trait::async_trait;

/// The standby a source fails over to, as far as the coordinator's writes
/// go.
#[async_trait]
pub trait Standby: Send + Sync {
    /// Wait until the standby has every coordinator write made before the
    /// call. Errs if it can't tell — the source's WAL position unreadable,
    /// or pg2iceberg stopping — and then nothing waiting on it may happen.
    async fn settle(&self) -> Result<(), String>;
}

/// No standby to wait for: before replication starts, or in a one-off
/// command.
pub struct NoStandby;

#[async_trait]
impl Standby for NoStandby {
    async fn settle(&self) -> Result<(), String> {
        Ok(())
    }
}
