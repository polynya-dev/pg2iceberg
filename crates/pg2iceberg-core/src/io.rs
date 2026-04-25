//! IO seam traits. All non-deterministic capabilities flow through these so the
//! sim can intercept them.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::time::Duration;

/// Microseconds since Unix epoch. The same precision Postgres uses.
#[derive(Copy, Clone, Eq, Ord, PartialEq, PartialOrd, Hash, Debug, Serialize, Deserialize)]
pub struct Timestamp(pub i64);

impl Timestamp {
    pub const EPOCH: Timestamp = Timestamp(0);
}

/// Stable identifier for a process participating in distributed materialization.
#[derive(Clone, Eq, Ord, PartialEq, PartialOrd, Hash, Debug, Serialize, Deserialize)]
#[serde(transparent)]
pub struct WorkerId(pub String);

#[async_trait]
pub trait Clock: Send + Sync {
    fn now(&self) -> Timestamp;
    async fn sleep(&self, d: Duration);
}

pub trait IdGen: Send + Sync {
    /// UUIDs for marker rows, staged file names, and similar non-secret IDs.
    fn new_uuid(&self) -> [u8; 16];

    /// Stable for the lifetime of this process. Used as the key in the
    /// `_pg2iceberg.consumer` heartbeat table.
    fn worker_id(&self) -> WorkerId;
}

/// Thin wrapper around task spawning so the sim can drive tasks from a seeded
/// scheduler instead of `tokio::spawn`.
pub trait Spawner: Send + Sync {
    fn spawn<F>(&self, fut: F)
    where
        F: std::future::Future<Output = ()> + Send + 'static;
}
