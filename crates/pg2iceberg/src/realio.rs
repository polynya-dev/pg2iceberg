//! Production impls of the `Clock` / `IdGen` / `Spawner` IO seams.
//!
//! These wrap real-world non-determinism (`SystemTime::now()`,
//! `Uuid::new_v4()`, `tokio::spawn`) so the rest of the binary never
//! touches them directly. The sim path uses parallel implementations
//! in `pg2iceberg-sim` that drive from a seed.

use async_trait::async_trait;
use pg2iceberg_core::{Clock, IdGen, Spawner, Timestamp, WorkerId};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use uuid::Uuid;

/// Wall-clock-backed [`Clock`]. Microsecond precision matches our
/// `Timestamp(i64)` shape and the Postgres-side resolution.
pub struct RealClock;

#[async_trait]
impl Clock for RealClock {
    fn now(&self) -> Timestamp {
        let micros = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_micros() as i64)
            .unwrap_or(0);
        Timestamp(micros)
    }

    async fn sleep(&self, d: Duration) {
        tokio::time::sleep(d).await;
    }
}

/// `Uuid::new_v4()`-backed [`IdGen`]. The `worker_id` is fixed for the
/// lifetime of this process so the coord's `consumer` heartbeat row
/// stays stable across reconnects.
pub struct RealIdGen {
    worker: WorkerId,
}

impl RealIdGen {
    pub fn new() -> Self {
        Self {
            worker: WorkerId(Uuid::new_v4().to_string()),
        }
    }

    /// Construct with an operator-supplied worker id. Useful for
    /// distributed deployments that want stable identity across
    /// process restarts (e.g., the kubernetes pod name).
    pub fn with_worker_id(worker: impl Into<String>) -> Self {
        Self {
            worker: WorkerId(worker.into()),
        }
    }
}

impl Default for RealIdGen {
    fn default() -> Self {
        Self::new()
    }
}

impl IdGen for RealIdGen {
    fn new_uuid(&self) -> [u8; 16] {
        *Uuid::new_v4().as_bytes()
    }

    fn worker_id(&self) -> WorkerId {
        self.worker.clone()
    }
}

/// `tokio::spawn`-backed [`Spawner`]. The single-task `run` loop
/// doesn't currently spawn anything, but the type stays here so a
/// follow-on (e.g., the invariant watcher running in its own task) has
/// a drop-in.
#[allow(dead_code)]
pub struct TokioSpawner;

impl Spawner for TokioSpawner {
    fn spawn<F>(&self, fut: F)
    where
        F: std::future::Future<Output = ()> + Send + 'static,
    {
        tokio::spawn(fut);
    }
}
