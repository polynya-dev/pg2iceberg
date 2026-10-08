//! Production impls of the `Clock` / `IdGen` / `Spawner` IO seams.
//!
//! These wrap real-world non-determinism (`SystemTime::now()`,
//! `Uuid::new_v4()`, `tokio::spawn`) so the rest of the binary never
//! touches them directly. The sim path uses parallel implementations
//! in `pg2iceberg-sim` that drive from a seed.

use async_trait::async_trait;
use pg2iceberg_core::{Clock, IdGen, Spawner, Timestamp, WorkerId};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
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

/// [`Clock`] that never steps: the wall-clock time at creation, plus the
/// time since by the monotonic clock. For measuring how long something
/// took — an NTP step of the wall clock would skew that — not for
/// timestamps compared across processes.
pub struct MonotonicClock {
    start_micros: i64,
    start: Instant,
}

impl MonotonicClock {
    pub fn new() -> Self {
        Self {
            start_micros: RealClock.now().0,
            start: Instant::now(),
        }
    }
}

impl Default for MonotonicClock {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Clock for MonotonicClock {
    fn now(&self) -> Timestamp {
        let elapsed = self.start.elapsed().as_micros() as i64;
        Timestamp(self.start_micros.saturating_add(elapsed))
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
    /// Currently unused; the YAML config shape doesn't expose a
    /// worker-id field yet (Go's `MaterializerWorkerID` will map
    /// here when distributed-mode lands).
    #[allow(dead_code)]
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
