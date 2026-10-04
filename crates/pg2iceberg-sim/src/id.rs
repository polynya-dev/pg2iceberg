//! Deterministic [`IdGen`]: UUIDs from a counter, so DST runs replay
//! exactly. Share one instance across simulated process restarts — real
//! UUIDs don't repeat across processes, and neither do these.

use pg2iceberg_core::{IdGen, WorkerId};
use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug, Default)]
pub struct SeqIdGen {
    next: AtomicU64,
}

impl SeqIdGen {
    pub fn new() -> Self {
        Self::default()
    }
}

impl IdGen for SeqIdGen {
    fn new_uuid(&self) -> [u8; 16] {
        let n = self.next.fetch_add(1, Ordering::SeqCst);
        let mut uuid = [0u8; 16];
        uuid[8..].copy_from_slice(&n.to_be_bytes());
        uuid
    }

    fn worker_id(&self) -> WorkerId {
        WorkerId("sim".into())
    }
}
