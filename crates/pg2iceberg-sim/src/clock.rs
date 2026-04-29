//! Deterministic test clock backed by an explicit microsecond counter.
//!
//! No `Instant::now()` underneath — the time only moves when the test moves
//! it. Sleeps are a no-op (DST drives the scheduler externally; for sim-coord
//! tests we never sleep on the clock at all).

use async_trait::async_trait;
use pg2iceberg_core::{Clock, Timestamp};
use std::sync::{Arc, Mutex};
use std::time::Duration;

#[derive(Clone, Debug)]
pub struct TestClock {
    micros: Arc<Mutex<i64>>,
}

impl TestClock {
    /// Construct a clock at the given microsecond epoch.
    pub fn at(micros: i64) -> Self {
        Self {
            micros: Arc::new(Mutex::new(micros)),
        }
    }

    pub fn advance(&self, d: Duration) {
        let mut m = self.micros.lock().unwrap();
        *m = m.saturating_add(duration_to_micros(d));
    }

    pub fn set(&self, micros: i64) {
        *self.micros.lock().unwrap() = micros;
    }
}

#[async_trait]
impl Clock for TestClock {
    fn now(&self) -> Timestamp {
        Timestamp(*self.micros.lock().unwrap())
    }

    async fn sleep(&self, _d: Duration) {
        // Sim runs serially; tests advance the clock explicitly.
    }
}

pub fn duration_to_micros(d: Duration) -> i64 {
    let raw = d.as_micros();
    i64::try_from(raw).unwrap_or(i64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn now_reflects_set_and_advance() {
        let c = TestClock::at(1_000);
        assert_eq!(c.now(), Timestamp(1_000));
        c.advance(Duration::from_secs(1));
        assert_eq!(c.now(), Timestamp(1_000 + 1_000_000));
        c.set(0);
        assert_eq!(c.now(), Timestamp(0));
    }

    #[test]
    fn duration_to_micros_handles_overflow() {
        let huge = Duration::from_secs(u64::MAX);
        // Saturates rather than panicking.
        assert_eq!(duration_to_micros(huge), i64::MAX);
    }
}
