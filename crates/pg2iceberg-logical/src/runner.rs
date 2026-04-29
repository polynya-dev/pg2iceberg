//! Pure scheduling logic for the production async ticker.
//!
//! The binary's main loop:
//!
//! ```ignore
//! let mut ticker = Ticker::new(clock.now(), Schedule::default());
//! loop {
//!     drain_replication_stream(&mut pipeline, &mut stream).await?;
//!     for handler in ticker.fire_due(clock.now()) {
//!         match handler {
//!             Handler::Flush       => pipeline.flush().await?,
//!             Handler::Standby     => stream.send_standby(pipeline.flushed_lsn()),
//!             Handler::Materialize => { materializer.cycle().await?; }
//!             Handler::Watcher     => { watcher.check(...).await; }
//!         }
//!     }
//!     let next = ticker.next_due();
//!     let now  = clock.now();
//!     if next.0 > now.0 {
//!         clock.sleep(Duration::from_micros((next.0 - now.0) as u64)).await;
//!     }
//! }
//! ```
//!
//! `Ticker` is sync, owns no IO, and holds no `Arc`s — it just does
//! interval bookkeeping. That makes it cheap to test deterministically
//! and lets the binary glue it to whichever flavor of replication stream
//! and watcher it uses.

use pg2iceberg_core::Timestamp;
use std::time::Duration;

/// Periodic-task intervals. Defaults mirror the Go reference (10 s flush /
/// 10 s materialize / 10 s standby; watcher is new in the Rust port and
/// runs less frequently — invariant checks aren't free).
///
/// Compaction is intentionally *not* a separate handler — Go invokes it
/// at the end of every materializer cycle gated by file-count
/// thresholds, so we do the same. See [`crate::Materializer::compact_cycle`].
#[derive(Clone, Debug)]
pub struct Schedule {
    pub flush: Duration,
    pub materialize: Duration,
    pub standby: Duration,
    pub watcher: Duration,
}

impl Default for Schedule {
    fn default() -> Self {
        Self {
            flush: Duration::from_secs(10),
            materialize: Duration::from_secs(10),
            standby: Duration::from_secs(10),
            watcher: Duration::from_secs(30),
        }
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash)]
pub enum Handler {
    Flush,
    Standby,
    Materialize,
    Watcher,
}

pub struct Ticker {
    schedule: Schedule,
    last_flush: Timestamp,
    last_standby: Timestamp,
    last_materialize: Timestamp,
    last_watcher: Timestamp,
}

impl Ticker {
    pub fn new(now: Timestamp, schedule: Schedule) -> Self {
        Self {
            schedule,
            last_flush: now,
            last_standby: now,
            last_materialize: now,
            last_watcher: now,
        }
    }

    /// Returns the handlers due at `now`, in a stable order:
    /// `Flush → Standby → Materialize → Watcher`. Each handler that fires
    /// has its `last_*` timestamp bumped to `now`.
    ///
    /// We bump to `now` rather than `last + interval` so the binary doesn't
    /// fire a backlog of handlers if it overslept (e.g., 360 missed flushes
    /// after an hour stuck in GC). Slight schedule drift on heavy load is
    /// preferable to a thundering herd.
    pub fn fire_due(&mut self, now: Timestamp) -> Vec<Handler> {
        let mut out = Vec::new();
        if elapsed_since(now, self.last_flush) >= self.schedule.flush {
            out.push(Handler::Flush);
            self.last_flush = now;
        }
        if elapsed_since(now, self.last_standby) >= self.schedule.standby {
            out.push(Handler::Standby);
            self.last_standby = now;
        }
        if elapsed_since(now, self.last_materialize) >= self.schedule.materialize {
            out.push(Handler::Materialize);
            self.last_materialize = now;
        }
        if elapsed_since(now, self.last_watcher) >= self.schedule.watcher {
            out.push(Handler::Watcher);
            self.last_watcher = now;
        }
        out
    }

    /// Absolute timestamp at which the next handler will be due. Binary
    /// uses this to size the next `Clock::sleep` so it doesn't busy-wait.
    pub fn next_due(&self) -> Timestamp {
        let f = saturating_add(self.last_flush, self.schedule.flush);
        let s = saturating_add(self.last_standby, self.schedule.standby);
        let m = saturating_add(self.last_materialize, self.schedule.materialize);
        let w = saturating_add(self.last_watcher, self.schedule.watcher);
        Timestamp(f.0.min(s.0).min(m.0).min(w.0))
    }
}

fn elapsed_since(now: Timestamp, then: Timestamp) -> Duration {
    let delta = now.0.saturating_sub(then.0);
    Duration::from_micros(delta.max(0) as u64)
}

fn saturating_add(t: Timestamp, d: Duration) -> Timestamp {
    let micros = i64::try_from(d.as_micros()).unwrap_or(i64::MAX);
    Timestamp(t.0.saturating_add(micros))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn schedule_secs(flush: u64, standby: u64, materialize: u64, watcher: u64) -> Schedule {
        Schedule {
            flush: Duration::from_secs(flush),
            standby: Duration::from_secs(standby),
            materialize: Duration::from_secs(materialize),
            watcher: Duration::from_secs(watcher),
        }
    }

    fn t(secs: u64) -> Timestamp {
        Timestamp((secs * 1_000_000) as i64)
    }

    #[test]
    fn nothing_due_at_creation_time() {
        let mut tk = Ticker::new(t(0), Schedule::default());
        assert!(tk.fire_due(t(0)).is_empty());
    }

    #[test]
    fn flush_fires_at_interval_boundary_not_before() {
        let mut tk = Ticker::new(t(0), schedule_secs(10, 60, 60, 60));
        assert!(tk.fire_due(t(9)).is_empty());
        let due = tk.fire_due(t(10));
        assert_eq!(due, vec![Handler::Flush]);
    }

    #[test]
    fn fire_due_resets_per_handler_baseline() {
        let mut tk = Ticker::new(t(0), schedule_secs(10, 60, 60, 60));
        tk.fire_due(t(10)); // flush fires, baseline → 10
                            // At t=15, only 5s elapsed since last flush — no fire.
        assert!(tk.fire_due(t(15)).is_empty());
        // At t=20, 10s elapsed — fires again.
        assert_eq!(tk.fire_due(t(20)), vec![Handler::Flush]);
    }

    #[test]
    fn distinct_intervals_fire_independently() {
        let mut tk = Ticker::new(t(0), schedule_secs(5, 10, 15, 30));
        // t=5: only flush.
        assert_eq!(tk.fire_due(t(5)), vec![Handler::Flush]);
        // t=10: flush again + standby.
        assert_eq!(tk.fire_due(t(10)), vec![Handler::Flush, Handler::Standby]);
        // t=15: flush + materialize.
        assert_eq!(
            tk.fire_due(t(15)),
            vec![Handler::Flush, Handler::Materialize]
        );
        // t=30: all four (flush, standby, materialize, watcher).
        // last_flush was bumped to 15 at t=15 → elapsed 15 >= 5; ✓.
        // last_standby was 10 → elapsed 20 >= 10; ✓.
        // last_materialize was 15 → elapsed 15 >= 15; ✓.
        // last_watcher was 0 → elapsed 30 >= 30; ✓.
        let due = tk.fire_due(t(30));
        assert!(due.contains(&Handler::Flush));
        assert!(due.contains(&Handler::Standby));
        assert!(due.contains(&Handler::Materialize));
        assert!(due.contains(&Handler::Watcher));
    }

    #[test]
    fn oversleep_does_not_create_backlog() {
        // Asleep for 1 hour. Flush should fire ONCE, not 360 times.
        let mut tk = Ticker::new(t(0), schedule_secs(10, 10, 10, 30));
        let due = tk.fire_due(t(3600));
        assert_eq!(due.iter().filter(|h| **h == Handler::Flush).count(), 1);
        // Subsequent fire_due at t=3610 should fire again — interval has
        // elapsed once more.
        let due2 = tk.fire_due(t(3610));
        assert!(due2.contains(&Handler::Flush));
    }

    #[test]
    fn next_due_returns_earliest_handler() {
        let tk = Ticker::new(t(0), schedule_secs(5, 10, 15, 30));
        // Earliest is flush at t=5.
        assert_eq!(tk.next_due(), t(5));
    }

    #[test]
    fn next_due_after_partial_fire_picks_next_pending() {
        let mut tk = Ticker::new(t(0), schedule_secs(5, 10, 15, 30));
        tk.fire_due(t(5)); // flush fires, last_flush = 5
                           // Next due is min(5+5, 0+10, 0+15, 0+30) = min(10, 10, 15, 30) = 10.
        assert_eq!(tk.next_due(), t(10));
    }

    #[test]
    fn handler_order_is_stable() {
        let mut tk = Ticker::new(t(0), schedule_secs(1, 1, 1, 1));
        let due = tk.fire_due(t(100));
        // Even when all four are due simultaneously, order is stable so the
        // binary's match arm runs flush before materialize, etc.
        assert_eq!(
            due,
            vec![
                Handler::Flush,
                Handler::Standby,
                Handler::Materialize,
                Handler::Watcher
            ]
        );
    }
}
