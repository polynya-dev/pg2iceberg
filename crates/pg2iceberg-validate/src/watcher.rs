//! `InvariantWatcher`: continuous runtime checks of the plan-§9 invariants.
//!
//! DST proves these properties under random workloads with shrinking; the
//! watcher proves them in production by sampling state at runtime and
//! recording violations to the metrics surface. The intent isn't "catch
//! every bug" but "fail loud the moment durable state and in-memory state
//! diverge — before the divergence has time to compound."
//!
//! Three checks (a subset of plan §9 — the ones doable without snapshot
//! readback, which would be too expensive to run continuously):
//!
//! - **Invariant 1**: `pipeline.flushed_lsn ≤ slot.confirmed_flush_lsn`. The
//!   slot must never be ahead of what the pipeline acknowledges.
//! - **Invariant 2**: `mat_cursor[t] ≤ max(log_index.end_offset[t])`. The
//!   materializer cursor must never point past committed offsets.
//! - **Invariant 3**: `pipeline.flushed_lsn` monotonic across watcher
//!   ticks (modulo a documented recovery rewind). The watcher caches the
//!   last observed value and flags any backwards jump.

use pg2iceberg_coord::Coordinator;
use pg2iceberg_core::metrics::{names, Labels};
use pg2iceberg_core::{Lsn, Metrics, TableIdent};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use thiserror::Error;

#[derive(Clone, Debug, Error, PartialEq)]
pub enum InvariantViolation {
    #[error(
        "invariant 1: pipeline.flushed_lsn ({pipeline_flushed}) > slot.confirmed_flush_lsn \
         ({slot_confirmed}); the standby ack hasn't caught up to what the pipeline thinks"
    )]
    PipelineAheadOfSlot {
        pipeline_flushed: Lsn,
        slot_confirmed: Lsn,
    },

    #[error(
        "invariant 2: mat_cursor[{table}] ({cursor}) > max(log_index.end_offset[{table}]) \
         ({max_offset}); the materializer cursor is ahead of committed coord state"
    )]
    CursorAheadOfLogIndex {
        table: TableIdent,
        cursor: i64,
        max_offset: u64,
    },

    #[error(
        "invariant 3: pipeline.flushed_lsn went backwards from {prior} to {current} between \
         watcher ticks; production should never observe this outside a documented restart"
    )]
    FlushedLsnRegressed { prior: Lsn, current: Lsn },
}

/// Snapshot of state the watcher needs from one tick. The binary populates
/// `slot_confirmed_flush_lsn` from the source PG; the rest are direct reads
/// from the coord.
#[derive(Clone, Debug)]
pub struct WatcherInputs {
    pub pipeline_flushed_lsn: Lsn,
    pub slot_confirmed_flush_lsn: Lsn,
    /// `(table, mat_cursor_value)` for each watched group/table. The
    /// watcher cross-checks against `coord.read_log` to derive
    /// `max(end_offset)`.
    pub group: String,
    pub watched_tables: Vec<TableIdent>,
}

pub struct InvariantWatcher {
    coord: Arc<dyn Coordinator>,
    metrics: Arc<dyn Metrics>,
    /// Last observed `pipeline_flushed_lsn` for invariant 3. Atomically
    /// updated each tick.
    last_flushed_lsn: AtomicU64,
}

impl InvariantWatcher {
    pub fn new(coord: Arc<dyn Coordinator>, metrics: Arc<dyn Metrics>) -> Self {
        Self {
            coord,
            metrics,
            last_flushed_lsn: AtomicU64::new(0),
        }
    }

    /// Run one watcher tick. Returns the list of violations observed (empty
    /// = healthy). Also emits a counter per violation for the metrics
    /// surface so dashboards can alert.
    pub async fn check(&self, inputs: &WatcherInputs) -> Vec<InvariantViolation> {
        let mut violations = Vec::new();

        // 1. pipeline.flushed_lsn ≤ slot.confirmed_flush_lsn.
        if inputs.pipeline_flushed_lsn > inputs.slot_confirmed_flush_lsn {
            violations.push(InvariantViolation::PipelineAheadOfSlot {
                pipeline_flushed: inputs.pipeline_flushed_lsn,
                slot_confirmed: inputs.slot_confirmed_flush_lsn,
            });
        }

        // 2. mat_cursor[t] ≤ max(log_index.end_offset[t]).
        for table in &inputs.watched_tables {
            let cursor_opt = match self.coord.get_cursor(&inputs.group, table).await {
                Ok(c) => c,
                Err(_) => continue, // Transient coord errors don't trip invariants.
            };
            let cursor = match cursor_opt {
                Some(c) if c >= 0 => c,
                _ => continue, // Unset / sentinel; skip.
            };
            let entries = match self.coord.read_log(table, 0, usize::MAX).await {
                Ok(e) => e,
                Err(_) => continue,
            };
            let max_end = entries.iter().map(|e| e.end_offset).max().unwrap_or(0);
            if (cursor as u64) > max_end {
                violations.push(InvariantViolation::CursorAheadOfLogIndex {
                    table: table.clone(),
                    cursor,
                    max_offset: max_end,
                });
            }
        }

        // 3. pipeline.flushed_lsn monotonic.
        let prior = self.last_flushed_lsn.load(Ordering::SeqCst);
        let current = inputs.pipeline_flushed_lsn.0;
        if current < prior {
            violations.push(InvariantViolation::FlushedLsnRegressed {
                prior: Lsn(prior),
                current: Lsn(current),
            });
        }
        // Always update — a regressed value is still the new floor for the
        // next tick (otherwise we'd alert every tick after a real-but-rare
        // recovery rewind).
        self.last_flushed_lsn.store(current, Ordering::SeqCst);

        // Emit one counter per violation for the metrics dashboard.
        for v in &violations {
            let mut labels = Labels::new();
            let invariant_id = match v {
                InvariantViolation::PipelineAheadOfSlot { .. } => "pipeline_ahead_of_slot",
                InvariantViolation::CursorAheadOfLogIndex { .. } => "cursor_ahead_of_log_index",
                InvariantViolation::FlushedLsnRegressed { .. } => "flushed_lsn_regressed",
            };
            labels.insert("invariant".into(), invariant_id.into());
            self.metrics
                .counter(names::INVARIANT_VIOLATIONS_TOTAL, &labels, 1);
        }

        violations
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pg2iceberg_coord::schema::CoordSchema;
    use pg2iceberg_coord::{CommitBatch, OffsetClaim};
    use pg2iceberg_core::{InMemoryMetrics, Namespace};
    use pg2iceberg_sim::clock::TestClock;
    use pg2iceberg_sim::coord::MemoryCoordinator;
    use pollster::block_on;

    fn ident() -> TableIdent {
        TableIdent {
            namespace: Namespace(vec!["public".into()]),
            name: "orders".into(),
        }
    }

    fn boot() -> (
        Arc<MemoryCoordinator>,
        Arc<InMemoryMetrics>,
        InvariantWatcher,
    ) {
        let clock = TestClock::at(0);
        let arc_clock: Arc<dyn pg2iceberg_core::Clock> = Arc::new(clock);
        let coord = Arc::new(MemoryCoordinator::new(
            CoordSchema::default_name(),
            arc_clock,
        ));
        let metrics = Arc::new(InMemoryMetrics::new());
        let watcher = InvariantWatcher::new(coord.clone() as Arc<dyn Coordinator>, metrics.clone());
        (coord, metrics, watcher)
    }

    #[test]
    fn healthy_state_yields_no_violations() {
        let (_coord, _metrics, watcher) = boot();
        let inputs = WatcherInputs {
            pipeline_flushed_lsn: Lsn(100),
            slot_confirmed_flush_lsn: Lsn(100),
            group: "default".into(),
            watched_tables: vec![],
        };
        let v = block_on(watcher.check(&inputs));
        assert!(v.is_empty());
    }

    #[test]
    fn pipeline_ahead_of_slot_caught() {
        let (_coord, metrics, watcher) = boot();
        let inputs = WatcherInputs {
            pipeline_flushed_lsn: Lsn(200),
            slot_confirmed_flush_lsn: Lsn(100),
            group: "default".into(),
            watched_tables: vec![],
        };
        let v = block_on(watcher.check(&inputs));
        assert_eq!(v.len(), 1);
        assert!(matches!(
            v[0],
            InvariantViolation::PipelineAheadOfSlot { .. }
        ));

        // Counter should have ticked once for this invariant.
        let mut labels = Labels::new();
        labels.insert("invariant".into(), "pipeline_ahead_of_slot".into());
        assert_eq!(
            metrics.counter_value(names::INVARIANT_VIOLATIONS_TOTAL, &labels),
            1
        );
    }

    #[test]
    fn cursor_ahead_of_log_index_caught() {
        let (coord, _metrics, watcher) = boot();

        // Stage one log_index entry [0, 5) by claiming offsets, then
        // manually advance the cursor PAST end_offset=5.
        block_on(coord.claim_offsets(&CommitBatch {
            claims: vec![OffsetClaim {
                table: ident(),
                record_count: 5,
                byte_size: 100,
                s3_path: "p0".into(),
            }],
            flushable_lsn: Lsn(1),
            markers: vec![],
        }))
        .unwrap();
        block_on(coord.ensure_cursor("default", &ident())).unwrap();
        block_on(coord.set_cursor("default", &ident(), 99)).unwrap();

        let inputs = WatcherInputs {
            pipeline_flushed_lsn: Lsn(0),
            slot_confirmed_flush_lsn: Lsn(0),
            group: "default".into(),
            watched_tables: vec![ident()],
        };
        let v = block_on(watcher.check(&inputs));
        assert_eq!(v.len(), 1);
        assert!(matches!(
            v[0],
            InvariantViolation::CursorAheadOfLogIndex { ref table, cursor: 99, max_offset: 5 } if table == &ident()
        ));
    }

    #[test]
    fn flushed_lsn_regression_caught_only_on_subsequent_tick() {
        let (_coord, _metrics, watcher) = boot();
        // First tick: establishes the baseline at LSN 100. No regression yet.
        let v1 = block_on(watcher.check(&WatcherInputs {
            pipeline_flushed_lsn: Lsn(100),
            slot_confirmed_flush_lsn: Lsn(100),
            group: "default".into(),
            watched_tables: vec![],
        }));
        assert!(v1.is_empty());

        // Second tick: LSN went backwards. Flag it.
        let v2 = block_on(watcher.check(&WatcherInputs {
            pipeline_flushed_lsn: Lsn(50),
            slot_confirmed_flush_lsn: Lsn(50),
            group: "default".into(),
            watched_tables: vec![],
        }));
        assert_eq!(v2.len(), 1);
        assert!(matches!(
            v2[0],
            InvariantViolation::FlushedLsnRegressed {
                prior: Lsn(100),
                current: Lsn(50)
            }
        ));

        // Third tick: stays at 50. No new regression (50 is now the floor).
        let v3 = block_on(watcher.check(&WatcherInputs {
            pipeline_flushed_lsn: Lsn(50),
            slot_confirmed_flush_lsn: Lsn(50),
            group: "default".into(),
            watched_tables: vec![],
        }));
        assert!(v3.is_empty());
    }

    #[test]
    fn unset_cursor_does_not_trip_invariant_2() {
        let (coord, _metrics, watcher) = boot();
        // Cursor never set for this table; ensure_cursor sets it to -1.
        block_on(coord.ensure_cursor("default", &ident())).unwrap();
        let inputs = WatcherInputs {
            pipeline_flushed_lsn: Lsn(0),
            slot_confirmed_flush_lsn: Lsn(0),
            group: "default".into(),
            watched_tables: vec![ident()],
        };
        let v = block_on(watcher.check(&inputs));
        assert!(v.is_empty(), "got: {v:?}");
    }
}
