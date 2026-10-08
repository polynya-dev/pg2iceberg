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
//! - **Invariant 1**: `slot.confirmed_flush_lsn ≤ coord.flushed_lsn`. The
//!   slot must never be ahead of what pg2iceberg recorded acknowledging —
//!   every ack is recorded first — or something else advanced it, and
//!   the WAL in between was never staged. (The slot lagging what's staged
//!   is normal: it's acked on the standby tick, after the flush. That
//!   lag is a gauge, [`names::SLOT_ACK_LAG`].)
//! - **Invariant 2**: `mat_cursor[t] ≤ max(log_index.end_offset[t])`. The
//!   materializer cursor must never point past committed offsets.
//! - **Invariant 3**: `pipeline.flushed_lsn` monotonic across watcher
//!   ticks (modulo a documented recovery rewind). The watcher caches the
//!   last observed value and flags any backwards jump.

use pg2iceberg_coord::Coordinator;
use pg2iceberg_core::metrics::{labels, names, set_state, Labels};
use pg2iceberg_core::{Lsn, Metrics, TableIdent};
use pg2iceberg_pg::{SlotHealth, WalStatus};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use thiserror::Error;

#[derive(Clone, Debug, Error, PartialEq)]
pub enum InvariantViolation {
    #[error(
        "invariant 1: slot {slot_name}'s confirmed_flush_lsn ({slot_confirmed}) is past \
         the LSN pg2iceberg recorded acking ({recorded}): something else advanced the slot \
         (`pg_replication_slot_advance`, another consumer), and the WAL in between was never \
         staged. Run `pg2iceberg cleanup` and re-snapshot to recover."
    )]
    SlotAheadOfRecord {
        slot_name: String,
        slot_confirmed: Lsn,
        recorded: Lsn,
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

    /// Slot's `wal_status = unreserved` — past `max_slot_wal_keep_size`.
    /// At the next checkpoint, PG will recycle the WAL behind this slot
    /// and transition to `lost`, breaking replication. Operator's last
    /// chance to scale up the consumer / fix lag. Surfaced as a
    /// non-fatal warning (the pipeline keeps running until the slot
    /// actually transitions to `lost`).
    #[error(
        "invariant 4: replication slot {slot_name:?} is in `unreserved` state \
         (safe_wal_size={safe_wal_size:?}); WAL will be recycled at the next \
         checkpoint and the slot will transition to `lost`. fix consumer lag \
         or raise `max_slot_wal_keep_size` immediately"
    )]
    SlotWalUnreserved {
        slot_name: String,
        safe_wal_size: Option<i64>,
    },

    /// Slot transitioned to `wal_status = lost` mid-run. Unrecoverable.
    /// The main loop classifies this as fatal via [`is_fatal`] and
    /// returns `LifecycleError::SlotHealth` rather than wait for the
    /// next `recv()` to return a confusing protocol error.
    #[error(
        "invariant 5: replication slot {slot_name:?} transitioned to `lost` \
         (restart_lsn={restart_lsn}); WAL has been recycled and the slot \
         cannot be resumed. drop the slot and the Iceberg tables, then \
         re-snapshot from scratch — there is no safe way to skip ahead"
    )]
    SlotWalLost { slot_name: String, restart_lsn: Lsn },

    /// Slot's `conflicting` flipped to `true` mid-run. Same fatal
    /// classification as `SlotWalLost` — the slot is killed by a
    /// physical-replication conflict and can't be resumed.
    #[error(
        "invariant 6: replication slot {slot_name:?} is conflicting (killed \
         by physical-replication conflict during recovery); the slot cannot \
         be resumed safely. drop the slot and the Iceberg tables, then \
         re-snapshot from scratch"
    )]
    SlotConflicting { slot_name: String },
}

impl InvariantViolation {
    /// `true` for violations the main loop should treat as fatal —
    /// i.e. propagate as [`LifecycleError::SlotHealth`] rather than
    /// log-and-continue. Currently: `SlotAheadOfRecord`, `SlotWalLost`
    /// and `SlotConflicting` (each signals WAL the pipeline will never
    /// see). The other variants are healthy-pipeline alerts that don't
    /// warrant tearing the loop down.
    pub fn is_fatal(&self) -> bool {
        matches!(
            self,
            InvariantViolation::SlotAheadOfRecord { .. }
                | InvariantViolation::SlotWalLost { .. }
                | InvariantViolation::SlotConflicting { .. }
        )
    }
}

/// Snapshot of state the watcher needs from one tick. The binary populates
/// `slot_confirmed_flush_lsn` from the source PG; the rest are direct reads
/// from the coord.
#[derive(Clone, Debug, Default)]
pub struct WatcherInputs {
    pub pipeline_flushed_lsn: Lsn,
    /// `Lsn::ZERO` if the slot couldn't be read: invariant 1 skips.
    pub slot_confirmed_flush_lsn: Lsn,
    /// [`Coordinator::flushed_lsn`]: the highest LSN pg2iceberg recorded
    /// acking. `Lsn::ZERO` if none (or unreadable): invariant 1 skips.
    pub coord_flushed_lsn: Lsn,
    /// `(table, mat_cursor_value)` for each watched group/table. The
    /// watcher cross-checks against `coord.read_log` to derive
    /// `max(end_offset)`.
    pub group: String,
    pub watched_tables: Vec<TableIdent>,
    /// Slot's `wal_status` (PG 13+). `None` skips invariant 4.
    /// Drives the `SlotWalUnreserved` warning: when the slot is past
    /// `max_slot_wal_keep_size` but not yet `lost`, the watcher logs
    /// a warning so the operator gets paged before the transition
    /// to `lost` breaks replication.
    pub slot_wal_status: Option<pg2iceberg_pg::WalStatus>,
    /// Slot's `safe_wal_size` (PG 13+) — bytes until the slot
    /// crosses into `unreserved`. Surfaced verbatim in the warning
    /// for operator triage; no invariant logic uses it directly.
    pub slot_safe_wal_size: Option<i64>,
    /// Slot name — surfaced in warning/fatal message bodies.
    pub slot_name: String,
    /// Slot's `restart_lsn`, surfaced in the `SlotWalLost` fatal
    /// message so the operator can correlate against PG's WAL
    /// retention.
    pub slot_restart_lsn: Lsn,
    /// Slot's `conflicting` flag (PG 16+). `true` triggers the
    /// `SlotConflicting` fatal violation.
    pub slot_conflicting: bool,
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

        // 1. slot.confirmed_flush_lsn ≤ coord.flushed_lsn.
        let slot = inputs.slot_confirmed_flush_lsn;
        let recorded = inputs.coord_flushed_lsn;
        if slot > Lsn::ZERO && recorded > Lsn::ZERO && slot > recorded {
            violations.push(InvariantViolation::SlotAheadOfRecord {
                slot_name: inputs.slot_name.clone(),
                slot_confirmed: slot,
                recorded,
            });
        }
        if slot > Lsn::ZERO {
            self.metrics.gauge(
                names::SLOT_ACK_LAG,
                &Labels::new(),
                inputs.pipeline_flushed_lsn.0.saturating_sub(slot.0) as f64,
            );
        }

        // 2. mat_cursor[t] ≤ max(log_index.end_offset[t]). Also gauges
        //    the rows in between: the materializer's backlog. Reads only
        //    the log past the cursor — nothing truncates the log, so all of
        //    it grows without bound; the backlog doesn't.
        for table in &inputs.watched_tables {
            let cursor = match self.coord.get_cursor(&inputs.group, table).await {
                Ok(c) => c.unwrap_or(-1),
                Err(_) => continue, // Transient coord errors don't trip invariants.
            };
            // Unset / sentinel: nothing materialized yet.
            let materialized = cursor.max(0) as u64;
            // Past the entry the cursor sits at the end of.
            let Some(end) = self
                .log_end_past(table, materialized.saturating_sub(1))
                .await
            else {
                continue;
            };
            self.metrics.gauge(
                names::MATERIALIZER_BACKLOG_ROWS,
                &labels([("table", &table.to_string())]),
                end.saturating_sub(materialized) as f64,
            );
            // No entry ends at or past the cursor.
            if cursor > 0 && end < materialized {
                let Some(max_offset) = self.log_end_past(table, 0).await else {
                    continue;
                };
                violations.push(InvariantViolation::CursorAheadOfLogIndex {
                    table: table.clone(),
                    cursor,
                    max_offset,
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

        // 4. Slot wal_status == Unreserved → warn (last chance before lost).
        //    `Lost` is invariant 5 below; surfacing it from the watcher
        //    means the lifecycle bounces with the actionable
        //    `SlotHealth` error rather than waiting for the next
        //    `recv()` to return a confusing protocol-level message.
        match inputs.slot_wal_status {
            Some(pg2iceberg_pg::WalStatus::Unreserved) => {
                violations.push(InvariantViolation::SlotWalUnreserved {
                    slot_name: inputs.slot_name.clone(),
                    safe_wal_size: inputs.slot_safe_wal_size,
                });
            }
            Some(pg2iceberg_pg::WalStatus::Lost) => {
                violations.push(InvariantViolation::SlotWalLost {
                    slot_name: inputs.slot_name.clone(),
                    restart_lsn: inputs.slot_restart_lsn,
                });
            }
            _ => {}
        }

        // 6. Slot conflicting → fatal. Same reasoning as `Lost`.
        if inputs.slot_conflicting {
            violations.push(InvariantViolation::SlotConflicting {
                slot_name: inputs.slot_name.clone(),
            });
        }

        // Emit one counter per violation for the metrics dashboard.
        for v in &violations {
            let mut labels = Labels::new();
            let invariant_id = match v {
                InvariantViolation::SlotAheadOfRecord { .. } => "slot_ahead_of_record",
                InvariantViolation::CursorAheadOfLogIndex { .. } => "cursor_ahead_of_log_index",
                InvariantViolation::FlushedLsnRegressed { .. } => "flushed_lsn_regressed",
                InvariantViolation::SlotWalUnreserved { .. } => "slot_wal_unreserved",
                InvariantViolation::SlotWalLost { .. } => "slot_wal_lost",
                InvariantViolation::SlotConflicting { .. } => "slot_conflicting",
            };
            labels.insert("invariant".into(), invariant_id.into());
            self.metrics
                .counter(names::INVARIANT_VIOLATIONS_TOTAL, &labels, 1);
        }

        violations
    }

    /// The end of `table`'s log, read from just past `after` a page at a
    /// time: `after` itself if no entry ends past it. `None` if the log
    /// couldn't be read.
    async fn log_end_past(&self, table: &TableIdent, after: u64) -> Option<u64> {
        const PAGE: usize = 1024;
        let mut end = after;
        loop {
            let page = self.coord.read_log(table, end, PAGE).await.ok()?;
            // Ordered by end offset.
            match page.last() {
                Some(last) => end = last.end_offset,
                None => return Some(end),
            }
            if page.len() < PAGE {
                return Some(end);
            }
        }
    }
}

/// Gauge the replication slot as `health` reports it — nothing when it
/// couldn't be read.
pub fn record_slot_health(metrics: &dyn Metrics, health: Option<&SlotHealth>) {
    let Some(h) = health else {
        return;
    };
    let no_labels = Labels::new();
    if let Some(current) = h.current_wal_lsn {
        metrics.gauge(
            names::REPLICATION_LAG,
            &no_labels,
            current.0.saturating_sub(h.confirmed_flush_lsn.0) as f64,
        );
        if h.restart_lsn > Lsn::ZERO {
            metrics.gauge(
                names::SLOT_RETAINED_WAL,
                &no_labels,
                current.0.saturating_sub(h.restart_lsn.0) as f64,
            );
        }
    }
    if let Some(safe) = h.safe_wal_size {
        metrics.gauge(names::SLOT_SAFE_WAL_SIZE, &no_labels, safe as f64);
    }
    if let Some(status) = h.wal_status {
        let current = match status {
            WalStatus::Reserved => "reserved",
            WalStatus::Extended => "extended",
            WalStatus::Unreserved => "unreserved",
            WalStatus::Lost => "lost",
        };
        set_state(
            metrics,
            names::SLOT_WAL_STATUS,
            "status",
            &["reserved", "extended", "unreserved", "lost"],
            current,
        );
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
            ..Default::default()
        };
        let v = block_on(watcher.check(&inputs));
        assert!(v.is_empty());
    }

    #[test]
    fn slot_lagging_what_is_staged_is_no_violation() {
        let (_coord, metrics, watcher) = boot();
        let inputs = WatcherInputs {
            pipeline_flushed_lsn: Lsn(200),
            slot_confirmed_flush_lsn: Lsn(100),
            coord_flushed_lsn: Lsn(200),
            group: "default".into(),
            watched_tables: vec![],
            ..Default::default()
        };
        assert!(block_on(watcher.check(&inputs)).is_empty());
        assert_eq!(
            metrics.gauge_value(names::SLOT_ACK_LAG, &Labels::new()),
            Some(100.0)
        );
    }

    #[test]
    fn slot_ahead_of_its_record_caught() {
        let (_coord, metrics, watcher) = boot();
        let inputs = WatcherInputs {
            pipeline_flushed_lsn: Lsn(200),
            slot_confirmed_flush_lsn: Lsn(300),
            coord_flushed_lsn: Lsn(200),
            group: "default".into(),
            watched_tables: vec![],
            ..Default::default()
        };
        let v = block_on(watcher.check(&inputs));
        assert_eq!(v.len(), 1);
        assert!(matches!(v[0], InvariantViolation::SlotAheadOfRecord { .. }));
        assert!(v[0].is_fatal());

        // Counter should have ticked once for this invariant.
        let mut labels = Labels::new();
        labels.insert("invariant".into(), "slot_ahead_of_record".into());
        assert_eq!(
            metrics.counter_value(names::INVARIANT_VIOLATIONS_TOTAL, &labels),
            1
        );
    }

    #[test]
    fn an_unread_slot_or_record_is_no_violation() {
        let (_coord, _metrics, watcher) = boot();
        for (slot, recorded) in [(Lsn::ZERO, Lsn(200)), (Lsn(300), Lsn::ZERO)] {
            let inputs = WatcherInputs {
                slot_confirmed_flush_lsn: slot,
                coord_flushed_lsn: recorded,
                group: "default".into(),
                ..Default::default()
            };
            assert!(block_on(watcher.check(&inputs)).is_empty());
        }
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
            replicated_lsn: None,
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
            ..Default::default()
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
            ..Default::default()
        }));
        assert!(v1.is_empty());

        // Second tick: LSN went backwards. Flag it.
        let v2 = block_on(watcher.check(&WatcherInputs {
            pipeline_flushed_lsn: Lsn(50),
            slot_confirmed_flush_lsn: Lsn(50),
            group: "default".into(),
            watched_tables: vec![],
            ..Default::default()
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
            ..Default::default()
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
            ..Default::default()
        };
        let v = block_on(watcher.check(&inputs));
        assert!(v.is_empty(), "got: {v:?}");
    }

    #[test]
    fn backlog_is_what_the_cursor_has_yet_to_reach() {
        let (coord, metrics, watcher) = boot();
        for (count, path) in [(5, "p0"), (7, "p1")] {
            block_on(coord.claim_offsets(&CommitBatch {
                claims: vec![OffsetClaim {
                    table: ident(),
                    record_count: count,
                    byte_size: 100,
                    s3_path: path.into(),
                }],
                flushable_lsn: Lsn(1),
                replicated_lsn: None,
                markers: vec![],
            }))
            .unwrap();
        }
        let inputs = WatcherInputs {
            group: "default".into(),
            watched_tables: vec![ident()],
            ..Default::default()
        };
        let backlog = || {
            let table = labels([("table", "public.orders")]);
            metrics.gauge_value(names::MATERIALIZER_BACKLOG_ROWS, &table)
        };
        // Nothing materialized yet.
        block_on(coord.ensure_cursor("default", &ident())).unwrap();
        block_on(watcher.check(&inputs));
        assert_eq!(backlog(), Some(12.0));
        // The first entry materialized.
        block_on(coord.set_cursor("default", &ident(), 5)).unwrap();
        block_on(watcher.check(&inputs));
        assert_eq!(backlog(), Some(7.0));
        // Caught up: the cursor at the log's end is no violation.
        block_on(coord.set_cursor("default", &ident(), 12)).unwrap();
        assert!(block_on(watcher.check(&inputs)).is_empty());
        assert_eq!(backlog(), Some(0.0));
    }

    /// The backlog is read a page of the log at a time: one longer than a
    /// page counts in full.
    #[test]
    fn backlog_spans_log_pages() {
        let (coord, metrics, watcher) = boot();
        for i in 0..2_500 {
            block_on(coord.claim_offsets(&CommitBatch {
                claims: vec![OffsetClaim {
                    table: ident(),
                    record_count: 2,
                    byte_size: 100,
                    s3_path: format!("p{i}"),
                }],
                flushable_lsn: Lsn(1),
                replicated_lsn: None,
                markers: vec![],
            }))
            .unwrap();
        }
        block_on(coord.ensure_cursor("default", &ident())).unwrap();
        block_on(coord.set_cursor("default", &ident(), 1_000)).unwrap();
        let inputs = WatcherInputs {
            group: "default".into(),
            watched_tables: vec![ident()],
            ..Default::default()
        };
        assert!(block_on(watcher.check(&inputs)).is_empty());
        let table = labels([("table", "public.orders")]);
        assert_eq!(
            metrics.gauge_value(names::MATERIALIZER_BACKLOG_ROWS, &table),
            Some(4_000.0)
        );
    }

    #[test]
    fn slot_health_is_gauged() {
        let metrics = InMemoryMetrics::new();
        record_slot_health(&metrics, None);
        assert_eq!(
            metrics.gauge_value(names::REPLICATION_LAG, &Labels::new()),
            None
        );

        record_slot_health(
            &metrics,
            Some(&SlotHealth {
                exists: true,
                restart_lsn: Lsn(1_000),
                confirmed_flush_lsn: Lsn(1_500),
                wal_status: Some(WalStatus::Extended),
                conflicting: false,
                safe_wal_size: Some(4_096),
                current_wal_lsn: Some(Lsn(2_000)),
            }),
        );
        let gauge = |name, l: &Labels| metrics.gauge_value(name, l);
        let none = Labels::new();
        assert_eq!(gauge(names::REPLICATION_LAG, &none), Some(500.0));
        assert_eq!(gauge(names::SLOT_RETAINED_WAL, &none), Some(1_000.0));
        assert_eq!(gauge(names::SLOT_SAFE_WAL_SIZE, &none), Some(4_096.0));
        let status = |s| gauge(names::SLOT_WAL_STATUS, &labels([("status", s)]));
        assert_eq!(status("extended"), Some(1.0));
        assert_eq!(status("reserved"), Some(0.0));
        assert_eq!(status("lost"), Some(0.0));
    }
}
