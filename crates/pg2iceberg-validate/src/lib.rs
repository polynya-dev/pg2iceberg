//! Startup validation + `verify` diff.
//!
//! Two surfaces, both meant for users to run as production diagnostics:
//!
//! - [`validate_startup`] runs the 8 checks from `pipeline/validate.go` —
//!   does the persisted checkpoint match what's on disk in Iceberg / what
//!   the replication slot says? If not, refuse to start with an actionable
//!   error.
//! - [`verify::verify_table`] reads PG ground truth at a snapshot LSN, reads
//!   Iceberg at the snapshot id committed at that LSN, and reports per-PK
//!   diffs. This is the headline "is my mirror correct?" command.

pub mod runtime;
pub mod verify;
pub mod watcher;

pub use runtime::{
    drain_and_shutdown, run_logical_lifecycle, run_logical_main_loop, run_materialize_tick,
    run_watcher_tick, LifecycleError, LogicalLifecycle, LogicalLoop, MainLoopError,
    MaterializeTickOutcome, SnapshotSourceFactoryFut,
};
pub use watcher::{InvariantViolation, InvariantWatcher, WatcherInputs};

use pg2iceberg_core::{Lsn, Mode, TableIdent};
use thiserror::Error;

/// State observed at pipeline startup. Built by the binary by querying
/// the coord (for per-table snapshot state), the source PG (for
/// replication slot info), and the catalog (for which tables exist +
/// their snapshot status). Replaces the old single-blob `Checkpoint`
/// in favor of per-table state assembled into [`TableExistence`].
#[derive(Clone, Debug, Default)]
pub struct StartupValidation {
    pub tables: Vec<TableExistence>,
    /// `None` for query mode (no replication slot).
    pub slot: Option<SlotState>,
    pub config_mode: Mode,
    pub slot_name: String,
    /// Publication name — used in the `TableMissingFromPublication`
    /// violation message. Empty in query mode.
    pub publication_name: String,
    /// Highest LSN we (pg2iceberg) have ever told the slot to flush
    /// past, read from `_pg2iceberg.flushed_lsn`. Compared to
    /// `slot.confirmed_flush_lsn` to detect external advancement
    /// (see [`Violation::SlotAdvancedExternally`]). `Lsn::ZERO`
    /// means "no record yet" — treated as the bootstrap case (fresh
    /// install or first slot creation), where we trust the slot
    /// unconditionally.
    pub coord_flushed_lsn: Lsn,
}

impl StartupValidation {
    /// `true` when no registered table has a stored snapshot state in
    /// `_pg2iceberg.tables`. Replaces the old `checkpoint.is_none()`
    /// check — same semantic ("first run after install / cleanup")
    /// derived from per-table state.
    pub fn fresh(&self) -> bool {
        self.tables.iter().all(|t| t.stored_state.is_none())
    }
}

#[derive(Clone, Debug, Default)]
pub struct TableExistence {
    pub pg_table: TableIdent,
    pub iceberg_name: String,
    pub existed: bool,
    /// `None` for tables that didn't exist or have no snapshots yet.
    pub current_snapshot_id: Option<i64>,
    /// PG `pg_class.oid` of the source table at validation time.
    /// `None` if the table doesn't exist in PG. Compared against
    /// `stored_state.pg_oid` to detect identity changes
    /// (`DROP TABLE` + recreate).
    pub current_pg_oid: Option<u32>,
    /// `true` if the source table is currently a member of the
    /// configured publication. `false` (combined with
    /// `stored_state.snapshot_complete`) signals that the operator
    /// removed the table from the publication mid-run — any DML
    /// during the gap was filtered, and re-adding the table requires
    /// a re-snapshot.
    pub in_publication: bool,
    /// Per-table snapshot status from `_pg2iceberg.tables`. `None`
    /// means we've never recorded a row for this table — treat as
    /// "fresh."
    pub stored_state: Option<pg2iceberg_coord::TableSnapshotState>,
}

#[derive(Clone, Debug, Default)]
pub struct SlotState {
    pub exists: bool,
    pub restart_lsn: Lsn,
    pub confirmed_flush_lsn: Lsn,
    /// `Some` on PG 13+; `None` on older versions. Drives the
    /// `wal_status = lost` startup-validation invariant — without
    /// this we'd attempt `START_REPLICATION` on a slot whose WAL
    /// is gone and surface a confusing PG error mid-startup.
    pub wal_status: Option<pg2iceberg_pg::WalStatus>,
    /// PG 14+. `true` indicates the slot was killed by physical-
    /// replication conflict during recovery — also unrecoverable.
    pub conflicting: bool,
}

/// One violation found during startup validation. Each variant prints an
/// actionable error message describing how to recover.
#[derive(Clone, Debug, Error, PartialEq)]
pub enum Violation {
    #[error(
        "no per-table snapshot state recorded but Iceberg table(s) already exist: {tables:?}; \
         delete the tables to start fresh, or `pg2iceberg cleanup` to wipe coord state"
    )]
    OrphanedTables { tables: Vec<String> },

    #[error(
        "no per-table snapshot state recorded but replication slot {slot_name:?} already exists; \
         drop the slot with `SELECT pg_drop_replication_slot('{slot_name}')`, or run `pg2iceberg cleanup`"
    )]
    OrphanedSlot { slot_name: String },

    #[error(
        "table state recorded but Iceberg table(s) missing: {tables:?}; \
         run `pg2iceberg cleanup` to re-snapshot, or recreate the Iceberg tables"
    )]
    MissingTables { tables: Vec<String> },

    #[error(
        "table state has snapshot_lsn {snapshot_lsn} but replication slot {slot_name:?} does \
         not exist; WAL data since that position is lost; run `pg2iceberg cleanup` and \
         re-snapshot"
    )]
    SlotGoneButLsnExists {
        snapshot_lsn: Lsn,
        slot_name: String,
    },

    #[error(
        "replication slot {slot_name:?} restart_lsn ({restart_lsn}) is ahead of recorded \
         snapshot_lsn ({snapshot_lsn}); WAL has been recycled and data is lost; \
         run `pg2iceberg cleanup` and re-snapshot"
    )]
    SlotAheadOfCheckpoint {
        restart_lsn: Lsn,
        snapshot_lsn: Lsn,
        slot_name: String,
    },

    #[error(
        "table {table:?} is marked snapshot_complete but its snapshot_lsn is 0; the pipeline \
         crashed after the snapshot but before the first CDC flush; \
         run `pg2iceberg cleanup` to re-snapshot"
    )]
    SnapshotCompleteButLsnZero { table: String },

    #[error(
        "table {table:?} is marked snapshot_complete but has no snapshots in the catalog; \
         the table may have been recreated externally; run `pg2iceberg cleanup` to re-snapshot"
    )]
    SnapshotCompleteButTableNoSnapshot { table: String },

    #[error(
        "replication slot {slot_name:?} has wal_status=lost; the WAL behind \
         restart_lsn ({restart_lsn}) has been recycled past `max_slot_wal_keep_size` \
         and the slot cannot be resumed. drop the slot and the Iceberg tables, then \
         re-snapshot from scratch — there is no safe way to skip ahead"
    )]
    SlotLost {
        slot_name: String,
        restart_lsn: Lsn,
    },

    #[error(
        "replication slot {slot_name:?} is conflicting (killed by physical-replication \
         conflict during recovery); the slot cannot be resumed safely. drop the slot \
         and the Iceberg tables, then re-snapshot from scratch"
    )]
    SlotConflicting { slot_name: String },

    #[error(
        "table {table:?} pg_class.oid changed from {stored_oid} to {current_oid}; \
         the table was dropped + recreated, so the pre-drop Iceberg data is now \
         stale (rows no longer in PG, and CDC won't re-emit them). drop the \
         Iceberg table and clear the corresponding `_pg2iceberg.tables` row to \
         trigger a fresh snapshot, or run `pg2iceberg cleanup`"
    )]
    TableIdentityChanged {
        table: String,
        stored_oid: u32,
        current_oid: u32,
    },

    #[error(
        "table {table:?} is in YAML and was previously snapshotted but is no longer \
         a member of publication {publication_name:?}; any DML that occurred while \
         the table was outside the publication was filtered by the slot. \
         drop the Iceberg table and clear its row in `_pg2iceberg.tables` to \
         trigger a re-snapshot, then re-add to the publication"
    )]
    TableMissingFromPublication {
        table: String,
        publication_name: String,
    },

    #[error(
        "replication slot {slot_name:?} has been advanced externally: \
         our recorded flushed_lsn is {coord_lsn} but the slot's \
         confirmed_flush_lsn is {slot_lsn}. Possible causes: \
         `pg_replication_slot_advance` was called from outside pg2iceberg, \
         the slot was dropped + recreated, or a separate consumer \
         (e.g. `pg_recvlogical`) acked the slot during a downtime. WAL \
         between our record and the slot's position has been skipped — \
         data written to PG in that window will not appear in Iceberg. \
         Run `pg2iceberg cleanup` and re-snapshot to recover."
    )]
    SlotAdvancedExternally {
        slot_name: String,
        coord_lsn: Lsn,
        slot_lsn: Lsn,
    },
}

/// Aggregate error returned by [`validate_startup`].
#[derive(Clone, Debug, Error)]
#[error("startup validation failed:\n  - {0:#?}", .violations)]
pub struct ValidationError {
    pub violations: Vec<Violation>,
}

pub fn validate_startup(v: &StartupValidation) -> std::result::Result<(), ValidationError> {
    let mut violations = Vec::new();
    let fresh = v.fresh();
    let _ = v.config_mode; // retained for future query-mode-specific invariants

    // 1. Fresh install but Iceberg tables exist (orphans).
    if fresh {
        let orphaned: Vec<String> = v
            .tables
            .iter()
            .filter(|t| t.existed)
            .map(|t| t.iceberg_name.clone())
            .collect();
        if !orphaned.is_empty() {
            violations.push(Violation::OrphanedTables { tables: orphaned });
        }
    }

    // 2. Fresh install but replication slot exists.
    if fresh {
        if let Some(slot) = &v.slot {
            if slot.exists {
                violations.push(Violation::OrphanedSlot {
                    slot_name: v.slot_name.clone(),
                });
            }
        }
    }

    // 3. Previously-tracked table missing in catalog. "Tracked"
    //    means a row in `_pg2iceberg.tables` exists for it
    //    (regardless of snapshot_complete).
    let missing: Vec<String> = v
        .tables
        .iter()
        .filter(|t| !t.existed && t.stored_state.is_some())
        .map(|t| t.iceberg_name.clone())
        .collect();
    if !missing.is_empty() {
        violations.push(Violation::MissingTables { tables: missing });
    }

    // 4. Some table has snapshot_lsn > 0 but slot is gone. WAL
    //    behind that LSN is lost; can't resume CDC safely.
    if let Some(slot) = &v.slot {
        if !slot.exists {
            for t in &v.tables {
                if let Some(state) = &t.stored_state {
                    if state.snapshot_lsn > Lsn::ZERO {
                        violations.push(Violation::SlotGoneButLsnExists {
                            snapshot_lsn: state.snapshot_lsn,
                            slot_name: v.slot_name.clone(),
                        });
                        break;
                    }
                }
            }
        }
    }

    // 5. Slot's restart_lsn is ahead of every recorded snapshot_lsn
    //    (WAL recycled past where we last said we were caught up).
    //    Use the *max* recorded snapshot_lsn — if the slot is past
    //    even our furthest-ahead table, the gap is real.
    if let Some(slot) = &v.slot {
        if slot.exists {
            let max_snapshot_lsn = v
                .tables
                .iter()
                .filter_map(|t| t.stored_state.as_ref().map(|s| s.snapshot_lsn))
                .max()
                .unwrap_or(Lsn::ZERO);
            if max_snapshot_lsn > Lsn::ZERO && slot.restart_lsn > max_snapshot_lsn {
                violations.push(Violation::SlotAheadOfCheckpoint {
                    restart_lsn: slot.restart_lsn,
                    snapshot_lsn: max_snapshot_lsn,
                    slot_name: v.slot_name.clone(),
                });
            }
        }
    }

    // 6. Per-table snapshot_complete but snapshot_lsn = 0 (crashed
    //    after marking complete but before the LSN was stamped, or
    //    legacy bug). Only applies to logical mode.
    if v.config_mode == Mode::Logical {
        for t in &v.tables {
            if let Some(state) = &t.stored_state {
                if state.snapshot_complete && state.snapshot_lsn == Lsn::ZERO {
                    violations.push(Violation::SnapshotCompleteButLsnZero {
                        table: t.iceberg_name.clone(),
                    });
                }
            }
        }
    }

    // 7. Per-table snapshot_complete but the catalog table has no
    //    snapshots — the iceberg table was likely recreated
    //    externally.
    for t in &v.tables {
        if let Some(state) = &t.stored_state {
            if state.snapshot_complete && t.existed && t.current_snapshot_id.is_none() {
                violations.push(Violation::SnapshotCompleteButTableNoSnapshot {
                    table: t.iceberg_name.clone(),
                });
            }
        }
    }

    // 8. Slot is `lost` — WAL recycled past `max_slot_wal_keep_size`.
    //    `wal_status = None` (pre-PG-13) skips this check.
    if let Some(slot) = &v.slot {
        if slot.exists
            && matches!(slot.wal_status, Some(pg2iceberg_pg::WalStatus::Lost))
        {
            violations.push(Violation::SlotLost {
                slot_name: v.slot_name.clone(),
                restart_lsn: slot.restart_lsn,
            });
        }
    }

    // 9. Slot is `conflicting` (PG 14+) — killed by physical-rep conflict.
    if let Some(slot) = &v.slot {
        if slot.exists && slot.conflicting {
            violations.push(Violation::SlotConflicting {
                slot_name: v.slot_name.clone(),
            });
        }
    }

    // 10. Per-table `pg_class.oid` changed → `DROP TABLE` + recreate.
    //     `stored_oid == 0` is treated as "not yet recorded" and
    //     skipped (legacy / first run). Identical to the old
    //     invariant 11; just sourced from per-table state now.
    for t in &v.tables {
        let stored_oid = t
            .stored_state
            .as_ref()
            .map(|s| s.pg_oid)
            .unwrap_or(0);
        if stored_oid == 0 {
            continue;
        }
        let current_oid = match t.current_pg_oid {
            Some(o) => o,
            None => continue, // table missing — caught by invariant 3
        };
        if stored_oid != current_oid {
            violations.push(Violation::TableIdentityChanged {
                table: t.iceberg_name.clone(),
                stored_oid,
                current_oid,
            });
        }
    }

    // 11. Tracked table missing from current publication. If we
    //     previously snapshotted it but it's not in the publication
    //     right now, any DML during the gap was filtered — re-snapshot.
    for t in &v.tables {
        let was_snapshotted = t
            .stored_state
            .as_ref()
            .map(|s| s.snapshot_complete)
            .unwrap_or(false);
        if was_snapshotted && !t.in_publication {
            violations.push(Violation::TableMissingFromPublication {
                table: t.iceberg_name.clone(),
                publication_name: v.publication_name.clone(),
            });
        }
    }

    // 12. Slot was advanced externally during downtime. Compare our
    //     durable record to the slot's reported `confirmed_flush_lsn`.
    //     Skip when:
    //     - our record is `Lsn::ZERO` (fresh install / no slot baseline yet —
    //       the lifecycle stamps the bootstrap value at slot creation)
    //     - slot doesn't exist (separate `OrphanedSlot` / `MissingTables`
    //       paths handle that)
    //     - slot ≤ our record (normal operation: the slot lags us
    //       between the coord write and the standby ack, which we
    //       intentionally allow — re-delivery is idempotent via fold).
    //
    //     Any positive gap means external advancement. We do not
    //     apply a tolerance: the standby tick writes coord *before*
    //     ack-ing the slot, so in normal operation the slot's value
    //     never exceeds our recorded value.
    if let Some(slot) = &v.slot {
        if slot.exists
            && v.coord_flushed_lsn > Lsn::ZERO
            && slot.confirmed_flush_lsn > v.coord_flushed_lsn
        {
            violations.push(Violation::SlotAdvancedExternally {
                slot_name: v.slot_name.clone(),
                coord_lsn: v.coord_flushed_lsn,
                slot_lsn: slot.confirmed_flush_lsn,
            });
        }
    }

    if violations.is_empty() {
        Ok(())
    } else {
        Err(ValidationError { violations })
    }
}
