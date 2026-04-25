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

pub mod verify;
pub mod watcher;

pub use watcher::{InvariantViolation, InvariantWatcher, WatcherInputs};

use pg2iceberg_core::{Lsn, Mode, TableIdent};
use thiserror::Error;

/// State observed at pipeline startup. Built by the binary by querying the
/// coord (for the persisted checkpoint), the source PG (for replication
/// slot info), and the catalog (for which tables exist + their snapshot
/// status).
#[derive(Clone, Debug)]
pub struct StartupValidation {
    pub checkpoint: Option<pg2iceberg_core::Checkpoint>,
    pub tables: Vec<TableExistence>,
    /// `None` for query mode (no replication slot).
    pub slot: Option<SlotState>,
    pub config_mode: Mode,
    pub slot_name: String,
}

#[derive(Clone, Debug)]
pub struct TableExistence {
    pub pg_table: TableIdent,
    pub iceberg_name: String,
    pub existed: bool,
    /// `None` for tables that didn't exist or have no snapshots yet.
    pub current_snapshot_id: Option<i64>,
}

#[derive(Clone, Debug)]
pub struct SlotState {
    pub exists: bool,
    pub restart_lsn: Lsn,
    pub confirmed_flush_lsn: Lsn,
}

/// One violation found during startup validation. Each variant prints an
/// actionable error message describing how to recover.
#[derive(Clone, Debug, Error, PartialEq)]
pub enum Violation {
    #[error(
        "checkpoint was created by {checkpoint_mode:?} mode but config specifies {config_mode:?} \
         mode; change source.mode back to {checkpoint_mode:?}, or delete the checkpoint to start fresh"
    )]
    ModeMismatch {
        checkpoint_mode: Mode,
        config_mode: Mode,
    },

    #[error(
        "no checkpoint found but Iceberg table(s) already exist: {tables:?}; delete the tables \
         to start fresh, or restore the checkpoint"
    )]
    OrphanedTables { tables: Vec<String> },

    #[error(
        "no checkpoint found but replication slot {slot_name:?} already exists; drop the slot \
         with `SELECT pg_drop_replication_slot('{slot_name}')`, or restore the checkpoint"
    )]
    OrphanedSlot { slot_name: String },

    #[error(
        "checkpoint exists but Iceberg table(s) missing: {tables:?}; delete the checkpoint to \
         re-snapshot, or recreate the tables"
    )]
    MissingTables { tables: Vec<String> },

    #[error(
        "checkpoint has LSN {checkpoint_lsn} but replication slot {slot_name:?} does not exist; \
         WAL data since that position is lost; delete the checkpoint and Iceberg tables to \
         re-snapshot"
    )]
    SlotGoneButLsnExists {
        checkpoint_lsn: Lsn,
        slot_name: String,
    },

    #[error(
        "replication slot {slot_name:?} restart_lsn ({restart_lsn}) is ahead of checkpoint LSN \
         ({checkpoint_lsn}); WAL has been recycled and data is lost; delete the checkpoint and \
         Iceberg tables to re-snapshot"
    )]
    SlotAheadOfCheckpoint {
        restart_lsn: Lsn,
        checkpoint_lsn: Lsn,
        slot_name: String,
    },

    #[error(
        "checkpoint says snapshot is complete but LSN is 0; the pipeline crashed after the \
         snapshot but before the first CDC flush; delete the checkpoint and Iceberg tables to \
         re-snapshot"
    )]
    SnapshotCompleteButLsnZero,

    #[error(
        "checkpoint says snapshot is complete but table {table:?} has no snapshots; table may \
         have been recreated externally; delete the checkpoint to re-snapshot"
    )]
    SnapshotCompleteButTableNoSnapshot { table: String },
}

/// Aggregate error returned by [`validate_startup`].
#[derive(Clone, Debug, Error)]
#[error("startup validation failed:\n  - {0:#?}", .violations)]
pub struct ValidationError {
    pub violations: Vec<Violation>,
}

pub fn validate_startup(v: &StartupValidation) -> std::result::Result<(), ValidationError> {
    let mut violations = Vec::new();
    let fresh = v.checkpoint.is_none();

    // 1. Mode mismatch.
    if let Some(cp) = &v.checkpoint {
        if cp.mode != v.config_mode {
            violations.push(Violation::ModeMismatch {
                checkpoint_mode: cp.mode,
                config_mode: v.config_mode,
            });
        }
    }

    // 2. Fresh checkpoint but Iceberg tables exist (orphans).
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

    // 3. Fresh checkpoint but replication slot exists.
    if fresh {
        if let Some(slot) = &v.slot {
            if slot.exists {
                violations.push(Violation::OrphanedSlot {
                    slot_name: v.slot_name.clone(),
                });
            }
        }
    }

    // 4. Checkpoint exists but previously-tracked tables missing.
    if let Some(cp) = &v.checkpoint {
        let tracked: std::collections::BTreeSet<&TableIdent> = cp.tracked_tables.iter().collect();
        let missing: Vec<String> = v
            .tables
            .iter()
            .filter(|t| !t.existed && tracked.contains(&t.pg_table))
            .map(|t| t.iceberg_name.clone())
            .collect();
        if !missing.is_empty() {
            violations.push(Violation::MissingTables { tables: missing });
        }
    }

    // 5. Checkpoint has LSN > 0 but slot is gone.
    if let Some(cp) = &v.checkpoint {
        if cp.flushed_lsn > Lsn::ZERO {
            if let Some(slot) = &v.slot {
                if !slot.exists {
                    violations.push(Violation::SlotGoneButLsnExists {
                        checkpoint_lsn: cp.flushed_lsn,
                        slot_name: v.slot_name.clone(),
                    });
                }
            }
        }
    }

    // 6. Slot's restart_lsn is ahead of checkpoint LSN (WAL recycled).
    if let Some(cp) = &v.checkpoint {
        if cp.flushed_lsn > Lsn::ZERO {
            if let Some(slot) = &v.slot {
                if slot.exists && slot.restart_lsn > cp.flushed_lsn {
                    violations.push(Violation::SlotAheadOfCheckpoint {
                        restart_lsn: slot.restart_lsn,
                        checkpoint_lsn: cp.flushed_lsn,
                        slot_name: v.slot_name.clone(),
                    });
                }
            }
        }
    }

    // 7. Snapshot complete but LSN = 0 (crashed after snapshot, before first CDC flush).
    //    Only applies to logical mode. Query mode doesn't track LSN.
    if let Some(cp) = &v.checkpoint {
        if cp.snapshot_state == pg2iceberg_core::SnapshotState::Complete
            && cp.flushed_lsn == Lsn::ZERO
            && v.config_mode == Mode::Logical
        {
            violations.push(Violation::SnapshotCompleteButLsnZero);
        }
    }

    // 8. Snapshot complete but a tracked table has no snapshots in the catalog.
    if let Some(cp) = &v.checkpoint {
        if cp.snapshot_state == pg2iceberg_core::SnapshotState::Complete {
            for t in &v.tables {
                if t.existed && t.current_snapshot_id.is_none() {
                    violations.push(Violation::SnapshotCompleteButTableNoSnapshot {
                        table: t.iceberg_name.clone(),
                    });
                }
            }
        }
    }

    if violations.is_empty() {
        Ok(())
    } else {
        Err(ValidationError { violations })
    }
}
