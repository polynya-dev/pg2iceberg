//! Startup validation tests — one test per of the 8 plan-§8 checks, plus a
//! happy-path "all good" test.
//!
//! Each test constructs a `StartupValidation` that *should* trip exactly
//! one violation, then asserts the violation list contains it.

use pg2iceberg_core::{Checkpoint, Lsn, Mode, Namespace, TableIdent};
use pg2iceberg_validate::{
    validate_startup, SlotState, StartupValidation, TableExistence, ValidationError, Violation,
};

fn ident(name: &str) -> TableIdent {
    TableIdent {
        namespace: Namespace(vec!["public".into()]),
        name: name.into(),
    }
}

fn fresh_logical() -> StartupValidation {
    StartupValidation {
        checkpoint: None,
        tables: vec![],
        slot: Some(SlotState {
            exists: false,
            restart_lsn: Lsn::ZERO,
            confirmed_flush_lsn: Lsn::ZERO,
            ..Default::default()
        }),
        config_mode: Mode::Logical,
        slot_name: "pg2iceberg".into(),
        ..Default::default()
    }
}

fn cp_logical(flushed_lsn: u64) -> Checkpoint {
    let mut cp = Checkpoint::fresh(Mode::Logical);
    cp.flushed_lsn = Lsn(flushed_lsn);
    cp.snapshot_complete = true;
    cp
}

fn assert_one_violation(err: &ValidationError, expected: &Violation) {
    assert!(
        err.violations.iter().any(|v| v == expected),
        "expected violation {:?} not in {:?}",
        expected,
        err.violations
    );
}

#[test]
fn happy_path_passes() {
    let v = fresh_logical();
    assert!(validate_startup(&v).is_ok());
}

// 1. Mode mismatch.
#[test]
fn mode_mismatch_violation() {
    let mut v = fresh_logical();
    v.checkpoint = Some(Checkpoint {
        mode: Mode::Query, // checkpoint says query
        ..cp_logical(0)
    });
    v.config_mode = Mode::Logical; // config says logical
    let err = validate_startup(&v).unwrap_err();
    assert_one_violation(
        &err,
        &Violation::ModeMismatch {
            checkpoint_mode: Mode::Query,
            config_mode: Mode::Logical,
        },
    );
}

// 2. Fresh checkpoint but Iceberg tables exist.
#[test]
fn orphaned_tables_violation() {
    let mut v = fresh_logical();
    v.tables.push(TableExistence {
        pg_table: ident("orders"),
        iceberg_name: "orders".into(),
        existed: true,
        current_snapshot_id: Some(1),
        ..Default::default()
    });
    let err = validate_startup(&v).unwrap_err();
    assert_one_violation(
        &err,
        &Violation::OrphanedTables {
            tables: vec!["orders".into()],
        },
    );
}

// 3. Fresh checkpoint but replication slot exists.
#[test]
fn orphaned_slot_violation() {
    let mut v = fresh_logical();
    v.slot = Some(SlotState {
        exists: true,
        restart_lsn: Lsn(50),
        confirmed_flush_lsn: Lsn(50),
        ..Default::default()
    });
    let err = validate_startup(&v).unwrap_err();
    assert_one_violation(
        &err,
        &Violation::OrphanedSlot {
            slot_name: "pg2iceberg".into(),
        },
    );
}

// 4. Checkpoint exists but tracked table is missing from Iceberg.
#[test]
fn missing_tables_violation() {
    let mut v = fresh_logical();
    let mut cp = cp_logical(100);
    cp.snapshoted_tables.insert("public.orders".into(), true);
    v.checkpoint = Some(cp);
    v.tables.push(TableExistence {
        pg_table: ident("orders"),
        iceberg_name: "orders".into(),
        existed: false, // user dropped the Iceberg table
        current_snapshot_id: None,
        ..Default::default()
    });
    v.slot = Some(SlotState {
        exists: true,
        restart_lsn: Lsn(100),
        confirmed_flush_lsn: Lsn(100),
        ..Default::default()
    });
    let err = validate_startup(&v).unwrap_err();
    assert_one_violation(
        &err,
        &Violation::MissingTables {
            tables: vec!["orders".into()],
        },
    );
}

// 5. Checkpoint has LSN but slot is gone.
#[test]
fn slot_gone_but_lsn_exists_violation() {
    let mut v = fresh_logical();
    v.checkpoint = Some(cp_logical(100));
    v.slot = Some(SlotState {
        exists: false, // slot disappeared
        restart_lsn: Lsn::ZERO,
        confirmed_flush_lsn: Lsn::ZERO,
        ..Default::default()
    });
    let err = validate_startup(&v).unwrap_err();
    assert_one_violation(
        &err,
        &Violation::SlotGoneButLsnExists {
            checkpoint_lsn: Lsn(100),
            slot_name: "pg2iceberg".into(),
        },
    );
}

// 6. Slot's restart_lsn is ahead of checkpoint LSN (WAL recycled).
#[test]
fn slot_ahead_of_checkpoint_violation() {
    let mut v = fresh_logical();
    v.checkpoint = Some(cp_logical(100));
    v.slot = Some(SlotState {
        exists: true,
        restart_lsn: Lsn(200), // WAL recycled past checkpoint
        confirmed_flush_lsn: Lsn(200),
        ..Default::default()
    });
    let err = validate_startup(&v).unwrap_err();
    assert_one_violation(
        &err,
        &Violation::SlotAheadOfCheckpoint {
            restart_lsn: Lsn(200),
            checkpoint_lsn: Lsn(100),
            slot_name: "pg2iceberg".into(),
        },
    );
}

// 7. Snapshot complete but LSN is 0 (crashed after snapshot).
#[test]
fn snapshot_complete_but_lsn_zero_violation() {
    let mut v = fresh_logical();
    let mut cp = cp_logical(0);
    cp.snapshot_complete = true;
    v.checkpoint = Some(cp);
    v.slot = Some(SlotState {
        exists: true,
        restart_lsn: Lsn::ZERO,
        confirmed_flush_lsn: Lsn::ZERO,
        ..Default::default()
    });
    let err = validate_startup(&v).unwrap_err();
    assert_one_violation(&err, &Violation::SnapshotCompleteButLsnZero);
}

// 7 — query mode shouldn't trigger because LSN doesn't apply.
#[test]
fn snapshot_complete_lsn_zero_in_query_mode_is_ok() {
    let mut v = fresh_logical();
    v.config_mode = Mode::Query;
    let mut cp = Checkpoint::fresh(Mode::Query);
    cp.snapshot_complete = true;
    v.checkpoint = Some(cp);
    v.slot = None;
    assert!(validate_startup(&v).is_ok());
}

// 8. Snapshot complete but a table has no current snapshot.
#[test]
fn snapshot_complete_but_table_has_no_snapshot_violation() {
    let mut v = fresh_logical();
    let mut cp = cp_logical(100);
    cp.snapshot_complete = true;
    v.checkpoint = Some(cp);
    v.slot = Some(SlotState {
        exists: true,
        restart_lsn: Lsn(100),
        confirmed_flush_lsn: Lsn(100),
        ..Default::default()
    });
    v.tables.push(TableExistence {
        pg_table: ident("orders"),
        iceberg_name: "orders".into(),
        existed: true,
        current_snapshot_id: None, // table exists but has no snapshot
        ..Default::default()
    });
    let err = validate_startup(&v).unwrap_err();
    assert_one_violation(
        &err,
        &Violation::SnapshotCompleteButTableNoSnapshot {
            table: "orders".into(),
        },
    );
}

// 9. Slot wal_status == Lost — WAL recycled past `max_slot_wal_keep_size`.
#[test]
fn slot_lost_violation() {
    let mut v = fresh_logical();
    v.checkpoint = Some(cp_logical(100));
    v.slot = Some(SlotState {
        exists: true,
        restart_lsn: Lsn(50),
        confirmed_flush_lsn: Lsn(50),
        wal_status: Some(pg2iceberg_pg::WalStatus::Lost),
        conflicting: false,
    });
    let err = validate_startup(&v).unwrap_err();
    assert_one_violation(
        &err,
        &Violation::SlotLost {
            slot_name: "pg2iceberg".into(),
            restart_lsn: Lsn(50),
        },
    );
}

// 9b. wal_status = Reserved/Extended/Unreserved is NOT a startup violation.
//     `Unreserved` is a watcher warning, not a startup-fail. The pipeline
//     should still attempt to start so the operator can act on the
//     warning before WAL is actually recycled.
#[test]
fn slot_unreserved_does_not_fail_startup() {
    let mut v = fresh_logical();
    v.checkpoint = Some(cp_logical(100));
    v.slot = Some(SlotState {
        exists: true,
        restart_lsn: Lsn(100),
        confirmed_flush_lsn: Lsn(100),
        wal_status: Some(pg2iceberg_pg::WalStatus::Unreserved),
        conflicting: false,
    });
    // Healthy + LSN-aligned + Unreserved → no violations from startup.
    assert!(validate_startup(&v).is_ok());
}

// 10. Slot conflicting (PG 14+) — physical-rep conflict killed the slot.
#[test]
fn slot_conflicting_violation() {
    let mut v = fresh_logical();
    v.checkpoint = Some(cp_logical(100));
    v.slot = Some(SlotState {
        exists: true,
        restart_lsn: Lsn(100),
        confirmed_flush_lsn: Lsn(100),
        wal_status: Some(pg2iceberg_pg::WalStatus::Reserved),
        conflicting: true,
    });
    let err = validate_startup(&v).unwrap_err();
    assert_one_violation(
        &err,
        &Violation::SlotConflicting {
            slot_name: "pg2iceberg".into(),
        },
    );
}

// Multiple violations stack.
#[test]
fn multiple_violations_all_reported() {
    // Fresh checkpoint + orphaned slot + orphaned table.
    let mut v = fresh_logical();
    v.tables.push(TableExistence {
        pg_table: ident("orders"),
        iceberg_name: "orders".into(),
        existed: true,
        current_snapshot_id: Some(1),
        ..Default::default()
    });
    v.slot = Some(SlotState {
        exists: true,
        restart_lsn: Lsn(50),
        confirmed_flush_lsn: Lsn(50),
        ..Default::default()
    });
    let err = validate_startup(&v).unwrap_err();
    assert_eq!(err.violations.len(), 2, "got: {:?}", err.violations);
}
