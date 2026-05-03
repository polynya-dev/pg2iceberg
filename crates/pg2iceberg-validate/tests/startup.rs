//! Startup validation tests — one per invariant in `validate_startup`.
//!
//! Each test constructs a [`StartupValidation`] that *should* trip
//! exactly one violation, then asserts the violation list contains it.
//!
//! Per-table state replaces the old single-blob `Checkpoint`; each
//! [`TableExistence::stored_state`] is the per-row image of what
//! `_pg2iceberg.tables` holds. `None` = "fresh" (treated as
//! never-snapshotted).

use pg2iceberg_coord::TableSnapshotState;
use pg2iceberg_core::{Lsn, Mode, Namespace, TableIdent};
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

/// A per-table snapshot state — "this table has been snapshotted at
/// LSN `lsn`, with `pg_oid`." Common shape for the post-snapshot
/// invariants.
fn snapshot_state(pg_oid: u32, lsn: u64) -> TableSnapshotState {
    TableSnapshotState {
        pg_oid,
        snapshot_complete: true,
        snapshot_lsn: Lsn(lsn),
        completed_at_micros: Some(1_000_000),
    }
}

/// Build a `TableExistence` with the per-table state stamped in. Used
/// by post-snapshot tests that need the validator to think this table
/// was already through the snapshot phase.
fn snapshotted_table(name: &str, existed: bool, lsn: u64) -> TableExistence {
    TableExistence {
        pg_table: ident(name),
        iceberg_name: name.into(),
        existed,
        current_snapshot_id: if existed { Some(1) } else { None },
        current_pg_oid: Some(42),
        in_publication: true,
        stored_state: Some(snapshot_state(42, lsn)),
    }
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

// 1. Fresh install but Iceberg tables exist.
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

// 2. Fresh install but replication slot exists.
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

// 3. Per-table state recorded but Iceberg table is missing.
#[test]
fn missing_tables_violation() {
    let mut v = fresh_logical();
    // Table was previously snapshotted (state recorded) but the user
    // dropped the Iceberg table.
    v.tables.push(snapshotted_table("orders", false, 100));
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

// 4. Snapshot LSN > 0 but slot is gone.
#[test]
fn slot_gone_but_lsn_exists_violation() {
    let mut v = fresh_logical();
    v.tables.push(snapshotted_table("orders", true, 100));
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
            snapshot_lsn: Lsn(100),
            slot_name: "pg2iceberg".into(),
        },
    );
}

// 5. Slot's restart_lsn is ahead of `coord_flushed_lsn` (the moving
//    checkpoint). Compares against `coord_flushed_lsn` rather than
//    `snapshot_lsn` because snapshot_lsn doesn't move with CDC, so
//    using it would false-positive on every restart with normal CDC
//    progress (the chaos soak surfaced this).
#[test]
fn slot_ahead_of_checkpoint_violation() {
    let mut v = fresh_logical();
    v.tables.push(snapshotted_table("orders", true, 100));
    v.coord_flushed_lsn = Lsn(150);
    v.slot = Some(SlotState {
        exists: true,
        restart_lsn: Lsn(200), // ahead of coord checkpoint — tamper / external advance
        confirmed_flush_lsn: Lsn(200),
        ..Default::default()
    });
    let err = validate_startup(&v).unwrap_err();
    assert_one_violation(
        &err,
        &Violation::SlotAheadOfCheckpoint {
            restart_lsn: Lsn(200),
            snapshot_lsn: Lsn(150),
            slot_name: "pg2iceberg".into(),
        },
    );
}

/// Slot ahead of snapshot_lsn but in lockstep with coord_flushed_lsn —
/// this is normal CDC progress and must NOT trip
/// `SlotAheadOfCheckpoint`.
#[test]
fn slot_ahead_of_snapshot_lsn_but_tracking_coord_does_not_violate() {
    let mut v = fresh_logical();
    v.tables.push(snapshotted_table("orders", true, 100));
    // coord_flushed_lsn moves with the slot; snapshot_lsn stays at
    // the original snapshot completion point.
    v.coord_flushed_lsn = Lsn(200);
    v.slot = Some(SlotState {
        exists: true,
        restart_lsn: Lsn(200),
        confirmed_flush_lsn: Lsn(200),
        ..Default::default()
    });
    validate_startup(&v).expect("normal CDC progress should validate cleanly");
}

// 6. Snapshot complete but LSN is 0 (crashed mid-stamp).
#[test]
fn snapshot_complete_but_lsn_zero_violation() {
    let mut v = fresh_logical();
    v.tables.push(TableExistence {
        pg_table: ident("orders"),
        iceberg_name: "orders".into(),
        existed: true,
        current_snapshot_id: Some(1),
        current_pg_oid: Some(42),
        in_publication: true,
        stored_state: Some(TableSnapshotState {
            pg_oid: 42,
            snapshot_complete: true,
            snapshot_lsn: Lsn::ZERO, // crashed before LSN stamp
            completed_at_micros: Some(0),
        }),
    });
    v.slot = Some(SlotState {
        exists: true,
        restart_lsn: Lsn::ZERO,
        confirmed_flush_lsn: Lsn::ZERO,
        ..Default::default()
    });
    let err = validate_startup(&v).unwrap_err();
    assert_one_violation(
        &err,
        &Violation::SnapshotCompleteButLsnZero {
            table: "orders".into(),
        },
    );
}

// 6 — query mode shouldn't trigger because LSN doesn't apply.
#[test]
fn snapshot_complete_lsn_zero_in_query_mode_is_ok() {
    let mut v = fresh_logical();
    v.config_mode = Mode::Query;
    v.tables.push(TableExistence {
        pg_table: ident("orders"),
        iceberg_name: "orders".into(),
        existed: true,
        current_snapshot_id: Some(1),
        current_pg_oid: Some(42),
        in_publication: true,
        stored_state: Some(TableSnapshotState {
            pg_oid: 42,
            snapshot_complete: true,
            snapshot_lsn: Lsn::ZERO,
            completed_at_micros: Some(0),
        }),
    });
    v.slot = None;
    assert!(validate_startup(&v).is_ok());
}

// 7. Snapshot complete but Iceberg table has no snapshots — used to
//    fire `SnapshotCompleteButTableNoSnapshot`. The invariant was
//    removed (see lib.rs comment); a legitimate `AddTable` on an
//    empty source table reaches this exact state and shouldn't be
//    flagged. Identity changes are still caught via invariant 11.
#[test]
fn snapshot_complete_but_table_has_no_snapshot_does_not_violate() {
    let mut v = fresh_logical();
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
        current_snapshot_id: None, // empty table, never had data — OK
        current_pg_oid: Some(42),
        in_publication: true,
        stored_state: Some(snapshot_state(42, 100)),
    });
    validate_startup(&v).expect("empty-table state should validate cleanly");
}

// 8. Slot wal_status == Lost — WAL recycled past `max_slot_wal_keep_size`.
#[test]
fn slot_lost_violation() {
    let mut v = fresh_logical();
    v.tables.push(snapshotted_table("orders", true, 100));
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

// 8b. wal_status = Reserved/Extended/Unreserved is NOT a startup violation.
#[test]
fn slot_unreserved_does_not_fail_startup() {
    let mut v = fresh_logical();
    v.tables.push(snapshotted_table("orders", true, 100));
    v.slot = Some(SlotState {
        exists: true,
        restart_lsn: Lsn(100),
        confirmed_flush_lsn: Lsn(100),
        wal_status: Some(pg2iceberg_pg::WalStatus::Unreserved),
        conflicting: false,
    });
    assert!(validate_startup(&v).is_ok());
}

// 9. Slot conflicting (PG 14+) — physical-rep conflict killed the slot.
#[test]
fn slot_conflicting_violation() {
    let mut v = fresh_logical();
    v.tables.push(snapshotted_table("orders", true, 100));
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

// 10. Per-table pg_class.oid changed → DROP TABLE + recreate.
#[test]
fn table_identity_changed_violation() {
    let mut v = fresh_logical();
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
        current_snapshot_id: Some(1),
        current_pg_oid: Some(99), // different from stored
        in_publication: true,
        stored_state: Some(snapshot_state(42, 100)),
    });
    let err = validate_startup(&v).unwrap_err();
    assert_one_violation(
        &err,
        &Violation::TableIdentityChanged {
            table: "orders".into(),
            stored_oid: 42,
            current_oid: 99,
        },
    );
}

// 11. Tracked table missing from publication.
#[test]
fn table_missing_from_publication_violation() {
    let mut v = fresh_logical();
    v.publication_name = "pg2iceberg_pub".into();
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
        current_snapshot_id: Some(1),
        current_pg_oid: Some(42),
        in_publication: false, // operator dropped from publication
        stored_state: Some(snapshot_state(42, 100)),
    });
    let err = validate_startup(&v).unwrap_err();
    assert_one_violation(
        &err,
        &Violation::TableMissingFromPublication {
            table: "orders".into(),
            publication_name: "pg2iceberg_pub".into(),
        },
    );
}

// 12. Slot's confirmed_flush_lsn is ahead of our recorded baseline →
//     external advancement (`pg_replication_slot_advance`,
//     drop+recreate, stray `pg_recvlogical`).
#[test]
fn slot_advanced_externally_violation() {
    let mut v = fresh_logical();
    v.tables.push(snapshotted_table("orders", true, 100));
    // We last acked at 100. Slot is now at 5000 — 4900 LSNs of
    // skipped WAL we never saw.
    v.coord_flushed_lsn = Lsn(100);
    v.slot = Some(SlotState {
        exists: true,
        restart_lsn: Lsn(100),
        confirmed_flush_lsn: Lsn(5000),
        ..Default::default()
    });
    let err = validate_startup(&v).unwrap_err();
    assert_one_violation(
        &err,
        &Violation::SlotAdvancedExternally {
            slot_name: "pg2iceberg".into(),
            coord_lsn: Lsn(100),
            slot_lsn: Lsn(5000),
        },
    );
}

// 12 — Bootstrap: coord_flushed_lsn = 0 means no baseline yet
//      (fresh install before slot creation, or pre-tamper-check
//      coord state). Skip the check.
#[test]
fn slot_advanced_externally_skipped_when_no_baseline() {
    let mut v = fresh_logical();
    v.tables.push(snapshotted_table("orders", true, 100));
    v.coord_flushed_lsn = Lsn::ZERO; // bootstrap
    v.slot = Some(SlotState {
        exists: true,
        restart_lsn: Lsn(100),
        confirmed_flush_lsn: Lsn(5000),
        ..Default::default()
    });
    // Slot is way ahead but no baseline to compare against — skip.
    // Other invariants may fire, but not SlotAdvancedExternally.
    let err = validate_startup(&v);
    if let Err(e) = err {
        assert!(
            !e.violations
                .iter()
                .any(|x| matches!(x, Violation::SlotAdvancedExternally { .. })),
            "tamper check must skip when coord_flushed_lsn is zero; got {:?}",
            e.violations
        );
    }
}

// 12 — Slot lagging or matching our record is normal (the standby
//      ack hasn't fired since the last coord write, or both are in
//      sync).
#[test]
fn slot_at_or_behind_coord_record_is_ok() {
    let mut v = fresh_logical();
    v.tables.push(snapshotted_table("orders", true, 100));
    v.coord_flushed_lsn = Lsn(5000);
    // Slot lags us by one tick's worth — normal: we wrote coord
    // before the standby ack and crashed in between.
    v.slot = Some(SlotState {
        exists: true,
        restart_lsn: Lsn(100),
        confirmed_flush_lsn: Lsn(4500),
        ..Default::default()
    });
    let err = validate_startup(&v);
    if let Err(e) = err {
        assert!(
            !e.violations
                .iter()
                .any(|x| matches!(x, Violation::SlotAdvancedExternally { .. })),
            "slot lagging coord must not trigger tamper; got {:?}",
            e.violations
        );
    }
}

#[test]
fn slot_exactly_matches_coord_record_is_ok() {
    let mut v = fresh_logical();
    v.tables.push(snapshotted_table("orders", true, 100));
    v.coord_flushed_lsn = Lsn(5000);
    v.slot = Some(SlotState {
        exists: true,
        restart_lsn: Lsn(100),
        confirmed_flush_lsn: Lsn(5000),
        ..Default::default()
    });
    assert!(validate_startup(&v).is_ok());
}

// Multiple violations stack.
#[test]
fn multiple_violations_all_reported() {
    // Fresh install + orphaned slot + orphaned table.
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
