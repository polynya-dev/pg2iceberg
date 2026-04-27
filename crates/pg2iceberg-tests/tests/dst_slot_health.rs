//! DST coverage for the `pg_replication_slots` health surface.
//!
//! Three pinned scenarios:
//!
//! 1. `lost_slot_fails_startup_validation` — slot's `wal_status = lost`
//!    (WAL recycled past `max_slot_wal_keep_size`). The lifecycle's
//!    startup validation must surface `Violation::SlotLost` and refuse
//!    to start. Without this guard, `START_REPLICATION` would fail
//!    mid-startup with a confusing PG error and (worse) a naive
//!    drop+recreate would silently skip data.
//!
//! 2. `conflicting_slot_fails_startup_validation` — slot's
//!    `conflicting = true` (PG 14+ physical-replication-conflict
//!    kill). Same fail-fast contract as `lost`.
//!
//! 3. `unreserved_slot_emits_watcher_warning` — slot's
//!    `wal_status = unreserved`. The pipeline keeps running (last
//!    chance to fix consumer lag) but the watcher logs an
//!    `InvariantViolation::SlotWalUnreserved` so the operator gets
//!    paged.
//!
//! All three exercise the production path:
//!   `SimPgClient::slot_health` → `SlotMonitor::slot_health_for_watcher`
//!   → `validate_startup` / `InvariantWatcher::check`
//! with the same trait surface the binary uses against real PG. A
//! prod-only bug would mean either the SQL query is wrong (covered by
//! Phase B integration tests) or the trait wiring is wrong (covered
//! here).

use std::sync::Arc;

use pg2iceberg_core::{
    Checkpoint, ColumnName, ColumnSchema, IcebergType, Lsn, Mode, Namespace, TableIdent,
    TableSchema,
};
use pg2iceberg_pg::WalStatus;
use pg2iceberg_sim::clock::TestClock;
use pg2iceberg_sim::coord::MemoryCoordinator;
use pg2iceberg_sim::postgres::{SimPgClient, SimPostgres, SimWalStatus};
use pg2iceberg_validate::{
    run_watcher_tick, validate_startup, InvariantViolation, InvariantWatcher, SlotState,
    StartupValidation, TableExistence, Violation,
};
use pollster::block_on;

const SLOT: &str = "p2i-slot";
const PUB: &str = "p2i-pub";

fn ident() -> TableIdent {
    TableIdent {
        namespace: Namespace(vec!["public".into()]),
        name: "orders".into(),
    }
}

fn schema() -> TableSchema {
    TableSchema {
        ident: ident(),
        columns: vec![ColumnSchema {
            name: "id".into(),
            field_id: 1,
            ty: IcebergType::Int,
            nullable: false,
            is_primary_key: true,
        }],
        partition_spec: vec![],
    }
}

/// Sim-driven equivalent of `run_startup_validation` — same call
/// sequence the lifecycle uses, but reachable from a test without
/// instantiating the full LogicalLifecycle. Goes through
/// `slot_health_for_watcher` → `slot_health` so the SimPgClient
/// trait wiring is exercised end-to-end.
async fn build_startup_validation(
    db: &SimPostgres,
    slot_name: &str,
) -> StartupValidation {
    let pg = SimPgClient::new(db.clone());
    let pg_arc: Arc<dyn pg2iceberg_pg::PgClient> = Arc::new(pg);

    let health = pg_arc.slot_health(slot_name).await.unwrap();
    let slot = match health {
        Some(h) => Some(SlotState {
            exists: true,
            restart_lsn: h.restart_lsn,
            confirmed_flush_lsn: h.confirmed_flush_lsn,
            wal_status: h.wal_status,
            conflicting: h.conflicting,
        }),
        None => Some(SlotState {
            exists: false,
            restart_lsn: Lsn::ZERO,
            confirmed_flush_lsn: Lsn::ZERO,
            wal_status: None,
            conflicting: false,
        }),
    };

    // Stage a Complete checkpoint so the irrelevant invariants don't
    // also fire. We're isolating the slot-health path.
    let mut cp = Checkpoint::fresh(Mode::Logical);
    cp.snapshot_complete = true;
    cp.flushed_lsn = Lsn(100);
    cp.snapshoted_tables.insert(ident().to_string(), true);

    StartupValidation {
        checkpoint: Some(cp),
        tables: vec![TableExistence {
            pg_table: ident(),
            iceberg_name: "public.orders".into(),
            existed: true,
            current_snapshot_id: Some(1),
        }],
        slot,
        config_mode: Mode::Logical,
        slot_name: slot_name.into(),
    }
}

#[test]
fn lost_slot_fails_startup_validation() {
    let db = SimPostgres::new();
    db.create_table(schema()).unwrap();
    db.create_publication(PUB, &[ident()]).unwrap();
    db.create_slot(SLOT, PUB).unwrap();
    // Force the slot into the unrecoverable `lost` state.
    db.set_slot_wal_status(SLOT, SimWalStatus::Lost).unwrap();

    let v = block_on(build_startup_validation(&db, SLOT));
    let err = validate_startup(&v).expect_err("lost slot must fail validation");

    assert!(
        err.violations.iter().any(|x| matches!(
            x,
            Violation::SlotLost { slot_name, .. } if slot_name == SLOT
        )),
        "expected SlotLost in violations, got {:?}",
        err.violations
    );
}

#[test]
fn conflicting_slot_fails_startup_validation() {
    let db = SimPostgres::new();
    db.create_table(schema()).unwrap();
    db.create_publication(PUB, &[ident()]).unwrap();
    db.create_slot(SLOT, PUB).unwrap();
    db.set_slot_conflicting(SLOT, true).unwrap();

    let v = block_on(build_startup_validation(&db, SLOT));
    let err = validate_startup(&v).expect_err("conflicting slot must fail validation");

    assert!(
        err.violations.iter().any(|x| matches!(
            x,
            Violation::SlotConflicting { slot_name } if slot_name == SLOT
        )),
        "expected SlotConflicting in violations, got {:?}",
        err.violations
    );
}

#[test]
fn unreserved_slot_does_not_fail_startup_but_emits_watcher_warning() {
    // Two-stage check: startup is non-fatal (operator's last chance to
    // act); watcher tick observes and warns.
    let db = SimPostgres::new();
    db.create_table(schema()).unwrap();
    db.create_publication(PUB, &[ident()]).unwrap();
    db.create_slot(SLOT, PUB).unwrap();
    db.set_slot_wal_status(SLOT, SimWalStatus::Unreserved).unwrap();

    // Startup: must NOT fail on unreserved alone.
    let v = block_on(build_startup_validation(&db, SLOT));
    assert!(
        validate_startup(&v).is_ok(),
        "unreserved alone should not fail startup; got {:?}",
        validate_startup(&v).err()
    );

    // Watcher tick: must surface SlotWalUnreserved.
    let pg = Arc::new(SimPgClient::new(db.clone()));
    let pg_dyn: Arc<dyn pg2iceberg_pg::SlotMonitor> = pg.clone();
    let health = block_on(pg_dyn.slot_health_for_watcher(SLOT))
        .unwrap()
        .expect("slot exists");

    let clock = TestClock::at(0);
    let arc_clock: Arc<dyn pg2iceberg_core::Clock> = Arc::new(clock);
    let coord = Arc::new(MemoryCoordinator::new(
        pg2iceberg_coord::schema::CoordSchema::default_name(),
        arc_clock,
    ));
    let metrics: Arc<dyn pg2iceberg_core::Metrics> =
        Arc::new(pg2iceberg_core::InMemoryMetrics::new());
    let watcher = InvariantWatcher::new(
        coord as Arc<dyn pg2iceberg_coord::Coordinator>,
        metrics,
    );

    let violations = block_on(run_watcher_tick(
        &watcher,
        Lsn(100),
        health.confirmed_flush_lsn,
        health.wal_status,
        health.safe_wal_size,
        SLOT,
        "default",
        &[],
    ));

    assert!(
        violations.iter().any(|v| matches!(
            v,
            InvariantViolation::SlotWalUnreserved { slot_name, .. } if slot_name == SLOT
        )),
        "expected SlotWalUnreserved in violations, got {:?}",
        violations
    );
}

#[test]
fn reserved_slot_passes_both_startup_and_watcher() {
    // Healthy slot: no startup violation, no watcher warning. Locks
    // in that the new invariants don't false-positive on the common
    // path.
    let db = SimPostgres::new();
    db.create_table(schema()).unwrap();
    db.create_publication(PUB, &[ident()]).unwrap();
    db.create_slot(SLOT, PUB).unwrap();

    let v = block_on(build_startup_validation(&db, SLOT));
    assert!(validate_startup(&v).is_ok());

    let pg = Arc::new(SimPgClient::new(db.clone()));
    let pg_dyn: Arc<dyn pg2iceberg_pg::SlotMonitor> = pg.clone();
    let health = block_on(pg_dyn.slot_health_for_watcher(SLOT))
        .unwrap()
        .expect("slot exists");
    assert_eq!(health.wal_status, Some(WalStatus::Reserved));
    assert!(!health.conflicting);

    let clock = TestClock::at(0);
    let arc_clock: Arc<dyn pg2iceberg_core::Clock> = Arc::new(clock);
    let coord = Arc::new(MemoryCoordinator::new(
        pg2iceberg_coord::schema::CoordSchema::default_name(),
        arc_clock,
    ));
    let metrics: Arc<dyn pg2iceberg_core::Metrics> =
        Arc::new(pg2iceberg_core::InMemoryMetrics::new());
    let watcher = InvariantWatcher::new(
        coord as Arc<dyn pg2iceberg_coord::Coordinator>,
        metrics,
    );

    let violations = block_on(run_watcher_tick(
        &watcher,
        Lsn(100),
        health.confirmed_flush_lsn,
        health.wal_status,
        health.safe_wal_size,
        SLOT,
        "default",
        &[],
    ));
    assert!(
        !violations
            .iter()
            .any(|v| matches!(v, InvariantViolation::SlotWalUnreserved { .. })),
        "Reserved slot must NOT trip SlotWalUnreserved; got {:?}",
        violations
    );
}
