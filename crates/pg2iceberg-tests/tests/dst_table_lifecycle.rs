//! DST coverage for the table-lifecycle invariants:
//!
//! 1. **Identity change** (`Violation::TableIdentityChanged`): operator
//!    runs `DROP TABLE` + recreate in PG. New `pg_class.oid`, but the
//!    Iceberg table still holds the pre-drop rows. CDC won't replay
//!    them. Startup must refuse to resume.
//!
//! 2. **Publication-membership drift**
//!    (`Violation::TableMissingFromPublication`): operator runs
//!    `ALTER PUBLICATION DROP TABLE` then later wants to re-add. DML
//!    during the gap was filtered by the slot. Re-adding to the
//!    publication and resuming would silently miss those events.
//!    Startup must refuse.
//!
//! 3. **Stale tracked-table warning**: operator removed a table from
//!    YAML but `cp.snapshoted_tables` still has it. We don't fail —
//!    just emit a tracing warning so log_index for the orphan
//!    doesn't grow silently. (Tested via behavior — startup passes,
//!    no violation surfaces.)
//!
//! All scenarios go through the production trait surface
//! (`SimPgClient::table_oid`, `publication_tables`,
//! `slot_health_for_watcher`) so wiring bugs in either prod or sim
//! show up here.

use std::sync::Arc;

use pg2iceberg_core::{
    Checkpoint, ColumnSchema, IcebergType, Lsn, Mode, Namespace, TableIdent, TableSchema,
};
use pg2iceberg_sim::postgres::{SimPgClient, SimPostgres};
use pg2iceberg_validate::{
    validate_startup, SlotState, StartupValidation, TableExistence, Violation,
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

/// Build a healthy `StartupValidation` from sim state, then let
/// caller mutate `cp.snapshoted_table_oids` / `snapshoted_tables` to
/// model "previously snapshotted at oid X."
async fn build_startup(
    db: &SimPostgres,
    cp: Checkpoint,
) -> StartupValidation {
    let pg: Arc<dyn pg2iceberg_pg::PgClient> = Arc::new(SimPgClient::new(db.clone()));

    let pub_members: std::collections::BTreeSet<TableIdent> = pg
        .publication_tables(PUB)
        .await
        .unwrap_or_default()
        .into_iter()
        .collect();

    let pg_oid = pg
        .table_oid(&ident().namespace.0.join("."), &ident().name)
        .await
        .unwrap();

    let health = pg.slot_health(SLOT).await.unwrap();
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

    StartupValidation {
        checkpoint: Some(cp),
        tables: vec![TableExistence {
            pg_table: ident(),
            iceberg_name: "public.orders".into(),
            existed: true,
            current_snapshot_id: Some(1),
            current_pg_oid: pg_oid,
            in_publication: pub_members.contains(&ident()),
        }],
        slot,
        config_mode: Mode::Logical,
        slot_name: SLOT.into(),
        publication_name: PUB.into(),
    }
}

#[test]
fn drop_recreate_table_fires_table_identity_changed() {
    let db = SimPostgres::new();
    db.create_table(schema()).unwrap();
    db.create_publication(PUB, &[ident()]).unwrap();
    db.create_slot(SLOT, PUB).unwrap();

    // Capture the original oid (the one we'd have stamped in
    // snapshoted_table_oids when the snapshot phase first ran).
    let original_oid = db.table_oid(&ident()).expect("table exists");

    // Operator runs `DROP TABLE` + recreate. Sim helper bumps the oid.
    db.drop_and_recreate_table(schema()).unwrap();
    let new_oid = db.table_oid(&ident()).expect("table exists");
    assert_ne!(original_oid, new_oid, "recreate must yield fresh oid");

    // Stage a checkpoint that says "we snapshotted at original_oid."
    let mut cp = Checkpoint::fresh(Mode::Logical);
    cp.snapshot_complete = true;
    cp.flushed_lsn = Lsn(100);
    cp.snapshoted_tables.insert(ident().to_string(), true);
    cp.snapshoted_table_oids
        .insert(ident().to_string(), original_oid);

    let v = block_on(build_startup(&db, cp));
    let err =
        validate_startup(&v).expect_err("identity change must fail validation");

    assert!(
        err.violations.iter().any(|x| matches!(
            x,
            Violation::TableIdentityChanged { stored_oid, current_oid, .. }
                if *stored_oid == original_oid && *current_oid == new_oid
        )),
        "expected TableIdentityChanged in violations, got {:?}",
        err.violations
    );
}

#[test]
fn drop_table_from_publication_fires_table_missing_from_publication() {
    let db = SimPostgres::new();
    db.create_table(schema()).unwrap();
    db.create_publication(PUB, &[ident()]).unwrap();
    db.create_slot(SLOT, PUB).unwrap();

    let oid = db.table_oid(&ident()).unwrap();

    // Stage a checkpoint that says "we snapshotted this table."
    let mut cp = Checkpoint::fresh(Mode::Logical);
    cp.snapshot_complete = true;
    cp.flushed_lsn = Lsn(100);
    cp.snapshoted_tables.insert(ident().to_string(), true);
    cp.snapshoted_table_oids.insert(ident().to_string(), oid);

    // Operator runs `ALTER PUBLICATION DROP TABLE`.
    db.drop_table_from_publication(PUB, &ident()).unwrap();

    let v = block_on(build_startup(&db, cp));
    let err = validate_startup(&v)
        .expect_err("publication membership drift must fail validation");

    assert!(
        err.violations.iter().any(|x| matches!(
            x,
            Violation::TableMissingFromPublication { table, publication_name }
                if table == "public.orders" && publication_name == PUB
        )),
        "expected TableMissingFromPublication in violations, got {:?}",
        err.violations
    );
}

#[test]
fn untouched_table_passes_both_invariants() {
    // Locks the policy: oid unchanged + table in publication →
    // invariants 11/12 don't false-positive.
    let db = SimPostgres::new();
    db.create_table(schema()).unwrap();
    db.create_publication(PUB, &[ident()]).unwrap();
    db.create_slot(SLOT, PUB).unwrap();

    let oid = db.table_oid(&ident()).unwrap();
    let mut cp = Checkpoint::fresh(Mode::Logical);
    cp.snapshot_complete = true;
    cp.flushed_lsn = Lsn(100);
    cp.snapshoted_tables.insert(ident().to_string(), true);
    cp.snapshoted_table_oids.insert(ident().to_string(), oid);

    let v = block_on(build_startup(&db, cp));
    assert!(validate_startup(&v).is_ok());
}

#[test]
fn legacy_checkpoint_without_oid_skips_invariant_11() {
    // Backward-compat path: a checkpoint written before
    // `snapshoted_table_oids` existed has stored_oid = 0 (absent
    // entry). Invariant 11 must skip — otherwise every legacy
    // checkpoint refuses to resume on first upgrade.
    let db = SimPostgres::new();
    db.create_table(schema()).unwrap();
    db.create_publication(PUB, &[ident()]).unwrap();
    db.create_slot(SLOT, PUB).unwrap();

    let mut cp = Checkpoint::fresh(Mode::Logical);
    cp.snapshot_complete = true;
    cp.flushed_lsn = Lsn(100);
    cp.snapshoted_tables.insert(ident().to_string(), true);
    // Note: snapshoted_table_oids deliberately NOT populated.

    let v = block_on(build_startup(&db, cp));
    assert!(validate_startup(&v).is_ok());
}

#[test]
fn stale_yaml_removal_passes_startup_with_no_violation() {
    // (3) hygiene check: operator removed table from YAML but
    // checkpoint still has it in snapshoted_tables. We don't fail
    // startup — runtime emits a tracing::warn instead. Validation
    // here just confirms the pass: the stale entry alone doesn't
    // trip any invariant.
    let db = SimPostgres::new();
    db.create_table(schema()).unwrap();
    db.create_publication(PUB, &[ident()]).unwrap();
    db.create_slot(SLOT, PUB).unwrap();

    let oid = db.table_oid(&ident()).unwrap();
    let mut cp = Checkpoint::fresh(Mode::Logical);
    cp.snapshot_complete = true;
    cp.flushed_lsn = Lsn(100);
    cp.snapshoted_tables.insert(ident().to_string(), true);
    cp.snapshoted_table_oids.insert(ident().to_string(), oid);
    // Pretend YAML now has a different table that was never snapshotted.
    cp.snapshoted_tables
        .insert("public.removed_table".into(), true);
    cp.snapshoted_table_oids
        .insert("public.removed_table".into(), 99999);

    let v = block_on(build_startup(&db, cp));
    // The healthy table (in YAML) is in the validation. The stale
    // one is in cp but not in `v.tables` — i.e. not in YAML. No
    // invariant fires for that case.
    assert!(validate_startup(&v).is_ok());
}
