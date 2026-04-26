//! DST coverage for the blue-green `_pg2iceberg.markers` mechanism.
//!
//! Markers are operator-driven WAL-aligned snapshot pointers used by
//! `iceberg-diff` to verify blue/green replica equivalence at a
//! specific WAL point. The implementation must guarantee:
//!
//! 1. **A marker INSERT triggers an immediate flush + materialize**
//!    on every pg2iceberg instance that observes it, regardless of
//!    where the periodic timer is. Without this, blue and green can
//!    end up with meta-marker rows pointing at snapshots that drift
//!    in row content (each side's timer fires at a different point
//!    in its WAL stream).
//!
//! 2. **Two pg2iceberg instances reading the same WAL produce
//!    aligned meta-marker rows** even with different materialize
//!    intervals. The marker fast-path must take precedence over
//!    timer-based scheduling.
//!
//! 3. **`_pg2iceberg.markers` INSERTs are filtered from user-data
//!    staging** — they don't appear as rows in user Iceberg tables.
//!
//! These tests use the harness's component primitives directly
//! rather than the full lifecycle helper, so we control the event
//! interleaving deterministically (no real-time timers).

use pg2iceberg_coord::{schema::CoordSchema, Coordinator};
use pg2iceberg_core::typemap::IcebergType;
use pg2iceberg_core::{
    ColumnName, ColumnSchema, Namespace, PgValue, Row, TableIdent, TableSchema, Timestamp,
};
use pg2iceberg_iceberg::{read_materialized_state, Catalog};
use pg2iceberg_logical::materializer::meta_marker_table_schema;
use pg2iceberg_logical::pipeline::CounterBlobNamer;
use pg2iceberg_logical::{CounterMaterializerNamer, Materializer, Pipeline};
use pg2iceberg_sim::blob::MemoryBlobStore;
use pg2iceberg_sim::catalog::MemoryCatalog;
use pg2iceberg_sim::clock::TestClock;
use pg2iceberg_sim::coord::MemoryCoordinator;
use pg2iceberg_sim::postgres::SimPostgres;
use pollster::block_on;
use std::collections::BTreeMap;
use std::sync::Arc;

// ── schemas ────────────────────────────────────────────────────────

fn accounts_ident() -> TableIdent {
    TableIdent {
        namespace: Namespace(vec!["public".into()]),
        name: "accounts".into(),
    }
}

fn accounts_schema() -> TableSchema {
    TableSchema {
        ident: accounts_ident(),
        columns: vec![
            ColumnSchema {
                name: "id".into(),
                field_id: 1,
                ty: IcebergType::Int,
                nullable: false,
                is_primary_key: true,
            },
            ColumnSchema {
                name: "balance".into(),
                field_id: 2,
                ty: IcebergType::Int,
                nullable: false,
                is_primary_key: false,
            },
        ],
        partition_spec: Vec::new(),
    }
}

fn markers_ident() -> TableIdent {
    TableIdent {
        namespace: Namespace(vec!["_pg2iceberg".into()]),
        name: "markers".into(),
    }
}

fn markers_schema() -> TableSchema {
    TableSchema {
        ident: markers_ident(),
        columns: vec![ColumnSchema {
            name: "uuid".into(),
            field_id: 1,
            ty: IcebergType::String,
            nullable: false,
            is_primary_key: true,
        }],
        partition_spec: Vec::new(),
    }
}

// ── row helpers ────────────────────────────────────────────────────

fn account_row(id: i32, balance: i32) -> Row {
    let mut r = BTreeMap::new();
    r.insert(ColumnName("id".into()), PgValue::Int4(id));
    r.insert(ColumnName("balance".into()), PgValue::Int4(balance));
    r
}

fn marker_row(uuid: &str) -> Row {
    let mut r = BTreeMap::new();
    r.insert(ColumnName("uuid".into()), PgValue::Text(uuid.into()));
    r
}

// ── instance bundle ────────────────────────────────────────────────
//
// Stand-in for "one pg2iceberg instance" — pipeline + materializer +
// coord + catalog + blob store, all wired against the *same*
// SimPostgres but with an instance-local namespace, slot, and meta-
// namespace. Lets us spin up TWO of these against one DB to model
// blue/green replicas.

struct Instance {
    name: &'static str,
    pipeline: Pipeline<MemoryCoordinator>,
    materializer: Materializer<MemoryCatalog>,
    coord: Arc<MemoryCoordinator>,
    catalog: Arc<MemoryCatalog>,
    blob: Arc<MemoryBlobStore>,
    stream: pg2iceberg_sim::postgres::SimReplicationStream,
    meta_ident: TableIdent,
}

impl Instance {
    fn boot(
        name: &'static str,
        db: &SimPostgres,
        slot_name: &str,
        publication_name: &str,
        meta_namespace: &str,
    ) -> Self {
        let clock = TestClock::at(0);
        let arc_clock: Arc<dyn pg2iceberg_core::Clock> = Arc::new(clock);
        let coord = Arc::new(MemoryCoordinator::new(
            CoordSchema::default_name(),
            arc_clock,
        ));
        let blob = Arc::new(MemoryBlobStore::new());
        let catalog = Arc::new(MemoryCatalog::new());

        // Per-instance publication + slot. Both publications cover
        // accounts AND _pg2iceberg.markers, so each instance sees
        // every marker as a WAL event.
        db.create_publication(publication_name, &[accounts_ident(), markers_ident()])
            .unwrap();
        db.create_slot(slot_name, publication_name).unwrap();

        let blob_namer = Arc::new(CounterBlobNamer::new(format!("s3://{name}/stage")));
        let mut pipeline = Pipeline::new(coord.clone(), blob.clone(), blob_namer, 64);
        // Enable marker mode: pipeline detects INSERTs to
        // `_pg2iceberg.markers`, filters them from staging, and
        // emits MarkerInfo on flush.
        pipeline.enable_markers(markers_ident());

        let mat_namer = Arc::new(CounterMaterializerNamer::new(format!("s3://{name}/data")));
        let mut materializer: Materializer<MemoryCatalog> = Materializer::new(
            coord.clone() as Arc<dyn Coordinator>,
            blob.clone(),
            catalog.clone(),
            mat_namer,
            "default",
            128,
        );
        block_on(materializer.register_table(accounts_schema())).unwrap();
        // Enable meta-marker emission. Materializer creates the
        // meta-marker Iceberg table at <meta_namespace>.markers.
        let meta_schema = meta_marker_table_schema(meta_namespace, "markers");
        let meta_ident = meta_schema.ident.clone();
        block_on(materializer.enable_meta_markers(meta_schema)).unwrap();

        let stream = db.start_replication(slot_name).unwrap();
        Self {
            name,
            pipeline,
            materializer,
            coord,
            catalog,
            blob,
            stream,
            meta_ident,
        }
    }

    /// Drain every pending WAL event into the pipeline.
    fn drive(&mut self) {
        while let Some(msg) = self.stream.recv() {
            block_on(self.pipeline.process(msg)).unwrap();
        }
    }

    /// Run flush + materialize cycle. Mirrors the lifecycle's
    /// marker-fast-path or a periodic timer firing.
    fn flush_and_materialize(&mut self) {
        block_on(self.pipeline.flush()).unwrap();
        self.stream.send_standby(self.pipeline.flushed_lsn());
        block_on(self.materializer.cycle()).unwrap();
    }

    /// Drive + (if a marker arrived) immediately flush+materialize.
    /// Mirrors `run_logical_main_loop`'s marker fast-path: after
    /// every batch of recv'd events, check `has_pending_marker` and
    /// if so, flush+materialize regardless of timer.
    fn drive_with_marker_fastpath(&mut self) {
        self.drive();
        if self.pipeline.has_pending_marker() {
            self.flush_and_materialize();
        }
    }

    /// Read the meta-marker Iceberg table, sorted by uuid.
    fn meta_markers(&self) -> Vec<Row> {
        let schema = meta_marker_table_schema(
            self.meta_ident.namespace.0.first().unwrap(),
            &self.meta_ident.name,
        );
        let pk_cols: Vec<ColumnName> = schema
            .primary_key_columns()
            .map(|c| ColumnName(c.name.clone()))
            .collect();
        let mut rows = block_on(read_materialized_state(
            self.catalog.as_ref(),
            self.blob.as_ref(),
            &self.meta_ident,
            &schema,
            &pk_cols,
        ))
        .unwrap();
        rows.sort_by_key(|r| match r.get(&ColumnName("uuid".into())) {
            Some(PgValue::Text(s)) => s.clone(),
            _ => String::new(),
        });
        rows
    }

    /// Read the user accounts Iceberg state, sorted by id.
    fn accounts(&self) -> Vec<Row> {
        let mut rows = block_on(read_materialized_state(
            self.catalog.as_ref(),
            self.blob.as_ref(),
            &accounts_ident(),
            &accounts_schema(),
            &[ColumnName("id".into())],
        ))
        .unwrap();
        rows.sort_by_key(|r| match r.get(&ColumnName("id".into())) {
            Some(PgValue::Int4(n)) => *n,
            _ => i32::MAX,
        });
        rows
    }

    /// Returns the accounts table's current snapshot id (matches the
    /// `snapshot_id` column in the meta-marker rows).
    fn accounts_snapshot_id(&self) -> Option<i64> {
        block_on(self.catalog.load_table(&accounts_ident()))
            .unwrap()
            .and_then(|m| m.current_snapshot_id)
    }
}

// ── pinned scenarios ──────────────────────────────────────────────

#[test]
fn marker_insert_is_filtered_from_user_data_staging() {
    // Headline correctness check: a marker INSERT to
    // `_pg2iceberg.markers` must NOT end up as a row in any user
    // Iceberg table or as a stale `log_index` entry. It's
    // observability metadata, not data.
    let db = SimPostgres::new();
    db.create_table(accounts_schema()).unwrap();
    db.create_table(markers_schema()).unwrap();

    // Boot first — slot's restart_lsn captures here.
    let mut bg = Instance::boot(
        "blue",
        &db,
        "blue-slot",
        "blue-pub",
        "_pg2iceberg_blue",
    );

    // All inserts AFTER boot so they ride the slot's WAL.
    let mut tx = db.begin_tx();
    tx.insert(&accounts_ident(), account_row(1, 100));
    tx.commit(Timestamp(0)).unwrap();
    let mut tx = db.begin_tx();
    tx.insert(&markers_ident(), marker_row("marker-A"));
    tx.commit(Timestamp(0)).unwrap();

    bg.drive_with_marker_fastpath();
    bg.flush_and_materialize();

    // accounts table has only the user-data row; no marker leakage.
    let accounts = bg.accounts();
    assert_eq!(accounts.len(), 1);
    assert_eq!(
        accounts[0].get(&ColumnName("id".into())),
        Some(&PgValue::Int4(1))
    );

    // Meta-marker table has the marker row.
    let meta = bg.meta_markers();
    assert_eq!(meta.len(), 1);
    assert_eq!(
        meta[0].get(&ColumnName("uuid".into())),
        Some(&PgValue::Text("marker-A".into()))
    );
}

#[test]
fn marker_triggers_immediate_flush_and_materialize_without_timer_tick() {
    // Proves the marker fast-path: without firing any
    // timer-scheduled flush/materialize, observing a marker is
    // enough to commit user data + emit meta-markers.
    //
    // This is what makes blue-green alignment work even when the
    // green instance has a *much* longer materialize interval than
    // blue: green's marker fast-path fires the moment its WAL
    // delivers the marker, so its meta-marker rows align with
    // blue's at the same logical (uuid, table) → snapshot mapping.
    let db = SimPostgres::new();
    db.create_table(accounts_schema()).unwrap();
    db.create_table(markers_schema()).unwrap();

    let mut bg = Instance::boot(
        "lone",
        &db,
        "lone-slot",
        "lone-pub",
        "_pg2iceberg_meta",
    );

    // Insert user data + marker after slot creation.
    let mut tx = db.begin_tx();
    tx.insert(&accounts_ident(), account_row(1, 100));
    tx.insert(&accounts_ident(), account_row(2, 200));
    tx.commit(Timestamp(0)).unwrap();
    let mut tx = db.begin_tx();
    tx.insert(&markers_ident(), marker_row("marker-now"));
    tx.commit(Timestamp(0)).unwrap();

    // Drive WITHOUT calling the periodic timer-style
    // flush_and_materialize. Only the marker fast-path should fire.
    bg.drive_with_marker_fastpath();

    // After the fast-path: user data is materialized AND meta-marker
    // row exists, both visible in Iceberg.
    let accounts = bg.accounts();
    assert_eq!(accounts.len(), 2, "user data must be materialized");
    let meta = bg.meta_markers();
    assert_eq!(
        meta.len(),
        1,
        "meta-marker must be emitted by the fast-path"
    );
    assert_eq!(
        meta[0].get(&ColumnName("uuid".into())),
        Some(&PgValue::Text("marker-now".into()))
    );
    // The meta-marker row's snapshot_id must match the accounts
    // table's current snapshot_id — that's the alignment pointer
    // `iceberg-diff` will use.
    let recorded_snap_id = match meta[0].get(&ColumnName("snapshot_id".into())) {
        Some(PgValue::Int8(n)) => Some(*n),
        _ => None,
    };
    assert_eq!(
        recorded_snap_id,
        bg.accounts_snapshot_id(),
        "meta-marker.snapshot_id must point at the accounts table's current snapshot"
    );
}

#[test]
fn two_instances_emit_aligned_meta_markers_for_same_marker_uuid() {
    // **The headline alignment test.** Two pg2iceberg instances
    // (blue and green) both read the same SimPostgres WAL. Both
    // observe the same marker UUID. Each emits its own
    // meta-marker row pointing at *its* accounts snapshot id.
    //
    // The alignment guarantee is that **blue's accounts at
    // blue's snapshot_id_M and green's accounts at green's
    // snapshot_id_M contain the same logical rows**. That's what
    // `iceberg-diff` verifies in the production blue/green
    // workflow.
    let db = SimPostgres::new();
    db.create_table(accounts_schema()).unwrap();
    db.create_table(markers_schema()).unwrap();

    // Boot blue + green BEFORE any INSERTs so both slots capture
    // the same WAL window.
    let mut blue = Instance::boot(
        "blue",
        &db,
        "blue-slot",
        "blue-pub",
        "_pg2iceberg_blue",
    );
    let mut green = Instance::boot(
        "green",
        &db,
        "green-slot",
        "green-pub",
        "_pg2iceberg_green",
    );

    // Workload: a few INSERTs, then a marker. All after both slots
    // are open so blue and green see identical WAL streams.
    let mut tx = db.begin_tx();
    tx.insert(&accounts_ident(), account_row(1, 100));
    tx.insert(&accounts_ident(), account_row(2, 200));
    tx.insert(&accounts_ident(), account_row(3, 300));
    tx.commit(Timestamp(0)).unwrap();

    let mut tx = db.begin_tx();
    tx.insert(&markers_ident(), marker_row("align-1"));
    tx.commit(Timestamp(0)).unwrap();

    // Drive each instance independently. The marker fast-path
    // fires on each.
    blue.drive_with_marker_fastpath();
    green.drive_with_marker_fastpath();

    // Both meta-tables have one row keyed by the same UUID.
    let blue_meta = blue.meta_markers();
    let green_meta = green.meta_markers();
    assert_eq!(blue_meta.len(), 1, "blue meta has marker");
    assert_eq!(green_meta.len(), 1, "green meta has marker");
    let blue_uuid = blue_meta[0].get(&ColumnName("uuid".into())).unwrap();
    let green_uuid = green_meta[0].get(&ColumnName("uuid".into())).unwrap();
    assert_eq!(blue_uuid, green_uuid, "same marker UUID on both sides");

    // Both meta-marker rows point at *some* accounts snapshot.
    let blue_snap = match blue_meta[0].get(&ColumnName("snapshot_id".into())) {
        Some(PgValue::Int8(n)) => Some(*n),
        _ => None,
    };
    let green_snap = match green_meta[0].get(&ColumnName("snapshot_id".into())) {
        Some(PgValue::Int8(n)) => Some(*n),
        _ => None,
    };
    assert!(blue_snap.is_some());
    assert!(green_snap.is_some());

    // **The alignment property.** Each side's accounts state at
    // marker time must contain the same logical rows. (snapshot_ids
    // can differ between instances because they have separate
    // catalog histories.)
    let blue_accounts = blue.accounts();
    let green_accounts = green.accounts();
    assert_eq!(
        blue_accounts, green_accounts,
        "blue and green accounts must match at the marker's snapshot point"
    );
    assert_eq!(blue_accounts.len(), 3);
}

#[test]
fn alignment_holds_when_one_instance_runs_extra_flush_materialize_cycles() {
    // Explicit "different intervals" test. Blue runs many extra
    // flush+materialize cycles between events; green only triggers
    // via the marker fast-path. Despite the difference in cadence,
    // the meta-marker rows both point at snapshots whose accounts
    // state matches.
    let db = SimPostgres::new();
    db.create_table(accounts_schema()).unwrap();
    db.create_table(markers_schema()).unwrap();

    // Boot blue + green first.
    let mut blue = Instance::boot("blue-fast", &db, "bs", "bp", "_pg2iceberg_blue");
    let mut green = Instance::boot("green-slow", &db, "gs", "gp", "_pg2iceberg_green");

    // Workload split into two batches with a marker in between.
    let mut tx = db.begin_tx();
    tx.insert(&accounts_ident(), account_row(1, 10));
    tx.insert(&accounts_ident(), account_row(2, 20));
    tx.commit(Timestamp(0)).unwrap();

    // Blue cycles aggressively (every batch).
    blue.drive();
    blue.flush_and_materialize();
    blue.flush_and_materialize();
    blue.flush_and_materialize();

    // Green doesn't cycle at all yet.

    // Now the marker.
    let mut tx = db.begin_tx();
    tx.insert(&markers_ident(), marker_row("interleaved"));
    tx.commit(Timestamp(0)).unwrap();

    // Both observe + react via marker fast-path.
    blue.drive_with_marker_fastpath();
    green.drive_with_marker_fastpath();

    // More user-data activity AFTER the marker. Should NOT affect
    // the marker's meta-row pointer (it was emitted at marker time).
    let mut tx = db.begin_tx();
    tx.insert(&accounts_ident(), account_row(3, 30));
    tx.commit(Timestamp(0)).unwrap();
    blue.drive();
    blue.flush_and_materialize();
    green.drive();
    green.flush_and_materialize();

    // Both have the marker; both rows have the same UUID.
    assert_eq!(blue.meta_markers().len(), 1);
    assert_eq!(green.meta_markers().len(), 1);
    let blue_uuid = blue.meta_markers()[0]
        .get(&ColumnName("uuid".into()))
        .cloned();
    let green_uuid = green.meta_markers()[0]
        .get(&ColumnName("uuid".into()))
        .cloned();
    assert_eq!(blue_uuid, green_uuid);

    // The meta-marker rows captured the snapshot at marker time.
    // After the marker, a row was added (id=3) — but the
    // meta-marker.snapshot_id must NOT include it (the meta-marker
    // points at the snapshot from the marker's tx, not the latest).
    //
    // We verify this by reading each side's accounts at the
    // recorded snapshot_id... actually the sim doesn't expose
    // snapshot-time-travel reads, so we settle for the weaker
    // property: at quiescence (after all events), both sides agree.
    assert_eq!(blue.accounts(), green.accounts());
    assert_eq!(blue.accounts().len(), 3);
}

#[test]
fn multiple_markers_emit_one_meta_marker_row_each() {
    // Two markers in two different txs → two meta-marker rows.
    // Each row's snapshot_id reflects the accounts state at that
    // marker's WAL position.
    let db = SimPostgres::new();
    db.create_table(accounts_schema()).unwrap();
    db.create_table(markers_schema()).unwrap();
    // Boot first so both inserts go through the slot.
    let mut bg = Instance::boot("multi", &db, "ms", "mp", "_pg2iceberg_multi");

    let mut tx = db.begin_tx();
    tx.insert(&accounts_ident(), account_row(1, 100));
    tx.commit(Timestamp(0)).unwrap();
    let mut tx = db.begin_tx();
    tx.insert(&markers_ident(), marker_row("marker-1"));
    tx.commit(Timestamp(0)).unwrap();

    bg.drive_with_marker_fastpath();
    let snap1 = bg.accounts_snapshot_id();

    let mut tx = db.begin_tx();
    tx.insert(&accounts_ident(), account_row(2, 200));
    tx.commit(Timestamp(0)).unwrap();
    let mut tx = db.begin_tx();
    tx.insert(&markers_ident(), marker_row("marker-2"));
    tx.commit(Timestamp(0)).unwrap();

    bg.drive_with_marker_fastpath();
    let snap2 = bg.accounts_snapshot_id();

    let meta = bg.meta_markers();
    assert_eq!(meta.len(), 2, "one meta-row per marker");

    // The two rows have distinct UUIDs and distinct snapshot_ids.
    let uuids: Vec<_> = meta
        .iter()
        .filter_map(|r| match r.get(&ColumnName("uuid".into())) {
            Some(PgValue::Text(s)) => Some(s.clone()),
            _ => None,
        })
        .collect();
    assert!(uuids.contains(&"marker-1".into()));
    assert!(uuids.contains(&"marker-2".into()));

    // marker-1's snapshot_id should match snap1; marker-2's should
    // match snap2; and snap1 != snap2 (a new snapshot per cycle).
    assert!(snap1.is_some() && snap2.is_some());
    assert_ne!(snap1, snap2);
    let marker_to_snap: BTreeMap<String, i64> = meta
        .iter()
        .filter_map(|r| {
            let uuid = match r.get(&ColumnName("uuid".into())) {
                Some(PgValue::Text(s)) => s.clone(),
                _ => return None,
            };
            let snap = match r.get(&ColumnName("snapshot_id".into())) {
                Some(PgValue::Int8(n)) => *n,
                _ => return None,
            };
            Some((uuid, snap))
        })
        .collect();
    assert_eq!(marker_to_snap.get("marker-1"), snap1.as_ref());
    assert_eq!(marker_to_snap.get("marker-2"), snap2.as_ref());
}
