//! Phase A integration: `PostgresCoordinator` against a real Postgres
//! container. Covers the durability-critical surfaces — migrate,
//! claim_offsets (empty / claims / markers / mixed), read_log,
//! truncate_log, cursor + consumer CRUD, checkpoint round-trip,
//! and the marker eligibility query that drives blue-green meta-marker
//! emission.
//!
//! Gated behind `--features integration`. Run with:
//!
//! ```sh
//! DOCKER_HOST="unix:///Users/$USER/.colima/default/docker.sock" \
//! TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE="/var/run/docker.sock" \
//! TESTCONTAINERS_RYUK_DISABLED=true \
//!   cargo test --features integration --test integration_coord -- \
//!   --test-threads=1
//! ```
//!
//! `--test-threads=1` because tests share a single PG container (one
//! per binary, started lazily) and isolate via per-test schema names.
//! Parallelism within the binary would race on the shared container's
//! resources without buying us much.

#![cfg(feature = "integration")]

use std::sync::Arc;
use std::time::Duration;

use pg2iceberg_coord::{
    prod::{connect_with, PostgresCoordinator, TlsMode},
    schema::CoordSchema,
    CommitBatch, Coordinator, MarkerInfo, OffsetClaim, StagedLogSize,
};
use pg2iceberg_core::{Lsn, Namespace, TableIdent, WorkerId};
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::{ContainerAsync, ImageExt};
use tokio::sync::OnceCell;

/// One PG container per test binary, lazily started. Tests isolate
/// via unique `_pg2iceberg_<uuid>` coord schemas.
static PG: OnceCell<SharedPg> = OnceCell::const_new();

struct SharedPg {
    _container: ContainerAsync<Postgres>,
    dsn: String,
}

async fn shared_pg() -> &'static SharedPg {
    PG.get_or_init(|| async {
        let container = Postgres::default()
            // Pin a current PG so the coord schema migrations and
            // SQL idioms run against the same version operators
            // most likely deploy to. testcontainers-modules' default
            // is `11-alpine`, which is too old for some of our
            // queries (e.g. `pg_replication_slots.wal_status`).
            .with_tag("16-alpine")
            .start()
            .await
            .expect("start postgres container");
        let host = container.get_host().await.expect("postgres host");
        let port = container
            .get_host_port_ipv4(5432)
            .await
            .expect("postgres port");
        // testcontainers-modules' Postgres defaults: db=postgres, user=postgres, password=postgres
        let dsn =
            format!("host={host} port={port} user=postgres password=postgres dbname=postgres");
        SharedPg {
            _container: container,
            dsn,
        }
    })
    .await
}

/// Connect + migrate a fresh per-test coord schema.
async fn fresh_coord() -> Arc<PostgresCoordinator> {
    let pg = shared_pg().await;
    let conn = connect_with(&pg.dsn, TlsMode::Disable)
        .await
        .expect("coord connect");
    // Per-test schema isolates state. CoordSchema sanitization permits
    // letters/digits/underscores; UUID has dashes so use simple-form.
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let raw = format!("_pg2iceberg_test_{suffix}");
    let schema = CoordSchema::sanitize(&raw);
    let coord = Arc::new(PostgresCoordinator::new(conn, schema));
    coord.migrate().await.expect("migrate");
    coord
}

fn ident(ns: &str, name: &str) -> TableIdent {
    TableIdent {
        namespace: Namespace(vec![ns.into()]),
        name: name.into(),
    }
}

fn claim(table: &TableIdent, count: u64, path: &str) -> OffsetClaim {
    OffsetClaim {
        table: table.clone(),
        record_count: count,
        byte_size: count * 100,
        s3_path: path.into(),
    }
}

/// Postgres dropping the coordinator's connection — restarting, failing
/// over, or terminating it — costs at most the call in flight: the next
/// one reconnects, to the same state.
#[tokio::test]
async fn coordinator_reconnects_after_its_connection_is_dropped() {
    let pg = shared_pg().await;
    let app = format!("coord_{}", uuid::Uuid::new_v4().simple());
    let conn = connect_with(
        &format!("{} application_name={app}", pg.dsn),
        TlsMode::Disable,
    )
    .await
    .expect("coord connect");
    let schema = CoordSchema::sanitize(&format!(
        "_pg2iceberg_test_{}",
        uuid::Uuid::new_v4().simple()
    ));
    let coord = PostgresCoordinator::new(conn, schema);
    coord.migrate().await.expect("migrate");
    coord
        .set_flushed_lsn(Lsn(42))
        .await
        .expect("set_flushed_lsn");

    let admin = connect_with(&pg.dsn, TlsMode::Disable)
        .await
        .expect("admin connect");
    let terminated: i64 = admin
        .client
        .query_one(
            "SELECT count(*) FILTER (WHERE pg_terminate_backend(pid, 10000)) \
             FROM pg_stat_activity WHERE application_name = $1",
            &[&app],
        )
        .await
        .expect("terminate")
        .get(0);
    assert_eq!(terminated, 1);

    let lsn = match coord.flushed_lsn().await {
        Ok(lsn) => lsn,
        Err(_) => coord.flushed_lsn().await.expect("reconnected"),
    };
    assert_eq!(lsn, Lsn(42));
}

#[tokio::test]
async fn migrate_is_idempotent_against_real_pg() {
    let coord = fresh_coord().await;
    // Second invocation must not error — every statement uses
    // CREATE TABLE IF NOT EXISTS / ADD COLUMN IF NOT EXISTS /
    // DROP TABLE IF EXISTS.
    coord.migrate().await.expect("migrate twice");
    coord.migrate().await.expect("migrate thrice");
}

/// Deployments migrated by older releases have a `lock` table that
/// nothing uses any more; migrating drops it, rows and all.
#[tokio::test]
async fn migrate_drops_the_retired_lock_table() {
    let pg = shared_pg().await;
    let raw = format!("_pg2iceberg_test_{}", uuid::Uuid::new_v4().simple());
    let admin = connect_with(&pg.dsn, TlsMode::Disable)
        .await
        .expect("admin connect");
    admin
        .client
        .batch_execute(&format!(
            "CREATE SCHEMA {raw};
             CREATE TABLE {raw}.lock (
                 table_name TEXT PRIMARY KEY,
                 worker_id  TEXT NOT NULL,
                 expires_at TIMESTAMPTZ NOT NULL
             );
             INSERT INTO {raw}.lock VALUES ('public.orders', 'w1', now());"
        ))
        .await
        .expect("create the old lock table");
    let lock_exists = || async {
        admin
            .client
            .query_one(
                "SELECT to_regclass($1) IS NOT NULL",
                &[&format!("{raw}.lock")],
            )
            .await
            .expect("to_regclass")
            .get::<_, bool>(0)
    };
    assert!(lock_exists().await);

    let conn = connect_with(&pg.dsn, TlsMode::Disable)
        .await
        .expect("coord connect");
    let coord = PostgresCoordinator::new(conn, CoordSchema::sanitize(&raw));
    coord.migrate().await.expect("migrate");
    assert!(!lock_exists().await);
    coord.migrate().await.expect("migrate again");
    assert!(!lock_exists().await);
}

/// `replicated_lsn` follows the highest `CommitBatch::replicated_lsn`
/// claimed — with claims or without — and ignores batches that carry
/// none (a backfill's).
#[tokio::test]
async fn replicated_lsn_tracks_replication_claims_only() {
    let coord = fresh_coord().await;
    let t = ident("public", "orders");
    assert_eq!(coord.replicated_lsn().await.unwrap(), Lsn::ZERO);

    let batch = |claims: Vec<OffsetClaim>, lsn: u64, replicated: Option<u64>| CommitBatch {
        replicated_lsn: replicated.map(Lsn),
        ..CommitBatch::without_markers(claims, Lsn(lsn))
    };
    coord
        .claim_offsets(&batch(
            vec![claim(&t, 1, "s3://r/0.parquet")],
            100,
            Some(100),
        ))
        .await
        .unwrap();
    assert_eq!(coord.replicated_lsn().await.unwrap(), Lsn(100));
    // A backfill's claim, even at a later LSN, says nothing about replication.
    coord
        .claim_offsets(&batch(vec![claim(&t, 1, "s3://r/1.parquet")], 500, None))
        .await
        .unwrap();
    assert_eq!(coord.replicated_lsn().await.unwrap(), Lsn(100));
    // A claim-less batch (WAL that carried nothing for us) still counts.
    coord
        .claim_offsets(&batch(vec![], 150, Some(150)))
        .await
        .unwrap();
    assert_eq!(coord.replicated_lsn().await.unwrap(), Lsn(150));
    // Never moves back.
    coord
        .claim_offsets(&batch(
            vec![claim(&t, 1, "s3://r/2.parquet")],
            120,
            Some(120),
        ))
        .await
        .unwrap();
    assert_eq!(coord.replicated_lsn().await.unwrap(), Lsn(150));
}

#[tokio::test]
async fn empty_claim_offsets_returns_receipt_without_grants() {
    let coord = fresh_coord().await;
    let receipt = coord
        .claim_offsets(&CommitBatch::without_markers(vec![], Lsn(42)))
        .await
        .expect("empty claim_offsets");
    assert_eq!(receipt.flushable_lsn, Lsn(42));
    assert!(receipt.grants.is_empty());
}

#[tokio::test]
async fn claim_offsets_assigns_monotonic_per_table_offsets() {
    let coord = fresh_coord().await;
    let t = ident("public", "orders");

    // First batch: 3 rows. Should land at [0, 3).
    let r1 = coord
        .claim_offsets(&CommitBatch::without_markers(
            vec![claim(&t, 3, "s3://b/0001.parquet")],
            Lsn(100),
        ))
        .await
        .expect("first claim");
    assert_eq!(r1.grants.len(), 1);
    assert_eq!(r1.grants[0].start_offset, 0);
    assert_eq!(r1.grants[0].end_offset, 3);

    // Second batch: 5 rows. Should land at [3, 8) — sequential after r1.
    let r2 = coord
        .claim_offsets(&CommitBatch::without_markers(
            vec![claim(&t, 5, "s3://b/0002.parquet")],
            Lsn(200),
        ))
        .await
        .expect("second claim");
    assert_eq!(r2.grants[0].start_offset, 3);
    assert_eq!(r2.grants[0].end_offset, 8);
    assert_eq!(r2.flushable_lsn, Lsn(200));
}

#[tokio::test]
async fn read_log_returns_entries_with_flushable_lsn_preserved() {
    let coord = fresh_coord().await;
    let t = ident("public", "orders");

    coord
        .claim_offsets(&CommitBatch::without_markers(
            vec![claim(&t, 2, "s3://b/a.parquet")],
            Lsn(0x1000),
        ))
        .await
        .unwrap();
    coord
        .claim_offsets(&CommitBatch::without_markers(
            vec![claim(&t, 4, "s3://b/b.parquet")],
            Lsn(0x2000),
        ))
        .await
        .unwrap();

    let entries = coord.read_log(&t, 0, 16).await.expect("read_log");
    assert_eq!(entries.len(), 2);
    assert_eq!(entries[0].start_offset, 0);
    assert_eq!(entries[0].end_offset, 2);
    assert_eq!(entries[0].flushable_lsn, Lsn(0x1000));
    assert_eq!(entries[1].start_offset, 2);
    assert_eq!(entries[1].end_offset, 6);
    assert_eq!(entries[1].flushable_lsn, Lsn(0x2000));

    // after_offset filter — skip first entry.
    let entries = coord.read_log(&t, 2, 16).await.expect("read_log w/ filter");
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].start_offset, 2);
}

#[tokio::test]
async fn truncate_log_returns_paths_and_drops_rows() {
    let coord = fresh_coord().await;
    let t = ident("public", "orders");
    coord
        .claim_offsets(&CommitBatch::without_markers(
            vec![claim(&t, 2, "s3://b/a.parquet")],
            Lsn(1),
        ))
        .await
        .unwrap();
    coord
        .claim_offsets(&CommitBatch::without_markers(
            vec![claim(&t, 4, "s3://b/b.parquet")],
            Lsn(2),
        ))
        .await
        .unwrap();

    coord
        .claim_offsets(&CommitBatch::without_markers(
            vec![claim(&t, 1, "s3://b/c.parquet")],
            Lsn(3),
        ))
        .await
        .unwrap();
    let size = coord.staged_log_size(&t).await.expect("staged_log_size");
    assert_eq!(
        size,
        StagedLogSize {
            files: 3,
            bytes: 700
        }
    );

    // At most `limit`, the first by offset: [0, 2), not [2, 6).
    let dropped = coord.truncate_log(&t, 6, 1).await.expect("truncate_log");
    assert_eq!(dropped, vec!["s3://b/a.parquet".to_string()]);
    let dropped = coord.truncate_log(&t, 6, 16).await.expect("truncate_log");
    assert_eq!(dropped, vec!["s3://b/b.parquet".to_string()]);

    let remaining = coord.read_log(&t, 0, 16).await.unwrap();
    assert_eq!(remaining.len(), 1);
    assert_eq!(remaining[0].s3_path, "s3://b/c.parquet");
    let size = coord.staged_log_size(&t).await.expect("staged_log_size");
    assert_eq!(
        size,
        StagedLogSize {
            files: 1,
            bytes: 100
        }
    );
    let other = ident("public", "other");
    let none = coord
        .staged_log_size(&other)
        .await
        .expect("staged_log_size");
    assert_eq!(none, StagedLogSize::default());
}

#[tokio::test]
async fn table_epochs_count_writes_per_table() {
    let coord = fresh_coord().await;
    let orders = ident("public", "orders");
    let items = ident("public", "items");
    let never = ident("public", "never");
    let all = [orders.clone(), items.clone(), never.clone()];

    assert!(coord.table_epochs(&all).await.unwrap().is_empty());
    assert_eq!(coord.bump_table_epoch(&orders).await.unwrap(), 1);
    assert_eq!(coord.bump_table_epoch(&orders).await.unwrap(), 2);
    assert_eq!(coord.bump_table_epoch(&items).await.unwrap(), 1);

    let epochs = coord.table_epochs(&all).await.unwrap();
    assert_eq!(epochs.get(&orders), Some(&2));
    assert_eq!(epochs.get(&items), Some(&1));
    assert_eq!(epochs.get(&never), None);
}

/// A write in flight keeps the earliest start it's recorded with — the key
/// ranges of a snapshot each record theirs — until it ends; tables and
/// writers apart.
#[tokio::test]
async fn writes_in_flight_keep_their_earliest_start_until_they_end() {
    let coord = fresh_coord().await;
    let orders = ident("public", "orders");
    let items = ident("public", "items");
    let writes = |t: &TableIdent| {
        let coord = &coord;
        let t = t.clone();
        async move {
            coord
                .writes_in_flight(&t)
                .await
                .unwrap()
                .into_iter()
                .map(|w| (w.writer, w.since_ms))
                .collect::<Vec<_>>()
        }
    };
    assert!(writes(&orders).await.is_empty());
    coord.begin_write(&orders, "snapshot", 200).await.unwrap();
    coord.begin_write(&orders, "snapshot", 100).await.unwrap();
    coord.begin_write(&orders, "snapshot", 300).await.unwrap();
    coord.begin_write(&orders, "compaction", 400).await.unwrap();
    coord.begin_write(&items, "snapshot", 500).await.unwrap();
    assert_eq!(
        writes(&orders).await,
        [("compaction".into(), 400), ("snapshot".into(), 100)]
    );
    coord.end_write(&orders, "snapshot").await.unwrap();
    coord.end_write(&orders, "snapshot").await.unwrap();
    assert_eq!(writes(&orders).await, [("compaction".into(), 400)]);
    assert_eq!(writes(&items).await, [("snapshot".into(), 500)]);
    // Ended, a writer starts afresh.
    coord.begin_write(&orders, "snapshot", 600).await.unwrap();
    assert_eq!(
        writes(&orders).await,
        [("compaction".into(), 400), ("snapshot".into(), 600)]
    );
}

#[tokio::test]
async fn cursor_round_trip() {
    let coord = fresh_coord().await;
    let t = ident("public", "orders");
    let group = "default";

    assert_eq!(coord.get_cursor(group, &t).await.unwrap(), None);
    coord.ensure_cursor(group, &t).await.unwrap();
    assert_eq!(coord.get_cursor(group, &t).await.unwrap(), Some(-1));

    coord.set_cursor(group, &t, 42).await.unwrap();
    assert_eq!(coord.get_cursor(group, &t).await.unwrap(), Some(42));

    // set_cursor on missing row errors NotFound.
    let other = ident("public", "missing");
    let err = coord.set_cursor(group, &other, 0).await.unwrap_err();
    assert!(matches!(err, pg2iceberg_coord::CoordError::NotFound(_)));
}

#[tokio::test]
async fn table_cursors_lists_every_groups_cursor_of_the_table() {
    let coord = fresh_coord().await;
    let t = ident("public", "orders");
    coord.ensure_cursor("b", &t).await.unwrap();
    coord.ensure_cursor("a", &t).await.unwrap();
    coord
        .ensure_cursor("a", &ident("public", "other"))
        .await
        .unwrap();
    coord.set_cursor("b", &t, 7).await.unwrap();
    assert_eq!(
        coord.table_cursors(&t).await.unwrap(),
        vec![("a".to_string(), -1), ("b".to_string(), 7)]
    );
    let none = ident("public", "none");
    assert!(coord.table_cursors(&none).await.unwrap().is_empty());
}

#[tokio::test]
async fn consumer_lifecycle_and_expiry_sweep() {
    let coord = fresh_coord().await;
    let group = "g";
    let w1 = WorkerId("w1".into());
    let w2 = WorkerId("w2".into());

    coord
        .register_consumer(group, &w1, Duration::from_secs(60))
        .await
        .unwrap();
    coord
        .register_consumer(group, &w2, Duration::from_secs(60))
        .await
        .unwrap();

    let active = coord.active_consumers(group).await.unwrap();
    assert_eq!(active, vec![w1.clone(), w2.clone()]);

    // Register w2 with a 0-second TTL — expires immediately. The
    // active_consumers sweep should drop it.
    coord
        .register_consumer(group, &w2, Duration::from_secs(0))
        .await
        .unwrap();
    // Give PG a beat for `now() > expires_at` to be unambiguous.
    tokio::time::sleep(Duration::from_millis(50)).await;
    let active = coord.active_consumers(group).await.unwrap();
    assert_eq!(active, vec![w1.clone()]);

    coord.unregister_consumer(group, &w1).await.unwrap();
    let active = coord.active_consumers(group).await.unwrap();
    assert!(active.is_empty());
}

#[tokio::test]
async fn pipeline_meta_round_trip() {
    let coord = fresh_coord().await;
    // Fresh: no row → returns 0.
    assert_eq!(coord.pipeline_system_identifier().await.unwrap(), 0);

    // Stamp idempotently.
    coord.set_pipeline_system_identifier(7777).await.unwrap();
    assert_eq!(coord.pipeline_system_identifier().await.unwrap(), 7777);
    coord.set_pipeline_system_identifier(7777).await.unwrap();
    assert_eq!(coord.pipeline_system_identifier().await.unwrap(), 7777);

    // Mismatched stamp returns SystemIdMismatch and doesn't overwrite.
    let err = coord
        .set_pipeline_system_identifier(8888)
        .await
        .unwrap_err();
    assert!(matches!(
        err,
        pg2iceberg_coord::CoordError::SystemIdMismatch {
            stored: 7777,
            connected: 8888
        }
    ));
    assert_eq!(coord.pipeline_system_identifier().await.unwrap(), 7777);
}

#[tokio::test]
async fn table_state_round_trip() {
    let coord = fresh_coord().await;
    let t = ident("public", "orders");
    assert!(coord.table_state(&t).await.unwrap().is_none());

    coord
        .mark_table_snapshot_complete(&t, 12345, Lsn(0xDEAD_BEEF))
        .await
        .unwrap();
    let state = coord.table_state(&t).await.unwrap().unwrap();
    assert!(state.snapshot_complete);
    assert_eq!(state.pg_oid, 12345);
    assert_eq!(state.snapshot_lsn, Lsn(0xDEAD_BEEF));
    assert!(state.completed_at_micros.is_some());
}

#[tokio::test]
async fn snapshot_progress_round_trip() {
    let coord = fresh_coord().await;
    let t = ident("public", "orders");
    assert!(coord.snapshot_progress(&t).await.unwrap().is_none());

    coord.set_snapshot_progress(&t, "[123]").await.unwrap();
    assert_eq!(
        coord.snapshot_progress(&t).await.unwrap().as_deref(),
        Some("[123]")
    );

    coord.set_snapshot_progress(&t, "[456]").await.unwrap();
    assert_eq!(
        coord.snapshot_progress(&t).await.unwrap().as_deref(),
        Some("[456]")
    );

    coord.clear_snapshot_progress(&t).await.unwrap();
    assert!(coord.snapshot_progress(&t).await.unwrap().is_none());
    // Idempotent.
    coord.clear_snapshot_progress(&t).await.unwrap();
}

#[tokio::test]
async fn marker_only_flush_persists_markers_atomically() {
    // Production blue-green pattern: a flush whose only payload is a
    // marker INSERT (no user-data claims). Earlier `claim_offsets`
    // implementations early-returned on empty claims and silently
    // dropped markers — DST caught it. This guards the prod variant.
    let coord = fresh_coord().await;
    let uuid = uuid::Uuid::new_v4().to_string();
    let batch = CommitBatch {
        claims: vec![],
        flushable_lsn: Lsn(500),
        replicated_lsn: None,
        markers: vec![MarkerInfo {
            uuid: uuid.clone(),
            commit_lsn: Lsn(500),
        }],
    };
    let receipt = coord.claim_offsets(&batch).await.expect("marker-only");
    assert!(receipt.grants.is_empty());
    assert_eq!(receipt.flushable_lsn, Lsn(500));

    // The marker should be visible to pending_markers_for_table for any
    // table that has no log_index entries (vacuously eligible).
    let t = ident("public", "orders");
    let pending = coord.pending_markers_for_table(&t, -1).await.unwrap();
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].uuid, uuid);
    assert_eq!(pending[0].commit_lsn, Lsn(500));
}

#[tokio::test]
async fn marker_eligibility_blocks_until_cursor_passes_marker_lsn() {
    let coord = fresh_coord().await;
    let t = ident("public", "orders");
    let uuid = uuid::Uuid::new_v4().to_string();

    // Stage one log_index entry at flushable_lsn=100 covering offsets [0, 3),
    // and persist a marker at commit_lsn=200 in the same batch.
    let batch = CommitBatch {
        claims: vec![claim(&t, 3, "s3://b/a.parquet")],
        flushable_lsn: Lsn(100),
        replicated_lsn: None,
        markers: vec![MarkerInfo {
            uuid: uuid.clone(),
            commit_lsn: Lsn(200),
        }],
    };
    coord.claim_offsets(&batch).await.unwrap();

    // Cursor at -1: log_index entry has flushable_lsn(100) <= marker(200)
    // but end_offset(3) > cursor(-1), so marker is NOT yet eligible.
    assert!(coord
        .pending_markers_for_table(&t, -1)
        .await
        .unwrap()
        .is_empty());

    // Cursor at 2: still end_offset(3) > 2, blocked.
    assert!(coord
        .pending_markers_for_table(&t, 2)
        .await
        .unwrap()
        .is_empty());

    // Cursor at 3: end_offset(3) <= 3, eligible.
    let pending = coord.pending_markers_for_table(&t, 3).await.unwrap();
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].uuid, uuid);

    // After recording emission, no longer pending.
    coord.record_marker_emitted(&uuid, &t).await.unwrap();
    assert!(coord
        .pending_markers_for_table(&t, 3)
        .await
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn marker_eligibility_per_table_independent() {
    // A marker eligible for table A is not auto-emitted for table B.
    // record_marker_emitted is per (uuid, table); per-table state is
    // tracked independently.
    let coord = fresh_coord().await;
    let a = ident("public", "a");
    let b = ident("public", "b");
    let uuid = uuid::Uuid::new_v4().to_string();

    coord
        .claim_offsets(&CommitBatch {
            claims: vec![],
            flushable_lsn: Lsn(50),
            replicated_lsn: None,
            markers: vec![MarkerInfo {
                uuid: uuid.clone(),
                commit_lsn: Lsn(50),
            }],
        })
        .await
        .unwrap();

    // Both eligible (no log_index rows for either, so vacuously).
    assert_eq!(
        coord.pending_markers_for_table(&a, -1).await.unwrap().len(),
        1
    );
    assert_eq!(
        coord.pending_markers_for_table(&b, -1).await.unwrap().len(),
        1
    );

    // Emit for A only; B still pending.
    coord.record_marker_emitted(&uuid, &a).await.unwrap();
    assert!(coord
        .pending_markers_for_table(&a, -1)
        .await
        .unwrap()
        .is_empty());
    assert_eq!(
        coord.pending_markers_for_table(&b, -1).await.unwrap().len(),
        1
    );
}
