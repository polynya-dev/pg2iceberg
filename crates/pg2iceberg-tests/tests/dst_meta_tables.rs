//! End-to-end DST for the four control-plane meta tables:
//! `<meta_ns>.commits`, `<meta_ns>.checkpoints`, `<meta_ns>.compactions`,
//! `<meta_ns>.maintenance`. Verifies that:
//!
//! - `enable_meta_recording` creates the four tables in the catalog.
//! - A successful `cycle_table` auto-buffers a row in `commits` and
//!   `flush_meta` commits it as an Iceberg snapshot on the meta
//!   table.
//! - `expire_cycle` produces a `<meta_ns>.maintenance` row with
//!   `operation = "expire_snapshots"`.
//! - `cleanup_orphans_cycle` produces a row with
//!   `operation = "clean_orphans"`.
//! - `compact_table` auto-records a `<meta_ns>.compactions` row.
//! - The recorded values match the user-table commit's stats:
//!   snapshot_id, lsn, rows, data_files.
//!
//! Schema correctness (field IDs, types, partition spec) is covered
//! by unit tests in `pg2iceberg-iceberg/src/meta.rs`. This test
//! focuses on the *wiring*: that records flow from the materializer
//! through the buffer into Iceberg without manual flush calls.

use std::sync::Arc;

use bytes::Bytes;
use pg2iceberg_coord::schema::CoordSchema;
use pg2iceberg_coord::Coordinator;
use pg2iceberg_core::{
    ColumnName, ColumnSchema, IcebergType, Lsn, Namespace, Op, PgValue, Row, TableIdent,
    TableSchema, Timestamp,
};
use pg2iceberg_iceberg::meta::{
    self as meta, MAINTENANCE_OP_CLEAN_ORPHANS, MAINTENANCE_OP_EXPIRE_SNAPSHOTS,
};
use pg2iceberg_iceberg::{read_data_file, Catalog};
use pg2iceberg_logical::materializer::{CounterMaterializerNamer, Materializer};
use pg2iceberg_logical::pipeline::{CounterBlobNamer, Pipeline};
use pg2iceberg_pg::DecodedMessage;
use pg2iceberg_sim::blob::MemoryBlobStore;
use pg2iceberg_sim::catalog::MemoryCatalog;
use pg2iceberg_sim::clock::TestClock;
use pg2iceberg_sim::coord::MemoryCoordinator;
use pg2iceberg_sim::postgres::{SimPostgres, SimReplicationStream};
use pg2iceberg_stream::BlobStore;
use pollster::block_on;
use std::collections::BTreeMap;

const SLOT: &str = "p2i-slot";
const PUB: &str = "p2i-pub";
const META_NS: &str = "_pg2iceberg_meta";

fn ident() -> TableIdent {
    TableIdent {
        namespace: Namespace(vec!["public".into()]),
        name: "orders".into(),
    }
}

fn schema() -> TableSchema {
    TableSchema {
        ident: ident(),
        columns: vec![
            ColumnSchema {
                name: "id".into(),
                field_id: 1,
                ty: IcebergType::Int,
                nullable: false,
                is_primary_key: true,
            },
            ColumnSchema {
                name: "qty".into(),
                field_id: 2,
                ty: IcebergType::Int,
                nullable: false,
                is_primary_key: false,
            },
        ],
        partition_spec: vec![],
    }
}

fn row(id: i32, qty: i32) -> Row {
    let mut r = BTreeMap::new();
    r.insert(ColumnName("id".into()), PgValue::Int4(id));
    r.insert(ColumnName("qty".into()), PgValue::Int4(qty));
    r
}

struct Harness {
    db: SimPostgres,
    blob: Arc<MemoryBlobStore>,
    catalog: Arc<MemoryCatalog>,
    pipeline: Pipeline<MemoryCoordinator>,
    materializer: Materializer<MemoryCatalog>,
    stream: SimReplicationStream,
}

impl Harness {
    fn boot() -> Self {
        let db = SimPostgres::new();
        db.create_table(schema()).unwrap();
        db.create_publication(PUB, &[ident()]).unwrap();
        db.create_slot(SLOT, PUB).unwrap();

        let clock = TestClock::at(0);
        let arc_clock: Arc<dyn pg2iceberg_core::Clock> = Arc::new(clock);
        let coord = Arc::new(MemoryCoordinator::new(
            CoordSchema::default_name(),
            arc_clock,
        ));
        let blob = Arc::new(MemoryBlobStore::new());
        let catalog = Arc::new(MemoryCatalog::new());

        let mut pipeline = Pipeline::new(
            coord.clone(),
            blob.clone(),
            Arc::new(CounterBlobNamer::new("s3://stage")),
            64,
        );
        pipeline.register_primary_keys(ident(), vec![ColumnName("id".into())]);

        let mut materializer = Materializer::new(
            coord.clone() as Arc<dyn Coordinator>,
            blob.clone(),
            catalog.clone(),
            Arc::new(CounterMaterializerNamer::new("s3://table")),
            "default",
            128,
        );
        block_on(materializer.register_table(schema())).unwrap();
        block_on(materializer.enable_meta_recording(META_NS, "worker-1")).unwrap();

        let stream = db.start_replication(SLOT).unwrap();
        Self {
            db,
            blob,
            catalog,
            pipeline,
            materializer,
            stream,
        }
    }

    fn drive_then_materialize(&mut self) {
        while let Some(msg) = self.stream.recv() {
            if let DecodedMessage::Relation { ident, columns } = &msg {
                block_on(self.materializer.apply_relation(ident, columns)).unwrap();
            }
            block_on(self.pipeline.process(msg)).unwrap();
        }
        block_on(self.pipeline.flush()).unwrap();
        block_on(self.materializer.cycle()).unwrap();
    }

    /// Read every row from a meta table by walking its snapshots and
    /// decoding each data file in turn. Meta tables are append-only,
    /// so concatenating each snapshot's data files gives the full
    /// row history.
    fn read_meta_table(&self, table_name: &str) -> Vec<Row> {
        let ident = TableIdent {
            namespace: Namespace(vec![META_NS.into()]),
            name: table_name.into(),
        };
        let snaps = block_on(self.catalog.snapshots(&ident)).unwrap();
        let meta = block_on(self.catalog.load_table(&ident))
            .unwrap()
            .expect("meta table created by enable_meta_recording");
        let mut out = Vec::new();
        for s in snaps {
            for df in s.data_files {
                let bytes = block_on(self.blob.get(&df.path)).unwrap();
                let rows = read_data_file(&bytes, &meta.schema.columns).unwrap();
                out.extend(rows);
            }
        }
        out
    }
}

// ── 1. enable_meta_recording creates the four tables ─────────────────

#[test]
fn enable_meta_recording_creates_four_tables_in_catalog() {
    let h = Harness::boot();
    for name in ["commits", "checkpoints", "compactions", "maintenance"] {
        let ident = TableIdent {
            namespace: Namespace(vec![META_NS.into()]),
            name: name.into(),
        };
        let meta = block_on(h.catalog.load_table(&ident))
            .unwrap()
            .unwrap_or_else(|| panic!("meta table `{name}` must exist"));
        assert_eq!(meta.schema.columns[0].name, "ts");
        // Every meta table is day(ts)-partitioned.
        assert_eq!(meta.schema.partition_spec.len(), 1);
        assert_eq!(meta.schema.partition_spec[0].source_column, "ts");
    }
}

// ── 2. cycle_table auto-records a commits row ────────────────────────

#[test]
fn cycle_table_records_commits_row_with_user_table_stats() {
    let mut h = Harness::boot();
    let mut tx = h.db.begin_tx();
    tx.insert(&ident(), row(1, 10));
    tx.insert(&ident(), row(2, 20));
    tx.insert(&ident(), row(3, 30));
    tx.commit(Timestamp(1_700_000_000_000_000)).unwrap();
    h.drive_then_materialize();

    let commits = h.read_meta_table("commits");
    assert_eq!(commits.len(), 1, "one row per cycle commit");
    let r = &commits[0];

    // Stats that must reflect the user-table commit.
    assert_eq!(
        r.get(&ColumnName("table_name".into())),
        Some(&PgValue::Text("public.orders".into()))
    );
    assert_eq!(
        r.get(&ColumnName("mode".into())),
        Some(&PgValue::Text("logical".into()))
    );
    assert_eq!(r.get(&ColumnName("rows".into())), Some(&PgValue::Int8(3)));
    // Worker id stamped from `enable_meta_recording`.
    assert_eq!(
        r.get(&ColumnName("worker_id".into())),
        Some(&PgValue::Text("worker-1".into())),
    );
    // snapshot_id and sequence_number both populated and equal.
    let snap = match r.get(&ColumnName("snapshot_id".into())) {
        Some(PgValue::Int8(v)) => *v,
        other => panic!("expected Int8, got {other:?}"),
    };
    let seq = match r.get(&ColumnName("sequence_number".into())) {
        Some(PgValue::Int8(v)) => *v,
        other => panic!("expected Int8, got {other:?}"),
    };
    assert!(snap > 0, "snapshot_id must be a real catalog id");
    assert_eq!(
        snap, seq,
        "Rust port collapses snapshot_id and sequence_number"
    );

    // Optional fields populated when nonzero.
    if let Some(PgValue::Int8(lsn)) = r.get(&ColumnName("lsn".into())) {
        assert!(*lsn > 0, "lsn must be a real WAL position");
    } else {
        panic!("lsn should be set after a real commit");
    }
}

// ── 3. multiple cycles produce multiple rows ─────────────────────────

#[test]
fn multiple_cycles_produce_one_commits_row_each() {
    let mut h = Harness::boot();
    for i in 1..=3 {
        let mut tx = h.db.begin_tx();
        tx.insert(&ident(), row(i, i * 10));
        tx.commit(Timestamp(0)).unwrap();
        h.drive_then_materialize();
    }
    let commits = h.read_meta_table("commits");
    assert_eq!(
        commits.len(),
        3,
        "one row per cycle, three cycles → three rows"
    );
}

// ── 4. expire_cycle records a maintenance row ────────────────────────

#[test]
fn expire_cycle_records_maintenance_row() {
    let mut h = Harness::boot();
    // Need ≥2 snapshots so expiry has something to drop. Two cycles
    // each with an INSERT gives us two user-table snapshots.
    for i in 1..=2 {
        let mut tx = h.db.begin_tx();
        tx.insert(&ident(), row(i, i * 10));
        tx.commit(Timestamp(0)).unwrap();
        h.drive_then_materialize();
    }

    // Drive expire with retention=0 so every non-current snapshot
    // ages out instantly. (MemoryCatalog uses snapshot timestamps
    // anchored to its own clock; retention=0 → cutoff = latest_ts,
    // so anything older than the current snapshot expires.)
    let _ = block_on(h.materializer.expire_cycle(0)).unwrap();

    let maint = h.read_meta_table("maintenance");
    let expire_rows: Vec<&Row> = maint
        .iter()
        .filter(|r| {
            r.get(&ColumnName("operation".into()))
                == Some(&PgValue::Text(MAINTENANCE_OP_EXPIRE_SNAPSHOTS.into()))
        })
        .collect();
    assert!(
        !expire_rows.is_empty(),
        "expire_cycle should produce at least one maintenance row; got: {maint:?}"
    );
    let row0 = expire_rows[0];
    assert_eq!(
        row0.get(&ColumnName("table_name".into())),
        Some(&PgValue::Text("public.orders".into())),
    );
    // bytes_freed=0 for snapshot expiry (metadata-only) → encodes as Null.
    assert_eq!(
        row0.get(&ColumnName("bytes_freed".into())),
        Some(&PgValue::Null),
    );
}

// ── 5. cleanup_orphans_cycle records a maintenance row ───────────────

#[test]
fn cleanup_orphans_cycle_records_maintenance_row() {
    let mut h = Harness::boot();

    // Create a real materialized snapshot so the table has a known
    // file set; then drop an orphan blob into the same prefix.
    let mut tx = h.db.begin_tx();
    tx.insert(&ident(), row(1, 10));
    tx.commit(Timestamp(0)).unwrap();
    h.drive_then_materialize();

    // Plant a real orphan: a parquet at a path that's NOT in any
    // snapshot. Has to live under the prefix the cleanup scan uses.
    block_on(h.blob.put(
        "materialized/orders/orphan-001.parquet",
        Bytes::from_static(b"orphan-bytes"),
    ))
    .unwrap();

    // grace=0 means "treat everything as old enough to delete"; we
    // pass now_ms much larger than the orphan's mtime so it's
    // definitely past grace.
    let _ = block_on(
        h.materializer
            .cleanup_orphans_cycle("materialized", i64::MAX / 2, 0),
    )
    .unwrap();

    let maint = h.read_meta_table("maintenance");
    let clean_rows: Vec<&Row> = maint
        .iter()
        .filter(|r| {
            r.get(&ColumnName("operation".into()))
                == Some(&PgValue::Text(MAINTENANCE_OP_CLEAN_ORPHANS.into()))
        })
        .collect();
    assert!(
        !clean_rows.is_empty(),
        "cleanup_orphans should produce ≥1 row; got: {maint:?}"
    );
    let row0 = clean_rows[0];
    if let Some(PgValue::Int4(items)) = row0.get(&ColumnName("items_affected".into())) {
        assert!(*items >= 1, "at least one orphan deleted");
    } else {
        panic!("items_affected should be set");
    }
}

// ── 6. record_checkpoint API works (manual call) ─────────────────────

#[test]
fn record_checkpoint_buffers_then_flushes_via_explicit_flush_meta() {
    // No periodic CDC checkpoint saves are wired in the Rust port
    // yet (gap from Go). The `record_checkpoint` API stays exposed
    // for callers who want to emit one manually (e.g. snapshot
    // completion, or future periodic-save wiring).
    let mut h = Harness::boot();
    h.materializer.record_checkpoint(meta::CheckpointStats {
        ts_micros: 1_700_000_000_000_000,
        worker_id: String::new(),
        lsn: 12345,
        last_flush_at_micros: 1_700_000_000_000_000,
        pg2iceberg_commit_sha: "abc1234".into(),
    });
    block_on(h.materializer.flush_meta()).unwrap();

    let checkpoints = h.read_meta_table("checkpoints");
    assert_eq!(checkpoints.len(), 1);
    let r = &checkpoints[0];
    assert_eq!(
        r.get(&ColumnName("lsn".into())),
        Some(&PgValue::Int8(12345))
    );
    assert_eq!(
        r.get(&ColumnName("worker_id".into())),
        Some(&PgValue::Text("worker-1".into())),
        "blank worker_id falls back to recorder default",
    );
    assert_eq!(
        r.get(&ColumnName("pg2iceberg_commit_sha".into())),
        Some(&PgValue::Text("abc1234".into())),
    );
}

// ── 7. flush_meta is idempotent on empty buffers ─────────────────────

#[test]
fn flush_meta_no_op_when_buffers_empty() {
    let mut h = Harness::boot();
    // Fresh materializer, no commits ever ran. flush_meta should
    // succeed and produce no Iceberg snapshots on any meta table.
    block_on(h.materializer.flush_meta()).unwrap();
    for name in ["commits", "checkpoints", "compactions", "maintenance"] {
        let snaps = block_on(h.catalog.snapshots(&TableIdent {
            namespace: Namespace(vec![META_NS.into()]),
            name: name.into(),
        }))
        .unwrap();
        assert!(
            snaps.is_empty(),
            "no commits should have been written to {name}"
        );
    }
}

// ── 7b. compact_table auto-records a compactions row ─────────────────

#[test]
fn compact_table_records_compactions_row() {
    let mut h = Harness::boot();

    // Stage enough small commits that compaction has something to
    // rewrite. Each cycle produces one data file; with the default
    // compaction policy a low file_count_threshold triggers it.
    for i in 1..=4 {
        let mut tx = h.db.begin_tx();
        tx.insert(&ident(), row(i, i * 10));
        tx.commit(Timestamp(0)).unwrap();
        h.drive_then_materialize();
    }

    let cfg = pg2iceberg_iceberg::CompactionConfig {
        target_size_bytes: 1024 * 1024,
        // Force compaction: any table with >=2 small files qualifies.
        data_file_threshold: 2,
        delete_file_threshold: 1,
    };
    let outcome = block_on(h.materializer.compact_table(&ident(), &cfg)).unwrap();
    assert!(outcome.is_some(), "compaction should have run");

    let compactions = h.read_meta_table("compactions");
    assert_eq!(compactions.len(), 1, "one row per compaction commit");
    let r = &compactions[0];
    assert_eq!(
        r.get(&ColumnName("table_name".into())),
        Some(&PgValue::Text("public.orders".into())),
    );
    if let Some(PgValue::Int4(n)) = r.get(&ColumnName("input_data_files".into())) {
        assert!(*n >= 2, "compaction folded ≥2 files into one");
    } else {
        panic!("input_data_files should be set");
    }
    if let Some(PgValue::Int4(n)) = r.get(&ColumnName("output_data_files".into())) {
        assert!(*n >= 1, "compaction must produce at least one output file");
    } else {
        panic!("output_data_files should be set");
    }
}

// ── 8. meta recording disabled by default (no leaks) ─────────────────

#[test]
fn no_meta_recording_when_not_enabled() {
    // Don't call enable_meta_recording. record_* calls must be
    // silent no-ops; flush_meta is a no-op; no meta tables exist.
    let db = SimPostgres::new();
    db.create_table(schema()).unwrap();
    db.create_publication(PUB, &[ident()]).unwrap();
    db.create_slot(SLOT, PUB).unwrap();
    let clock = TestClock::at(0);
    let arc_clock: Arc<dyn pg2iceberg_core::Clock> = Arc::new(clock);
    let coord = Arc::new(MemoryCoordinator::new(
        CoordSchema::default_name(),
        arc_clock,
    ));
    let blob = Arc::new(MemoryBlobStore::new());
    let catalog = Arc::new(MemoryCatalog::new());

    let mut materializer = Materializer::new(
        coord.clone() as Arc<dyn Coordinator>,
        blob.clone(),
        catalog.clone(),
        Arc::new(CounterMaterializerNamer::new("s3://table")),
        "default",
        128,
    );
    block_on(materializer.register_table(schema())).unwrap();

    // record_* on a recorder-less materializer must not panic and
    // must not write anything.
    materializer.record_checkpoint(meta::CheckpointStats {
        ts_micros: 1,
        worker_id: String::new(),
        lsn: 1,
        last_flush_at_micros: 0,
        pg2iceberg_commit_sha: String::new(),
    });
    block_on(materializer.flush_meta()).unwrap();

    // No meta tables created.
    let ident = TableIdent {
        namespace: Namespace(vec![META_NS.into()]),
        name: "commits".into(),
    };
    assert!(
        block_on(catalog.load_table(&ident)).unwrap().is_none(),
        "no meta tables should be created when recording is disabled"
    );

    // Suppress unused warnings.
    let _ = (db, &blob);
    let _: Lsn = Lsn(0);
    let _ = ident;
    let _ = Op::Insert;
}
