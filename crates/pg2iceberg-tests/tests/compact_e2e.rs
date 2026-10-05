//! End-to-end compaction tests against `MemoryCatalog` + `MemoryBlobStore`.
//!
//! These exercise [`pg2iceberg_iceberg::compact::compact_table`] across a
//! range of real-world cases:
//! - threshold gating (no compaction below threshold)
//! - small-file merge (many → one)
//! - equality-delete application inline
//! - **partition-aware output** (each partition compacts independently)
//! - second-cycle no-op
//! - sequence-aware delete semantics
//!
//! They live here (workspace integration crate) instead of in
//! `pg2iceberg-iceberg/src/compact.rs` because the harness needs both the
//! sim's `MemoryCatalog` (which depends on `pg2iceberg-iceberg`, so cycling
//! it as a dev-dep is impossible) and a `BlobStore` impl.

use bytes::Bytes;
use pg2iceberg_core::partition::{PartitionField, PartitionLiteral, Transform};
use pg2iceberg_core::typemap::IcebergType;
use pg2iceberg_core::value::PgValue;
use pg2iceberg_core::{ColumnName, ColumnSchema, Namespace, Op, Row, TableIdent, TableSchema};
use pg2iceberg_iceberg::{
    compact_table, fold::MaterializedRow, read_materialized_state, rebuild_from_catalog, Catalog,
    CompactionConfig, DataFile, FileIndex, PreparedCommit, TableWriter,
};
use pg2iceberg_sim::blob::MemoryBlobStore;
use pg2iceberg_sim::catalog::MemoryCatalog;
use pg2iceberg_stream::BlobStore;
use pollster::block_on;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

fn ident() -> TableIdent {
    TableIdent {
        namespace: Namespace(vec!["public".into()]),
        name: "orders".into(),
    }
}

fn schema_id_qty() -> TableSchema {
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
        partition_spec: Vec::new(),
        pg_schema: None,
    }
}

fn schema_partitioned_by_region() -> TableSchema {
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
                name: "region".into(),
                field_id: 2,
                ty: IcebergType::String,
                nullable: false,
                is_primary_key: false,
            },
        ],
        partition_spec: vec![PartitionField {
            source_column: "region".into(),
            name: "region".into(),
            transform: Transform::Identity,
        }],
        pg_schema: None,
    }
}

fn pk_cols() -> Vec<ColumnName> {
    vec![ColumnName("id".into())]
}

fn row_id_qty(id: i32, qty: i32) -> Row {
    let mut r = BTreeMap::new();
    r.insert(ColumnName("id".into()), PgValue::Int4(id));
    r.insert(ColumnName("qty".into()), PgValue::Int4(qty));
    r
}

fn row_id_region(id: i32, region: &str) -> Row {
    let mut r = BTreeMap::new();
    r.insert(ColumnName("id".into()), PgValue::Int4(id));
    r.insert(ColumnName("region".into()), PgValue::Text(region.into()));
    r
}

fn pk_only(id: i32) -> Row {
    let mut r = BTreeMap::new();
    r.insert(ColumnName("id".into()), PgValue::Int4(id));
    r
}

struct Harness {
    cat: MemoryCatalog,
    blob: Arc<MemoryBlobStore>,
    counter: AtomicU64,
}

impl Harness {
    fn new() -> Self {
        Self {
            cat: MemoryCatalog::new(),
            blob: Arc::new(MemoryBlobStore::new()),
            counter: AtomicU64::new(0),
        }
    }

    /// Append a single materializer-style commit (data + equality deletes)
    /// to the catalog. Returns the data files written.
    fn commit(&self, schema: &TableSchema, rows: Vec<MaterializedRow>) -> Vec<DataFile> {
        let writer = TableWriter::new(schema.clone());
        let prepared = writer.prepare(&rows, &FileIndex::new()).unwrap();

        let mut data_files = Vec::new();
        for chunk in prepared.data {
            let n = self.counter.fetch_add(1, Ordering::SeqCst);
            let path = format!("test/data-{n}.parquet");
            block_on(self.blob.put(&path, Bytes::clone(&chunk.chunk.bytes))).unwrap();
            data_files.push(DataFile {
                path,
                record_count: chunk.chunk.record_count,
                byte_size: chunk.chunk.bytes.len() as u64,
                equality_field_ids: vec![],
                partition_values: chunk.partition_values,
            });
        }
        let mut delete_files = Vec::new();
        for chunk in prepared.equality_deletes {
            let n = self.counter.fetch_add(1, Ordering::SeqCst);
            let path = format!("test/delete-{n}.parquet");
            block_on(self.blob.put(&path, Bytes::clone(&chunk.chunk.bytes))).unwrap();
            delete_files.push(DataFile {
                path,
                record_count: chunk.chunk.record_count,
                byte_size: chunk.chunk.bytes.len() as u64,
                equality_field_ids: prepared.pk_field_ids.clone(),
                partition_values: chunk.partition_values,
            });
        }
        block_on(self.cat.commit_snapshot(PreparedCommit {
            ident: schema.ident.clone(),
            data_files: data_files.clone(),
            equality_deletes: delete_files,
        }))
        .unwrap();
        data_files
    }

    fn run_compaction(
        &self,
        schema: &TableSchema,
        config: &CompactionConfig,
    ) -> Option<pg2iceberg_iceberg::CompactionOutcome> {
        let live_pks = self.file_index(schema);
        let counter = &self.counter;
        block_on(compact_table(
            &self.cat,
            self.blob.as_ref(),
            move |_ident, idx| {
                let n = counter.fetch_add(1, Ordering::SeqCst);
                let path = format!("test/compact-{n}-{idx}.parquet");
                async move { path }
            },
            &schema.ident,
            schema,
            &pk_cols(),
            Some(&live_pks),
            config,
        ))
        .unwrap()
    }

    /// The PK → file index the materializer would hold for this table.
    fn file_index(&self, schema: &TableSchema) -> FileIndex {
        block_on(rebuild_from_catalog(
            &self.cat,
            self.blob.as_ref(),
            &schema.ident,
            schema,
            &pk_cols(),
        ))
        .unwrap()
    }
}

fn ensure_table(h: &Harness, s: &TableSchema) {
    block_on(h.cat.ensure_namespace(&s.ident.namespace)).unwrap();
    block_on(h.cat.create_table(s)).unwrap();
}

fn insert(h: &Harness, s: &TableSchema, row: Row) {
    h.commit(
        s,
        vec![MaterializedRow {
            op: Op::Insert,
            row,
            unchanged_cols: vec![],
            unchanged_from: None,
        }],
    );
}

fn delete(h: &Harness, s: &TableSchema, row: Row) {
    h.commit(
        s,
        vec![MaterializedRow {
            op: Op::Delete,
            row,
            unchanged_cols: vec![],
            unchanged_from: None,
        }],
    );
}

#[test]
fn below_threshold_returns_none() {
    let h = Harness::new();
    let s = schema_id_qty();
    ensure_table(&h, &s);
    insert(&h, &s, row_id_qty(1, 10));
    let cfg = CompactionConfig {
        data_file_threshold: 8,
        delete_file_threshold: 4,
        target_size_bytes: 1024 * 1024 * 1024,
        ..Default::default()
    };
    assert!(h.run_compaction(&s, &cfg).is_none());
}

#[test]
fn small_files_are_compacted_into_one() {
    let h = Harness::new();
    let s = schema_id_qty();
    ensure_table(&h, &s);
    for i in 1..=5 {
        insert(&h, &s, row_id_qty(i, i * 10));
    }
    let cfg = CompactionConfig {
        data_file_threshold: 3,
        delete_file_threshold: 1,
        target_size_bytes: 1024 * 1024 * 1024,
        ..Default::default()
    };
    let out = h.run_compaction(&s, &cfg).expect("compaction should run");
    assert_eq!(out.input_data_files, 5);
    assert_eq!(out.output_data_files, 1);
    assert_eq!(out.rows_rewritten, 5);
    assert_eq!(out.rows_removed_by_deletes, 0);

    let visible = block_on(read_materialized_state(
        &h.cat,
        h.blob.as_ref(),
        &s.ident,
        &s,
        &pk_cols(),
    ))
    .unwrap();
    assert_eq!(visible.len(), 5);
}

#[test]
fn equality_deletes_are_applied_inline() {
    let h = Harness::new();
    let s = schema_id_qty();
    ensure_table(&h, &s);
    insert(&h, &s, row_id_qty(1, 10));
    insert(&h, &s, row_id_qty(2, 20));
    delete(&h, &s, pk_only(1));

    let cfg = CompactionConfig {
        data_file_threshold: 1,
        delete_file_threshold: 1,
        target_size_bytes: 1024 * 1024 * 1024,
        ..Default::default()
    };
    let out = h.run_compaction(&s, &cfg).expect("compaction should run");
    assert_eq!(out.input_delete_files, 1);
    assert_eq!(out.rows_removed_by_deletes, 1);

    let visible = block_on(read_materialized_state(
        &h.cat,
        h.blob.as_ref(),
        &s.ident,
        &s,
        &pk_cols(),
    ))
    .unwrap();
    assert_eq!(visible.len(), 1);
    assert_eq!(
        visible[0].get(&ColumnName("id".into())),
        Some(&PgValue::Int4(2))
    );
}

#[test]
fn partitioned_table_compacted_output_has_correct_partition_values() {
    // The Go-reference compaction bug we don't replicate: output files
    // must group by partition. Each output `DataFile` carries a
    // single non-empty partition tuple, and rows from different
    // partitions never share a file.
    let h = Harness::new();
    let s = schema_partitioned_by_region();
    ensure_table(&h, &s);
    for i in 1..=3 {
        insert(&h, &s, row_id_region(i, "us"));
    }
    for i in 4..=5 {
        insert(&h, &s, row_id_region(i, "eu"));
    }

    let cfg = CompactionConfig {
        data_file_threshold: 3,
        delete_file_threshold: 1,
        target_size_bytes: 1024 * 1024 * 1024,
        ..Default::default()
    };
    let out = h.run_compaction(&s, &cfg).expect("compaction should run");
    assert!(
        out.output_data_files >= 2,
        "expected ≥2 output files for 2 partitions, got {}",
        out.output_data_files
    );

    let snaps = block_on(h.cat.snapshots(&s.ident)).unwrap();
    let last = snaps.last().expect("at least one snapshot post-compaction");
    let mut by_region: BTreeMap<String, u64> = BTreeMap::new();
    for df in &last.data_files {
        assert_eq!(
            df.partition_values.len(),
            1,
            "expected exactly one partition value per output file, got {:?}",
            df.partition_values
        );
        if let PartitionLiteral::String(r) = &df.partition_values[0] {
            *by_region.entry(r.clone()).or_default() += df.record_count;
        } else {
            panic!(
                "expected String partition value, got {:?}",
                df.partition_values[0]
            );
        }
    }
    assert_eq!(by_region.get("us"), Some(&3));
    assert_eq!(by_region.get("eu"), Some(&2));

    let visible = block_on(read_materialized_state(
        &h.cat,
        h.blob.as_ref(),
        &s.ident,
        &s,
        &pk_cols(),
    ))
    .unwrap();
    assert_eq!(visible.len(), 5);
}

#[test]
fn empty_history_returns_none() {
    let h = Harness::new();
    let s = schema_id_qty();
    ensure_table(&h, &s);
    let cfg = CompactionConfig::default();
    assert!(h.run_compaction(&s, &cfg).is_none());
}

#[test]
fn second_compaction_is_noop_after_first() {
    let h = Harness::new();
    let s = schema_id_qty();
    ensure_table(&h, &s);
    for i in 1..=5 {
        insert(&h, &s, row_id_qty(i, i * 10));
    }
    let cfg = CompactionConfig {
        data_file_threshold: 3,
        delete_file_threshold: 1,
        target_size_bytes: 1024 * 1024 * 1024,
        ..Default::default()
    };
    let first = h.run_compaction(&s, &cfg).expect("first compaction runs");
    assert_eq!(first.output_data_files, 1);
    let second = h.run_compaction(&s, &cfg);
    assert!(
        second.is_none(),
        "second compaction should noop, got {second:?}"
    );
}

#[test]
fn delete_at_seq_5_does_not_drop_row_inserted_at_seq_7() {
    // Sequence-aware semantics: equality-delete with seq S applies only to
    // rows from snapshots strictly before S. A re-insert after the delete
    // is immune.
    let h = Harness::new();
    let s = schema_id_qty();
    ensure_table(&h, &s);

    // Snap 1: insert PK 1.
    insert(&h, &s, row_id_qty(1, 10));
    // Snap 2: delete PK 1 (seq 2).
    delete(&h, &s, pk_only(1));
    // Snap 3: re-insert PK 1 with new value (seq 3). Should survive the
    // earlier delete.
    insert(&h, &s, row_id_qty(1, 99));

    let cfg = CompactionConfig {
        data_file_threshold: 1,
        delete_file_threshold: 1,
        target_size_bytes: 1024 * 1024 * 1024,
        ..Default::default()
    };
    let out = h.run_compaction(&s, &cfg).expect("compaction should run");
    // The delete at seq 2 drops the snap-1 row but cannot drop the snap-3
    // insert.
    assert_eq!(out.rows_removed_by_deletes, 1);

    let visible = block_on(read_materialized_state(
        &h.cat,
        h.blob.as_ref(),
        &s.ident,
        &s,
        &pk_cols(),
    ))
    .unwrap();
    assert_eq!(visible.len(), 1);
    assert_eq!(
        visible[0].get(&ColumnName("qty".into())),
        Some(&PgValue::Int4(99)),
        "the survivor should be the re-insert (qty=99), not the original (qty=10)"
    );
}

// ── Phase D additions: comprehensive scenarios ──────────────────────

fn ident_named(name: &str) -> TableIdent {
    TableIdent {
        namespace: Namespace(vec!["public".into()]),
        name: name.into(),
    }
}

fn schema_named(name: &str) -> TableSchema {
    TableSchema {
        ident: ident_named(name),
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
        partition_spec: Vec::new(),
        pg_schema: None,
    }
}

#[test]
fn compact_cycle_independently_handles_multiple_registered_tables() {
    // Two tables, one above threshold, one below. compact_table runs
    // per-ident; the one above threshold compacts, the other noops.
    let h = Harness::new();

    let s_hot = schema_named("hot_orders");
    let s_cold = schema_named("cold_audit");
    block_on(h.cat.ensure_namespace(&ident_named("hot_orders").namespace)).unwrap();
    block_on(h.cat.create_table(&s_hot)).unwrap();
    block_on(h.cat.create_table(&s_cold)).unwrap();

    // hot_orders gets 5 small files; cold_audit gets 1.
    for i in 1..=5 {
        h.commit(
            &s_hot,
            vec![MaterializedRow {
                op: Op::Insert,
                row: row_id_qty(i, i * 10),
                unchanged_cols: vec![],
                unchanged_from: None,
            }],
        );
    }
    h.commit(
        &s_cold,
        vec![MaterializedRow {
            op: Op::Insert,
            row: row_id_qty(1, 1),
            unchanged_cols: vec![],
            unchanged_from: None,
        }],
    );

    let cfg = CompactionConfig {
        data_file_threshold: 3,
        delete_file_threshold: 1,
        target_size_bytes: 1024 * 1024 * 1024,
        ..Default::default()
    };

    let counter = &h.counter;
    let outcome_hot = block_on(compact_table(
        &h.cat,
        h.blob.as_ref(),
        |_, idx| {
            let n = counter.fetch_add(1, Ordering::SeqCst);
            let path = format!("test/compact-hot-{n}-{idx}.parquet");
            async move { path }
        },
        &s_hot.ident,
        &s_hot,
        &pk_cols(),
        None,
        &cfg,
    ))
    .unwrap();
    let outcome_cold = block_on(compact_table(
        &h.cat,
        h.blob.as_ref(),
        |_, idx| {
            let n = counter.fetch_add(1, Ordering::SeqCst);
            let path = format!("test/compact-cold-{n}-{idx}.parquet");
            async move { path }
        },
        &s_cold.ident,
        &s_cold,
        &pk_cols(),
        None,
        &cfg,
    ))
    .unwrap();
    assert!(outcome_hot.is_some(), "hot table should compact");
    assert!(outcome_cold.is_none(), "cold table should noop");

    // Verifier still sees correct state on both tables.
    let visible_hot = block_on(read_materialized_state(
        &h.cat,
        h.blob.as_ref(),
        &s_hot.ident,
        &s_hot,
        &pk_cols(),
    ))
    .unwrap();
    let visible_cold = block_on(read_materialized_state(
        &h.cat,
        h.blob.as_ref(),
        &s_cold.ident,
        &s_cold,
        &pk_cols(),
    ))
    .unwrap();
    assert_eq!(visible_hot.len(), 5);
    assert_eq!(visible_cold.len(), 1);
}

#[test]
fn many_small_files_compact_into_few_with_deletes_applied() {
    // Stress: 20 small files with interspersed deletes. After compaction,
    // the final state must still match: insert/delete semantics survive.
    let h = Harness::new();
    let s = schema_id_qty();
    ensure_table(&h, &s);

    for i in 1..=20 {
        insert(&h, &s, row_id_qty(i, i * 10));
    }
    for i in (1..=20).step_by(2) {
        delete(&h, &s, pk_only(i));
    }

    let cfg = CompactionConfig {
        data_file_threshold: 5,
        delete_file_threshold: 1,
        target_size_bytes: 1024 * 1024 * 1024,
        ..Default::default()
    };
    let outcome = h.run_compaction(&s, &cfg).expect("compaction should run");
    assert!(outcome.input_data_files >= 20);
    assert!(outcome.input_delete_files >= 10);
    assert_eq!(outcome.rows_removed_by_deletes, 10);

    let visible = block_on(read_materialized_state(
        &h.cat,
        h.blob.as_ref(),
        &s.ident,
        &s,
        &pk_cols(),
    ))
    .unwrap();
    assert_eq!(visible.len(), 10);
    let mut survivors: Vec<i32> = visible
        .iter()
        .filter_map(|r| match r.get(&ColumnName("id".into())) {
            Some(PgValue::Int4(n)) => Some(*n),
            _ => None,
        })
        .collect();
    survivors.sort();
    assert_eq!(survivors, vec![2, 4, 6, 8, 10, 12, 14, 16, 18, 20]);
}

#[test]
fn partitioned_table_with_deletes_compacts_per_partition() {
    // Combines the partition-bug regression with delete handling — the
    // hard case: per-partition output AND seq-aware delete application
    // both hold. Deletes in partition "us" must not leak into "eu".
    let h = Harness::new();
    let s = schema_partitioned_by_region();
    ensure_table(&h, &s);

    for i in 1..=4 {
        insert(&h, &s, row_id_region(i, "us"));
    }
    for i in 5..=7 {
        insert(&h, &s, row_id_region(i, "eu"));
    }
    // Need to delete with full row (region) so partition can be computed,
    // since partition column is not in PK.
    delete(&h, &s, row_id_region(2, "us"));
    delete(&h, &s, row_id_region(6, "eu"));

    let cfg = CompactionConfig {
        data_file_threshold: 3,
        delete_file_threshold: 1,
        target_size_bytes: 1024 * 1024 * 1024,
        ..Default::default()
    };
    let outcome = h.run_compaction(&s, &cfg).expect("compaction should run");
    assert!(outcome.output_data_files >= 2, "expected ≥2 partitions");
    assert_eq!(outcome.rows_removed_by_deletes, 2);

    let snaps = block_on(h.cat.snapshots(&s.ident)).unwrap();
    let last = snaps.last().unwrap();
    let mut by_region: BTreeMap<String, u64> = BTreeMap::new();
    for df in &last.data_files {
        assert_eq!(df.partition_values.len(), 1);
        if let PartitionLiteral::String(r) = &df.partition_values[0] {
            *by_region.entry(r.clone()).or_default() += df.record_count;
        }
    }
    assert_eq!(by_region.get("us"), Some(&3));
    assert_eq!(by_region.get("eu"), Some(&2));

    let visible = block_on(read_materialized_state(
        &h.cat,
        h.blob.as_ref(),
        &s.ident,
        &s,
        &pk_cols(),
    ))
    .unwrap();
    assert_eq!(visible.len(), 5);
}

#[test]
fn repeated_compaction_cycles_keep_state_correct() {
    // Compaction → more inserts → compaction again. Verifies
    // Snapshot.removed_paths accumulates correctly across multiple
    // Replace snapshots without breaking the verifier walk.
    let h = Harness::new();
    let s = schema_id_qty();
    ensure_table(&h, &s);

    for i in 1..=5 {
        insert(&h, &s, row_id_qty(i, i));
    }
    let cfg = CompactionConfig {
        data_file_threshold: 3,
        delete_file_threshold: 1,
        target_size_bytes: 1024 * 1024 * 1024,
        ..Default::default()
    };
    let r1 = h.run_compaction(&s, &cfg).expect("round 1 compacts");
    assert_eq!(r1.output_data_files, 1);

    for i in 6..=9 {
        insert(&h, &s, row_id_qty(i, i));
    }
    let r2 = h.run_compaction(&s, &cfg).expect("round 2 compacts");
    assert_eq!(r2.input_data_files, 5, "1 carried + 4 new");
    assert_eq!(r2.output_data_files, 1);

    let visible = block_on(read_materialized_state(
        &h.cat,
        h.blob.as_ref(),
        &s.ident,
        &s,
        &pk_cols(),
    ))
    .unwrap();
    assert_eq!(visible.len(), 9);
}

// ── Bounded memory ──────────────────────────────────────────────────

/// Records every path compaction fetches, so tests can assert which
/// files a pass actually read.
struct CountingBlob {
    inner: Arc<MemoryBlobStore>,
    gets: std::sync::Mutex<Vec<String>>,
}

#[async_trait::async_trait]
impl BlobStore for CountingBlob {
    async fn put(&self, path: &str, bytes: Bytes) -> pg2iceberg_stream::Result<()> {
        self.inner.put(path, bytes).await
    }
    async fn get(&self, path: &str) -> pg2iceberg_stream::Result<Bytes> {
        self.gets.lock().unwrap().push(path.to_string());
        self.inner.get(path).await
    }
    async fn list(
        &self,
        prefix: &str,
    ) -> pg2iceberg_stream::Result<Vec<pg2iceberg_stream::BlobInfo>> {
        self.inner.list(prefix).await
    }
    async fn delete(&self, path: &str) -> pg2iceberg_stream::Result<()> {
        self.inner.delete(path).await
    }
}

impl Harness {
    /// Like `run_compaction`, but also returns every path the pass read.
    fn run_compaction_counting(
        &self,
        schema: &TableSchema,
        config: &CompactionConfig,
    ) -> (Option<pg2iceberg_iceberg::CompactionOutcome>, Vec<String>) {
        let live_pks = self.file_index(schema);
        let blob = CountingBlob {
            inner: self.blob.clone(),
            gets: std::sync::Mutex::new(Vec::new()),
        };
        let counter = &self.counter;
        let out = block_on(compact_table(
            &self.cat,
            &blob,
            move |_ident, idx| {
                let n = counter.fetch_add(1, Ordering::SeqCst);
                let path = format!("test/compact-{n}-{idx}.parquet");
                async move { path }
            },
            &schema.ident,
            schema,
            &pk_cols(),
            Some(&live_pks),
            config,
        ))
        .unwrap();
        (out, blob.gets.into_inner().unwrap())
    }
}

fn op_row(op: Op, row: Row) -> MaterializedRow {
    MaterializedRow {
        op,
        row,
        unchanged_cols: vec![],
        unchanged_from: None,
    }
}

/// Visible `id → qty` per `read_materialized_state`; panics on a
/// duplicate PK, which no correct table may expose.
fn visible_id_qty(h: &Harness, s: &TableSchema) -> BTreeMap<i32, i32> {
    let rows = block_on(read_materialized_state(
        &h.cat,
        h.blob.as_ref(),
        &s.ident,
        s,
        &pk_cols(),
    ))
    .unwrap();
    let mut out = BTreeMap::new();
    for r in rows {
        let (Some(PgValue::Int4(id)), Some(PgValue::Int4(qty))) = (
            r.get(&ColumnName("id".into())),
            r.get(&ColumnName("qty".into())),
        ) else {
            panic!("unexpected row shape: {r:?}");
        };
        assert!(out.insert(*id, *qty).is_none(), "duplicate visible pk {id}");
    }
    out
}

fn live_data_files(h: &Harness, s: &TableSchema) -> Vec<DataFile> {
    let snaps = block_on(h.cat.snapshots(&s.ident)).unwrap();
    let removed: std::collections::BTreeSet<String> = snaps
        .iter()
        .flat_map(|s| s.removed_paths.iter().cloned())
        .collect();
    snaps
        .into_iter()
        .flat_map(|s| s.data_files)
        .filter(|f| !removed.contains(&f.path))
        .collect()
}

/// 12 files × 250 rows, then one commit that updates and deletes a row
/// in every file, so the whole table must be rewritten. The pass must
/// stream it: never more than one input file's rows decoded at once,
/// output rolled at the target size, and contents unchanged.
#[test]
fn compaction_holds_at_most_one_input_file_of_rows() {
    let h = Harness::new();
    let s = schema_id_qty();
    ensure_table(&h, &s);
    const FILES: i32 = 12;
    const PER_FILE: i32 = 250;
    for f in 0..FILES {
        let rows = (0..PER_FILE)
            .map(|i| op_row(Op::Insert, row_id_qty(f * PER_FILE + i, f * PER_FILE + i)))
            .collect();
        h.commit(&s, rows);
    }
    let mut churn = Vec::new();
    for f in 0..FILES {
        churn.push(op_row(Op::Update, row_id_qty(f * PER_FILE + 7, -1)));
        churn.push(op_row(Op::Delete, pk_only(f * PER_FILE + 13)));
    }
    h.commit(&s, churn);

    let before = visible_id_qty(&h, &s);
    assert_eq!(before.len(), (FILES * PER_FILE - FILES) as usize);
    let largest_input = live_data_files(&h, &s)
        .iter()
        .map(|f| f.record_count)
        .max()
        .unwrap();
    assert_eq!(largest_input, PER_FILE as u64);

    let target = 4 * 1024;
    let cfg = CompactionConfig {
        data_file_threshold: 1,
        delete_file_threshold: 1,
        target_size_bytes: target,
        ..Default::default()
    };
    let out = h.run_compaction(&s, &cfg).expect("compaction should run");
    assert!(out.rows_rewritten >= (FILES * PER_FILE) as u64 - 1);
    assert!(
        out.peak_rows_in_memory <= largest_input,
        "held {} rows at once; one input file has {largest_input}",
        out.peak_rows_in_memory
    );
    assert!(
        out.output_data_files > 1,
        "output must roll at target_size_bytes, got one file of {} bytes",
        out.bytes_after
    );
    assert_eq!(visible_id_qty(&h, &s), before);
}

/// One file far larger than a decode batch: the pass decodes it batch
/// by batch instead of all at once.
#[test]
fn compaction_decodes_a_large_file_in_batches() {
    let h = Harness::new();
    let s = schema_id_qty();
    ensure_table(&h, &s);
    let rows = (0..5000)
        .map(|i| op_row(Op::Insert, row_id_qty(i, i)))
        .collect();
    h.commit(&s, rows);
    h.commit(
        &s,
        (0..10)
            .map(|i| op_row(Op::Delete, pk_only(i * 400)))
            .collect(),
    );
    let before = visible_id_qty(&h, &s);

    let cfg = CompactionConfig {
        data_file_threshold: 1,
        delete_file_threshold: 1,
        target_size_bytes: 1024 * 1024 * 1024,
        ..Default::default()
    };
    let out = h.run_compaction(&s, &cfg).expect("compaction should run");
    assert_eq!(out.rows_removed_by_deletes, 10);
    // A fixed bound, not `DECODE_BATCH_ROWS`: the point is that the
    // 5,000-row file is never decoded whole, whatever the constant says.
    assert!(
        out.peak_rows_in_memory <= 1024,
        "held {} rows at once",
        out.peak_rows_in_memory
    );
    assert_eq!(visible_id_qty(&h, &s), before);
}

/// Six large, clean files and one delete that hits a single one of them.
/// Finding the affected file must not decode the other five.
#[test]
fn compaction_reads_only_the_files_it_rewrites() {
    let h = Harness::new();
    let s = schema_id_qty();
    ensure_table(&h, &s);
    let mut files = Vec::new();
    for f in 0..6 {
        let rows = (0..200)
            .map(|i| op_row(Op::Insert, row_id_qty(f * 200 + i, i)))
            .collect();
        files.extend(h.commit(&s, rows));
    }
    h.commit(&s, vec![op_row(Op::Delete, pk_only(3 * 200 + 5))]);
    let before = visible_id_qty(&h, &s);

    // Every data file is far above `target / 2`, so only the delete
    // makes one of them worth rewriting.
    let cfg = CompactionConfig {
        data_file_threshold: 1,
        delete_file_threshold: 1,
        target_size_bytes: 64,
        ..Default::default()
    };
    let (out, gets) = h.run_compaction_counting(&s, &cfg);
    let out = out.expect("compaction should run");
    assert_eq!(out.input_data_files, 1);
    assert_eq!(out.rows_removed_by_deletes, 1);
    let data_reads: Vec<&String> = gets
        .iter()
        .filter(|p| files.iter().any(|f| &f.path == *p))
        .collect();
    assert_eq!(
        data_reads,
        vec![&files[3].path],
        "only the file holding the deleted row should be read"
    );
    assert_eq!(visible_id_qty(&h, &s), before);
}
