//! End-to-end compaction tests against `MemoryCatalog` + `MemoryBlobStore`.
//!
//! These exercise [`pg2iceberg_iceberg::compact::compact_table`] across a
//! range of real-world cases:
//! - threshold gating (no compaction below threshold)
//! - small-file merge (many → one)
//! - equality-delete application inline
//! - **partition-aware output** (the Go bug we don't replicate)
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
    compact_table, fold::MaterializedRow, read_materialized_state, Catalog, CompactionConfig,
    DataFile, FileIndex, PreparedCommit, TableWriter,
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
            config,
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
    };
    let out = h.run_compaction(&s, &cfg).expect("compaction should run");
    // The delete at seq 2 cannot drop the snap-3 insert.
    assert_eq!(out.rows_removed_by_deletes, 0);

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
            }],
        );
    }
    h.commit(
        &s_cold,
        vec![MaterializedRow {
            op: Op::Insert,
            row: row_id_qty(1, 1),
            unchanged_cols: vec![],
        }],
    );

    let cfg = CompactionConfig {
        data_file_threshold: 3,
        delete_file_threshold: 1,
        target_size_bytes: 1024 * 1024 * 1024,
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
