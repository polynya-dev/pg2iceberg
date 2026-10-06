//! End-to-end snapshot expiry tests against `MemoryCatalog`. The sim's
//! `timestamp_ms` is derived from the monotonic snapshot id (`id * 1000`)
//! so retention math is deterministic without a wall clock.
//!
//! Prod-side parity (against the real iceberg-rust catalog) lives in
//! `pg2iceberg-iceberg::prod::catalog::tests`; this file covers the
//! invariants that materializer-driven workflows depend on.

use bytes::Bytes;
use pg2iceberg_core::typemap::IcebergType;
use pg2iceberg_core::value::PgValue;
use pg2iceberg_core::{ColumnName, ColumnSchema, Namespace, Op, Row, TableIdent, TableSchema};
use pg2iceberg_iceberg::{
    catch_up_from_catalog, fold::MaterializedRow, rebuild_from_catalog, Catalog, DataFile,
    FileIndex, PreparedCommit, PreparedCompaction, TableWriter,
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
        partition_spec: Vec::new(),
        pg_schema: None,
    }
}

fn row(id: i32, qty: i32) -> Row {
    let mut r = BTreeMap::new();
    r.insert(ColumnName("id".into()), PgValue::Int4(id));
    r.insert(ColumnName("qty".into()), PgValue::Int4(qty));
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

    /// Commit `rows` as one snapshot; returns its files' paths.
    fn commit(&self, schema: &TableSchema, rows: Vec<MaterializedRow>) -> Vec<String> {
        let writer = TableWriter::new(schema.clone());
        let prepared = writer.prepare(&rows, &FileIndex::new()).unwrap();
        let data_files = self.put(prepared.data, vec![]);
        let equality_deletes = self.put(prepared.equality_deletes, prepared.pk_field_ids);
        let paths = data_files
            .iter()
            .chain(&equality_deletes)
            .map(|f| f.path.clone())
            .collect();
        block_on(self.cat.commit_snapshot(PreparedCommit {
            ident: schema.ident.clone(),
            data_files,
            equality_deletes,
        }))
        .unwrap();
        paths
    }

    /// Commit a compaction pass rewriting `removed` as one file of `rows`.
    fn compact(&self, schema: &TableSchema, removed: Vec<String>, rows: Vec<Row>) -> Vec<String> {
        self.compact_planned_at(schema, removed, rows, None)
    }

    /// As [`Self::compact`], for a pass that read the table at snapshot
    /// `planned_at`.
    fn compact_planned_at(
        &self,
        schema: &TableSchema,
        removed: Vec<String>,
        rows: Vec<Row>,
        planned_at: Option<i64>,
    ) -> Vec<String> {
        let writer = TableWriter::new(schema.clone());
        let rows: Vec<MaterializedRow> = rows.into_iter().map(inserted).collect();
        let prepared = writer.prepare(&rows, &FileIndex::new()).unwrap();
        let added_data_files = self.put(prepared.data, vec![]);
        let paths = added_data_files.iter().map(|f| f.path.clone()).collect();
        block_on(self.cat.commit_compaction(PreparedCompaction {
            ident: schema.ident.clone(),
            added_data_files,
            removed_paths: removed,
            data_sequence_number: planned_at,
        }))
        .unwrap();
        paths
    }

    fn put(
        &self,
        chunks: Vec<pg2iceberg_iceberg::PreparedChunk>,
        equality_field_ids: Vec<i32>,
    ) -> Vec<DataFile> {
        chunks
            .into_iter()
            .map(|chunk| {
                let n = self.counter.fetch_add(1, Ordering::SeqCst);
                let path = format!("test/data-{n}.parquet");
                block_on(self.blob.put(&path, Bytes::clone(&chunk.chunk.bytes))).unwrap();
                DataFile {
                    path,
                    record_count: chunk.chunk.record_count,
                    byte_size: chunk.chunk.bytes.len() as u64,
                    equality_field_ids: equality_field_ids.clone(),
                    partition_values: chunk.partition_values,
                    sequence_number: None,
                }
            })
            .collect()
    }
}

fn inserted(r: Row) -> MaterializedRow {
    MaterializedRow {
        op: Op::Insert,
        row: r,
        unchanged_cols: vec![],
        unchanged_from: None,
    }
}

fn deleted(id: i32) -> MaterializedRow {
    MaterializedRow {
        op: Op::Delete,
        row: BTreeMap::from([(ColumnName("id".into()), PgValue::Int4(id))]),
        unchanged_cols: vec![],
        unchanged_from: None,
    }
}

fn ensure(h: &Harness, s: &TableSchema) {
    block_on(h.cat.ensure_namespace(&s.ident.namespace)).unwrap();
    block_on(h.cat.create_table(s)).unwrap();
}

fn insert(h: &Harness, s: &TableSchema, r: Row) {
    h.commit(s, vec![inserted(r)]);
}

/// Bring `fi`, the table's FileIndex as of snapshot `at`, up to date.
fn catch_up(h: &Harness, s: &TableSchema, fi: &mut FileIndex, at: Option<i64>) -> Option<i64> {
    block_on(catch_up_from_catalog(
        fi,
        at,
        &h.cat,
        h.blob.as_ref(),
        &ident(),
        s,
        &[ColumnName("id".into())],
    ))
    .unwrap()
}

fn rebuilt(h: &Harness, s: &TableSchema) -> FileIndex {
    block_on(rebuild_from_catalog(
        &h.cat,
        h.blob.as_ref(),
        &ident(),
        s,
        &[ColumnName("id".into())],
    ))
    .unwrap()
}

#[test]
fn expire_snapshots_drops_snapshots_older_than_retention() {
    let h = Harness::new();
    let s = schema();
    ensure(&h, &s);
    // Five snapshots → ids 1, 2, 3, 4, 5 → timestamps 1000, 2000, 3000, 4000, 5000.
    for i in 1..=5 {
        insert(&h, &s, row(i, i * 10));
    }

    // Retention 2500ms relative to latest (5000ms) → cutoff = 2500.
    // Snapshots with timestamp < 2500 are eligible: ids 1, 2.
    let n = block_on(h.cat.expire_snapshots(&ident(), 2500)).unwrap();
    assert_eq!(n, 2, "snapshots 1 and 2 should be expired");
    // Already expired: a second pass has nothing left to do.
    assert_eq!(block_on(h.cat.expire_snapshots(&ident(), 2500)).unwrap(), 0);

    // Expiry drops snapshot metadata, not table state: the files 1 and 2
    // added are still live, so `snapshots` still reports them (under
    // their own sequence numbers) and every row stays visible.
    let snaps = block_on(h.cat.snapshots(&ident())).unwrap();
    let ids: Vec<i64> = snaps.iter().map(|s| s.id).collect();
    assert_eq!(ids, vec![1, 2, 3, 4, 5]);
    assert_eq!(visible_rows(&h, &s), 5);
}

fn visible_rows(h: &Harness, s: &TableSchema) -> usize {
    block_on(pg2iceberg_iceberg::read_materialized_state(
        &h.cat,
        h.blob.as_ref(),
        &ident(),
        s,
        &[ColumnName("id".into())],
    ))
    .unwrap()
    .len()
}

#[test]
fn expire_snapshots_never_drops_current() {
    let h = Harness::new();
    let s = schema();
    ensure(&h, &s);
    insert(&h, &s, row(1, 10));

    // Retention 0 → would expire everything except current. Current is
    // the only snapshot, so nothing expires.
    let n = block_on(h.cat.expire_snapshots(&ident(), 0)).unwrap();
    assert_eq!(n, 0);
    let snaps = block_on(h.cat.snapshots(&ident())).unwrap();
    assert_eq!(snaps.len(), 1);
}

#[test]
fn expire_snapshots_with_huge_retention_expires_nothing() {
    let h = Harness::new();
    let s = schema();
    ensure(&h, &s);
    for i in 1..=3 {
        insert(&h, &s, row(i, i * 10));
    }
    // Retention way bigger than total span — nothing expires.
    let n = block_on(h.cat.expire_snapshots(&ident(), 10_000_000)).unwrap();
    assert_eq!(n, 0);
    let snaps = block_on(h.cat.snapshots(&ident())).unwrap();
    assert_eq!(snaps.len(), 3);
}

#[test]
fn expire_snapshots_on_empty_history_returns_zero() {
    let h = Harness::new();
    let s = schema();
    ensure(&h, &s);
    let n = block_on(h.cat.expire_snapshots(&ident(), 0)).unwrap();
    assert_eq!(n, 0);
}

#[test]
fn expire_then_more_inserts_keeps_state_consistent() {
    let h = Harness::new();
    let s = schema();
    ensure(&h, &s);
    for i in 1..=4 {
        insert(&h, &s, row(i, i));
    }
    // Latest=4000ms; cutoff=4000-1500=2500. Snapshots with ts<2500
    // are id=1 (ts=1000) and id=2 (ts=2000) → both expire.
    let n = block_on(h.cat.expire_snapshots(&ident(), 1500)).unwrap();
    assert_eq!(n, 2);
    // Then add two more.
    insert(&h, &s, row(5, 50));
    insert(&h, &s, row(6, 60));

    let snaps = block_on(h.cat.snapshots(&ident())).unwrap();
    let ids: Vec<i64> = snaps.iter().map(|s| s.id).collect();
    assert_eq!(ids, vec![1, 2, 3, 4, 5, 6]);

    // Every row stays visible: expiry dropped only snapshot metadata.
    assert_eq!(visible_rows(&h, &s), 6);
}

/// A FileIndex catches up with the table by replaying the snapshots after
/// its own, not the whole table.
#[test]
fn file_index_catches_up_on_new_snapshots_only() {
    let h = Harness::new();
    let s = schema();
    ensure(&h, &s);
    for i in 1..=3 {
        insert(&h, &s, row(i, i));
    }
    let mut fi = FileIndex::new();
    let at = catch_up(&h, &s, &mut fi, None);
    insert(&h, &s, row(4, 4));

    let reads = h.blob.gets();
    assert_eq!(catch_up(&h, &s, &mut fi, at), Some(4));
    assert_eq!(h.blob.gets() - reads, 1, "reads only the new file");
    assert_eq!(fi, rebuilt(&h, &s));
}

/// An expired snapshot's stand-in lists the files it added that are still
/// live, not what it removed: catching up past one rebuilds.
#[test]
fn file_index_catching_up_past_an_expired_snapshot_rebuilds() {
    let h = Harness::new();
    let s = schema();
    ensure(&h, &s);
    let first = h.commit(&s, vec![inserted(row(1, 1)), inserted(row(2, 2))]);
    let mut fi = FileIndex::new();
    let at = catch_up(&h, &s, &mut fi, None);
    // Row 2 deleted; row 5's file stays live.
    let mut changed = h.commit(&s, vec![deleted(2), inserted(row(5, 5))]);
    let delete = changed.split_off(1);
    // The first file rewritten without row 2, retiring its delete.
    h.compact(&s, [first, delete].concat(), vec![row(1, 1)]);
    insert(&h, &s, row(6, 6));
    // Snapshots 1–3 expire (cutoff 4000 - 500): 2 and 3 become stand-ins.
    assert_eq!(block_on(h.cat.expire_snapshots(&ident(), 500)).unwrap(), 3);

    assert_eq!(catch_up(&h, &s, &mut fi, at), Some(4));
    assert_eq!(fi, rebuilt(&h, &s));
}

/// Expired snapshots whose files are all gone are missing from history
/// altogether: catching up across the gap rebuilds.
#[test]
fn file_index_catching_up_across_missing_snapshots_rebuilds() {
    let h = Harness::new();
    let s = schema();
    ensure(&h, &s);
    let first = h.commit(&s, vec![inserted(row(1, 1)), inserted(row(2, 2))]);
    let mut fi = FileIndex::new();
    let at = catch_up(&h, &s, &mut fi, None);
    let delete = h.commit(&s, vec![deleted(2)]);
    let second = h.compact(&s, [first, delete].concat(), vec![row(1, 1)]);
    h.compact(&s, second, vec![row(1, 1)]);
    insert(&h, &s, row(6, 6));
    // Snapshots 1–3 expire (cutoff 5000 - 1500), every file they added
    // gone: history starts at 4.
    assert_eq!(block_on(h.cat.expire_snapshots(&ident(), 1500)).unwrap(), 3);
    let ids: Vec<i64> = block_on(h.cat.snapshots(&ident()))
        .unwrap()
        .iter()
        .map(|s| s.id)
        .collect();
    assert_eq!(ids, vec![4, 5]);

    assert_eq!(catch_up(&h, &s, &mut fi, at), Some(5));
    assert_eq!(fi, rebuilt(&h, &s));
}

/// A compaction planned before a row's update commits after it: its copy
/// of the old row stays deleted, so the key belongs to the update's file —
/// whether the index catches up or rebuilds.
#[test]
fn file_index_catches_up_across_a_compaction_planned_before_an_update() {
    let h = Harness::new();
    let s = schema();
    ensure(&h, &s);
    let first = h.commit(&s, vec![inserted(row(1, 1)), inserted(row(2, 2))]);
    let mut fi = FileIndex::new();
    let at = catch_up(&h, &s, &mut fi, None);
    // A pass plans at snapshot 1; row 1 is updated (snapshot 2) before it
    // commits (snapshot 3).
    let update = h.commit(&s, vec![deleted(1), inserted(row(1, 10))]);
    h.compact_planned_at(&s, first, vec![row(1, 1), row(2, 2)], Some(1));

    assert_eq!(catch_up(&h, &s, &mut fi, at), Some(3));
    let rebuilt = rebuilt(&h, &s);
    assert_eq!(fi, rebuilt);
    let key = pg2iceberg_iceberg::PkKey::from_row(&row(1, 10), &[ColumnName("id".into())]);
    assert_eq!(rebuilt.lookup(&key), Some(update[0].as_str()));
}
