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
    fold::MaterializedRow, Catalog, DataFile, FileIndex, PreparedCommit, TableWriter,
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

    fn commit(&self, schema: &TableSchema, rows: Vec<MaterializedRow>) {
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
        block_on(self.cat.commit_snapshot(PreparedCommit {
            ident: schema.ident.clone(),
            data_files,
            equality_deletes: vec![],
        }))
        .unwrap();
    }
}

fn ensure(h: &Harness, s: &TableSchema) {
    block_on(h.cat.ensure_namespace(&s.ident.namespace)).unwrap();
    block_on(h.cat.create_table(s)).unwrap();
}

fn insert(h: &Harness, s: &TableSchema, r: Row) {
    h.commit(
        s,
        vec![MaterializedRow {
            op: Op::Insert,
            row: r,
            unchanged_cols: vec![],
        }],
    );
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

    let snaps = block_on(h.cat.snapshots(&ident())).unwrap();
    assert_eq!(snaps.len(), 3, "3 snapshots survive");
    let ids: Vec<i64> = snaps.iter().map(|s| s.id).collect();
    assert_eq!(ids, vec![3, 4, 5]);
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
    assert_eq!(ids, vec![3, 4, 5, 6]);

    // Note: in our sim model, `Snapshot.data_files` is "files added in
    // this snapshot" rather than the full live set, so expiring a
    // snapshot drops its added files from the verifier's view. Real
    // iceberg's manifest lists carry forward live files into every
    // snapshot, so post-expiry the current snapshot's manifest list
    // still references everything that hasn't been compacted away. The
    // prod-side `IcebergRustCatalog::expire_snapshots` uses
    // `TableUpdate::RemoveSnapshots`, which preserves visibility
    // correctly. We don't assert post-expiry row visibility against the
    // sim because it would test the sim's representation, not the
    // expiry-trait contract.
    let visible = block_on(pg2iceberg_iceberg::read_materialized_state(
        &h.cat,
        h.blob.as_ref(),
        &ident(),
        &s,
        &[ColumnName("id".into())],
    ))
    .unwrap();
    // Only post-expire snapshots' added rows are visible (PKs 3, 4, 5, 6).
    assert_eq!(visible.len(), 4);
}
