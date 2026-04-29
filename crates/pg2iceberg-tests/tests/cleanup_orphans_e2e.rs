//! End-to-end orphan-file cleanup tests.
//!
//! Walks the same path the `pg2iceberg maintain` subcommand uses:
//! [`cleanup_orphans`](pg2iceberg_iceberg::cleanup_orphans) lists blobs
//! under a prefix, diffs against the catalog's snapshot history, and
//! deletes orphans older than the grace period. We exercise it against
//! `MemoryCatalog` + `MemoryBlobStore`.
//!
//! Phase F adds three protections:
//! - **Reference preservation**: a blob still pointed to by some live
//!   snapshot is never deleted, even if it's outside the freshly-set
//!   grace window.
//! - **Grace-period protection**: a brand-new orphan (e.g. just-uploaded
//!   crashed mid-cycle) gets a window before it can be deleted, so a
//!   compaction commit racing with cleanup doesn't lose data.
//! - **Compaction-replaced files**: post-compaction, the old data files'
//!   paths show up in `Snapshot.removed_paths`, marking them
//!   unreferenced. They become eligible orphans.

use bytes::Bytes;
use pg2iceberg_core::typemap::IcebergType;
use pg2iceberg_core::value::PgValue;
use pg2iceberg_core::{ColumnName, ColumnSchema, Namespace, Op, Row, TableIdent, TableSchema};
use pg2iceberg_iceberg::{
    cleanup_orphans, fold::MaterializedRow, Catalog, DataFile, FileIndex, PreparedCommit,
    PreparedCompaction, TableWriter,
};
use pg2iceberg_sim::blob::MemoryBlobStore;
use pg2iceberg_sim::catalog::MemoryCatalog;
use pg2iceberg_stream::BlobStore;
use pollster::block_on;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

const PREFIX: &str = "data/orders/";

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

    /// Append a single row to a fresh snapshot via the materializer-style
    /// path (writer → blob put → catalog commit). Returns the data
    /// file's path.
    fn commit_row(&self, schema: &TableSchema, r: Row) -> String {
        let writer = TableWriter::new(schema.clone());
        let prepared = writer
            .prepare(
                &[MaterializedRow {
                    op: Op::Insert,
                    row: r,
                    unchanged_cols: vec![],
                }],
                &FileIndex::new(),
            )
            .unwrap();
        let chunk = prepared.data.into_iter().next().unwrap();
        let n = self.counter.fetch_add(1, Ordering::SeqCst);
        let path = format!("{PREFIX}data-{n}.parquet");
        block_on(self.blob.put(&path, Bytes::clone(&chunk.chunk.bytes))).unwrap();
        block_on(self.cat.commit_snapshot(PreparedCommit {
            ident: schema.ident.clone(),
            data_files: vec![DataFile {
                path: path.clone(),
                record_count: chunk.chunk.record_count,
                byte_size: chunk.chunk.bytes.len() as u64,
                equality_field_ids: vec![],
                partition_values: chunk.partition_values,
            }],
            equality_deletes: vec![],
        }))
        .unwrap();
        path
    }

    /// Write a blob outside the catalog's tracking — simulates a crashed
    /// upload, a foreign writer, or a leftover from a failed commit.
    fn write_orphan(&self, path: &str) {
        block_on(self.blob.put(path, Bytes::from_static(b"orphan"))).unwrap();
    }
}

fn ensure(h: &Harness, s: &TableSchema) {
    block_on(h.cat.ensure_namespace(&s.ident.namespace)).unwrap();
    block_on(h.cat.create_table(s)).unwrap();
}

#[test]
fn unreferenced_blobs_outside_grace_window_get_deleted() {
    let h = Harness::new();
    let s = schema();
    ensure(&h, &s);
    h.commit_row(&s, row(1, 10));
    // Sim's MemoryBlobStore stamps last_modified_ms from a counter
    // starting at 0; both commits above are at ms 0..N. Now jump the
    // clock so subsequent puts look "newer".
    h.blob.set_clock_ms(1000);
    h.write_orphan(&format!("{PREFIX}orphan-old.parquet"));
    // Bump clock so this orphan is past the grace window when we
    // run cleanup at now=2000 with grace=500.
    h.blob.set_clock_ms(2500);

    let outcome = block_on(cleanup_orphans(
        &h.cat,
        h.blob.as_ref(),
        &ident(),
        PREFIX,
        2500, // now
        500,  // grace
    ))
    .unwrap();
    // The committed row's data file is referenced; the orphan at ms=1000
    // is older than cutoff (2000), so it deletes.
    assert_eq!(outcome.deleted, 1);
    assert!(outcome.bytes_freed > 0);
    assert_eq!(outcome.grace_protected, 0);

    // Referenced data file is still present.
    let remaining: Vec<String> = h.blob.paths();
    assert_eq!(remaining.len(), 1);
    assert!(
        remaining[0].starts_with(&format!("{PREFIX}data-")),
        "the surviving blob should be the committed data file, got {:?}",
        remaining[0]
    );
}

#[test]
fn fresh_orphans_are_grace_protected() {
    let h = Harness::new();
    let s = schema();
    ensure(&h, &s);
    h.commit_row(&s, row(1, 10));
    // Orphan written "now" — within the grace window.
    h.blob.set_clock_ms(1000);
    h.write_orphan(&format!("{PREFIX}orphan-fresh.parquet"));

    let outcome = block_on(cleanup_orphans(
        &h.cat,
        h.blob.as_ref(),
        &ident(),
        PREFIX,
        1000, // now == orphan's mtime
        500,  // grace; cutoff = 500, orphan at 1000 > 500
    ))
    .unwrap();
    assert_eq!(outcome.deleted, 0, "fresh orphans must be protected");
    assert_eq!(outcome.grace_protected, 1);

    // Both paths still present.
    assert_eq!(h.blob.paths().len(), 2);
}

#[test]
fn referenced_blobs_are_never_deleted() {
    let h = Harness::new();
    let s = schema();
    ensure(&h, &s);
    let path = h.commit_row(&s, row(1, 10));

    // Even with grace=0 and far-future now, the referenced blob stays.
    let outcome = block_on(cleanup_orphans(
        &h.cat,
        h.blob.as_ref(),
        &ident(),
        PREFIX,
        i64::MAX / 2,
        0,
    ))
    .unwrap();
    assert_eq!(outcome.deleted, 0);
    assert!(h.blob.paths().contains(&path));
}

#[test]
fn paths_outside_prefix_are_ignored() {
    let h = Harness::new();
    let s = schema();
    ensure(&h, &s);
    h.commit_row(&s, row(1, 10));

    // Write something under a different prefix; cleanup shouldn't touch it.
    block_on(
        h.blob
            .put("staged/somewhere/else.parquet", Bytes::from_static(b"x")),
    )
    .unwrap();

    let outcome = block_on(cleanup_orphans(
        &h.cat,
        h.blob.as_ref(),
        &ident(),
        PREFIX,
        i64::MAX / 2,
        0,
    ))
    .unwrap();
    assert_eq!(outcome.deleted, 0);
    assert!(h
        .blob
        .paths()
        .contains(&"staged/somewhere/else.parquet".to_string()));
}

#[test]
fn compaction_replaced_files_become_orphans() {
    // Three small commits, then compact them to one. The original three
    // files end up in `removed_paths` of the compaction snapshot, so
    // cleanup recognizes them as orphans.
    let h = Harness::new();
    let s = schema();
    ensure(&h, &s);
    let p1 = h.commit_row(&s, row(1, 10));
    let p2 = h.commit_row(&s, row(2, 20));
    let p3 = h.commit_row(&s, row(3, 30));

    // Manually issue a compaction snapshot dropping the three originals
    // and adding one compacted file. (Real compaction would rewrite the
    // rows; here we just need the metadata to say "p1..p3 are gone".)
    let compacted_path = format!("{PREFIX}data-compact.parquet");
    block_on(
        h.blob
            .put(&compacted_path, Bytes::from_static(b"compacted")),
    )
    .unwrap();
    block_on(h.cat.commit_compaction(PreparedCompaction {
        ident: ident(),
        added_data_files: vec![DataFile {
            path: compacted_path.clone(),
            record_count: 3,
            byte_size: 9,
            equality_field_ids: vec![],
            partition_values: vec![],
        }],
        removed_paths: vec![p1.clone(), p2.clone(), p3.clone()],
    }))
    .unwrap();

    // Cleanup with grace=0 → all three originals delete; the compacted
    // file stays.
    let outcome = block_on(cleanup_orphans(
        &h.cat,
        h.blob.as_ref(),
        &ident(),
        PREFIX,
        i64::MAX / 2,
        0,
    ))
    .unwrap();
    assert_eq!(outcome.deleted, 3);

    let surviving: Vec<String> = h.blob.paths();
    assert_eq!(surviving.len(), 1);
    assert_eq!(surviving[0], compacted_path);
}

#[test]
fn cleanup_with_no_orphans_is_a_noop() {
    let h = Harness::new();
    let s = schema();
    ensure(&h, &s);
    h.commit_row(&s, row(1, 10));

    let outcome = block_on(cleanup_orphans(
        &h.cat,
        h.blob.as_ref(),
        &ident(),
        PREFIX,
        i64::MAX / 2,
        0,
    ))
    .unwrap();
    assert_eq!(outcome.deleted, 0);
    assert_eq!(outcome.bytes_freed, 0);
    assert_eq!(outcome.grace_protected, 0);
}
