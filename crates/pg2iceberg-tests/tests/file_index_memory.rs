//! Memory held by the materializer's `FileIndex`, measured with a
//! counting global allocator.
//!
//! The index holds one entry per live row, so it grows with the table;
//! what these tests bound is the cost per row, and that rebuilding it
//! on startup costs no more than the index itself — not the table's
//! update history.
//!
//! The allocator counts every thread, so the tests in this binary take
//! a lock and run one at a time.

use bytes::Bytes;
use peak_alloc::PeakAlloc;
use pg2iceberg_core::typemap::IcebergType;
use pg2iceberg_core::value::PgValue;
use pg2iceberg_core::{ColumnName, ColumnSchema, Namespace, Op, Row, TableIdent, TableSchema};
use pg2iceberg_iceberg::{
    fold::MaterializedRow, rebuild_from_catalog, Catalog, DataFile, FileIndex, PreparedCommit,
    TableWriter,
};
use pg2iceberg_sim::blob::MemoryBlobStore;
use pg2iceberg_sim::catalog::MemoryCatalog;
use pg2iceberg_stream::BlobStore;
use pollster::block_on;
use std::collections::BTreeMap;
use std::sync::Mutex;

#[global_allocator]
static ALLOC: PeakAlloc = PeakAlloc;

static ONE_AT_A_TIME: Mutex<()> = Mutex::new(());

/// Runs `f`; returns its result with the bytes it left allocated and
/// the most it had allocated at once.
fn measure<T>(f: impl FnOnce() -> T) -> (T, usize, usize) {
    let base = ALLOC.current_usage();
    ALLOC.reset_peak_usage();
    let out = f();
    let retained = ALLOC.current_usage().saturating_sub(base);
    let peak = ALLOC.peak_usage().saturating_sub(base);
    (out, retained, peak)
}

/// Live rows in each table.
const ROWS: usize = 40_000;
/// Rows per data file.
const FILE_ROWS: usize = 4_000;
/// Times every row is updated after the initial load.
const UPDATE_ROUNDS: usize = 3;
/// Index bytes per live row. A map entry is a compact key plus a file
/// number; even right after the hash table doubles this leaves room.
const MAX_BYTES_PER_ROW: usize = 96;
/// Extra bytes a rebuild may use beyond the index: one decoded batch
/// and one encoded file.
const REBUILD_SLACK: usize = 1 << 20;

fn ident() -> TableIdent {
    TableIdent {
        namespace: Namespace(vec!["public".into()]),
        name: "orders".into(),
    }
}

fn schema(pk_ty: IcebergType) -> TableSchema {
    TableSchema {
        ident: ident(),
        columns: vec![
            ColumnSchema {
                name: "id".into(),
                field_id: 1,
                ty: pk_ty,
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

fn pk_cols() -> Vec<ColumnName> {
    vec![ColumnName("id".into())]
}

fn row(id: PgValue, qty: i32) -> Row {
    let mut r = BTreeMap::new();
    r.insert(ColumnName("id".into()), id);
    r.insert(ColumnName("qty".into()), PgValue::Int4(qty));
    r
}

/// A table of `ROWS` live rows whose every row was then updated
/// `UPDATE_ROUNDS` times — each update an equality delete plus a new
/// data row, as the materializer writes them.
fn table_with_history(
    pk_ty: IcebergType,
    id: impl Fn(usize) -> PgValue,
) -> (MemoryCatalog, MemoryBlobStore, TableSchema) {
    let cat = MemoryCatalog::new();
    let blob = MemoryBlobStore::new();
    let schema = schema(pk_ty);
    block_on(cat.ensure_namespace(&ident().namespace)).unwrap();
    block_on(cat.create_table(&schema)).unwrap();
    let writer = TableWriter::new(schema.clone());
    let mut n = 0;
    for round in 0..=UPDATE_ROUNDS {
        let op = if round == 0 { Op::Insert } else { Op::Update };
        for start in (0..ROWS).step_by(FILE_ROWS) {
            let rows: Vec<MaterializedRow> = (start..start + FILE_ROWS)
                .map(|i| MaterializedRow {
                    op,
                    row: row(id(i), round as i32),
                    unchanged_cols: vec![],
                    unchanged_from: None,
                })
                .collect();
            let prepared = writer.prepare(&rows, &FileIndex::new()).unwrap();
            let mut put = |kind: &str, bytes: &Bytes| {
                n += 1;
                // A realistic warehouse path: the index stores paths too.
                let path = format!(
                    "s3://warehouse/public/orders/{kind}/{n:08}-0000-4000-8000-000000000000.parquet"
                );
                block_on(blob.put(&path, Bytes::clone(bytes))).unwrap();
                path
            };
            let data_files = prepared
                .data
                .iter()
                .map(|c| DataFile {
                    path: put("data", &c.chunk.bytes),
                    record_count: c.chunk.record_count,
                    byte_size: c.chunk.bytes.len() as u64,
                    equality_field_ids: vec![],
                    partition_values: c.partition_values.clone(),
                    sequence_number: None,
                })
                .collect();
            let equality_deletes = prepared
                .equality_deletes
                .iter()
                .map(|c| DataFile {
                    path: put("eq-delete", &c.chunk.bytes),
                    record_count: c.chunk.record_count,
                    byte_size: c.chunk.bytes.len() as u64,
                    equality_field_ids: prepared.pk_field_ids.clone(),
                    partition_values: c.partition_values.clone(),
                    sequence_number: None,
                })
                .collect();
            block_on(cat.commit_snapshot(PreparedCommit {
                ident: ident(),
                data_files,
                equality_deletes,
            }))
            .unwrap();
        }
    }
    (cat, blob, schema)
}

fn assert_bounded(pk_ty: IcebergType, id: impl Fn(usize) -> PgValue) {
    let _one = ONE_AT_A_TIME.lock().unwrap_or_else(|e| e.into_inner());
    let (cat, blob, schema) = table_with_history(pk_ty, id);
    let (index, retained, peak) = measure(|| {
        block_on(rebuild_from_catalog(
            &cat,
            &blob,
            &ident(),
            &schema,
            &pk_cols(),
        ))
        .unwrap()
    });
    assert_eq!(index.live_pk_count(), ROWS);
    let per_row = retained / ROWS;
    assert!(
        per_row <= MAX_BYTES_PER_ROW,
        "FileIndex holds {per_row} B per live row ({retained} B for {ROWS} rows), \
         over {MAX_BYTES_PER_ROW}; the rebuild peaked at {peak} B"
    );
    // The rebuild reads every live delete file, but what it holds must
    // not grow with them: at most the index plus one file's worth.
    let budget = ROWS * MAX_BYTES_PER_ROW + REBUILD_SLACK;
    assert!(
        peak <= budget,
        "rebuilding a {ROWS}-row index peaked at {peak} B, over {budget} B \
         (index alone: {retained} B)"
    );
}

#[test]
fn bigint_key_index_is_compact() {
    assert_bounded(IcebergType::Long, |i| PgValue::Int8(i as i64 * 7_919));
}

#[test]
fn uuid_key_index_is_compact() {
    assert_bounded(IcebergType::Uuid, |i| {
        let mut b = [0u8; 16];
        b[..8].copy_from_slice(&(i as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15).to_be_bytes());
        b[8..].copy_from_slice(&(i as u64).to_le_bytes());
        PgValue::Uuid(b)
    });
}
