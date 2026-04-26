//! Compaction: rewrite small data files + apply equality deletes inline,
//! emitting a single `Operation::Replace` snapshot.
//!
//! Mirrors `iceberg/compact.go`'s algorithm but routes new files through
//! `TableWriter::prepare` so partition-aware grouping is correct (Go's
//! reference dumps everything into one bucket — see
//! `project_compaction_partition_bug` memory).
//!
//! ## Algorithm
//!
//! 1. Load snapshot history; compute live data + delete files
//!    (cumulative across snapshots, skipping anything in
//!    `Snapshot.removed_paths`).
//! 2. Threshold check: bail with `None` if both file counts are below
//!    their thresholds.
//! 3. Read every live delete file → `delete_pks: PK → max delete-snap-seq`.
//! 4. Identify affected data files: those smaller than `target_size_bytes/2`,
//!    or those containing any deleted PK.
//! 5. Read affected data files → rows tagged with their source-snapshot seq.
//! 6. Dedup by PK, keeping the highest-seq row.
//! 7. Apply seq-aware deletes: drop rows where `delete_seq[pk] > row_seq`.
//!    Iceberg's spec: an equality delete with seq S applies to rows from
//!    files with seq < S. Higher-seq rows are immune.
//! 8. Run survivors through `TableWriter::prepare` — partition tuples are
//!    computed per row, files are grouped per partition, output is
//!    bit-identical to materializer-emitted commits.
//! 9. Upload each chunk; build `PreparedCompaction { added_data_files,
//!    removed_paths }` (removed = affected data + ALL delete files).
//! 10. `Catalog::commit_compaction` issues an `Operation::Replace`
//!     snapshot via the upstream `RewriteFilesAction`.
//!
//! ## Correctness rules we don't violate
//!
//! - **Partition awareness.** Output files are tagged with their
//!   partition tuple, courtesy of `TableWriter::prepare`. Mixed-partition
//!   files (Go's bug) cannot be produced by this code path because the
//!   writer fans rows out by partition.
//! - **Seq-aware deletes.** A delete with seq 5 cannot retroactively delete
//!   a row from snap 7. The dedup step keeps the highest-seq row;
//!   apply-deletes only drops it if `delete_seq > row_seq`.
//! - **No partial rewrites.** We rewrite a data file fully or not at all.
//!   Partial rewrites would lose rows.
//! - **Atomicity.** Either the entire `Replace` snapshot lands, or none
//!   of it. The catalog's commit path is the gate.

use crate::reader::read_data_file;
use crate::writer::{TableWriter, WriterError};
use crate::{Catalog, DataFile, FileIndex, IcebergError, PreparedCompaction, Snapshot};
use pg2iceberg_core::{ColumnName, ColumnSchema, Op, Row, TableIdent, TableSchema};
use pg2iceberg_stream::{BlobStore, StreamError};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use thiserror::Error;

/// Knobs that decide whether a table is worth compacting and how to size
/// the output. Mirrors Go's `iceberg/compact.go::CompactionConfig`.
#[derive(Clone, Debug)]
pub struct CompactionConfig {
    /// Trigger compaction when live data files >= this count.
    pub data_file_threshold: usize,
    /// Trigger compaction when live delete files >= this count. Delete
    /// files always get rewritten away when compaction runs (their PKs
    /// fold into the surviving rows), so even a single sufficiently-old
    /// delete is a reason to compact.
    pub delete_file_threshold: usize,
    /// Files smaller than `target_size_bytes / 2` are eligible for
    /// rewrite. Default in Go is `128 MiB / 2 = 64 MiB`.
    pub target_size_bytes: u64,
}

impl Default for CompactionConfig {
    fn default() -> Self {
        Self {
            data_file_threshold: 8,
            delete_file_threshold: 4,
            target_size_bytes: 128 * 1024 * 1024,
        }
    }
}

/// Stats about a successful compaction. The materializer / control plane
/// surfaces these for observability and to populate the `compactions`
/// metadata table.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct CompactionOutcome {
    pub input_data_files: usize,
    pub input_delete_files: usize,
    pub output_data_files: usize,
    pub rows_rewritten: u64,
    pub rows_removed_by_deletes: u64,
    pub bytes_before: u64,
    pub bytes_after: u64,
}

#[derive(Debug, Error)]
pub enum CompactError {
    #[error("catalog: {0}")]
    Catalog(#[from] IcebergError),
    #[error("blob: {0}")]
    Blob(#[from] StreamError),
    #[error("writer: {0}")]
    Writer(#[from] WriterError),
}

pub type Result<T> = std::result::Result<T, CompactError>;

/// One live file's metadata: byte size + the snapshot sequence it was
/// added in. Used both for size-thresholding and for seq-aware delete
/// application.
#[derive(Clone, Debug)]
struct LiveFile {
    byte_size: u64,
    /// Snapshot seq the file was added in. Iceberg attaches sequence
    /// numbers to manifest entries; for files added via `FastAppend`
    /// the entry's seq matches the snapshot's seq, which we surface as
    /// `Snapshot.id`. For files inherited through carry-forward this
    /// remains the *original* add seq — but our `snapshots()` impl
    /// only surfaces files under their first-added snapshot, so the
    /// snapshot we found a path in IS the snapshot it was added in.
    seq: i64,
}

/// Run a compaction cycle. Returns `Ok(None)` if the table is below
/// thresholds (no work needed), `Ok(Some(_))` after a successful
/// compaction commit.
///
/// `path_for_chunk` is an async closure called per output chunk; the
/// caller is responsible for producing globally-unique paths (e.g. via
/// UUID). `(table_ident, chunk_index)` is passed in. Async-shaped to
/// match the materializer's existing async-trait `MaterializerNamer`,
/// which generates UUID-suffixed paths via an async `IdGen`.
pub async fn compact_table<C, F, Fut>(
    catalog: &C,
    blob_store: &dyn BlobStore,
    path_for_chunk: F,
    ident: &TableIdent,
    schema: &TableSchema,
    pk_cols: &[ColumnName],
    config: &CompactionConfig,
) -> Result<Option<CompactionOutcome>>
where
    C: Catalog + ?Sized,
    F: Fn(&TableIdent, usize) -> Fut,
    Fut: std::future::Future<Output = String>,
{
    let snapshots = catalog.snapshots(ident).await?;
    if snapshots.is_empty() {
        return Ok(None);
    }

    let (live_data, live_deletes) = compute_live_files(&snapshots);

    if live_data.len() < config.data_file_threshold
        && live_deletes.len() < config.delete_file_threshold
    {
        return Ok(None);
    }

    // Read every live delete file → delete_pks: PK → max delete-snap-seq.
    let pk_schema: Vec<ColumnSchema> = schema
        .columns
        .iter()
        .filter(|c| c.is_primary_key)
        .cloned()
        .collect();
    let mut delete_pks: HashMap<String, i64> = HashMap::new();
    for (path, meta) in &live_deletes {
        let bytes = blob_store.get(path).await?;
        let rows = read_data_file(&bytes, &pk_schema)?;
        for row in rows {
            let key = crate::fold::pk_key(&row, pk_cols);
            // Highest seq wins — covers re-deletes of the same PK.
            delete_pks
                .entry(key)
                .and_modify(|e| {
                    if meta.seq > *e {
                        *e = meta.seq
                    }
                })
                .or_insert(meta.seq);
        }
    }

    // Affected data files: small ones + any containing a deleted PK.
    let small_threshold = config.target_size_bytes / 2;
    let mut affected: BTreeMap<String, LiveFile> = BTreeMap::new();
    for (path, meta) in &live_data {
        if meta.byte_size < small_threshold {
            affected.insert(path.clone(), meta.clone());
        }
    }
    if !delete_pks.is_empty() {
        for (path, meta) in &live_data {
            if affected.contains_key(path) {
                continue;
            }
            let bytes = blob_store.get(path).await?;
            let rows = read_data_file(&bytes, &schema.columns)?;
            if rows
                .iter()
                .any(|r| delete_pks.contains_key(&crate::fold::pk_key(r, pk_cols)))
            {
                affected.insert(path.clone(), meta.clone());
            }
        }
    }

    if affected.is_empty() && live_deletes.is_empty() {
        return Ok(None);
    }

    // Read affected files → rows tagged with their snapshot seq.
    let mut rows_with_seq: Vec<(Row, i64)> = Vec::new();
    let mut bytes_before: u64 = 0;
    for (path, meta) in &affected {
        bytes_before += meta.byte_size;
        let bytes = blob_store.get(path).await?;
        let rows = read_data_file(&bytes, &schema.columns)?;
        for row in rows {
            rows_with_seq.push((row, meta.seq));
        }
    }
    for meta in live_deletes.values() {
        bytes_before += meta.byte_size;
    }
    let total_input_rows = rows_with_seq.len() as u64;

    // Dedup by PK keeping highest seq.
    let mut dedup: BTreeMap<String, (Row, i64)> = BTreeMap::new();
    for (row, seq) in rows_with_seq {
        let key = crate::fold::pk_key(&row, pk_cols);
        match dedup.get(&key) {
            Some((_, existing_seq)) if *existing_seq >= seq => {
                // Keep the older entry — current one is stale.
            }
            _ => {
                dedup.insert(key, (row, seq));
            }
        }
    }

    let after_dedup = dedup.len();
    // Apply seq-aware deletes: drop iff delete_seq > row_seq.
    dedup.retain(|key, (_, row_seq)| match delete_pks.get(key) {
        Some(del_seq) => del_seq <= row_seq,
        None => true,
    });
    let rows_removed_by_deletes = (after_dedup - dedup.len()) as u64;

    // Wrap survivors as Insert ops for TableWriter.
    let materialized: Vec<crate::fold::MaterializedRow> = dedup
        .into_values()
        .map(|(row, _)| crate::fold::MaterializedRow {
            op: Op::Insert,
            row,
            unchanged_cols: vec![],
        })
        .collect();

    // If no survivors AND no removals, nothing to commit. (e.g. all rows
    // deleted but we don't have any data files to remove either.)
    if materialized.is_empty() && affected.is_empty() && live_deletes.is_empty() {
        return Ok(None);
    }

    let writer = TableWriter::new(schema.clone());
    let prepared = writer.prepare(&materialized, &FileIndex::new())?;

    // Upload compacted chunks; build added DataFiles.
    let mut added_files: Vec<DataFile> = Vec::with_capacity(prepared.data.len());
    let mut bytes_after: u64 = 0;
    for (i, chunk) in prepared.data.into_iter().enumerate() {
        let path = path_for_chunk(ident, i).await;
        let byte_size = chunk.chunk.bytes.len() as u64;
        bytes_after += byte_size;
        blob_store
            .put(&path, bytes::Bytes::clone(&chunk.chunk.bytes))
            .await?;
        added_files.push(DataFile {
            path,
            record_count: chunk.chunk.record_count,
            byte_size,
            equality_field_ids: vec![],
            partition_values: chunk.partition_values,
        });
    }

    // removed_paths = affected data files + every delete file (deletes
    // are folded into surviving data, no longer needed).
    let mut removed_paths: Vec<String> = affected.keys().cloned().collect();
    removed_paths.extend(live_deletes.keys().cloned());
    removed_paths.sort();
    removed_paths.dedup();

    catalog
        .commit_compaction(PreparedCompaction {
            ident: ident.clone(),
            added_data_files: added_files.clone(),
            removed_paths,
        })
        .await?;

    Ok(Some(CompactionOutcome {
        input_data_files: affected.len(),
        input_delete_files: live_deletes.len(),
        output_data_files: added_files.len(),
        rows_rewritten: total_input_rows,
        rows_removed_by_deletes,
        bytes_before,
        bytes_after,
    }))
}

/// Walk the snapshot history and compute (data, deletes) live now: each
/// map is `path -> (byte_size, seq)`. Files in any snapshot's
/// `removed_paths` are excluded.
fn compute_live_files(
    snapshots: &[Snapshot],
) -> (BTreeMap<String, LiveFile>, BTreeMap<String, LiveFile>) {
    let removed: BTreeSet<&str> = snapshots
        .iter()
        .flat_map(|s| s.removed_paths.iter().map(String::as_str))
        .collect();

    let mut data = BTreeMap::new();
    let mut deletes = BTreeMap::new();
    for snap in snapshots {
        for df in &snap.data_files {
            if removed.contains(df.path.as_str()) {
                continue;
            }
            data.insert(
                df.path.clone(),
                LiveFile {
                    byte_size: df.byte_size,
                    seq: snap.id,
                },
            );
        }
        for df in &snap.delete_files {
            if removed.contains(df.path.as_str()) {
                continue;
            }
            deletes.insert(
                df.path.clone(),
                LiveFile {
                    byte_size: df.byte_size,
                    seq: snap.id,
                },
            );
        }
    }
    (data, deletes)
}

// End-to-end compaction tests live in `pg2iceberg-tests/tests/compact_e2e.rs`
// — they need both `MemoryCatalog` (which depends on this crate, so cycling
// it as a dev-dep is impossible) and a `BlobStore` impl. Keep helper-level
// unit tests here.
#[cfg(test)]
mod tests {
    use super::*;

    fn data_file(path: &str, byte_size: u64) -> DataFile {
        DataFile {
            path: path.into(),
            record_count: 1,
            byte_size,
            equality_field_ids: vec![],
            partition_values: vec![],
        }
    }

    fn snap(id: i64, data: Vec<DataFile>, deletes: Vec<DataFile>, removed: Vec<&str>) -> Snapshot {
        Snapshot {
            id,
            data_files: data,
            delete_files: deletes,
            removed_paths: removed.into_iter().map(String::from).collect(),
            timestamp_ms: id * 1000,
        }
    }

    #[test]
    fn compute_live_files_excludes_removed_paths() {
        let snaps = vec![
            snap(
                1,
                vec![data_file("a.parquet", 100), data_file("b.parquet", 200)],
                vec![],
                vec![],
            ),
            // Snap 2 is a compaction that supersedes a.parquet.
            snap(
                2,
                vec![data_file("compact-0.parquet", 350)],
                vec![],
                vec!["a.parquet"],
            ),
        ];
        let (data, deletes) = compute_live_files(&snaps);
        assert!(!data.contains_key("a.parquet"), "a should be removed");
        assert!(data.contains_key("b.parquet"));
        assert!(data.contains_key("compact-0.parquet"));
        assert_eq!(data["b.parquet"].seq, 1);
        assert_eq!(data["compact-0.parquet"].seq, 2);
        assert!(deletes.is_empty());
    }

    #[test]
    fn compute_live_files_seq_matches_source_snapshot() {
        let snaps = vec![
            snap(1, vec![data_file("a.parquet", 100)], vec![], vec![]),
            snap(2, vec![data_file("b.parquet", 200)], vec![], vec![]),
            snap(3, vec![], vec![data_file("eq-1.parquet", 50)], vec![]),
        ];
        let (data, deletes) = compute_live_files(&snaps);
        assert_eq!(data["a.parquet"].seq, 1);
        assert_eq!(data["b.parquet"].seq, 2);
        assert_eq!(deletes["eq-1.parquet"].seq, 3);
    }

    #[test]
    fn compute_live_files_drops_delete_files_in_removed_paths() {
        let snaps = vec![
            snap(1, vec![data_file("a.parquet", 100)], vec![], vec![]),
            snap(2, vec![], vec![data_file("eq-1.parquet", 50)], vec![]),
            // Snap 3 compacts: drops a.parquet + eq-1.parquet, adds b.parquet.
            snap(
                3,
                vec![data_file("b.parquet", 80)],
                vec![],
                vec!["a.parquet", "eq-1.parquet"],
            ),
        ];
        let (data, deletes) = compute_live_files(&snaps);
        assert!(data.contains_key("b.parquet"));
        assert!(!data.contains_key("a.parquet"));
        assert!(deletes.is_empty(), "delete file should be removed");
    }

    #[test]
    fn config_default_thresholds_are_reasonable() {
        let c = CompactionConfig::default();
        assert!(c.data_file_threshold >= 4);
        assert!(c.delete_file_threshold >= 1);
        assert!(c.target_size_bytes >= 16 * 1024 * 1024);
    }
}
