//! Compaction: rewrite small data files + apply equality deletes inline,
//! emitting a single `Operation::Replace` snapshot.
//!
//! Mirrors `iceberg/compact.go`'s algorithm but derives each output row's
//! partition through `TableWriter` so partition-aware grouping is correct
//! (Go's reference dumps everything into one bucket — see
//! `project_compaction_partition_bug` memory).
//!
//! ## Algorithm
//!
//! 1. Load snapshot history; compute live data + delete files
//!    (cumulative across snapshots, skipping anything in
//!    `Snapshot.removed_paths`).
//! 2. Threshold check: bail with `None` if both file counts are below
//!    their thresholds.
//! 3. Plan the pass ([`plan_pass`]): candidates are small files
//!    (< `target_size_bytes / 2`) and *dirty* files — ones holding rows a
//!    live delete has killed. Oldest first, take candidates until
//!    `max_input_bytes_per_pass`; the rest wait for a later pass.
//! 4. Retire every delete file no remaining dirty file could still need.
//! 5. Load deleted PKs (`PK → max delete seq`) from the delete files that
//!    can apply to an input.
//! 6. Stream the inputs one record batch at a time: drop rows a delete
//!    applies to (`delete_seq > row_seq`), encode survivors straight into
//!    the open output file, and upload it once it reaches the target size.
//! 7. `Catalog::commit_compaction` issues one `Operation::Replace`
//!    snapshot removing the inputs + retired deletes and adding outputs.
//!
//! ## Memory
//!
//! At most one decoded record batch ([`DECODE_BATCH_ROWS`]) of one input
//! file is alive at a time, next to that file's encoded bytes and the
//! encoded bytes of one open output file (≈ `target_size_bytes`). What
//! grows with the table instead: the live-file lists (O(files)), the
//! deleted-PK set (O(live equality-delete rows) — what every MoR reader
//! holds anyway, and retiring deletes keeps it down) and the caller's
//! `FileIndex` (already resident in the materializer).
//!
//! ## Correctness rules we don't violate
//!
//! - **Partition awareness.** An output file holds one partition tuple,
//!   computed per row by `TableWriter`; a row of another tuple closes the
//!   open file first.
//! - **Seq-aware deletes.** A delete with seq 5 cannot retroactively delete
//!   a row from snap 7: a row is dropped only if `delete_seq > row_seq`.
//! - **Rewritten rows keep their verdict.** Outputs keep the data sequence
//!   number of the snapshot the pass read
//!   ([`PreparedCompaction::data_sequence_number`]), at or above every
//!   delete it saw. So none of those applies to a rewritten row — which is
//!   why each one that applies to an input row is applied here, before the
//!   row moves. A delete committed while the pass ran is above it and
//!   still applies, as it would have to the input; at the Replace
//!   snapshot's own number, the deleted row would come back.
//! - **A delete outlives every row it may apply to.** A delete with seq
//!   `t` is retired only when no dirty data file with seq `< t` remains
//!   after this pass. Clean files don't hold it back: by definition no
//!   live delete applies to any of their rows.
//! - **No partial rewrites.** We rewrite a data file fully or not at all.
//!   Partial rewrites would lose rows.
//! - **Atomicity.** Either the entire `Replace` snapshot lands, or none
//!   of it. The catalog's commit path is the gate.

use crate::reader::RowBatches;
use crate::writer::{StreamingDataFile, TableWriter, WriterError};
use crate::{Catalog, DataFile, FileIndex, IcebergError, PkKey, PreparedCompaction, Snapshot};
use pg2iceberg_core::{ColumnName, ColumnSchema, PartitionLiteral, Row, TableIdent, TableSchema};
use pg2iceberg_stream::{BlobStore, StreamError};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use thiserror::Error;

/// Rows decoded per record batch. Bounds how many rows a pass holds at
/// once, however large its input files are.
pub const DECODE_BATCH_ROWS: usize = 1024;

/// Knobs that decide whether a table is worth compacting and how to
/// size the output.
#[derive(Clone, Debug)]
pub struct CompactionConfig {
    /// Trigger compaction when live data files >= this count.
    pub data_file_threshold: usize,
    /// Trigger compaction when live delete files >= this count. A delete
    /// file is dropped once no data file it could apply to remains, so
    /// even a single sufficiently-old delete is a reason to compact.
    pub delete_file_threshold: usize,
    /// Files smaller than `target_size_bytes / 2` are eligible for
    /// rewrite, and output files roll over at this size. Default
    /// 128 MiB target → 64 MiB minimum.
    pub target_size_bytes: u64,
    /// Most data-file bytes one pass rewrites. Bounds a pass's I/O and
    /// duration (memory is bounded regardless); the remaining candidates
    /// wait for later passes. A pass always takes at least one file, or
    /// two when merging clean ones, so it makes progress.
    pub max_input_bytes_per_pass: u64,
}

impl CompactionConfig {
    /// Default pass budget, in target-size files.
    pub const DEFAULT_PASS_TARGET_FILES: u64 = 4;
}

impl Default for CompactionConfig {
    fn default() -> Self {
        let target_size_bytes = 128 * 1024 * 1024;
        Self {
            data_file_threshold: 8,
            delete_file_threshold: 4,
            target_size_bytes,
            max_input_bytes_per_pass: target_size_bytes * Self::DEFAULT_PASS_TARGET_FILES,
        }
    }
}

/// Stats about a successful compaction. The materializer / control plane
/// surfaces these for observability and to populate the `compactions`
/// metadata table.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct CompactionOutcome {
    pub input_data_files: usize,
    /// Delete files dropped by this pass.
    pub input_delete_files: usize,
    pub output_data_files: usize,
    pub rows_rewritten: u64,
    pub rows_removed_by_deletes: u64,
    pub bytes_before: u64,
    pub bytes_after: u64,
    /// Most decoded rows held in memory at once during the pass.
    pub peak_rows_in_memory: u64,
    /// Candidate files left over for a later pass by the input budget.
    pub pending_files: usize,
    /// Data files the pass rewrote (now removed from the table).
    pub rewritten_files: Vec<String>,
    /// Files the pass added, with the PKs each holds — so the caller can
    /// update its `FileIndex` without replaying the table. Bounded by the
    /// pass budget, like the rest of the pass.
    pub added_files: Vec<CompactedFile>,
    /// The table's current snapshot after the pass's commit.
    pub snapshot_id: Option<i64>,
}

/// One output file of a compaction pass.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct CompactedFile {
    pub path: String,
    pub pk_keys: Vec<PkKey>,
    pub partition_values: Vec<PartitionLiteral>,
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

/// One live file's manifest metadata.
#[derive(Clone, Debug)]
struct LiveFile {
    byte_size: u64,
    record_count: u64,
    partition_values: Vec<PartitionLiteral>,
    /// The file's data sequence number ([`DataFile::sequence_number_in`]):
    /// its snapshot's — `snapshots()` lists a file under the snapshot that
    /// added it — or, for a compaction's output, the one its pass read.
    seq: i64,
}

impl LiveFile {
    fn new(df: &DataFile, seq: i64) -> Self {
        Self {
            byte_size: df.byte_size,
            record_count: df.record_count,
            partition_values: df.partition_values.clone(),
            seq,
        }
    }
}

/// What one pass does: which data files it rewrites (in processing
/// order) and which delete files it drops.
#[derive(Debug, Default)]
struct PassPlan {
    inputs: Vec<(String, LiveFile)>,
    retired_deletes: Vec<String>,
    pending_files: usize,
}

/// Run one compaction pass. Returns `Ok(None)` if the table is below
/// thresholds or has nothing worth rewriting, `Ok(Some(_))` after a
/// successful compaction commit.
///
/// `live_pks` is the caller's PK → file index for this table, consistent
/// with the catalog (the materializer's own). It lets the pass find the
/// files holding deleted rows without reading any file: a file has dead
/// rows iff fewer of its rows are live than it holds. Correctness leans
/// on one direction of it — a PK it maps to a file must be live there —
/// which the materializer already relies on for TOAST resolution. A
/// stale index can only make files look dirtier (costing a rewrite),
/// never cleaner. With `None`, any file older than a live delete counts
/// as dirty, so deletes retire only once every older file is rewritten.
///
/// `path_for_chunk` is an async closure called per output chunk; the
/// caller is responsible for producing globally-unique paths (e.g. via
/// UUID). `(table_ident, chunk_index)` is passed in. Async-shaped to
/// match the materializer's existing async-trait `MaterializerNamer`,
/// which generates UUID-suffixed paths via an async `IdGen`.
#[allow(clippy::too_many_arguments)]
pub async fn compact_table<C, F, Fut>(
    catalog: &C,
    blob_store: &dyn BlobStore,
    path_for_chunk: F,
    ident: &TableIdent,
    schema: &TableSchema,
    pk_cols: &[ColumnName],
    live_pks: Option<&FileIndex>,
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

    let plan = plan_pass(&live_data, &live_deletes, live_pks, config);
    if plan.inputs.is_empty() && plan.retired_deletes.is_empty() {
        return Ok(None);
    }

    let mut peak_rows: u64 = 0;

    // Deleted PKs → highest delete seq. Only deletes newer than some
    // input can drop one of its rows.
    let pk_schema: Vec<ColumnSchema> = schema
        .columns
        .iter()
        .filter(|c| c.is_primary_key)
        .cloned()
        .collect();
    let oldest_input = plan.inputs.iter().map(|(_, f)| f.seq).min();
    let mut delete_pks: HashMap<PkKey, i64> = HashMap::new();
    for (path, meta) in &live_deletes {
        if !oldest_input.is_some_and(|s| meta.seq > s) {
            continue;
        }
        let bytes = blob_store.get(path).await?;
        for batch in RowBatches::new(bytes, &pk_schema, DECODE_BATCH_ROWS)? {
            let rows = batch?;
            peak_rows = peak_rows.max(rows.len() as u64);
            for row in rows {
                // Highest seq wins — covers re-deletes of the same PK.
                let seq = delete_pks
                    .entry(PkKey::from_row(&row, pk_cols))
                    .or_insert(meta.seq);
                *seq = (*seq).max(meta.seq);
            }
        }
    }

    let writer = TableWriter::new(schema.clone());
    let mut out = OutputFiles::default();
    let mut rows_rewritten: u64 = 0;
    let mut rows_removed_by_deletes: u64 = 0;
    let mut bytes_before: u64 = 0;
    for (path, meta) in &plan.inputs {
        bytes_before += meta.byte_size;
        let bytes = blob_store.get(path).await?;
        for batch in RowBatches::new(bytes, &schema.columns, DECODE_BATCH_ROWS)? {
            let rows = batch?;
            peak_rows = peak_rows.max(rows.len() as u64);
            rows_rewritten += rows.len() as u64;
            // A delete applies iff it is strictly newer than the row.
            // Rewritten rows land above every live delete, so a delete
            // not applied now would never apply again.
            let survivors: Vec<&Row> = rows
                .iter()
                .filter(|r| {
                    delete_pks
                        .get(&PkKey::from_row(r, pk_cols))
                        .is_none_or(|del_seq| *del_seq <= meta.seq)
                })
                .collect();
            rows_removed_by_deletes += (rows.len() - survivors.len()) as u64;
            // Write runs of rows sharing a partition tuple; a new tuple
            // closes the open file. Inputs come grouped by partition, so
            // a run is normally the whole batch.
            let tuples = survivors
                .iter()
                .map(|r| writer.partition_tuple(r))
                .collect::<std::result::Result<Vec<_>, _>>()?;
            let mut run_start = 0;
            while run_start < survivors.len() {
                let tuple = &tuples[run_start];
                let run_end = (run_start..survivors.len())
                    .find(|&i| tuples[i] != *tuple)
                    .unwrap_or(survivors.len());
                if out.open_tuple().is_some_and(|t| t != tuple) {
                    out.close(blob_store, &path_for_chunk, ident).await?;
                }
                out.write(&writer, tuple, &survivors[run_start..run_end], pk_cols)?;
                if out.open_size() >= config.target_size_bytes {
                    out.close(blob_store, &path_for_chunk, ident).await?;
                }
                run_start = run_end;
            }
        }
    }
    out.close(blob_store, &path_for_chunk, ident).await?;
    for path in &plan.retired_deletes {
        bytes_before += live_deletes[path].byte_size;
    }

    let mut removed_paths: Vec<String> = plan.inputs.iter().map(|(p, _)| p.clone()).collect();
    removed_paths.extend(plan.retired_deletes.iter().cloned());
    removed_paths.sort();
    removed_paths.dedup();

    let bytes_after = out.added.iter().map(|f| f.byte_size).sum();
    let output_data_files = out.added.len();
    let added_files = out
        .added
        .iter()
        .zip(std::mem::take(&mut out.added_pks))
        .map(|(f, pk_keys)| CompactedFile {
            path: f.path.clone(),
            pk_keys,
            partition_values: f.partition_values.clone(),
        })
        .collect();
    let committed = catalog
        .commit_compaction(PreparedCompaction {
            ident: ident.clone(),
            added_data_files: out.added,
            removed_paths,
            // The table as the pass read it: the last snapshot.
            data_sequence_number: snapshots.last().map(|s| s.id),
        })
        .await?;

    Ok(Some(CompactionOutcome {
        input_data_files: plan.inputs.len(),
        input_delete_files: plan.retired_deletes.len(),
        output_data_files,
        rows_rewritten,
        rows_removed_by_deletes,
        bytes_before,
        bytes_after,
        peak_rows_in_memory: peak_rows,
        pending_files: plan.pending_files,
        rewritten_files: plan.inputs.into_iter().map(|(p, _)| p).collect(),
        added_files,
        snapshot_id: committed.current_snapshot_id,
    }))
}

/// The output side of a pass: at most one open file, plus the files
/// already uploaded.
#[derive(Default)]
struct OutputFiles {
    open: Option<(Vec<PartitionLiteral>, StreamingDataFile)>,
    /// PK keys of the rows in the open file.
    open_pks: Vec<PkKey>,
    added: Vec<DataFile>,
    /// PK keys of each added file, parallel to `added`.
    added_pks: Vec<Vec<PkKey>>,
}

impl OutputFiles {
    /// Append rows of partition `tuple`; the caller closes any open file
    /// of another tuple first.
    fn write(
        &mut self,
        writer: &TableWriter,
        tuple: &[PartitionLiteral],
        rows: &[&Row],
        pk_cols: &[ColumnName],
    ) -> Result<()> {
        if self.open.is_none() {
            self.open = Some((tuple.to_vec(), writer.start_data_file()?));
        }
        let (_, file) = self.open.as_mut().expect("opened above");
        file.write(rows)?;
        self.open_pks
            .extend(rows.iter().map(|r| PkKey::from_row(r, pk_cols)));
        Ok(())
    }

    fn open_tuple(&self) -> Option<&[PartitionLiteral]> {
        self.open.as_ref().map(|(t, _)| t.as_slice())
    }

    fn open_size(&self) -> u64 {
        self.open.as_ref().map_or(0, |(_, f)| f.estimated_size())
    }

    async fn close<F, Fut>(
        &mut self,
        blob_store: &dyn BlobStore,
        path_for_chunk: &F,
        ident: &TableIdent,
    ) -> Result<()>
    where
        F: Fn(&TableIdent, usize) -> Fut,
        Fut: std::future::Future<Output = String>,
    {
        let Some((partition_values, file)) = self.open.take() else {
            return Ok(());
        };
        let pks = std::mem::take(&mut self.open_pks);
        if file.record_count() == 0 {
            return Ok(());
        }
        let chunk = file.finish()?;
        let path = path_for_chunk(ident, self.added.len()).await;
        let byte_size = chunk.bytes.len() as u64;
        blob_store.put(&path, chunk.bytes).await?;
        self.added.push(DataFile {
            path,
            record_count: chunk.record_count,
            byte_size,
            equality_field_ids: vec![],
            partition_values,
            sequence_number: None,
        });
        self.added_pks.push(pks);
        Ok(())
    }
}

/// Decide what one pass rewrites and which deletes it retires.
fn plan_pass(
    live_data: &BTreeMap<String, LiveFile>,
    live_deletes: &BTreeMap<String, LiveFile>,
    live_pks: Option<&FileIndex>,
    config: &CompactionConfig,
) -> PassPlan {
    let newest_delete = live_deletes.values().map(|d| d.seq).max();
    let live_rows = live_pks.map(FileIndex::live_rows_per_file);
    // Dirty: some live delete may have killed one of the file's rows. Only
    // a delete newer than the file can; past that, trust the index's
    // count of live rows when there is one.
    let is_dirty = |path: &str, f: &LiveFile| {
        newest_delete.is_some_and(|d| d > f.seq)
            && live_rows
                .as_ref()
                .is_none_or(|counts| counts.get(path).copied().unwrap_or(0) < f.record_count)
    };

    // Candidates grouped by partition, each group oldest first.
    let small = config.target_size_bytes / 2;
    let mut groups: Vec<Vec<(&String, &LiveFile, bool)>> = Vec::new();
    for (path, f) in live_data {
        let dirty = is_dirty(path, f);
        if !dirty && f.byte_size >= small {
            continue;
        }
        match groups
            .iter_mut()
            .find(|g| g[0].1.partition_values == f.partition_values)
        {
            Some(g) => g.push((path, f, dirty)),
            None => groups.push(vec![(path, f, dirty)]),
        }
    }
    // A lone clean file has nothing to merge with: rewriting it would
    // reproduce it, every pass.
    groups.retain(|g| g.len() > 1 || g[0].2);
    for g in &mut groups {
        g.sort_by(|a, b| (a.1.seq, a.0).cmp(&(b.1.seq, b.0)));
    }
    // Oldest group first: retiring a delete needs every older dirty file
    // gone, so old files are the ones holding deletes back.
    groups.sort_by(|a, b| (a[0].1.seq, a[0].0).cmp(&(b[0].1.seq, b[0].0)));

    let mut inputs: Vec<(String, LiveFile)> = Vec::new();
    let mut taken_bytes: u64 = 0;
    let mut pending_files = 0;
    'groups: for g in &groups {
        let first_of_group = inputs.len();
        for (i, (path, f, _)) in g.iter().enumerate() {
            // Always make progress: a dirty file, or a pair of clean
            // ones, even when it alone exceeds the budget.
            let progress_floor = if g[0].2 { 1 } else { 2 };
            let within_floor = first_of_group == 0 && i < progress_floor;
            if !within_floor && taken_bytes + f.byte_size > config.max_input_bytes_per_pass {
                // A clean file cut off from its partners would be
                // rewritten alone; let it wait for them.
                if inputs.len() - first_of_group == 1 && !g[0].2 {
                    inputs.pop();
                }
                pending_files = groups.iter().map(Vec::len).sum::<usize>() - inputs.len();
                break 'groups;
            }
            taken_bytes += f.byte_size;
            inputs.push(((*path).clone(), (*f).clone()));
        }
    }

    // Retire a delete once no dirty file it could apply to (seq below
    // its own) survives this pass. Inputs are excluded: their rows have
    // every delete applied before they move.
    let taken: BTreeSet<&str> = inputs.iter().map(|(p, _)| p.as_str()).collect();
    let oldest_remaining_dirty = live_data
        .iter()
        .filter(|(path, f)| !taken.contains(path.as_str()) && is_dirty(path, f))
        .map(|(_, f)| f.seq)
        .min();
    let retired_deletes = live_deletes
        .iter()
        .filter(|(_, d)| oldest_remaining_dirty.is_none_or(|s| d.seq <= s))
        .map(|(p, _)| p.clone())
        .collect();

    PassPlan {
        inputs,
        retired_deletes,
        pending_files,
    }
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
                LiveFile::new(df, df.sequence_number_in(snap)),
            );
        }
        for df in &snap.delete_files {
            if removed.contains(df.path.as_str()) {
                continue;
            }
            deletes.insert(
                df.path.clone(),
                LiveFile::new(df, df.sequence_number_in(snap)),
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
            sequence_number: None,
        }
    }

    fn snap(id: i64, data: Vec<DataFile>, deletes: Vec<DataFile>, removed: Vec<&str>) -> Snapshot {
        Snapshot {
            id,
            data_files: data,
            delete_files: deletes,
            removed_paths: removed.into_iter().map(String::from).collect(),
            timestamp_ms: id * 1000,
            expired: false,
            log_range: None,
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
