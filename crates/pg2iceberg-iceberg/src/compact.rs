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
//! 2. Decide what's worth doing, if anything: merging small files
//!    (< `target_size_bytes / 2`), once there are `data_file_threshold`
//!    data files; rewriting *dirty* files — ones holding rows a live delete
//!    has killed — once those rows are `delete_ratio` of the rows the
//!    dirty files hold, or there are `delete_file_threshold` delete files
//!    ([`purge_due`]). Neither due: bail with `None`.
//! 3. Plan the pass ([`plan_pass`]): oldest first, take the candidates
//!    until `max_input_bytes_per_pass`; the rest wait for a later pass.
//! 4. Retire every delete file no remaining dirty file could still need.
//! 5. Load deleted PKs (`PK → max delete seq`) from the delete files that
//!    can apply to an input.
//! 6. Stream the inputs one record batch at a time: drop rows a delete
//!    applies to (`delete_seq > row_seq`), encode survivors straight into
//!    the open output file, and upload it once it reaches the target size.
//!    The inputs are read ahead of the one being rewritten, and an output
//!    uploads while the next is encoded ([`PassIo`]).
//! 7. `Catalog::commit_compaction` issues one `Operation::Replace`
//!    snapshot removing the inputs + retired deletes and adding outputs.
//!
//! ## Memory
//!
//! At most one decoded record batch ([`DECODE_BATCH_ROWS`]) of one input
//! file is alive at a time, next to that file's encoded bytes and the
//! encoded bytes of one open output file (≈ `target_size_bytes`) — and
//! the inputs read ahead and the outputs uploading, each at most
//! `target_size_bytes` (or one file, if larger). What
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

use crate::reader::{value_at, ArrowBatches};
use crate::writer::{StreamingDataFile, TableWriter, WriterError};
use crate::{Catalog, DataFile, FileIndex, IcebergError, PkKey, PreparedCompaction, Snapshot};
use arrow_array::{Array, BooleanArray, RecordBatch};
use arrow_select::filter::filter_record_batch;
use bytes::Bytes;
use futures::future::BoxFuture;
use futures::stream::{self, FuturesOrdered, FuturesUnordered, StreamExt};
use pg2iceberg_core::{ColumnName, ColumnSchema, PartitionLiteral, Row, TableIdent, TableSchema};
use pg2iceberg_stream::{BlobStore, StreamError};
use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::future::{poll_fn, Future};
use std::pin::pin;
use std::task::{Context, Poll};
use thiserror::Error;

/// Rows decoded per record batch. Bounds how many rows a pass holds at
/// once, however large its input files are.
pub const DECODE_BATCH_ROWS: usize = 1024;
/// Most files a pass reads at once: the delete files it applies, and the
/// inputs after the one it's rewriting. Read one at a time, a pass waits
/// a round trip per file.
pub const READ_AHEAD: usize = 8;

/// Knobs that decide whether a table is worth compacting and how to
/// size the output.
#[derive(Clone, Debug)]
pub struct CompactionConfig {
    /// Merge small files once live data files >= this count.
    pub data_file_threshold: usize,
    /// Rewrite the files deletes killed rows in once live delete files >=
    /// this count, however few rows they killed: each delete file costs a
    /// reader. A delete file is dropped once no data file it could apply
    /// to remains, and that takes rewriting every older dirty file.
    pub delete_file_threshold: usize,
    /// ... or once the rows deletes killed are this share of the rows the
    /// files holding them hold. Rewriting them then reads at most `1 /
    /// delete_ratio` rows per row deleted; sooner, under updates spread
    /// across a large table, a pass rewrites the table for a few of its
    /// rows.
    pub delete_ratio: f64,
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

    /// This config with only its delete-file backstop: no small-file
    /// merge, no rewrite for the share of rows deletes killed. What a
    /// materializer catching up on a backlog does, until it has.
    pub fn backstop_only(&self) -> Self {
        Self {
            data_file_threshold: usize::MAX,
            delete_ratio: f64::INFINITY,
            ..self.clone()
        }
    }
}

impl Default for CompactionConfig {
    fn default() -> Self {
        let target_size_bytes = 128 * 1024 * 1024;
        Self {
            data_file_threshold: 8,
            delete_file_threshold: 32,
            delete_ratio: 0.1,
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

/// What the caller's PK → file index knows of a table's rows: each data
/// file's live rows, as of snapshot `as_of` — the latest, with `None`.
/// The materializer's own index (see [`compact_table`]).
///
/// A pass reads the table as of that snapshot too, never later: counts
/// from before a delete would show the file it hid a row in as clean, and
/// the pass could retire that delete with the row still in the file.
#[derive(Clone, Debug, Default)]
pub struct LiveRows {
    pub as_of: Option<i64>,
    pub per_file: BTreeMap<String, u64>,
}

impl LiveRows {
    pub fn new(index: &FileIndex, as_of: Option<i64>) -> Self {
        Self {
            as_of,
            per_file: index
                .live_rows_per_file()
                .into_iter()
                .map(|(path, live)| (path.to_string(), live))
                .collect(),
        }
    }
}

/// A pass with its outputs uploaded and its commit still to make.
#[derive(Debug)]
pub struct PreparedPass {
    /// Its commit, for [`Catalog::commit_compaction`].
    pub commit: PreparedCompaction,
    /// What it did; `snapshot_id` is its commit's, once made.
    pub outcome: CompactionOutcome,
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

/// Run one compaction pass: [`prepare_pass`] on the table's snapshots,
/// and its commit. Returns `Ok(None)` if the table is below thresholds or
/// has nothing worth rewriting, `Ok(Some(_))` after a successful
/// compaction commit.
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
    let live = live_pks.map(|index| LiveRows::new(index, None));
    let Some(pass) = prepare_pass(
        &snapshots,
        blob_store,
        path_for_chunk,
        ident,
        schema,
        pk_cols,
        live.as_ref(),
        config,
    )
    .await?
    else {
        return Ok(None);
    };
    let committed = catalog.commit_compaction(pass.commit).await?;
    Ok(Some(CompactionOutcome {
        snapshot_id: committed.current_snapshot_id,
        ..pass.outcome
    }))
}

/// Everything of a compaction pass but its commit: plan it from the
/// table's `snapshots` (as [`Catalog::snapshots`] lists them) and `live`,
/// read and rewrite its inputs, and upload its outputs. `Ok(None)` if
/// nothing is due. The outputs stay unreferenced until the commit: a pass
/// abandoned before it leaves them to orphan cleanup.
///
/// `live` as for [`compact_table`]'s `live_pks`; the pass reads the table
/// as of its `as_of` ([`LiveRows`]).
#[allow(clippy::too_many_arguments)]
pub async fn prepare_pass<F, Fut>(
    snapshots: &[Snapshot],
    blob_store: &dyn BlobStore,
    path_for_chunk: F,
    ident: &TableIdent,
    schema: &TableSchema,
    pk_cols: &[ColumnName],
    live: Option<&LiveRows>,
    config: &CompactionConfig,
) -> Result<Option<PreparedPass>>
where
    F: Fn(&TableIdent, usize) -> Fut,
    Fut: std::future::Future<Output = String>,
{
    // The table as the counts know it: snapshots come oldest first.
    let snapshots = match live.and_then(|l| l.as_of) {
        Some(at) => &snapshots[..snapshots.partition_point(|s| s.id <= at)],
        None => snapshots,
    };
    if snapshots.is_empty() {
        return Ok(None);
    }
    let live_rows = live.map(|l| &l.per_file);

    let (live_data, live_deletes) = compute_live_files(snapshots);

    let is_dirty = dirty_check(&live_deletes, live_rows);
    let rewrite = Rewrite {
        merge: live_data.len() >= config.data_file_threshold,
        purge: purge_due(&live_data, &live_deletes, live_rows, &is_dirty, config),
    };
    if !rewrite.merge && !rewrite.purge {
        return Ok(None);
    }
    let plan = plan_pass(&live_data, &live_deletes, &is_dirty, rewrite, config);
    if plan.inputs.is_empty() && plan.retired_deletes.is_empty() {
        return Ok(None);
    }

    let mut peak_rows: u64 = 0;

    // Deleted PKs → highest delete seq. Only deletes newer than some
    // input can drop one of its rows.
    // The key's columns, in its order.
    let pk_schema: Vec<ColumnSchema> = pk_cols
        .iter()
        .filter_map(|pk| schema.columns.iter().find(|c| c.name == pk.0))
        .cloned()
        .collect();
    let oldest_input = plan.inputs.iter().map(|(_, f)| f.seq).min();
    let applying: Vec<(&String, i64)> = live_deletes
        .iter()
        .filter(|(_, meta)| oldest_input.is_some_and(|s| meta.seq > s))
        .map(|(path, meta)| (path, meta.seq))
        .collect();
    // The inputs start reading meanwhile.
    let mut io = PassIo::new(blob_store, &plan.inputs, config.target_size_bytes);
    let delete_pks = io
        .alongside(load_deletes(
            blob_store,
            applying,
            &pk_schema,
            &mut peak_rows,
        ))
        .await?;

    let writer = TableWriter::new(schema.clone());
    let mut out = OutputFiles::default();
    let mut rows_rewritten: u64 = 0;
    let mut rows_removed_by_deletes: u64 = 0;
    let mut bytes_before: u64 = 0;
    let partition_cols: Vec<ColumnSchema> = schema
        .partition_spec
        .iter()
        .filter_map(|f| schema.columns.iter().find(|c| c.name == f.source_column))
        .cloned()
        .collect();
    while let Some(((_, meta), bytes)) = io.next_input().await? {
        bytes_before += meta.byte_size;
        for batch in ArrowBatches::new(bytes, &schema.columns, DECODE_BATCH_ROWS)? {
            let batch = batch?;
            let n = batch.num_rows();
            peak_rows = peak_rows.max(n as u64);
            rows_rewritten += n as u64;
            // A delete applies iff it is strictly newer than the row.
            // Rewritten rows land above every live delete, so a delete
            // not applied now would never apply again.
            let keys = RowKeys::new(&batch, &schema.columns, &pk_schema);
            let mut live = Vec::with_capacity(n);
            let mut survivor_keys = Vec::with_capacity(n);
            for i in 0..n {
                let key = keys.at(i)?;
                let kept = delete_pks
                    .get(&key)
                    .is_none_or(|del_seq| *del_seq <= meta.seq);
                live.push(kept);
                if kept {
                    survivor_keys.push(key);
                }
            }
            let survivors = filter_record_batch(&batch, &BooleanArray::from(live))
                .map_err(|e| WriterError::Encode(format!("filter deleted rows: {e}")))?;
            rows_removed_by_deletes += (n - survivors.num_rows()) as u64;
            // Write runs of rows sharing a partition tuple; a new tuple
            // closes the open file. Inputs come grouped by partition, so
            // a run is normally the whole batch.
            let mut survivor_keys = survivor_keys.into_iter();
            for (tuple, start, len) in partition_runs(&writer, &survivors, &partition_cols)? {
                if out.open_tuple().is_some_and(|t| t != tuple.as_slice()) {
                    out.close(&mut io, &path_for_chunk, ident).await?;
                }
                out.write(
                    &writer,
                    &tuple,
                    &survivors.slice(start, len),
                    survivor_keys.by_ref().take(len),
                )?;
                if out.open_size() >= config.target_size_bytes {
                    out.close(&mut io, &path_for_chunk, ident).await?;
                }
            }
        }
    }
    out.close(&mut io, &path_for_chunk, ident).await?;
    // Every output landed before the commit names it.
    io.finish().await?;
    drop(io);
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
    Ok(Some(PreparedPass {
        commit: PreparedCompaction {
            ident: ident.clone(),
            added_data_files: out.added,
            removed_paths,
            // The table as the pass read it: the last snapshot.
            data_sequence_number: snapshots.last().map(|s| s.id),
        },
        outcome: CompactionOutcome {
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
            snapshot_id: None,
        },
    }))
}

/// Retire the delete files no data file they could apply to is left for,
/// and rewrite nothing: the part of a pass a table needs when another
/// engine compacts it — a managed catalog. Iceberg's Java rewrite leaves
/// the deletes it applied behind, and pg2iceberg commits one with every
/// change, so they would pile up.
///
/// Runs when the table holds at least `config.delete_file_threshold`
/// delete files; `live_pks` as for [`compact_table`]. Its commit adds no
/// files: a `Replace` that only removes deletes.
pub async fn retire_deletes<C>(
    catalog: &C,
    ident: &TableIdent,
    live_pks: Option<&FileIndex>,
    config: &CompactionConfig,
) -> Result<Option<CompactionOutcome>>
where
    C: Catalog + ?Sized,
{
    let snapshots = catalog.snapshots(ident).await?;
    let (live_data, live_deletes) = compute_live_files(&snapshots);
    if live_deletes.is_empty() || live_deletes.len() < config.delete_file_threshold {
        return Ok(None);
    }
    let live = live_pks.map(|index| LiveRows::new(index, None));
    let is_dirty = dirty_check(&live_deletes, live.as_ref().map(|l| &l.per_file));
    let retired = retirable_deletes(&live_data, &live_deletes, &is_dirty, &BTreeSet::new());
    if retired.is_empty() {
        return Ok(None);
    }
    let bytes_before = retired.iter().map(|p| live_deletes[p].byte_size).sum();
    let committed = catalog
        .commit_compaction(PreparedCompaction {
            ident: ident.clone(),
            added_data_files: Vec::new(),
            removed_paths: retired.clone(),
            data_sequence_number: snapshots.last().map(|s| s.id),
        })
        .await?;
    Ok(Some(CompactionOutcome {
        input_delete_files: retired.len(),
        bytes_before,
        snapshot_id: committed.current_snapshot_id,
        ..Default::default()
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
    /// Append rows of partition `tuple`, whose keys are `keys`; the caller
    /// closes any open file of another tuple first.
    fn write(
        &mut self,
        writer: &TableWriter,
        tuple: &[PartitionLiteral],
        rows: &RecordBatch,
        keys: impl Iterator<Item = PkKey>,
    ) -> Result<()> {
        if self.open.is_none() {
            self.open = Some((tuple.to_vec(), writer.start_data_file()?));
        }
        let (_, file) = self.open.as_mut().expect("opened above");
        file.write_batch(rows)?;
        self.open_pks.extend(keys);
        Ok(())
    }

    fn open_tuple(&self) -> Option<&[PartitionLiteral]> {
        self.open.as_ref().map(|(t, _)| t.as_slice())
    }

    fn open_size(&self) -> u64 {
        self.open.as_ref().map_or(0, |(_, f)| f.estimated_size())
    }

    /// Finish the open file and start its upload.
    async fn close<F, Fut>(
        &mut self,
        io: &mut PassIo<'_>,
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
        let path = io.alongside(path_for_chunk(ident, self.added.len())).await;
        let byte_size = chunk.bytes.len() as u64;
        io.upload(path.clone(), chunk.bytes).await?;
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

/// Deleted PKs → the highest seq of the deletes (`(path, seq)`) naming
/// them, read [`READ_AHEAD`] files at once.
async fn load_deletes(
    blob_store: &dyn BlobStore,
    deletes: Vec<(&String, i64)>,
    pk_schema: &[ColumnSchema],
    peak_rows: &mut u64,
) -> Result<HashMap<PkKey, i64>> {
    let mut reads: Vec<BoxFuture<'_, (i64, std::result::Result<Bytes, StreamError>)>> =
        Vec::with_capacity(deletes.len());
    for (path, seq) in deletes {
        reads.push(Box::pin(async move { (seq, blob_store.get(path).await) }));
    }
    let mut reads = stream::iter(reads).buffer_unordered(READ_AHEAD);
    let mut delete_pks: HashMap<PkKey, i64> = HashMap::new();
    while let Some((seq, bytes)) = reads.next().await {
        for batch in ArrowBatches::new(bytes?, pk_schema, DECODE_BATCH_ROWS)? {
            let batch = batch?;
            *peak_rows = (*peak_rows).max(batch.num_rows() as u64);
            let keys = RowKeys::new(&batch, pk_schema, pk_schema);
            for i in 0..batch.num_rows() {
                // Highest seq wins — covers re-deletes of the same PK.
                let at = delete_pks.entry(keys.at(i)?).or_insert(seq);
                *at = (*at).max(seq);
            }
        }
    }
    Ok(delete_pks)
}

type Input<'a> = &'a (String, LiveFile);

/// A pass's object store traffic, alongside the pass in its own task: its
/// inputs, read ahead of the one being rewritten — at most [`READ_AHEAD`]
/// files and `bound` bytes of them — and its outputs, uploading while the
/// next is encoded — at most `bound` bytes of them. Either way at least
/// one file, however large.
///
/// The requests move along whenever the pass awaits through here, and the
/// pass awaits nothing else meanwhile: a request left unpolled can hold
/// what the pass's next one waits for (a vended-credentials refresh holds
/// a lock every request takes), and the pass would never finish.
struct PassIo<'a> {
    blob_store: &'a dyn BlobStore,
    /// Inputs not yet being read.
    inputs: std::slice::Iter<'a, (String, LiveFile)>,
    reading: FuturesOrdered<BoxFuture<'a, (Input<'a>, std::result::Result<Bytes, StreamError>)>>,
    /// Read, not yet taken.
    read: VecDeque<(Input<'a>, std::result::Result<Bytes, StreamError>)>,
    /// Bytes of the inputs `reading` and `read`.
    reading_bytes: u64,
    uploading: FuturesUnordered<BoxFuture<'a, (u64, std::result::Result<(), StreamError>)>>,
    uploading_bytes: u64,
    /// The first upload that failed.
    failed: Option<StreamError>,
    bound: u64,
}

impl<'a> PassIo<'a> {
    fn new(blob_store: &'a dyn BlobStore, inputs: &'a [(String, LiveFile)], bound: u64) -> Self {
        Self {
            blob_store,
            inputs: inputs.iter(),
            reading: FuturesOrdered::new(),
            read: VecDeque::new(),
            reading_bytes: 0,
            uploading: FuturesUnordered::new(),
            uploading_bytes: 0,
            failed: None,
            bound,
        }
    }

    /// The next input and its bytes, in order; `None` after the last.
    async fn next_input(&mut self) -> Result<Option<(Input<'a>, Bytes)>> {
        poll_fn(|cx| {
            self.drive(cx);
            if let Some(e) = self.failed.take() {
                return Poll::Ready(Err(e.into()));
            }
            if let Some((input, bytes)) = self.read.pop_front() {
                self.reading_bytes -= input.1.byte_size;
                // Start reading the next, in its place.
                self.drive(cx);
                return Poll::Ready(Ok(Some((input, bytes?))));
            }
            if self.reading.is_empty() {
                return Poll::Ready(Ok(None));
            }
            Poll::Pending
        })
        .await
    }

    /// Start uploading `bytes` to `path`, once the uploads under way leave
    /// room for it.
    async fn upload(&mut self, path: String, bytes: Bytes) -> Result<()> {
        let size = bytes.len() as u64;
        poll_fn(|cx| {
            self.drive(cx);
            if let Some(e) = self.failed.take() {
                return Poll::Ready(Err(e));
            }
            if self.uploading.is_empty() || self.uploading_bytes + size <= self.bound {
                return Poll::Ready(Ok(()));
            }
            Poll::Pending
        })
        .await?;
        let blob_store = self.blob_store;
        self.uploading_bytes += size;
        self.uploading.push(Box::pin(async move {
            (size, blob_store.put(&path, bytes).await)
        }));
        // Send it on its way.
        poll_fn(|cx| {
            self.drive(cx);
            Poll::Ready(())
        })
        .await;
        Ok(())
    }

    /// Wait for the uploads under way.
    async fn finish(&mut self) -> Result<()> {
        poll_fn(|cx| {
            self.drive(cx);
            if let Some(e) = self.failed.take() {
                return Poll::Ready(Err(e.into()));
            }
            if self.uploading.is_empty() {
                return Poll::Ready(Ok(()));
            }
            Poll::Pending
        })
        .await
    }

    /// `work`, while the requests go on.
    async fn alongside<F: Future>(&mut self, work: F) -> F::Output {
        let mut work = pin!(work);
        poll_fn(|cx| {
            if let Poll::Ready(out) = work.as_mut().poll(cx) {
                return Poll::Ready(out);
            }
            self.drive(cx);
            Poll::Pending
        })
        .await
    }

    /// Move the requests along as far as they go now.
    fn drive(&mut self, cx: &mut Context<'_>) {
        while self.reading.len() + self.read.len() < READ_AHEAD {
            let Some(input) = self.inputs.as_slice().first() else {
                break;
            };
            let ahead = self.reading.len() + self.read.len();
            if ahead > 0 && self.reading_bytes + input.1.byte_size > self.bound {
                break;
            }
            self.inputs.next();
            self.reading_bytes += input.1.byte_size;
            let blob_store = self.blob_store;
            self.reading.push_back(Box::pin(
                async move { (input, blob_store.get(&input.0).await) },
            ));
        }
        while let Poll::Ready(Some(read)) = self.reading.poll_next_unpin(cx) {
            self.read.push_back(read);
        }
        while let Poll::Ready(Some((size, uploaded))) = self.uploading.poll_next_unpin(cx) {
            self.uploading_bytes -= size;
            if let Err(e) = uploaded {
                self.failed.get_or_insert(e);
            }
        }
    }
}

/// Whether some live delete may have killed one of a data file's rows.
/// Only a delete newer than the file can; past that, trust the index's
/// count of live rows when there is one.
fn dirty_check<'a>(
    live_deletes: &BTreeMap<String, LiveFile>,
    live_rows: Option<&'a BTreeMap<String, u64>>,
) -> impl Fn(&str, &LiveFile) -> bool + 'a {
    let newest_delete = live_deletes.values().map(|d| d.seq).max();
    move |path: &str, f: &LiveFile| {
        newest_delete.is_some_and(|d| d > f.seq)
            && live_rows
                .is_none_or(|counts| counts.get(path).copied().unwrap_or(0) < f.record_count)
    }
}

/// The deletes no dirty data file they could apply to (seq below their
/// own) is left for, once the files in `taken` are rewritten — a pass
/// applies every delete to their rows before they move.
fn retirable_deletes(
    live_data: &BTreeMap<String, LiveFile>,
    live_deletes: &BTreeMap<String, LiveFile>,
    is_dirty: &impl Fn(&str, &LiveFile) -> bool,
    taken: &BTreeSet<&str>,
) -> Vec<String> {
    let oldest_remaining_dirty = live_data
        .iter()
        .filter(|(path, f)| !taken.contains(path.as_str()) && is_dirty(path, f))
        .map(|(_, f)| f.seq)
        .min();
    live_deletes
        .iter()
        .filter(|(_, d)| oldest_remaining_dirty.is_none_or(|s| d.seq <= s))
        .map(|(p, _)| p.clone())
        .collect()
}

/// What a pass rewrites: small files, to merge them; dirty ones, to drop
/// their dead rows and the deletes that killed them.
#[derive(Clone, Copy, Debug)]
struct Rewrite {
    merge: bool,
    purge: bool,
}

/// Whether the deletes have killed enough rows — or are enough files — to
/// rewrite the dirty files for (see [`CompactionConfig::delete_ratio`]).
/// Without an index the rows deletes killed aren't known; the delete
/// files' rows stand in for them.
fn purge_due(
    live_data: &BTreeMap<String, LiveFile>,
    live_deletes: &BTreeMap<String, LiveFile>,
    live_rows: Option<&BTreeMap<String, u64>>,
    is_dirty: &impl Fn(&str, &LiveFile) -> bool,
    config: &CompactionConfig,
) -> bool {
    if live_deletes.is_empty() {
        return false;
    }
    if live_deletes.len() >= config.delete_file_threshold {
        return true;
    }
    let mut held: u64 = 0;
    let mut dead: u64 = 0;
    for (path, f) in live_data.iter().filter(|(p, f)| is_dirty(p, f)) {
        held += f.record_count;
        if let Some(live) = live_rows {
            let live = live.get(path.as_str()).copied().unwrap_or(0);
            dead += f.record_count.saturating_sub(live);
        }
    }
    if live_rows.is_none() {
        dead = live_deletes.values().map(|d| d.record_count).sum();
    }
    held > 0 && dead as f64 >= config.delete_ratio * held as f64
}

/// Decide what one pass rewrites and which deletes it retires.
fn plan_pass(
    live_data: &BTreeMap<String, LiveFile>,
    live_deletes: &BTreeMap<String, LiveFile>,
    is_dirty: &impl Fn(&str, &LiveFile) -> bool,
    rewrite: Rewrite,
    config: &CompactionConfig,
) -> PassPlan {
    // Candidates grouped by partition, each group oldest first. A pass
    // that rewrites anything merges the small files it can.
    let small = config.target_size_bytes / 2;
    let mut groups: Vec<Vec<(&String, &LiveFile, bool)>> = Vec::new();
    for (path, f) in live_data {
        let dirty = is_dirty(path, f);
        let candidate =
            (rewrite.purge && dirty) || ((rewrite.merge || rewrite.purge) && f.byte_size < small);
        if !candidate {
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

    let taken: BTreeSet<&str> = inputs.iter().map(|(p, _)| p.as_str()).collect();
    let retired_deletes = retirable_deletes(live_data, live_deletes, is_dirty, &taken);

    PassPlan {
        inputs,
        retired_deletes,
        pending_files,
    }
}

/// The primary keys of a batch's rows, read straight from its key
/// columns — what [`PkKey::from_row`] makes of each decoded row.
struct RowKeys<'a> {
    columns: Vec<(&'a ColumnSchema, &'a dyn Array)>,
}

impl<'a> RowKeys<'a> {
    /// `batch` is in `cols`' schema, as [`ArrowBatches`] reads it.
    fn new(batch: &'a RecordBatch, cols: &[ColumnSchema], pk_schema: &'a [ColumnSchema]) -> Self {
        let columns = pk_schema
            .iter()
            .map(|pk| {
                let i = cols
                    .iter()
                    .position(|c| c.field_id == pk.field_id)
                    .expect("primary key columns are the table's");
                (pk, batch.column(i).as_ref())
            })
            .collect();
        Self { columns }
    }

    fn at(&self, i: usize) -> Result<PkKey> {
        let values = self
            .columns
            .iter()
            .map(|(col, arr)| value_at(col, *arr, i))
            .collect::<std::result::Result<Vec<_>, _>>()?;
        Ok(PkKey::from_values(&values))
    }
}

/// The batch's runs of rows sharing a partition tuple, as `(tuple, start,
/// len)`. `partition_cols` are the spec's source columns; the batch is in
/// the table's schema.
fn partition_runs(
    writer: &TableWriter,
    batch: &RecordBatch,
    partition_cols: &[ColumnSchema],
) -> Result<Vec<(Vec<PartitionLiteral>, usize, usize)>> {
    let n = batch.num_rows();
    if n == 0 {
        return Ok(Vec::new());
    }
    if partition_cols.is_empty() {
        return Ok(vec![(Vec::new(), 0, n)]);
    }
    let schema = writer.schema();
    let arrays: Vec<(&ColumnSchema, &dyn Array)> = partition_cols
        .iter()
        .map(|c| {
            let i = schema
                .columns
                .iter()
                .position(|s| s.field_id == c.field_id)
                .expect("partition source columns are the table's");
            (c, batch.column(i).as_ref())
        })
        .collect();
    let mut runs: Vec<(Vec<PartitionLiteral>, usize, usize)> = Vec::new();
    for i in 0..n {
        let mut row = Row::new();
        for (col, arr) in &arrays {
            row.insert(ColumnName(col.name.clone()), value_at(col, *arr, i)?);
        }
        let tuple = writer.partition_tuple(&row)?;
        match runs.last_mut() {
            Some((t, _, len)) if *t == tuple => *len += 1,
            _ => runs.push((tuple, i, 1)),
        }
    }
    Ok(runs)
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

    /// A delete retires once no dirty data file older than it is left —
    /// the rule a pass that rewrites nothing still applies.
    #[test]
    fn deletes_retire_once_no_older_dirty_file_is_left() {
        let file = |seq: i64| LiveFile {
            byte_size: 1,
            record_count: 2,
            partition_values: Vec::new(),
            seq,
        };
        let files = |entries: &[(&str, i64)]| -> BTreeMap<String, LiveFile> {
            entries
                .iter()
                .map(|(path, seq)| (path.to_string(), file(*seq)))
                .collect()
        };
        let deletes = files(&[("eq-2", 2), ("eq-4", 4)]);
        // No index: every data file older than the newest delete is dirty.
        let is_dirty = dirty_check(&deletes, None);
        let none = BTreeSet::new();

        // A file from before both: either may apply to it.
        let retired = retirable_deletes(&files(&[("a", 1), ("b", 3)]), &deletes, &is_dirty, &none);
        assert!(retired.is_empty(), "{retired:?}");
        // Rewritten at 3 (a managed catalog's compaction keeps the number
        // its pass read): `eq-2` has nothing left to apply to.
        let retired = retirable_deletes(&files(&[("c", 3), ("b", 3)]), &deletes, &is_dirty, &none);
        assert_eq!(retired, ["eq-2"]);
        // Nothing older than either.
        let retired = retirable_deletes(&files(&[("d", 4)]), &deletes, &is_dirty, &none);
        assert_eq!(retired, ["eq-2", "eq-4"]);
    }

    #[test]
    fn config_default_thresholds_are_reasonable() {
        let c = CompactionConfig::default();
        assert!(c.data_file_threshold >= 4);
        assert!(c.delete_file_threshold >= 1);
        assert!(c.target_size_bytes >= 16 * 1024 * 1024);
    }
}
