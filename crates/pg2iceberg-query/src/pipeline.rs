//! Query-mode pipeline orchestrator.
//!
//! Cycle:
//! 1. For each registered table, read rows whose watermark column has
//!    advanced past the stored cursor (`WatermarkSource::read_after`).
//! 2. Upsert each row into the per-table [`Buffer`]. Watermark advances to
//!    the max value observed.
//! 3. On flush:
//!    - Drain the buffer to `MaterializedRow`s (all `Op::Insert`).
//!    - `promote_re_inserts` against `FileIndex` so PKs already in the
//!      catalog become `Op::Update` — the writer will emit equality
//!      deletes that void the prior data file rows.
//!    - `TableWriter::prepare` → upload → `Catalog::commit_snapshot`.
//!    - Update FileIndex with newly-written PKs and removed PKs.
//!
//! No coord, no staging Parquet, no slot. The watermark per table IS the
//! cursor. Persistence is a Phase 10.5 task (write into the
//! `_pg2iceberg.checkpoints` table).

use crate::buffer::Buffer;
use crate::{watermark_compare, QueryError, WatermarkSource};
use bytes::Bytes;
use pg2iceberg_coord::{CoordError, Coordinator};
use pg2iceberg_core::{ColumnName, PgValue, TableIdent, TableSchema};
use pg2iceberg_iceberg::{
    promote_re_inserts, rebuild_from_catalog, Catalog, DataFile, FileIndex, IcebergError,
    PreparedCommit, TableWriter, WriterError,
};
use pg2iceberg_logical::materializer::MaterializerNamer;
use pg2iceberg_stream::{BlobStore, StreamError};
use std::collections::BTreeMap;
use std::sync::Arc;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum QueryPipelineError {
    #[error("query: {0}")]
    Query(#[from] QueryError),
    #[error("blob: {0}")]
    Blob(#[from] StreamError),
    #[error("catalog: {0}")]
    Catalog(#[from] IcebergError),
    #[error("writer: {0}")]
    Writer(#[from] WriterError),
    #[error("coord: {0}")]
    Coord(#[from] CoordError),
    #[error("table not registered: {0}")]
    UnknownTable(TableIdent),
}

pub type Result<T> = std::result::Result<T, QueryPipelineError>;

struct QueryTable {
    pk_cols: Vec<ColumnName>,
    watermark_col: String,
    /// Highest watermark value observed; `None` means "never polled."
    watermark: Option<PgValue>,
    buffer: Buffer,
    file_index: FileIndex,
    writer: TableWriter,
}

/// Default per-poll-call row cap. ~1k rows keeps peak memory bounded on
/// first poll of a large table; subsequent polls usually return far fewer.
pub const DEFAULT_POLL_CHUNK_SIZE: usize = 1024;

pub struct QueryPipeline<C: Catalog> {
    coord: Arc<dyn Coordinator>,
    catalog: Arc<C>,
    blob_store: Arc<dyn BlobStore>,
    namer: Arc<dyn MaterializerNamer>,
    tables: BTreeMap<TableIdent, QueryTable>,
    /// Max rows per `WatermarkSource::read_after` call. The pipeline loops
    /// until a short batch is returned, so this caps peak memory without
    /// limiting throughput.
    poll_chunk_size: usize,
}

impl<C: Catalog> QueryPipeline<C> {
    pub fn new(
        coord: Arc<dyn Coordinator>,
        catalog: Arc<C>,
        blob_store: Arc<dyn BlobStore>,
        namer: Arc<dyn MaterializerNamer>,
    ) -> Self {
        Self {
            coord,
            catalog,
            blob_store,
            namer,
            tables: BTreeMap::new(),
            poll_chunk_size: DEFAULT_POLL_CHUNK_SIZE,
        }
    }

    /// Override the per-poll-call row cap. Use this for testing chunking
    /// behavior or when memory is very constrained.
    pub fn with_poll_chunk_size(mut self, n: usize) -> Self {
        assert!(n > 0, "poll_chunk_size must be > 0");
        self.poll_chunk_size = n;
        self
    }

    /// Register a table. Creates the catalog table if missing. Restores
    /// the watermark from a persisted checkpoint and rebuilds the
    /// FileIndex from catalog snapshot history so a restarted process
    /// resumes from where the prior process left off — no re-polling rows
    /// already materialized AND re-insert promotion still works for PKs
    /// committed by the prior process.
    pub async fn register_table(
        &mut self,
        schema: TableSchema,
        watermark_col: impl Into<String>,
    ) -> Result<()> {
        let ident = schema.ident.clone();
        self.catalog.ensure_namespace(&ident.namespace).await?;
        if self.catalog.load_table(&ident).await?.is_none() {
            self.catalog.create_table(&schema).await?;
        }
        let pk_cols: Vec<ColumnName> = schema
            .primary_key_columns()
            .map(|c| ColumnName(c.name.clone()))
            .collect();
        let writer = TableWriter::new(schema.clone());
        let buffer = Buffer::new(pk_cols.clone());

        // Restore per-table watermark from coord if it exists.
        let restored_watermark = self.coord.query_watermark(&ident).await?;

        // Rebuild FileIndex from catalog so re-insert promotion works
        // across restarts. For a fresh catalog this is a no-op.
        let file_index = rebuild_from_catalog(
            self.catalog.as_ref(),
            self.blob_store.as_ref(),
            &ident,
            &schema,
            &pk_cols,
        )
        .await
        .map_err(|e| QueryPipelineError::Catalog(IcebergError::Other(e.to_string())))?;

        self.tables.insert(
            ident,
            QueryTable {
                pk_cols,
                watermark_col: watermark_col.into(),
                watermark: restored_watermark,
                buffer,
                file_index,
                writer,
            },
        );
        Ok(())
    }

    /// Run one poll across every registered table. Returns total rows
    /// upserted into buffers (after PK dedup at the buffer layer; the raw
    /// row count from the source may be higher).
    pub async fn poll<S: WatermarkSource + ?Sized>(&mut self, source: &S) -> Result<usize> {
        let idents: Vec<TableIdent> = self.tables.keys().cloned().collect();
        let mut total = 0;
        for ident in idents {
            total += self.poll_table(source, &ident).await?;
        }
        Ok(total)
    }

    pub async fn poll_table<S: WatermarkSource + ?Sized>(
        &mut self,
        source: &S,
        ident: &TableIdent,
    ) -> Result<usize> {
        let chunk_size = self.poll_chunk_size;
        let entry = self
            .tables
            .get_mut(ident)
            .ok_or_else(|| QueryPipelineError::UnknownTable(ident.clone()))?;
        let watermark_col = entry.watermark_col.clone();

        let mut total = 0usize;
        // Loop until the source returns a short batch (< chunk_size). Each
        // iteration advances `entry.watermark`, so the next call's `after`
        // sees only rows past what we've already buffered.
        loop {
            let after = entry.watermark.clone();
            let rows = source
                .read_after(ident, &watermark_col, after.as_ref(), Some(chunk_size))
                .await?;
            let received = rows.len();
            if received == 0 {
                break;
            }
            let watermark_key = ColumnName(watermark_col.clone());
            let mut new_max: Option<PgValue> = entry.watermark.clone();
            for row in rows {
                if let Some(v) = row.get(&watermark_key) {
                    new_max = match new_max {
                        None => Some(v.clone()),
                        Some(ref cur) => match watermark_compare(v, cur) {
                            Ok(std::cmp::Ordering::Greater) => Some(v.clone()),
                            _ => new_max,
                        },
                    };
                }
                entry.buffer.upsert(row);
            }
            entry.watermark = new_max;
            total += received;
            if received < chunk_size {
                break;
            }
        }
        Ok(total)
    }

    /// Drain buffers, write data files + equality deletes for any
    /// re-insert PKs, commit catalog snapshots. Returns total rows
    /// committed across all tables.
    pub async fn flush(&mut self) -> Result<usize> {
        let idents: Vec<TableIdent> = self.tables.keys().cloned().collect();
        let mut total = 0;
        for ident in idents {
            total += self.flush_table(&ident).await?;
        }
        Ok(total)
    }

    pub async fn flush_table(&mut self, ident: &TableIdent) -> Result<usize> {
        let entry = self
            .tables
            .get_mut(ident)
            .ok_or_else(|| QueryPipelineError::UnknownTable(ident.clone()))?;
        if entry.buffer.is_empty() {
            return Ok(0);
        }

        let mut rows = entry.buffer.drain();
        promote_re_inserts(&mut rows, &entry.file_index, &entry.pk_cols);

        let prepared = entry.writer.prepare(&rows, &entry.file_index)?;

        let pk_field_ids = entry.writer.pk_field_ids();

        // Track per-data-file PK group so the FileIndex update points each
        // PK at its partition file.
        let mut data_files: Vec<DataFile> = Vec::with_capacity(prepared.data.len());
        let mut data_pk_groups: Vec<(String, Vec<String>, Vec<pg2iceberg_core::PartitionLiteral>)> =
            Vec::with_capacity(prepared.data.len());
        let mut deleted_pks: Vec<String> = Vec::new();

        for chunk in prepared.data {
            let path = self.namer.next_path(ident, "query-data").await;
            let byte_size = chunk.chunk.bytes.len() as u64;
            self.blob_store
                .put(&path, Bytes::clone(&chunk.chunk.bytes))
                .await?;
            data_files.push(DataFile {
                path: path.clone(),
                record_count: chunk.chunk.record_count,
                byte_size,
                equality_field_ids: vec![],
                partition_values: chunk.partition_values.clone(),
            });
            data_pk_groups.push((path, chunk.pk_keys, chunk.partition_values));
        }

        let mut delete_files: Vec<DataFile> = Vec::with_capacity(prepared.equality_deletes.len());
        for chunk in prepared.equality_deletes {
            let path = self.namer.next_path(ident, "query-eq-delete").await;
            let byte_size = chunk.chunk.bytes.len() as u64;
            self.blob_store
                .put(&path, Bytes::clone(&chunk.chunk.bytes))
                .await?;
            delete_files.push(DataFile {
                path,
                record_count: chunk.chunk.record_count,
                byte_size,
                equality_field_ids: pk_field_ids.clone(),
                partition_values: chunk.partition_values,
            });
            deleted_pks.extend(chunk.pk_keys);
        }

        self.catalog
            .commit_snapshot(PreparedCommit {
                ident: ident.clone(),
                data_files,
                equality_deletes: delete_files,
            })
            .await?;

        // Update FileIndex post-commit (mirrors the materializer ordering).
        let entry_mut = self.tables.get_mut(ident).expect("checked above");
        entry_mut.file_index.remove_pks(&deleted_pks);
        for (path, pks, partition_values) in data_pk_groups {
            entry_mut.file_index.add_file(path, pks, partition_values);
        }

        // Persist the updated watermark snapshot. Order: catalog commit
        // succeeded first; if checkpoint save fails, the next flush will
        // re-save with the same or higher watermark. Worst case on crash
        // is re-polling a few rows — idempotent because `promote_re_inserts`
        // collapses repeats.
        self.save_checkpoint().await?;

        Ok(rows.len())
    }

    /// Test-only accessor for the current watermark (None if never polled).
    pub fn watermark(&self, ident: &TableIdent) -> Option<&PgValue> {
        self.tables.get(ident).and_then(|t| t.watermark.as_ref())
    }

    /// Persist the current per-table watermarks into the coord.
    /// Called after every successful flush; can also be called by
    /// callers as a manual "checkpoint now" operation. Each table's
    /// watermark is stored in its own row in
    /// `_pg2iceberg.query_watermarks` — no global blob, no OCC.
    pub async fn save_checkpoint(&self) -> Result<()> {
        for (ident, table) in &self.tables {
            if let Some(wm) = &table.watermark {
                self.coord.set_query_watermark(ident, wm).await?;
            }
        }
        Ok(())
    }
}
