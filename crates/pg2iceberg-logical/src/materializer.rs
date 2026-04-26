//! Materializer: reads the coord log, decodes staged Parquet, folds events,
//! resolves TOAST + re-insert promotion, writes Iceberg data + delete files,
//! commits to the catalog, and advances the cursor.
//!
//! Mirrors `logical/materializer.go`. Phase 8 first cut delivers the core
//! single-worker cycle. Distributed mode (multi-worker round-robin),
//! combined-mode CachedStream, and the async ticker are explicit follow-ons.
//!
//! ## Cycle ordering (the durability gate, again)
//!
//! 1. Read coord cursor for `(group, table)`.
//! 2. `read_log` for entries strictly after the cursor.
//! 3. Fetch every staged Parquet file via the blob store; decode into
//!    `MatEvent`s (LSN-ordered already from the writer side).
//! 4. Fold by PK; resolve TOAST against the FileIndex; promote re-inserts.
//! 5. `TableWriter::prepare` → upload data file + equality-delete file
//!    chunks via the blob store.
//! 6. `Catalog::commit_snapshot` — materializer **must not advance the
//!    cursor before this returns success**. On failure, the next cycle
//!    replays from the same cursor (idempotent because Iceberg snapshot
//!    history is append-only and the staged Parquet is unchanged).
//! 7. `Coordinator::set_cursor` to the highest end_offset processed.
//! 8. Update FileIndex with the new data file + removed PKs.

use async_trait::async_trait;
use bytes::Bytes;
use pg2iceberg_coord::{Coordinator, LogEntry};
use pg2iceberg_core::metrics::{names, Labels};
use pg2iceberg_core::{ColumnName, Metrics, NoopMetrics, Row, TableIdent, TableSchema};
use pg2iceberg_iceberg::{
    fold_events, promote_re_inserts, read_data_file, rebuild_from_catalog, resolve_unchanged_cols,
    Catalog, DataFile, FileIndex, IcebergError, MaterializedRow, PreparedCommit, TableWriter,
    WriterError,
};
use pg2iceberg_stream::codec::decode_chunk;
use pg2iceberg_stream::{BlobStore, MatEvent, StreamError};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum MaterializerError {
    #[error("coord: {0}")]
    Coord(#[from] pg2iceberg_coord::CoordError),
    #[error("blob: {0}")]
    Blob(#[from] StreamError),
    #[error("catalog: {0}")]
    Catalog(#[from] IcebergError),
    #[error("writer: {0}")]
    Writer(#[from] WriterError),
    #[error("table not registered: {0}")]
    UnknownTable(TableIdent),
}

pub type Result<T> = std::result::Result<T, MaterializerError>;

#[async_trait]
pub trait MaterializerNamer: Send + Sync {
    /// Generate a unique blob path for a materialized data file or
    /// equality-delete file. `kind` is `"data"` or `"eq-delete"`.
    async fn next_path(&self, table: &TableIdent, kind: &str) -> String;
}

/// Deterministic counter-based namer. Production uses an `IdGen`-backed UUID
/// suffix; this is the sim variant.
pub struct CounterMaterializerNamer {
    counter: std::sync::atomic::AtomicU64,
    base: String,
}

impl CounterMaterializerNamer {
    pub fn new(base: impl Into<String>) -> Self {
        Self {
            counter: std::sync::atomic::AtomicU64::new(0),
            base: base.into(),
        }
    }
}

#[async_trait]
impl MaterializerNamer for CounterMaterializerNamer {
    async fn next_path(&self, table: &TableIdent, kind: &str) -> String {
        let n = self
            .counter
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        format!("{}/{}/{kind}-{n:010}.parquet", self.base, table.name)
    }
}

struct TableEntry {
    schema: TableSchema,
    pk_cols: Vec<ColumnName>,
    file_index: FileIndex,
    /// Cached writer; `TableWriter::new` precomputes Arrow schemas once.
    writer: TableWriter,
}

pub struct Materializer<C: Catalog> {
    coord: Arc<dyn Coordinator>,
    blob_store: Arc<dyn BlobStore>,
    catalog: Arc<C>,
    namer: Arc<dyn MaterializerNamer>,
    tables: BTreeMap<TableIdent, TableEntry>,
    group: String,
    cycle_limit: usize,
    metrics: Arc<dyn Metrics>,
}

impl<C: Catalog> Materializer<C> {
    pub fn new(
        coord: Arc<dyn Coordinator>,
        blob_store: Arc<dyn BlobStore>,
        catalog: Arc<C>,
        namer: Arc<dyn MaterializerNamer>,
        group: impl Into<String>,
        cycle_limit: usize,
    ) -> Self {
        Self::with_metrics(
            coord,
            blob_store,
            catalog,
            namer,
            group,
            cycle_limit,
            Arc::new(NoopMetrics),
        )
    }

    pub fn with_metrics(
        coord: Arc<dyn Coordinator>,
        blob_store: Arc<dyn BlobStore>,
        catalog: Arc<C>,
        namer: Arc<dyn MaterializerNamer>,
        group: impl Into<String>,
        cycle_limit: usize,
        metrics: Arc<dyn Metrics>,
    ) -> Self {
        assert!(cycle_limit > 0);
        Self {
            coord,
            blob_store,
            catalog,
            namer,
            tables: BTreeMap::new(),
            group: group.into(),
            cycle_limit,
            metrics,
        }
    }

    /// Register a materialized table. Creates the catalog table if missing,
    /// ensures the coord cursor exists, and rebuilds the FileIndex from
    /// catalog snapshot history so a restarted process correctly handles
    /// re-inserts of PKs committed by the prior process. For a fresh
    /// catalog this is a no-op.
    pub async fn register_table(&mut self, schema: TableSchema) -> Result<()> {
        let ident = schema.ident.clone();
        self.catalog.ensure_namespace(&ident.namespace).await?;
        if self.catalog.load_table(&ident).await?.is_none() {
            self.catalog.create_table(&schema).await?;
        }
        self.coord.ensure_cursor(&self.group, &ident).await?;

        let pk_cols: Vec<ColumnName> = schema
            .primary_key_columns()
            .map(|c| ColumnName(c.name.clone()))
            .collect();
        let writer = TableWriter::new(schema.clone());

        let file_index = rebuild_from_catalog(
            self.catalog.as_ref(),
            self.blob_store.as_ref(),
            &ident,
            &schema,
            &pk_cols,
        )
        .await
        .map_err(|e| MaterializerError::Catalog(IcebergError::Other(e.to_string())))?;

        self.tables.insert(
            ident,
            TableEntry {
                schema,
                pk_cols,
                file_index,
                writer,
            },
        );
        Ok(())
    }

    pub async fn cycle(&mut self) -> Result<usize> {
        let idents: Vec<TableIdent> = self.tables.keys().cloned().collect();
        let mut total = 0;
        for ident in idents {
            total += self.cycle_table(&ident).await?;
        }
        Ok(total)
    }

    pub async fn cycle_table(&mut self, ident: &TableIdent) -> Result<usize> {
        let mut labels = Labels::new();
        labels.insert("table".into(), ident.name.clone());
        self.metrics
            .counter(names::MATERIALIZER_CYCLE_TOTAL, &labels, 1);

        let entry = self
            .tables
            .get(ident)
            .ok_or_else(|| MaterializerError::UnknownTable(ident.clone()))?;

        // 1. Cursor.
        let cursor = self
            .coord
            .get_cursor(&self.group, ident)
            .await?
            .unwrap_or(-1);
        let after_offset = if cursor < 0 { 0 } else { cursor as u64 };

        // 2. Log entries.
        let entries = self
            .coord
            .read_log(ident, after_offset, self.cycle_limit)
            .await?;
        if entries.is_empty() {
            return Ok(0);
        }
        let max_end_offset = entries.iter().map(|e| e.end_offset).max().unwrap();

        // 3. Decode every staged file.
        let mut all_events: Vec<MatEvent> = Vec::new();
        for e in &entries {
            let bytes = self.blob_store.get(&e.s3_path).await?;
            let mut chunk = decode_chunk(&bytes)?;
            all_events.append(&mut chunk);
        }

        // Fold + TOAST + re-insert. Pre-fetch any prior data files needed
        // for TOAST resolution; that's cheap when no UPDATE has unchanged_cols.
        let mut folded = fold_events(all_events, &entry.pk_cols);
        let prior_paths = collect_toast_paths(&folded, &entry.file_index, &entry.pk_cols);
        let prior_rows_by_path = self.fetch_prior_rows(&prior_paths, &entry.schema).await?;
        resolve_unchanged_cols(
            &mut folded,
            &entry.pk_cols,
            &entry.file_index,
            &prior_rows_by_path,
        )?;
        promote_re_inserts(&mut folded, &entry.file_index, &entry.pk_cols);

        if folded.is_empty() {
            // Nothing to materialize; still advance the cursor — those log
            // entries were processed.
            self.coord
                .set_cursor(&self.group, ident, max_end_offset as i64)
                .await?;
            return Ok(0);
        }

        // 5. Prepare + upload. Output is one parquet file per (partition,
        //    kind) — for unpartitioned tables that's a single file per kind.
        //    The writer consults FileIndex for tier-2 resolution of
        //    partition tuples on `Delete` rows whose row payload doesn't
        //    carry the partition source columns.
        let prepared_files = entry.writer.prepare(&folded, &entry.file_index)?;
        let pk_field_ids = entry.writer.pk_field_ids();

        // Track per-data-file which PKs ended up where, so the FileIndex
        // update below points at the right partition file.
        let mut data_files: Vec<DataFile> = Vec::with_capacity(prepared_files.data.len());
        let mut data_pk_groups: Vec<(String, Vec<String>, Vec<pg2iceberg_core::PartitionLiteral>)> =
            Vec::with_capacity(prepared_files.data.len());
        let mut deleted_pks: Vec<String> = Vec::new();

        for chunk in prepared_files.data {
            let path = self.namer.next_path(ident, "data").await;
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

        let mut delete_files: Vec<DataFile> =
            Vec::with_capacity(prepared_files.equality_deletes.len());
        for chunk in prepared_files.equality_deletes {
            let path = self.namer.next_path(ident, "eq-delete").await;
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

        // 6. Commit catalog snapshot — durability gate.
        self.catalog
            .commit_snapshot(PreparedCommit {
                ident: ident.clone(),
                data_files,
                equality_deletes: delete_files,
            })
            .await?;

        // 7. Advance cursor only after commit success.
        self.coord
            .set_cursor(&self.group, ident, max_end_offset as i64)
            .await?;

        // 8. Update FileIndex.
        let entry_mut = self.tables.get_mut(ident).expect("checked above");
        // Removed PKs first (so a later add for the same PK overrides cleanly).
        entry_mut.file_index.remove_pks(&deleted_pks);
        for (path, pks, partition_values) in data_pk_groups {
            entry_mut.file_index.add_file(path, pks, partition_values);
        }

        let folded_len = folded.len();
        self.metrics
            .counter(names::MATERIALIZER_ROWS_TOTAL, &labels, folded_len as u64);
        Ok(folded_len)
    }

    async fn fetch_prior_rows(
        &self,
        paths: &BTreeSet<String>,
        schema: &TableSchema,
    ) -> Result<BTreeMap<String, Vec<Row>>> {
        let mut out = BTreeMap::new();
        for path in paths {
            let bytes = self.blob_store.get(path).await?;
            let rows = read_data_file(&bytes, &schema.columns)?;
            out.insert(path.clone(), rows);
        }
        Ok(out)
    }
}

fn collect_toast_paths(
    rows: &[MaterializedRow],
    file_index: &FileIndex,
    _pk_cols: &[ColumnName],
) -> BTreeSet<String> {
    let mut paths = BTreeSet::new();
    for r in rows {
        if r.unchanged_cols.is_empty() {
            continue;
        }
        let key = pg2iceberg_iceberg::pk_key(&r.row, _pk_cols);
        if let Some(p) = file_index.lookup(&key) {
            paths.insert(p.to_string());
        }
    }
    paths
}

// LogEntry is unused publicly here; suppress dead-import lint by re-exporting
// for users that want the materializer + raw log-entry struct in one go.
pub use pg2iceberg_coord::LogEntry as MaterializerLogEntry;

#[allow(dead_code)]
fn _hint(_: LogEntry) {}
