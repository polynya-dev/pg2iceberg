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
use pg2iceberg_core::typemap::IcebergType;
use pg2iceberg_core::{
    ColumnName, ColumnSchema, Lsn, Metrics, Namespace, NoopMetrics, Op, PgValue, Row, TableIdent,
    TableSchema,
};
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

/// Build the [`TableSchema`] for the blue-green meta-marker table.
/// Each pg2iceberg instance writes one row per `(marker_uuid, table)`
/// pair to its instance-local meta-marker table; an external
/// `iceberg-diff` tool joins blue's and green's tables on
/// `marker_uuid` to verify replica equivalence at WAL points.
///
/// Composite PK on `(uuid, table_name)` so re-emission (e.g. across
/// crash + replay) deduplicates idempotently via the materializer's
/// PK-keyed equality-delete write path.
pub fn meta_marker_table_schema(meta_namespace: &str, table_name: &str) -> TableSchema {
    TableSchema {
        ident: TableIdent {
            namespace: Namespace(vec![meta_namespace.to_string()]),
            name: table_name.to_string(),
        },
        columns: vec![
            ColumnSchema {
                name: "uuid".into(),
                field_id: 1,
                ty: IcebergType::String,
                nullable: false,
                is_primary_key: true,
            },
            ColumnSchema {
                name: "table_name".into(),
                field_id: 2,
                ty: IcebergType::String,
                nullable: false,
                is_primary_key: true,
            },
            ColumnSchema {
                name: "snapshot_id".into(),
                field_id: 3,
                ty: IcebergType::Long,
                nullable: false,
                is_primary_key: false,
            },
        ],
        partition_spec: Vec::new(),
    }
}

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
    #[error("compaction: {0}")]
    Compact(String),
    #[error("orphan cleanup: {0}")]
    Cleanup(String),
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
    /// Optional meta-marker table state. When `Some`, every
    /// successful `cycle_table` queries pending markers from the
    /// coord and emits `(uuid, table_name, snapshot_id)` rows to
    /// this table — that's what gives blue-green replicas WAL-aligned
    /// snapshot pointers for `iceberg-diff` to compare.
    meta_marker: Option<MetaMarkerWriter>,
}

struct MetaMarkerWriter {
    schema: TableSchema,
    writer: TableWriter,
    /// Empty FileIndex used for meta-marker writes. The meta-marker
    /// table is append-only from this materializer's perspective —
    /// we never rewrite it, just insert. (Merge-on-read on the
    /// reader side handles dedup if a marker is re-emitted across a
    /// crash.)
    file_index: FileIndex,
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
            meta_marker: None,
        }
    }

    /// Enable blue-green meta-marker emission. After every successful
    /// `cycle_table`, the materializer queries pending markers up
    /// to that cycle's max LSN and writes
    /// `(uuid, table_name, snapshot_id)` rows to the meta-marker
    /// Iceberg table. Caller must have already created the
    /// meta-marker table in the catalog (e.g. via
    /// `catalog.create_table(&schema)`).
    pub async fn enable_meta_markers(&mut self, schema: TableSchema) -> Result<()> {
        self.catalog.ensure_namespace(&schema.ident.namespace).await?;
        if self.catalog.load_table(&schema.ident).await?.is_none() {
            self.catalog.create_table(&schema).await?;
        }
        let writer = TableWriter::new(schema.clone());
        self.meta_marker = Some(MetaMarkerWriter {
            schema,
            writer,
            file_index: FileIndex::new(),
        });
        Ok(())
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

    /// Apply a pgoutput Relation message: diff `incoming_columns`
    /// against the registered schema for `ident`, build a
    /// `Vec<SchemaChange>`, and call `Catalog::evolve_schema`. The
    /// in-memory `TableEntry::schema` and `TableWriter` are
    /// rebuilt from the post-evolution schema so subsequent
    /// materialize cycles encode rows with the new shape.
    ///
    /// **No-op when the table isn't registered** (the lifecycle
    /// only registers tables in YAML — incoming Relations for
    /// untracked tables, e.g. `_pg2iceberg.markers`, are silently
    /// skipped).
    ///
    /// **No-op when columns match.** pgoutput re-emits Relation
    /// messages liberally (e.g. on every cache invalidation, even
    /// for unchanged schemas); the diff just produces an empty
    /// change list and we return without touching the catalog.
    pub async fn apply_relation(
        &mut self,
        ident: &TableIdent,
        incoming_columns: &[pg2iceberg_pg::RelationColumn],
    ) -> Result<()> {
        let entry = match self.tables.get_mut(ident) {
            Some(e) => e,
            None => return Ok(()),
        };

        let mut current_names: std::collections::BTreeSet<String> =
            entry.schema.columns.iter().map(|c| c.name.clone()).collect();
        let incoming_names: std::collections::BTreeSet<String> = incoming_columns
            .iter()
            .map(|c| c.name.clone())
            .collect();

        let mut changes: Vec<pg2iceberg_iceberg::SchemaChange> = Vec::new();
        for c in incoming_columns {
            if !current_names.contains(&c.name) {
                changes.push(pg2iceberg_iceberg::SchemaChange::AddColumn {
                    name: c.name.clone(),
                    ty: c.ty,
                    nullable: c.nullable,
                });
            }
        }
        for c in &entry.schema.columns {
            // PK columns are immutable in our model — pgoutput won't
            // drop them anyway. Skip to avoid soft-dropping a PK
            // (which would set `nullable = true` on the PK column).
            if c.is_primary_key {
                continue;
            }
            if !incoming_names.contains(&c.name) {
                changes.push(pg2iceberg_iceberg::SchemaChange::DropColumn {
                    name: c.name.clone(),
                });
            }
        }
        if changes.is_empty() {
            return Ok(());
        }

        // Catalog-side first so a failure leaves the materializer's
        // in-memory state unchanged (next Relation will re-trigger
        // the diff). After success, rebuild the TableWriter so it
        // encodes with the new column set.
        self.catalog.evolve_schema(ident, changes.clone()).await?;
        pg2iceberg_iceberg::apply_schema_changes(&mut entry.schema, &changes)
            .map_err(MaterializerError::Catalog)?;
        entry.writer = TableWriter::new(entry.schema.clone());
        // current_names is rebuilt next call from the updated schema;
        // explicit insertion keeps Clippy happy + makes the
        // post-state self-consistent within this call.
        for c in &entry.schema.columns {
            current_names.insert(c.name.clone());
        }
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

    /// Run a compaction pass over every registered table. Returns the
    /// vector of `(ident, outcome)` for tables where compaction actually
    /// ran. Tables below their threshold contribute nothing.
    ///
    /// This is intentionally separate from `cycle()` because compaction
    /// is much more expensive (reads + rewrites whole parquet files) and
    /// runs on a slower cadence — typically once per minute — driven by
    /// the binary's `Ticker::Handler::Compact`.
    pub async fn compact_cycle(
        &mut self,
        config: &pg2iceberg_iceberg::CompactionConfig,
    ) -> Result<Vec<(TableIdent, pg2iceberg_iceberg::CompactionOutcome)>> {
        let idents: Vec<TableIdent> = self.tables.keys().cloned().collect();
        let mut out = Vec::new();
        for ident in idents {
            if let Some(outcome) = self.compact_table(&ident, config).await? {
                out.push((ident, outcome));
            }
        }
        Ok(out)
    }

    /// Run an orphan-file-cleanup pass over every registered table.
    /// Returns `(ident, outcome)` for tables where at least one orphan
    /// was deleted or grace-protected.
    ///
    /// `prefix` is the materializer's data-blob root (e.g. "materialized/").
    /// Each table's blobs land under `{prefix}{table_name}/`; we list
    /// per-table to avoid scanning the entire prefix on every cycle.
    /// `now_ms` is the current wall-clock time (or test clock); orphans
    /// younger than `now_ms - grace_period_ms` are protected.
    pub async fn cleanup_orphans_cycle(
        &self,
        prefix: &str,
        now_ms: i64,
        grace_period_ms: i64,
    ) -> Result<Vec<(TableIdent, pg2iceberg_iceberg::CleanupOutcome)>> {
        let idents: Vec<TableIdent> = self.tables.keys().cloned().collect();
        let mut out = Vec::new();
        for ident in idents {
            let table_prefix = format!(
                "{}{}/",
                prefix.trim_end_matches('/').to_string() + "/",
                ident.name
            );
            // The leading `prefix.trim_end_matches('/').to_string() + "/"`
            // normalizes any trailing slash so `format!` doesn't double-up.
            let outcome = pg2iceberg_iceberg::cleanup_orphans(
                self.catalog.as_ref(),
                self.blob_store.as_ref(),
                &ident,
                &table_prefix,
                now_ms,
                grace_period_ms,
            )
            .await
            .map_err(|e| MaterializerError::Cleanup(e.to_string()))?;
            if outcome.deleted > 0 || outcome.grace_protected > 0 {
                out.push((ident, outcome));
            }
        }
        Ok(out)
    }

    /// Run a snapshot-expiry pass over every registered table. Returns
    /// `(ident, expired_count)` for tables that actually had snapshots
    /// dropped. `retention_ms` is the maximum age (in ms) a non-current
    /// snapshot may have before it's expired.
    pub async fn expire_cycle(&mut self, retention_ms: i64) -> Result<Vec<(TableIdent, usize)>> {
        let idents: Vec<TableIdent> = self.tables.keys().cloned().collect();
        let mut out = Vec::new();
        for ident in idents {
            let n = self
                .catalog
                .expire_snapshots(&ident, retention_ms)
                .await
                .map_err(MaterializerError::Catalog)?;
            if n > 0 {
                out.push((ident, n));
            }
        }
        Ok(out)
    }

    /// Compact a single table. After a successful compaction commit, the
    /// table's in-memory FileIndex is rebuilt from the catalog so
    /// subsequent materializer cycles route deletes against the new file
    /// set rather than the old (now-superseded) one.
    pub async fn compact_table(
        &mut self,
        ident: &TableIdent,
        config: &pg2iceberg_iceberg::CompactionConfig,
    ) -> Result<Option<pg2iceberg_iceberg::CompactionOutcome>> {
        let entry = self
            .tables
            .get(ident)
            .ok_or_else(|| MaterializerError::UnknownTable(ident.clone()))?;
        let schema = entry.schema.clone();
        let pk_cols = entry.pk_cols.clone();

        let namer = self.namer.clone();
        let outcome = pg2iceberg_iceberg::compact_table(
            self.catalog.as_ref(),
            self.blob_store.as_ref(),
            move |t, _idx| {
                let n = namer.clone();
                let t = t.clone();
                async move { n.next_path(&t, "compact").await }
            },
            ident,
            &schema,
            &pk_cols,
            config,
        )
        .await
        .map_err(|e| MaterializerError::Compact(e.to_string()))?;

        // If we actually rewrote anything, rebuild FileIndex from the
        // new catalog state. Stale entries pointing at compacted-away
        // files would route deletes incorrectly.
        if outcome.is_some() {
            let entry_mut = self.tables.get_mut(ident).expect("checked above");
            let fresh = pg2iceberg_iceberg::rebuild_from_catalog(
                self.catalog.as_ref(),
                self.blob_store.as_ref(),
                ident,
                &entry_mut.schema,
                &entry_mut.pk_cols,
            )
            .await
            .map_err(|e| MaterializerError::Compact(e.to_string()))?;
            entry_mut.file_index = fresh;
        }

        Ok(outcome)
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
            // No new events for this table — but a marker may have
            // landed in coord that this table is now eligible to
            // emit (because it has nothing pending past the
            // marker's commit_lsn). Ask the coord; emit if any
            // pending markers pass the eligibility check.
            if self.meta_marker.is_some() {
                self.emit_pending_markers(ident, cursor).await?;
            }
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

        // 3b. Expand any TRUNCATE events into per-PK deletes against
        //     the current FileIndex. A `TRUNCATE` in PG drops every
        //     row; in Iceberg we model that as one equality-delete
        //     per known PK so the next snapshot's MoR reads return
        //     zero rows. Subsequent same-tx INSERTs survive the
        //     fold's last-write-wins on the same PK.
        all_events = expand_truncates(all_events, &entry.file_index, &entry.pk_cols);

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

        // 9. Blue-green meta-marker emission. After cursor +
        //    FileIndex are durable, query the coord for any pending
        //    markers covered by this cycle's LSN window. Writes
        //    `(uuid, table_name, snapshot_id)` rows to the
        //    meta-marker Iceberg table — that's the per-instance
        //    audit trail for blue-green replica diffing.
        // Pass the *new* cursor (= max_end_offset) so the coord's
        // eligibility check considers everything we just committed
        // as processed.
        if self.meta_marker.is_some() {
            self.emit_pending_markers(ident, max_end_offset as i64)
                .await?;
        }

        let folded_len = folded.len();
        self.metrics
            .counter(names::MATERIALIZER_ROWS_TOTAL, &labels, folded_len as u64);
        Ok(folded_len)
    }

    /// Emit meta-marker rows for any pending markers eligible for
    /// `user_table` given its current `cursor` (see
    /// [`Coordinator::pending_markers_for_table`] for the
    /// eligibility rule). Idempotent across crashes/replays via
    /// `coord.record_marker_emitted`. No-op if marker mode is off.
    /// Returns the number of meta-marker rows written.
    pub async fn emit_pending_markers(
        &mut self,
        user_table: &TableIdent,
        cursor: i64,
    ) -> Result<usize> {
        let pending = self
            .coord
            .pending_markers_for_table(user_table, cursor)
            .await?;
        if pending.is_empty() {
            return Ok(0);
        }
        let meta = match &mut self.meta_marker {
            Some(m) => m,
            None => return Ok(0),
        };

        // Look up the *user* table's current snapshot id — that's
        // the value that goes into each meta-marker row, pointing
        // at the snapshot blue/green should be diffed at.
        let user_meta = self
            .catalog
            .load_table(user_table)
            .await?
            .ok_or_else(|| MaterializerError::UnknownTable(user_table.clone()))?;
        let snapshot_id = user_meta.current_snapshot_id.unwrap_or(0);

        // Build one row per pending marker. PK is `(uuid,
        // table_name)`, so duplicates between cycles dedup at
        // read-time via merge-on-read.
        let table_name_str = format!("{}", user_table);
        let rows: Vec<MaterializedRow> = pending
            .iter()
            .map(|m| {
                let mut row: Row = std::collections::BTreeMap::new();
                row.insert(ColumnName("uuid".into()), PgValue::Text(m.uuid.clone()));
                row.insert(
                    ColumnName("table_name".into()),
                    PgValue::Text(table_name_str.clone()),
                );
                row.insert(ColumnName("snapshot_id".into()), PgValue::Int8(snapshot_id));
                MaterializedRow {
                    op: Op::Insert,
                    row,
                    unchanged_cols: Vec::new(),
                }
            })
            .collect();

        // Stage + commit to the meta-marker Iceberg table.
        let prepared = meta.writer.prepare(&rows, &meta.file_index)?;
        let mut data_files: Vec<DataFile> = Vec::with_capacity(prepared.data.len());
        for chunk in prepared.data {
            let path = self
                .namer
                .next_path(&meta.schema.ident, "meta-marker")
                .await;
            let byte_size = chunk.chunk.bytes.len() as u64;
            self.blob_store
                .put(&path, Bytes::clone(&chunk.chunk.bytes))
                .await?;
            data_files.push(DataFile {
                path,
                record_count: chunk.chunk.record_count,
                byte_size,
                equality_field_ids: vec![],
                partition_values: chunk.partition_values,
            });
        }
        // Re-emission of an already-emitted marker would conflict on
        // the (uuid, table_name) PK. We rely on `record_marker_emitted`
        // dedup in the coord to prevent that, but the materializer's
        // PK-keyed equality-delete dedup also catches it on read if
        // a write slips through.
        self.catalog
            .commit_snapshot(PreparedCommit {
                ident: meta.schema.ident.clone(),
                data_files,
                equality_deletes: Vec::new(),
            })
            .await?;
        for m in &pending {
            self.coord
                .record_marker_emitted(&m.uuid, user_table)
                .await?;
        }
        Ok(pending.len())
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

/// Expand `Op::Truncate` events into per-PK `Op::Delete` events
/// against the current FileIndex. Mirrors PG's TRUNCATE semantics:
/// every row known to Iceberg right now is wiped, so the materializer
/// emits an equality-delete for each.
///
/// Subsequent post-truncate Insert/Update events for the same PK
/// will overwrite the synthetic Delete during the fold (last-write-
/// wins keyed by PK). PKs not touched post-truncate stay as Delete.
///
/// The Truncate sentinel events are dropped from the output — they
/// have no row payload of their own and the writer would reject
/// them.
fn expand_truncates(
    events: Vec<MatEvent>,
    file_index: &FileIndex,
    pk_cols: &[ColumnName],
) -> Vec<MatEvent> {
    if !events.iter().any(|e| e.op == Op::Truncate) {
        return events;
    }
    let mut out: Vec<MatEvent> = Vec::with_capacity(events.len() + file_index.live_pk_count());
    for evt in events {
        if evt.op != Op::Truncate {
            out.push(evt);
            continue;
        }
        // Decode each PK key (a JSON array of PgValues) back into a
        // PK-only Row keyed by the schema's PK columns. Bad keys
        // (shouldn't happen — FileIndex stores what `pk_key` produces)
        // are skipped rather than crashing the cycle.
        for pk_key_str in file_index.all_pks() {
            let values: Vec<pg2iceberg_core::PgValue> =
                match serde_json::from_str(pk_key_str) {
                    Ok(v) => v,
                    Err(_) => continue,
                };
            if values.len() != pk_cols.len() {
                continue;
            }
            let mut row = Row::new();
            for (col, val) in pk_cols.iter().zip(values.into_iter()) {
                row.insert(col.clone(), val);
            }
            out.push(MatEvent {
                op: Op::Delete,
                lsn: evt.lsn,
                commit_ts: evt.commit_ts,
                xid: evt.xid,
                unchanged_cols: Vec::new(),
                row,
            });
        }
        // Drop the Truncate sentinel itself — its expansion is in `out`.
    }
    out
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
