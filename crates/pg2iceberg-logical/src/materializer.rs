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
    ColumnName, ColumnSchema, Metrics, Namespace, NoopMetrics, Op, PgValue, Row, TableIdent,
    TableSchema,
};
use pg2iceberg_iceberg::meta::{
    self as meta_schema, CheckpointStats, CompactionStats, FlushStats, MaintenanceStats,
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
    /// Optional control-plane meta tables (commits / checkpoints /
    /// compactions / maintenance). Enabled via
    /// [`Self::enable_meta_recording`]. When set, each user-table
    /// commit / compaction / expiry / orphan-cleanup auto-buffers a
    /// row and `flush_meta` (called at end of each cycle) commits
    /// the buffered rows to Iceberg.
    meta_recorder: Option<MetaRecorder>,
    /// Distributed-materializer mode toggle. When `Some`, each
    /// `cycle()` invocation:
    ///   1. Refreshes this worker's heartbeat via
    ///      `Coordinator::register_consumer`.
    ///   2. Reads the active-worker list and deterministically
    ///      round-robins tables across them (sorted tables, sorted
    ///      workers, `table[i]` → `workers[i % N]`).
    ///   3. Skips `cycle_table` for tables not assigned to this
    ///      worker.
    ///
    /// When `None`, every registered table runs every cycle —
    /// the single-process default. The `consumer_ttl` field
    /// controls how long a missed heartbeat survives before the
    /// worker drops out of the active list.
    distributed: Option<DistributedMode>,
}

/// Distributed-mode parameters. Built by
/// [`Materializer::enable_distributed_mode`].
#[derive(Clone)]
struct DistributedMode {
    worker_id: pg2iceberg_core::WorkerId,
    consumer_ttl: std::time::Duration,
    /// Cached previous assignment so we can log rebalance deltas
    /// (added/removed tables) when the active worker set changes.
    last_assigned: Option<std::collections::BTreeSet<TableIdent>>,
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

/// One control-plane meta table's writer + buffered rows. Flush
/// converts buffered rows into a single Iceberg snapshot (one
/// `commit_snapshot` call per non-empty buffer per `flush_meta()`
/// invocation). Append-only — no equality deletes, no FileIndex
/// updates needed; downstream consumers expect monotonic snapshot
/// history over the meta tables.
struct MetaTableState {
    schema: TableSchema,
    writer: TableWriter,
    file_index: FileIndex,
    pending: Vec<MaterializedRow>,
}

impl MetaTableState {
    fn new(schema: TableSchema) -> Self {
        let writer = TableWriter::new(schema.clone());
        Self {
            schema,
            writer,
            file_index: FileIndex::new(),
            pending: Vec::new(),
        }
    }
}

/// Holds writers for the four control-plane meta tables (commits,
/// checkpoints, compactions, maintenance). The `markers` table is
/// handled separately by [`MetaMarkerWriter`] because its rows come
/// from coordinator state rather than materializer outcomes.
///
/// Rows are buffered and flushed in batches via
/// [`Materializer::flush_meta`]. The materializer auto-flushes at
/// the end of each `cycle_table` / `compact_table` / `expire_cycle`
/// / `cleanup_orphans_cycle` so the meta tables stay caught up
/// without explicit caller orchestration.
///
/// A failure inside `flush_meta` does **not** roll back already-
/// committed data snapshots — meta is best-effort observability,
/// not part of the durability contract. We log the failure and
/// drop the buffer so a poison-pill row doesn't pin every later
/// flush.
struct MetaRecorder {
    commits: MetaTableState,
    checkpoints: MetaTableState,
    compactions: MetaTableState,
    maintenance: MetaTableState,
    /// Set on every recorded row's `worker_id` field if the per-stat
    /// caller leaves it blank. Empty here means "not in distributed
    /// mode" and stays NULL in the resulting parquet.
    worker_id: String,
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
            meta_recorder: None,
            distributed: None,
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
        self.catalog
            .ensure_namespace(&schema.ident.namespace)
            .await?;
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

    /// Enable control-plane meta-table recording. Creates the four
    /// meta tables (`<meta_namespace>.{commits, checkpoints,
    /// compactions, maintenance}`) in the catalog if they don't
    /// already exist, then attaches a [`MetaRecorder`] that buffers
    /// row data on every user-table commit / compaction / maintenance
    /// op. Buffered rows are committed to their respective Iceberg
    /// table on each cycle's tail call to [`Self::flush_meta`].
    ///
    /// Idempotent — safe to call repeatedly with the same
    /// `meta_namespace`. The materializer no-ops if a table
    /// already exists in the catalog with the matching schema; an
    /// existing table with a *different* schema (e.g. an old
    /// version pre-dating a column add) returns the catalog's error
    /// because we don't auto-evolve the meta schema yet.
    ///
    /// `worker_id` is stamped on every recorded row's
    /// `worker_id` column when the per-stat caller leaves it
    /// blank. Pass an empty string when not running in
    /// distributed/horizontal mode.
    pub async fn enable_meta_recording(
        &mut self,
        meta_namespace: &str,
        worker_id: impl Into<String>,
    ) -> Result<()> {
        let ns = pg2iceberg_core::Namespace(vec![meta_namespace.to_string()]);
        self.catalog.ensure_namespace(&ns).await?;

        let commits_schema = meta_schema::meta_commits_schema(meta_namespace);
        let checkpoints_schema = meta_schema::meta_checkpoints_schema(meta_namespace);
        let compactions_schema = meta_schema::meta_compactions_schema(meta_namespace);
        let maintenance_schema = meta_schema::meta_maintenance_schema(meta_namespace);

        for s in [
            &commits_schema,
            &checkpoints_schema,
            &compactions_schema,
            &maintenance_schema,
        ] {
            if self.catalog.load_table(&s.ident).await?.is_none() {
                self.catalog.create_table(s).await?;
            }
        }

        self.meta_recorder = Some(MetaRecorder {
            commits: MetaTableState::new(commits_schema),
            checkpoints: MetaTableState::new(checkpoints_schema),
            compactions: MetaTableState::new(compactions_schema),
            maintenance: MetaTableState::new(maintenance_schema),
            worker_id: worker_id.into(),
        });
        Ok(())
    }

    /// Buffer a `<meta_ns>.commits` row. No-op when meta recording is
    /// disabled. Worker id is filled in from the recorder's default
    /// when the stat's worker_id is empty.
    pub fn record_flush(&mut self, mut stats: FlushStats) {
        let mr = match self.meta_recorder.as_mut() {
            Some(r) => r,
            None => return,
        };
        if stats.worker_id.is_empty() {
            stats.worker_id = mr.worker_id.clone();
        }
        mr.commits.pending.push(MaterializedRow {
            op: Op::Insert,
            row: stats.to_row(),
            unchanged_cols: Vec::new(),
        });
    }

    /// Buffer a `<meta_ns>.checkpoints` row.
    pub fn record_checkpoint(&mut self, mut stats: CheckpointStats) {
        let mr = match self.meta_recorder.as_mut() {
            Some(r) => r,
            None => return,
        };
        if stats.worker_id.is_empty() {
            stats.worker_id = mr.worker_id.clone();
        }
        mr.checkpoints.pending.push(MaterializedRow {
            op: Op::Insert,
            row: stats.to_row(),
            unchanged_cols: Vec::new(),
        });
    }

    /// Buffer a `<meta_ns>.compactions` row.
    pub fn record_compaction(&mut self, mut stats: CompactionStats) {
        let mr = match self.meta_recorder.as_mut() {
            Some(r) => r,
            None => return,
        };
        if stats.worker_id.is_empty() {
            stats.worker_id = mr.worker_id.clone();
        }
        mr.compactions.pending.push(MaterializedRow {
            op: Op::Insert,
            row: stats.to_row(),
            unchanged_cols: Vec::new(),
        });
    }

    /// Buffer a `<meta_ns>.maintenance` row.
    pub fn record_maintenance(&mut self, mut stats: MaintenanceStats) {
        let mr = match self.meta_recorder.as_mut() {
            Some(r) => r,
            None => return,
        };
        if stats.worker_id.is_empty() {
            stats.worker_id = mr.worker_id.clone();
        }
        mr.maintenance.pending.push(MaterializedRow {
            op: Op::Insert,
            row: stats.to_row(),
            unchanged_cols: Vec::new(),
        });
    }

    /// Drain every meta-table buffer and commit one Iceberg snapshot
    /// per non-empty buffer. Called automatically at the tail of
    /// each `cycle_table` / `compact_table` / `expire_cycle` /
    /// `cleanup_orphans_cycle`, and exposed publicly so external
    /// drivers (snapshot-only mode, custom one-shots) can flush
    /// after their own record_* calls.
    ///
    /// Errors are returned but do **not** roll back already-committed
    /// data snapshots — meta is observability-only. The caller
    /// typically logs and continues; a transient catalog blip
    /// shouldn't take down the data path.
    pub async fn flush_meta(&mut self) -> Result<()> {
        // Flush each meta table independently. Splitting the
        // borrows by-table lets us hold a single &mut to one buffer
        // at a time without juggling a self-referencing struct.
        let mut tables: Vec<&mut MetaTableState> = match self.meta_recorder.as_mut() {
            Some(r) => vec![
                &mut r.commits,
                &mut r.checkpoints,
                &mut r.compactions,
                &mut r.maintenance,
            ],
            None => return Ok(()),
        };
        for state in tables.iter_mut() {
            if state.pending.is_empty() {
                continue;
            }
            let rows = std::mem::take(&mut state.pending);
            let prepared = state.writer.prepare(&rows, &state.file_index)?;
            let mut data_files: Vec<DataFile> = Vec::with_capacity(prepared.data.len());
            for chunk in prepared.data {
                let path = self.namer.next_path(&state.schema.ident, "meta").await;
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
            self.catalog
                .commit_snapshot(PreparedCommit {
                    ident: state.schema.ident.clone(),
                    data_files,
                    equality_deletes: Vec::new(),
                })
                .await?;
        }
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
    /// Detects three kinds of evolution:
    ///
    /// - **AddColumn**: a name in `incoming` that's not in our
    ///   schema. Field id is allocated by `apply_schema_changes`
    ///   from the schema's current high-water mark.
    /// - **DropColumn**: a non-PK name in our schema that's not in
    ///   `incoming`. Soft-drop — column stays as nullable so older
    ///   data files keep resolving.
    /// - **PromoteColumnType**: a name present in both, but
    ///   `incoming.ty != current.ty` *and* the change is a legal
    ///   Iceberg promotion (int→long, float→double, decimal
    ///   precision increase). Illegal type changes (e.g. long→int,
    ///   text→int) are rejected with `MaterializerError::Catalog`
    ///   so the lifecycle fails loudly rather than silently
    ///   truncate or coerce downstream readers.
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

        let mut current_names: std::collections::BTreeSet<String> = entry
            .schema
            .columns
            .iter()
            .map(|c| c.name.clone())
            .collect();
        let incoming_names: std::collections::BTreeSet<String> =
            incoming_columns.iter().map(|c| c.name.clone()).collect();
        let current_by_name: std::collections::BTreeMap<String, &ColumnSchema> = entry
            .schema
            .columns
            .iter()
            .map(|c| (c.name.clone(), c))
            .collect();

        let mut changes: Vec<pg2iceberg_iceberg::SchemaChange> = Vec::new();
        for c in incoming_columns {
            match current_by_name.get(&c.name) {
                None => {
                    changes.push(pg2iceberg_iceberg::SchemaChange::AddColumn {
                        name: c.name.clone(),
                        ty: c.ty,
                        nullable: c.nullable,
                    });
                }
                Some(existing) => {
                    if existing.ty != c.ty {
                        // PK columns are part of the equality-delete
                        // predicate; promoting a PK type would
                        // invalidate every prior delete file's pk_key
                        // hash. Refuse — operators must re-snapshot
                        // for that case.
                        if existing.is_primary_key {
                            return Err(MaterializerError::Catalog(IcebergError::Other(format!(
                                "cannot promote primary-key column {}: {:?} → {:?} \
                                 (PK type is part of the equality-delete contract; \
                                 changing it requires a full re-snapshot)",
                                c.name, existing.ty, c.ty
                            ))));
                        }
                        if !pg2iceberg_iceberg::is_legal_type_promotion(existing.ty, c.ty) {
                            return Err(MaterializerError::Catalog(IcebergError::Other(format!(
                                "column {} type change {:?} → {:?} is not a legal Iceberg \
                                 promotion. Allowed: int→long, float→double, decimal \
                                 precision increase. Other changes (narrowing, cross-family) \
                                 require a full re-snapshot.",
                                c.name, existing.ty, c.ty
                            ))));
                        }
                        changes.push(pg2iceberg_iceberg::SchemaChange::PromoteColumnType {
                            name: c.name.clone(),
                            new_ty: c.ty,
                        });
                    }
                }
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

    /// Enable distributed-materializer mode. Subsequent [`Self::cycle`]
    /// calls will heartbeat this worker, read the active-worker
    /// list, and round-robin tables across workers. Pass a stable,
    /// process-unique `worker_id` (e.g. a k8s pod name); two
    /// processes claiming the same id race to refresh the same
    /// `_pg2iceberg.consumers` row and produce undefined assignment.
    /// `consumer_ttl` is how long a worker survives without
    /// heartbeating — Go's default is 30s, which we mirror.
    pub fn enable_distributed_mode(
        &mut self,
        worker_id: pg2iceberg_core::WorkerId,
        consumer_ttl: std::time::Duration,
    ) {
        self.distributed = Some(DistributedMode {
            worker_id,
            consumer_ttl,
            last_assigned: None,
        });
    }

    /// Drop this worker from the coordinator's active set so peer
    /// workers rebalance immediately rather than waiting for the
    /// heartbeat TTL to expire. No-op when distributed mode is off.
    /// Logs but doesn't propagate errors — shutdown ergonomics
    /// shouldn't be tripped by a transient coord blip.
    pub async fn shutdown_distributed(&mut self) {
        let dm = match self.distributed.as_ref() {
            Some(d) => d,
            None => return,
        };
        if let Err(e) = self
            .coord
            .unregister_consumer(&self.group, &dm.worker_id)
            .await
        {
            tracing::warn!(
                error = %e,
                worker = %dm.worker_id.0,
                "unregister_consumer failed during shutdown"
            );
        } else {
            tracing::info!(
                worker = %dm.worker_id.0,
                group = %self.group,
                "worker unregistered from consumer group"
            );
        }
    }

    pub async fn cycle(&mut self) -> Result<usize> {
        // 1. Default path: process every registered table.
        let mut idents: Vec<TableIdent> = self.tables.keys().cloned().collect();

        // 2. Distributed path: refresh heartbeat, read active-worker
        //    list, compute deterministic round-robin assignment,
        //    filter `idents` to just our slice.
        //
        //    Mirrors `pg2iceberg/logical/materializer.go:225-282`. No
        //    locks — every worker computes the same assignment from
        //    the same active-worker list, and rebalances are
        //    "atomic" in the sense that the assignment changes the
        //    moment the active-worker list does.
        if let Some(dm) = self.distributed.clone() {
            self.coord
                .register_consumer(&self.group, &dm.worker_id, dm.consumer_ttl)
                .await?;
            let mut workers = self.coord.active_consumers(&self.group).await?;
            // Coordinator returns workers sorted by id (per the trait
            // contract on `pg2iceberg-coord`); enforce here so a
            // misbehaving impl can't break determinism.
            workers.sort_by(|a, b| a.0.cmp(&b.0));
            if workers.is_empty() {
                return Ok(0);
            }
            // Sort tables for deterministic distribution. Without
            // this, a HashMap iteration order would yield different
            // assignments on different workers.
            idents.sort();
            let n = workers.len();
            let assigned: std::collections::BTreeSet<TableIdent> = idents
                .iter()
                .enumerate()
                .filter(|(i, _)| workers[i % n] == dm.worker_id)
                .map(|(_, t)| t.clone())
                .collect();
            // Log rebalance deltas. The first cycle prints `assigned`
            // unconditionally; subsequent cycles only when membership
            // changed.
            let dm_state = self
                .distributed
                .as_mut()
                .expect("distributed checked above");
            match &dm_state.last_assigned {
                None => tracing::info!(
                    worker = %dm.worker_id.0,
                    workers = workers.len(),
                    tables = assigned.len(),
                    "distributed assignment computed"
                ),
                Some(prev) if prev != &assigned => {
                    let added: Vec<&TableIdent> = assigned.difference(prev).collect();
                    let removed: Vec<&TableIdent> = prev.difference(&assigned).collect();
                    tracing::info!(
                        worker = %dm.worker_id.0,
                        workers = workers.len(),
                        added = ?added,
                        removed = ?removed,
                        tables = assigned.len(),
                        "distributed assignment rebalanced"
                    );
                }
                _ => {}
            }
            dm_state.last_assigned = Some(assigned.clone());
            idents.retain(|t| assigned.contains(t));
        }

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
    ///
    /// Per-table outcomes are also recorded into `<meta_ns>.maintenance`
    /// when meta recording is enabled, with `operation =
    /// "clean_orphans"`. Meta-flush errors are logged and swallowed so
    /// observability blips don't block cleanup retries.
    pub async fn cleanup_orphans_cycle(
        &mut self,
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
            let started_micros = now_micros();
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
                if self.meta_recorder.is_some() {
                    self.record_maintenance(MaintenanceStats {
                        ts_micros: started_micros,
                        worker_id: String::new(),
                        table_name: format!("{}", ident),
                        operation: meta_schema::MAINTENANCE_OP_CLEAN_ORPHANS.into(),
                        items_affected: outcome.deleted as i32,
                        bytes_freed: outcome.bytes_freed as i64,
                        duration_ms: (now_micros() - started_micros) / 1000,
                        pg2iceberg_commit_sha: String::new(),
                    });
                }
                out.push((ident, outcome));
            }
        }
        if self.meta_recorder.is_some() {
            if let Err(e) = self.flush_meta().await {
                tracing::warn!(error = %e, "orphan-cleanup meta flush failed");
            }
        }
        Ok(out)
    }

    /// Run a snapshot-expiry pass over every registered table. Returns
    /// `(ident, expired_count)` for tables that actually had snapshots
    /// dropped. `retention_ms` is the maximum age (in ms) a non-current
    /// snapshot may have before it's expired.
    ///
    /// Per-table outcomes are recorded into `<meta_ns>.maintenance`
    /// when meta recording is enabled, with `operation =
    /// "expire_snapshots"`.
    pub async fn expire_cycle(&mut self, retention_ms: i64) -> Result<Vec<(TableIdent, usize)>> {
        let idents: Vec<TableIdent> = self.tables.keys().cloned().collect();
        let mut out = Vec::new();
        for ident in idents {
            let started_micros = now_micros();
            let n = self
                .catalog
                .expire_snapshots(&ident, retention_ms)
                .await
                .map_err(MaterializerError::Catalog)?;
            if n > 0 {
                if self.meta_recorder.is_some() {
                    self.record_maintenance(MaintenanceStats {
                        ts_micros: started_micros,
                        worker_id: String::new(),
                        table_name: format!("{}", ident),
                        operation: meta_schema::MAINTENANCE_OP_EXPIRE_SNAPSHOTS.into(),
                        items_affected: n as i32,
                        bytes_freed: 0,
                        duration_ms: (now_micros() - started_micros) / 1000,
                        pg2iceberg_commit_sha: String::new(),
                    });
                }
                out.push((ident, n));
            }
        }
        if self.meta_recorder.is_some() {
            if let Err(e) = self.flush_meta().await {
                tracing::warn!(error = %e, "expire-snapshots meta flush failed");
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
        let started_micros = now_micros();
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
        if let Some(o) = &outcome {
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

            // Record + flush a meta `compactions` row. Best-effort:
            // any meta-write error is logged but doesn't roll back
            // the compaction snapshot (already durable above).
            if self.meta_recorder.is_some() {
                let post = self.catalog.load_table(ident).await?;
                let snap_id = post
                    .as_ref()
                    .and_then(|m| m.current_snapshot_id)
                    .unwrap_or(0);
                self.record_compaction(CompactionStats {
                    ts_micros: started_micros,
                    worker_id: String::new(),
                    table_name: format!("{}", ident),
                    partition: String::new(),
                    snapshot_id: snap_id,
                    sequence_number: snap_id,
                    input_data_files: o.input_data_files as i32,
                    input_delete_files: o.input_delete_files as i32,
                    output_data_files: o.output_data_files as i32,
                    rows_rewritten: o.rows_rewritten as i64,
                    rows_removed: o.rows_removed_by_deletes as i64,
                    bytes_before: o.bytes_before as i64,
                    bytes_after: o.bytes_after as i64,
                    duration_ms: (now_micros() - started_micros) / 1000,
                    pg2iceberg_commit_sha: String::new(),
                });
                if let Err(e) = self.flush_meta().await {
                    tracing::warn!(error = %e, table = %ident, "compaction meta flush failed");
                }
            }
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

        // 3b. Capture per-cycle observability stats from `all_events`
        //     BEFORE the fold collapses them to per-PK. We need the
        //     full event stream to compute max LSN, max source-side
        //     ts, and the distinct-xid count for the meta `commits`
        //     row.
        let max_lsn: i64 = all_events.iter().map(|e| e.lsn.0 as i64).max().unwrap_or(0);
        let max_source_ts_micros: i64 = all_events.iter().map(|e| e.commit_ts.0).max().unwrap_or(0);
        let tx_count: i32 = {
            let mut xids: BTreeSet<u32> = BTreeSet::new();
            for e in &all_events {
                if let Some(x) = e.xid {
                    xids.insert(x);
                }
            }
            xids.len() as i32
        };

        // 3c. Expand any TRUNCATE events into per-PK deletes against
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
        let data_files_count = data_files.len() as i32;
        let delete_files_count = delete_files.len() as i32;
        let bytes_written: i64 = data_files
            .iter()
            .chain(delete_files.iter())
            .map(|f| f.byte_size as i64)
            .sum();
        let cycle_started_micros = now_micros();
        let post_commit_meta = self
            .catalog
            .commit_snapshot(PreparedCommit {
                ident: ident.clone(),
                data_files,
                equality_deletes: delete_files,
            })
            .await?;
        let commit_duration_ms = (now_micros() - cycle_started_micros) / 1000;

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

        // 10. Control-plane meta `commits` row + flush. Best-effort:
        //     a meta-flush failure is logged but doesn't roll back
        //     the user-table commit (already durable above).
        let folded_len = folded.len();
        if self.meta_recorder.is_some() {
            let snap_id = post_commit_meta.current_snapshot_id.unwrap_or(0);
            self.record_flush(FlushStats {
                ts_micros: cycle_started_micros,
                worker_id: String::new(),
                table_name: format!("{}", ident),
                mode: "logical".into(),
                snapshot_id: snap_id,
                sequence_number: snap_id,
                lsn: max_lsn,
                rows: folded_len as i64,
                bytes: bytes_written,
                duration_ms: commit_duration_ms,
                data_files: data_files_count,
                delete_files: delete_files_count,
                max_source_ts_micros,
                schema_id: 0,
                tx_count,
                pg2iceberg_commit_sha: String::new(),
            });
            if let Err(e) = self.flush_meta().await {
                tracing::warn!(
                    error = %e,
                    table = %ident,
                    "flush_meta failed; data commit was successful"
                );
            }
        }

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

/// Wall-clock micros since unix epoch. Used by meta-table rows for
/// `ts` / `last_flush_at`. We don't read this from a `Clock` trait
/// because the meta tables are observability-only — drift between
/// `Clock`-driven test time and wall-clock here is acceptable.
fn now_micros() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_micros() as i64)
        .unwrap_or(0)
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
            let values: Vec<pg2iceberg_core::PgValue> = match serde_json::from_str(pk_key_str) {
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
