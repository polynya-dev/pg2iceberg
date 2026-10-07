//! Materializer: reads the coord log, decodes staged Parquet, folds events,
//! resolves TOAST + re-insert promotion, writes Iceberg data + delete files,
//! commits to the catalog, and advances the cursor.
//!
//! Single-worker cycle is the core. Distributed mode (multi-worker
//! round-robin), combined-mode CachedStream, and the async ticker
//! are layered on top — see the `enable_distributed_mode`,
//! `compact_cycle`, and `expire_cycle` entry points.
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

use crate::catalog_cache::CachingCatalog;
use crate::relation_event;
use async_trait::async_trait;
use bytes::Bytes;
use pg2iceberg_coord::{Coordinator, LogEntry};
use pg2iceberg_core::metrics::{names, Labels};
use pg2iceberg_core::typemap::IcebergType;
use pg2iceberg_core::{
    is_snapshot_xid, ColumnName, ColumnSchema, IdGen, Lsn, Metrics, Namespace, NoopMetrics, Op,
    PgValue, Row, TableIdent, TableSchema,
};
use pg2iceberg_iceberg::meta::{
    self as meta_schema, CheckpointStats, CompactionStats, FlushStats, MaintenanceStats,
};
use pg2iceberg_iceberg::reader::RowBatches;
use pg2iceberg_iceberg::{
    apply_schema_changes, catch_up_from_catalog, fold_events, is_dropped_column,
    promote_re_inserts, read_data_file, reconcile_columns, resolve_unchanged_cols, toast_source,
    Catalog, DataFile, FileIndex, IcebergError, LogRange, MaterializedRow, PkKey, PreparedCommit,
    SchemaChange, TableMetadata, TableWriter, WriterError,
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
        pg_schema: None,
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
    /// The unit being built starts in a log range the catalog says a
    /// commit already applied, up to `end` (see [`LogRange`]): its cursor
    /// update never happened. Handled within a cycle, never returned.
    #[error("log already applied up to offset {end}")]
    AlreadyApplied { end: u64 },
}

pub type Result<T> = std::result::Result<T, MaterializerError>;

#[async_trait]
pub trait MaterializerNamer: Send + Sync {
    /// Generate a unique blob path for a materialized file. `kind`
    /// is one of `"data"`, `"eq-delete"`, `"compact"`, `"meta"`,
    /// `"meta-marker"`. `partition_segment` is the Hive-style
    /// `col=val/col2=val2` path component for partitioned data /
    /// delete files; empty for unpartitioned tables and for
    /// non-data file kinds (meta, marker, etc.).
    ///
    /// Iceberg readers track file location explicitly in the
    /// manifest, so the layout below is convention rather than
    /// requirement — but every mainstream Iceberg writer (Spark,
    /// Trino, Flink) uses it, and some readers (older Spark,
    /// ClickHouse's `_path` virtual column) lean on it for partition
    /// pruning. Sticking to it keeps cross-engine debugging painless.
    async fn next_path(&self, table: &TableIdent, kind: &str, partition_segment: &str) -> String;

    /// The directory every path [`Self::next_path`] hands out for
    /// `table` lies under, and that holds nothing else — so orphan
    /// cleanup can treat any file in it that `table` doesn't reference
    /// as its own leftover.
    async fn table_dir(&self, table: &TableIdent) -> String;
}

/// Deterministic counter-based namer for the sim and tests. Never use it
/// in production: its counter restarts at 0 in every process, so after a
/// restart it hands out paths a previous process already committed — and
/// uploading to one overwrites a live data file. Production uses
/// [`UuidMaterializerNamer`].
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
    async fn next_path(&self, table: &TableIdent, kind: &str, partition_segment: &str) -> String {
        let n = self
            .counter
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        file_path(
            &self.base,
            table,
            kind,
            partition_segment,
            &format!("{n:010}"),
        )
    }

    async fn table_dir(&self, table: &TableIdent) -> String {
        table_dir(&self.base, table)
    }
}

/// Production namer: a random UUID per file, so paths never repeat —
/// not across restarts, and not between distributed workers.
pub struct UuidMaterializerNamer {
    id_gen: Arc<dyn IdGen>,
    base: String,
}

impl UuidMaterializerNamer {
    pub fn new(id_gen: Arc<dyn IdGen>, base: impl Into<String>) -> Self {
        Self {
            id_gen,
            base: base.into(),
        }
    }
}

#[async_trait]
impl MaterializerNamer for UuidMaterializerNamer {
    async fn next_path(&self, table: &TableIdent, kind: &str, partition_segment: &str) -> String {
        let id: String = self
            .id_gen
            .new_uuid()
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect();
        file_path(&self.base, table, kind, partition_segment, &id)
    }

    async fn table_dir(&self, table: &TableIdent) -> String {
        table_dir(&self.base, table)
    }
}

/// `<base>/<namespace>.<table>`: one directory per table, namespace
/// included, so same-named tables in different namespaces never share
/// one. (Files written before the namespace was added sit in
/// `<base>/<table>`; orphan cleanup leaves them alone.)
fn table_dir(base: &str, table: &TableIdent) -> String {
    format!("{}/{table}", base.trim_end_matches('/'))
}

/// Layout: `<table_dir>/data/[<col=val>/...]/<kind>-<id>.parquet`.
/// Data and eq-delete files share the `data/` subdirectory (Iceberg
/// writers always co-locate them); meta / compact / marker outputs go in
/// their own per-kind subdir.
fn file_path(
    base: &str,
    table: &TableIdent,
    kind: &str,
    partition_segment: &str,
    id: &str,
) -> String {
    let dir = table_dir(base, table);
    let kind_dir = match kind {
        "data" | "eq-delete" => "data",
        other => other,
    };
    if partition_segment.is_empty() {
        format!("{dir}/{kind_dir}/{kind}-{id}.parquet")
    } else {
        format!("{dir}/{kind_dir}/{partition_segment}/{kind}-{id}.parquet")
    }
}

/// Render the Hive-style `col1=val1/col2=val2` path segment for a
/// chunk's partition values. Empty when the table is unpartitioned
/// or the chunk has no partition values. Field names are taken from
/// `partition_spec` in declaration order; values are URL-encoded so
/// path-unsafe characters (`/`, `=`, spaces, `%`) survive a
/// round-trip through S3 + filesystem listings.
pub fn render_partition_segment(
    partition_spec: &[pg2iceberg_core::PartitionField],
    partition_values: &[pg2iceberg_core::PartitionLiteral],
) -> String {
    if partition_spec.is_empty() || partition_values.is_empty() {
        return String::new();
    }
    let mut out = String::new();
    for (i, (field, value)) in partition_spec
        .iter()
        .zip(partition_values.iter())
        .enumerate()
    {
        if i > 0 {
            out.push('/');
        }
        out.push_str(&hive_escape(&field.name));
        out.push('=');
        out.push_str(&hive_escape(&format_partition_literal(value)));
    }
    out
}

/// Format a `PartitionLiteral` as the string PG/Spark/Trino use in
/// Hive partition paths. Mirrors what the Iceberg spec calls
/// "partition transform string representation":
/// integers/booleans render as their literal text; null becomes
/// `__HIVE_DEFAULT_PARTITION__`; binary becomes lowercase hex.
fn format_partition_literal(v: &pg2iceberg_core::PartitionLiteral) -> String {
    use pg2iceberg_core::PartitionLiteral;
    match v {
        PartitionLiteral::Null => "__HIVE_DEFAULT_PARTITION__".to_string(),
        PartitionLiteral::Int(x) => x.to_string(),
        PartitionLiteral::Long(x) => x.to_string(),
        PartitionLiteral::Float(x) => format!("{}", x.0),
        PartitionLiteral::Double(x) => format!("{}", x.0),
        PartitionLiteral::String(s) => s.clone(),
        PartitionLiteral::Boolean(b) => b.to_string(),
        // Iceberg's `PartitionPath.escape` renders `byte[]` values as
        // standard Base64 (RFC 4648, with `=` padding). The `hive_escape`
        // step below URL-escapes any `/` or `+` characters that would
        // be unsafe in path segments. Hex would be simpler but doesn't
        // match what Spark / Trino / iceberg-rust readers produce.
        PartitionLiteral::Binary(b) => base64_standard(b),
        PartitionLiteral::Decimal { unscaled, scale } => {
            // Render as decimal-with-point so reads can round-trip
            // back without ambiguity. Scale=0 → integer literal.
            if *scale == 0 {
                unscaled.to_string()
            } else {
                let abs = unscaled.unsigned_abs();
                let s = abs.to_string();
                let scale_usize = *scale as usize;
                let sign = if *unscaled < 0 { "-" } else { "" };
                if s.len() > scale_usize {
                    let split = s.len() - scale_usize;
                    format!("{sign}{}.{}", &s[..split], &s[split..])
                } else {
                    let pad = scale_usize - s.len();
                    format!("{sign}0.{}{}", "0".repeat(pad), s)
                }
            }
        }
    }
}

/// Standard RFC 4648 Base64 encoder. Inlined here rather than
/// pulling in the `base64` crate because the only callers are
/// partition-path rendering (a few bytes per call); that doesn't
/// merit a transitive dep.
fn base64_standard(bytes: &[u8]) -> String {
    const ALPHABET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(bytes.len().div_ceil(3) * 4);
    let mut chunks = bytes.chunks_exact(3);
    for c in chunks.by_ref() {
        let n = ((c[0] as u32) << 16) | ((c[1] as u32) << 8) | (c[2] as u32);
        out.push(ALPHABET[((n >> 18) & 0x3f) as usize] as char);
        out.push(ALPHABET[((n >> 12) & 0x3f) as usize] as char);
        out.push(ALPHABET[((n >> 6) & 0x3f) as usize] as char);
        out.push(ALPHABET[(n & 0x3f) as usize] as char);
    }
    let rem = chunks.remainder();
    match rem.len() {
        1 => {
            let n = (rem[0] as u32) << 16;
            out.push(ALPHABET[((n >> 18) & 0x3f) as usize] as char);
            out.push(ALPHABET[((n >> 12) & 0x3f) as usize] as char);
            out.push('=');
            out.push('=');
        }
        2 => {
            let n = ((rem[0] as u32) << 16) | ((rem[1] as u32) << 8);
            out.push(ALPHABET[((n >> 18) & 0x3f) as usize] as char);
            out.push(ALPHABET[((n >> 12) & 0x3f) as usize] as char);
            out.push(ALPHABET[((n >> 6) & 0x3f) as usize] as char);
            out.push('=');
        }
        _ => {}
    }
    out
}

/// Percent-encode characters that aren't safe in S3 / filesystem
/// path segments. Mirrors what Spark / Trino / Hive use for
/// partition path encoding so paths are interchangeable.
fn hive_escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '/' | '=' | '%' | '\\' | '"' | '\'' | ' ' | ':' | '?' | '#' | '[' | ']' | '+' => {
                let mut buf = [0u8; 4];
                for b in c.encode_utf8(&mut buf).bytes() {
                    out.push_str(&format!("%{b:02X}"));
                }
            }
            _ => out.push(c),
        }
    }
    out
}

struct TableEntry {
    schema: TableSchema,
    pk_cols: Vec<ColumnName>,
    file_index: FileIndex,
    /// The table's snapshot `file_index` reflects (`None`: before its
    /// first). Another process committing moves the table past it.
    index_at: Option<i64>,
    /// Cached writer; `TableWriter::new` precomputes Arrow schemas once.
    writer: TableWriter,
    /// When `true`, `cycle()` skips this table — its CDC events
    /// accumulate in `log_index` but are NOT applied to Iceberg
    /// until a snapshot task calls [`Materializer::mark_snapshot_complete`].
    /// Set by [`Materializer::register_table_pending`] for tables
    /// that need a backfill snapshot before any CDC apply (the
    /// mid-stream-add path). Defaults to `false` for the legacy
    /// [`Materializer::register_table`] entry, preserving existing
    /// behavior where the lifecycle gates the whole snapshot phase.
    gated_until_snapshot: bool,
    /// A backfill staged alongside the table's changes.
    backfill: Backfill,
}

/// A table backfilled mid-stream ([`Materializer::register_table_pending`])
/// has its snapshot staged into its log alongside its changes, in no
/// useful order: a change streamed before the backfill reached its row
/// lands ahead of the row's older snapshot copy, and applied in log order
/// the old copy would win (or bring back a deleted row, or leave an
/// unchanged TOAST value nothing to resolve from). So its snapshot rows
/// are applied first — tracked by a cursor of their own, under
/// [`Materializer::snapshot_group`] — and then its changes, less those
/// the snapshot already reflects.
#[derive(Clone, Copy, Debug, PartialEq)]
enum Backfill {
    /// Not looked up yet.
    Unknown,
    /// None to apply.
    Absent,
    /// Snapshot rows to apply, from this log offset on (-1: the start).
    Applying(i64),
    /// Snapshot rows applied. Changes that committed before `since` — the
    /// first snapshot's LSN (a backfill resumed after a restart takes a
    /// newer one), so every snapshot row reflects them — are skipped.
    Applied { since: Option<Lsn> },
}

/// The snapshot cursor's value once a backfill's rows are all applied.
const SNAPSHOT_APPLIED: i64 = i64::MAX;

/// Log entries fetched per `read_log` call while a cycle drains a table.
const LOG_PAGE_ENTRIES: usize = 64;
/// Default `cycle_rows`, in multiples of `batch_rows`.
const DEFAULT_STEPS_PER_CYCLE: usize = 8;

/// Steps read but not yet committed. Committed together, as one atomic
/// `commit_snapshots`, so a transaction spanning several steps becomes
/// visible at once.
#[derive(Default)]
struct Unit {
    /// Steps already folded and uploaded.
    steps: Vec<PreparedCommit>,
    /// Decoded events of the step being built.
    buf: Vec<MatEvent>,
    /// Log offset of the first entry consumed.
    start_offset: Option<u64>,
    /// Log offset just past the last entry consumed.
    end_offset: Option<u64>,
    folded: usize,
    xids: BTreeSet<u32>,
    max_lsn: i64,
    max_source_ts_micros: i64,
    /// Snapshot rows of a backfill: committing advances the snapshot
    /// cursor (see [`Backfill`]).
    snapshot: bool,
    /// Columns filled with their default ([`FILL_PROPERTY`]), by field id.
    filled: BTreeSet<i32>,
    /// Table properties the commit removes.
    remove_properties: BTreeSet<String>,
}

/// Table property prefix marking a column — `pg2iceberg.fill.<field id>`
/// — added with a default Postgres stores for the rows that predate it,
/// which carry no WAL for it: the value, as JSON, to fill into the rows
/// the table held when the column was added. Set with the column, and
/// removed by the commit of the fill, so a crash between the two leaves
/// the fill to do again, and a column is filled once.
const FILL_PROPERTY: &str = "pg2iceberg.fill.";

/// Whether a cut between `last` (end of what's buffered) and `next`
/// (start of the next log entry) falls between transactions. A spilled
/// transaction's chunks are consecutive in the log and share its xid; a
/// replayed duplicate also carries the same xid, so it's kept with the
/// original.
fn is_tx_boundary(last: Option<&MatEvent>, next: Option<&MatEvent>) -> bool {
    match (last.and_then(|e| e.xid), next.and_then(|e| e.xid)) {
        (Some(a), Some(b)) => a != b,
        _ => true,
    }
}

pub struct Materializer<C: Catalog> {
    coord: Arc<dyn Coordinator>,
    blob_store: Arc<dyn BlobStore>,
    /// Every read and write of the tables' Iceberg metadata goes through
    /// it: see [`CachingCatalog`].
    catalog: Arc<CachingCatalog<C>>,
    namer: Arc<dyn MaterializerNamer>,
    tables: BTreeMap<TableIdent, TableEntry>,
    group: String,
    /// Most change events folded into one snapshot step — the bound on
    /// what a cycle holds in memory, however large a transaction is.
    batch_rows: usize,
    /// Change events a cycle reads before it stops between commits (it
    /// always finishes the transaction it's in). Lets one cycle catch up
    /// through many steps instead of one batch per tick.
    cycle_rows: usize,
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
        batch_rows: usize,
    ) -> Self {
        Self::with_metrics(
            coord,
            blob_store,
            catalog,
            namer,
            group,
            batch_rows,
            Arc::new(NoopMetrics),
        )
    }

    pub fn with_metrics(
        coord: Arc<dyn Coordinator>,
        blob_store: Arc<dyn BlobStore>,
        catalog: Arc<C>,
        namer: Arc<dyn MaterializerNamer>,
        group: impl Into<String>,
        batch_rows: usize,
        metrics: Arc<dyn Metrics>,
    ) -> Self {
        assert!(batch_rows > 0);
        Self {
            catalog: Arc::new(CachingCatalog::new(catalog, coord.clone())),
            coord,
            blob_store,
            namer,
            tables: BTreeMap::new(),
            group: group.into(),
            batch_rows,
            cycle_rows: batch_rows.saturating_mul(DEFAULT_STEPS_PER_CYCLE),
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
        // Same blob-store hook as `register_table` — for vended-creds
        // mode, the marker table needs its own per-table credentials.
        self.blob_store
            .register_table(&schema.ident)
            .await
            .map_err(|e| MaterializerError::Catalog(IcebergError::Other(e.to_string())))?;
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
            // Vended-creds blob: register the meta tables too — they
            // each get their own STS-vended creds. Static-creds blob
            // ignores.
            self.blob_store
                .register_table(&s.ident)
                .await
                .map_err(|e| MaterializerError::Catalog(IcebergError::Other(e.to_string())))?;
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
            unchanged_from: None,
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
            unchanged_from: None,
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
            unchanged_from: None,
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
            unchanged_from: None,
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
                // Meta tables are partitioned by `day(ts)` per
                // `pg2iceberg-iceberg::meta`; render the partition
                // segment so files land under the right directory.
                let segment =
                    render_partition_segment(&state.schema.partition_spec, &chunk.partition_values);
                let path = self
                    .namer
                    .next_path(&state.schema.ident, "meta", &segment)
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
                    sequence_number: None,
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
        // An existing table keeps its Iceberg schema — the columns and
        // field ids the stream has evolved it to. `schema`'s columns come
        // from discovery, which numbers them by position: after a column
        // is dropped, the ones behind it would take others' field ids and
        // land under the wrong Iceberg column. Nor does discovery's view
        // of the source's *current* columns apply yet: the stream may
        // still replay transactions from before a change. Its Relation
        // messages, staged in the log, evolve the schema in order.
        let schema = match self.catalog.load_table(&ident).await? {
            None => {
                self.catalog.create_table(&schema).await?;
                schema
            }
            Some(meta) => TableSchema {
                columns: meta.schema.columns,
                ..schema
            },
        };
        // Notify the blob store that the table now exists in the
        // catalog. Static-creds backends ignore this; the
        // vended-credentials router uses it to load per-table STS
        // creds and add an entry to its lookup table on the fly.
        // Idempotent — safe to call repeatedly. Errors propagate as
        // catalog errors so a vended-creds misconfig surfaces here
        // (the same place catalog/coord errors do) instead of deeper
        // in the put path.
        self.blob_store
            .register_table(&ident)
            .await
            .map_err(|e| MaterializerError::Catalog(IcebergError::Other(e.to_string())))?;
        self.coord.ensure_cursor(&self.group, &ident).await?;

        let pk_cols: Vec<ColumnName> = schema
            .primary_key_columns()
            .map(|c| ColumnName(c.name.clone()))
            .collect();
        let writer = TableWriter::new(schema.clone());

        let mut file_index = FileIndex::new();
        let index_at = catch_up_from_catalog(
            &mut file_index,
            None,
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
                index_at,
                writer,
                gated_until_snapshot: false,
                backfill: Backfill::Unknown,
            },
        );
        Ok(())
    }

    /// Variant of [`Self::register_table`] for the mid-stream-add
    /// path: registers the table the same way, but marks it
    /// `gated_until_snapshot = true` so [`Self::cycle`] won't apply
    /// any of its CDC events to Iceberg until a backfill snapshot
    /// task calls [`Self::mark_snapshot_complete`].
    ///
    /// Used by `pg2iceberg_validate::run_logical_lifecycle` when it
    /// detects a configured table whose `coord.table_state` is not
    /// yet `snapshot_complete = true`. The lifecycle then spawns a
    /// background snapshot task; while that task runs, CDC events
    /// for this table land in `log_index` but stay un-materialized.
    /// Once the task finishes its final per-table flush and stamps
    /// the coord row, it calls `mark_snapshot_complete` to ungate
    /// materialization.
    pub async fn register_table_pending(&mut self, schema: TableSchema) -> Result<()> {
        self.register_table(schema.clone()).await?;
        // Its snapshot rows are to be applied ahead of its changes; this
        // cursor records that, durably, and tracks their progress.
        self.coord
            .ensure_cursor(&self.snapshot_group(), &schema.ident)
            .await?;
        if let Some(entry) = self.tables.get_mut(&schema.ident) {
            entry.gated_until_snapshot = true;
        }
        Ok(())
    }

    /// Lift the gate set by [`Self::register_table_pending`]. Called
    /// by the background snapshot task once the table's
    /// `coord.tables` row is stamped with `snapshot_complete = true`
    /// so the next `cycle()` picks the table up. No-op if the table
    /// wasn't gated (idempotent).
    pub fn mark_snapshot_complete(&mut self, ident: &TableIdent) {
        if let Some(entry) = self.tables.get_mut(ident) {
            entry.gated_until_snapshot = false;
        }
    }

    /// Apply a staged relation event: bring the table's Iceberg schema
    /// in line with the source columns it carries ([`reconcile_columns`]),
    /// and rebuild the in-memory `TableEntry::schema` and `TableWriter`
    /// from the result, so the rows after it encode with the new shape.
    ///
    /// - **AddColumn** for a new name, with a fresh field id.
    /// - **DropColumn** (soft) for a non-PK column the source lacks: it
    ///   stays, nullable, so older data files keep resolving.
    /// - **PromoteColumnType** for a legal Iceberg promotion (int→long,
    ///   float→double, decimal precision increase). Illegal type
    ///   changes (e.g. long→int, text→int) are rejected with
    ///   `MaterializerError::Catalog` so the table fails loudly rather
    ///   than silently truncate or coerce downstream readers.
    /// - **RenameColumn + AddColumn** for a column the source dropped
    ///   and re-added: Postgres gives it no values, so it's a new
    ///   column, and the dropped one keeps its values under another
    ///   name.
    ///
    /// - A column added with a default Postgres stores for the rows that
    ///   predate it is filled into the rows the table holds
    ///   ([`FILL_PROPERTY`]): they carry no WAL for it.
    ///
    /// Reconciles against the catalog's schema, not the in-memory one:
    /// the event may already have been applied — before a crash, whose
    /// retry re-reads it, or by another worker — and applying it again
    /// is then a no-op.
    async fn apply_columns(
        &mut self,
        ident: &TableIdent,
        unit: &mut Unit,
        at: &MatEvent,
        relation: &relation_event::Relation,
    ) -> Result<()> {
        let meta = self
            .catalog
            .load_table(ident)
            .await?
            .ok_or_else(|| MaterializerError::UnknownTable(ident.clone()))?;
        let changes = reconcile_columns(&meta.schema, &relation.columns)
            .map_err(MaterializerError::Catalog)?;
        let meta = if changes.is_empty() {
            meta
        } else {
            let marks = self.fill_marks(ident, &meta.schema, &changes, &relation.defaults)?;
            self.catalog.evolve_schema(ident, changes, marks).await?
        };
        let entry = self
            .tables
            .get_mut(ident)
            .ok_or_else(|| MaterializerError::UnknownTable(ident.clone()))?;
        entry.schema.columns = meta.schema.columns.clone();
        entry.writer = TableWriter::new(entry.schema.clone());
        self.fill_marked(ident, unit, at, &meta).await
    }

    /// The [`FILL_PROPERTY`] marks for the columns `changes` add to
    /// `schema` with a default Postgres stores, if the table holds rows to
    /// fill. An added column whose value Postgres no longer has is
    /// reported instead — a volatile default (which gave each row its
    /// own), or a table rewrite, drop or rename since the column was added
    /// — as those rows keep NULL for it.
    fn fill_marks(
        &self,
        ident: &TableIdent,
        schema: &TableSchema,
        changes: &[SchemaChange],
        defaults: &relation_event::Defaults,
    ) -> Result<BTreeMap<String, String>> {
        let mut marks = BTreeMap::new();
        let entry = self.tables.get(ident).expect("checked by caller");
        if entry.file_index.live_pk_count() == 0 {
            return Ok(marks);
        }
        let mut evolved = schema.clone();
        apply_schema_changes(&mut evolved, changes).map_err(MaterializerError::Catalog)?;
        let added = evolved
            .columns
            .iter()
            .filter(|c| !schema.columns.iter().any(|old| old.field_id == c.field_id));
        for col in added {
            let reason = match defaults.get(&col.name) {
                None => continue,
                Some(relation_event::DefaultValue::Stored(value)) => {
                    match relation_event::value_to_json(value) {
                        Some(json) => {
                            marks.insert(format!("{FILL_PROPERTY}{}", col.field_id), json);
                            continue;
                        }
                        None => "not_stored",
                    }
                }
                Some(relation_event::DefaultValue::NotStored) => "not_stored",
                Some(relation_event::DefaultValue::ColumnGone) => "column_gone",
            };
            if reason == "column_gone" {
                tracing::warn!(
                    table = %ident,
                    column = %col.name,
                    "column dropped or renamed in Postgres before pg2iceberg read its default: \
                     if it had one, the rows already in Iceberg keep NULL for it"
                );
            } else {
                tracing::warn!(
                    table = %ident,
                    column = %col.name,
                    "column added with a default Postgres doesn't store for existing rows \
                     (a volatile default, or the table was rewritten since): the rows \
                     already in Iceberg keep NULL for it"
                );
            }
            let mut labels = Labels::new();
            labels.insert("table".into(), ident.name.clone());
            labels.insert("column".into(), col.name.clone());
            labels.insert("reason".into(), reason.into());
            self.metrics
                .counter(names::UNFILLED_COLUMN_DEFAULTS, &labels, 1);
        }
        Ok(marks)
    }

    /// Fill in each column marked for it ([`FILL_PROPERTY`]), unless
    /// `unit` did already: the rows the table holds are staged again, with
    /// the column set, as updates after the rows before `at`. The unit's
    /// commit removes the marks. A mark on a column dropped since has
    /// nothing to fill.
    async fn fill_marked(
        &mut self,
        ident: &TableIdent,
        unit: &mut Unit,
        at: &MatEvent,
        meta: &TableMetadata,
    ) -> Result<()> {
        for (key, json) in &meta.properties {
            let Some(field_id) = key
                .strip_prefix(FILL_PROPERTY)
                .and_then(|id| id.parse::<i32>().ok())
            else {
                continue;
            };
            if unit.filled.contains(&field_id) {
                continue;
            }
            let col = meta.schema.columns.iter().find(|c| c.field_id == field_id);
            if let Some(col) = col.filter(|c| !is_dropped_column(c)) {
                let value = relation_event::value_from_json(json).ok_or_else(|| {
                    MaterializerError::Catalog(IcebergError::Other(format!(
                        "{ident}: malformed table property {key}: {json}"
                    )))
                })?;
                self.fill_column(ident, unit, at, ColumnName(col.name.clone()), value)
                    .await?;
                unit.filled.insert(field_id);
            }
            unit.remove_properties.insert(key.clone());
        }
        Ok(())
    }

    /// Stage every row the table holds again, with `column` set to
    /// `value`, as updates after the rows before `at` — in steps of at most
    /// `batch_rows`, like any transaction, and decoding as many at a time.
    async fn fill_column(
        &mut self,
        ident: &TableIdent,
        unit: &mut Unit,
        at: &MatEvent,
        column: ColumnName,
        value: PgValue,
    ) -> Result<()> {
        let entry = self.tables.get(ident).expect("checked by caller");
        let files: Vec<String> = entry
            .file_index
            .live_files()
            .into_iter()
            .map(str::to_string)
            .collect();
        let (columns, pk_cols) = (entry.schema.columns.clone(), entry.pk_cols.clone());
        for path in files {
            let bytes = self.blob_store.get(&path).await?;
            for batch in RowBatches::new(bytes, &columns, self.batch_rows.max(1))? {
                for mut row in batch? {
                    // A row deleted or updated since lives elsewhere, or
                    // nowhere.
                    let entry = self.tables.get(ident).expect("checked by caller");
                    let key = PkKey::from_row(&row, &pk_cols);
                    if entry.file_index.lookup(&key) != Some(path.as_str()) {
                        continue;
                    }
                    row.insert(column.clone(), value.clone());
                    unit.buf.push(MatEvent {
                        op: Op::Update,
                        lsn: at.lsn,
                        commit_ts: at.commit_ts,
                        xid: at.xid,
                        unchanged_cols: Vec::new(),
                        row,
                        moved_from: None,
                    });
                    if unit.buf.len() >= self.batch_rows {
                        self.prepare_step(ident, unit).await?;
                    }
                }
            }
        }
        Ok(())
    }

    /// The `mat_cursor` group of a backfill's snapshot rows (see
    /// [`Backfill`]).
    fn snapshot_group(&self) -> String {
        format!("{}#snapshot", self.group)
    }

    /// The `mat_cursor` group `unit` advances.
    fn unit_group(&self, unit: &Unit) -> String {
        if unit.snapshot {
            self.snapshot_group()
        } else {
            self.group.clone()
        }
    }

    /// The table's [`Backfill`], looked up once.
    async fn backfill(&mut self, ident: &TableIdent) -> Result<Backfill> {
        let known = self.tables.get(ident).map(|e| e.backfill);
        if let Some(b) = known.filter(|b| *b != Backfill::Unknown) {
            return Ok(b);
        }
        let backfill = match self.coord.get_cursor(&self.snapshot_group(), ident).await? {
            None => Backfill::Absent,
            Some(SNAPSHOT_APPLIED) => Backfill::Applied {
                since: self.first_snapshot_lsn(ident).await?,
            },
            Some(at) => Backfill::Applying(at),
        };
        if let Some(entry) = self.tables.get_mut(ident) {
            entry.backfill = backfill;
        }
        Ok(backfill)
    }

    /// The LSN of the table's first snapshot row: the earliest snapshot,
    /// since backfill runs stage in order.
    async fn first_snapshot_lsn(&self, ident: &TableIdent) -> Result<Option<Lsn>> {
        let mut after = 0;
        loop {
            let entries = self.coord.read_log(ident, after, LOG_PAGE_ENTRIES).await?;
            if entries.is_empty() {
                return Ok(None);
            }
            for e in entries {
                let bytes = self.blob_store.get(&e.s3_path).await?;
                let first = decode_chunk(&bytes)?
                    .into_iter()
                    .find(|evt| is_snapshot_xid(evt.xid));
                if let Some(evt) = first {
                    return Ok(Some(evt.lsn));
                }
                after = e.end_offset;
            }
        }
    }

    /// Apply a backfill's snapshot rows ahead of the table's changes: read
    /// the log on from `from`, folding only snapshot rows, to its end —
    /// the backfill is complete, so no more come. They're folded in
    /// bounded steps, like a large transaction, and committed together:
    /// the table goes from empty to its snapshot at once, never shown
    /// half loaded. A crash before the commit re-applies them, which
    /// changes nothing.
    async fn apply_snapshot_rows(&mut self, ident: &TableIdent, from: i64) -> Result<usize> {
        let folded = match self.fold_snapshot_rows(ident, from).await {
            // Their commit landed; marking them applied didn't happen.
            Err(MaterializerError::AlreadyApplied { .. }) => 0,
            result => result?,
        };
        self.coord
            .set_cursor(&self.snapshot_group(), ident, SNAPSHOT_APPLIED)
            .await?;
        let since = self.first_snapshot_lsn(ident).await?;
        if let Some(entry) = self.tables.get_mut(ident) {
            entry.backfill = Backfill::Applied { since };
        }
        Ok(folded)
    }

    async fn fold_snapshot_rows(&mut self, ident: &TableIdent, from: i64) -> Result<usize> {
        let mut after = from.max(0) as u64;
        let mut unit = Unit {
            snapshot: true,
            ..Unit::default()
        };
        loop {
            let entries = self.coord.read_log(ident, after, LOG_PAGE_ENTRIES).await?;
            if entries.is_empty() {
                break;
            }
            for e in entries {
                let bytes = self.blob_store.get(&e.s3_path).await?;
                let rows: Vec<MatEvent> = decode_chunk(&bytes)?
                    .into_iter()
                    .filter(|evt| is_snapshot_xid(evt.xid))
                    .collect();
                if unit.start_offset.is_none() {
                    self.begin_unit(ident, &mut unit, e.start_offset).await?;
                }
                if !unit.buf.is_empty() && unit.buf.len() + rows.len() > self.batch_rows {
                    self.prepare_step(ident, &mut unit).await?;
                }
                unit.buf.extend(rows);
                unit.end_offset = Some(e.end_offset);
                after = e.end_offset;
            }
        }
        self.commit_unit(ident, &mut unit).await
    }

    /// Buffer a log entry's events into `unit`, applying its relation
    /// events in place: rows before one are staged under the old schema
    /// first.
    async fn buffer_entry(
        &mut self,
        ident: &TableIdent,
        unit: &mut Unit,
        events: Vec<MatEvent>,
    ) -> Result<()> {
        let backfill = self.tables.get(ident).map(|e| e.backfill);
        for evt in events {
            if let Some(Backfill::Applied { since }) = backfill {
                // Snapshot rows went first; and a change the snapshot
                // already reflects would turn its row back.
                let reflected =
                    evt.op != Op::Relation && since.is_some_and(|since| evt.lsn < since);
                if is_snapshot_xid(evt.xid) || reflected {
                    continue;
                }
            }
            if evt.op != Op::Relation {
                unit.buf.push(evt);
                continue;
            }
            if !unit.buf.is_empty() {
                self.prepare_step(ident, unit).await?;
            }
            let relation = relation_event::decode(&evt.row).ok_or_else(|| {
                MaterializerError::Blob(StreamError::Decode(format!(
                    "malformed relation event for {ident}: {:?}",
                    evt.row
                )))
            })?;
            self.apply_columns(ident, unit, &evt, &relation).await?;
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

    /// Keep nothing longer than `ttl` by `clock` of what was read of the
    /// tables' Iceberg metadata, besides dropping it once another process
    /// writes them: the bound on staleness should a writer die before
    /// recording its write. See [`CachingCatalog`].
    pub fn set_catalog_cache_ttl(
        &self,
        clock: Arc<dyn pg2iceberg_core::Clock>,
        ttl: std::time::Duration,
    ) {
        self.catalog.set_ttl(clock, ttl);
    }

    pub async fn cycle(&mut self) -> Result<usize> {
        // Forget what other processes changed since the last cycle.
        self.catalog.sync().await;
        // 1. Default path: process every registered table whose
        //    backfill-snapshot gate is open. Tables registered via
        //    [`Self::register_table_pending`] (mid-stream additions
        //    waiting on a background snapshot task) are filtered
        //    out — their CDC events stay durable in `log_index` but
        //    don't reach Iceberg until the task calls
        //    [`Self::mark_snapshot_complete`]. Without this, a table
        //    would receive Update/Delete CDC events for rows that
        //    don't yet exist in Iceberg and silently materialize
        //    phantoms.
        let mut idents: Vec<TableIdent> = self
            .tables
            .iter()
            .filter(|(_, e)| !e.gated_until_snapshot)
            .map(|(t, _)| t.clone())
            .collect();

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
        self.catalog.sync().await;
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
    /// Each table's scope is its own directory, as the materializer's
    /// namer lays it out ([`MaterializerNamer::table_dir`]) — never a
    /// directory another table writes to. `now_ms` is the current
    /// wall-clock time (or test clock); orphans younger than
    /// `now_ms - grace_period_ms` are protected.
    ///
    /// Per-table outcomes are also recorded into `<meta_ns>.maintenance`
    /// when meta recording is enabled, with `operation =
    /// "clean_orphans"`. Meta-flush errors are logged and swallowed so
    /// observability blips don't block cleanup retries.
    pub async fn cleanup_orphans_cycle(
        &mut self,
        now_ms: i64,
        grace_period_ms: i64,
    ) -> Result<Vec<(TableIdent, pg2iceberg_iceberg::CleanupOutcome)>> {
        self.catalog.sync().await;
        let idents: Vec<TableIdent> = self.tables.keys().cloned().collect();
        let mut out = Vec::new();
        for ident in idents {
            // Trailing `/`: `ns.orders/` must not match `ns.orders2/`.
            let table_prefix = format!("{}/", self.namer.table_dir(&ident).await);
            let started_micros = now_micros();
            // Uncached: it deletes what the table doesn't reference.
            let outcome = pg2iceberg_iceberg::cleanup_orphans(
                self.catalog.uncached(),
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
        self.catalog.sync().await;
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
    /// The materializer's `FileIndex` for `ident` (tests compare it with a
    /// rebuild from the catalog).
    pub fn file_index(&self, ident: &TableIdent) -> Option<&FileIndex> {
        self.tables.get(ident).map(|t| &t.file_index)
    }

    /// The table's snapshot its FileIndex reflects (`None`: before its
    /// first). Behind the table's current one, the index hasn't caught up
    /// with another process's commits yet — it does before it's used.
    pub fn file_index_snapshot(&self, ident: &TableIdent) -> Option<i64> {
        self.tables.get(ident).and_then(|t| t.index_at)
    }

    pub async fn compact_table(
        &mut self,
        ident: &TableIdent,
        config: &pg2iceberg_iceberg::CompactionConfig,
    ) -> Result<Option<pg2iceberg_iceberg::CompactionOutcome>> {
        if !self.tables.contains_key(ident) {
            return Err(MaterializerError::UnknownTable(ident.clone()));
        }
        // The pass picks files by the index's live counts.
        self.sync_file_index(ident).await?;
        let entry = self.tables.get(ident).expect("checked above");
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
                // Compaction always rewrites whole partitions, so
                // we don't yet thread per-partition compaction
                // outputs into separate dirs. The empty segment
                // groups all compaction outputs under
                // `<table>/compact/`.
                async move { n.next_path(&t, "compact", "").await }
            },
            ident,
            &schema,
            &pk_cols,
            // Lets the pass find files with deleted rows without reading
            // the table; the index tracks exactly this catalog's state.
            Some(&entry.file_index),
            config,
        )
        .await;
        let outcome = match outcome {
            Ok(outcome) => outcome,
            Err(e) => {
                // The commit may have applied all the same (its response
                // lost). If the index can't learn that now, the next sync
                // will.
                if let Err(sync) = self.sync_file_index(ident).await {
                    tracing::warn!(error = %sync, table = %ident, "FileIndex sync failed");
                }
                return Err(MaterializerError::Compact(e.to_string()));
            }
        };

        if let Some(o) = &outcome {
            let entry_mut = self.tables.get_mut(ident).expect("checked above");
            if o.snapshot_id == Some(entry_mut.index_at.unwrap_or(0) + 1) {
                // Remap what the pass rewrote rather than replaying its
                // snapshot, which would re-read the outputs. Outputs
                // first: they take over every live PK of the inputs, so
                // removing the inputs then has nothing left to scan for.
                for f in &o.added_files {
                    entry_mut.file_index.add_file(
                        f.path.clone(),
                        f.pk_keys.clone(),
                        f.partition_values.clone(),
                    );
                }
                for path in &o.rewritten_files {
                    entry_mut.file_index.remove_file(path);
                }
                entry_mut.index_at = o.snapshot_id;
            } else {
                // Another process committed during the pass.
                self.catch_up_file_index(ident).await?;
            }

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

    /// Override how many change events one cycle reads before stopping
    /// between commits. Defaults to `batch_rows` × 8.
    pub fn with_cycle_rows(mut self, cycle_rows: usize) -> Self {
        assert!(cycle_rows > 0);
        self.cycle_rows = cycle_rows;
        self
    }

    /// Materialize pending log entries for one table. Entries are folded
    /// in steps of at most `batch_rows` events; steps are committed
    /// together, as one atomic `commit_snapshots`, up to a transaction
    /// boundary — so a transaction spanning many steps is never partly
    /// visible, and memory stays bounded however large it is. Keeps
    /// committing until the log is drained or `cycle_rows` events were
    /// read. Returns rows folded.
    pub async fn cycle_table(&mut self, ident: &TableIdent) -> Result<usize> {
        let mut labels = Labels::new();
        labels.insert("table".into(), ident.name.clone());
        self.metrics
            .counter(names::MATERIALIZER_CYCLE_TOTAL, &labels, 1);
        if !self.tables.contains_key(ident) {
            return Err(MaterializerError::UnknownTable(ident.clone()));
        }
        if let Backfill::Applying(from) = self.backfill(ident).await? {
            // Only once the backfill has staged every row — its rows are
            // done at the log's end — whatever the gate says.
            let complete = self
                .coord
                .table_state(ident)
                .await?
                .is_some_and(|t| t.snapshot_complete);
            if !complete {
                return Ok(0);
            }
            let folded = self.apply_snapshot_rows(ident, from).await?;
            // None folded — already applied by a commit whose marking
            // never happened, or none to apply: on to the changes.
            if folded > 0 {
                return Ok(folded);
            }
        }
        loop {
            match self.apply_changes(ident).await {
                // Skip what landed, and apply what's past it.
                Err(MaterializerError::AlreadyApplied { end }) => {
                    self.coord
                        .set_cursor(&self.group, ident, end as i64)
                        .await?;
                }
                result => return result,
            }
        }
    }

    /// Fold and commit the table's changes past its cursor, in units.
    async fn apply_changes(&mut self, ident: &TableIdent) -> Result<usize> {
        let cursor = self
            .coord
            .get_cursor(&self.group, ident)
            .await?
            .unwrap_or(-1);
        let mut after = if cursor < 0 { 0 } else { cursor as u64 };
        let mut unit = Unit::default();
        let mut events_read = 0usize;
        let mut folded = 0usize;
        let mut read_any = false;
        'read: loop {
            let entries = self.coord.read_log(ident, after, LOG_PAGE_ENTRIES).await?;
            if entries.is_empty() {
                break;
            }
            read_any = true;
            for e in entries {
                let bytes = self.blob_store.get(&e.s3_path).await?;
                let events = decode_chunk(&bytes)?;
                // A schema change starts its entry (the pipeline stages it
                // so). Commit the rows before it — written under the old
                // schema — before applying it: a crash in between then
                // re-reads this entry, and applying the change again is a
                // no-op. Within a transaction (DDL mid-transaction) the
                // rows so far can only be staged as a step.
                if events.first().is_some_and(|e| e.op == Op::Relation) {
                    if is_tx_boundary(unit.buf.last(), events.first()) {
                        folded += self.commit_unit(ident, &mut unit).await?;
                    } else {
                        self.prepare_step(ident, &mut unit).await?;
                    }
                }
                if !unit.buf.is_empty() && unit.buf.len() + events.len() > self.batch_rows {
                    if is_tx_boundary(unit.buf.last(), events.first()) {
                        folded += self.commit_unit(ident, &mut unit).await?;
                        if events_read >= self.cycle_rows {
                            // Leave this entry for the next cycle.
                            break 'read;
                        }
                    } else {
                        // A transaction spans this boundary: stage what's
                        // buffered as an intermediate step and keep going.
                        self.prepare_step(ident, &mut unit).await?;
                    }
                }
                events_read += events.len();
                if unit.start_offset.is_none() {
                    self.begin_unit(ident, &mut unit, e.start_offset).await?;
                }
                self.buffer_entry(ident, &mut unit, events).await?;
                unit.end_offset = Some(e.end_offset);
                after = e.end_offset;
            }
        }

        if !read_any {
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
        // The end of the claimed log is always a transaction boundary:
        // claims hold whole transactions.
        folded += self.commit_unit(ident, &mut unit).await?;
        Ok(folded)
    }

    /// Start `unit` at the log entry at offset `start`, before applying
    /// any of it — its schema changes apply as they're read: catch the
    /// FileIndex up with other processes' commits, and check that no
    /// commit already applied the log from here. One that did, whose
    /// cursor update never happened, would be applied twice — against
    /// the rows and schema it changed.
    async fn begin_unit(&mut self, ident: &TableIdent, unit: &mut Unit, start: u64) -> Result<()> {
        unit.start_offset = Some(start);
        // The index is untouched since `index_at`.
        let applied = self.sync_file_index(ident).await?;
        if let Some(&end) = applied.get(&self.unit_group(unit)) {
            if end > start {
                return Err(MaterializerError::AlreadyApplied { end });
            }
        }
        Ok(())
    }

    /// Fold the buffered events into one snapshot step: upload its data
    /// and equality-delete files and add the step to `unit`. FileIndex,
    /// caught up with other processes' commits as the unit began
    /// ([`Self::begin_unit`]), is updated now, as if committed, so later
    /// steps of the same unit promote re-inserts and resolve TOAST against
    /// these rows; [`Self::commit_unit`] rebuilds it if the commit fails.
    async fn prepare_step(&mut self, ident: &TableIdent, unit: &mut Unit) -> Result<()> {
        let events = std::mem::take(&mut unit.buf);
        if events.is_empty() {
            return Ok(());
        }
        // Observability stats come from the raw events, before the fold
        // collapses them to one row per PK.
        for e in &events {
            unit.max_lsn = unit.max_lsn.max(e.lsn.0 as i64);
            unit.max_source_ts_micros = unit.max_source_ts_micros.max(e.commit_ts.0);
            if let Some(x) = e.xid {
                unit.xids.insert(x);
            }
        }

        let entry = self
            .tables
            .get(ident)
            .ok_or_else(|| MaterializerError::UnknownTable(ident.clone()))?;
        // Expand any TRUNCATE events into per-PK deletes against the
        // current FileIndex. A `TRUNCATE` in PG drops every row; in
        // Iceberg we model that as one equality-delete per known PK so the
        // next snapshot's MoR reads return zero rows. Subsequent same-tx
        // INSERTs survive the fold's last-write-wins on the same PK.
        let events = expand_truncates(events, &entry.file_index, &entry.schema);

        // Fold + TOAST + re-insert. Pre-fetch any prior data files needed
        // for TOAST resolution; that's cheap when no UPDATE has unchanged_cols.
        let mut folded = fold_events(events, &entry.pk_cols);
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
            return Ok(());
        }
        unit.folded += folded.len();

        // Prepare + upload. Output is one parquet file per (partition,
        // kind) — for unpartitioned tables that's a single file per kind.
        // The writer consults FileIndex for tier-2 resolution of
        // partition tuples on `Delete` rows whose row payload doesn't
        // carry the partition source columns.
        let prepared_files = entry.writer.prepare(&folded, &entry.file_index)?;
        let pk_field_ids = entry.writer.pk_field_ids();

        // Track per-data-file which PKs ended up where, so the FileIndex
        // update below points at the right partition file.
        let mut data_files: Vec<DataFile> = Vec::with_capacity(prepared_files.data.len());
        let mut data_pk_groups: Vec<(String, Vec<PkKey>, Vec<pg2iceberg_core::PartitionLiteral>)> =
            Vec::with_capacity(prepared_files.data.len());
        let mut deleted_pks: Vec<PkKey> = Vec::new();

        for chunk in prepared_files.data {
            let segment =
                render_partition_segment(&entry.schema.partition_spec, &chunk.partition_values);
            let path = self.namer.next_path(ident, "data", &segment).await;
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
                sequence_number: None,
            });
            data_pk_groups.push((path, chunk.pk_keys, chunk.partition_values));
        }

        let mut delete_files: Vec<DataFile> =
            Vec::with_capacity(prepared_files.equality_deletes.len());
        for chunk in prepared_files.equality_deletes {
            let segment =
                render_partition_segment(&entry.schema.partition_spec, &chunk.partition_values);
            let path = self.namer.next_path(ident, "eq-delete", &segment).await;
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
                sequence_number: None,
            });
            deleted_pks.extend(chunk.pk_keys);
        }

        let entry_mut = self.tables.get_mut(ident).expect("checked above");
        // Removed PKs first (so a later add for the same PK overrides cleanly).
        entry_mut.file_index.remove_pks(&deleted_pks);
        for (path, pks, partition_values) in data_pk_groups {
            entry_mut.file_index.add_file(path, pks, partition_values);
        }
        unit.steps.push(PreparedCommit {
            ident: ident.clone(),
            data_files,
            equality_deletes: delete_files,
        });
        Ok(())
    }

    /// Commit every step of `unit` as one atomic table update, then
    /// advance the cursor past it. Returns rows folded.
    async fn commit_unit(&mut self, ident: &TableIdent, unit: &mut Unit) -> Result<usize> {
        self.prepare_step(ident, unit).await?;
        let unit = std::mem::take(unit);
        let Some(end_offset) = unit.end_offset else {
            return Ok(0);
        };
        let files = || unit.steps.iter().flat_map(|s| s.data_files.iter());
        let data_files_count = files().count() as i32;
        let delete_files_count = unit
            .steps
            .iter()
            .map(|s| s.equality_deletes.len())
            .sum::<usize>() as i32;
        let bytes_written: i64 = unit
            .steps
            .iter()
            .flat_map(|s| s.data_files.iter().chain(s.equality_deletes.iter()))
            .map(|f| f.byte_size as i64)
            .sum();

        // The commit adds one snapshot per step with files.
        let snapshots = unit
            .steps
            .iter()
            .filter(|s| !s.data_files.is_empty() || !s.equality_deletes.is_empty())
            .count() as i64;

        let group = self.unit_group(&unit);
        let log_range = LogRange {
            group: group.clone(),
            start: unit.start_offset.unwrap_or(end_offset),
            end: end_offset,
        };

        // Commit catalog snapshots — durability gate.
        let started_micros = now_micros();
        let committed = if unit.steps.is_empty() {
            None
        } else {
            match self
                .catalog
                .commit_snapshots(unit.steps, Some(log_range), unit.remove_properties)
                .await
            {
                Ok(meta) => {
                    // FileIndex holds the steps on top of `index_at`; their
                    // snapshots must follow it directly, or another process
                    // committed in between.
                    let entry = self.tables.get_mut(ident).expect("checked by caller");
                    let expected = match snapshots {
                        0 => entry.index_at,
                        n => Some(entry.index_at.unwrap_or(0) + n),
                    };
                    if meta.current_snapshot_id == expected {
                        entry.index_at = expected;
                    } else {
                        self.rebuild_file_index(ident).await?;
                    }
                    Some(meta)
                }
                Err(e) => {
                    // FileIndex already reflects the uncommitted steps;
                    // restore it to the catalog's truth before surfacing
                    // the error, so a retry doesn't fold against rows that
                    // never landed.
                    self.rebuild_file_index(ident).await?;
                    // Unless they did, the response lost: then carry on as
                    // committed, rather than fail and have the next cycle
                    // find out.
                    let applied = self
                        .catalog
                        .load_table(ident)
                        .await?
                        .and_then(|meta| meta.log_ends.get(&group).copied());
                    if !applied.is_some_and(|end| end >= end_offset) {
                        return Err(e.into());
                    }
                    tracing::warn!(error = %e, table = %ident, "commit reported failure but landed");
                    None
                }
            }
        };
        let commit_duration_ms = (now_micros() - started_micros) / 1000;

        // Advance cursor only after commit success.
        self.coord
            .set_cursor(&group, ident, end_offset as i64)
            .await?;

        // Blue-green meta-marker emission. After cursor + FileIndex are
        // durable, query the coord for any pending markers covered by
        // this unit's LSN window. Writes `(uuid, table_name, snapshot_id)`
        // rows to the meta-marker Iceberg table — that's the per-instance
        // audit trail for blue-green replica diffing. Pass the *new*
        // cursor so the coord's eligibility check considers everything we
        // just committed as processed.
        // (Snapshot rows carry no markers; their cursor isn't this one.)
        if self.meta_marker.is_some() && !unit.snapshot {
            self.emit_pending_markers(ident, end_offset as i64).await?;
        }

        // Control-plane meta `commits` row + flush. Best-effort: a
        // meta-flush failure is logged but doesn't roll back the
        // user-table commit (already durable above).
        if let (Some(meta), true) = (&committed, self.meta_recorder.is_some()) {
            let snap_id = meta.current_snapshot_id.unwrap_or(0);
            self.record_flush(FlushStats {
                ts_micros: started_micros,
                worker_id: String::new(),
                table_name: format!("{}", ident),
                mode: "logical".into(),
                snapshot_id: snap_id,
                sequence_number: snap_id,
                lsn: unit.max_lsn,
                rows: unit.folded as i64,
                bytes: bytes_written,
                duration_ms: commit_duration_ms,
                data_files: data_files_count,
                delete_files: delete_files_count,
                max_source_ts_micros: unit.max_source_ts_micros,
                schema_id: 0,
                tx_count: unit.xids.len() as i32,
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

        let mut labels = Labels::new();
        labels.insert("table".into(), ident.name.clone());
        self.metrics
            .counter(names::MATERIALIZER_ROWS_TOTAL, &labels, unit.folded as u64);
        Ok(unit.folded)
    }

    async fn rebuild_file_index(&mut self, ident: &TableIdent) -> Result<()> {
        let entry = self.tables.get_mut(ident).expect("checked by caller");
        entry.file_index = FileIndex::new();
        entry.index_at = None;
        self.catch_up_file_index(ident).await
    }

    /// Catch the table's FileIndex up with commits this materializer
    /// didn't make, or didn't learn the outcome of: the worker that owned
    /// the table before it, a `compact` job, a commit whose response was
    /// lost. The index must be untouched since `index_at`. Returns the
    /// table's [`TableMetadata::log_ends`](pg2iceberg_iceberg::TableMetadata::log_ends).
    async fn sync_file_index(&mut self, ident: &TableIdent) -> Result<BTreeMap<String, u64>> {
        let meta = self.catalog.load_table(ident).await?;
        let current = meta.as_ref().and_then(|m| m.current_snapshot_id);
        if let Some(meta) = &meta {
            self.sync_schema(ident, &meta.schema);
        }
        let log_ends = meta.map(|m| m.log_ends).unwrap_or_default();
        let entry = self.tables.get(ident).expect("checked by caller");
        if current != entry.index_at {
            self.catch_up_file_index(ident).await?;
        }
        Ok(log_ends)
    }

    /// Take up the table's columns as the catalog has them, if another
    /// process changed them — a worker that applied a schema change this
    /// one didn't read. Written with the old columns, a column's values
    /// would land in whichever field had its name.
    fn sync_schema(&mut self, ident: &TableIdent, catalog: &TableSchema) {
        let entry = self.tables.get_mut(ident).expect("checked by caller");
        if entry.schema.columns != catalog.columns {
            entry.schema.columns = catalog.columns.clone();
            entry.writer = TableWriter::new(entry.schema.clone());
        }
    }

    async fn catch_up_file_index(&mut self, ident: &TableIdent) -> Result<()> {
        let entry = self.tables.get_mut(ident).expect("checked by caller");
        entry.index_at = catch_up_from_catalog(
            &mut entry.file_index,
            entry.index_at,
            self.catalog.as_ref(),
            self.blob_store.as_ref(),
            ident,
            &entry.schema,
            &entry.pk_cols,
        )
        .await
        .map_err(|e| MaterializerError::Catalog(IcebergError::Other(e.to_string())))?;
        Ok(())
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
                    unchanged_from: None,
                }
            })
            .collect();

        // Stage + commit to the meta-marker Iceberg table.
        let prepared = meta.writer.prepare(&rows, &meta.file_index)?;
        let mut data_files: Vec<DataFile> = Vec::with_capacity(prepared.data.len());
        for chunk in prepared.data {
            // Marker meta-table is unpartitioned (the schema has no
            // partition spec), so the segment is always empty.
            let path = self
                .namer
                .next_path(&meta.schema.ident, "meta-marker", "")
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
                sequence_number: None,
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
/// emits an equality-delete for each — and the events before it in this
/// step, rows the FileIndex doesn't hold yet, are dropped. (Earlier
/// steps of the unit are in the FileIndex already.)
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
    schema: &TableSchema,
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
        out.clear();
        // Decode each PK key back into a PK-only Row, over the PK
        // columns in the order every key was built from (`pk_cols`).
        // Keys that don't fit (shouldn't happen — FileIndex stores what
        // `PkKey::from_row` produces) are skipped rather than crashing
        // the cycle.
        let pk_schema: Vec<ColumnSchema> = schema.primary_key_columns().cloned().collect();
        for pk in file_index.all_pks() {
            let Some(row) = pk.to_row(&pk_schema) else {
                continue;
            };
            out.push(MatEvent {
                op: Op::Delete,
                lsn: evt.lsn,
                commit_ts: evt.commit_ts,
                xid: evt.xid,
                unchanged_cols: Vec::new(),
                moved_from: None,
                row,
            });
        }
        // Drop the Truncate sentinel itself — its expansion is in `out`.
    }
    out
}

/// The columns of `schema` that `source` doesn't have.
fn collect_toast_paths(
    rows: &[MaterializedRow],
    file_index: &FileIndex,
    pk_cols: &[ColumnName],
) -> BTreeSet<String> {
    let mut paths = BTreeSet::new();
    for r in rows {
        if r.unchanged_cols.is_empty() || r.op == Op::Delete {
            continue;
        }
        if let Some(p) = file_index.lookup(&toast_source(r, pk_cols, file_index)) {
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
