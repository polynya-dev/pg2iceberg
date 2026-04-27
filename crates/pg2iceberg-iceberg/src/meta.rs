//! Control-plane meta tables.
//!
//! Mirrors `pg2iceberg/iceberg/meta_schema.go` + `meta_recorder.go` from the
//! Go reference. Five Iceberg tables under the operator-configured
//! `meta_namespace` capture pipeline observability *as data*: every commit,
//! checkpoint, compaction, maintenance op, and blue-green marker becomes a
//! row that downstream tooling (Polynya UI, dashboards, custom verifiers)
//! reads with a plain Iceberg query — no scraping logs, no extra metrics
//! pipeline.
//!
//! - `<meta_ns>.commits` — one row per user-table commit (logical mode:
//!   per materialize cycle; query mode: per query-flush cycle).
//! - `<meta_ns>.checkpoints` — one row per checkpoint save.
//! - `<meta_ns>.compactions` — one row per compaction commit.
//! - `<meta_ns>.maintenance` — one row per snapshot-expiry / orphan-cleanup
//!   pass per table.
//! - `<meta_ns>.markers` — one row per (marker_uuid, user_table) pair, the
//!   blue-green replica-alignment join key. **Lives in this module's name
//!   constants but the *writer* for it is in `pg2iceberg-logical` because
//!   it sources rows from the coordinator's pending-markers state, not from
//!   the materializer's commit outcomes.**
//!
//! All four schemas owned here use the same shape rules as Go:
//! - Field IDs are immutable; new columns append with the next ID.
//! - New columns must be nullable (Iceberg rejects required-column adds
//!   on a non-empty table).
//! - Columns are never renamed or removed — that would break readers.
//! - Each table is `day(ts)`-partitioned for efficient time-range scans.
//!
//! The schemas here intentionally **do not** include a primary key. Meta
//! tables are append-only from the writer's perspective; merge-on-read
//! semantics (equality deletes) don't apply. We rely on the buffer-then-
//! commit pattern in `MetaRecorder` to produce one Iceberg snapshot per
//! flush rather than one per row.

use crate::TableSchema;
use pg2iceberg_core::value::TimestampMicros;
use pg2iceberg_core::{
    ColumnName, ColumnSchema, IcebergType, Namespace, PartitionField, PgValue, Row, TableIdent,
    Transform,
};

/// Operational name for the per-commit meta table.
pub const META_COMMITS: &str = "commits";
/// Operational name for the per-checkpoint-save meta table.
pub const META_CHECKPOINTS: &str = "checkpoints";
/// Operational name for the per-compaction-commit meta table.
pub const META_COMPACTIONS: &str = "compactions";
/// Operational name for the per-maintenance-op meta table.
pub const META_MAINTENANCE: &str = "maintenance";
/// Operational name for the blue-green markers meta table. Defined here for
/// completeness; the writer lives in `pg2iceberg-logical` because it
/// consumes coordinator state (`pending_markers`).
pub const META_MARKERS: &str = "markers";

/// Maintenance-op kind: dropped expired snapshots from catalog history.
pub const MAINTENANCE_OP_EXPIRE_SNAPSHOTS: &str = "expire_snapshots";
/// Maintenance-op kind: deleted orphan files from the blob store.
pub const MAINTENANCE_OP_CLEAN_ORPHANS: &str = "clean_orphans";

/// Default `day(ts)` partition spec shared by every meta table — timestamp
/// columns are named `ts`, partition field is `ts_day`. Lets readers prune
/// to a recent window cheaply without per-table partition tuning.
fn day_partition_spec() -> Vec<PartitionField> {
    vec![PartitionField {
        source_column: "ts".into(),
        name: "ts_day".into(),
        transform: Transform::Day,
    }]
}

fn ns_for(meta_namespace: &str) -> Namespace {
    Namespace(vec![meta_namespace.to_string()])
}

fn col(name: &str, field_id: i32, ty: IcebergType, nullable: bool) -> ColumnSchema {
    ColumnSchema {
        name: name.into(),
        field_id,
        ty,
        nullable,
        is_primary_key: false,
    }
}

/// Schema for `<meta_ns>.commits`. Field-id assignments must stay stable;
/// see module docs.
pub fn meta_commits_schema(meta_namespace: &str) -> TableSchema {
    TableSchema {
        ident: TableIdent {
            namespace: ns_for(meta_namespace),
            name: META_COMMITS.into(),
        },
        columns: vec![
            col("ts", 1, IcebergType::TimestampTz, false),
            col("worker_id", 2, IcebergType::String, true),
            col("table_name", 3, IcebergType::String, false),
            col("mode", 4, IcebergType::String, false),
            col("snapshot_id", 5, IcebergType::Long, false),
            col("sequence_number", 6, IcebergType::Long, false),
            col("lsn", 7, IcebergType::Long, true),
            col("rows", 8, IcebergType::Long, true),
            col("bytes", 9, IcebergType::Long, true),
            col("duration_ms", 10, IcebergType::Long, true),
            col("data_files", 11, IcebergType::Int, true),
            col("delete_files", 12, IcebergType::Int, true),
            col("max_source_ts", 13, IcebergType::TimestampTz, true),
            col("schema_id", 14, IcebergType::Int, true),
            col("tx_count", 15, IcebergType::Int, true),
            col("pg2iceberg_commit_sha", 16, IcebergType::String, true),
        ],
        partition_spec: day_partition_spec(),
    }
}

/// Schema for `<meta_ns>.checkpoints`.
pub fn meta_checkpoints_schema(meta_namespace: &str) -> TableSchema {
    TableSchema {
        ident: TableIdent {
            namespace: ns_for(meta_namespace),
            name: META_CHECKPOINTS.into(),
        },
        columns: vec![
            col("ts", 1, IcebergType::TimestampTz, false),
            col("worker_id", 2, IcebergType::String, true),
            col("lsn", 3, IcebergType::Long, true),
            col("last_flush_at", 4, IcebergType::TimestampTz, true),
            col("pg2iceberg_commit_sha", 5, IcebergType::String, true),
        ],
        partition_spec: day_partition_spec(),
    }
}

/// Schema for `<meta_ns>.compactions`.
pub fn meta_compactions_schema(meta_namespace: &str) -> TableSchema {
    TableSchema {
        ident: TableIdent {
            namespace: ns_for(meta_namespace),
            name: META_COMPACTIONS.into(),
        },
        columns: vec![
            col("ts", 1, IcebergType::TimestampTz, false),
            col("worker_id", 2, IcebergType::String, true),
            col("table_name", 3, IcebergType::String, false),
            col("partition", 4, IcebergType::String, true),
            col("snapshot_id", 5, IcebergType::Long, false),
            col("sequence_number", 6, IcebergType::Long, false),
            col("input_data_files", 7, IcebergType::Int, true),
            col("input_delete_files", 8, IcebergType::Int, true),
            col("output_data_files", 9, IcebergType::Int, true),
            col("rows_rewritten", 10, IcebergType::Long, true),
            col("rows_removed", 11, IcebergType::Long, true),
            col("bytes_before", 12, IcebergType::Long, true),
            col("bytes_after", 13, IcebergType::Long, true),
            col("duration_ms", 14, IcebergType::Long, true),
            col("pg2iceberg_commit_sha", 15, IcebergType::String, true),
        ],
        partition_spec: day_partition_spec(),
    }
}

/// Schema for `<meta_ns>.maintenance`.
pub fn meta_maintenance_schema(meta_namespace: &str) -> TableSchema {
    TableSchema {
        ident: TableIdent {
            namespace: ns_for(meta_namespace),
            name: META_MAINTENANCE.into(),
        },
        columns: vec![
            col("ts", 1, IcebergType::TimestampTz, false),
            col("worker_id", 2, IcebergType::String, true),
            col("table_name", 3, IcebergType::String, false),
            col("operation", 4, IcebergType::String, false),
            col("items_affected", 5, IcebergType::Int, true),
            col("bytes_freed", 6, IcebergType::Long, true),
            col("duration_ms", 7, IcebergType::Long, true),
            col("pg2iceberg_commit_sha", 8, IcebergType::String, true),
        ],
        partition_spec: day_partition_spec(),
    }
}

// ── Stat structs ─────────────────────────────────────────────────────────
//
// These are the inputs to the `Materializer::record_*` methods. Each has a
// `to_row()` that converts to the `Row` shape the underlying TableWriter
// expects. Optional fields are encoded as `PgValue::Null`; required fields
// must be present.

/// One row's worth of data for `<meta_ns>.commits`. Built by the
/// materializer after a successful `Catalog::commit_snapshot` for a user
/// table.
#[derive(Clone, Debug)]
pub struct FlushStats {
    /// Wall-clock micros at the moment the commit landed.
    pub ts_micros: i64,
    /// Empty string when not running in distributed/horizontal mode.
    pub worker_id: String,
    /// PG-qualified table name, e.g. `"public.orders"`.
    pub table_name: String,
    /// `"logical"` or `"query"`.
    pub mode: String,
    pub snapshot_id: i64,
    /// Iceberg `sequence_number`. The Rust port collapses iceberg's
    /// random-63-bit `snapshot_id` and the sequence_number into the same
    /// value at the prod-catalog boundary (see
    /// `pg2iceberg-iceberg/src/prod/catalog.rs::snapshots`). Pass the same
    /// value as `snapshot_id` here; an upstream rebuild that needs the
    /// distinction can pull it from `snapshots()` separately.
    pub sequence_number: i64,
    pub lsn: i64,
    pub rows: i64,
    pub bytes: i64,
    pub duration_ms: i64,
    pub data_files: i32,
    pub delete_files: i32,
    /// Latest source-side commit ts (PG `BEGIN` timestamp) across the
    /// events bundled into this commit. `0` means unknown.
    pub max_source_ts_micros: i64,
    pub schema_id: i32,
    pub tx_count: i32,
    /// Build-time git SHA. Empty when not stamped (dev builds).
    pub pg2iceberg_commit_sha: String,
}

impl FlushStats {
    pub fn to_row(&self) -> Row {
        let mut r = Row::new();
        r.insert(
            ColumnName("ts".into()),
            PgValue::TimestampTz(TimestampMicros(self.ts_micros)),
        );
        r.insert(
            ColumnName("worker_id".into()),
            opt_text(&self.worker_id),
        );
        r.insert(
            ColumnName("table_name".into()),
            PgValue::Text(self.table_name.clone()),
        );
        r.insert(ColumnName("mode".into()), PgValue::Text(self.mode.clone()));
        r.insert(ColumnName("snapshot_id".into()), PgValue::Int8(self.snapshot_id));
        r.insert(
            ColumnName("sequence_number".into()),
            PgValue::Int8(self.sequence_number),
        );
        r.insert(ColumnName("lsn".into()), opt_int8(self.lsn));
        r.insert(ColumnName("rows".into()), opt_int8(self.rows));
        r.insert(ColumnName("bytes".into()), opt_int8(self.bytes));
        r.insert(
            ColumnName("duration_ms".into()),
            opt_int8(self.duration_ms),
        );
        r.insert(
            ColumnName("data_files".into()),
            opt_int4(self.data_files),
        );
        r.insert(
            ColumnName("delete_files".into()),
            opt_int4(self.delete_files),
        );
        r.insert(
            ColumnName("max_source_ts".into()),
            opt_ts(self.max_source_ts_micros),
        );
        r.insert(
            ColumnName("schema_id".into()),
            opt_int4(self.schema_id),
        );
        r.insert(
            ColumnName("tx_count".into()),
            opt_int4(self.tx_count),
        );
        r.insert(
            ColumnName("pg2iceberg_commit_sha".into()),
            opt_text(&self.pg2iceberg_commit_sha),
        );
        r
    }
}

/// One row's worth of data for `<meta_ns>.checkpoints`. Built by the
/// lifecycle after a successful `Coordinator::save_checkpoint`.
#[derive(Clone, Debug)]
pub struct CheckpointStats {
    pub ts_micros: i64,
    pub worker_id: String,
    pub lsn: i64,
    pub last_flush_at_micros: i64,
    pub pg2iceberg_commit_sha: String,
}

impl CheckpointStats {
    pub fn to_row(&self) -> Row {
        let mut r = Row::new();
        r.insert(
            ColumnName("ts".into()),
            PgValue::TimestampTz(TimestampMicros(self.ts_micros)),
        );
        r.insert(
            ColumnName("worker_id".into()),
            opt_text(&self.worker_id),
        );
        r.insert(ColumnName("lsn".into()), opt_int8(self.lsn));
        r.insert(
            ColumnName("last_flush_at".into()),
            opt_ts(self.last_flush_at_micros),
        );
        r.insert(
            ColumnName("pg2iceberg_commit_sha".into()),
            opt_text(&self.pg2iceberg_commit_sha),
        );
        r
    }
}

/// One row's worth of data for `<meta_ns>.compactions`. Built by the
/// materializer after a successful `Catalog::commit_compaction`.
#[derive(Clone, Debug)]
pub struct CompactionStats {
    pub ts_micros: i64,
    pub worker_id: String,
    pub table_name: String,
    /// Empty string for unpartitioned tables; otherwise the partition path
    /// like `"day=2026-04-21"`. (Compaction is per-partition under the
    /// `Materializer::compact_table` path.)
    pub partition: String,
    pub snapshot_id: i64,
    pub sequence_number: i64,
    pub input_data_files: i32,
    pub input_delete_files: i32,
    pub output_data_files: i32,
    pub rows_rewritten: i64,
    pub rows_removed: i64,
    pub bytes_before: i64,
    pub bytes_after: i64,
    pub duration_ms: i64,
    pub pg2iceberg_commit_sha: String,
}

impl CompactionStats {
    pub fn to_row(&self) -> Row {
        let mut r = Row::new();
        r.insert(
            ColumnName("ts".into()),
            PgValue::TimestampTz(TimestampMicros(self.ts_micros)),
        );
        r.insert(
            ColumnName("worker_id".into()),
            opt_text(&self.worker_id),
        );
        r.insert(
            ColumnName("table_name".into()),
            PgValue::Text(self.table_name.clone()),
        );
        r.insert(ColumnName("partition".into()), opt_text(&self.partition));
        r.insert(ColumnName("snapshot_id".into()), PgValue::Int8(self.snapshot_id));
        r.insert(
            ColumnName("sequence_number".into()),
            PgValue::Int8(self.sequence_number),
        );
        r.insert(
            ColumnName("input_data_files".into()),
            opt_int4(self.input_data_files),
        );
        r.insert(
            ColumnName("input_delete_files".into()),
            opt_int4(self.input_delete_files),
        );
        r.insert(
            ColumnName("output_data_files".into()),
            opt_int4(self.output_data_files),
        );
        r.insert(
            ColumnName("rows_rewritten".into()),
            opt_int8(self.rows_rewritten),
        );
        r.insert(
            ColumnName("rows_removed".into()),
            opt_int8(self.rows_removed),
        );
        r.insert(
            ColumnName("bytes_before".into()),
            opt_int8(self.bytes_before),
        );
        r.insert(
            ColumnName("bytes_after".into()),
            opt_int8(self.bytes_after),
        );
        r.insert(
            ColumnName("duration_ms".into()),
            opt_int8(self.duration_ms),
        );
        r.insert(
            ColumnName("pg2iceberg_commit_sha".into()),
            opt_text(&self.pg2iceberg_commit_sha),
        );
        r
    }
}

/// One row's worth of data for `<meta_ns>.maintenance`. Built by the
/// materializer after `expire_cycle` / `cleanup_orphans_cycle` finish a
/// per-table outcome.
#[derive(Clone, Debug)]
pub struct MaintenanceStats {
    pub ts_micros: i64,
    pub worker_id: String,
    pub table_name: String,
    /// One of [`MAINTENANCE_OP_EXPIRE_SNAPSHOTS`] /
    /// [`MAINTENANCE_OP_CLEAN_ORPHANS`].
    pub operation: String,
    pub items_affected: i32,
    pub bytes_freed: i64,
    pub duration_ms: i64,
    pub pg2iceberg_commit_sha: String,
}

impl MaintenanceStats {
    pub fn to_row(&self) -> Row {
        let mut r = Row::new();
        r.insert(
            ColumnName("ts".into()),
            PgValue::TimestampTz(TimestampMicros(self.ts_micros)),
        );
        r.insert(
            ColumnName("worker_id".into()),
            opt_text(&self.worker_id),
        );
        r.insert(
            ColumnName("table_name".into()),
            PgValue::Text(self.table_name.clone()),
        );
        r.insert(
            ColumnName("operation".into()),
            PgValue::Text(self.operation.clone()),
        );
        r.insert(
            ColumnName("items_affected".into()),
            opt_int4(self.items_affected),
        );
        r.insert(
            ColumnName("bytes_freed".into()),
            opt_int8(self.bytes_freed),
        );
        r.insert(
            ColumnName("duration_ms".into()),
            opt_int8(self.duration_ms),
        );
        r.insert(
            ColumnName("pg2iceberg_commit_sha".into()),
            opt_text(&self.pg2iceberg_commit_sha),
        );
        r
    }
}

// ── nullability shims ────────────────────────────────────────────────────
//
// Match Go's `nullableInt64`/`nullableTime`/etc semantics: an empty/zero
// value gets written as SQL NULL so dashboard consumers can `WHERE x IS
// NOT NULL` to filter cheaply. Callers who genuinely want to write a zero
// can construct the `PgValue::Int8(0)` themselves and skip this helper —
// but the convention here is "0 means unknown" for these fields.

fn opt_text(s: &str) -> PgValue {
    if s.is_empty() {
        PgValue::Null
    } else {
        PgValue::Text(s.to_string())
    }
}

fn opt_int8(v: i64) -> PgValue {
    if v == 0 {
        PgValue::Null
    } else {
        PgValue::Int8(v)
    }
}

fn opt_int4(v: i32) -> PgValue {
    if v == 0 {
        PgValue::Null
    } else {
        PgValue::Int4(v)
    }
}

fn opt_ts(v: i64) -> PgValue {
    if v == 0 {
        PgValue::Null
    } else {
        PgValue::TimestampTz(TimestampMicros(v))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn commits_schema_field_ids_are_unique_and_match_go() {
        let s = meta_commits_schema("_pg2iceberg_blue");
        assert_eq!(s.ident.namespace.0, vec!["_pg2iceberg_blue".to_string()]);
        assert_eq!(s.ident.name, "commits");
        // Field IDs 1..=16 from the Go reference must be preserved
        // exactly. Renumbering would break readers built against the
        // Go schema.
        let ids: Vec<i32> = s.columns.iter().map(|c| c.field_id).collect();
        assert_eq!(ids, (1..=16).collect::<Vec<_>>());
        // Required columns Go declared as non-null.
        for name in ["ts", "table_name", "mode", "snapshot_id", "sequence_number"] {
            let c = s.columns.iter().find(|c| c.name == name).unwrap();
            assert!(!c.nullable, "{name} must be required");
        }
        // Day-partitioned on `ts`.
        assert_eq!(s.partition_spec.len(), 1);
        assert_eq!(s.partition_spec[0].source_column, "ts");
        assert_eq!(s.partition_spec[0].name, "ts_day");
        assert_eq!(s.partition_spec[0].transform, Transform::Day);
    }

    #[test]
    fn checkpoints_schema_minimal() {
        let s = meta_checkpoints_schema("meta");
        assert_eq!(s.columns.len(), 5);
        let ids: Vec<i32> = s.columns.iter().map(|c| c.field_id).collect();
        assert_eq!(ids, vec![1, 2, 3, 4, 5]);
    }

    #[test]
    fn compactions_schema_includes_partition_column() {
        let s = meta_compactions_schema("meta");
        assert!(s.columns.iter().any(|c| c.name == "partition"));
        assert_eq!(s.columns.len(), 15);
    }

    #[test]
    fn maintenance_schema_required_op() {
        let s = meta_maintenance_schema("meta");
        let op = s.columns.iter().find(|c| c.name == "operation").unwrap();
        assert!(!op.nullable);
        assert_eq!(s.columns.len(), 8);
    }

    #[test]
    fn flush_stats_round_trips_optional_fields_as_null() {
        let s = FlushStats {
            ts_micros: 1_700_000_000_000_000,
            worker_id: String::new(),
            table_name: "public.orders".into(),
            mode: "logical".into(),
            snapshot_id: 42,
            sequence_number: 42,
            lsn: 0,
            rows: 0,
            bytes: 0,
            duration_ms: 0,
            data_files: 0,
            delete_files: 0,
            max_source_ts_micros: 0,
            schema_id: 0,
            tx_count: 0,
            pg2iceberg_commit_sha: String::new(),
        };
        let r = s.to_row();
        assert_eq!(
            r.get(&ColumnName("worker_id".into())),
            Some(&PgValue::Null),
            "empty worker_id encodes as Null"
        );
        assert_eq!(
            r.get(&ColumnName("lsn".into())),
            Some(&PgValue::Null),
            "zero lsn encodes as Null"
        );
        assert_eq!(
            r.get(&ColumnName("table_name".into())),
            Some(&PgValue::Text("public.orders".into())),
            "required field passes through verbatim"
        );
        assert_eq!(
            r.get(&ColumnName("snapshot_id".into())),
            Some(&PgValue::Int8(42)),
        );
    }

    #[test]
    fn maintenance_stats_to_row() {
        let s = MaintenanceStats {
            ts_micros: 1,
            worker_id: "w1".into(),
            table_name: "public.orders".into(),
            operation: MAINTENANCE_OP_EXPIRE_SNAPSHOTS.into(),
            items_affected: 3,
            bytes_freed: 0,
            duration_ms: 50,
            pg2iceberg_commit_sha: "abc1234".into(),
        };
        let r = s.to_row();
        assert_eq!(
            r.get(&ColumnName("operation".into())),
            Some(&PgValue::Text("expire_snapshots".into())),
        );
        assert_eq!(
            r.get(&ColumnName("items_affected".into())),
            Some(&PgValue::Int4(3)),
        );
        assert_eq!(
            r.get(&ColumnName("bytes_freed".into())),
            Some(&PgValue::Null),
            "bytes_freed=0 (irrelevant for snapshot expiry) encodes as Null"
        );
    }
}
