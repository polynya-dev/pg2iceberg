//! Catalog and TableWriter trait surface.
//!
//! Wraps `iceberg-rust` in production. The sim impl in `pg2iceberg-sim` keeps
//! metadata in memory.

pub mod compact;
pub mod file_index;
pub mod fold;
pub mod materialize;
pub mod meta;
pub mod orphan;
pub mod pk;
#[cfg(feature = "prod")]
pub mod prod;
pub mod reader;
pub mod verify;
pub mod writer;

pub use compact::{
    compact_table, retire_deletes, CompactError, CompactedFile, CompactionConfig, CompactionOutcome,
};
pub use file_index::{catch_up_from_catalog, rebuild_from_catalog, FileIndex};
pub use fold::{fold_events, pk_key, MaterializedRow};
pub use materialize::{promote_re_inserts, resolve_unchanged_cols, toast_source};
pub use orphan::{cleanup_orphans, CleanupError, CleanupOutcome};
pub use pk::PkKey;
pub use reader::{read_data_file, read_data_file_by_field_id};
pub use verify::read_materialized_state;
pub use writer::{DataChunk, PreparedChunk, PreparedFiles, TableWriter, WriterError};

use async_trait::async_trait;
use pg2iceberg_core::{Namespace, PartitionLiteral, TableIdent, TableSchema};
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Clone, Debug, Error)]
pub enum IcebergError {
    #[error("not found: {0}")]
    NotFound(String),
    #[error("conflict: {0}")]
    Conflict(String),
    #[error("other: {0}")]
    Other(String),
}

pub type Result<T> = std::result::Result<T, IcebergError>;

/// Opaque token returned by the catalog representing the current table state.
/// Carry it through the prepare/commit cycle so we can detect concurrent
/// modifications.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TableMetadata {
    pub ident: TableIdent,
    pub schema: TableSchema,
    pub current_snapshot_id: Option<i64>,
    /// Catalog-vended config (e.g. `s3.access-key-id` etc.). Used by the
    /// vended-credentials S3 router.
    pub config: std::collections::BTreeMap<String, String>,
    /// Table's storage location, surfaced from
    /// `iceberg::TableMetadata::location()`. Populated for catalogs
    /// that return per-table locations (every Iceberg REST catalog
    /// today does); empty for catalogs that don't. The vended-creds
    /// router needs this to derive the per-table S3 bucket + base
    /// path independently of any catalog-vended `location` config
    /// key (Lakekeeper, for instance, doesn't include `location` in
    /// the response `config` map even though it sets it in
    /// `metadata`).
    pub location: String,
    /// Per cursor group, how far commits have applied the change log, as
    /// the table's properties and its live snapshots record it (see
    /// [`LogRange`]).
    #[serde(default)]
    pub log_ends: std::collections::BTreeMap<String, u64>,
    /// The table's properties.
    #[serde(default)]
    pub properties: std::collections::BTreeMap<String, String>,
}

/// The change-log range a materializer commit applied for one cursor
/// group: offsets `[start, end)`. Recorded on the commit's last snapshot,
/// so a commit whose cursor update never happened — the process died in
/// between, or the commit's response was lost — is known to have landed,
/// and isn't applied again.
///
/// Every snapshot pg2iceberg commits — compactions too — also carries
/// each group's end so far forward. But another engine's commits carry
/// nothing of pg2iceberg's — a managed catalog's compaction — and once
/// expiry leaves only those, the snapshots know nothing: the table's
/// `pg2iceberg.log-ends` property, set by the same commit as the range,
/// holds every group's end too.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogRange {
    pub group: String,
    pub start: u64,
    pub end: u64,
}

impl LogRange {
    const GROUP: &'static str = "pg2iceberg.log-group";
    const START: &'static str = "pg2iceberg.log-start";
    const END: &'static str = "pg2iceberg.log-end";
    const ENDS: &'static str = "pg2iceberg.log-ends";

    /// As snapshot summary properties.
    pub fn to_properties(&self) -> Vec<(String, String)> {
        vec![
            (Self::GROUP.into(), self.group.clone()),
            (Self::START.into(), self.start.to_string()),
            (Self::END.into(), self.end.to_string()),
        ]
    }

    /// From a snapshot's summary properties, if they record one.
    pub fn from_properties<'a>(get: impl Fn(&str) -> Option<&'a str>) -> Option<Self> {
        Some(Self {
            group: get(Self::GROUP)?.to_string(),
            start: get(Self::START)?.parse().ok()?,
            end: get(Self::END)?.parse().ok()?,
        })
    }
}

/// Merge `from` into `into`, keeping each group's furthest end.
pub fn merge_log_ends(
    into: &mut std::collections::BTreeMap<String, u64>,
    from: impl IntoIterator<Item = (String, u64)>,
) {
    for (group, end) in from {
        let at = into.entry(group).or_insert(end);
        *at = (*at).max(end);
    }
}

/// How far commits had applied the log, per group, as a snapshot's
/// summary properties record it — the ends carried forward, and its own
/// commit's range — or the table's properties.
pub fn recorded_log_ends<'a>(
    get: impl Fn(&str) -> Option<&'a str>,
) -> std::collections::BTreeMap<String, u64> {
    let mut ends: std::collections::BTreeMap<String, u64> = get(LogRange::ENDS)
        .and_then(|v| serde_json::from_str(v).ok())
        .unwrap_or_default();
    if let Some(range) = LogRange::from_properties(&get) {
        merge_log_ends(&mut ends, [(range.group, range.end)]);
    }
    ends
}

/// The property recording `ends` — a snapshot's, carrying them forward,
/// or the table's — unless there are none.
pub fn log_ends_property(
    ends: &std::collections::BTreeMap<String, u64>,
) -> Option<(String, String)> {
    (!ends.is_empty()).then(|| {
        (
            LogRange::ENDS.to_string(),
            serde_json::to_string(ends).expect("a map of numbers serializes"),
        )
    })
}

/// Built by the materializer (combining `TableWriter::prepare` output with
/// blob-store paths) and consumed by [`Catalog::commit_snapshot`].
#[derive(Clone, Debug)]
pub struct PreparedCommit {
    pub ident: TableIdent,
    pub data_files: Vec<DataFile>,
    pub equality_deletes: Vec<DataFile>,
}

/// Built by [`crate::compact::compact_table`] and consumed by
/// [`Catalog::commit_compaction`]. Produces an `Operation::Replace`
/// snapshot: drop everything in `removed_paths`, add everything in
/// `added_data_files` atomically.
#[derive(Clone, Debug)]
pub struct PreparedCompaction {
    pub ident: TableIdent,
    /// Newly written compacted data files (and any equality-delete files
    /// the rewrite leaves in place — usually empty since compaction
    /// applies pending deletes inline).
    pub added_data_files: Vec<DataFile>,
    /// File paths that this compaction supersedes. Iceberg will drop them
    /// from the new snapshot's manifest list; readers see the compacted
    /// output instead. The commit fails if one is already gone — another
    /// pass rewrote it, and this one's output would duplicate its rows.
    pub removed_paths: Vec<String>,
    /// The snapshot the pass read its inputs at. Its outputs keep that data
    /// sequence number rather than taking the new snapshot's: deletes
    /// committed while the pass ran still apply to them. (With the new
    /// snapshot's, a row deleted meanwhile would come back.)
    pub data_sequence_number: Option<i64>,
}

#[derive(Clone, Debug)]
pub struct DataFile {
    pub path: String,
    pub record_count: u64,
    pub byte_size: u64,
    /// For equality-delete files, the field IDs that participate in the
    /// equality predicate (typically the PK columns). Empty for data files.
    pub equality_field_ids: Vec<i32>,
    /// One literal per partition spec field, in `TableSchema.partition_spec`
    /// order. Empty for unpartitioned tables. The catalog translates this to
    /// an `iceberg::spec::Struct` at commit time.
    pub partition_values: Vec<PartitionLiteral>,
    /// The file's data sequence number when it isn't its snapshot's: a
    /// compaction's output keeps the one its pass was planned at, so
    /// deletes committed while the pass ran still apply to it. `None`: the
    /// snapshot's (see [`Self::sequence_number_in`]).
    pub sequence_number: Option<i64>,
}

impl DataFile {
    /// The file's data sequence number, as listed in `snapshot`. An
    /// equality delete applies to data files with a lower one.
    pub fn sequence_number_in(&self, snapshot: &Snapshot) -> i64 {
        self.sequence_number.unwrap_or(snapshot.id)
    }
}

/// One Iceberg snapshot. Snapshots are append-only and ordered by `id`.
/// Iceberg MoR semantics: a delete file at snapshot `N` applies only to data
/// files with a data sequence number `< N` — their snapshot's, unless they
/// carry their own ([`DataFile::sequence_number`]). Data and deletes from
/// the same snapshot are kept consistent by the materializer.
///
/// Compaction snapshots populate `removed_paths` with the paths of data /
/// delete files that were live in prior snapshots but are no longer
/// referenced by this snapshot's manifest list. Verifier and FileIndex
/// rebuild paths skip any data/delete file whose path appears in a
/// compaction's `removed_paths` set.
#[derive(Clone, Debug)]
pub struct Snapshot {
    pub id: i64,
    pub data_files: Vec<DataFile>,
    pub delete_files: Vec<DataFile>,
    /// File paths superseded by this snapshot. Empty for non-compaction
    /// snapshots.
    pub removed_paths: Vec<String>,
    /// Wall-clock timestamp in milliseconds since epoch. Used by
    /// snapshot expiry — `expire_snapshots(retention)` drops snapshots
    /// older than `now - retention`. Surfaced from
    /// `iceberg::spec::Snapshot::timestamp_ms()` in the prod path; the
    /// sim catalog populates it from its `Clock` source.
    pub timestamp_ms: i64,
    /// A stand-in for an expired snapshot: only the files it added that
    /// are still live, not what it removed (see [`Catalog::snapshots`]).
    pub expired: bool,
    /// The log range the commit this snapshot ends applied, if it
    /// recorded one. `None` for a stand-in.
    pub log_range: Option<LogRange>,
}

#[derive(Clone, Debug)]
pub enum SchemaChange {
    AddColumn {
        name: String,
        ty: pg2iceberg_core::IcebergType,
        nullable: bool,
    },
    /// Soft-drop: column is retained as nullable in Iceberg.
    DropColumn { name: String },
    /// Widen a column's type. Limited to Iceberg-spec-legal promotions:
    /// `int → long`, `float → double`, and `decimal(P,S) → decimal(P',S)`
    /// with `P' >= P`. Anything else (narrowing, cross-family conversion)
    /// is rejected at apply time so we don't silently corrupt downstream
    /// readers.
    PromoteColumnType {
        name: String,
        new_ty: pg2iceberg_core::IcebergType,
    },
    /// Rename a column, keeping its field id — and so its data. Moves a
    /// dropped column out of the way of a re-added one with the same
    /// name (see [`dropped_column_name`]).
    RenameColumn { from: String, to: String },
}

/// The name a column takes once the source drops it. Its values stay
/// readable under it, and its name is free for a column the source adds
/// later — a new one (Postgres gives it no values), with a new field id.
pub fn dropped_column_name(name: &str, field_id: i32) -> String {
    format!("{name}__dropped_{field_id}")
}

/// Whether `col` is a column the source dropped ([`dropped_column_name`]).
pub fn is_dropped_column(col: &pg2iceberg_core::ColumnSchema) -> bool {
    col.name.ends_with(&format!("__dropped_{}", col.field_id))
}

/// True if `to` is a spec-legal Iceberg promotion of `from`. Reference:
/// Iceberg spec §"Schema Evolution / Type Promotion". Returns `true`
/// for the no-op `from == to` case so callers can use this as a
/// "compatible?" predicate.
pub fn is_legal_type_promotion(
    from: pg2iceberg_core::IcebergType,
    to: pg2iceberg_core::IcebergType,
) -> bool {
    use pg2iceberg_core::IcebergType as T;
    if from == to {
        return true;
    }
    match (from, to) {
        (T::Int, T::Long) => true,
        (T::Float, T::Double) => true,
        (
            T::Decimal {
                precision: p1,
                scale: s1,
            },
            T::Decimal {
                precision: p2,
                scale: s2,
            },
        ) => s1 == s2 && p2 >= p1,
        _ => false,
    }
}

/// Apply [`SchemaChange`] variants in-place to a [`TableSchema`]. Shared
/// between the sim and prod catalogs so they handle field-id allocation
/// and soft-drop semantics identically.
///
/// - `AddColumn` appends a new non-PK column. Field id = current highest +
///   1 (Iceberg's metadata builder forbids id reuse, so monotonic allocation
///   is the only safe choice).
/// - `DropColumn` is a soft drop — the column stays in the schema but
///   becomes nullable. Preserves read compatibility for older data files
///   that still carry the column.
/// - `PromoteColumnType` widens the column's type in place. Field id and
///   nullability are preserved (Iceberg requires the field id to stay
///   stable across promotions so older data files keep resolving). A
///   non-promotion (e.g. `long → int`) is rejected with `IcebergError::Other`.
///
/// Errors on duplicate adds, drops of unknown columns, promotions of
/// unknown columns, and illegal promotions so the caller doesn't silently
/// no-op or corrupt the schema.
pub fn apply_schema_changes(
    schema: &mut pg2iceberg_core::TableSchema,
    changes: &[SchemaChange],
) -> Result<()> {
    for change in changes {
        match change {
            SchemaChange::AddColumn { name, ty, nullable } => {
                if schema.columns.iter().any(|c| c.name == *name) {
                    return Err(IcebergError::Conflict(format!(
                        "AddColumn: column {name} already exists"
                    )));
                }
                let next_id = schema.columns.iter().map(|c| c.field_id).max().unwrap_or(0) + 1;
                schema.columns.push(pg2iceberg_core::ColumnSchema {
                    name: name.clone(),
                    field_id: next_id,
                    ty: *ty,
                    nullable: *nullable,
                    is_primary_key: false,
                });
            }
            SchemaChange::DropColumn { name } => {
                let col = schema
                    .columns
                    .iter_mut()
                    .find(|c| c.name == *name)
                    .ok_or_else(|| {
                        IcebergError::NotFound(format!("DropColumn: column {name} not in schema"))
                    })?;
                col.nullable = true;
            }
            SchemaChange::PromoteColumnType { name, new_ty } => {
                let col = schema
                    .columns
                    .iter_mut()
                    .find(|c| c.name == *name)
                    .ok_or_else(|| {
                        IcebergError::NotFound(format!(
                            "PromoteColumnType: column {name} not in schema"
                        ))
                    })?;
                if !is_legal_type_promotion(col.ty, *new_ty) {
                    return Err(IcebergError::Other(format!(
                        "PromoteColumnType: {name} {:?} → {:?} is not a legal Iceberg \
                         promotion (allowed: int→long, float→double, decimal precision \
                         increase). Refusing to silently truncate or coerce data.",
                        col.ty, new_ty
                    )));
                }
                col.ty = *new_ty;
            }
            SchemaChange::RenameColumn { from, to } => {
                if schema.columns.iter().any(|c| c.name == *to) {
                    return Err(IcebergError::Conflict(format!(
                        "RenameColumn: column {to} already exists"
                    )));
                }
                let col = schema
                    .columns
                    .iter_mut()
                    .find(|c| c.name == *from)
                    .ok_or_else(|| {
                        IcebergError::NotFound(format!("RenameColumn: column {from} not in schema"))
                    })?;
                col.name = to.clone();
            }
        }
    }
    Ok(())
}

/// The changes that bring `table` (the Iceberg schema) in line with the
/// source table's current columns, `source` (name and type, in the
/// source's column order).
///
/// Columns match by name — Iceberg field ids stay with their columns.
/// One the source no longer has is renamed out of the way
/// ([`dropped_column_name`]), its values intact, so a column the source
/// adds with its name later is a new one (new field id). The schema then
/// says which columns are dropped, for every process that reads it.
///
/// A drop shows with the table's next change. A drop and re-add with no
/// change between shows only by order: Postgres appends a re-added column
/// after every surviving one and never reorders columns otherwise, while
/// `table` keeps the order columns were added in. So from the first
/// column that comes before one it used to follow, the rest were
/// re-added. A column re-added while it was already last, with no change
/// to the table in between, can't be told from one that was never
/// dropped.
///
/// Type changes must be legal promotions, and never of a key column.
pub fn reconcile_columns(
    table: &TableSchema,
    source: &[(String, pg2iceberg_core::IcebergType)],
) -> Result<Vec<SchemaChange>> {
    use std::collections::{BTreeMap, BTreeSet};
    let position: BTreeMap<&str, (usize, &pg2iceberg_core::ColumnSchema)> = table
        .columns
        .iter()
        .enumerate()
        .filter(|(_, c)| !is_dropped_column(c))
        .map(|(i, c)| (c.name.as_str(), (i, c)))
        .collect();

    let mut readded: BTreeSet<&str> = BTreeSet::new();
    let mut last_kept: Option<usize> = None;
    let mut out_of_order = false;
    for (name, _) in source {
        let Some(&(at, col)) = position.get(name.as_str()) else {
            continue;
        };
        if col.is_primary_key {
            continue;
        }
        out_of_order |= last_kept.is_some_and(|last| at < last);
        if out_of_order {
            readded.insert(name);
        } else {
            last_kept = Some(at);
        }
    }

    let mut changes = Vec::new();
    for (name, ty) in source {
        match position.get(name.as_str()) {
            Some(&(_, col)) if readded.contains(name.as_str()) => {
                if !col.nullable {
                    changes.push(SchemaChange::DropColumn { name: name.clone() });
                }
                changes.push(SchemaChange::RenameColumn {
                    from: name.clone(),
                    to: dropped_column_name(name, col.field_id),
                });
                changes.push(SchemaChange::AddColumn {
                    name: name.clone(),
                    ty: *ty,
                    nullable: true,
                });
            }
            Some(&(_, col)) => {
                if col.ty == *ty {
                    continue;
                }
                // Key columns are part of the equality-delete
                // predicate; promoting one would invalidate every prior
                // delete file's keys.
                if col.is_primary_key {
                    return Err(IcebergError::Other(format!(
                        "cannot promote primary-key column {name}: {:?} → {ty:?} \
                         (PK type is part of the equality-delete contract; \
                         changing it requires a full re-snapshot)",
                        col.ty
                    )));
                }
                if !is_legal_type_promotion(col.ty, *ty) {
                    return Err(IcebergError::Other(format!(
                        "column {name} type change {:?} → {ty:?} is not a legal Iceberg \
                         promotion. Allowed: int→long, float→double, decimal \
                         precision increase. Other changes (narrowing, cross-family) \
                         require a full re-snapshot.",
                        col.ty
                    )));
                }
                changes.push(SchemaChange::PromoteColumnType {
                    name: name.clone(),
                    new_ty: *ty,
                });
            }
            // Iceberg requires an added column to be optional, so files
            // written before it read it as NULL.
            None => changes.push(SchemaChange::AddColumn {
                name: name.clone(),
                ty: *ty,
                nullable: true,
            }),
        }
    }
    for col in &table.columns {
        // Key columns are never dropped.
        if col.is_primary_key
            || is_dropped_column(col)
            || source.iter().any(|(n, _)| *n == col.name)
        {
            continue;
        }
        if !col.nullable {
            changes.push(SchemaChange::DropColumn {
                name: col.name.clone(),
            });
        }
        changes.push(SchemaChange::RenameColumn {
            from: col.name.clone(),
            to: dropped_column_name(&col.name, col.field_id),
        });
    }
    Ok(changes)
}

#[async_trait]
pub trait Catalog: Send + Sync {
    async fn ensure_namespace(&self, ns: &Namespace) -> Result<()>;
    /// `Ok(None)` for "table doesn't exist yet" — the materializer treats
    /// this distinctly from a transient catalog error.
    async fn load_table(&self, ident: &TableIdent) -> Result<Option<TableMetadata>>;
    async fn create_table(&self, schema: &TableSchema) -> Result<TableMetadata>;
    async fn commit_snapshot(&self, prepared: PreparedCommit) -> Result<TableMetadata>;
    /// Commit several steps of one table as a single atomic update. Each
    /// non-empty step becomes its own snapshot with the next sequence
    /// number — so a step's equality deletes hide rows written by earlier
    /// steps — but readers of the table see none of them or all of them.
    /// Lets one source transaction be written in bounded-memory pieces
    /// and still become visible at once. Steps must share one table.
    /// `log_range` is recorded on the last snapshot (see [`LogRange`]),
    /// and the table properties `remove_properties` are removed in the
    /// same update.
    ///
    /// The default handles a single step, and records no `log_range`; a
    /// catalog that can't make several atomic must error rather than
    /// commit them one by one.
    async fn commit_snapshots(
        &self,
        steps: Vec<PreparedCommit>,
        log_range: Option<LogRange>,
        remove_properties: std::collections::BTreeSet<String>,
    ) -> Result<TableMetadata> {
        let _ = log_range;
        if !remove_properties.is_empty() {
            return Err(IcebergError::Other(
                "commit_snapshots: this catalog can't remove properties".into(),
            ));
        }
        let mut steps: Vec<PreparedCommit> = steps
            .into_iter()
            .filter(|s| !s.data_files.is_empty() || !s.equality_deletes.is_empty())
            .collect();
        match steps.len() {
            0 => Err(IcebergError::Other(
                "commit_snapshots: no non-empty steps".into(),
            )),
            1 => self.commit_snapshot(steps.remove(0)).await,
            n => Err(IcebergError::Other(format!(
                "commit_snapshots: this catalog can't commit {n} steps atomically"
            ))),
        }
    }
    /// Commit a compaction snapshot (Operation::Replace): drop the listed
    /// files, add the new ones, atomically. Default impl errors so impls
    /// that don't yet support compaction surface a clear error rather
    /// than silently no-op.
    async fn commit_compaction(&self, prepared: PreparedCompaction) -> Result<TableMetadata> {
        let _ = prepared;
        Err(IcebergError::Other(
            "commit_compaction not implemented for this Catalog impl".into(),
        ))
    }
    /// Apply `changes` to the table's schema ([`apply_schema_changes`])
    /// and set the table properties `set_properties`, in one update.
    async fn evolve_schema(
        &self,
        ident: &TableIdent,
        changes: Vec<SchemaChange>,
        set_properties: std::collections::BTreeMap<String, String>,
    ) -> Result<TableMetadata>;
    /// Drop snapshots older than `retention` (in milliseconds since
    /// the most recent snapshot — *not* wall clock — so tests with
    /// `TestClock` work deterministically). Never drops the current
    /// snapshot. Returns the number of expired snapshots.
    ///
    /// Default impl errors so impls that don't yet support expiry
    /// surface a clear error rather than silently no-op.
    async fn expire_snapshots(&self, ident: &TableIdent, retention_ms: i64) -> Result<usize> {
        let _ = (ident, retention_ms);
        Err(IcebergError::Other(
            "expire_snapshots not implemented for this Catalog impl".into(),
        ))
    }
    /// Append-only snapshot history for `ident`, ordered by snapshot id
    /// ascending. Replaying it — each snapshot's deletes and data files,
    /// minus every `removed_paths` — yields the table's current state;
    /// the verifier, FileIndex rebuild, compaction and orphan cleanup all
    /// rely on that.
    ///
    /// That must hold after snapshot expiry too: expiring a snapshot drops
    /// its metadata, not the files it added, which stay live until a later
    /// snapshot removes them. Those files are reported under a stand-in
    /// snapshot per sequence number (`id` = the files' sequence number,
    /// `timestamp_ms` = 0 when unknown, `expired` set).
    async fn snapshots(&self, ident: &TableIdent) -> Result<Vec<Snapshot>>;
}

#[cfg(test)]
mod log_range_tests {
    use super::*;

    fn range(group: &str, start: u64, end: u64) -> LogRange {
        LogRange {
            group: group.into(),
            start,
            end,
        }
    }

    #[test]
    fn round_trips_through_snapshot_properties() {
        let r = range("default", 3, 9);
        let props: std::collections::HashMap<String, String> =
            r.to_properties().into_iter().collect();
        assert_eq!(
            LogRange::from_properties(|k| props.get(k).map(String::as_str)),
            Some(r)
        );
    }

    #[test]
    fn needs_every_property() {
        let props: std::collections::HashMap<String, String> = range("default", 3, 9)
            .to_properties()
            .into_iter()
            .filter(|(k, _)| k != LogRange::END)
            .collect();
        assert_eq!(
            LogRange::from_properties(|k| props.get(k).map(String::as_str)),
            None
        );
    }

    #[test]
    fn merging_keeps_each_groups_furthest_end() {
        let mut ends = std::collections::BTreeMap::new();
        merge_log_ends(
            &mut ends,
            [("default".to_string(), 4), ("default#snapshot".into(), 2)],
        );
        merge_log_ends(
            &mut ends,
            [("default".to_string(), 7), ("default#snapshot".into(), 1)],
        );
        assert_eq!(ends.get("default"), Some(&7));
        assert_eq!(ends.get("default#snapshot"), Some(&2));
    }

    #[test]
    fn recorded_ends_combine_the_carried_and_the_own() {
        let carried = std::collections::BTreeMap::from([
            ("default".to_string(), 4),
            ("default#snapshot".to_string(), 2),
        ]);
        let mut props: std::collections::HashMap<String, String> =
            range("default", 4, 9).to_properties().into_iter().collect();
        props.extend(log_ends_property(&carried));
        let ends = recorded_log_ends(|k| props.get(k).map(String::as_str));
        assert_eq!(ends.get("default"), Some(&9));
        assert_eq!(ends.get("default#snapshot"), Some(&2));
        assert_eq!(log_ends_property(&Default::default()), None);
    }
}

#[cfg(test)]
mod schema_change_tests {
    use super::*;
    use pg2iceberg_core::{ColumnSchema, IcebergType, TableSchema};

    fn schema_with_int_qty() -> TableSchema {
        TableSchema {
            ident: TableIdent {
                namespace: Namespace(vec!["public".into()]),
                name: "t".into(),
            },
            columns: vec![
                ColumnSchema {
                    name: "id".into(),
                    field_id: 1,
                    ty: IcebergType::Int,
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

    #[test]
    fn legal_promotions_match_iceberg_spec() {
        assert!(is_legal_type_promotion(IcebergType::Int, IcebergType::Long));
        assert!(is_legal_type_promotion(
            IcebergType::Float,
            IcebergType::Double
        ));
        assert!(is_legal_type_promotion(
            IcebergType::Decimal {
                precision: 10,
                scale: 2
            },
            IcebergType::Decimal {
                precision: 18,
                scale: 2
            }
        ));
        // Same type is always "compatible" — caller decides whether to
        // emit a SchemaChange.
        assert!(is_legal_type_promotion(IcebergType::Int, IcebergType::Int));
    }

    #[test]
    fn illegal_promotions_rejected() {
        // Narrowing
        assert!(!is_legal_type_promotion(
            IcebergType::Long,
            IcebergType::Int
        ));
        assert!(!is_legal_type_promotion(
            IcebergType::Double,
            IcebergType::Float
        ));
        // Cross-family
        assert!(!is_legal_type_promotion(
            IcebergType::String,
            IcebergType::Int
        ));
        assert!(!is_legal_type_promotion(
            IcebergType::Date,
            IcebergType::Timestamp
        ));
        // Decimal with different scale (Iceberg requires same scale)
        assert!(!is_legal_type_promotion(
            IcebergType::Decimal {
                precision: 10,
                scale: 2
            },
            IcebergType::Decimal {
                precision: 18,
                scale: 4
            }
        ));
        // Decimal precision decrease
        assert!(!is_legal_type_promotion(
            IcebergType::Decimal {
                precision: 18,
                scale: 2
            },
            IcebergType::Decimal {
                precision: 10,
                scale: 2
            }
        ));
    }

    #[test]
    fn apply_promote_column_type_widens_in_place_preserving_field_id() {
        let mut s = schema_with_int_qty();
        apply_schema_changes(
            &mut s,
            &[SchemaChange::PromoteColumnType {
                name: "qty".into(),
                new_ty: IcebergType::Long,
            }],
        )
        .unwrap();
        let qty = s.columns.iter().find(|c| c.name == "qty").unwrap();
        assert_eq!(qty.ty, IcebergType::Long);
        assert_eq!(qty.field_id, 2, "field id preserved");
        assert!(!qty.nullable, "nullability preserved");
    }

    #[test]
    fn apply_promote_column_type_rejects_illegal_change() {
        let mut s = schema_with_int_qty();
        let err = apply_schema_changes(
            &mut s,
            &[SchemaChange::PromoteColumnType {
                name: "qty".into(),
                new_ty: IcebergType::String,
            }],
        )
        .unwrap_err();
        assert!(matches!(err, IcebergError::Other(_)));
        // State unchanged on rejection.
        let qty = s.columns.iter().find(|c| c.name == "qty").unwrap();
        assert_eq!(qty.ty, IcebergType::Int);
    }

    #[test]
    fn apply_promote_column_type_unknown_column_errors() {
        let mut s = schema_with_int_qty();
        let err = apply_schema_changes(
            &mut s,
            &[SchemaChange::PromoteColumnType {
                name: "ghost".into(),
                new_ty: IcebergType::Long,
            }],
        )
        .unwrap_err();
        assert!(matches!(err, IcebergError::NotFound(_)));
    }

    #[test]
    fn apply_mixed_changes_in_order() {
        // ADD a new column, DROP an existing column, PROMOTE another.
        // All should apply in the order given. The PROMOTE happens
        // before the DROP soft-flips the column to nullable, so the
        // promotion's "preserve nullability" semantics matter.
        let mut s = TableSchema {
            ident: TableIdent {
                namespace: Namespace(vec!["public".into()]),
                name: "t".into(),
            },
            columns: vec![
                ColumnSchema {
                    name: "id".into(),
                    field_id: 1,
                    ty: IcebergType::Int,
                    nullable: false,
                    is_primary_key: true,
                },
                ColumnSchema {
                    name: "old_col".into(),
                    field_id: 2,
                    ty: IcebergType::String,
                    nullable: false,
                    is_primary_key: false,
                },
                ColumnSchema {
                    name: "amount".into(),
                    field_id: 3,
                    ty: IcebergType::Int,
                    nullable: false,
                    is_primary_key: false,
                },
            ],
            partition_spec: Vec::new(),
            pg_schema: None,
        };
        apply_schema_changes(
            &mut s,
            &[
                SchemaChange::AddColumn {
                    name: "new_col".into(),
                    ty: IcebergType::String,
                    nullable: true,
                },
                SchemaChange::DropColumn {
                    name: "old_col".into(),
                },
                SchemaChange::PromoteColumnType {
                    name: "amount".into(),
                    new_ty: IcebergType::Long,
                },
            ],
        )
        .unwrap();

        let new_col = s.columns.iter().find(|c| c.name == "new_col").unwrap();
        assert_eq!(new_col.field_id, 4, "next id after current max (3) + 1");
        assert!(new_col.nullable);

        let old_col = s.columns.iter().find(|c| c.name == "old_col").unwrap();
        assert!(old_col.nullable, "soft-dropped");
        assert_eq!(old_col.field_id, 2, "id preserved");

        let amount = s.columns.iter().find(|c| c.name == "amount").unwrap();
        assert_eq!(amount.ty, IcebergType::Long);
        assert_eq!(amount.field_id, 3);
    }
}

#[cfg(test)]
mod reconcile_tests {
    use super::*;
    use pg2iceberg_core::{ColumnSchema, IcebergType as T};

    /// `id` (key), `note`, `qty` — field ids 1, 2, 3.
    fn table() -> TableSchema {
        let col = |name: &str, field_id: i32, ty: T, key: bool| ColumnSchema {
            name: name.into(),
            field_id,
            ty,
            nullable: !key,
            is_primary_key: key,
        };
        TableSchema {
            ident: TableIdent {
                namespace: Namespace(vec!["public".into()]),
                name: "t".into(),
            },
            columns: vec![
                col("id", 1, T::Int, true),
                ColumnSchema {
                    nullable: false,
                    ..col("note", 2, T::String, false)
                },
                col("qty", 3, T::Int, false),
            ],
            partition_spec: vec![],
            pg_schema: None,
        }
    }

    fn source(cols: &[(&str, T)]) -> Vec<(String, T)> {
        cols.iter().map(|(n, t)| (n.to_string(), *t)).collect()
    }

    /// `(name, field id, nullable)` of the schema after `changes`.
    fn after(changes: &[SchemaChange]) -> Vec<(String, i32, bool)> {
        let mut s = table();
        apply_schema_changes(&mut s, changes).unwrap();
        s.columns
            .into_iter()
            .map(|c| (c.name, c.field_id, c.nullable))
            .collect()
    }

    fn cols(v: &[(&str, i32, bool)]) -> Vec<(String, i32, bool)> {
        v.iter().map(|(n, i, b)| (n.to_string(), *i, *b)).collect()
    }

    #[test]
    fn unchanged_columns_need_nothing() {
        let src = source(&[("id", T::Int), ("note", T::String), ("qty", T::Int)]);
        assert!(reconcile_columns(&table(), &src).unwrap().is_empty());
    }

    #[test]
    fn a_dropped_column_is_renamed_out_of_the_way() {
        // Discovery after `DROP COLUMN note` numbers qty 2 by position;
        // it keeps field id 3, and note keeps its values, renamed.
        let src = source(&[("id", T::Int), ("qty", T::Int)]);
        let changes = reconcile_columns(&table(), &src).unwrap();
        assert_eq!(
            after(&changes),
            cols(&[
                ("id", 1, false),
                ("note__dropped_2", 2, true),
                ("qty", 3, true)
            ])
        );
    }

    #[test]
    fn a_dropped_column_stays_dropped() {
        let src = source(&[("id", T::Int), ("qty", T::Int)]);
        let mut dropped = table();
        apply_schema_changes(&mut dropped, &reconcile_columns(&table(), &src).unwrap()).unwrap();
        assert!(reconcile_columns(&dropped, &src).unwrap().is_empty());
    }

    #[test]
    fn a_new_column_gets_a_new_field_id() {
        let src = source(&[
            ("id", T::Int),
            ("note", T::String),
            ("qty", T::Int),
            ("tag", T::String),
        ]);
        let changes = reconcile_columns(&table(), &src).unwrap();
        assert_eq!(after(&changes).last(), Some(&("tag".into(), 4, true)));
    }

    #[test]
    fn a_column_re_added_out_of_order_is_new() {
        // `DROP COLUMN note; ADD COLUMN note`: Postgres appends it.
        let src = source(&[("id", T::Int), ("qty", T::Int), ("note", T::String)]);
        let changes = reconcile_columns(&table(), &src).unwrap();
        assert_eq!(
            after(&changes),
            cols(&[
                ("id", 1, false),
                ("note__dropped_2", 2, true),
                ("qty", 3, true),
                ("note", 4, true),
            ])
        );
    }

    #[test]
    fn a_column_seen_dropped_and_back_is_new() {
        // qty was last, so its re-add keeps the order; the schema shows
        // it was dropped.
        let mut s = table();
        let gone = source(&[("id", T::Int), ("note", T::String)]);
        let changes = reconcile_columns(&s, &gone).unwrap();
        apply_schema_changes(&mut s, &changes).unwrap();
        let back = source(&[("id", T::Int), ("note", T::String), ("qty", T::Int)]);
        let changes = reconcile_columns(&s, &back).unwrap();
        apply_schema_changes(&mut s, &changes).unwrap();
        let columns: Vec<_> = s
            .columns
            .into_iter()
            .map(|c| (c.name, c.field_id, c.nullable))
            .collect();
        assert_eq!(
            columns,
            cols(&[
                ("id", 1, false),
                ("note", 2, false),
                ("qty__dropped_3", 3, true),
                ("qty", 4, true),
            ])
        );
    }

    #[test]
    fn type_changes_must_be_legal_promotions_of_non_key_columns() {
        let promote = source(&[("id", T::Int), ("note", T::String), ("qty", T::Long)]);
        let changes = reconcile_columns(&table(), &promote).unwrap();
        assert!(matches!(
            changes.as_slice(),
            [SchemaChange::PromoteColumnType { name, new_ty: T::Long }] if name == "qty"
        ));
        let narrow = source(&[("id", T::Int), ("note", T::Int), ("qty", T::Int)]);
        assert!(reconcile_columns(&table(), &narrow).is_err());
        let key = source(&[("id", T::Long), ("note", T::String), ("qty", T::Int)]);
        let err = reconcile_columns(&table(), &key).unwrap_err();
        assert!(err.to_string().contains("primary-key"));
    }
}
