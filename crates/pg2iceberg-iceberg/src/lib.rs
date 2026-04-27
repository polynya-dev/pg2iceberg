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
#[cfg(feature = "prod")]
pub mod prod;
pub mod reader;
pub mod verify;
pub mod writer;

pub use compact::{compact_table, CompactError, CompactionConfig, CompactionOutcome};
pub use file_index::{rebuild_from_catalog, FileIndex};
pub use fold::{fold_events, pk_key, MaterializedRow};
pub use materialize::{promote_re_inserts, resolve_unchanged_cols};
pub use orphan::{cleanup_orphans, CleanupError, CleanupOutcome};
pub use reader::read_data_file;
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
    /// output instead.
    pub removed_paths: Vec<String>,
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
}

/// One Iceberg snapshot. Snapshots are append-only and ordered by `id`.
/// Iceberg MoR semantics: a delete file at snapshot `N` applies only to data
/// files at snapshots `< N` (data and deletes from the same snapshot are
/// kept consistent by the materializer).
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
        }
    }
    Ok(())
}

#[async_trait]
pub trait Catalog: Send + Sync {
    async fn ensure_namespace(&self, ns: &Namespace) -> Result<()>;
    /// `Ok(None)` for "table doesn't exist yet" — the materializer treats
    /// this distinctly from a transient catalog error.
    async fn load_table(&self, ident: &TableIdent) -> Result<Option<TableMetadata>>;
    async fn create_table(&self, schema: &TableSchema) -> Result<TableMetadata>;
    async fn commit_snapshot(&self, prepared: PreparedCommit) -> Result<TableMetadata>;
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
    async fn evolve_schema(
        &self,
        ident: &TableIdent,
        changes: Vec<SchemaChange>,
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
    /// Append-only snapshot history for `ident`. Used by the verifier (MoR
    /// reads) and by materializer restart code (FileIndex rebuild). Ordered
    /// by snapshot id ascending.
    async fn snapshots(&self, ident: &TableIdent) -> Result<Vec<Snapshot>>;
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
        assert!(!is_legal_type_promotion(IcebergType::Long, IcebergType::Int));
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
