//! An independent reader of a materialized table: what a query engine
//! (Spark, Trino) returns for it, following the Iceberg spec rather than
//! pg2iceberg's own read path.
//!
//! Tests use it as ground truth, so it deliberately shares none of the
//! assumptions under test:
//!
//! - Columns are matched by field id, not name
//!   ([`read_data_file_by_field_id`]).
//! - An equality delete removes a row only if it is in the same partition
//!   and has a higher sequence number.
//! - A row lives in the partition its own values compute to. One stored
//!   elsewhere is skipped by any query that prunes on its partition
//!   column, so reading it is an error rather than a row.
//! - The live file set comes from the table's actual state — the full
//!   history ([`live_files_from_history`]) or the current snapshot's
//!   manifests — never from `Catalog::snapshots`.
//! - On a v3 table, each row's lineage ([`engine_read_lineage`]) is what
//!   the spec says it is: the `_row_id` and
//!   `_last_updated_sequence_number` the file stores for it, or else
//!   inherited — the file's first row id plus the row's position in it,
//!   and the file's data sequence number.

use pg2iceberg_core::partition::{apply_transform, PartitionLiteral};
use pg2iceberg_core::{ColumnName, ColumnSchema, IcebergType, PgValue, Row, TableSchema};
use pg2iceberg_iceberg::{read_data_file_by_field_id, PkKey, Snapshot};
use pg2iceberg_stream::BlobStore;
use std::collections::{BTreeMap, BTreeSet};

/// The field id the spec reserves for `_row_id`.
pub const ROW_ID_FIELD_ID: i32 = 2147483540;
/// The field id the spec reserves for `_last_updated_sequence_number`.
pub const LAST_UPDATED_SEQUENCE_NUMBER_FIELD_ID: i32 = 2147483539;

/// A data or equality-delete file in the table's current state.
#[derive(Clone, Debug)]
pub struct LiveFile {
    pub path: String,
    /// The file's data sequence number.
    pub seq: i64,
    /// The file's partition tuple. Empty for unpartitioned tables.
    pub partition: Vec<PartitionLiteral>,
    /// `Some(equality field ids)` for an equality-delete file.
    pub equality_ids: Option<Vec<i32>>,
    /// A v3 data file's first row id: the id of each row it doesn't store
    /// one for is this plus the row's position in the file. `None` for a
    /// delete file, and on a table without row lineage (v2).
    pub first_row_id: Option<i64>,
}

/// A row as a v3 engine reads it, with its lineage.
#[derive(Clone, Debug, PartialEq)]
pub struct LineageRow {
    pub row: Row,
    /// `_row_id`: the one the file stores for the row, else the file's
    /// first row id plus the row's position. `None` if neither exists.
    pub row_id: Option<i64>,
    /// `_last_updated_sequence_number`: the one the file stores for the
    /// row, else — in a file with row lineage — the file's data sequence
    /// number.
    pub last_updated: Option<i64>,
}

/// The live files of a table whose full commit history is `snapshots`:
/// every file a snapshot added that no snapshot removed, at its data
/// sequence number — the snapshot's, unless the file keeps an older one
/// (a compaction's output) — and, on a v3 table, at the first row id
/// its commit gave it.
pub fn live_files_from_history(snapshots: &[Snapshot]) -> Vec<LiveFile> {
    let removed: BTreeSet<&str> = snapshots
        .iter()
        .flat_map(|s| s.removed_paths.iter().map(String::as_str))
        .collect();
    let mut out = Vec::new();
    for snap in snapshots {
        let files = snap
            .data_files
            .iter()
            .map(|f| (f, None, f.first_row_id))
            .chain(
                snap.delete_files
                    .iter()
                    .map(|f| (f, Some(f.equality_field_ids.clone()), None)),
            );
        for (f, equality_ids, first_row_id) in files {
            if !removed.contains(f.path.as_str()) {
                out.push(LiveFile {
                    path: f.path.clone(),
                    seq: f.sequence_number_in(snap),
                    partition: f.partition_values.clone(),
                    equality_ids,
                    first_row_id,
                });
            }
        }
    }
    out
}

/// The rows a query engine reads from `files`, with columns resolved
/// against `schema` (the table's current schema) by field id.
pub async fn engine_read(
    blob: &dyn BlobStore,
    schema: &TableSchema,
    files: &[LiveFile],
) -> Result<Vec<Row>, String> {
    Ok(engine_read_lineage(blob, schema, files)
        .await?
        .into_iter()
        .map(|r| r.row)
        .collect())
}

/// [`engine_read`], with each row's lineage.
pub async fn engine_read_lineage(
    blob: &dyn BlobStore,
    schema: &TableSchema,
    files: &[LiveFile],
) -> Result<Vec<LineageRow>, String> {
    let by_id: BTreeMap<i32, &ColumnSchema> =
        schema.columns.iter().map(|c| (c.field_id, c)).collect();

    struct Deletes {
        cols: Vec<ColumnName>,
        seq: i64,
        partition: Vec<PartitionLiteral>,
        keys: BTreeSet<PkKey>,
    }
    let mut deletes = Vec::new();
    for f in files {
        let Some(ids) = &f.equality_ids else {
            continue;
        };
        let cols = ids
            .iter()
            .map(|id| {
                by_id
                    .get(id)
                    .map(|c| (*c).clone())
                    .ok_or_else(|| format!("{}: equality field id {id} not in schema", f.path))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let names: Vec<ColumnName> = cols.iter().map(|c| ColumnName(c.name.clone())).collect();
        let keys = read(blob, &f.path, &cols)
            .await?
            .iter()
            .map(|r| PkKey::from_row(r, &names))
            .collect();
        deletes.push(Deletes {
            cols: names,
            seq: f.seq,
            partition: f.partition.clone(),
            keys,
        });
    }

    let row_id_col = lineage_column("_row_id", ROW_ID_FIELD_ID);
    let seq_col = lineage_column(
        "_last_updated_sequence_number",
        LAST_UPDATED_SEQUENCE_NUMBER_FIELD_ID,
    );
    let mut cols = schema.columns.clone();
    cols.extend([row_id_col.clone(), seq_col.clone()]);
    let mut out = Vec::new();
    for f in files.iter().filter(|f| f.equality_ids.is_none()) {
        for (pos, mut row) in read(blob, &f.path, &cols).await?.into_iter().enumerate() {
            let stored_id =
                take_long(&mut row, &row_id_col).map_err(|e| format!("{}: {e}", f.path))?;
            let stored_seq =
                take_long(&mut row, &seq_col).map_err(|e| format!("{}: {e}", f.path))?;
            let deleted = deletes.iter().any(|d| {
                d.seq > f.seq
                    && d.partition == f.partition
                    && d.keys.contains(&PkKey::from_row(&row, &d.cols))
            });
            if deleted {
                continue;
            }
            let home = partition_of(&row, schema).map_err(|e| format!("{}: {e}", f.path))?;
            if home != f.partition {
                return Err(format!(
                    "{}: {row:?} is stored in partition {:?}, but its values put it in {home:?}",
                    f.path, f.partition
                ));
            }
            out.push(LineageRow {
                row,
                row_id: stored_id.or(f.first_row_id.map(|first| first + pos as i64)),
                last_updated: stored_seq.or(f.first_row_id.map(|_| f.seq)),
            });
        }
    }
    Ok(out)
}

/// One of the two columns of row lineage the spec reserves.
fn lineage_column(name: &str, field_id: i32) -> ColumnSchema {
    ColumnSchema {
        name: name.into(),
        field_id,
        ty: IcebergType::Long,
        nullable: true,
        is_primary_key: false,
    }
}

/// Remove `col` from `row`: the value the file stores for it, if any.
fn take_long(row: &mut Row, col: &ColumnSchema) -> Result<Option<i64>, String> {
    match row.remove(&ColumnName(col.name.clone())) {
        None | Some(PgValue::Null) => Ok(None),
        Some(PgValue::Int8(n)) => Ok(Some(n)),
        Some(other) => Err(format!("{} holds {other:?}", col.name)),
    }
}

/// The partition `row`'s values compute to under `schema`'s spec.
fn partition_of(row: &Row, schema: &TableSchema) -> Result<Vec<PartitionLiteral>, String> {
    schema
        .partition_spec
        .iter()
        .map(|f| {
            let value = row
                .get(&ColumnName(f.source_column.clone()))
                .ok_or_else(|| format!("{row:?} has no partition column {}", f.source_column))?;
            apply_transform(value, f.transform).map_err(|e| e.to_string())
        })
        .collect()
}

async fn read(blob: &dyn BlobStore, path: &str, cols: &[ColumnSchema]) -> Result<Vec<Row>, String> {
    let bytes = blob.get(path).await.map_err(|e| format!("{path}: {e}"))?;
    read_data_file_by_field_id(bytes, cols).map_err(|e| format!("{path}: {e}"))
}
