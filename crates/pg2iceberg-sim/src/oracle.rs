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

use pg2iceberg_core::partition::{apply_transform, PartitionLiteral};
use pg2iceberg_core::{ColumnName, ColumnSchema, Row, TableSchema};
use pg2iceberg_iceberg::{read_data_file_by_field_id, PkKey, Snapshot};
use pg2iceberg_stream::BlobStore;
use std::collections::{BTreeMap, BTreeSet};

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
}

/// The live files of a table whose full commit history is `snapshots`:
/// every file a snapshot added that no snapshot removed, at its data
/// sequence number — the snapshot's, unless the file keeps an older one
/// (a compaction's output).
pub fn live_files_from_history(snapshots: &[Snapshot]) -> Vec<LiveFile> {
    let removed: BTreeSet<&str> = snapshots
        .iter()
        .flat_map(|s| s.removed_paths.iter().map(String::as_str))
        .collect();
    let mut out = Vec::new();
    for snap in snapshots {
        let files = snap.data_files.iter().map(|f| (f, None)).chain(
            snap.delete_files
                .iter()
                .map(|f| (f, Some(f.equality_field_ids.clone()))),
        );
        for (f, equality_ids) in files {
            if !removed.contains(f.path.as_str()) {
                out.push(LiveFile {
                    path: f.path.clone(),
                    seq: f.sequence_number_in(snap),
                    partition: f.partition_values.clone(),
                    equality_ids,
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

    let mut out = Vec::new();
    for f in files.iter().filter(|f| f.equality_ids.is_none()) {
        for row in read(blob, &f.path, &schema.columns).await? {
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
            out.push(row);
        }
    }
    Ok(out)
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
