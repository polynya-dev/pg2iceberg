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
//! - A deletion vector removes the rows at its positions of the data file
//!   it references, if in the same partition and at a sequence number no
//!   higher than its own. It is read from its Puffin blob by
//!   [`crate::deletion_vector`], which shares no code with its writers.
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

use crate::deletion_vector::{decode_blob, puffin_blobs, DeletionVectorRef, DELETION_VECTOR_V1};
use pg2iceberg_core::partition::{apply_transform, PartitionLiteral};
use pg2iceberg_core::{ColumnName, ColumnSchema, IcebergType, PgValue, Row, TableSchema};
use pg2iceberg_iceberg::{read_data_file_by_field_id, PkKey, Snapshot};
use pg2iceberg_stream::BlobStore;
use std::collections::{BTreeMap, BTreeSet};

/// The field id the spec reserves for `_row_id`.
pub const ROW_ID_FIELD_ID: i32 = 2147483540;
/// The field id the spec reserves for `_last_updated_sequence_number`.
pub const LAST_UPDATED_SEQUENCE_NUMBER_FIELD_ID: i32 = 2147483539;

/// A data file, equality-delete file or deletion vector in the table's
/// current state.
#[derive(Clone, Debug)]
pub struct LiveFile {
    /// Its location — for a deletion vector, its Puffin file's, which may
    /// hold others too.
    pub path: String,
    /// The file's data sequence number.
    pub seq: i64,
    /// The file's partition tuple. Empty for unpartitioned tables.
    pub partition: Vec<PartitionLiteral>,
    /// Its rows; for a deletion vector, the positions it deletes.
    pub record_count: u64,
    pub content: Content,
    /// A v3 data file's first row id: the id of each row it doesn't store
    /// one for is this plus the row's position in the file. `None` for a
    /// delete file, and on a table without row lineage (v2).
    pub first_row_id: Option<i64>,
}

/// What a [`LiveFile`] holds.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Content {
    Data,
    /// Equality deletes, on these field ids.
    EqualityDeletes(Vec<i32>),
    /// A deletion vector: positions deleted from one data file.
    DeletionVector(DeletionVectorRef),
}

impl LiveFile {
    pub fn is_data(&self) -> bool {
        self.content == Content::Data
    }

    /// The field ids of an equality-delete file.
    pub fn equality_ids(&self) -> Option<&[i32]> {
        match &self.content {
            Content::EqualityDeletes(ids) => Some(ids),
            _ => None,
        }
    }

    /// Where a deletion vector is.
    pub fn deletion_vector(&self) -> Option<&DeletionVectorRef> {
        match &self.content {
            Content::DeletionVector(dv) => Some(dv),
            _ => None,
        }
    }
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
    let removed_vectors: BTreeSet<(&str, u64)> = snapshots
        .iter()
        .flat_map(|s| &s.removed_deletion_vectors)
        .map(|key| (key.path.as_str(), key.offset))
        .collect();
    let mut out = Vec::new();
    for snap in snapshots {
        let files = snap
            .data_files
            .iter()
            .map(|f| (f, Content::Data, f.first_row_id))
            .chain(snap.delete_files.iter().map(|f| {
                (
                    f,
                    Content::EqualityDeletes(f.equality_field_ids.clone()),
                    None,
                )
            }));
        for (f, content, first_row_id) in files {
            if !removed.contains(f.path.as_str()) {
                out.push(LiveFile {
                    path: f.path.clone(),
                    seq: f.sequence_number_in(snap),
                    partition: f.partition_values.clone(),
                    record_count: f.record_count,
                    content,
                    first_row_id,
                });
            }
        }
        for dv in &snap.deletion_vectors {
            if !removed_vectors.contains(&(dv.path.as_str(), dv.offset)) {
                out.push(LiveFile {
                    path: dv.path.clone(),
                    seq: dv.sequence_number_in(snap),
                    partition: dv.partition_values.clone(),
                    record_count: dv.cardinality,
                    content: Content::DeletionVector(DeletionVectorRef {
                        referenced_data_file: dv.referenced_data_file.clone(),
                        offset: dv.offset,
                        length: dv.length,
                    }),
                    first_row_id: None,
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
        let Some(ids) = f.equality_ids() else {
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

    // Each data file's deletion vector. At most one may apply to a file
    // (where two do, what a reader returns is undefined).
    let mut vectors: BTreeMap<&str, (&LiveFile, BTreeSet<u64>)> = BTreeMap::new();
    for f in files {
        let Some(dv) = f.deletion_vector() else {
            continue;
        };
        let positions = read_deletion_vector(blob, f, dv).await?;
        let referenced = dv.referenced_data_file.as_str();
        if let Some((other, _)) = vectors.insert(referenced, (f, positions.into_iter().collect())) {
            return Err(format!(
                "two deletion vectors for {referenced}: {} at {} and {} at {}",
                other.path,
                other.deletion_vector().expect("a vector").offset,
                f.path,
                dv.offset
            ));
        }
    }

    let row_id_col = lineage_column("_row_id", ROW_ID_FIELD_ID);
    let seq_col = lineage_column(
        "_last_updated_sequence_number",
        LAST_UPDATED_SEQUENCE_NUMBER_FIELD_ID,
    );
    let mut cols = schema.columns.clone();
    cols.extend([row_id_col.clone(), seq_col.clone()]);
    let mut out = Vec::new();
    for f in files.iter().filter(|f| f.is_data()) {
        let vector = vectors
            .get(f.path.as_str())
            .filter(|(dv, _)| f.seq <= dv.seq && dv.partition == f.partition)
            .map(|(_, positions)| positions);
        for (pos, mut row) in read(blob, &f.path, &cols).await?.into_iter().enumerate() {
            if vector.is_some_and(|positions| positions.contains(&(pos as u64))) {
                continue;
            }
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

/// The positions deletion vector `dv`, in Puffin file `f`, deletes: its
/// blob, where its manifest entry says it is — as engines read it, without
/// the file's footer — holding as many as the entry says.
async fn read_deletion_vector(
    blob: &dyn BlobStore,
    f: &LiveFile,
    dv: &DeletionVectorRef,
) -> Result<Vec<u64>, String> {
    let at = format!("{}@{}", f.path, dv.offset);
    let bytes = blob.get(&f.path).await.map_err(|e| format!("{at}: {e}"))?;
    let end = dv
        .offset
        .checked_add(dv.length)
        .filter(|end| *end <= bytes.len() as u64);
    let Some(end) = end else {
        return Err(format!(
            "{at}: a {}-byte blob past the file's {} bytes",
            dv.length,
            bytes.len()
        ));
    };
    let positions =
        decode_blob(&bytes[dv.offset as usize..end as usize]).map_err(|e| format!("{at}: {e}"))?;
    if positions.len() as u64 != f.record_count {
        return Err(format!(
            "{at}: deletes {} positions, its entry's record count is {}",
            positions.len(),
            f.record_count
        ));
    }
    Ok(positions)
}

/// 25. Deletion vectors are as the spec has them: a data file has at
///     most one; it is live — whoever removes a data file removes its
///     vector — in the vector's partition, at a sequence number no higher;
///     the vector deletes only rows the file has; and its Puffin file's
///     footer describes its blob where its entry says, uncompressed, with
///     the data file and the count of positions it deletes, at snapshot
///     and sequence number -1.
pub async fn check_deletion_vectors(
    blob: &dyn BlobStore,
    files: &[LiveFile],
) -> Result<(), String> {
    let data: BTreeMap<&str, &LiveFile> = files
        .iter()
        .filter(|f| f.is_data())
        .map(|f| (f.path.as_str(), f))
        .collect();
    let mut seen: BTreeMap<&str, &LiveFile> = BTreeMap::new();
    for f in files {
        let Some(dv) = f.deletion_vector() else {
            continue;
        };
        let referenced = dv.referenced_data_file.as_str();
        let at = format!("deletion vector {}@{} of {referenced}", f.path, dv.offset);
        if let Some(other) = seen.insert(referenced, f) {
            return Err(format!("{at}: {referenced} has another, in {}", other.path));
        }
        let bytes = blob.get(&f.path).await.map_err(|e| format!("{at}: {e}"))?;
        let footer = puffin_blobs(&bytes).map_err(|e| format!("{at}: {e}"))?;
        let meta = footer
            .iter()
            .find(|b| b.offset == dv.offset)
            .ok_or_else(|| format!("{at}: the footer lists no blob there: {footer:?}"))?;
        let expected = [
            (
                "type",
                meta.blob_type.clone(),
                DELETION_VECTOR_V1.to_string(),
            ),
            ("length", meta.length.to_string(), dv.length.to_string()),
            (
                "compression-codec",
                format!("{:?}", meta.compression_codec),
                "None".into(),
            ),
            ("snapshot-id", meta.snapshot_id.to_string(), "-1".into()),
            (
                "sequence-number",
                meta.sequence_number.to_string(),
                "-1".into(),
            ),
            (
                "referenced-data-file",
                format!("{:?}", meta.properties.get("referenced-data-file")),
                format!("{:?}", Some(referenced)),
            ),
            (
                "cardinality",
                format!("{:?}", meta.properties.get("cardinality")),
                format!("{:?}", Some(f.record_count.to_string())),
            ),
        ];
        for (name, footer_says, expected) in expected {
            if footer_says != expected {
                return Err(format!(
                    "{at}: its footer's {name} is {footer_says}, not {expected}"
                ));
            }
        }
        let positions = read_deletion_vector(blob, f, dv).await?;
        let Some(target) = data.get(referenced) else {
            return Err(format!("{at}: {referenced} isn't live"));
        };
        if target.partition != f.partition {
            return Err(format!(
                "{at}: in partition {:?}, its data file in {:?}",
                f.partition, target.partition
            ));
        }
        if target.seq > f.seq {
            return Err(format!(
                "{at}: at sequence number {}, below its data file's {}",
                f.seq, target.seq
            ));
        }
        if let Some(past) = positions.iter().find(|p| **p >= target.record_count) {
            return Err(format!(
                "{at}: deletes position {past}, but {referenced} holds {} rows",
                target.record_count
            ));
        }
    }
    Ok(())
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::blob::MemoryBlobStore;
    use crate::deletion_vector::write::{blob as dv_blob, puffin, Container};
    use pg2iceberg_core::{Namespace, TableIdent};
    use pg2iceberg_iceberg::TableWriter;
    use pollster::block_on;

    fn schema() -> TableSchema {
        TableSchema {
            ident: TableIdent {
                namespace: Namespace(vec!["public".into()]),
                name: "t".into(),
            },
            columns: vec![ColumnSchema {
                name: "id".into(),
                field_id: 1,
                ty: IcebergType::Int,
                nullable: false,
                is_primary_key: true,
            }],
            partition_spec: Vec::new(),
            pg_schema: None,
        }
    }

    /// A data file `path` holding ids `0..rows`, at `seq`.
    fn data_file(store: &MemoryBlobStore, path: &str, rows: i32, seq: i64) -> LiveFile {
        let rows: Vec<Row> = (0..rows)
            .map(|id| Row::from([(ColumnName("id".into()), PgValue::Int4(id))]))
            .collect();
        let mut file = TableWriter::new(schema()).start_data_file().unwrap();
        file.write(&rows.iter().collect::<Vec<_>>()).unwrap();
        let chunk = file.finish().unwrap();
        block_on(store.put(path, chunk.bytes)).unwrap();
        LiveFile {
            path: path.into(),
            seq,
            partition: Vec::new(),
            record_count: chunk.record_count,
            content: Content::Data,
            first_row_id: None,
        }
    }

    /// Puffin file `path` of one deletion vector per `(data file,
    /// positions)`, at `seq`.
    fn vectors(
        store: &MemoryBlobStore,
        path: &str,
        dvs: &[(&str, &[u64])],
        seq: i64,
    ) -> Vec<LiveFile> {
        let blobs: Vec<(&str, Vec<u8>, u64)> = dvs
            .iter()
            .map(|(data, positions)| {
                (
                    *data,
                    dv_blob(positions, |_| Container::Array),
                    positions.len() as u64,
                )
            })
            .collect();
        let (file, at) = puffin(&blobs);
        block_on(store.put(path, file.into())).unwrap();
        dvs.iter()
            .zip(at)
            .map(|((data, positions), (offset, length))| LiveFile {
                path: path.into(),
                seq,
                partition: Vec::new(),
                record_count: positions.len() as u64,
                content: Content::DeletionVector(DeletionVectorRef {
                    referenced_data_file: (*data).into(),
                    offset,
                    length,
                }),
                first_row_id: None,
            })
            .collect()
    }

    fn ids(store: &MemoryBlobStore, files: &[LiveFile]) -> Result<Vec<i32>, String> {
        let rows = block_on(engine_read(store, &schema(), files))?;
        Ok(rows
            .iter()
            .map(|r| match r[&ColumnName("id".into())] {
                PgValue::Int4(id) => id,
                ref v => panic!("{v:?}"),
            })
            .collect())
    }

    #[test]
    fn a_deletion_vector_deletes_its_positions_of_its_data_file() {
        let store = MemoryBlobStore::new();
        let mut files = vec![data_file(&store, "a", 5, 1), data_file(&store, "b", 2, 1)];
        files.extend(vectors(&store, "v", &[("a", &[1, 3]), ("b", &[0])], 2));
        assert_eq!(ids(&store, &files).unwrap(), vec![0, 2, 4, 1]);
        block_on(check_deletion_vectors(&store, &files)).unwrap();
    }

    #[test]
    fn a_deletion_vector_applies_to_rows_of_its_own_commit() {
        let store = MemoryBlobStore::new();
        let mut files = vec![data_file(&store, "a", 3, 4)];
        files.extend(vectors(&store, "v", &[("a", &[2])], 4));
        assert_eq!(ids(&store, &files).unwrap(), vec![0, 1]);
    }

    #[test]
    fn a_deletion_vector_older_than_its_data_file_applies_to_none_of_it() {
        let store = MemoryBlobStore::new();
        let mut files = vec![data_file(&store, "a", 3, 4)];
        files.extend(vectors(&store, "v", &[("a", &[2])], 3));
        assert_eq!(ids(&store, &files).unwrap(), vec![0, 1, 2]);
        let err = block_on(check_deletion_vectors(&store, &files)).unwrap_err();
        assert!(err.contains("below its data file's"), "{err}");
    }

    #[test]
    fn two_deletion_vectors_for_one_data_file_are_an_error() {
        let store = MemoryBlobStore::new();
        let mut files = vec![data_file(&store, "a", 3, 1)];
        files.extend(vectors(&store, "v1", &[("a", &[0])], 2));
        files.extend(vectors(&store, "v2", &[("a", &[0, 1])], 3));
        assert!(ids(&store, &files)
            .unwrap_err()
            .contains("two deletion vectors"));
        assert!(block_on(check_deletion_vectors(&store, &files))
            .unwrap_err()
            .contains("has another"));
    }

    #[test]
    fn a_deletion_vector_must_hold_what_its_entry_says() {
        let store = MemoryBlobStore::new();
        let mut files = vec![data_file(&store, "a", 3, 1)];
        files.extend(vectors(&store, "v", &[("a", &[0, 1])], 2));
        files[1].record_count = 3;
        assert!(ids(&store, &files).unwrap_err().contains("record count"));
    }

    #[test]
    fn invariant_25_finds_a_vector_whose_data_file_is_gone() {
        let store = MemoryBlobStore::new();
        let files = vectors(&store, "v", &[("a", &[0])], 2);
        let err = block_on(check_deletion_vectors(&store, &files)).unwrap_err();
        assert!(err.contains("isn't live"), "{err}");
    }

    #[test]
    fn invariant_25_finds_a_position_past_the_data_files_rows() {
        let store = MemoryBlobStore::new();
        let mut files = vec![data_file(&store, "a", 3, 1)];
        files.extend(vectors(&store, "v", &[("a", &[3])], 2));
        let err = block_on(check_deletion_vectors(&store, &files)).unwrap_err();
        assert!(err.contains("holds 3 rows"), "{err}");
    }

    #[test]
    fn invariant_25_finds_an_entry_the_footer_disagrees_with() {
        let store = MemoryBlobStore::new();
        let mut files = vec![data_file(&store, "a", 3, 1), data_file(&store, "b", 3, 1)];
        files.extend(vectors(&store, "v", &[("a", &[0])], 2));
        // The entry names another data file than the blob was written for.
        if let Content::DeletionVector(dv) = &mut files[2].content {
            dv.referenced_data_file = "b".into();
        }
        let err = block_on(check_deletion_vectors(&store, &files)).unwrap_err();
        assert!(err.contains("referenced-data-file"), "{err}");
    }
}
