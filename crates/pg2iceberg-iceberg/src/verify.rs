//! `read_materialized_state`: compute the visible row set of a materialized
//! Iceberg table by applying merge-on-read semantics to the snapshot history.
//!
//! Iceberg MoR rule: an equality-delete file at snapshot `N` applies to data
//! files at snapshots strictly less than `N`. Data and deletes from the same
//! commit (the materializer always writes them together for an Update) stay
//! mutually consistent: the new data row is visible, the prior data row is
//! invalidated.
//!
//! This is the read path for the future Phase 12 `verify` subcommand. For
//! Phase 8 it powers the materializer's end-to-end correctness tests.

use crate::fold::pk_key;
use crate::reader::read_data_file;
use crate::writer::WriterError;
use crate::{Catalog, IcebergError};
use async_trait::async_trait;
use pg2iceberg_core::{ColumnName, ColumnSchema, Row, TableIdent, TableSchema};
use pg2iceberg_stream::BlobStore;
use std::collections::BTreeSet;

#[derive(Debug, thiserror::Error)]
pub enum VerifyError {
    #[error("catalog: {0}")]
    Catalog(#[from] IcebergError),
    #[error("blob: {0}")]
    Blob(#[from] pg2iceberg_stream::StreamError),
    #[error("decode: {0}")]
    Decode(#[from] WriterError),
}

impl VerifyError {
    /// Helper used by `file_index::rebuild_from_catalog` to thread errors
    /// through the dyn-catalog boundary without exposing `Result` aliases
    /// in two different modules.
    pub(crate) fn from_dyn(e: VerifyError) -> Self {
        e
    }
}

pub type Result<T> = std::result::Result<T, VerifyError>;

/// Compute the visible row set for a materialized table.
///
/// Walks every snapshot's data files; for each row, includes it iff no later
/// snapshot's equality-delete files mark its PK as deleted.
///
/// Output rows are in (snapshot id, in-file order) — caller sorts by PK if
/// it needs deterministic comparison.
pub async fn read_materialized_state(
    catalog: &dyn DynCatalog,
    blob_store: &dyn BlobStore,
    ident: &TableIdent,
    schema: &TableSchema,
    pk_cols: &[ColumnName],
) -> Result<Vec<Row>> {
    let snapshots = catalog.snapshots(ident).await?;
    if snapshots.is_empty() {
        return Ok(Vec::new());
    }

    // PK-only schema for decoding equality-delete files.
    let pk_schema: Vec<ColumnSchema> = schema
        .columns
        .iter()
        .filter(|c| c.is_primary_key)
        .cloned()
        .collect();

    // Cumulative set of file paths removed by compaction snapshots.
    // A data/delete file whose path is in here at the time we'd otherwise
    // read it is invisible to subsequent snapshots — its rows have been
    // folded into a compacted file (and any deletes already applied at
    // compaction time).
    let removed_paths: BTreeSet<String> = snapshots
        .iter()
        .flat_map(|s| s.removed_paths.iter().cloned())
        .collect();

    // Per-snapshot deleted-pk sets, in snapshot order. Skip delete files
    // that have been compacted away — their PKs are already filtered out
    // of the surviving data files.
    let mut deletes_per_snap: Vec<(i64, BTreeSet<String>)> = Vec::with_capacity(snapshots.len());
    for snap in &snapshots {
        let mut snap_deleted = BTreeSet::new();
        for df in &snap.delete_files {
            if removed_paths.contains(&df.path) {
                continue;
            }
            let bytes = blob_store.get(&df.path).await?;
            let rows = read_data_file(&bytes, &pk_schema)?;
            for row in rows {
                snap_deleted.insert(pk_key(&row, pk_cols));
            }
        }
        deletes_per_snap.push((snap.id, snap_deleted));
    }

    let mut visible: Vec<Row> = Vec::new();
    for snap in &snapshots {
        for df in &snap.data_files {
            // Compaction-replaced files are invisible — their surviving rows
            // live in a later compaction snapshot's `data_files`.
            if removed_paths.contains(&df.path) {
                continue;
            }
            let bytes = blob_store.get(&df.path).await?;
            let rows = read_data_file(&bytes, &schema.columns)?;
            for row in rows {
                let key = pk_key(&row, pk_cols);
                let deleted_later = deletes_per_snap
                    .iter()
                    .any(|(sid, set)| *sid > snap.id && set.contains(&key));
                if !deleted_later {
                    visible.push(row);
                }
            }
        }
    }
    Ok(visible)
}

/// Object-safe alias for `Catalog`. The blanket impl below makes any
/// `Catalog` usable through `&dyn DynCatalog`, so the verifier doesn't have
/// to be generic over the catalog type.
#[async_trait]
pub trait DynCatalog: Send + Sync {
    async fn snapshots(&self, ident: &TableIdent) -> Result<Vec<crate::Snapshot>>;
}

#[async_trait]
impl<C: Catalog + ?Sized> DynCatalog for C {
    async fn snapshots(&self, ident: &TableIdent) -> Result<Vec<crate::Snapshot>> {
        Ok(<C as Catalog>::snapshots(self, ident).await?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fold::MaterializedRow;
    use crate::writer::TableWriter;
    use crate::DataFile;
    use pg2iceberg_core::typemap::IcebergType;
    use pg2iceberg_core::{Namespace, Op, PgValue};
    use std::collections::BTreeMap;

    // We use the sim's MemoryCatalog + MemoryBlobStore via a small reach-down
    // here. Keeping these as `cfg(test)` deps would require pulling
    // pg2iceberg-sim as a dev-dep into iceberg, which would cycle. Instead
    // we hand-roll minimal test doubles below.

    use std::sync::Mutex;

    #[derive(Default)]
    struct FakeCatalog {
        snaps: Mutex<BTreeMap<TableIdent, Vec<crate::Snapshot>>>,
    }
    #[async_trait]
    impl DynCatalog for FakeCatalog {
        async fn snapshots(&self, ident: &TableIdent) -> Result<Vec<crate::Snapshot>> {
            Ok(self
                .snaps
                .lock()
                .unwrap()
                .get(ident)
                .cloned()
                .unwrap_or_default())
        }
    }

    #[derive(Default)]
    struct FakeBlob {
        inner: Mutex<BTreeMap<String, bytes::Bytes>>,
    }
    #[async_trait]
    impl BlobStore for FakeBlob {
        async fn put(
            &self,
            path: &str,
            bytes: bytes::Bytes,
        ) -> std::result::Result<(), pg2iceberg_stream::StreamError> {
            self.inner.lock().unwrap().insert(path.to_string(), bytes);
            Ok(())
        }
        async fn get(
            &self,
            path: &str,
        ) -> std::result::Result<bytes::Bytes, pg2iceberg_stream::StreamError> {
            self.inner
                .lock()
                .unwrap()
                .get(path)
                .cloned()
                .ok_or_else(|| pg2iceberg_stream::StreamError::Io(format!("missing: {path}")))
        }
        async fn list(
            &self,
            prefix: &str,
        ) -> std::result::Result<Vec<pg2iceberg_stream::BlobInfo>, pg2iceberg_stream::StreamError>
        {
            let lock = self.inner.lock().unwrap();
            Ok(lock
                .iter()
                .filter(|(k, _)| k.starts_with(prefix))
                .map(|(k, v)| pg2iceberg_stream::BlobInfo {
                    path: k.clone(),
                    size: v.len() as u64,
                    last_modified_ms: 0,
                })
                .collect())
        }
        async fn delete(
            &self,
            path: &str,
        ) -> std::result::Result<(), pg2iceberg_stream::StreamError> {
            self.inner.lock().unwrap().remove(path);
            Ok(())
        }
    }

    fn ident() -> TableIdent {
        TableIdent {
            namespace: Namespace(vec!["public".into()]),
            name: "orders".into(),
        }
    }

    fn schema() -> TableSchema {
        TableSchema {
            ident: ident(),
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

    fn col(n: &str) -> ColumnName {
        ColumnName(n.into())
    }

    fn row(id: i32, qty: i32) -> Row {
        let mut r = BTreeMap::new();
        r.insert(col("id"), PgValue::Int4(id));
        r.insert(col("qty"), PgValue::Int4(qty));
        r
    }

    fn pk_only(id: i32) -> Row {
        let mut r = BTreeMap::new();
        r.insert(col("id"), PgValue::Int4(id));
        r
    }

    /// Helper: write a snapshot to FakeCatalog + FakeBlob using TableWriter.
    fn commit_snapshot(
        cat: &FakeCatalog,
        blob: &FakeBlob,
        s: &TableSchema,
        snap_id: i64,
        rows: &[MaterializedRow],
        path_prefix: &str,
    ) {
        use pollster::block_on;
        let w = TableWriter::new(s.clone());
        let prepared = w.prepare(rows, &crate::FileIndex::new()).unwrap();
        let mut data_files = Vec::new();
        let mut delete_files = Vec::new();
        for (i, chunk) in prepared.data.into_iter().enumerate() {
            let path = if i == 0 {
                format!("{path_prefix}-data")
            } else {
                format!("{path_prefix}-data-{i}")
            };
            block_on(blob.put(&path, chunk.chunk.bytes.clone())).unwrap();
            data_files.push(DataFile {
                path,
                record_count: chunk.chunk.record_count,
                byte_size: 0,
                equality_field_ids: vec![],
                partition_values: chunk.partition_values,
            });
        }
        for (i, chunk) in prepared.equality_deletes.into_iter().enumerate() {
            let path = if i == 0 {
                format!("{path_prefix}-eqdel")
            } else {
                format!("{path_prefix}-eqdel-{i}")
            };
            block_on(blob.put(&path, chunk.chunk.bytes.clone())).unwrap();
            delete_files.push(DataFile {
                path,
                record_count: chunk.chunk.record_count,
                byte_size: 0,
                equality_field_ids: prepared.pk_field_ids.clone(),
                partition_values: chunk.partition_values,
            });
        }
        cat.snaps
            .lock()
            .unwrap()
            .entry(ident())
            .or_default()
            .push(crate::Snapshot {
                id: snap_id,
                data_files,
                delete_files,
                removed_paths: Vec::new(),
                timestamp_ms: snap_id * 1000,
            });
    }

    #[test]
    fn empty_history_yields_empty_state() {
        use pollster::block_on;
        let cat = FakeCatalog::default();
        let blob = FakeBlob::default();
        let v = block_on(read_materialized_state(
            &cat,
            &blob,
            &ident(),
            &schema(),
            &[col("id")],
        ))
        .unwrap();
        assert!(v.is_empty());
    }

    #[test]
    fn single_insert_visible() {
        use pollster::block_on;
        let cat = FakeCatalog::default();
        let blob = FakeBlob::default();
        let s = schema();
        commit_snapshot(
            &cat,
            &blob,
            &s,
            1,
            &[MaterializedRow {
                op: Op::Insert,
                row: row(1, 10),
                unchanged_cols: vec![],
            }],
            "snap1",
        );
        let v = block_on(read_materialized_state(
            &cat,
            &blob,
            &ident(),
            &s,
            &[col("id")],
        ))
        .unwrap();
        assert_eq!(v, vec![row(1, 10)]);
    }

    #[test]
    fn update_supersedes_prior_snapshot_via_mor() {
        use pollster::block_on;
        let cat = FakeCatalog::default();
        let blob = FakeBlob::default();
        let s = schema();
        // Snap 1: INSERT 1, qty=10
        commit_snapshot(
            &cat,
            &blob,
            &s,
            1,
            &[MaterializedRow {
                op: Op::Insert,
                row: row(1, 10),
                unchanged_cols: vec![],
            }],
            "snap1",
        );
        // Snap 2: UPDATE 1, qty=99 → emits data file + equality delete on PK 1.
        // Verifier should see (1, 99), not (1, 10) — the snap-2 delete excludes
        // the snap-1 data, the snap-2 data is unaffected by the same-snap delete.
        commit_snapshot(
            &cat,
            &blob,
            &s,
            2,
            &[MaterializedRow {
                op: Op::Update,
                row: row(1, 99),
                unchanged_cols: vec![],
            }],
            "snap2",
        );
        let v = block_on(read_materialized_state(
            &cat,
            &blob,
            &ident(),
            &s,
            &[col("id")],
        ))
        .unwrap();
        assert_eq!(v, vec![row(1, 99)]);
    }

    #[test]
    fn delete_drops_row_from_visibility() {
        use pollster::block_on;
        let cat = FakeCatalog::default();
        let blob = FakeBlob::default();
        let s = schema();
        commit_snapshot(
            &cat,
            &blob,
            &s,
            1,
            &[MaterializedRow {
                op: Op::Insert,
                row: row(1, 10),
                unchanged_cols: vec![],
            }],
            "snap1",
        );
        commit_snapshot(
            &cat,
            &blob,
            &s,
            2,
            &[MaterializedRow {
                op: Op::Delete,
                row: pk_only(1),
                unchanged_cols: vec![],
            }],
            "snap2",
        );
        let v = block_on(read_materialized_state(
            &cat,
            &blob,
            &ident(),
            &s,
            &[col("id")],
        ))
        .unwrap();
        assert!(v.is_empty());
    }

    #[test]
    fn re_insert_after_delete_visible_via_promote() {
        // Materializer would have promoted the second insert to Update because
        // the PK is in FileIndex. Simulate that here: snap 3 has a fresh data
        // row + equality delete. Verifier shows the new row.
        use pollster::block_on;
        let cat = FakeCatalog::default();
        let blob = FakeBlob::default();
        let s = schema();
        commit_snapshot(
            &cat,
            &blob,
            &s,
            1,
            &[MaterializedRow {
                op: Op::Insert,
                row: row(1, 10),
                unchanged_cols: vec![],
            }],
            "snap1",
        );
        commit_snapshot(
            &cat,
            &blob,
            &s,
            2,
            &[MaterializedRow {
                op: Op::Delete,
                row: pk_only(1),
                unchanged_cols: vec![],
            }],
            "snap2",
        );
        commit_snapshot(
            &cat,
            &blob,
            &s,
            3,
            &[MaterializedRow {
                op: Op::Insert,
                row: row(1, 200),
                unchanged_cols: vec![],
            }],
            "snap3",
        );
        let v = block_on(read_materialized_state(
            &cat,
            &blob,
            &ident(),
            &s,
            &[col("id")],
        ))
        .unwrap();
        assert_eq!(v, vec![row(1, 200)]);
    }
}
