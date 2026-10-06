//! In-memory PK ↔ file map for a single materialized table.
//!
//! Mirrors `iceberg/tablewriter.go:84-126`. The materializer maintains one
//! `FileIndex` per table and updates it after each commit:
//! - Newly-written data files contribute their PKs ([`add_file`]).
//! - Equality-deleted PKs get removed ([`remove_pks`]).
//!
//! Used for two correctness reasons:
//! 1. **TOAST resolution.** TOAST `unchanged_cols` placeholders need the prior
//!    column values, which live in some prior data file. The materializer
//!    asks the index for the file path, fetches it, and copies the unchanged
//!    columns in.
//! 2. **Re-insert promotion.** An `Insert` whose PK already lives in a prior
//!    data file must be downgraded to `Update` so the writer emits an
//!    equality delete; otherwise readers would see two rows for that PK.
//!
//! Keys are [`PkKey`]s, which compare stored values: a key built from a
//! WAL event equals the key read back from a data file.
//!
//! On materializer restart, the index is rebuilt from the catalog's
//! snapshot history — see [`rebuild_from_catalog`].

use crate::pk::PkKey;
use pg2iceberg_core::PartitionLiteral;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fmt;

/// A data file's slot in [`FileIndex::files`].
type FileId = u32;

/// Live PK → data file map. One entry per live row, so it is kept
/// compact: a [`PkKey`] (inline for typical keys) plus a 4-byte file
/// number, with each file's path stored once.
#[derive(Default, Clone)]
pub struct FileIndex {
    /// Live PK → the data file holding its current row.
    keys: HashMap<PkKey, FileId>,
    /// Data files holding at least one live row; `None` slots are free.
    files: Vec<Option<IndexedFile>>,
    free: Vec<FileId>,
    ids: HashMap<String, FileId>,
}

#[derive(Clone)]
struct IndexedFile {
    path: String,
    /// One literal per partition spec field; empty for unpartitioned
    /// tables. Lets a cross-batch `Delete` on a partitioned table recover
    /// the partition tuple of its PK's prior data file.
    partition_values: Vec<PartitionLiteral>,
    /// Keys in `keys` pointing at this file. The file leaves the index
    /// when it reaches zero.
    live: u64,
}

impl FileIndex {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register all PKs in a freshly written data file. Replaces any prior
    /// PK→file mapping (the new file is now authoritative for these PKs).
    /// `partition_values` is the data file's partition tuple (empty for
    /// unpartitioned tables).
    pub fn add_file(
        &mut self,
        path: String,
        pk_keys: impl IntoIterator<Item = PkKey>,
        partition_values: Vec<PartitionLiteral>,
    ) {
        let id = match self.ids.get(&path) {
            Some(&id) => {
                if !partition_values.is_empty() {
                    self.file_mut(id).partition_values = partition_values;
                }
                id
            }
            None => self.insert_file(path, partition_values),
        };
        for pk in pk_keys {
            match self.keys.insert(pk, id) {
                Some(prev) if prev == id => {}
                Some(prev) => {
                    self.file_mut(id).live += 1;
                    self.release(prev);
                }
                None => self.file_mut(id).live += 1,
            }
        }
        if self.file(id).live == 0 {
            self.drop_file(id);
        }
    }

    pub fn lookup(&self, pk_key: &PkKey) -> Option<&str> {
        self.keys.get(pk_key).map(|&id| self.file(id).path.as_str())
    }

    pub fn contains_pk(&self, pk_key: &PkKey) -> bool {
        self.keys.contains_key(pk_key)
    }

    /// Resolve the partition tuple of the data file currently holding `pk_key`.
    /// Returns `None` when the PK isn't indexed *or* when its file is
    /// unpartitioned. Used by `TableWriter::prepare` to recover partition
    /// values for a `Delete` row whose row payload doesn't carry the
    /// partition source columns (see Go's `ExtractPartBucketKey` /
    /// `ParsePartitionPath` in `iceberg/partition.go` — same intent, but
    /// we carry structured values per file instead of parsing hive paths).
    pub fn partition_values_for_pk(&self, pk_key: &PkKey) -> Option<&[PartitionLiteral]> {
        let file = self.file(*self.keys.get(pk_key)?);
        (!file.partition_values.is_empty()).then_some(file.partition_values.as_slice())
    }

    /// Forget `path` (rewritten by compaction) and every PK still pointing
    /// at it. Add the compaction outputs with [`Self::add_file`] first:
    /// that moves the surviving PKs off `path`, leaving nothing to scan.
    pub fn remove_file(&mut self, path: &str) {
        let Some(&id) = self.ids.get(path) else {
            return;
        };
        if self.file(id).live > 0 {
            self.keys.retain(|_, f| *f != id);
        }
        self.drop_file(id);
    }

    /// Mark these PKs as deleted. A file leaves the index once none of
    /// its PKs are live.
    pub fn remove_pks<'a>(&mut self, pk_keys: impl IntoIterator<Item = &'a PkKey>) {
        for pk in pk_keys {
            if let Some(id) = self.keys.remove(pk) {
                self.release(id);
            }
        }
    }

    /// Returns the set of file paths that contain at least one of the given
    /// PKs. Used by the materializer to know which files to fetch for TOAST
    /// resolution.
    pub fn affected_files(&self, pk_keys: &[PkKey]) -> BTreeSet<String> {
        pk_keys
            .iter()
            .filter_map(|pk| self.lookup(pk))
            .map(str::to_string)
            .collect()
    }

    /// Data files holding at least one live PK, sorted.
    pub fn live_files(&self) -> Vec<&str> {
        let mut out: Vec<&str> = self.live().map(|f| f.path.as_str()).collect();
        out.sort_unstable();
        out
    }

    pub fn live_pk_count(&self) -> usize {
        self.keys.len()
    }

    /// Live rows per data file. A file whose live rows are fewer than its
    /// records holds dead rows, which compaction can drop.
    pub fn live_rows_per_file(&self) -> BTreeMap<&str, u64> {
        self.live().map(|f| (f.path.as_str(), f.live)).collect()
    }

    /// Iterate every currently-live PK key, in no particular order. Used
    /// by the materializer's TRUNCATE expansion: a `TRUNCATE` event has
    /// no per-row payload, so we materialize it by emitting one
    /// equality-delete per known PK before continuing the cycle.
    pub fn all_pks(&self) -> impl Iterator<Item = &PkKey> {
        self.keys.keys()
    }

    fn live(&self) -> impl Iterator<Item = &IndexedFile> {
        self.files.iter().flatten()
    }

    fn file(&self, id: FileId) -> &IndexedFile {
        self.files[id as usize].as_ref().expect("live file id")
    }

    fn file_mut(&mut self, id: FileId) -> &mut IndexedFile {
        self.files[id as usize].as_mut().expect("live file id")
    }

    fn insert_file(&mut self, path: String, partition_values: Vec<PartitionLiteral>) -> FileId {
        let file = IndexedFile {
            path: path.clone(),
            partition_values,
            live: 0,
        };
        let id = match self.free.pop() {
            Some(id) => {
                self.files[id as usize] = Some(file);
                id
            }
            None => {
                let id = FileId::try_from(self.files.len()).expect("under 2^32 live data files");
                self.files.push(Some(file));
                id
            }
        };
        self.ids.insert(path, id);
        id
    }

    /// One fewer PK points at `id`.
    fn release(&mut self, id: FileId) {
        let file = self.file_mut(id);
        file.live -= 1;
        if file.live == 0 {
            self.drop_file(id);
        }
    }

    fn drop_file(&mut self, id: FileId) {
        if let Some(file) = self.files[id as usize].take() {
            self.ids.remove(&file.path);
            self.free.push(id);
        }
    }

    /// PK → path, and path → partition tuple: what the index means,
    /// independent of file numbering.
    #[allow(clippy::type_complexity)]
    fn view(&self) -> (BTreeMap<&PkKey, &str>, BTreeMap<&str, &[PartitionLiteral]>) {
        let pks = self
            .keys
            .iter()
            .map(|(pk, &id)| (pk, self.file(id).path.as_str()))
            .collect();
        let partitions = self
            .live()
            .map(|f| (f.path.as_str(), f.partition_values.as_slice()))
            .collect();
        (pks, partitions)
    }
}

impl PartialEq for FileIndex {
    fn eq(&self, other: &Self) -> bool {
        self.keys.len() == other.keys.len() && self.view() == other.view()
    }
}

impl fmt::Debug for FileIndex {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let (pks, partitions) = self.view();
        f.debug_struct("FileIndex")
            .field("pks", &pks)
            .field("partitions", &partitions)
            .finish()
    }
}

/// Rebuild a `FileIndex` for `ident` from the catalog's snapshot history.
///
/// Used on materializer restart so re-insert promotion keeps working —
/// without this, a freshly-booted process has an empty FileIndex and a
/// re-insert of a previously-materialized PK won't emit the equality
/// delete that's needed to void the prior data file row, producing
/// duplicate rows in MoR readers.
///
/// MoR semantics: an equality-delete file at snapshot `N` voids data file
/// rows whose PK matches at snapshots `< N`. Replaying the snapshots in
/// order — each one's deletes, then its data files — therefore leaves
/// exactly the live PKs, as the materializer's own updates do. Files
/// are streamed a batch at a time and only their PK columns decoded, so
/// the rebuild holds little beyond the index itself.
pub async fn rebuild_from_catalog(
    catalog: &dyn pg2iceberg_iceberg_dyn::DynCatalog,
    blob_store: &dyn pg2iceberg_stream::BlobStore,
    ident: &pg2iceberg_core::TableIdent,
    schema: &pg2iceberg_core::TableSchema,
    pk_cols: &[pg2iceberg_core::ColumnName],
) -> std::result::Result<FileIndex, crate::verify::VerifyError> {
    let mut fi = FileIndex::new();
    catch_up_from_catalog(&mut fi, None, catalog, blob_store, ident, schema, pk_cols).await?;
    Ok(fi)
}

/// Bring `fi` — `ident`'s index as of its snapshot `at` (`None`: before
/// the first, so `fi` is empty) — up to the catalog's current snapshot,
/// and return that snapshot.
///
/// Replays only the snapshots after `at`, while history holds every one
/// of them. Past an expired one it rebuilds instead: a stand-in lists the
/// files its snapshot added that are still live, not what it removed.
pub async fn catch_up_from_catalog(
    fi: &mut FileIndex,
    at: Option<i64>,
    catalog: &dyn pg2iceberg_iceberg_dyn::DynCatalog,
    blob_store: &dyn pg2iceberg_stream::BlobStore,
    ident: &pg2iceberg_core::TableIdent,
    schema: &pg2iceberg_core::TableSchema,
    pk_cols: &[pg2iceberg_core::ColumnName],
) -> std::result::Result<Option<i64>, crate::verify::VerifyError> {
    use crate::verify::VerifyError;

    let snapshots = catalog
        .snapshots(ident)
        .await
        .map_err(VerifyError::from_dyn)?;
    let current = snapshots.last().map(|s| s.id);
    let newer = match at {
        Some(at) => snapshots
            .iter()
            .position(|s| s.id > at)
            .unwrap_or(snapshots.len()),
        None => 0,
    };
    // Sequence numbers count snapshots: one missing is expired too.
    let whole = snapshots[newer..]
        .iter()
        .zip(at.unwrap_or(0) + 1..)
        .all(|(s, seq)| s.id == seq && !s.expired);
    let replay = if at.is_some() && whole && current >= at {
        &snapshots[newer..]
    } else {
        *fi = FileIndex::new();
        &snapshots[..]
    };

    let pk_schema: Vec<pg2iceberg_core::ColumnSchema> = schema
        .columns
        .iter()
        .filter(|c| c.is_primary_key)
        .cloned()
        .collect();

    // Same compaction-aware skipping as the verifier — files superseded
    // by a Replace snapshot don't contribute to the FileIndex.
    let removed_paths: BTreeSet<&str> = replay
        .iter()
        .flat_map(|s| s.removed_paths.iter().map(String::as_str))
        .collect();

    for snap in replay {
        for df in &snap.delete_files {
            if !removed_paths.contains(df.path.as_str()) {
                for_each_pk_batch(blob_store, &df.path, &pk_schema, pk_cols, |pks| {
                    fi.remove_pks(&pks)
                })
                .await?;
            }
        }
        for df in &snap.data_files {
            if !removed_paths.contains(df.path.as_str()) {
                for_each_pk_batch(blob_store, &df.path, &pk_schema, pk_cols, |pks| {
                    fi.add_file(df.path.clone(), pks, df.partition_values.clone())
                })
                .await?;
            }
        }
    }
    // Files indexed before `at` that a later snapshot removed. Their rows
    // that live on moved to the files replacing them, above.
    for path in removed_paths {
        fi.remove_file(path);
    }
    Ok(current)
}

/// Calls `f` with the PKs of each batch of rows in the file at `path`.
async fn for_each_pk_batch(
    blob_store: &dyn pg2iceberg_stream::BlobStore,
    path: &str,
    pk_schema: &[pg2iceberg_core::ColumnSchema],
    pk_cols: &[pg2iceberg_core::ColumnName],
    mut f: impl FnMut(Vec<PkKey>),
) -> std::result::Result<(), crate::verify::VerifyError> {
    use crate::verify::VerifyError;
    let bytes = blob_store.get(path).await.map_err(VerifyError::Blob)?;
    let batches =
        crate::reader::RowBatches::new(bytes, pk_schema, crate::compact::DECODE_BATCH_ROWS)
            .map_err(VerifyError::Decode)?;
    for batch in batches {
        let rows = batch.map_err(VerifyError::Decode)?;
        f(rows.iter().map(|r| PkKey::from_row(r, pk_cols)).collect());
    }
    Ok(())
}

/// Avoid a circular module reference by re-exporting `DynCatalog` through a
/// private module. `verify::DynCatalog` is the canonical name.
mod pg2iceberg_iceberg_dyn {
    pub use crate::verify::DynCatalog;
}

#[cfg(test)]
mod tests {
    use super::*;
    use pg2iceberg_core::{ColumnName, PgValue, Row};

    fn k(id: &str) -> PkKey {
        let row: Row = [(ColumnName("id".into()), PgValue::Text(id.into()))].into();
        PkKey::from_row(&row, &[ColumnName("id".into())])
    }

    fn ks(ids: &[&str]) -> Vec<PkKey> {
        ids.iter().map(|id| k(id)).collect()
    }

    #[test]
    fn add_then_lookup() {
        let mut fi = FileIndex::new();
        fi.add_file("p0".into(), ks(&["k1", "k2"]), Vec::new());
        assert_eq!(fi.lookup(&k("k1")), Some("p0"));
        assert_eq!(fi.lookup(&k("k2")), Some("p0"));
        assert_eq!(fi.lookup(&k("missing")), None);
        assert!(fi.contains_pk(&k("k1")));
        assert!(!fi.contains_pk(&k("missing")));
    }

    #[test]
    fn remove_pks_clears_mapping_and_drops_empty_files() {
        let mut fi = FileIndex::new();
        fi.add_file("p0".into(), ks(&["k1", "k2"]), Vec::new());
        fi.remove_pks(&ks(&["k1"]));
        assert_eq!(fi.lookup(&k("k1")), None);
        assert_eq!(fi.lookup(&k("k2")), Some("p0"));
        assert_eq!(fi.live_pk_count(), 1);

        fi.remove_pks(&ks(&["k2"]));
        assert!(fi.live_files().is_empty());
        assert_eq!(fi.live_pk_count(), 0);
    }

    #[test]
    fn add_file_with_overlapping_pk_remaps_to_new_file() {
        // Re-insert flow: a PK lives in p0, then a new file p1 covers it.
        let mut fi = FileIndex::new();
        fi.add_file("p0".into(), ks(&["k1", "k2"]), Vec::new());
        fi.add_file("p1".into(), ks(&["k1"]), Vec::new());
        assert_eq!(fi.lookup(&k("k1")), Some("p1"));
        assert_eq!(
            fi.live_rows_per_file(),
            BTreeMap::from([("p0", 1), ("p1", 1)])
        );
        // p0's last live PK moves too: p0 leaves the index.
        fi.add_file("p2".into(), ks(&["k2"]), Vec::new());
        assert_eq!(fi.live_files(), vec!["p1", "p2"]);
    }

    #[test]
    fn affected_files_collects_distinct_paths() {
        let mut fi = FileIndex::new();
        fi.add_file("p0".into(), ks(&["k1", "k2"]), Vec::new());
        fi.add_file("p1".into(), ks(&["k3"]), Vec::new());
        let s = fi.affected_files(&ks(&["k1", "k3", "missing"]));
        let v: Vec<&String> = s.iter().collect();
        assert_eq!(v, vec![&"p0".to_string(), &"p1".to_string()]);
    }

    #[test]
    fn partition_values_for_pk_returns_files_partition_tuple() {
        let mut fi = FileIndex::new();
        fi.add_file(
            "p0".into(),
            ks(&["k1"]),
            vec![PartitionLiteral::String("us".into())],
        );
        fi.add_file(
            "p1".into(),
            ks(&["k2"]),
            vec![PartitionLiteral::String("eu".into())],
        );
        assert_eq!(
            fi.partition_values_for_pk(&k("k1")),
            Some(&[PartitionLiteral::String("us".into())][..])
        );
        assert_eq!(
            fi.partition_values_for_pk(&k("k2")),
            Some(&[PartitionLiteral::String("eu".into())][..])
        );
        // Missing PK → None.
        assert_eq!(fi.partition_values_for_pk(&k("missing")), None);
    }

    #[test]
    fn partition_values_for_unpartitioned_file_returns_none() {
        // Unpartitioned files store no partition_values; lookup returns
        // None even though the PK is indexed. Callers should only consult
        // this for partitioned schemas.
        let mut fi = FileIndex::new();
        fi.add_file("p0".into(), ks(&["k1"]), Vec::new());
        assert_eq!(fi.partition_values_for_pk(&k("k1")), None);
        assert!(fi.contains_pk(&k("k1")));
    }

    #[test]
    fn remove_file_after_its_pks_moved_needs_no_scan_and_forgets_it() {
        let mut fi = FileIndex::new();
        fi.add_file("in".into(), ks(&["k1", "k2"]), Vec::new());
        fi.add_file("out".into(), ks(&["k1", "k2"]), Vec::new());
        fi.remove_file("in");
        assert_eq!(fi.live_files(), vec!["out"]);
        assert_eq!(fi.live_pk_count(), 2);
    }

    #[test]
    fn remove_file_drops_pks_still_pointing_at_it() {
        let mut fi = FileIndex::new();
        fi.add_file("a".into(), ks(&["k1", "k2"]), Vec::new());
        fi.add_file("b".into(), ks(&["k3"]), Vec::new());
        fi.remove_file("a");
        assert_eq!(fi.live_pk_count(), 1);
        assert_eq!(fi.lookup(&k("k3")), Some("b"));
        assert_eq!(fi.live_files(), vec!["b"]);
    }

    #[test]
    fn file_slots_are_reused() {
        let mut fi = FileIndex::new();
        for i in 0..100 {
            fi.add_file(format!("p{i}"), ks(&["k"]), Vec::new());
        }
        assert_eq!(fi.live_files(), vec!["p99"]);
        assert!(
            fi.files.len() <= 2,
            "{} slots for one live file",
            fi.files.len()
        );
    }

    #[test]
    fn equality_ignores_file_numbering() {
        let mut a = FileIndex::new();
        a.add_file("p0".into(), ks(&["k1"]), Vec::new());
        a.add_file("p1".into(), ks(&["k2"]), Vec::new());
        let mut b = FileIndex::new();
        b.add_file("p1".into(), ks(&["k2"]), Vec::new());
        b.add_file("p0".into(), ks(&["k1"]), Vec::new());
        assert_eq!(a, b);
        b.add_file("p1".into(), ks(&["k1"]), Vec::new());
        assert_ne!(a, b);
    }
}
