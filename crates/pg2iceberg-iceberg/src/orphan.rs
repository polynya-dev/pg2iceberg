//! Orphan-file cleanup: list blobs under a prefix, diff against
//! everything referenced by the table's snapshot history, delete the
//! ones older than a grace period.
//!
//! Mirrors `iceberg/maintain.go::cleanOrphanFiles` but scoped to data /
//! delete files under a configured materializer prefix. Metadata-side
//! files (manifest lists, manifests, metadata.json) live elsewhere in
//! iceberg's table location and aren't our responsibility — iceberg-rust
//! manages them.
//!
//! ## Why a separate prefix?
//!
//! The materializer's `MaterializerNamer` writes data files under a
//! configurable base (e.g. `materialized/<table>/`). Iceberg's table
//! metadata is rooted at the catalog's `location` field which can be
//! different (e.g. `s3://warehouse/public/orders/`). We only diff under
//! the materializer's base — operators are responsible for setting it
//! to a path they control and that nothing else writes to.

use crate::Snapshot;
use pg2iceberg_core::TableIdent;
use pg2iceberg_stream::{BlobInfo, BlobStore};
use std::collections::BTreeSet;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum CleanupError {
    #[error("catalog: {0}")]
    Catalog(#[from] crate::IcebergError),
    #[error("blob: {0}")]
    Blob(#[from] pg2iceberg_stream::StreamError),
}

pub type Result<T> = std::result::Result<T, CleanupError>;

/// Result of one cleanup pass.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct CleanupOutcome {
    /// Number of blobs that were eligible (in the prefix, not referenced,
    /// older than grace) and got deleted.
    pub deleted: usize,
    /// Total bytes freed by the deletions.
    pub bytes_freed: u64,
    /// Blobs that matched everything except the grace period — kept for
    /// now, eligible on the next pass once they age out.
    pub grace_protected: usize,
}

/// Find and delete unreferenced blobs under `prefix`. A blob is an
/// orphan iff:
///
/// 1. Its path lives under `prefix`.
/// 2. No live snapshot references it (data file or delete file).
/// 3. Its `last_modified_ms` is older than `now_ms - grace_period_ms`.
///
/// `now_ms` comes from the caller — production passes wall-clock millis;
/// tests pass deterministic values. `grace_period_ms` of 0 means "delete
/// any unreferenced blob regardless of age."
pub async fn cleanup_orphans(
    catalog: &dyn crate::verify::DynCatalog,
    blob_store: &dyn BlobStore,
    ident: &TableIdent,
    prefix: &str,
    now_ms: i64,
    grace_period_ms: i64,
) -> Result<CleanupOutcome> {
    let snapshots = catalog
        .snapshots(ident)
        .await
        .map_err(crate::verify::VerifyError::from_dyn)
        .map_err(|e| CleanupError::Catalog(crate::IcebergError::Other(e.to_string())))?;
    let referenced = referenced_paths(&snapshots);

    let blobs = blob_store.list(prefix).await?;
    let cutoff = now_ms - grace_period_ms;

    let mut to_delete: Vec<BlobInfo> = Vec::new();
    let mut grace_protected: usize = 0;
    for blob in blobs {
        if referenced.contains(&blob.path) {
            continue;
        }
        if blob.last_modified_ms > cutoff {
            grace_protected += 1;
            continue;
        }
        to_delete.push(blob);
    }

    let mut bytes_freed: u64 = 0;
    for blob in &to_delete {
        bytes_freed += blob.size;
        blob_store.delete(&blob.path).await?;
    }
    Ok(CleanupOutcome {
        deleted: to_delete.len(),
        bytes_freed,
        grace_protected,
    })
}

/// Compute the set of file paths still referenced by *any* live
/// snapshot's data or delete files. A path is unreferenced iff it
/// appears in some snapshot's `removed_paths` (compaction-replaced) or
/// no snapshot ever wrote it.
///
/// We don't include manifest-list / manifest / metadata.json paths —
/// those live in iceberg's metadata directory under the table location,
/// not the materializer's data prefix, so they're outside the cleanup
/// scope (see module-level docs).
fn referenced_paths(snapshots: &[Snapshot]) -> BTreeSet<String> {
    let removed: BTreeSet<&str> = snapshots
        .iter()
        .flat_map(|s| s.removed_paths.iter().map(String::as_str))
        .collect();
    let mut out = BTreeSet::new();
    for snap in snapshots {
        for df in &snap.data_files {
            if removed.contains(df.path.as_str()) {
                continue;
            }
            out.insert(df.path.clone());
        }
        for df in &snap.delete_files {
            if removed.contains(df.path.as_str()) {
                continue;
            }
            out.insert(df.path.clone());
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::DataFile;
    use pg2iceberg_core::Namespace;

    fn snap(id: i64, data: Vec<&str>, deletes: Vec<&str>, removed: Vec<&str>) -> Snapshot {
        let to_df = |p: &&str| DataFile {
            path: (*p).into(),
            record_count: 1,
            byte_size: 1,
            equality_field_ids: vec![],
            partition_values: vec![],
        };
        Snapshot {
            id,
            data_files: data.iter().map(to_df).collect(),
            delete_files: deletes.iter().map(to_df).collect(),
            removed_paths: removed.into_iter().map(String::from).collect(),
            timestamp_ms: id * 1000,
        }
    }

    #[test]
    fn referenced_paths_excludes_removed() {
        let snaps = vec![
            snap(1, vec!["a", "b"], vec![], vec![]),
            snap(2, vec!["compact-0"], vec![], vec!["a"]),
        ];
        let r = referenced_paths(&snaps);
        assert!(r.contains("b"));
        assert!(r.contains("compact-0"));
        assert!(!r.contains("a"), "compaction-removed path is unreferenced");
    }

    #[test]
    fn referenced_paths_includes_delete_files() {
        let snaps = vec![snap(1, vec!["d1"], vec!["eq-1"], vec![])];
        let r = referenced_paths(&snaps);
        assert!(r.contains("d1"));
        assert!(r.contains("eq-1"));
    }

    #[test]
    fn referenced_paths_handles_empty_history() {
        let snaps: Vec<Snapshot> = Vec::new();
        let r = referenced_paths(&snaps);
        assert!(r.is_empty());
    }

    fn _unused_namespace() -> Namespace {
        Namespace(vec![])
    }
}
