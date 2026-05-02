//! Object-storage trait used by the staging layer.
//!
//! Production wraps `object_store` (S3); sim is in-memory. Kept narrower than
//! `object_store` itself because the pipeline only ever needs put/get of
//! whole-blob bytes — no streaming, no atomic-rename gymnastics. `list` and
//! `delete` are added for orphan-file cleanup (Phase F maintenance op);
//! they aren't on the hot replication path.

use crate::Result;
use async_trait::async_trait;
use bytes::Bytes;
use pg2iceberg_core::TableIdent;

/// Metadata about a stored blob, returned by [`BlobStore::list`]. We
/// normalize `last_modified` to milliseconds since epoch so the orphan
/// cleanup path can compare against grace periods uniformly across
/// backends (S3 reports a `DateTime`; the in-memory sim reports
/// monotonic ms).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlobInfo {
    pub path: String,
    pub size: u64,
    pub last_modified_ms: i64,
}

#[async_trait]
pub trait BlobStore: Send + Sync {
    async fn put(&self, path: &str, bytes: Bytes) -> Result<()>;
    async fn get(&self, path: &str) -> Result<Bytes>;
    /// Enumerate every blob whose path starts with `prefix`. Returns
    /// info for each (path, size, last-modified-ms). Used by the
    /// orphan-file cleanup maintenance op.
    async fn list(&self, prefix: &str) -> Result<Vec<BlobInfo>>;
    /// Delete a single blob. Returns `Ok(())` even if the blob doesn't
    /// exist — orphan cleanup races with concurrent compaction commits
    /// can produce phantom deletes; treat them as no-ops.
    async fn delete(&self, path: &str) -> Result<()>;
    /// Notify the blob store that a new table has been registered with
    /// the catalog. Static-creds backends ignore this. Vended-creds
    /// routers use it to load per-table credentials and add a new
    /// per-table object store on the fly — necessary so newly-created
    /// tables (e.g. `_pg2iceberg.markers`, or YAML tables added in a
    /// redeploy that weren't in the catalog yet at startup) are
    /// routable for subsequent put/get calls.
    ///
    /// Idempotent. Implementations should treat repeated calls for the
    /// same `ident` as a refresh (or no-op if creds are still fresh).
    async fn register_table(&self, ident: &TableIdent) -> Result<()> {
        let _ = ident;
        Ok(())
    }
}
