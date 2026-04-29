//! Deterministic fault injection for DST.
//!
//! The pipeline's durability invariants are *structurally* enforced —
//! `flushed_lsn` only advances after a coord-commit receipt. This module
//! lets us prove the invariants survive *empirical* failure too: the
//! receipt-issuing path can fail at the blob layer (PUT), at the coord
//! layer (claim_offsets), or at the catalog layer (commit_snapshot), in
//! any combination, at any call index.
//!
//! ## Shape
//!
//! [`FaultPlan`] is a shared, deterministic budget: each operation
//! consults its per-op call counter and a set of `(op_key, call_index)`
//! pairs that should fail. Tests script which calls fail; the harness
//! re-runs the workload and checks that invariants still hold.
//!
//! [`FaultyBlobStore`] wraps any [`BlobStore`] and translates a fault
//! decision to `StreamError::Io`. Same pattern for
//! [`FaultyCoordinator`] (→ `CoordError::Pg`) and [`FaultyCatalog`]
//! (→ `IcebergError::Other`).
//!
//! ## What this catches
//!
//! - "Pipeline crashes between blob PUT and coord commit" — orphan blob
//!   under the staging prefix; `log_index` doesn't reference it; replay
//!   re-PUTs at a new path; orphan cleanup eventually reaps the first.
//!   Invariant: every `log_index.s3_path` is reachable; PG ↔ Iceberg
//!   match at quiescence.
//! - "Materializer crashes mid-`commit_snapshot`" — partial Iceberg
//!   state. The materializer rebuilds its FileIndex from catalog
//!   history on next start; the cursor isn't advanced until commit
//!   succeeds; replay is idempotent.
//! - "Snapshot phase crashes mid-chunk" — surfaces the
//!   non-resumable-snapshot gap when the binary uses the free
//!   `run_snapshot` function instead of `Snapshotter::run_chunks`.
//!
//! ## Determinism
//!
//! Counters live behind a `Mutex` and increment monotonically per op
//! key. Given the same workload + the same fault plan, the same call
//! indices fail every run. Tests should `set_failures` *before* the
//! call counter ticks past the chosen index.

use crate::blob::MemoryBlobStore;
use crate::catalog::MemoryCatalog;
use crate::coord::MemoryCoordinator;
use async_trait::async_trait;
use bytes::Bytes;
use pg2iceberg_coord::{
    CommitBatch, CoordCommitReceipt, CoordError, Coordinator, LogEntry,
    Result as CoordResult,
};
use pg2iceberg_coord::TableSnapshotState;
use pg2iceberg_core::{Lsn, TableIdent, WorkerId};
use pg2iceberg_iceberg::{
    Catalog, IcebergError, PreparedCommit, PreparedCompaction, Result as IcebergResult,
    SchemaChange, Snapshot, TableMetadata,
};
use pg2iceberg_stream::{BlobInfo, BlobStore, Result as BlobResult, StreamError};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

// ── operation keys ──────────────────────────────────────────────────
//
// Stable string identifiers so callers can script `plan.fail("blob.put",
// [3])` to make the *fourth* PUT fail. Keep these stable across the
// codebase; tests pin them.

pub mod ops {
    // Blob store.
    pub const BLOB_PUT: &str = "blob.put";
    pub const BLOB_GET: &str = "blob.get";
    pub const BLOB_LIST: &str = "blob.list";
    pub const BLOB_DELETE: &str = "blob.delete";

    // Coordinator.
    pub const COORD_CLAIM: &str = "coord.claim_offsets";
    pub const COORD_READ_LOG: &str = "coord.read_log";
    pub const COORD_TRUNCATE: &str = "coord.truncate_log";
    pub const COORD_SET_CURSOR: &str = "coord.set_cursor";
    /// Fault key for `Coordinator::set_snapshot_progress`. The
    /// snapshot phase calls this after every chunk; injecting here
    /// drives the resumability fault DSTs (mid-chunk crash → resume
    /// at next PK).
    pub const COORD_SAVE_CP: &str = "coord.set_snapshot_progress";

    // Catalog.
    pub const CAT_LOAD_TABLE: &str = "cat.load_table";
    pub const CAT_COMMIT_SNAPSHOT: &str = "cat.commit_snapshot";
    pub const CAT_COMMIT_COMPACTION: &str = "cat.commit_compaction";
    pub const CAT_SNAPSHOTS: &str = "cat.snapshots";
}

#[derive(Default, Debug)]
struct PlanInner {
    counters: BTreeMap<&'static str, u64>,
    failures: BTreeSet<(&'static str, u64)>,
    /// Cumulative count of injected faults — for assertions like
    /// "the workload tripped at least one fault."
    injected: u64,
}

/// Deterministic, shared fault budget. Construct one, give clones to
/// [`FaultyBlobStore`] / [`FaultyCoordinator`] / [`FaultyCatalog`], then
/// script which calls fail with [`Self::fail`].
#[derive(Clone, Default)]
pub struct FaultPlan {
    inner: Arc<Mutex<PlanInner>>,
}

impl FaultPlan {
    pub fn new() -> Self {
        Self::default()
    }

    /// Mark `op`'s `Nth, Mth, ...` calls (0-indexed) as failures.
    pub fn fail(&self, op: &'static str, indices: impl IntoIterator<Item = u64>) {
        let mut g = self.inner.lock().unwrap();
        for i in indices {
            g.failures.insert((op, i));
        }
    }

    /// Convenience: fail every call to `op` from now on. Useful for
    /// "all later writes fail" experiments.
    pub fn fail_all(&self, op: &'static str) {
        let mut g = self.inner.lock().unwrap();
        // Range up to a soft ceiling — the harness's workload won't
        // exceed this, but a real prod call would never reach it.
        for i in 0..1024 {
            g.failures.insert((op, i));
        }
    }

    /// Clear every scripted failure (call counters keep advancing).
    pub fn clear_failures(&self) {
        self.inner.lock().unwrap().failures.clear();
    }

    /// Check + advance: returns `true` iff this call should fail. The
    /// counter increments unconditionally so subsequent failures stay
    /// at deterministic indices.
    fn tick(&self, op: &'static str) -> bool {
        let mut g = self.inner.lock().unwrap();
        let n = g.counters.entry(op).or_insert(0);
        let idx = *n;
        *n += 1;
        if g.failures.contains(&(op, idx)) {
            g.injected += 1;
            true
        } else {
            false
        }
    }

    /// Total calls observed for `op` (whether failed or succeeded).
    pub fn counter(&self, op: &'static str) -> u64 {
        self.inner
            .lock()
            .unwrap()
            .counters
            .get(op)
            .copied()
            .unwrap_or(0)
    }

    /// Total faults actually injected so far.
    pub fn injected_count(&self) -> u64 {
        self.inner.lock().unwrap().injected
    }
}

// ── BlobStore wrapper ──────────────────────────────────────────────

pub struct FaultyBlobStore {
    inner: Arc<MemoryBlobStore>,
    plan: FaultPlan,
}

impl FaultyBlobStore {
    pub fn new(inner: Arc<MemoryBlobStore>, plan: FaultPlan) -> Self {
        Self { inner, plan }
    }

    pub fn inner(&self) -> &Arc<MemoryBlobStore> {
        &self.inner
    }
}

#[async_trait]
impl BlobStore for FaultyBlobStore {
    async fn put(&self, path: &str, bytes: Bytes) -> BlobResult<()> {
        if self.plan.tick(ops::BLOB_PUT) {
            return Err(StreamError::Io(format!("injected fault: blob.put {path:?}")));
        }
        self.inner.put(path, bytes).await
    }

    async fn get(&self, path: &str) -> BlobResult<Bytes> {
        if self.plan.tick(ops::BLOB_GET) {
            return Err(StreamError::Io(format!("injected fault: blob.get {path:?}")));
        }
        self.inner.get(path).await
    }

    async fn list(&self, prefix: &str) -> BlobResult<Vec<BlobInfo>> {
        if self.plan.tick(ops::BLOB_LIST) {
            return Err(StreamError::Io(format!(
                "injected fault: blob.list {prefix:?}"
            )));
        }
        self.inner.list(prefix).await
    }

    async fn delete(&self, path: &str) -> BlobResult<()> {
        if self.plan.tick(ops::BLOB_DELETE) {
            return Err(StreamError::Io(format!(
                "injected fault: blob.delete {path:?}"
            )));
        }
        self.inner.delete(path).await
    }
}

// ── Coordinator wrapper ────────────────────────────────────────────

pub struct FaultyCoordinator {
    inner: Arc<MemoryCoordinator>,
    plan: FaultPlan,
}

impl FaultyCoordinator {
    pub fn new(inner: Arc<MemoryCoordinator>, plan: FaultPlan) -> Self {
        Self { inner, plan }
    }

    pub fn inner(&self) -> &Arc<MemoryCoordinator> {
        &self.inner
    }
}

#[async_trait]
impl Coordinator for FaultyCoordinator {
    async fn claim_offsets(&self, batch: &CommitBatch) -> CoordResult<CoordCommitReceipt> {
        if self.plan.tick(ops::COORD_CLAIM) {
            return Err(CoordError::Pg("injected fault: claim_offsets".into()));
        }
        self.inner.claim_offsets(batch).await
    }

    async fn read_log(
        &self,
        table: &TableIdent,
        after_offset: u64,
        limit: usize,
    ) -> CoordResult<Vec<LogEntry>> {
        if self.plan.tick(ops::COORD_READ_LOG) {
            return Err(CoordError::Pg("injected fault: read_log".into()));
        }
        self.inner.read_log(table, after_offset, limit).await
    }

    async fn truncate_log(
        &self,
        table: &TableIdent,
        before_offset: u64,
    ) -> CoordResult<Vec<String>> {
        if self.plan.tick(ops::COORD_TRUNCATE) {
            return Err(CoordError::Pg("injected fault: truncate_log".into()));
        }
        self.inner.truncate_log(table, before_offset).await
    }

    async fn ensure_cursor(&self, group: &str, table: &TableIdent) -> CoordResult<()> {
        self.inner.ensure_cursor(group, table).await
    }

    async fn get_cursor(&self, group: &str, table: &TableIdent) -> CoordResult<Option<i64>> {
        self.inner.get_cursor(group, table).await
    }

    async fn set_cursor(
        &self,
        group: &str,
        table: &TableIdent,
        to_offset: i64,
    ) -> CoordResult<()> {
        if self.plan.tick(ops::COORD_SET_CURSOR) {
            return Err(CoordError::Pg("injected fault: set_cursor".into()));
        }
        self.inner.set_cursor(group, table, to_offset).await
    }

    async fn register_consumer(
        &self,
        group: &str,
        worker: &WorkerId,
        ttl: Duration,
    ) -> CoordResult<()> {
        self.inner.register_consumer(group, worker, ttl).await
    }

    async fn unregister_consumer(&self, group: &str, worker: &WorkerId) -> CoordResult<()> {
        self.inner.unregister_consumer(group, worker).await
    }

    async fn active_consumers(&self, group: &str) -> CoordResult<Vec<WorkerId>> {
        self.inner.active_consumers(group).await
    }

    async fn try_lock(
        &self,
        table: &TableIdent,
        worker: &WorkerId,
        ttl: Duration,
    ) -> CoordResult<bool> {
        self.inner.try_lock(table, worker, ttl).await
    }

    async fn renew_lock(
        &self,
        table: &TableIdent,
        worker: &WorkerId,
        ttl: Duration,
    ) -> CoordResult<bool> {
        self.inner.renew_lock(table, worker, ttl).await
    }

    async fn release_lock(&self, table: &TableIdent, worker: &WorkerId) -> CoordResult<()> {
        self.inner.release_lock(table, worker).await
    }

    async fn pipeline_system_identifier(&self) -> CoordResult<u64> {
        self.inner.pipeline_system_identifier().await
    }

    async fn set_pipeline_system_identifier(&self, sysid: u64) -> CoordResult<()> {
        self.inner.set_pipeline_system_identifier(sysid).await
    }

    async fn flushed_lsn(&self) -> CoordResult<Lsn> {
        self.inner.flushed_lsn().await
    }

    async fn set_flushed_lsn(&self, lsn: Lsn) -> CoordResult<()> {
        self.inner.set_flushed_lsn(lsn).await
    }

    async fn table_state(&self, ident: &TableIdent) -> CoordResult<Option<TableSnapshotState>> {
        self.inner.table_state(ident).await
    }

    async fn mark_table_snapshot_complete(
        &self,
        ident: &TableIdent,
        pg_oid: u32,
        snapshot_lsn: Lsn,
    ) -> CoordResult<()> {
        self.inner
            .mark_table_snapshot_complete(ident, pg_oid, snapshot_lsn)
            .await
    }

    async fn snapshot_progress(&self, ident: &TableIdent) -> CoordResult<Option<String>> {
        self.inner.snapshot_progress(ident).await
    }

    async fn set_snapshot_progress(
        &self,
        ident: &TableIdent,
        last_pk_key: &str,
    ) -> CoordResult<()> {
        if self.plan.tick(ops::COORD_SAVE_CP) {
            return Err(CoordError::Pg("injected fault: set_snapshot_progress".into()));
        }
        self.inner.set_snapshot_progress(ident, last_pk_key).await
    }

    async fn clear_snapshot_progress(&self, ident: &TableIdent) -> CoordResult<()> {
        self.inner.clear_snapshot_progress(ident).await
    }

    async fn query_watermark(
        &self,
        ident: &TableIdent,
    ) -> CoordResult<Option<pg2iceberg_core::PgValue>> {
        self.inner.query_watermark(ident).await
    }

    async fn set_query_watermark(
        &self,
        ident: &TableIdent,
        watermark: &pg2iceberg_core::PgValue,
    ) -> CoordResult<()> {
        self.inner.set_query_watermark(ident, watermark).await
    }
}

// ── Catalog wrapper ────────────────────────────────────────────────

pub struct FaultyCatalog {
    inner: Arc<MemoryCatalog>,
    plan: FaultPlan,
}

impl FaultyCatalog {
    pub fn new(inner: Arc<MemoryCatalog>, plan: FaultPlan) -> Self {
        Self { inner, plan }
    }

    pub fn inner(&self) -> &Arc<MemoryCatalog> {
        &self.inner
    }
}

#[async_trait]
impl Catalog for FaultyCatalog {
    async fn ensure_namespace(&self, ns: &pg2iceberg_core::Namespace) -> IcebergResult<()> {
        self.inner.ensure_namespace(ns).await
    }

    async fn load_table(&self, ident: &TableIdent) -> IcebergResult<Option<TableMetadata>> {
        if self.plan.tick(ops::CAT_LOAD_TABLE) {
            return Err(IcebergError::Other("injected fault: load_table".into()));
        }
        self.inner.load_table(ident).await
    }

    async fn create_table(
        &self,
        schema: &pg2iceberg_core::TableSchema,
    ) -> IcebergResult<TableMetadata> {
        self.inner.create_table(schema).await
    }

    async fn commit_snapshot(&self, prepared: PreparedCommit) -> IcebergResult<TableMetadata> {
        if self.plan.tick(ops::CAT_COMMIT_SNAPSHOT) {
            return Err(IcebergError::Other(
                "injected fault: commit_snapshot".into(),
            ));
        }
        self.inner.commit_snapshot(prepared).await
    }

    async fn commit_compaction(
        &self,
        prepared: PreparedCompaction,
    ) -> IcebergResult<TableMetadata> {
        if self.plan.tick(ops::CAT_COMMIT_COMPACTION) {
            return Err(IcebergError::Other(
                "injected fault: commit_compaction".into(),
            ));
        }
        self.inner.commit_compaction(prepared).await
    }

    async fn evolve_schema(
        &self,
        ident: &TableIdent,
        changes: Vec<SchemaChange>,
    ) -> IcebergResult<TableMetadata> {
        self.inner.evolve_schema(ident, changes).await
    }

    async fn expire_snapshots(
        &self,
        ident: &TableIdent,
        retention_ms: i64,
    ) -> IcebergResult<usize> {
        self.inner.expire_snapshots(ident, retention_ms).await
    }

    async fn snapshots(&self, ident: &TableIdent) -> IcebergResult<Vec<Snapshot>> {
        if self.plan.tick(ops::CAT_SNAPSHOTS) {
            return Err(IcebergError::Other("injected fault: snapshots".into()));
        }
        self.inner.snapshots(ident).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pollster::block_on;

    #[test]
    fn fault_plan_ticks_deterministically() {
        let p = FaultPlan::new();
        p.fail("op", [1, 3]);
        assert!(!p.tick("op")); // 0
        assert!(p.tick("op")); // 1 — fails
        assert!(!p.tick("op")); // 2
        assert!(p.tick("op")); // 3 — fails
        assert!(!p.tick("op")); // 4
        assert_eq!(p.counter("op"), 5);
        assert_eq!(p.injected_count(), 2);
    }

    #[test]
    fn faulty_blob_store_fails_only_at_scripted_indices() {
        let plan = FaultPlan::new();
        plan.fail(ops::BLOB_PUT, [1]);
        let store = FaultyBlobStore::new(Arc::new(MemoryBlobStore::new()), plan.clone());
        block_on(store.put("a", Bytes::from_static(b"x"))).unwrap(); // 0 — ok
        let err = block_on(store.put("b", Bytes::from_static(b"y"))).unwrap_err();
        assert!(matches!(err, StreamError::Io(_)));
        block_on(store.put("c", Bytes::from_static(b"z"))).unwrap(); // 2 — ok
        assert_eq!(plan.injected_count(), 1);
        // The failed PUT did not reach the inner store.
        let paths = store.inner().paths();
        assert!(paths.contains(&"a".to_string()));
        assert!(!paths.contains(&"b".to_string()));
        assert!(paths.contains(&"c".to_string()));
    }
}
