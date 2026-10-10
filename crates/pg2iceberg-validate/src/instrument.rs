//! Request metrics and spans for the three stores pg2iceberg talks to: the
//! Iceberg catalog, the object store, and the coordinator.
//!
//! [`Instrumented`] wraps one and forwards every call unchanged, recording
//! how long it took and whether it failed, and running it in a `request`
//! span (exported as `<store>.<op>`, e.g. `catalog.commit_snapshots`) under
//! whatever unit of work made it. Every method is forwarded, including the
//! traits' provided ones, so the wrapped store's own overrides still run.
//!
//! Recording on every completed request is also what keeps a
//! [`Registry`](pg2iceberg_core::Registry)'s liveness signal fresh while a
//! long materializer cycle or snapshot is busy: a process whose requests
//! keep finishing is slow, not stuck.

use async_trait::async_trait;
use bytes::Bytes;
use pg2iceberg_coord::{
    CommitBatch, CoordCommitReceipt, Coordinator, LogEntry, MarkerInfo, TableSnapshotState,
};
use pg2iceberg_core::metrics::{labels, names, seconds_between};
use pg2iceberg_core::{Clock, Lsn, Metrics, Namespace, TableIdent, TableSchema, WorkerId};
use pg2iceberg_iceberg::{
    Catalog, IcebergError, LogRange, PreparedCommit, PreparedCompaction, SchemaChange, Snapshot,
    TableMetadata,
};
use pg2iceberg_stream::{BlobInfo, BlobStore};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Display;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;
use tracing::Instrument as _;

/// How one kind of store's requests are recorded.
struct Store {
    /// The `store` span field, and the exported span name's prefix.
    name: &'static str,
    duration: &'static str,
    errors: &'static str,
}

const CATALOG: Store = Store {
    name: "catalog",
    duration: names::CATALOG_REQUEST_DURATION,
    errors: names::CATALOG_REQUEST_ERRORS_TOTAL,
};

const OBJECT_STORE: Store = Store {
    name: "object_store",
    duration: names::BLOB_REQUEST_DURATION,
    errors: names::BLOB_REQUEST_ERRORS_TOTAL,
};

const COORDINATOR: Store = Store {
    name: "coordinator",
    duration: names::COORD_REQUEST_DURATION,
    errors: names::COORD_REQUEST_ERRORS_TOTAL,
};

/// A store whose requests are timed and counted (see the module docs).
pub struct Instrumented<T: ?Sized> {
    inner: Arc<T>,
    metrics: Arc<dyn Metrics>,
    clock: Arc<dyn Clock>,
}

impl<T: ?Sized> Instrumented<T> {
    pub fn new(inner: Arc<T>, metrics: Arc<dyn Metrics>, clock: Arc<dyn Clock>) -> Self {
        Self {
            inner,
            metrics,
            clock,
        }
    }

    /// The wrapped store.
    pub fn inner(&self) -> &Arc<T> {
        &self.inner
    }

    /// Run `request` in its span, recording its duration and, if it
    /// fails, an error — labelled with `error_kind`'s answer when it has
    /// one.
    async fn observe<R, E: Display>(
        &self,
        store: &Store,
        op: &'static str,
        error_kind: fn(&E) -> Option<&'static str>,
        request: impl Future<Output = Result<R, E>>,
    ) -> Result<R, E> {
        let span = tracing::info_span!(
            "request",
            otel.name = %format_args!("{}.{op}", store.name),
            otel.kind = "client",
            store = store.name,
            op,
            otel.status_description = tracing::field::Empty,
        );
        let start = self.clock.now();
        let out = request.instrument(span.clone()).await;
        let op_labels = labels([("op", op)]);
        let elapsed = seconds_between(start, self.clock.now());
        self.metrics.histogram(store.duration, &op_labels, elapsed);
        if let Err(e) = &out {
            pg2iceberg_logical::spans::record_error(&span, e);
            let mut error_labels = op_labels;
            if let Some(kind) = error_kind(e) {
                error_labels.insert("kind".into(), kind.into());
            }
            self.metrics.counter(store.errors, &error_labels, 1);
        }
        out
    }
}

// ── Iceberg catalog ──────────────────────────────────────────────────

fn catalog_error_kind(e: &IcebergError) -> Option<&'static str> {
    Some(match e {
        IcebergError::Conflict(_) => "conflict",
        IcebergError::NotFound(_) => "not_found",
        IcebergError::Other(_) => "other",
    })
}

impl<C: Catalog + ?Sized> Instrumented<C> {
    async fn catalog<R>(
        &self,
        op: &'static str,
        request: impl Future<Output = pg2iceberg_iceberg::Result<R>>,
    ) -> pg2iceberg_iceberg::Result<R> {
        self.observe(&CATALOG, op, catalog_error_kind, request)
            .await
    }
}

#[async_trait]
impl<C: Catalog + ?Sized> Catalog for Instrumented<C> {
    async fn ensure_namespace(&self, ns: &Namespace) -> pg2iceberg_iceberg::Result<()> {
        self.catalog("ensure_namespace", self.inner.ensure_namespace(ns))
            .await
    }

    async fn load_table(
        &self,
        ident: &TableIdent,
    ) -> pg2iceberg_iceberg::Result<Option<TableMetadata>> {
        self.catalog("load_table", self.inner.load_table(ident))
            .await
    }

    async fn create_table(
        &self,
        schema: &TableSchema,
    ) -> pg2iceberg_iceberg::Result<TableMetadata> {
        self.catalog("create_table", self.inner.create_table(schema))
            .await
    }

    async fn commit_snapshot(
        &self,
        prepared: PreparedCommit,
    ) -> pg2iceberg_iceberg::Result<TableMetadata> {
        self.catalog("commit_snapshot", self.inner.commit_snapshot(prepared))
            .await
    }

    async fn commit_snapshots(
        &self,
        steps: Vec<PreparedCommit>,
        log_range: Option<LogRange>,
        remove_properties: BTreeSet<String>,
    ) -> pg2iceberg_iceberg::Result<TableMetadata> {
        self.catalog(
            "commit_snapshots",
            self.inner
                .commit_snapshots(steps, log_range, remove_properties),
        )
        .await
    }

    async fn commit_compaction(
        &self,
        prepared: PreparedCompaction,
    ) -> pg2iceberg_iceberg::Result<TableMetadata> {
        self.catalog("commit_compaction", self.inner.commit_compaction(prepared))
            .await
    }

    async fn evolve_schema(
        &self,
        ident: &TableIdent,
        changes: Vec<SchemaChange>,
        set_properties: BTreeMap<String, String>,
    ) -> pg2iceberg_iceberg::Result<TableMetadata> {
        self.catalog(
            "evolve_schema",
            self.inner.evolve_schema(ident, changes, set_properties),
        )
        .await
    }

    async fn expire_snapshots(
        &self,
        ident: &TableIdent,
        retention_ms: i64,
    ) -> pg2iceberg_iceberg::Result<usize> {
        self.catalog(
            "expire_snapshots",
            self.inner.expire_snapshots(ident, retention_ms),
        )
        .await
    }

    async fn snapshots(&self, ident: &TableIdent) -> pg2iceberg_iceberg::Result<Vec<Snapshot>> {
        self.catalog("snapshots", self.inner.snapshots(ident)).await
    }
}

// ── Object store ─────────────────────────────────────────────────────

impl<B: BlobStore + ?Sized> Instrumented<B> {
    async fn blob<R>(
        &self,
        op: &'static str,
        request: impl Future<Output = pg2iceberg_stream::Result<R>>,
    ) -> pg2iceberg_stream::Result<R> {
        self.observe(&OBJECT_STORE, op, |_| None, request).await
    }

    fn bytes(&self, op: &str, n: usize) {
        self.metrics
            .counter(names::BLOB_BYTES_TOTAL, &labels([("op", op)]), n as u64);
    }
}

#[async_trait]
impl<B: BlobStore + ?Sized> BlobStore for Instrumented<B> {
    async fn put(&self, path: &str, bytes: Bytes) -> pg2iceberg_stream::Result<()> {
        let n = bytes.len();
        let out = self.blob("put", self.inner.put(path, bytes)).await;
        if out.is_ok() {
            self.bytes("put", n);
        }
        out
    }

    async fn get(&self, path: &str) -> pg2iceberg_stream::Result<Bytes> {
        let out = self.blob("get", self.inner.get(path)).await;
        if let Ok(bytes) = &out {
            self.bytes("get", bytes.len());
        }
        out
    }

    async fn list(&self, prefix: &str) -> pg2iceberg_stream::Result<Vec<BlobInfo>> {
        self.blob("list", self.inner.list(prefix)).await
    }

    async fn delete(&self, path: &str) -> pg2iceberg_stream::Result<()> {
        self.blob("delete", self.inner.delete(path)).await
    }

    async fn register_table(&self, ident: &TableIdent) -> pg2iceberg_stream::Result<()> {
        self.blob("register_table", self.inner.register_table(ident))
            .await
    }
}

// ── Coordinator ──────────────────────────────────────────────────────

impl<C: Coordinator + ?Sized> Instrumented<C> {
    async fn coord<R>(
        &self,
        op: &'static str,
        request: impl Future<Output = pg2iceberg_coord::Result<R>>,
    ) -> pg2iceberg_coord::Result<R> {
        self.observe(&COORDINATOR, op, |_| None, request).await
    }
}

#[async_trait]
impl<C: Coordinator + ?Sized> Coordinator for Instrumented<C> {
    async fn claim_offsets(
        &self,
        batch: &CommitBatch,
    ) -> pg2iceberg_coord::Result<CoordCommitReceipt> {
        self.coord("claim_offsets", self.inner.claim_offsets(batch))
            .await
    }

    async fn read_log(
        &self,
        table: &TableIdent,
        after_offset: u64,
        limit: usize,
    ) -> pg2iceberg_coord::Result<Vec<LogEntry>> {
        self.coord("read_log", self.inner.read_log(table, after_offset, limit))
            .await
    }

    async fn truncate_log(
        &self,
        table: &TableIdent,
        before_offset: u64,
    ) -> pg2iceberg_coord::Result<Vec<String>> {
        self.coord(
            "truncate_log",
            self.inner.truncate_log(table, before_offset),
        )
        .await
    }

    async fn ensure_cursor(&self, group: &str, table: &TableIdent) -> pg2iceberg_coord::Result<()> {
        self.coord("ensure_cursor", self.inner.ensure_cursor(group, table))
            .await
    }

    async fn get_cursor(
        &self,
        group: &str,
        table: &TableIdent,
    ) -> pg2iceberg_coord::Result<Option<i64>> {
        self.coord("get_cursor", self.inner.get_cursor(group, table))
            .await
    }

    async fn set_cursor(
        &self,
        group: &str,
        table: &TableIdent,
        to_offset: i64,
    ) -> pg2iceberg_coord::Result<()> {
        self.coord("set_cursor", self.inner.set_cursor(group, table, to_offset))
            .await
    }

    async fn register_consumer(
        &self,
        group: &str,
        worker: &WorkerId,
        ttl: Duration,
    ) -> pg2iceberg_coord::Result<()> {
        self.coord(
            "register_consumer",
            self.inner.register_consumer(group, worker, ttl),
        )
        .await
    }

    async fn unregister_consumer(
        &self,
        group: &str,
        worker: &WorkerId,
    ) -> pg2iceberg_coord::Result<()> {
        self.coord(
            "unregister_consumer",
            self.inner.unregister_consumer(group, worker),
        )
        .await
    }

    async fn active_consumers(&self, group: &str) -> pg2iceberg_coord::Result<Vec<WorkerId>> {
        self.coord("active_consumers", self.inner.active_consumers(group))
            .await
    }

    async fn try_lock(
        &self,
        table: &TableIdent,
        worker: &WorkerId,
        ttl: Duration,
    ) -> pg2iceberg_coord::Result<bool> {
        self.coord("try_lock", self.inner.try_lock(table, worker, ttl))
            .await
    }

    async fn renew_lock(
        &self,
        table: &TableIdent,
        worker: &WorkerId,
        ttl: Duration,
    ) -> pg2iceberg_coord::Result<bool> {
        self.coord("renew_lock", self.inner.renew_lock(table, worker, ttl))
            .await
    }

    async fn release_lock(
        &self,
        table: &TableIdent,
        worker: &WorkerId,
    ) -> pg2iceberg_coord::Result<()> {
        self.coord("release_lock", self.inner.release_lock(table, worker))
            .await
    }

    async fn pipeline_system_identifier(&self) -> pg2iceberg_coord::Result<u64> {
        self.coord(
            "pipeline_system_identifier",
            self.inner.pipeline_system_identifier(),
        )
        .await
    }

    async fn set_pipeline_system_identifier(&self, sysid: u64) -> pg2iceberg_coord::Result<()> {
        self.coord(
            "set_pipeline_system_identifier",
            self.inner.set_pipeline_system_identifier(sysid),
        )
        .await
    }

    async fn flushed_lsn(&self) -> pg2iceberg_coord::Result<Lsn> {
        self.coord("flushed_lsn", self.inner.flushed_lsn()).await
    }

    async fn set_flushed_lsn(&self, lsn: Lsn) -> pg2iceberg_coord::Result<()> {
        self.coord("set_flushed_lsn", self.inner.set_flushed_lsn(lsn))
            .await
    }

    async fn replicated_lsn(&self) -> pg2iceberg_coord::Result<Lsn> {
        self.coord("replicated_lsn", self.inner.replicated_lsn())
            .await
    }

    async fn table_state(
        &self,
        ident: &TableIdent,
    ) -> pg2iceberg_coord::Result<Option<TableSnapshotState>> {
        self.coord("table_state", self.inner.table_state(ident))
            .await
    }

    async fn mark_table_snapshot_complete(
        &self,
        ident: &TableIdent,
        pg_oid: u32,
        snapshot_lsn: Lsn,
    ) -> pg2iceberg_coord::Result<()> {
        self.coord(
            "mark_table_snapshot_complete",
            self.inner
                .mark_table_snapshot_complete(ident, pg_oid, snapshot_lsn),
        )
        .await
    }

    async fn bump_table_epoch(&self, table: &TableIdent) -> pg2iceberg_coord::Result<i64> {
        self.coord("bump_table_epoch", self.inner.bump_table_epoch(table))
            .await
    }

    async fn table_epochs(
        &self,
        tables: &[TableIdent],
    ) -> pg2iceberg_coord::Result<BTreeMap<TableIdent, i64>> {
        self.coord("table_epochs", self.inner.table_epochs(tables))
            .await
    }

    async fn snapshot_progress(
        &self,
        ident: &TableIdent,
    ) -> pg2iceberg_coord::Result<Option<String>> {
        self.coord("snapshot_progress", self.inner.snapshot_progress(ident))
            .await
    }

    async fn set_snapshot_progress(
        &self,
        ident: &TableIdent,
        last_pk_key: &str,
    ) -> pg2iceberg_coord::Result<()> {
        self.coord(
            "set_snapshot_progress",
            self.inner.set_snapshot_progress(ident, last_pk_key),
        )
        .await
    }

    async fn clear_snapshot_progress(&self, ident: &TableIdent) -> pg2iceberg_coord::Result<()> {
        self.coord(
            "clear_snapshot_progress",
            self.inner.clear_snapshot_progress(ident),
        )
        .await
    }

    async fn pending_markers_for_table(
        &self,
        table: &TableIdent,
        cursor: i64,
    ) -> pg2iceberg_coord::Result<Vec<MarkerInfo>> {
        self.coord(
            "pending_markers_for_table",
            self.inner.pending_markers_for_table(table, cursor),
        )
        .await
    }

    async fn record_marker_emitted(
        &self,
        uuid: &str,
        table: &TableIdent,
    ) -> pg2iceberg_coord::Result<()> {
        self.coord(
            "record_marker_emitted",
            self.inner.record_marker_emitted(uuid, table),
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pg2iceberg_core::metrics::Registry;
    use pg2iceberg_sim::blob::MemoryBlobStore;
    use pg2iceberg_sim::catalog::MemoryCatalog;
    use pg2iceberg_sim::clock::TestClock;
    use pollster::block_on;

    fn instrumented<T: ?Sized>(inner: Arc<T>) -> (Instrumented<T>, Arc<Registry>) {
        let clock = Arc::new(TestClock::at(0));
        let registry = Arc::new(Registry::new(clock.clone()));
        (Instrumented::new(inner, registry.clone(), clock), registry)
    }

    #[test]
    fn blob_requests_are_timed_and_their_bytes_counted() {
        let (blob, registry) = instrumented(Arc::new(MemoryBlobStore::new()));
        block_on(blob.put("s3://b/k", Bytes::from_static(b"hello"))).unwrap();
        assert_eq!(block_on(blob.get("s3://b/k")).unwrap().len(), 5);
        assert!(block_on(blob.get("s3://b/missing")).is_err());

        let put = labels([("op", "put")]);
        let get = labels([("op", "get")]);
        assert_eq!(
            registry.histogram_count(names::BLOB_REQUEST_DURATION, &put),
            1
        );
        assert_eq!(
            registry.histogram_count(names::BLOB_REQUEST_DURATION, &get),
            2
        );
        assert_eq!(registry.counter_value(names::BLOB_BYTES_TOTAL, &put), 5);
        assert_eq!(registry.counter_value(names::BLOB_BYTES_TOTAL, &get), 5);
        assert_eq!(
            registry.counter_value(names::BLOB_REQUEST_ERRORS_TOTAL, &get),
            1
        );
        assert_eq!(
            registry.counter_value(names::BLOB_REQUEST_ERRORS_TOTAL, &put),
            0
        );
    }

    #[test]
    fn catalog_errors_are_counted_by_kind() {
        let (catalog, registry) = instrumented(Arc::new(MemoryCatalog::new()));
        let missing = TableIdent {
            namespace: Namespace(vec!["public".into()]),
            name: "missing".into(),
        };
        assert!(block_on(catalog.load_table(&missing)).unwrap().is_none());
        let err = block_on(catalog.evolve_schema(&missing, Vec::new(), BTreeMap::new()))
            .expect_err("no such table");
        let kind = catalog_error_kind(&err).unwrap();

        let load = labels([("op", "load_table")]);
        assert_eq!(
            registry.histogram_count(names::CATALOG_REQUEST_DURATION, &load),
            1
        );
        let failed = labels([("op", "evolve_schema"), ("kind", kind)]);
        assert_eq!(
            registry.counter_value(names::CATALOG_REQUEST_ERRORS_TOTAL, &failed),
            1
        );
    }
}
