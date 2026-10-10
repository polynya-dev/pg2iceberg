//! What a pg2iceberg process has read of its tables' Iceberg metadata,
//! kept until a pg2iceberg process writes them.
//!
//! pg2iceberg is the only writer of its tables — `run`, the other workers
//! in distributed mode, `compact` and `maintain` jobs — so what one process
//! read of a table stays true until one of them writes it. Each catalog
//! call is a round trip, often to a catalog and object store across the
//! internet; a materializer makes several per table per cycle.
//!
//! Unless the catalog maintains the tables too (`Maintenance::Managed`):
//! its commits tell no pg2iceberg process, so nothing read is kept past
//! the cycle that read it ([`CachingCatalog::set_other_writers`]).

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use pg2iceberg_coord::Coordinator;
use pg2iceberg_core::{Clock, Namespace, TableIdent, TableSchema, Timestamp};
use pg2iceberg_iceberg::{
    Catalog, LogRange, PreparedCommit, PreparedCompaction, Result, SchemaChange, Snapshot,
    TableMetadata,
};

/// A [`Catalog`] that caches each table's metadata and snapshot history.
///
/// Every write through it bumps the table's epoch in the coordinator.
/// [`Self::sync`], at the start of each materializer cycle, drops the
/// tables whose epoch moved past what this process knew: another process
/// wrote them. Its own writes keep the cache current instead, since a
/// commit answers with the table's new metadata — unless the epoch shows
/// another write slipped in.
///
/// A write that fails may have applied anyway, so its table is dropped. A
/// writer that dies between its commit and the bump leaves the epoch
/// behind: with a TTL ([`Self::set_ttl`]) nothing is kept longer than it.
pub struct CachingCatalog<C> {
    inner: Arc<C>,
    coord: Arc<dyn Coordinator>,
    state: Mutex<State>,
}

#[derive(Default)]
struct State {
    tables: HashMap<TableIdent, Entry>,
    /// Each table's epoch as this process last knew it; 0 = never bumped.
    epochs: HashMap<TableIdent, i64>,
    ttl: Option<(Arc<dyn Clock>, Duration)>,
    /// Whether something other than pg2iceberg writes the tables.
    other_writers: bool,
}

#[derive(Default)]
struct Entry {
    meta: Option<TableMetadata>,
    snapshots: Option<Vec<Snapshot>>,
    /// When it was read from the catalog; set with a TTL only.
    read_at: Option<Timestamp>,
}

impl State {
    fn now(&self) -> Option<Timestamp> {
        self.ttl.as_ref().map(|(clock, _)| clock.now())
    }

    fn entry(&mut self, ident: &TableIdent) -> &mut Entry {
        let read_at = self.now();
        self.tables.entry(ident.clone()).or_insert_with(|| Entry {
            read_at,
            ..Entry::default()
        })
    }

    fn expire(&mut self) {
        let Some((clock, ttl)) = &self.ttl else {
            return;
        };
        let now = clock.now().0;
        let ttl = i64::try_from(ttl.as_micros()).unwrap_or(i64::MAX);
        // One read before the TTL was set is of unknown age.
        self.tables
            .retain(|_, e| e.read_at.is_some_and(|t| now.saturating_sub(t.0) < ttl));
    }
}

impl<C: Catalog> CachingCatalog<C> {
    /// The TTL long-running processes keep: well inside
    /// `maintenance_grace` (30m by default), past which orphan cleanup
    /// may delete files a stale history still points to.
    pub const DEFAULT_TTL: Duration = Duration::from_secs(60);

    pub fn new(inner: Arc<C>, coord: Arc<dyn Coordinator>) -> Self {
        Self {
            inner,
            coord,
            state: Mutex::default(),
        }
    }

    /// The catalog itself, for reads that must not be stale whatever
    /// another process failed to record — orphan cleanup's, which deletes
    /// every file the table doesn't reference.
    pub fn uncached(&self) -> &C {
        &self.inner
    }

    /// Keep nothing read from the catalog longer than `ttl` by `clock`.
    pub fn set_ttl(&self, clock: Arc<dyn Clock>, ttl: Duration) {
        self.lock().ttl = Some((clock, ttl));
    }

    /// Whether something other than pg2iceberg writes the tables — a
    /// managed catalog compacting and expiring them. Its writes bump no
    /// epoch, so then [`Self::sync`] drops everything read: each cycle
    /// starts from the tables as they are, rereading only those it
    /// works on.
    pub fn set_other_writers(&self, other_writers: bool) {
        self.lock().other_writers = other_writers;
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, State> {
        self.state.lock().expect("catalog cache poisoned")
    }

    /// Drop the tables another process wrote since this one last knew,
    /// and those kept past the TTL — or, with other writers, every one.
    pub async fn sync(&self) {
        {
            let mut s = self.lock();
            if s.other_writers {
                s.tables.clear();
                return;
            }
        }
        let tables: Vec<TableIdent> = {
            let s = self.lock();
            let known: std::collections::BTreeSet<&TableIdent> =
                s.tables.keys().chain(s.epochs.keys()).collect();
            known.into_iter().cloned().collect()
        };
        if tables.is_empty() {
            return;
        }
        let epochs = self.coord.table_epochs(&tables).await;
        let mut s = self.lock();
        match epochs {
            Ok(epochs) => {
                for t in tables {
                    let epoch = epochs.get(&t).copied().unwrap_or(0);
                    if s.epochs.insert(t.clone(), epoch) != Some(epoch) {
                        s.tables.remove(&t);
                    }
                }
            }
            Err(e) => {
                tracing::warn!(error = %e, "can't read table epochs: dropping the catalog cache");
                s.tables.clear();
                s.epochs.clear();
            }
        }
        s.expire();
    }

    /// After a write to `ident` that left the table at `meta` — `None`
    /// when unknown: the write failed, or returns no metadata.
    async fn wrote(&self, ident: &TableIdent, meta: Option<&TableMetadata>) {
        let bumped = self.coord.bump_table_epoch(ident).await;
        let mut s = self.lock();
        let known = s.epochs.get(ident).copied().unwrap_or(0);
        match bumped {
            Ok(epoch) => {
                s.epochs.insert(ident.clone(), epoch);
                if epoch != known + 1 {
                    // Another process wrote it too.
                    s.tables.remove(ident);
                    return;
                }
            }
            Err(e) => tracing::warn!(
                error = %e,
                table = %ident,
                "can't record a write to the table: other pg2iceberg processes \
                 caching it won't see the write until their cache expires"
            ),
        }
        match meta {
            Some(meta) => {
                let read_at = s.now();
                s.tables.insert(
                    ident.clone(),
                    Entry {
                        meta: Some(meta.clone()),
                        snapshots: None,
                        read_at,
                    },
                );
            }
            None => {
                s.tables.remove(ident);
            }
        }
    }

    async fn written(
        &self,
        ident: &TableIdent,
        result: Result<TableMetadata>,
    ) -> Result<TableMetadata> {
        self.wrote(ident, result.as_ref().ok()).await;
        result
    }
}

#[async_trait]
impl<C: Catalog> Catalog for CachingCatalog<C> {
    async fn ensure_namespace(&self, ns: &Namespace) -> Result<()> {
        self.inner.ensure_namespace(ns).await
    }

    async fn load_table(&self, ident: &TableIdent) -> Result<Option<TableMetadata>> {
        if let Some(meta) = self.lock().tables.get(ident).and_then(|e| e.meta.clone()) {
            return Ok(Some(meta));
        }
        let meta = self.inner.load_table(ident).await?;
        if let Some(meta) = &meta {
            self.lock().entry(ident).meta = Some(meta.clone());
        }
        Ok(meta)
    }

    async fn create_table(&self, schema: &TableSchema) -> Result<TableMetadata> {
        let result = self.inner.create_table(schema).await;
        self.written(&schema.ident, result).await
    }

    async fn commit_snapshot(&self, prepared: PreparedCommit) -> Result<TableMetadata> {
        let ident = prepared.ident.clone();
        let result = self.inner.commit_snapshot(prepared).await;
        self.written(&ident, result).await
    }

    async fn commit_snapshots(
        &self,
        steps: Vec<PreparedCommit>,
        log_range: Option<LogRange>,
        remove_properties: std::collections::BTreeSet<String>,
    ) -> Result<TableMetadata> {
        let ident = steps.first().map(|s| s.ident.clone());
        let result = self
            .inner
            .commit_snapshots(steps, log_range, remove_properties)
            .await;
        match ident {
            Some(ident) => self.written(&ident, result).await,
            None => result,
        }
    }

    async fn commit_compaction(&self, prepared: PreparedCompaction) -> Result<TableMetadata> {
        let ident = prepared.ident.clone();
        let result = self.inner.commit_compaction(prepared).await;
        self.written(&ident, result).await
    }

    async fn evolve_schema(
        &self,
        ident: &TableIdent,
        changes: Vec<SchemaChange>,
        set_properties: std::collections::BTreeMap<String, String>,
    ) -> Result<TableMetadata> {
        let result = self
            .inner
            .evolve_schema(ident, changes, set_properties)
            .await;
        self.written(ident, result).await
    }

    async fn expire_snapshots(&self, ident: &TableIdent, retention_ms: i64) -> Result<usize> {
        let result = self.inner.expire_snapshots(ident, retention_ms).await;
        self.wrote(ident, None).await;
        result
    }

    async fn snapshots(&self, ident: &TableIdent) -> Result<Vec<Snapshot>> {
        if let Some(snapshots) = self
            .lock()
            .tables
            .get(ident)
            .and_then(|e| e.snapshots.clone())
        {
            return Ok(snapshots);
        }
        let snapshots = self.inner.snapshots(ident).await?;
        self.lock().entry(ident).snapshots = Some(snapshots.clone());
        Ok(snapshots)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pg2iceberg_core::typemap::IcebergType;
    use pg2iceberg_core::ColumnSchema;
    use pg2iceberg_iceberg::DataFile;
    use pg2iceberg_sim::catalog::MemoryCatalog;
    use pg2iceberg_sim::clock::TestClock;
    use pg2iceberg_sim::coord::MemoryCoordinator;
    use pollster::block_on;

    fn ident() -> TableIdent {
        TableIdent {
            namespace: Namespace(vec!["public".into()]),
            name: "orders".into(),
        }
    }

    fn schema() -> TableSchema {
        TableSchema {
            ident: ident(),
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

    fn append(path: &str) -> PreparedCommit {
        PreparedCommit {
            ident: ident(),
            data_files: vec![DataFile {
                path: path.into(),
                record_count: 1,
                byte_size: 1,
                equality_field_ids: Vec::new(),
                partition_values: Vec::new(),
                sequence_number: None,
            }],
            equality_deletes: Vec::new(),
        }
    }

    /// A catalog whose next commit, once it lands, is followed by another
    /// process's commit to the same table — recorded in the coordinator
    /// before the first committer records its own.
    struct OtherWriterAfterNextCommit {
        inner: Arc<MemoryCatalog>,
        other: Mutex<Option<CachingCatalog<MemoryCatalog>>>,
    }

    #[async_trait]
    impl Catalog for OtherWriterAfterNextCommit {
        async fn ensure_namespace(&self, ns: &Namespace) -> Result<()> {
            self.inner.ensure_namespace(ns).await
        }
        async fn load_table(&self, ident: &TableIdent) -> Result<Option<TableMetadata>> {
            self.inner.load_table(ident).await
        }
        async fn create_table(&self, schema: &TableSchema) -> Result<TableMetadata> {
            self.inner.create_table(schema).await
        }
        async fn commit_snapshot(&self, prepared: PreparedCommit) -> Result<TableMetadata> {
            let meta = self.inner.commit_snapshot(prepared).await;
            let other = self.other.lock().unwrap().take();
            if let Some(other) = other {
                other
                    .commit_snapshot(append("s3://t/other.parquet"))
                    .await?;
            }
            meta
        }
        async fn evolve_schema(
            &self,
            ident: &TableIdent,
            changes: Vec<SchemaChange>,
            set_properties: std::collections::BTreeMap<String, String>,
        ) -> Result<TableMetadata> {
            self.inner
                .evolve_schema(ident, changes, set_properties)
                .await
        }
        async fn snapshots(&self, ident: &TableIdent) -> Result<Vec<Snapshot>> {
            self.inner.snapshots(ident).await
        }
    }

    /// Another engine — a managed catalog compacting the table — writes
    /// it without a word to the coordinator. With other writers, a sync
    /// forgets what was read, so the next cycle sees the table as it is.
    #[test]
    fn with_other_writers_a_sync_forgets_what_was_read() {
        let (coord, _clock) = MemoryCoordinator::with_test_clock(TestClock::at(0));
        let coord: Arc<dyn Coordinator> = Arc::new(coord);
        let shared = Arc::new(MemoryCatalog::new());
        let ours = CachingCatalog::new(shared.clone(), coord);
        block_on(ours.ensure_namespace(&ident().namespace)).unwrap();
        block_on(ours.create_table(&schema())).unwrap();
        let current = |c: &CachingCatalog<MemoryCatalog>| {
            block_on(c.load_table(&ident()))
                .unwrap()
                .unwrap()
                .current_snapshot_id
        };
        let theirs = |n: usize| {
            block_on(shared.commit_snapshot(append(&format!("s3://t/theirs-{n}.parquet"))))
                .unwrap();
        };

        assert_eq!(current(&ours), None);
        theirs(0);
        block_on(ours.sync());
        assert_eq!(
            current(&ours),
            None,
            "no other writers: kept until a pg2iceberg write"
        );

        ours.set_other_writers(true);
        block_on(ours.sync());
        assert_eq!(current(&ours), Some(1));
        theirs(1);
        block_on(ours.sync());
        assert_eq!(current(&ours), Some(2));
        assert_eq!(block_on(ours.snapshots(&ident())).unwrap().len(), 2);
    }

    /// Another process writes the table after this one's commit lands but
    /// before this one records it: the commit's response is already
    /// stale, and the epoch — moved twice — is what tells.
    #[test]
    fn a_write_recorded_before_ours_drops_our_commits_response() {
        let (coord, _clock) = MemoryCoordinator::with_test_clock(TestClock::at(0));
        let coord: Arc<dyn Coordinator> = Arc::new(coord);
        let shared = Arc::new(MemoryCatalog::new());
        let catalog = Arc::new(OtherWriterAfterNextCommit {
            inner: shared.clone(),
            other: Mutex::new(None),
        });
        let ours = CachingCatalog::new(catalog.clone(), coord.clone());
        block_on(ours.ensure_namespace(&ident().namespace)).unwrap();
        block_on(ours.create_table(&schema())).unwrap();
        block_on(ours.sync());

        *catalog.other.lock().unwrap() = Some(CachingCatalog::new(shared.clone(), coord.clone()));
        block_on(ours.commit_snapshot(append("s3://t/ours.parquet"))).unwrap();

        let current = block_on(shared.load_table(&ident())).unwrap().unwrap();
        let seen = block_on(ours.load_table(&ident())).unwrap().unwrap();
        assert_eq!(seen.current_snapshot_id, current.current_snapshot_id);
        let history = block_on(ours.snapshots(&ident())).unwrap();
        assert_eq!(history.len(), 2, "{history:?}");
    }
}
