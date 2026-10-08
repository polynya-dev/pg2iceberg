//! `IcebergRustCatalog`: wraps any `iceberg::Catalog` (Memory, REST, Glue,
//! SQL, ...) behind our [`crate::Catalog`] trait so the materializer can
//! drive a real Iceberg backend.
//!
//! Translation rules:
//!
//! - **Mixed data + equality-delete commits.** `commit_snapshot` builds a
//!   single `Vec<DataFile>` from `prepared.data_files` (content = Data)
//!   and `prepared.equality_deletes` (content = EqualityDeletes), then
//!   submits via [`iceberg::transaction::Transaction::fast_append`]. The
//!   forked `FastAppendAction` routes by `content_type()` into separate
//!   data and delete manifests at commit time.
//! - **Schema evolution.** Translates our `Vec<SchemaChange>` to a target
//!   `iceberg::Schema` (via [`crate::apply_schema_changes`]) and submits
//!   via the forked `Transaction::replace_schema()`.
//! - **`load_table` not-found.** Maps `ErrorKind::TableNotFound` and
//!   `NamespaceNotFound` → `Ok(None)` (the materializer treats not-found
//!   distinctly from transient errors).
//! - **History, whoever wrote it.** `snapshots` takes what a snapshot
//!   holds from its live manifest entries — Java writes the files a
//!   commit removes as `DELETED` entries — and what it removed as what its
//!   parent held that it doesn't, whatever its operation: a managed
//!   catalog maintaining the table commits rewrites and deletes of its own.
//!
//! See [`super::gap_audit`] for the full method-by-method status and the
//! list of fork patches we depend on.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;

use async_trait::async_trait;
use iceberg::spec::{
    DataContentType, DataFile as IcebergDataFile, DataFileBuilder, DataFileFormat, FormatVersion,
    Literal, NestedField, PrimitiveLiteral, PrimitiveType, Schema as IcebergSchema, Struct, Type,
};
use iceberg::table::Table;
use iceberg::transaction::{ActionCommit, ApplyTransactionAction, Transaction, TransactionAction};
use iceberg::TableUpdate;
use iceberg::{
    Catalog as IcebergCatalogTrait, ErrorKind, NamespaceIdent, TableCreation,
    TableIdent as IcebergTableIdent,
};
use pg2iceberg_core::{
    typemap::IcebergType, ColumnSchema, Namespace, PartitionLiteral, TableIdent, TableSchema,
};

use crate::{
    apply_schema_changes, log_ends_property, merge_log_ends, recorded_log_ends, Catalog, DataFile,
    IcebergError, LogRange, PreparedCommit, Result, SchemaChange, Snapshot, TableMetadata,
};

/// Wraps an `iceberg::Catalog` (e.g. `MemoryCatalog`, `RestCatalog`,
/// `GlueCatalog`) and exposes it as our [`Catalog`] trait.
pub struct IcebergRustCatalog<C: IcebergCatalogTrait> {
    inner: Arc<C>,
    manifests: Arc<ManifestCache>,
}

impl<C: IcebergCatalogTrait> IcebergRustCatalog<C> {
    pub fn new(inner: Arc<C>) -> Self {
        Self {
            inner,
            manifests: Arc::default(),
        }
    }
}

impl<C: IcebergCatalogTrait> Clone for IcebergRustCatalog<C> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
            manifests: Arc::clone(&self.manifests),
        }
    }
}

/// Each table's manifest lists and manifests, by path, as
/// [`Catalog::snapshots`] last read its history. They never change once
/// written — a commit writes new ones — so a read of the history only
/// fetches what's new since; without it, every call fetched each
/// snapshot's manifest list and every manifest it names, a round trip
/// each to object storage, and their number grows with every commit.
/// Holds only what the table's history still names.
#[derive(Default)]
struct ManifestCache {
    tables: std::sync::Mutex<HashMap<TableIdent, TableManifests>>,
}

#[derive(Clone, Default)]
struct TableManifests {
    lists: HashMap<String, Arc<iceberg::spec::ManifestList>>,
    manifests: HashMap<String, Arc<iceberg::spec::Manifest>>,
}

impl ManifestCache {
    fn get(&self, ident: &TableIdent) -> TableManifests {
        let tables = self.tables.lock().expect("manifest cache poisoned");
        tables.get(ident).cloned().unwrap_or_default()
    }

    fn put(&self, ident: &TableIdent, read: TableManifests) {
        let mut tables = self.tables.lock().expect("manifest cache poisoned");
        tables.insert(ident.clone(), read);
    }
}

impl<C: IcebergCatalogTrait> std::fmt::Debug for IcebergRustCatalog<C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IcebergRustCatalog").finish()
    }
}

#[async_trait]
impl<C: IcebergCatalogTrait + Send + Sync + 'static> Catalog for IcebergRustCatalog<C> {
    async fn ensure_namespace(&self, ns: &Namespace) -> Result<()> {
        let ident = to_iceberg_namespace(ns)?;
        if self
            .inner
            .namespace_exists(&ident)
            .await
            .map_err(map_iceberg_err)?
        {
            return Ok(());
        }
        match self.inner.create_namespace(&ident, HashMap::new()).await {
            Ok(_) => Ok(()),
            // Concurrent create races against our exists-check.
            Err(e) if e.kind() == ErrorKind::NamespaceAlreadyExists => Ok(()),
            Err(e) => Err(map_iceberg_err(e)),
        }
    }

    async fn load_table(&self, ident: &TableIdent) -> Result<Option<TableMetadata>> {
        let it = to_iceberg_table_ident(ident)?;
        match self.inner.load_table(&it).await {
            Ok(table) => Ok(Some(metadata_from_table(ident, &table)?)),
            // Either a missing table or a missing namespace means "no such
            // table" from the materializer's perspective.
            Err(e)
                if e.kind() == ErrorKind::TableNotFound
                    || e.kind() == ErrorKind::NamespaceNotFound =>
            {
                Ok(None)
            }
            // iceberg-rust's REST catalog (as of 0.9) returns
            // `ErrorKind::Unexpected` with this exact message on a
            // 404 from the REST server (instead of the expected
            // `TableNotFound`). Caught by the testcontainers
            // integration test against `apache/iceberg-rest-fixture`.
            // Treat it as "no such table" — startup validation
            // depends on this returning `None` for first-run
            // greenfield deployments.
            Err(e)
                if e.kind() == ErrorKind::Unexpected
                    && e.message()
                        .contains("Tried to load a table that does not exist") =>
            {
                Ok(None)
            }
            Err(e) => Err(map_iceberg_err(e)),
        }
    }

    async fn create_table(&self, schema: &TableSchema) -> Result<TableMetadata> {
        let ns = to_iceberg_namespace(&schema.ident.namespace)?;
        let ice_schema = to_iceberg_schema(schema)?;
        // TypedBuilder switches type-state when `partition_spec()` is
        // called, so we have to choose at compile time which arm to
        // build. The `clone()` on `ice_schema` is the cost of avoiding
        // a more elaborate dynamic-build dance.
        //
        // Format v2, explicitly: what pg2iceberg writes and is tested
        // against, and what every engine reads (v3 isn't yet — ClickHouse,
        // open-source Trino).
        let creation = if schema.partition_spec.is_empty() {
            TableCreation::builder()
                .name(schema.ident.name.clone())
                .schema(ice_schema)
                .format_version(FormatVersion::V2)
                .build()
        } else {
            let unbound = to_iceberg_unbound_partition_spec(schema)?;
            TableCreation::builder()
                .name(schema.ident.name.clone())
                .schema(ice_schema)
                .partition_spec(unbound)
                .format_version(FormatVersion::V2)
                .build()
        };
        let table = self
            .inner
            .create_table(&ns, creation)
            .await
            .map_err(map_iceberg_err)?;
        metadata_from_table(&schema.ident, &table)
    }

    async fn commit_snapshot(&self, prepared: PreparedCommit) -> Result<TableMetadata> {
        self.commit_snapshots(vec![prepared], None, BTreeSet::new())
            .await
    }

    async fn commit_snapshots(
        &self,
        steps: Vec<PreparedCommit>,
        log_range: Option<LogRange>,
        remove_properties: BTreeSet<String>,
    ) -> Result<TableMetadata> {
        let ident = steps
            .first()
            .map(|s| s.ident.clone())
            .ok_or_else(|| IcebergError::Other("commit_snapshots: no steps".into()))?;
        if steps.iter().any(|s| s.ident != ident) {
            return Err(IcebergError::Other(
                "commit_snapshots: steps span several tables".into(),
            ));
        }
        let it = to_iceberg_table_ident(&ident)?;
        let table = self.inner.load_table(&it).await.map_err(map_iceberg_err)?;
        let mut files: Vec<Vec<IcebergDataFile>> = Vec::with_capacity(steps.len());
        for step in &steps {
            if !step.data_files.is_empty() || !step.equality_deletes.is_empty() {
                files.push(to_iceberg_files(step, &table)?);
            }
        }

        // Recorded on the last snapshot, with every group's end so far.
        let mut ends = table_log_ends(&table);
        if let Some(range) = &log_range {
            merge_log_ends(&mut ends, [(range.group.clone(), range.end)]);
        }
        let properties: HashMap<String, String> = log_range
            .iter()
            .flat_map(LogRange::to_properties)
            .chain(log_ends_property(&ends))
            .collect();
        let tx = Transaction::new(&table);
        let tx = match files.len() {
            // No work — match the sim-catalog noop semantics so the
            // materializer can flush "no data, no deletes" without a
            // snapshot bump.
            0 if remove_properties.is_empty() => return metadata_from_table(&ident, &table),
            0 => Ok(tx),
            1 => tx
                .fast_append()
                // The materializer guarantees unique file paths; skip
                // iceberg-rust's path-dedup, which would otherwise scan the
                // full manifest list on each commit.
                .with_check_duplicate(false)
                // FastAppendAction (forked) routes by `content_type()` into
                // separate data and delete manifests at commit time.
                .add_data_files(files.remove(0))
                .set_snapshot_properties(properties)
                .apply(tx),
            _ => ChainedAppendAction {
                steps: files,
                properties,
            }
            .apply(tx),
        }
        .map_err(map_iceberg_err)?;
        let tx = if remove_properties.is_empty() {
            tx
        } else {
            remove_properties
                .into_iter()
                .fold(tx.update_table_properties(), |action, key| {
                    action.remove(key)
                })
                .apply(tx)
                .map_err(map_iceberg_err)?
        };
        let updated = tx
            .commit(self.inner.as_ref())
            .await
            .map_err(map_iceberg_err)?;
        metadata_from_table(&ident, &updated)
    }

    async fn commit_compaction(
        &self,
        prepared: crate::PreparedCompaction,
    ) -> Result<TableMetadata> {
        if prepared.added_data_files.is_empty() && prepared.removed_paths.is_empty() {
            // Pure no-op: hand back current metadata without bumping a
            // snapshot.
            let it = to_iceberg_table_ident(&prepared.ident)?;
            let table = self.inner.load_table(&it).await.map_err(map_iceberg_err)?;
            return metadata_from_table(&prepared.ident, &table);
        }

        let it = to_iceberg_table_ident(&prepared.ident)?;
        let table = self.inner.load_table(&it).await.map_err(map_iceberg_err)?;
        let spec_id = table.metadata().default_partition_spec_id();
        let part_field_count = table.metadata().default_partition_spec().fields().len();

        // Translate our DataFile values into iceberg::DataFile, same
        // shape commit_snapshot uses.
        let mut iceberg_added: Vec<IcebergDataFile> =
            Vec::with_capacity(prepared.added_data_files.len());
        for df in &prepared.added_data_files {
            let partition =
                build_partition_struct(&df.partition_values, part_field_count, &df.path)?;
            iceberg_added.push(
                DataFileBuilder::default()
                    .content(DataContentType::Data)
                    .file_path(df.path.clone())
                    .file_format(DataFileFormat::Parquet)
                    .file_size_in_bytes(df.byte_size)
                    .record_count(df.record_count)
                    .partition(partition)
                    .partition_spec_id(spec_id)
                    .build()
                    .map_err(|e| IcebergError::Other(format!("compacted file build: {e}")))?,
            );
        }

        let tx = Transaction::new(&table);
        let mut action = tx
            .rewrite_files()
            .add_data_files(iceberg_added)
            .remove_paths(prepared.removed_paths.iter().cloned());
        if let Some(seq) = prepared.data_sequence_number {
            action = action.set_data_sequence_number(seq);
        }
        let action = action
            // iceberg-rust refuses a snapshot that adds no files and sets
            // no summary property (apache/iceberg-rust#1548), and a pass
            // whose inputs' rows were all deleted adds none.
            .set_snapshot_properties(
                [("pg2iceberg.operation".to_string(), "compaction".to_string())]
                    .into_iter()
                    // Carried forward: this snapshot may outlive those
                    // that recorded them.
                    .chain(log_ends_property(&table_log_ends(&table)))
                    .collect(),
            );
        let tx = action.apply(tx).map_err(map_iceberg_err)?;
        let updated = tx
            .commit(self.inner.as_ref())
            .await
            .map_err(map_iceberg_err)?;
        metadata_from_table(&prepared.ident, &updated)
    }

    async fn expire_snapshots(&self, ident: &TableIdent, retention_ms: i64) -> Result<usize> {
        let it = to_iceberg_table_ident(ident)?;
        let table = match self.inner.load_table(&it).await {
            Ok(t) => t,
            Err(e)
                if e.kind() == ErrorKind::TableNotFound
                    || e.kind() == ErrorKind::NamespaceNotFound =>
            {
                return Ok(0);
            }
            Err(e) => return Err(map_iceberg_err(e)),
        };

        let snaps: Vec<_> = table.metadata().snapshots().cloned().collect();
        if snaps.is_empty() {
            return Ok(0);
        }
        // Cutoff is relative to the most recent snapshot's timestamp.
        // Iceberg's metadata stores wall-clock millis on each snapshot;
        // we anchor "now" to the latest one so this works deterministically
        // against catalogs whose clock differs from ours.
        let latest_ts = snaps.iter().map(|s| s.timestamp_ms()).max().unwrap_or(0);
        let cutoff = latest_ts - retention_ms;
        let current_id = table.metadata().current_snapshot_id();

        let to_remove: Vec<i64> = snaps
            .iter()
            .filter(|s| s.timestamp_ms() < cutoff && Some(s.snapshot_id()) != current_id)
            .map(|s| s.snapshot_id())
            .collect();

        if to_remove.is_empty() {
            return Ok(0);
        }

        let count = to_remove.len();
        let action = ExpireSnapshotsAction {
            snapshot_ids: to_remove,
        };
        let tx = Transaction::new(&table);
        let tx = action.apply(tx).map_err(map_iceberg_err)?;
        tx.commit(self.inner.as_ref())
            .await
            .map_err(map_iceberg_err)?;
        Ok(count)
    }

    async fn evolve_schema(
        &self,
        ident: &TableIdent,
        changes: Vec<SchemaChange>,
        set_properties: BTreeMap<String, String>,
    ) -> Result<TableMetadata> {
        if changes.is_empty() && set_properties.is_empty() {
            // Match the sim semantics: a no-op evolve still returns current
            // metadata rather than erroring.
            let it = to_iceberg_table_ident(ident)?;
            let table = self.inner.load_table(&it).await.map_err(map_iceberg_err)?;
            return metadata_from_table(ident, &table);
        }

        let it = to_iceberg_table_ident(ident)?;
        let table = self.inner.load_table(&it).await.map_err(map_iceberg_err)?;

        // Translate iceberg schema → our shape, apply changes, translate back.
        // Keeping the round-trip in our type domain centralizes field-id
        // allocation rules (next id = current highest + 1) and the soft-drop
        // semantics for `DropColumn`.
        let part_spec = table.metadata().default_partition_spec();
        let mut our_schema =
            from_iceberg_schema(ident, table.metadata().current_schema(), part_spec.as_ref())?;
        apply_schema_changes(&mut our_schema, &changes)?;
        let new_iceberg_schema = to_iceberg_schema(&our_schema)?;

        let mut tx = Transaction::new(&table);
        if !changes.is_empty() {
            let action = tx.replace_schema().set_schema(new_iceberg_schema);
            tx = action.apply(tx).map_err(map_iceberg_err)?;
        }
        if !set_properties.is_empty() {
            tx = set_properties
                .into_iter()
                .fold(tx.update_table_properties(), |action, (key, value)| {
                    action.set(key, value)
                })
                .apply(tx)
                .map_err(map_iceberg_err)?;
        }
        let updated = tx
            .commit(self.inner.as_ref())
            .await
            .map_err(map_iceberg_err)?;
        metadata_from_table(ident, &updated)
    }

    async fn snapshots(&self, ident: &TableIdent) -> Result<Vec<Snapshot>> {
        let it = to_iceberg_table_ident(ident)?;
        let table = match self.inner.load_table(&it).await {
            Ok(t) => t,
            Err(e)
                if e.kind() == ErrorKind::TableNotFound
                    || e.kind() == ErrorKind::NamespaceNotFound =>
            {
                return Ok(Vec::new());
            }
            Err(e) => return Err(map_iceberg_err(e)),
        };
        let mut out: Vec<Snapshot> = Vec::new();
        let mut snaps: Vec<_> = table.metadata().snapshots().cloned().collect();
        // Sort by sequence_number ASC so we visit each snapshot AFTER its
        // parent. The path-cache lookup below relies on the parent already
        // being populated when we compute `removed_paths`.
        snaps.sort_by_key(|s| s.sequence_number());
        // Cache of snapshot_id → all live file paths in that snapshot's
        // manifest list. We need the previous snapshot's full path set to
        // compute `removed_paths` — diffing parent_paths - current_paths
        // gives the set of files the snapshot dropped.
        let mut paths_per_snap: BTreeMap<i64, std::collections::BTreeSet<String>> = BTreeMap::new();
        // Live files added by expired snapshots, by path. Expiry drops a
        // snapshot from the metadata but not the files it added: they stay
        // in the retained snapshots' manifests, as entries naming a
        // snapshot that no longer exists.
        let retained: std::collections::BTreeSet<i64> =
            snaps.iter().map(|s| s.snapshot_id()).collect();
        let mut expired_adds: BTreeMap<String, (i64, DataFile, DataContentType)> = BTreeMap::new();
        let cached = self.manifests.get(ident);
        let mut read = TableManifests::default();
        for snap in snaps {
            // Use the iceberg snapshot_id for manifest filtering (matches the
            // `added_snapshot_id` field stored in manifest entries), but report
            // `sequence_number` as our `Snapshot.id` so MoR ordering
            // (`delete.id > data.id`) stays monotonic. iceberg-rust generates
            // `snapshot_id` as a random 63-bit value — comparing those would
            // break the verifier and FileIndex.
            let snap_id = snap.snapshot_id();
            let seq_num = snap.sequence_number();
            let parent_id = snap.parent_snapshot_id();
            let list_path = snap.manifest_list();
            let manifest_list = match cached.lists.get(list_path) {
                Some(list) => Arc::clone(list),
                None => Arc::new(
                    table
                        .manifest_list_reader(&snap)
                        .load()
                        .await
                        .map_err(map_iceberg_err)?,
                ),
            };
            read.lists
                .insert(list_path.to_string(), Arc::clone(&manifest_list));
            let mut data_files: Vec<DataFile> = Vec::new();
            let mut delete_files: Vec<DataFile> = Vec::new();
            // All live file paths in this snapshot's manifest list,
            // regardless of which snapshot first added them. Used both as
            // the cache for the next snapshot's diff and as the "current"
            // side of this snapshot's.
            let mut all_paths_this_snap: std::collections::BTreeSet<String> =
                std::collections::BTreeSet::new();
            for entry in manifest_list.entries() {
                // Each manifest list names every manifest before it.
                let path = &entry.manifest_path;
                let manifest = match read.manifests.get(path).or(cached.manifests.get(path)) {
                    Some(manifest) => Arc::clone(manifest),
                    None => Arc::new(
                        entry
                            .load_manifest(table.file_io())
                            .await
                            .map_err(map_iceberg_err)?,
                    ),
                };
                read.manifests.insert(path.clone(), Arc::clone(&manifest));
                // A `DELETED` entry records a file its snapshot removed (other
                // engines write them; our rewrite omits removed files): no
                // file the snapshot holds.
                for me in manifest.entries().iter().filter(|me| me.is_alive()) {
                    let df = me.data_file();
                    all_paths_this_snap.insert(df.file_path().to_string());

                    let partition_values = iceberg_struct_to_partition_literals(df.partition());
                    let mut our = DataFile {
                        path: df.file_path().to_string(),
                        record_count: df.record_count(),
                        byte_size: df.file_size_in_bytes(),
                        equality_field_ids: df.equality_ids().unwrap_or_default(),
                        partition_values,
                        sequence_number: None,
                    };
                    // Match the sim's "files added in this commit"
                    // semantics: only surface entries first introduced
                    // by this snapshot. Judge by the entry's own snapshot
                    // id, not its manifest's: a Replace that drops part
                    // of a manifest rewrites the survivors into a new
                    // manifest it owns, as `Existing` entries that keep
                    // their original snapshot and sequence number.
                    // Surfacing them here would list them twice, the
                    // second time at the Replace's sequence number —
                    // above deletes that still apply to them. An entry
                    // naming an expired snapshot is collected for that
                    // snapshot's stand-in instead.
                    if me.snapshot_id() != Some(snap_id) {
                        let added_by_expired =
                            me.snapshot_id().is_none_or(|id| !retained.contains(&id));
                        if added_by_expired {
                            let seq = me.sequence_number().unwrap_or(seq_num);
                            expired_adds.entry(our.path.clone()).or_insert((
                                seq,
                                our,
                                df.content_type(),
                            ));
                        }
                        continue;
                    }
                    // A compaction's output keeps the sequence number its
                    // pass read the table at.
                    our.sequence_number = me.sequence_number().filter(|&seq| seq != seq_num);
                    match df.content_type() {
                        DataContentType::Data => data_files.push(our),
                        DataContentType::EqualityDeletes | DataContentType::PositionDeletes => {
                            delete_files.push(our)
                        }
                    }
                }
            }

            // What the snapshot removed: what its parent held that it
            // doesn't. A rewrite's inputs, but not only: another engine
            // may drop files in a `delete` or `overwrite` (dangling delete
            // files, say). An expired parent's files are gone with it.
            let removed_paths: Vec<String> = parent_id
                .and_then(|pid| paths_per_snap.get(&pid))
                .map(|parent_paths| {
                    parent_paths
                        .difference(&all_paths_this_snap)
                        .cloned()
                        .collect()
                })
                .unwrap_or_default();

            paths_per_snap.insert(snap_id, all_paths_this_snap);

            out.push(Snapshot {
                id: seq_num,
                data_files,
                delete_files,
                removed_paths,
                timestamp_ms: snap.timestamp_ms(),
                expired: false,
                log_range: log_range_of(&snap),
            });
        }
        // One stand-in snapshot per sequence number: MoR ordering only
        // needs the files' sequence numbers, which the manifests keep.
        let mut stand_ins: BTreeMap<i64, Snapshot> = BTreeMap::new();
        for (seq, df, content) in expired_adds.into_values() {
            let snap = stand_ins.entry(seq).or_insert_with(|| Snapshot {
                id: seq,
                data_files: Vec::new(),
                delete_files: Vec::new(),
                removed_paths: Vec::new(),
                timestamp_ms: 0,
                expired: true,
                log_range: None,
            });
            match content {
                DataContentType::Data => snap.data_files.push(df),
                DataContentType::EqualityDeletes | DataContentType::PositionDeletes => {
                    snap.delete_files.push(df)
                }
            }
        }
        out.extend(stand_ins.into_values());
        out.sort_by_key(|s| s.id);
        self.manifests.put(ident, read);
        Ok(out)
    }
}

// ───── inline transaction actions ────────────────────────────────────────

/// Translate one prepared step's data + equality-delete files into
/// iceberg `DataFile`s against `table`'s default partition spec.
fn to_iceberg_files(prepared: &PreparedCommit, table: &Table) -> Result<Vec<IcebergDataFile>> {
    let spec_id = table.metadata().default_partition_spec_id();
    let part_field_count = table.metadata().default_partition_spec().fields().len();
    let mut all_files: Vec<IcebergDataFile> =
        Vec::with_capacity(prepared.data_files.len() + prepared.equality_deletes.len());
    for df in &prepared.data_files {
        let partition = build_partition_struct(&df.partition_values, part_field_count, &df.path)?;
        all_files.push(
            DataFileBuilder::default()
                .content(DataContentType::Data)
                .file_path(df.path.clone())
                .file_format(DataFileFormat::Parquet)
                .file_size_in_bytes(df.byte_size)
                .record_count(df.record_count)
                .partition(partition)
                .partition_spec_id(spec_id)
                .build()
                .map_err(|e| IcebergError::Other(format!("data file build: {e}")))?,
        );
    }
    for df in &prepared.equality_deletes {
        if df.equality_field_ids.is_empty() {
            return Err(IcebergError::Other(format!(
                "equality-delete file {} has empty equality_field_ids; refusing to \
                 commit a delete that wouldn't match any rows",
                df.path
            )));
        }
        let partition = build_partition_struct(&df.partition_values, part_field_count, &df.path)?;
        all_files.push(
            DataFileBuilder::default()
                .content(DataContentType::EqualityDeletes)
                .file_path(df.path.clone())
                .file_format(DataFileFormat::Parquet)
                .file_size_in_bytes(df.byte_size)
                .record_count(df.record_count)
                .equality_ids(Some(df.equality_field_ids.clone()))
                .partition(partition)
                .partition_spec_id(spec_id)
                .build()
                .map_err(|e| IcebergError::Other(format!("delete file build: {e}")))?,
        );
    }
    Ok(all_files)
}

/// Several fast-appends committed as one table update. Each step becomes
/// its own snapshot — parent = the previous step, next sequence number —
/// so a step's equality deletes hide rows written by earlier steps; but
/// `main` moves in a single catalog commit, so readers see all steps or
/// none.
///
/// The fork's `Transaction` can't simply hold several `fast_append`s:
/// each re-asserts that `main` is still at its pre-commit snapshot, and
/// the catalog checks every requirement against the base table, so the
/// second one always conflicts. Instead this action chains the steps
/// against a local copy of the table and keeps only the first step's
/// requirements — what Java's `UpdateRequirements` does. Pure metadata
/// plumbing over public fork APIs, like [`ExpireSnapshotsAction`].
struct ChainedAppendAction {
    steps: Vec<Vec<IcebergDataFile>>,
    /// Summary properties of the last step's snapshot.
    properties: HashMap<String, String>,
}

#[async_trait]
impl TransactionAction for ChainedAppendAction {
    async fn commit(self: Arc<Self>, table: &Table) -> iceberg::Result<ActionCommit> {
        let mut local = table.clone();
        let mut updates = Vec::new();
        let mut requirements = None;
        for (i, files) in self.steps.iter().enumerate() {
            let mut append = Transaction::new(&local)
                .fast_append()
                .with_check_duplicate(false)
                .add_data_files(files.clone());
            if i + 1 == self.steps.len() {
                append = append.set_snapshot_properties(self.properties.clone());
            }
            let mut step = Arc::new(append).commit(&local).await?;
            let step_updates = step.take_updates();
            requirements.get_or_insert(step.take_requirements());
            let mut builder = local
                .metadata()
                .clone()
                .into_builder(local.metadata_location().map(str::to_string));
            for update in &step_updates {
                builder = update.clone().apply(builder)?;
            }
            // `local`'s runtime isn't public; pg2iceberg always runs
            // inside the tokio runtime iceberg-rust was given.
            let mut next = Table::builder()
                .identifier(local.identifier().clone())
                .file_io(local.file_io().clone())
                .metadata(builder.build()?.metadata)
                .runtime(iceberg::Runtime::try_current()?);
            if let Some(location) = local.metadata_location() {
                next = next.metadata_location(location);
            }
            local = next.build()?;
            updates.extend(step_updates);
        }
        Ok(ActionCommit::new(updates, requirements.unwrap_or_default()))
    }
}

/// Inline `TransactionAction` that emits `TableUpdate::RemoveSnapshots`.
/// We don't add this to the iceberg-rust fork because it's pure metadata
/// — `ActionCommit::new` and `TableUpdate` are public, no fork-side
/// plumbing needed (unlike `RewriteFilesAction`, which had to walk the
/// manifest list).
struct ExpireSnapshotsAction {
    snapshot_ids: Vec<i64>,
}

#[async_trait]
impl TransactionAction for ExpireSnapshotsAction {
    async fn commit(
        self: std::sync::Arc<Self>,
        _table: &iceberg::table::Table,
    ) -> iceberg::Result<ActionCommit> {
        Ok(ActionCommit::new(
            vec![TableUpdate::RemoveSnapshots {
                snapshot_ids: self.snapshot_ids.clone(),
            }],
            vec![],
        ))
    }
}

// ───── translation helpers ───────────────────────────────────────────────

fn to_iceberg_namespace(ns: &Namespace) -> Result<NamespaceIdent> {
    NamespaceIdent::from_strs(ns.0.iter().map(|s| s.as_str()))
        .map_err(|e| IcebergError::Other(format!("namespace ident: {e}")))
}

fn to_iceberg_table_ident(t: &TableIdent) -> Result<IcebergTableIdent> {
    Ok(IcebergTableIdent::new(
        to_iceberg_namespace(&t.namespace)?,
        t.name.clone(),
    ))
}

fn to_iceberg_type(ty: IcebergType) -> Type {
    use IcebergType::*;
    let p = match ty {
        Boolean => PrimitiveType::Boolean,
        Int => PrimitiveType::Int,
        Long => PrimitiveType::Long,
        Float => PrimitiveType::Float,
        Double => PrimitiveType::Double,
        Decimal { precision, scale } => PrimitiveType::Decimal {
            precision: precision as u32,
            scale: scale as u32,
        },
        String => PrimitiveType::String,
        Binary => PrimitiveType::Binary,
        Date => PrimitiveType::Date,
        Time => PrimitiveType::Time,
        Timestamp => PrimitiveType::Timestamp,
        TimestampTz => PrimitiveType::Timestamptz,
        Uuid => PrimitiveType::Uuid,
    };
    Type::Primitive(p)
}

fn from_iceberg_type(ty: &Type) -> Result<IcebergType> {
    let p = match ty {
        Type::Primitive(p) => p,
        Type::Struct(_) | Type::List(_) | Type::Map(_) => {
            return Err(IcebergError::Other(format!(
                "non-primitive iceberg type encountered: {ty:?}"
            )));
        }
    };
    Ok(match p {
        PrimitiveType::Boolean => IcebergType::Boolean,
        PrimitiveType::Int => IcebergType::Int,
        PrimitiveType::Long => IcebergType::Long,
        PrimitiveType::Float => IcebergType::Float,
        PrimitiveType::Double => IcebergType::Double,
        PrimitiveType::Decimal { precision, scale } => IcebergType::Decimal {
            precision: *precision as u8,
            scale: *scale as u8,
        },
        PrimitiveType::String => IcebergType::String,
        PrimitiveType::Binary => IcebergType::Binary,
        PrimitiveType::Date => IcebergType::Date,
        PrimitiveType::Time => IcebergType::Time,
        PrimitiveType::Timestamp => IcebergType::Timestamp,
        PrimitiveType::Timestamptz => IcebergType::TimestampTz,
        PrimitiveType::Uuid => IcebergType::Uuid,
        // Nanosecond timestamps + Fixed are not in our Postgres mapping;
        // surface them as an error rather than silently coercing.
        other => {
            return Err(IcebergError::Other(format!(
                "unsupported iceberg primitive: {other:?}"
            )));
        }
    })
}

fn to_iceberg_schema(schema: &TableSchema) -> Result<IcebergSchema> {
    let pk_ids: Vec<i32> = schema
        .columns
        .iter()
        .filter(|c| c.is_primary_key)
        .map(|c| c.field_id)
        .collect();
    let fields: Vec<_> = schema
        .columns
        .iter()
        .map(|c| {
            let ty = to_iceberg_type(c.ty);
            let nf = if c.nullable {
                NestedField::optional(c.field_id, &c.name, ty)
            } else {
                NestedField::required(c.field_id, &c.name, ty)
            };
            nf.into()
        })
        .collect();
    let mut b = IcebergSchema::builder()
        .with_schema_id(0)
        .with_fields(fields);
    if !pk_ids.is_empty() {
        b = b.with_identifier_field_ids(pk_ids);
    }
    b.build()
        .map_err(|e| IcebergError::Other(format!("schema build: {e}")))
}

fn from_iceberg_schema(
    ident: &TableIdent,
    schema: &IcebergSchema,
    partition_spec: &iceberg::spec::PartitionSpec,
) -> Result<TableSchema> {
    let pk_set: std::collections::BTreeSet<i32> = schema.identifier_field_ids().collect();
    let mut columns: Vec<ColumnSchema> = Vec::new();
    for f in schema.as_struct().fields().iter() {
        columns.push(ColumnSchema {
            name: f.name.clone(),
            field_id: f.id,
            ty: from_iceberg_type(&f.field_type)?,
            nullable: !f.required,
            is_primary_key: pk_set.contains(&f.id),
        });
    }

    // Build a `field_id -> column_name` index so we can resolve
    // PartitionField source IDs back to column names.
    let id_to_name: std::collections::HashMap<i32, String> = columns
        .iter()
        .map(|c| (c.field_id, c.name.clone()))
        .collect();
    let partition_fields = partition_spec
        .fields()
        .iter()
        .map(|f| {
            let source_column = id_to_name.get(&f.source_id).cloned().ok_or_else(|| {
                IcebergError::Other(format!(
                    "partition field {} references unknown source_id {}",
                    f.name, f.source_id
                ))
            })?;
            let transform = from_iceberg_transform(&f.transform)?;
            Ok(pg2iceberg_core::PartitionField {
                source_column,
                name: f.name.clone(),
                transform,
            })
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(TableSchema {
        ident: ident.clone(),
        columns,
        partition_spec: partition_fields,
        pg_schema: None,
    })
}

fn from_iceberg_transform(t: &iceberg::spec::Transform) -> Result<pg2iceberg_core::Transform> {
    use iceberg::spec::Transform as IT;
    use pg2iceberg_core::Transform as OT;
    Ok(match t {
        IT::Identity => OT::Identity,
        IT::Year => OT::Year,
        IT::Month => OT::Month,
        IT::Day => OT::Day,
        IT::Hour => OT::Hour,
        IT::Bucket(n) => OT::Bucket(*n),
        IT::Truncate(n) => OT::Truncate(*n),
        other => {
            return Err(IcebergError::Other(format!(
                "iceberg partition transform {other:?} is not supported by pg2iceberg"
            )))
        }
    })
}

fn to_iceberg_transform(t: pg2iceberg_core::Transform) -> iceberg::spec::Transform {
    use iceberg::spec::Transform as IT;
    use pg2iceberg_core::Transform as OT;
    match t {
        OT::Identity => IT::Identity,
        OT::Year => IT::Year,
        OT::Month => IT::Month,
        OT::Day => IT::Day,
        OT::Hour => IT::Hour,
        OT::Bucket(n) => IT::Bucket(n),
        OT::Truncate(n) => IT::Truncate(n),
    }
}

/// Build an `iceberg::spec::UnboundPartitionSpec` from our schema's
/// `partition_spec`. We use the *unbound* variant because at
/// `create_table` time the iceberg schema doesn't yet have stable
/// field ids for the partition fields; `TableMetadataBuilder` binds
/// them when the table metadata is constructed.
fn to_iceberg_unbound_partition_spec(
    schema: &TableSchema,
) -> Result<iceberg::spec::UnboundPartitionSpec> {
    // Explicit `spec_id = 0` and explicit per-field `field_id` are
    // required because `UnboundPartitionSpec`'s `Option<i32>` fields
    // serialise as JSON `null` when unset, and the Iceberg REST
    // server's strict Jackson deserialisation rejects null integers
    // (HTTP 500: `Cannot parse to an integer value: spec-id: null`
    // / `field-id: null`). The convention is that partition field
    // ids start at `PARTITION_FIELD_ID_START` (1000) and increment
    // per field, which matches what the catalog would assign during
    // binding anyway.
    const PARTITION_FIELD_ID_START: i32 = 1000;
    let mut fields = Vec::with_capacity(schema.partition_spec.len());
    for (i, f) in schema.partition_spec.iter().enumerate() {
        let source_id = schema.field_id_for(&f.source_column).ok_or_else(|| {
            IcebergError::Other(format!(
                "partition source column {} not in schema",
                f.source_column
            ))
        })?;
        fields.push(iceberg::spec::UnboundPartitionField {
            source_id,
            field_id: Some(PARTITION_FIELD_ID_START + i as i32),
            name: f.name.clone(),
            transform: to_iceberg_transform(f.transform),
        });
    }
    let builder = iceberg::spec::UnboundPartitionSpec::builder()
        .with_spec_id(0)
        .add_partition_fields(fields)
        .map_err(|e| IcebergError::Other(format!("add partition fields: {e}")))?;
    Ok(builder.build())
}

/// Translate our per-file `partition_values` to an iceberg `Struct`. Length
/// must match the table's partition spec; we error rather than pad/truncate
/// to keep upstream bugs visible.
fn build_partition_struct(
    values: &[PartitionLiteral],
    expected_field_count: usize,
    file_path: &str,
) -> Result<Struct> {
    if values.len() != expected_field_count {
        return Err(IcebergError::Other(format!(
            "file {} carries {} partition values but the table's default spec has {}; \
             writer and catalog disagree on partition arity",
            file_path,
            values.len(),
            expected_field_count
        )));
    }
    if expected_field_count == 0 {
        return Ok(Struct::empty());
    }
    let lits: Vec<Option<Literal>> = values.iter().map(partition_literal_to_iceberg).collect();
    Ok(Struct::from_iter(lits))
}

/// Inverse of [`partition_literal_to_iceberg`]. Used by `snapshots()` to
/// surface partition values back through our `DataFile`.
fn iceberg_struct_to_partition_literals(s: &Struct) -> Vec<PartitionLiteral> {
    s.iter().map(iceberg_literal_to_partition).collect()
}

fn iceberg_literal_to_partition(lit: Option<&Literal>) -> PartitionLiteral {
    use pg2iceberg_core::partition::{f32_no_nan::F32, f64_no_nan::F64};
    let Some(Literal::Primitive(p)) = lit else {
        return PartitionLiteral::Null;
    };
    match p {
        PrimitiveLiteral::Boolean(b) => PartitionLiteral::Boolean(*b),
        PrimitiveLiteral::Int(n) => PartitionLiteral::Int(*n),
        PrimitiveLiteral::Long(n) => PartitionLiteral::Long(*n),
        PrimitiveLiteral::Float(f) => PartitionLiteral::Float(F32(f.0)),
        PrimitiveLiteral::Double(f) => PartitionLiteral::Double(F64(f.0)),
        PrimitiveLiteral::String(s) => PartitionLiteral::String(s.clone()),
        PrimitiveLiteral::Binary(b) => PartitionLiteral::Binary(b.clone()),
        // UUID partition values were translated as 16-byte BE; round-trip them
        // back as raw binary so the writer's identity-on-UUID-source stays
        // self-consistent.
        PrimitiveLiteral::UInt128(u) => PartitionLiteral::Binary(u.to_be_bytes().to_vec()),
        // Decimal partition values: iceberg stores the unscaled `i128`. We
        // can't recover scale from the literal alone (it lives in the
        // partition spec field type) so we report 0 — verifier filtering
        // works on unscaled equality, which is what iceberg actually
        // compares on. Operators displaying partition values for human
        // consumption should consult the schema for the scale.
        PrimitiveLiteral::Int128(n) => PartitionLiteral::Decimal {
            unscaled: *n,
            scale: 0,
        },
        // AboveMax / BelowMin: surface as Null rather than panic so reads
        // of foreign-written partition values don't crash the verifier.
        _ => PartitionLiteral::Null,
    }
}

fn partition_literal_to_iceberg(lit: &PartitionLiteral) -> Option<Literal> {
    use pg2iceberg_core::partition::{f32_no_nan::F32, f64_no_nan::F64};
    match lit {
        PartitionLiteral::Null => None,
        PartitionLiteral::Boolean(b) => Some(Literal::bool(*b)),
        PartitionLiteral::Int(n) => Some(Literal::int(*n)),
        PartitionLiteral::Long(n) => Some(Literal::long(*n)),
        PartitionLiteral::Float(F32(f)) => Some(Literal::float(*f)),
        PartitionLiteral::Double(F64(f)) => Some(Literal::double(*f)),
        PartitionLiteral::String(s) => Some(Literal::string(s)),
        // For now Binary covers raw bytes and UUID identity-partition values.
        // iceberg-rust's UUID literal stores `UInt128`; if that becomes a
        // real validation issue we'll add a `PartitionLiteral::Uuid` variant
        // and translate it here. Today's writer rejects UUID identity
        // partitioning before it reaches the catalog (see `apply_transform`
        // for `PgValue::Uuid`).
        PartitionLiteral::Binary(b) => {
            if b.len() == 16 {
                let arr: [u8; 16] = b.as_slice().try_into().expect("len-checked");
                Some(Literal::Primitive(PrimitiveLiteral::UInt128(
                    u128::from_be_bytes(arr),
                )))
            } else {
                Some(Literal::binary(b.clone()))
            }
        }
        PartitionLiteral::Decimal { unscaled, .. } => Some(Literal::decimal(*unscaled)),
    }
}

fn metadata_from_table(ident: &TableIdent, table: &iceberg::table::Table) -> Result<TableMetadata> {
    let part_spec = table.metadata().default_partition_spec();
    // `Table::properties()` is the polynya-patches addition that
    // surfaces the REST `loadTable` response config (vended creds,
    // table-scoped storage props). Empty for non-REST catalogs and
    // for catalogs that don't return per-table config — callers
    // distinguish "no creds vended" from "creds present" by checking
    // whether `s3.access-key-id` is in the map.
    let config: BTreeMap<String, String> = table
        .properties()
        .iter()
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();
    Ok(TableMetadata {
        ident: ident.clone(),
        schema: from_iceberg_schema(ident, table.metadata().current_schema(), part_spec.as_ref())?,
        // We surface `sequence_number` rather than the random 63-bit
        // `snapshot_id`, matching `Snapshot.id` in `snapshots()` so callers
        // get consistent monotonic IDs across both surfaces.
        current_snapshot_id: table
            .metadata()
            .current_snapshot()
            .map(|s| s.sequence_number()),
        config,
        location: table.metadata().location().to_string(),
        log_ends: table_log_ends(table),
        properties: table
            .metadata()
            .properties()
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect(),
    })
}

/// [`TableMetadata::log_ends`]: what the table's snapshots record.
fn table_log_ends(table: &iceberg::table::Table) -> BTreeMap<String, u64> {
    let mut ends = BTreeMap::new();
    for snap in table.metadata().snapshots() {
        let properties = &snap.summary().additional_properties;
        merge_log_ends(
            &mut ends,
            recorded_log_ends(|key| properties.get(key).map(String::as_str)),
        );
    }
    ends
}

/// The log range a snapshot's summary records, if any.
fn log_range_of(snap: &iceberg::spec::Snapshot) -> Option<LogRange> {
    let properties = &snap.summary().additional_properties;
    LogRange::from_properties(|key| properties.get(key).map(String::as_str))
}

fn map_iceberg_err(e: iceberg::Error) -> IcebergError {
    match e.kind() {
        ErrorKind::TableNotFound | ErrorKind::NamespaceNotFound => {
            IcebergError::NotFound(e.to_string())
        }
        ErrorKind::TableAlreadyExists
        | ErrorKind::NamespaceAlreadyExists
        | ErrorKind::CatalogCommitConflicts => IcebergError::Conflict(e.to_string()),
        _ => IcebergError::Other(e.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use iceberg::memory::{MemoryCatalogBuilder, MEMORY_CATALOG_WAREHOUSE};
    use iceberg::CatalogBuilder;
    use pg2iceberg_core::ColumnSchema;

    async fn fresh() -> IcebergRustCatalog<iceberg::memory::MemoryCatalog> {
        let inner = MemoryCatalogBuilder::default()
            .load(
                "test",
                HashMap::from([(
                    MEMORY_CATALOG_WAREHOUSE.to_string(),
                    "memory:///warehouse".to_string(),
                )]),
            )
            .await
            .unwrap();
        IcebergRustCatalog::new(Arc::new(inner))
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
                    ty: IcebergType::Long,
                    nullable: false,
                    is_primary_key: false,
                },
                ColumnSchema {
                    name: "note".into(),
                    field_id: 3,
                    ty: IcebergType::String,
                    nullable: true,
                    is_primary_key: false,
                },
            ],
            partition_spec: Vec::new(),
            pg_schema: None,
        }
    }

    /// `orders` schema with a `created_at` timestamp column, ready to
    /// be partitioned by day.
    fn schema_with_timestamp() -> TableSchema {
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
                    name: "created_at".into(),
                    field_id: 2,
                    ty: IcebergType::TimestampTz,
                    nullable: false,
                    is_primary_key: false,
                },
            ],
            partition_spec: Vec::new(),
            pg_schema: None,
        }
    }

    #[tokio::test]
    async fn create_table_with_identity_partition_spec_round_trips() {
        use pg2iceberg_core::Transform;
        let c = fresh().await;
        c.ensure_namespace(&ident().namespace).await.unwrap();
        let mut s = schema();
        s.partition_spec = vec![pg2iceberg_core::PartitionField {
            source_column: "qty".into(),
            name: "qty".into(),
            transform: Transform::Identity,
        }];
        let meta = c.create_table(&s).await.unwrap();
        assert_eq!(meta.schema.partition_spec.len(), 1);
        assert_eq!(meta.schema.partition_spec[0].source_column, "qty");
        assert_eq!(meta.schema.partition_spec[0].transform, Transform::Identity);

        // Reload via load_table — confirms the partition spec round-trips
        // through the Iceberg metadata read path.
        let reloaded = c.load_table(&ident()).await.unwrap().unwrap();
        assert_eq!(reloaded.schema.partition_spec, meta.schema.partition_spec);
    }

    #[tokio::test]
    async fn create_table_with_day_transform_round_trips() {
        use pg2iceberg_core::Transform;
        let c = fresh().await;
        c.ensure_namespace(&ident().namespace).await.unwrap();
        let mut s = schema_with_timestamp();
        s.partition_spec = vec![pg2iceberg_core::PartitionField {
            source_column: "created_at".into(),
            name: "created_at_day".into(),
            transform: Transform::Day,
        }];
        let meta = c.create_table(&s).await.unwrap();
        assert_eq!(meta.schema.partition_spec.len(), 1);
        assert_eq!(meta.schema.partition_spec[0].source_column, "created_at");
        assert_eq!(meta.schema.partition_spec[0].transform, Transform::Day);
        assert_eq!(meta.schema.partition_spec[0].name, "created_at_day");
    }

    #[tokio::test]
    async fn commit_to_partitioned_table_with_partition_values_round_trips() {
        use pg2iceberg_core::{PartitionLiteral, Transform};
        let c = fresh().await;
        c.ensure_namespace(&ident().namespace).await.unwrap();
        let mut s = schema();
        s.partition_spec = vec![pg2iceberg_core::PartitionField {
            source_column: "qty".into(),
            name: "qty".into(),
            transform: Transform::Identity,
        }];
        c.create_table(&s).await.unwrap();
        // Two distinct partition values land in the same snapshot as two
        // separate data files.
        let meta = c
            .commit_snapshot(PreparedCommit {
                ident: ident(),
                data_files: vec![
                    DataFile {
                        path: "memory:///warehouse/public/orders/data-qty-1.parquet".into(),
                        record_count: 3,
                        byte_size: 256,
                        equality_field_ids: vec![],
                        partition_values: vec![PartitionLiteral::Long(1)],
                        sequence_number: None,
                    },
                    DataFile {
                        path: "memory:///warehouse/public/orders/data-qty-2.parquet".into(),
                        record_count: 5,
                        byte_size: 384,
                        equality_field_ids: vec![],
                        partition_values: vec![PartitionLiteral::Long(2)],
                        sequence_number: None,
                    },
                ],
                equality_deletes: vec![],
            })
            .await
            .unwrap();
        assert!(meta.current_snapshot_id.is_some());
        let snaps = c.snapshots(&ident()).await.unwrap();
        assert_eq!(snaps.len(), 1);
        assert_eq!(snaps[0].data_files.len(), 2);
        let mut by_qty: std::collections::BTreeMap<i64, u64> = std::collections::BTreeMap::new();
        for df in &snaps[0].data_files {
            assert_eq!(df.partition_values.len(), 1);
            if let PartitionLiteral::Long(q) = &df.partition_values[0] {
                by_qty.insert(*q, df.record_count);
            } else {
                panic!(
                    "expected Long partition value, got {:?}",
                    df.partition_values[0]
                );
            }
        }
        assert_eq!(by_qty.get(&1), Some(&3));
        assert_eq!(by_qty.get(&2), Some(&5));
    }

    #[tokio::test]
    async fn writer_to_catalog_roundtrips_partition_values_for_identity_partitioned_table() {
        use crate::TableWriter;
        use pg2iceberg_core::value::PgValue;
        use pg2iceberg_core::{ColumnName, Op, PartitionLiteral, Transform};
        use std::collections::BTreeMap;

        let c = fresh().await;
        c.ensure_namespace(&ident().namespace).await.unwrap();
        let mut s = schema();
        // Partition by `note` (string identity). Force `note` non-nullable
        // for this test so the writer doesn't have to handle null partitions.
        s.columns[2].nullable = false;
        s.partition_spec = vec![pg2iceberg_core::PartitionField {
            source_column: "note".into(),
            name: "note".into(),
            transform: Transform::Identity,
        }];
        c.create_table(&s).await.unwrap();

        // Prepare via TableWriter: 2 inserts into "us", 1 into "eu" → two
        // data chunks tagged with the right partition tuples.
        let w = TableWriter::new(s.clone());
        let mut row_a = BTreeMap::new();
        row_a.insert(ColumnName("id".into()), PgValue::Int4(1));
        row_a.insert(ColumnName("qty".into()), PgValue::Int8(10));
        row_a.insert(ColumnName("note".into()), PgValue::Text("us".into()));
        let mut row_b = BTreeMap::new();
        row_b.insert(ColumnName("id".into()), PgValue::Int4(2));
        row_b.insert(ColumnName("qty".into()), PgValue::Int8(20));
        row_b.insert(ColumnName("note".into()), PgValue::Text("us".into()));
        let mut row_c = BTreeMap::new();
        row_c.insert(ColumnName("id".into()), PgValue::Int4(3));
        row_c.insert(ColumnName("qty".into()), PgValue::Int8(30));
        row_c.insert(ColumnName("note".into()), PgValue::Text("eu".into()));
        let prepared = w
            .prepare(
                &[
                    crate::MaterializedRow {
                        op: Op::Insert,
                        row: row_a,
                        unchanged_cols: vec![],
                        unchanged_from: None,
                    },
                    crate::MaterializedRow {
                        op: Op::Insert,
                        row: row_b,
                        unchanged_cols: vec![],
                        unchanged_from: None,
                    },
                    crate::MaterializedRow {
                        op: Op::Insert,
                        row: row_c,
                        unchanged_cols: vec![],
                        unchanged_from: None,
                    },
                ],
                &crate::FileIndex::new(),
            )
            .unwrap();
        assert_eq!(prepared.data.len(), 2);

        // Commit each chunk as a separate DataFile carrying its
        // partition_values. This is the same shape the materializer
        // produces.
        let mut data_files: Vec<DataFile> = Vec::new();
        for (i, chunk) in prepared.data.into_iter().enumerate() {
            data_files.push(DataFile {
                path: format!("memory:///warehouse/public/orders/data-{i}.parquet"),
                record_count: chunk.chunk.record_count,
                byte_size: chunk.chunk.bytes.len() as u64,
                equality_field_ids: vec![],
                partition_values: chunk.partition_values,
                sequence_number: None,
            });
        }
        c.commit_snapshot(PreparedCommit {
            ident: ident(),
            data_files,
            equality_deletes: vec![],
        })
        .await
        .unwrap();

        // Read back: snapshots() must surface the same partition values we
        // wrote. This also covers `iceberg_struct_to_partition_literals`.
        let snaps = c.snapshots(&ident()).await.unwrap();
        assert_eq!(snaps.len(), 1);
        assert_eq!(snaps[0].data_files.len(), 2);
        let mut by_region: BTreeMap<String, u64> = BTreeMap::new();
        for df in &snaps[0].data_files {
            assert_eq!(df.partition_values.len(), 1);
            if let PartitionLiteral::String(r) = &df.partition_values[0] {
                by_region.insert(r.clone(), df.record_count);
            } else {
                panic!(
                    "expected String partition value, got {:?}",
                    df.partition_values[0]
                );
            }
        }
        assert_eq!(by_region.get("us"), Some(&2));
        assert_eq!(by_region.get("eu"), Some(&1));
    }

    #[tokio::test]
    async fn commit_to_partitioned_table_with_arity_mismatch_errors() {
        use pg2iceberg_core::Transform;
        let c = fresh().await;
        c.ensure_namespace(&ident().namespace).await.unwrap();
        let mut s = schema();
        s.partition_spec = vec![pg2iceberg_core::PartitionField {
            source_column: "qty".into(),
            name: "qty".into(),
            transform: Transform::Identity,
        }];
        c.create_table(&s).await.unwrap();
        // Empty partition_values for a partitioned table should error with a
        // clear arity mismatch — not silently land at `Struct::empty()`.
        let err = c
            .commit_snapshot(PreparedCommit {
                ident: ident(),
                data_files: vec![DataFile {
                    path: "memory:///warehouse/public/orders/data-bogus.parquet".into(),
                    record_count: 1,
                    byte_size: 64,
                    equality_field_ids: vec![],
                    partition_values: Vec::new(),
                    sequence_number: None,
                }],
                equality_deletes: vec![],
            })
            .await
            .unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("partition arity") || msg.contains("partition values"),
            "expected arity-mismatch error, got: {msg}"
        );
    }

    #[tokio::test]
    async fn ensure_namespace_is_idempotent() {
        let c = fresh().await;
        c.ensure_namespace(&ident().namespace).await.unwrap();
        c.ensure_namespace(&ident().namespace).await.unwrap();
    }

    #[tokio::test]
    async fn create_then_load_round_trips_schema() {
        let c = fresh().await;
        c.ensure_namespace(&ident().namespace).await.unwrap();
        let meta = c.create_table(&schema()).await.unwrap();
        // The translated schema should preserve our field ids, types,
        // nullability and PK marking.
        assert_eq!(meta.ident, ident());
        assert_eq!(meta.schema.columns.len(), 3);
        let id = &meta.schema.columns[0];
        assert_eq!(id.field_id, 1);
        assert_eq!(id.ty, IcebergType::Int);
        assert!(id.is_primary_key);
        assert!(!id.nullable);
        let note = &meta.schema.columns[2];
        assert!(note.nullable);
        assert!(!note.is_primary_key);

        let loaded = c.load_table(&ident()).await.unwrap().unwrap();
        assert_eq!(loaded.schema, meta.schema);
        assert!(loaded.current_snapshot_id.is_none());
    }

    #[tokio::test]
    async fn load_table_returns_none_when_missing() {
        let c = fresh().await;
        let got = c.load_table(&ident()).await.unwrap();
        assert!(got.is_none());
    }

    #[tokio::test]
    async fn commit_snapshot_appends_data_file_and_snapshot_history_grows() {
        let c = fresh().await;
        c.ensure_namespace(&ident().namespace).await.unwrap();
        c.create_table(&schema()).await.unwrap();

        for i in 0..3 {
            let meta = c
                .commit_snapshot(PreparedCommit {
                    ident: ident(),
                    data_files: vec![DataFile {
                        path: format!("memory:///warehouse/public/orders/data-{i}.parquet"),
                        record_count: 10 + i,
                        byte_size: 1024 + i * 100,
                        equality_field_ids: vec![],
                        partition_values: Vec::new(),
                        sequence_number: None,
                    }],
                    equality_deletes: vec![],
                })
                .await
                .unwrap();
            assert!(meta.current_snapshot_id.is_some());
        }

        let snaps = c.snapshots(&ident()).await.unwrap();
        assert_eq!(snaps.len(), 3);
        // Snapshots are returned in ascending id order.
        for w in snaps.windows(2) {
            assert!(w[0].id < w[1].id);
        }
        assert_eq!(snaps[0].data_files.len(), 1);
        assert_eq!(snaps[0].data_files[0].record_count, 10);
        assert_eq!(snaps[2].data_files[0].record_count, 12);
        for s in &snaps {
            assert!(s.delete_files.is_empty());
        }
    }

    /// Compaction round-trip: append three small data files via
    /// `commit_snapshot`, then `commit_compaction` swaps them for one
    /// compacted file. Subsequent `snapshots()` returns four snapshots
    /// total; the compaction one is `Operation::Replace` and carries
    /// `removed_paths` for the three originals. Verifier-style readers
    /// (and `compute_live_files`) see only the compacted file.
    #[tokio::test]
    async fn commit_compaction_swaps_files_atomically() {
        use crate::PreparedCompaction;
        let c = fresh().await;
        c.ensure_namespace(&ident().namespace).await.unwrap();
        c.create_table(&schema()).await.unwrap();

        // Append three data files in three separate snapshots.
        let mut original_paths: Vec<String> = Vec::new();
        for i in 0..3 {
            let path = format!("memory:///warehouse/public/orders/data-{i}.parquet");
            original_paths.push(path.clone());
            c.commit_snapshot(PreparedCommit {
                ident: ident(),
                data_files: vec![DataFile {
                    path,
                    record_count: 1,
                    byte_size: 100,
                    equality_field_ids: vec![],
                    partition_values: Vec::new(),
                    sequence_number: None,
                }],
                equality_deletes: vec![],
            })
            .await
            .unwrap();
        }

        // Compact: drop all three, add a single compacted file.
        let compacted_path = "memory:///warehouse/public/orders/data-compact-0.parquet";
        c.commit_compaction(PreparedCompaction {
            ident: ident(),
            added_data_files: vec![DataFile {
                path: compacted_path.into(),
                record_count: 3,
                byte_size: 256,
                equality_field_ids: vec![],
                partition_values: Vec::new(),
                sequence_number: None,
            }],
            removed_paths: original_paths.clone(),
            data_sequence_number: None,
        })
        .await
        .unwrap();

        let snaps = c.snapshots(&ident()).await.unwrap();
        assert_eq!(snaps.len(), 4, "3 appends + 1 compaction");
        // The compaction is the most recent snapshot. Its
        // `removed_paths` should list all three original files; its
        // `data_files` should contain only the compacted output.
        let compaction = snaps.last().unwrap();
        assert_eq!(compaction.data_files.len(), 1);
        assert_eq!(compaction.data_files[0].path, compacted_path);
        let mut got_removed = compaction.removed_paths.clone();
        got_removed.sort();
        let mut want_removed = original_paths.clone();
        want_removed.sort();
        assert_eq!(got_removed, want_removed);

        // Older snapshots' data_files are still present in the per-snapshot
        // delta view (this is the "files added in snap N" semantic). The
        // verifier and `compute_live_files` filter them out via the
        // cumulative `removed_paths` set.
        for snap in &snaps[..3] {
            assert_eq!(snap.data_files.len(), 1);
            assert!(snap.removed_paths.is_empty());
        }
    }

    /// A no-op compaction (nothing added, nothing removed) should not
    /// produce a snapshot bump.
    #[tokio::test]
    async fn commit_compaction_with_empty_input_is_noop() {
        use crate::PreparedCompaction;
        let c = fresh().await;
        c.ensure_namespace(&ident().namespace).await.unwrap();
        c.create_table(&schema()).await.unwrap();
        let meta = c
            .commit_compaction(PreparedCompaction {
                ident: ident(),
                added_data_files: vec![],
                removed_paths: vec![],
                data_sequence_number: None,
            })
            .await
            .unwrap();
        assert!(meta.current_snapshot_id.is_none());
    }

    /// A pass whose inputs' rows were all deleted removes them and adds
    /// nothing.
    #[tokio::test]
    async fn commit_compaction_with_no_outputs_removes_its_inputs() {
        use crate::PreparedCompaction;
        let c = fresh().await;
        c.ensure_namespace(&ident().namespace).await.unwrap();
        c.create_table(&schema()).await.unwrap();
        let path = "memory:///warehouse/public/orders/data-0.parquet";
        c.commit_snapshot(PreparedCommit {
            ident: ident(),
            data_files: vec![DataFile {
                path: path.into(),
                record_count: 1,
                byte_size: 100,
                equality_field_ids: vec![],
                partition_values: Vec::new(),
                sequence_number: None,
            }],
            equality_deletes: vec![],
        })
        .await
        .unwrap();

        c.commit_compaction(PreparedCompaction {
            ident: ident(),
            added_data_files: vec![],
            removed_paths: vec![path.into()],
            data_sequence_number: None,
        })
        .await
        .unwrap();

        let snaps = c.snapshots(&ident()).await.unwrap();
        assert_eq!(snaps.len(), 2);
        assert!(snaps[1].data_files.is_empty());
        assert_eq!(snaps[1].removed_paths, vec![path.to_string()]);
    }

    /// Snapshot expiry round-trip via real iceberg-rust: append several
    /// snapshots, expire the older ones with a small retention window,
    /// reload table — the surviving snapshot history matches what we
    /// asked for, and the current snapshot's manifest list still
    /// references all live files (so verifier-style readers continue
    /// to see the data).
    #[tokio::test]
    async fn expire_snapshots_drops_old_keeps_current_and_preserves_visibility() {
        let c = fresh().await;
        c.ensure_namespace(&ident().namespace).await.unwrap();
        c.create_table(&schema()).await.unwrap();

        // Three appends. iceberg-rust assigns wall-clock millis to each
        // snapshot via `chrono::Utc::now()`; in CI the three timestamps
        // can collide on fast machines. We sleep a millisecond between
        // commits so retention math has something to bite on.
        for i in 0..3 {
            c.commit_snapshot(PreparedCommit {
                ident: ident(),
                data_files: vec![DataFile {
                    path: format!("memory:///warehouse/public/orders/data-{i}.parquet"),
                    record_count: 1,
                    byte_size: 100,
                    equality_field_ids: vec![],
                    partition_values: Vec::new(),
                    sequence_number: None,
                }],
                equality_deletes: vec![],
            })
            .await
            .unwrap();
            tokio::time::sleep(std::time::Duration::from_millis(2)).await;
        }

        let pre = c.snapshots(&ident()).await.unwrap();
        assert_eq!(pre.len(), 3);

        // Retention 1ms — everything except the current (latest) snapshot
        // is older than that and should expire.
        let n = c.expire_snapshots(&ident(), 1).await.unwrap();
        assert_eq!(n, 2, "two old snapshots should expire");

        let post = c.snapshots(&ident()).await.unwrap();
        // The expired snapshots' metadata is gone, but the files they
        // added are still the table: reported under stand-ins at their
        // own sequence numbers, beside the current snapshot (3).
        assert_eq!(live_files(&post), live_files(&pre));
        assert_eq!(post.last().unwrap().id, 3);
        assert!(post[..post.len() - 1].iter().all(|s| s.timestamp_ms == 0));
    }

    /// Every live data / delete file and the sequence number it applies
    /// at, as a reader replaying `snapshots()` sees them.
    fn live_files(snaps: &[Snapshot]) -> BTreeMap<String, i64> {
        let removed: std::collections::BTreeSet<&str> = snaps
            .iter()
            .flat_map(|s| s.removed_paths.iter().map(String::as_str))
            .collect();
        snaps
            .iter()
            .flat_map(|s| {
                s.data_files
                    .iter()
                    .chain(&s.delete_files)
                    .map(move |f| (f.path.clone(), s.id))
            })
            .filter(|(p, _)| !removed.contains(p.as_str()))
            .collect()
    }

    /// Expiry must not change what `snapshots()` replays to — not for
    /// files the expired snapshots added, not for their deletes, and not
    /// across a compaction — or the FileIndex rebuild, compaction and
    /// orphan cleanup all work from a table that's missing files.
    #[tokio::test]
    async fn expiry_keeps_live_files_at_their_sequence_numbers() {
        use crate::PreparedCompaction;
        let c = fresh().await;
        c.ensure_namespace(&ident().namespace).await.unwrap();
        c.create_table(&schema()).await.unwrap();
        let file = |name: &str, eq_ids: Vec<i32>| DataFile {
            path: format!("memory:///warehouse/public/orders/{name}.parquet"),
            record_count: 1,
            byte_size: 100,
            equality_field_ids: eq_ids,
            partition_values: Vec::new(),
            sequence_number: None,
        };
        let tick = || tokio::time::sleep(std::time::Duration::from_millis(2));
        let append = |data: Vec<DataFile>, deletes: Vec<DataFile>| {
            c.commit_snapshot(PreparedCommit {
                ident: ident(),
                data_files: data,
                equality_deletes: deletes,
            })
        };
        append(vec![file("d0", vec![])], vec![]).await.unwrap();
        tick().await;
        append(vec![file("d1", vec![])], vec![file("e1", vec![1])])
            .await
            .unwrap();
        tick().await;
        append(vec![file("d2", vec![])], vec![]).await.unwrap();
        tick().await;
        c.commit_compaction(PreparedCompaction {
            ident: ident(),
            added_data_files: vec![file("c0", vec![])],
            removed_paths: vec![file("d0", vec![]).path],
            data_sequence_number: None,
        })
        .await
        .unwrap();
        tick().await;
        append(vec![file("d3", vec![])], vec![]).await.unwrap();

        let pre = live_files(&c.snapshots(&ident()).await.unwrap());
        let path = |n: &str| file(n, vec![]).path;
        assert_eq!(
            pre,
            BTreeMap::from([
                (path("d1"), 2),
                (path("e1"), 2),
                (path("d2"), 3),
                (path("c0"), 4),
                (path("d3"), 5),
            ])
        );
        assert_eq!(c.expire_snapshots(&ident(), 1).await.unwrap(), 4);
        assert_eq!(live_files(&c.snapshots(&ident()).await.unwrap()), pre);
    }

    /// Retention so high nothing expires.
    #[tokio::test]
    async fn expire_snapshots_with_huge_retention_is_noop() {
        let c = fresh().await;
        c.ensure_namespace(&ident().namespace).await.unwrap();
        c.create_table(&schema()).await.unwrap();
        c.commit_snapshot(PreparedCommit {
            ident: ident(),
            data_files: vec![DataFile {
                path: "memory:///warehouse/public/orders/data-0.parquet".into(),
                record_count: 1,
                byte_size: 100,
                equality_field_ids: vec![],
                partition_values: Vec::new(),
                sequence_number: None,
            }],
            equality_deletes: vec![],
        })
        .await
        .unwrap();
        // Retention = 1 day, well beyond what we just committed.
        let n = c.expire_snapshots(&ident(), 86_400_000).await.unwrap();
        assert_eq!(n, 0);
    }

    #[tokio::test]
    async fn empty_prepared_commit_is_a_noop() {
        let c = fresh().await;
        c.ensure_namespace(&ident().namespace).await.unwrap();
        c.create_table(&schema()).await.unwrap();
        let meta = c
            .commit_snapshot(PreparedCommit {
                ident: ident(),
                data_files: vec![],
                equality_deletes: vec![],
            })
            .await
            .unwrap();
        assert!(meta.current_snapshot_id.is_none());
        assert!(c.snapshots(&ident()).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn commit_snapshot_with_only_equality_deletes_produces_a_snapshot() {
        // After the fork patch, equality-delete commits flow through the same
        // FastAppendAction path as data commits. A delete-only commit should
        // still produce a snapshot whose delete_files list is non-empty.
        let c = fresh().await;
        c.ensure_namespace(&ident().namespace).await.unwrap();
        c.create_table(&schema()).await.unwrap();
        let meta = c
            .commit_snapshot(PreparedCommit {
                ident: ident(),
                data_files: vec![],
                equality_deletes: vec![DataFile {
                    path: "memory:///warehouse/public/orders/eq-deletes-0.parquet".into(),
                    record_count: 1,
                    byte_size: 64,
                    equality_field_ids: vec![1],
                    partition_values: Vec::new(),
                    sequence_number: None,
                }],
            })
            .await
            .unwrap();
        assert!(meta.current_snapshot_id.is_some());

        let snaps = c.snapshots(&ident()).await.unwrap();
        assert_eq!(snaps.len(), 1);
        assert!(snaps[0].data_files.is_empty());
        assert_eq!(snaps[0].delete_files.len(), 1);
        assert_eq!(snaps[0].delete_files[0].equality_field_ids, vec![1]);
    }

    #[tokio::test]
    async fn commit_snapshot_with_data_plus_equality_deletes_lands_in_one_snapshot() {
        let c = fresh().await;
        c.ensure_namespace(&ident().namespace).await.unwrap();
        c.create_table(&schema()).await.unwrap();
        let meta = c
            .commit_snapshot(PreparedCommit {
                ident: ident(),
                data_files: vec![DataFile {
                    path: "memory:///warehouse/public/orders/data-0.parquet".into(),
                    record_count: 5,
                    byte_size: 1024,
                    equality_field_ids: vec![],
                    partition_values: Vec::new(),
                    sequence_number: None,
                }],
                equality_deletes: vec![DataFile {
                    path: "memory:///warehouse/public/orders/eq-deletes-0.parquet".into(),
                    record_count: 2,
                    byte_size: 128,
                    equality_field_ids: vec![1],
                    partition_values: Vec::new(),
                    sequence_number: None,
                }],
            })
            .await
            .unwrap();
        assert!(meta.current_snapshot_id.is_some());

        let snaps = c.snapshots(&ident()).await.unwrap();
        // Both files belong to the same snapshot — not two.
        assert_eq!(snaps.len(), 1);
        assert_eq!(snaps[0].data_files.len(), 1);
        assert_eq!(snaps[0].delete_files.len(), 1);
        assert_eq!(snaps[0].data_files[0].record_count, 5);
        assert_eq!(snaps[0].delete_files[0].record_count, 2);
    }

    #[tokio::test]
    async fn commit_snapshot_rejects_delete_file_with_empty_equality_field_ids() {
        // An equality-delete file with no field-id list would match no rows
        // (or every row, depending on reader). Refuse rather than silently
        // commit a meaningless delete.
        let c = fresh().await;
        c.ensure_namespace(&ident().namespace).await.unwrap();
        c.create_table(&schema()).await.unwrap();
        let err = c
            .commit_snapshot(PreparedCommit {
                ident: ident(),
                data_files: vec![],
                equality_deletes: vec![DataFile {
                    path: "memory:///warehouse/public/orders/eq-deletes-bad.parquet".into(),
                    record_count: 1,
                    byte_size: 64,
                    equality_field_ids: vec![],
                    partition_values: Vec::new(),
                    sequence_number: None,
                }],
            })
            .await
            .unwrap_err();
        assert!(matches!(err, IcebergError::Other(_)));
        assert!(err.to_string().contains("empty equality_field_ids"));
    }

    #[tokio::test]
    async fn snapshots_on_missing_table_returns_empty() {
        let c = fresh().await;
        let snaps = c.snapshots(&ident()).await.unwrap();
        assert!(snaps.is_empty());
    }

    /// In-memory storage that records the path of every read.
    #[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
    struct CountingStorage {
        #[serde(skip)]
        inner: iceberg::io::MemoryStorage,
        #[serde(skip)]
        reads: Arc<std::sync::Mutex<Vec<String>>>,
    }

    impl CountingStorage {
        /// The paths read since the last call.
        fn take_reads(&self) -> Vec<String> {
            std::mem::take(&mut *self.reads.lock().unwrap())
        }
    }

    #[async_trait]
    #[typetag::serde]
    impl iceberg::io::Storage for CountingStorage {
        async fn exists(&self, path: &str) -> iceberg::Result<bool> {
            self.inner.exists(path).await
        }

        async fn metadata(&self, path: &str) -> iceberg::Result<iceberg::io::FileMetadata> {
            self.inner.metadata(path).await
        }

        async fn read(&self, path: &str) -> iceberg::Result<bytes::Bytes> {
            self.reads.lock().unwrap().push(path.to_string());
            self.inner.read(path).await
        }

        async fn reader(&self, path: &str) -> iceberg::Result<Box<dyn iceberg::io::FileRead>> {
            self.reads.lock().unwrap().push(path.to_string());
            self.inner.reader(path).await
        }

        async fn write(&self, path: &str, bs: bytes::Bytes) -> iceberg::Result<()> {
            self.inner.write(path, bs).await
        }

        async fn writer(&self, path: &str) -> iceberg::Result<Box<dyn iceberg::io::FileWrite>> {
            self.inner.writer(path).await
        }

        async fn delete(&self, path: &str) -> iceberg::Result<()> {
            self.inner.delete(path).await
        }

        async fn delete_prefix(&self, path: &str) -> iceberg::Result<()> {
            self.inner.delete_prefix(path).await
        }

        async fn delete_stream(
            &self,
            paths: futures::stream::BoxStream<'static, String>,
        ) -> iceberg::Result<()> {
            self.inner.delete_stream(paths).await
        }

        fn new_input(&self, path: &str) -> iceberg::Result<iceberg::io::InputFile> {
            Ok(iceberg::io::InputFile::new(
                Arc::new(self.clone()),
                path.to_string(),
            ))
        }

        fn new_output(&self, path: &str) -> iceberg::Result<iceberg::io::OutputFile> {
            Ok(iceberg::io::OutputFile::new(
                Arc::new(self.clone()),
                path.to_string(),
            ))
        }
    }

    #[derive(Debug, serde::Serialize, serde::Deserialize)]
    struct CountingStorageFactory {
        #[serde(skip)]
        storage: CountingStorage,
    }

    #[typetag::serde]
    impl iceberg::io::StorageFactory for CountingStorageFactory {
        fn build(
            &self,
            _config: &iceberg::io::StorageConfig,
        ) -> iceberg::Result<Arc<dyn iceberg::io::Storage>> {
            Ok(Arc::new(self.storage.clone()))
        }
    }

    /// A memory catalog whose files are in `storage`.
    async fn fresh_on(
        storage: CountingStorage,
    ) -> IcebergRustCatalog<iceberg::memory::MemoryCatalog> {
        let inner = MemoryCatalogBuilder::default()
            .with_storage_factory(Arc::new(CountingStorageFactory { storage }))
            .load(
                "test",
                HashMap::from([(
                    MEMORY_CATALOG_WAREHOUSE.to_string(),
                    "memory:///warehouse".to_string(),
                )]),
            )
            .await
            .unwrap();
        IcebergRustCatalog::new(Arc::new(inner))
    }

    async fn append(c: &IcebergRustCatalog<iceberg::memory::MemoryCatalog>, i: usize) {
        c.commit_snapshot(PreparedCommit {
            ident: ident(),
            data_files: vec![DataFile {
                path: format!("memory:///warehouse/public/orders/data-{i}.parquet"),
                record_count: 1,
                byte_size: 100,
                equality_field_ids: vec![],
                partition_values: Vec::new(),
                sequence_number: None,
            }],
            equality_deletes: vec![],
        })
        .await
        .unwrap();
    }

    /// Manifest lists and manifests never change once written: each
    /// commit writes new ones, its manifest list naming every manifest
    /// before it. Reading the history again reads only what's new since —
    /// re-reading the rest costs a round trip each to object storage, on
    /// every compaction check of every table, and grows with every commit.
    #[tokio::test]
    async fn snapshots_reads_each_manifest_file_once() {
        let storage = CountingStorage::default();
        let c = fresh_on(storage.clone()).await;
        c.ensure_namespace(&ident().namespace).await.unwrap();
        c.create_table(&schema()).await.unwrap();
        for i in 0..4 {
            append(&c, i).await;
        }
        let avro = |paths: Vec<String>| -> Vec<String> {
            paths.into_iter().filter(|p| p.ends_with(".avro")).collect()
        };

        storage.take_reads();
        let first = c.snapshots(&ident()).await.unwrap();
        let first_reads = avro(storage.take_reads());
        assert_eq!(first.len(), 4);
        // Four manifest lists and four manifests, though the lists name
        // ten between them.
        assert_eq!(first_reads.len(), 8, "{first_reads:?}");

        append(&c, 4).await;
        storage.take_reads();
        let second = c.snapshots(&ident()).await.unwrap();
        let second_reads = avro(storage.take_reads());
        assert_eq!(second.len(), 5);
        assert_eq!(live_files(&second).len(), 5);

        let again: Vec<&String> = second_reads
            .iter()
            .filter(|p| first_reads.contains(p))
            .collect();
        assert!(again.is_empty(), "read again: {again:?}");
        // The new snapshot's manifest list and its one new manifest.
        assert_eq!(second_reads.len(), 2, "{second_reads:?}");
    }

    #[tokio::test]
    async fn evolve_schema_add_column_appends_to_schema_with_fresh_field_id() {
        let c = fresh().await;
        c.ensure_namespace(&ident().namespace).await.unwrap();
        c.create_table(&schema()).await.unwrap();
        let meta = c
            .evolve_schema(
                &ident(),
                vec![SchemaChange::AddColumn {
                    name: "new_col".into(),
                    ty: IcebergType::String,
                    nullable: true,
                }],
                BTreeMap::new(),
            )
            .await
            .unwrap();
        assert_eq!(meta.schema.columns.len(), 4);
        let new_col = meta
            .schema
            .columns
            .iter()
            .find(|c| c.name == "new_col")
            .expect("new_col must be in schema after evolve");
        // Original schema had ids 1,2,3 — the new column should get 4.
        assert_eq!(new_col.field_id, 4);
        assert!(new_col.nullable);
        assert!(!new_col.is_primary_key);

        // Re-loading via load_table sees the same evolved schema.
        let reloaded = c.load_table(&ident()).await.unwrap().unwrap();
        assert_eq!(reloaded.schema, meta.schema);
    }

    #[tokio::test]
    async fn evolve_schema_drop_column_is_soft_drop_makes_column_nullable() {
        let c = fresh().await;
        c.ensure_namespace(&ident().namespace).await.unwrap();
        c.create_table(&schema()).await.unwrap();
        // `qty` (column 2) is non-nullable in the test schema.
        let pre = c.load_table(&ident()).await.unwrap().unwrap();
        assert!(!pre.schema.columns[1].nullable);

        let meta = c
            .evolve_schema(
                &ident(),
                vec![SchemaChange::DropColumn { name: "qty".into() }],
                BTreeMap::new(),
            )
            .await
            .unwrap();
        // Column count unchanged — soft-drop preserves it.
        assert_eq!(meta.schema.columns.len(), 3);
        let qty = meta
            .schema
            .columns
            .iter()
            .find(|c| c.name == "qty")
            .unwrap();
        assert!(qty.nullable, "soft-drop should mark column nullable");
        // field_id is preserved across the evolve.
        assert_eq!(qty.field_id, 2);
    }

    #[tokio::test]
    async fn evolve_schema_empty_changes_is_a_noop() {
        let c = fresh().await;
        c.ensure_namespace(&ident().namespace).await.unwrap();
        let original = c.create_table(&schema()).await.unwrap();
        let after = c
            .evolve_schema(&ident(), vec![], BTreeMap::new())
            .await
            .unwrap();
        assert_eq!(original.schema, after.schema);
    }

    #[tokio::test]
    async fn evolve_schema_add_existing_column_errors() {
        let c = fresh().await;
        c.ensure_namespace(&ident().namespace).await.unwrap();
        c.create_table(&schema()).await.unwrap();
        let err = c
            .evolve_schema(
                &ident(),
                vec![SchemaChange::AddColumn {
                    name: "qty".into(),
                    ty: IcebergType::Long,
                    nullable: true,
                }],
                BTreeMap::new(),
            )
            .await
            .unwrap_err();
        assert!(matches!(err, IcebergError::Conflict(_)));
    }

    #[tokio::test]
    async fn evolve_schema_drop_unknown_column_errors() {
        let c = fresh().await;
        c.ensure_namespace(&ident().namespace).await.unwrap();
        c.create_table(&schema()).await.unwrap();
        let err = c
            .evolve_schema(
                &ident(),
                vec![SchemaChange::DropColumn {
                    name: "ghost".into(),
                }],
                BTreeMap::new(),
            )
            .await
            .unwrap_err();
        assert!(matches!(err, IcebergError::NotFound(_)));
    }

    #[tokio::test]
    async fn evolve_schema_then_commit_snapshot_uses_new_schema_id() {
        // After an evolve, subsequent commits should target the new schema
        // version. We can't observe the schema id directly through our
        // metadata surface (we only carry sequence_number for current_snapshot_id),
        // but we can confirm the round-trip stays self-consistent: evolve,
        // commit a data file, reload, and assert the evolved column is
        // still present.
        let c = fresh().await;
        c.ensure_namespace(&ident().namespace).await.unwrap();
        c.create_table(&schema()).await.unwrap();
        c.evolve_schema(
            &ident(),
            vec![SchemaChange::AddColumn {
                name: "added".into(),
                ty: IcebergType::Int,
                nullable: true,
            }],
            BTreeMap::new(),
        )
        .await
        .unwrap();
        c.commit_snapshot(PreparedCommit {
            ident: ident(),
            data_files: vec![DataFile {
                path: "memory:///warehouse/public/orders/data-0.parquet".into(),
                record_count: 1,
                byte_size: 256,
                equality_field_ids: vec![],
                partition_values: Vec::new(),
                sequence_number: None,
            }],
            equality_deletes: vec![],
        })
        .await
        .unwrap();

        let reloaded = c.load_table(&ident()).await.unwrap().unwrap();
        assert!(reloaded.schema.columns.iter().any(|c| c.name == "added"));
        assert!(reloaded.current_snapshot_id.is_some());
    }

    #[tokio::test]
    async fn properties_set_with_a_schema_change_and_removed_with_a_commit() {
        let c = fresh().await;
        c.ensure_namespace(&ident().namespace).await.unwrap();
        c.create_table(&schema()).await.unwrap();
        let set = BTreeMap::from([
            ("pg2iceberg.a".to_string(), "1".to_string()),
            ("pg2iceberg.b".to_string(), "2".to_string()),
        ]);
        let meta = c
            .evolve_schema(
                &ident(),
                vec![SchemaChange::AddColumn {
                    name: "added".into(),
                    ty: IcebergType::Int,
                    nullable: true,
                }],
                set,
            )
            .await
            .unwrap();
        assert!(meta.schema.columns.iter().any(|c| c.name == "added"));
        assert_eq!(meta.properties.get("pg2iceberg.a").unwrap(), "1");

        let step = |n: usize| PreparedCommit {
            ident: ident(),
            data_files: vec![DataFile {
                path: format!("memory:///warehouse/public/orders/data-{n}.parquet"),
                record_count: 1,
                byte_size: 256,
                equality_field_ids: vec![],
                partition_values: Vec::new(),
                sequence_number: None,
            }],
            equality_deletes: vec![],
        };
        // On a single-step commit, and a chained one.
        let meta = c
            .commit_snapshots(vec![step(0)], None, BTreeSet::from(["pg2iceberg.a".into()]))
            .await
            .unwrap();
        assert!(!meta.properties.contains_key("pg2iceberg.a"));
        assert_eq!(meta.properties.get("pg2iceberg.b").unwrap(), "2");
        let meta = c
            .commit_snapshots(
                vec![step(1), step(2)],
                None,
                BTreeSet::from(["pg2iceberg.b".into()]),
            )
            .await
            .unwrap();
        assert!(!meta.properties.contains_key("pg2iceberg.b"));
        let reloaded = c.load_table(&ident()).await.unwrap().unwrap();
        assert_eq!(reloaded.properties, meta.properties);
        assert_eq!(reloaded.current_snapshot_id, Some(3));
    }

    #[tokio::test]
    async fn nested_namespace_ensure_works() {
        let c = fresh().await;
        let ns = Namespace(vec!["root".into(), "child".into()]);
        // Iceberg's MemoryCatalog needs the parent first.
        c.ensure_namespace(&Namespace(vec!["root".into()]))
            .await
            .unwrap();
        c.ensure_namespace(&ns).await.unwrap();
        c.ensure_namespace(&ns).await.unwrap();
    }

    #[test]
    fn type_round_trip_covers_full_postgres_subset() {
        for ty in [
            IcebergType::Boolean,
            IcebergType::Int,
            IcebergType::Long,
            IcebergType::Float,
            IcebergType::Double,
            IcebergType::Decimal {
                precision: 10,
                scale: 2,
            },
            IcebergType::String,
            IcebergType::Binary,
            IcebergType::Date,
            IcebergType::Time,
            IcebergType::Timestamp,
            IcebergType::TimestampTz,
            IcebergType::Uuid,
        ] {
            let back = from_iceberg_type(&to_iceberg_type(ty)).unwrap();
            assert_eq!(back, ty);
        }
    }
}
