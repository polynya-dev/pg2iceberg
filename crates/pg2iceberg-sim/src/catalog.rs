//! In-memory `Catalog` impl for tests and the DST harness.
//!
//! Stores append-only snapshot history per table. `commit_snapshot` allocates
//! a monotonic snapshot id (= `prev_id + 1`); the verifier uses these ids to
//! apply Iceberg MoR semantics (delete file at snap N applies only to data
//! files at snap < N).
//!
//! Expiring a snapshot drops its metadata, as in Iceberg, but not the table
//! state: files it added stay live until a later snapshot removes them.
//! [`ReaderView`] is that state — what a query engine reading the current
//! snapshot sees — independent of what [`Catalog::snapshots`] reports.
//!
//! Tables are created format v2 unless the catalog is told otherwise
//! ([`MemoryCatalog::with_format_version`], or for one table
//! [`MemoryCatalog::with_table_format_version`]), and can be upgraded
//! ([`MemoryCatalog::upgrade_format_version`]). A v3 table has row
//! lineage, as the spec requires: every commit gives each data file it
//! adds the next `record_count` row ids, the first one recorded on the
//! file ([`DataFile::first_row_id`]). A row's id is that plus its
//! position in the file, unless the file stores the row's own.
//!
//! A v3 table's deletes are deletion vectors, checked as Iceberg's Java
//! row delta and rewrite check them: a vector's data file must be live,
//! a table holds at most one per data file — a commit replacing one names
//! it — and a rewrite takes its inputs' vectors with them, failing if
//! one landed since the rewrite read the table. A v2 table takes none.
//! pg2iceberg commits no equality deletes to a v3 table: the catalog
//! refuses those of a commit prepared before the table's upgrade.

use async_trait::async_trait;
use pg2iceberg_core::{Namespace, TableIdent, TableSchema};
use pg2iceberg_iceberg::PreparedCommit;
use pg2iceberg_iceberg::{
    apply_schema_changes, live_deletion_vectors, log_ends_property, merge_log_ends,
    recorded_log_ends, Catalog, DataFile, DeletionVectorKey, FormatVersion, FormatVersions,
    IcebergError, LogRange, PreparedCompaction, Result, SchemaChange, Snapshot, TableMetadata,
    DELETES_SINCE_REWRITE_CONFLICT, DELETION_VECTOR_CONFLICT, EQUALITY_DELETES_ON_V3_CONFLICT,
    MISSING_DATA_FILE_CONFLICT,
};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};

#[derive(Default)]
struct State {
    namespaces: BTreeSet<Namespace>,
    tables: BTreeMap<TableIdent, MemTable>,
}

struct MemTable {
    metadata: TableMetadata,
    /// Every snapshot ever committed, expired or not.
    snapshots: Vec<Snapshot>,
    /// Ids of expired snapshots.
    expired: BTreeSet<i64>,
    next_snapshot_id: i64,
    /// Each snapshot's recorded log ends: those carried forward, and its
    /// own commit's (see [`LogRange`]).
    log_ends: BTreeMap<i64, BTreeMap<String, u64>>,
    format_version: FormatVersion,
    /// v3: the row id the next data file's first row takes.
    next_row_id: i64,
}

impl MemTable {
    /// Give rows their ids, as a v3 table's commit does: each data file
    /// the table holds without any yet — one it had before its upgrade to
    /// v3 — and then each of `files`, just committed, takes the next
    /// `record_count` of them, in order. Every commit does: its manifest
    /// list assigns ids to each manifest it names that has none. A v2
    /// table has no row lineage.
    /// The snapshot's row range ([`TableMetadata::row_range`]) is the ids
    /// it gave out.
    fn assign_row_ids(&mut self, files: &mut [DataFile]) {
        if self.format_version != FormatVersion::V3 {
            return;
        }
        let live: BTreeSet<String> = self.live_paths().into_iter().map(str::to_string).collect();
        let earlier = self
            .snapshots
            .iter_mut()
            .flat_map(|s| s.data_files.iter_mut())
            .filter(|f| f.first_row_id.is_none() && live.contains(&f.path));
        let first_row_id = self.next_row_id;
        for f in earlier.chain(files.iter_mut()) {
            f.first_row_id = Some(self.next_row_id);
            self.next_row_id += f.record_count as i64;
        }
        self.metadata.row_range = Some(pg2iceberg_iceberg::RowRange {
            first_row_id,
            rows: (self.next_row_id - first_row_id) as u64,
        });
    }

    /// Record snapshot `id`, just committed: it carries the table's log
    /// ends forward, with `range`'s.
    fn record_log_ends(&mut self, id: i64, range: Option<&LogRange>) {
        let mut ends = self.metadata.log_ends.clone();
        if let Some(range) = range {
            merge_log_ends(&mut ends, [(range.group.clone(), range.end)]);
        }
        self.log_ends.insert(id, ends);
        self.refresh_log_ends();
    }

    /// Record snapshot `id`, just committed by another engine: it records
    /// no log ends, carried forward or its own.
    fn record_no_log_ends(&mut self, id: i64) {
        self.log_ends.insert(id, BTreeMap::new());
        self.refresh_log_ends();
    }

    /// The paths of the files the table holds now.
    fn live_paths(&self) -> BTreeSet<&str> {
        let removed: BTreeSet<&str> = self
            .snapshots
            .iter()
            .flat_map(|snap| snap.removed_paths.iter().map(String::as_str))
            .collect();
        self.snapshots
            .iter()
            .flat_map(|snap| snap.data_files.iter().chain(&snap.delete_files))
            .map(|f| f.path.as_str())
            .filter(|path| !removed.contains(path))
            .collect()
    }

    /// The paths of the data files the table holds now.
    fn live_data_paths(&self) -> BTreeSet<String> {
        let removed: BTreeSet<&str> = self
            .snapshots
            .iter()
            .flat_map(|snap| snap.removed_paths.iter().map(String::as_str))
            .collect();
        self.snapshots
            .iter()
            .flat_map(|snap| &snap.data_files)
            .map(|f| f.path.as_str())
            .filter(|path| !removed.contains(path))
            .map(str::to_string)
            .collect()
    }

    /// The deletion vectors the table holds now, by the data file each
    /// names.
    fn live_vectors(&self) -> BTreeMap<String, DeletionVectorKey> {
        live_deletion_vectors(&self.snapshots)
            .into_iter()
            .map(|(file, (dv, _))| (file.to_string(), dv.key()))
            .collect()
    }

    /// Check `steps`, to be committed in order, as Iceberg's row delta
    /// checks one: a vector removed must still be in the table; one added
    /// names a data file the table holds — a rewrite may have removed it
    /// — that has no other. A v2 table takes none; and pg2iceberg commits
    /// no equality deletes to a v3 table.
    fn check_deletes<'a>(&self, steps: impl IntoIterator<Item = &'a PreparedCommit>) -> Result<()> {
        let mut data = self.live_data_paths();
        let mut vectors = self.live_vectors();
        for step in steps {
            let v3 = self.format_version == FormatVersion::V3;
            if v3 && !step.equality_deletes.is_empty() {
                return Err(IcebergError::Conflict(format!(
                    "{EQUALITY_DELETES_ON_V3_CONFLICT}: {}",
                    self.metadata.ident
                )));
            }
            if !v3 && !step.deletion_vectors.is_empty() {
                return Err(IcebergError::Other(format!(
                    "deletion vectors need format v3: {}",
                    self.metadata.ident
                )));
            }
            data.extend(step.data_files.iter().map(|f| f.path.clone()));
            for key in &step.removed_deletion_vectors {
                let Some(file) = vectors
                    .iter()
                    .find_map(|(file, live)| (live == key).then(|| file.clone()))
                else {
                    return Err(IcebergError::Conflict(format!(
                        "{DELETION_VECTOR_CONFLICT}: {key:?} is gone"
                    )));
                };
                vectors.remove(&file);
            }
            for dv in &step.deletion_vectors {
                if !data.contains(&dv.referenced_data_file) {
                    return Err(IcebergError::Conflict(format!(
                        "{MISSING_DATA_FILE_CONFLICT}: {}",
                        dv.referenced_data_file
                    )));
                }
                if let Some(other) = vectors.insert(dv.referenced_data_file.clone(), dv.key()) {
                    return Err(IcebergError::Conflict(format!(
                        "{DELETION_VECTOR_CONFLICT}: {} has {other:?}",
                        dv.referenced_data_file
                    )));
                }
            }
        }
        Ok(())
    }

    /// The vectors of the data files `removed` — they go with them, as
    /// Iceberg's Java rewrite removes them.
    fn vectors_of(&self, removed: &[String]) -> Vec<DeletionVectorKey> {
        let live = self.live_vectors();
        removed
            .iter()
            .filter_map(|path| live.get(path).cloned())
            .collect()
    }

    /// pg2iceberg's rewrite lists the vectors of the files it removes, as
    /// it read them: the commit fails unless those are the files' vectors
    /// still — rows deleted since would come back. The vectors it adds
    /// name the files it adds, one each.
    fn check_rewrite_vectors(&self, prepared: &PreparedCompaction) -> Result<()> {
        let listed: BTreeSet<&DeletionVectorKey> =
            prepared.removed_deletion_vectors.iter().collect();
        let held = self.vectors_of(&prepared.removed_paths);
        if listed != held.iter().collect() {
            return Err(IcebergError::Conflict(format!(
                "{DELETES_SINCE_REWRITE_CONFLICT}: the rewrite removes {listed:?}, \
                 its inputs have {held:?}"
            )));
        }
        let added: BTreeSet<&str> = prepared
            .added_data_files
            .iter()
            .map(|f| f.path.as_str())
            .collect();
        let mut named = BTreeSet::new();
        for dv in &prepared.deletion_vectors {
            let file = dv.referenced_data_file.as_str();
            if !added.contains(file) || !named.insert(file) {
                return Err(IcebergError::Other(format!(
                    "a rewrite's deletion vector must name a file it adds, one each: {file}"
                )));
            }
        }
        Ok(())
    }

    /// Like iceberg-rust's rewrite: a file another commit already removed
    /// fails a commit that removes it, or both passes' outputs would hold
    /// its rows.
    fn check_removable(&self, paths: &[String]) -> Result<()> {
        let live = self.live_paths();
        let missing: Vec<&String> = paths
            .iter()
            .filter(|path| !live.contains(path.as_str()))
            .collect();
        if !missing.is_empty() {
            return Err(IcebergError::Conflict(format!(
                "rewrite removes files no longer in the table: {missing:?}"
            )));
        }
        Ok(())
    }

    /// Like the fork's `RewriteFilesAction::validate_no_row_ids`: a
    /// rewrite whose outputs store no row lineage fails if a data file it
    /// removes has row ids — its rows would take new ones.
    fn check_row_lineage(&self, prepared: &pg2iceberg_iceberg::PreparedCompaction) -> Result<()> {
        if prepared.stores_row_lineage {
            return Ok(());
        }
        let removed: BTreeSet<&str> = prepared.removed_paths.iter().map(String::as_str).collect();
        let with_ids: Vec<&str> = self
            .snapshots
            .iter()
            .flat_map(|s| &s.data_files)
            .filter(|f| f.first_row_id.is_some() && removed.contains(f.path.as_str()))
            .map(|f| f.path.as_str())
            .collect();
        if !with_ids.is_empty() {
            return Err(IcebergError::Conflict(format!(
                "{}: {with_ids:?}",
                pg2iceberg_iceberg::ROW_IDS_CONFLICT
            )));
        }
        Ok(())
    }

    /// Bring `metadata.log_ends` in line with the table's properties and
    /// its unexpired snapshots.
    fn refresh_log_ends(&mut self) {
        let properties = &self.metadata.properties;
        let mut ends = recorded_log_ends(|key| properties.get(key).map(String::as_str));
        for (id, recorded) in &self.log_ends {
            if !self.expired.contains(id) {
                merge_log_ends(&mut ends, recorded.clone());
            }
        }
        self.metadata.log_ends = ends;
    }
}

#[derive(Default, Clone)]
pub struct MemoryCatalog {
    state: Arc<Mutex<State>>,
    /// The format version of each table it creates.
    formats: FormatVersions,
}

impl MemoryCatalog {
    pub fn new() -> Self {
        Self::default()
    }

    /// Create tables in format `version` (v2 by default), unless one has
    /// a version of its own, as `IcebergRustCatalog::with_format_version`
    /// does.
    pub fn with_format_version(mut self, version: FormatVersion) -> Self {
        self.formats.default = version;
        self
    }

    /// Create `ident` in format `version`, whatever the others take, as
    /// `IcebergRustCatalog::with_table_format_version` does.
    pub fn with_table_format_version(mut self, ident: TableIdent, version: FormatVersion) -> Self {
        self.formats.tables.insert(ident, version);
        self
    }

    /// Every snapshot ever committed to `ident`, expired or not: the
    /// history a reader of the current snapshot effectively sees.
    pub fn history(&self, ident: &TableIdent) -> Vec<Snapshot> {
        let s = self.state.lock().unwrap();
        s.tables
            .get(ident)
            .map(|t| t.snapshots.clone())
            .unwrap_or_default()
    }

    /// Test hook: upgrade `ident` to format `version`, as another engine
    /// does (`ALTER TABLE … SET TBLPROPERTIES ('format-version' = '3')`).
    /// Upgrading to v3 starts the table's row ids at 0; the rows it holds
    /// get theirs at its next commit.
    pub fn upgrade_format_version(&self, ident: &TableIdent, version: FormatVersion) -> Result<()> {
        let mut s = self.state.lock().unwrap();
        let table = s
            .tables
            .get_mut(ident)
            .ok_or_else(|| IcebergError::NotFound(format!("table: {ident}")))?;
        if (table.format_version, version) == (FormatVersion::V3, FormatVersion::V2) {
            return Err(IcebergError::Other(format!(
                "cannot downgrade {ident} from v3 to v2"
            )));
        }
        if version == FormatVersion::V3 && table.format_version != FormatVersion::V3 {
            table.next_row_id = 0;
        }
        table.format_version = version;
        table.metadata.format_version = version;
        Ok(())
    }

    /// The table's format version.
    pub fn format_version(&self, ident: &TableIdent) -> Option<FormatVersion> {
        let s = self.state.lock().unwrap();
        s.tables.get(ident).map(|t| t.format_version)
    }

    /// v3: the row id the table's next data file starts at — above every
    /// id its rows have been given.
    pub fn next_row_id(&self, ident: &TableIdent) -> Option<i64> {
        let s = self.state.lock().unwrap();
        s.tables
            .get(ident)
            .filter(|t| t.format_version == FormatVersion::V3)
            .map(|t| t.next_row_id)
    }

    /// Test hook: stamp a config map onto a table's metadata. The
    /// vended-credentials router consumes
    /// [`TableMetadata::config`] to extract S3 creds, but
    /// `commit_snapshot` doesn't write to that map (it only mutates
    /// `current_snapshot_id`). Tests that exercise the router path
    /// use this to plant fake creds without standing up a real
    /// REST catalog. Overwrites any existing config.
    pub fn set_table_config(
        &self,
        ident: &TableIdent,
        config: BTreeMap<String, String>,
    ) -> Result<()> {
        let mut s = self.state.lock().unwrap();
        let table = s
            .tables
            .get_mut(ident)
            .ok_or_else(|| IcebergError::NotFound(format!("table: {ident}")))?;
        table.metadata.config = config;
        Ok(())
    }

    /// Test hook: a rewrite another engine commits — a managed catalog's
    /// own compaction. Unlike [`Catalog::commit_compaction`], its snapshot
    /// records no log ends: only pg2iceberg writes those.
    ///
    /// Its outputs keep the sequence number of the snapshot the pass read
    /// (`prepared.data_sequence_number`), as Iceberg's Java rewrite does by
    /// default — or, with `own_sequence_number`, take the commit's own, as
    /// it does when told not to. Then, as it does, the commit fails if a
    /// delete file landed since the pass read the table: at the newer
    /// number, it would no longer apply to the rewritten rows. Whichever:
    /// it fails if a deletion vector landed since on a file it removes —
    /// one names a position in that file only — and it removes the
    /// vectors the files it removes have, as Java's does. Its own
    /// `removed_deletion_vectors` and `deletion_vectors` are ignored.
    pub fn commit_foreign_rewrite(
        &self,
        prepared: pg2iceberg_iceberg::PreparedCompaction,
        own_sequence_number: bool,
    ) -> Result<TableMetadata> {
        let mut s = self.state.lock().unwrap();
        let table = s
            .tables
            .get_mut(&prepared.ident)
            .ok_or_else(|| IcebergError::NotFound(format!("table: {}", prepared.ident)))?;
        table.check_removable(&prepared.removed_paths)?;
        table.check_row_lineage(&prepared)?;
        let read = prepared.data_sequence_number.unwrap_or(0);
        let removed: BTreeSet<&str> = prepared.removed_paths.iter().map(String::as_str).collect();
        let landed: Vec<&str> = table
            .snapshots
            .iter()
            .filter(|snap| snap.id > read)
            .flat_map(|snap| &snap.deletion_vectors)
            .map(|dv| dv.referenced_data_file.as_str())
            .filter(|file| removed.contains(file))
            .collect();
        if !landed.is_empty() {
            return Err(IcebergError::Conflict(format!(
                "deletion vectors landed since the rewrite read snapshot {read}: {landed:?}"
            )));
        }
        let removed_deletion_vectors = table.vectors_of(&prepared.removed_paths);
        if own_sequence_number {
            let since = table
                .snapshots
                .iter()
                .filter(|snap| snap.id > read && !snap.delete_files.is_empty())
                .map(|snap| snap.id)
                .collect::<Vec<_>>();
            if !since.is_empty() {
                return Err(IcebergError::Conflict(format!(
                    "delete files landed since the rewrite read snapshot {read}: {since:?}"
                )));
            }
        }
        let id = table.next_snapshot_id;
        table.next_snapshot_id += 1;
        let sequence_number = if own_sequence_number {
            None
        } else {
            prepared.data_sequence_number
        };
        let mut added: Vec<DataFile> = prepared
            .added_data_files
            .into_iter()
            .map(|f| DataFile {
                sequence_number,
                ..f
            })
            .collect();
        table.assign_row_ids(&mut added);
        table.snapshots.push(Snapshot {
            id,
            data_files: added,
            delete_files: Vec::new(),
            removed_paths: prepared.removed_paths,
            timestamp_ms: id * 1000,
            expired: false,
            log_range: None,
            deletion_vectors: Vec::new(),
            removed_deletion_vectors,
        });
        table.metadata.current_snapshot_id = Some(id);
        table.record_no_log_ends(id);
        Ok(table.metadata.clone())
    }

    /// Test hook: another engine removes files without rewriting any —
    /// a `delete` snapshot, as one that drops delete files nothing is left
    /// for them to apply to commits. The vectors of data files it removes
    /// go with them.
    pub fn commit_foreign_removal(
        &self,
        ident: &TableIdent,
        paths: Vec<String>,
    ) -> Result<TableMetadata> {
        let mut s = self.state.lock().unwrap();
        let table = s
            .tables
            .get_mut(ident)
            .ok_or_else(|| IcebergError::NotFound(format!("table: {ident}")))?;
        table.check_removable(&paths)?;
        table.assign_row_ids(&mut []);
        let removed_deletion_vectors = table.vectors_of(&paths);
        let id = table.next_snapshot_id;
        table.next_snapshot_id += 1;
        table.snapshots.push(Snapshot {
            id,
            data_files: Vec::new(),
            delete_files: Vec::new(),
            removed_paths: paths,
            timestamp_ms: id * 1000,
            expired: false,
            log_range: None,
            deletion_vectors: Vec::new(),
            removed_deletion_vectors,
        });
        table.metadata.current_snapshot_id = Some(id);
        table.record_no_log_ends(id);
        Ok(table.metadata.clone())
    }
}

#[async_trait]
impl Catalog for MemoryCatalog {
    async fn ensure_namespace(&self, ns: &Namespace) -> Result<()> {
        self.state.lock().unwrap().namespaces.insert(ns.clone());
        Ok(())
    }

    async fn load_table(&self, ident: &TableIdent) -> Result<Option<TableMetadata>> {
        Ok(self
            .state
            .lock()
            .unwrap()
            .tables
            .get(ident)
            .map(|t| t.metadata.clone()))
    }

    async fn create_table(&self, schema: &TableSchema) -> Result<TableMetadata> {
        let mut s = self.state.lock().unwrap();
        let ident = schema.ident.clone();
        if s.tables.contains_key(&ident) {
            return Err(IcebergError::Conflict(format!(
                "table already exists: {ident}"
            )));
        }
        if !s.namespaces.contains(&ident.namespace) {
            return Err(IcebergError::NotFound(format!(
                "namespace not registered: {}",
                ident.namespace
            )));
        }
        let format_version = self.formats.of(&ident);
        let metadata = TableMetadata {
            ident: ident.clone(),
            schema: schema.clone(),
            current_snapshot_id: None,
            config: BTreeMap::new(),
            location: String::new(),
            log_ends: BTreeMap::new(),
            properties: BTreeMap::new(),
            format_version,
            row_range: None,
        };
        s.tables.insert(
            ident,
            MemTable {
                metadata: metadata.clone(),
                snapshots: Vec::new(),
                expired: BTreeSet::new(),
                next_snapshot_id: 1,
                log_ends: BTreeMap::new(),
                format_version,
                next_row_id: 0,
            },
        );
        Ok(metadata)
    }

    async fn commit_snapshot(&self, prepared: PreparedCommit) -> Result<TableMetadata> {
        let ident = prepared.ident.clone();
        let mut s = self.state.lock().unwrap();
        let table = s
            .tables
            .get_mut(&ident)
            .ok_or_else(|| IcebergError::NotFound(format!("table: {ident}")))?;
        // Skip empty commits (no data, no deletes). This matches the materializer's
        // behavior of not bumping the snapshot id when there's nothing to write.
        commit_steps(table, vec![prepared], None)?;
        Ok(table.metadata.clone())
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
        // One lock for every step: readers observe all or none of them.
        let mut s = self.state.lock().unwrap();
        let table = s
            .tables
            .get_mut(&ident)
            .ok_or_else(|| IcebergError::NotFound(format!("table: {ident}")))?;
        if let Some(id) = commit_steps(table, steps, log_range.as_ref())? {
            // The table records the ends too, as the prod catalog does.
            if log_range.is_some() {
                table
                    .metadata
                    .properties
                    .extend(log_ends_property(&table.metadata.log_ends));
            }
            let snap = table.snapshots.last_mut().expect("just pushed");
            debug_assert_eq!(snap.id, id);
            snap.log_range = log_range;
        }
        for key in &remove_properties {
            table.metadata.properties.remove(key);
        }
        Ok(table.metadata.clone())
    }

    async fn commit_compaction(
        &self,
        prepared: pg2iceberg_iceberg::PreparedCompaction,
    ) -> Result<TableMetadata> {
        let mut s = self.state.lock().unwrap();
        let table = s
            .tables
            .get_mut(&prepared.ident)
            .ok_or_else(|| IcebergError::NotFound(format!("table: {}", prepared.ident)))?;

        // Empty compaction (nothing added, nothing removed) is a noop.
        if prepared.is_empty() {
            return Ok(table.metadata.clone());
        }
        table.check_removable(&prepared.removed_paths)?;
        table.check_row_lineage(&prepared)?;
        table.check_rewrite_vectors(&prepared)?;

        let id = table.next_snapshot_id;
        table.next_snapshot_id += 1;
        let mut added: Vec<DataFile> = prepared
            .added_data_files
            .into_iter()
            .map(|f| DataFile {
                sequence_number: prepared.data_sequence_number,
                ..f
            })
            .collect();
        table.assign_row_ids(&mut added);
        table.snapshots.push(Snapshot {
            id,
            data_files: added,
            delete_files: Vec::new(),
            removed_paths: prepared.removed_paths,
            timestamp_ms: id * 1000,
            expired: false,
            log_range: None,
            deletion_vectors: prepared.deletion_vectors,
            removed_deletion_vectors: prepared.removed_deletion_vectors,
        });
        table.metadata.current_snapshot_id = Some(id);
        table.record_log_ends(id, None);
        Ok(table.metadata.clone())
    }

    async fn expire_snapshots(&self, ident: &TableIdent, retention_ms: i64) -> Result<usize> {
        let mut s = self.state.lock().unwrap();
        let table = s
            .tables
            .get_mut(ident)
            .ok_or_else(|| IcebergError::NotFound(format!("table: {}", ident)))?;

        if table.snapshots.is_empty() {
            return Ok(0);
        }
        // Cutoff is relative to the most recent snapshot's timestamp.
        // Real catalogs use wall-clock now() instead; the sim uses the
        // monotonic surrogate so tests stay deterministic.
        let latest_ts = table
            .snapshots
            .iter()
            .map(|s| s.timestamp_ms)
            .max()
            .unwrap_or(0);
        let cutoff = latest_ts - retention_ms;
        // Never drop the current snapshot (Iceberg invariant).
        let current_id = table.metadata.current_snapshot_id;

        let expire: Vec<i64> = table
            .snapshots
            .iter()
            .filter(|snap| snap.timestamp_ms < cutoff && Some(snap.id) != current_id)
            .map(|snap| snap.id)
            .filter(|id| !table.expired.contains(id))
            .collect();
        table.expired.extend(&expire);
        table.refresh_log_ends();
        Ok(expire.len())
    }

    async fn evolve_schema(
        &self,
        ident: &TableIdent,
        changes: Vec<SchemaChange>,
        set_properties: BTreeMap<String, String>,
    ) -> Result<TableMetadata> {
        let mut s = self.state.lock().unwrap();
        let table = s
            .tables
            .get_mut(ident)
            .ok_or_else(|| IcebergError::NotFound(format!("table: {ident}")))?;
        // Use the same helper the prod catalog uses, so both backends apply
        // identical field-id allocation and soft-drop semantics. The sim
        // does not produce snapshots for schema-only commits — it just
        // updates the schema in metadata. Applied to a copy first, so a
        // failed change leaves the table as it was.
        let mut schema = table.metadata.schema.clone();
        apply_schema_changes(&mut schema, &changes)?;
        table.metadata.schema = schema;
        table.metadata.properties.extend(set_properties);
        Ok(table.metadata.clone())
    }

    /// An expired snapshot is reported with only the files it added that
    /// are still live, as the trait requires.
    async fn snapshots(&self, ident: &TableIdent) -> Result<Vec<Snapshot>> {
        let s = self.state.lock().unwrap();
        let Some(t) = s.tables.get(ident) else {
            return Ok(Vec::new());
        };
        let removed: BTreeSet<&str> = t
            .snapshots
            .iter()
            .flat_map(|snap| snap.removed_paths.iter().map(String::as_str))
            .collect();
        let live = |files: &[pg2iceberg_iceberg::DataFile]| {
            files
                .iter()
                .filter(|f| !removed.contains(f.path.as_str()))
                .cloned()
                .collect::<Vec<_>>()
        };
        let removed_vectors: BTreeSet<&DeletionVectorKey> = t
            .snapshots
            .iter()
            .flat_map(|snap| &snap.removed_deletion_vectors)
            .collect();
        Ok(t.snapshots
            .iter()
            .enumerate()
            .filter_map(|(i, snap)| {
                if !t.expired.contains(&snap.id) {
                    // Like a real catalog's: what a snapshot removed is
                    // what its parent held that it doesn't — unknown once
                    // its parent is expired, its metadata gone.
                    let parent_expired = i
                        .checked_sub(1)
                        .is_some_and(|p| t.expired.contains(&t.snapshots[p].id));
                    if parent_expired {
                        return Some(Snapshot {
                            removed_paths: Vec::new(),
                            removed_deletion_vectors: Vec::new(),
                            ..snap.clone()
                        });
                    }
                    return Some(snap.clone());
                }
                // Like a real catalog's: what the snapshot removed is gone
                // with its metadata.
                let stand_in = Snapshot {
                    data_files: live(&snap.data_files),
                    delete_files: live(&snap.delete_files),
                    removed_paths: Vec::new(),
                    deletion_vectors: snap
                        .deletion_vectors
                        .iter()
                        .filter(|dv| !removed_vectors.contains(&dv.key()))
                        .cloned()
                        .collect(),
                    removed_deletion_vectors: Vec::new(),
                    expired: true,
                    log_range: None,
                    ..snap.clone()
                };
                let empty = stand_in.data_files.is_empty()
                    && stand_in.delete_files.is_empty()
                    && stand_in.deletion_vectors.is_empty();
                (!empty).then_some(stand_in)
            })
            .collect())
    }
}

/// Commit `steps` to `table` in order, each that isn't empty as its own
/// snapshot, after checking them all: none lands, or every one does.
/// `range` is the last snapshot's log range. The last snapshot's id, if
/// any landed.
fn commit_steps(
    table: &mut MemTable,
    steps: Vec<PreparedCommit>,
    range: Option<&LogRange>,
) -> Result<Option<i64>> {
    let steps: Vec<PreparedCommit> = steps.into_iter().filter(|s| !s.is_empty()).collect();
    // As the prod catalog: a v3 table's commit is one snapshot, its
    // deletion vectors applying to the rows of their own snapshot.
    let vectors = steps
        .iter()
        .any(|s| !s.deletion_vectors.is_empty() || !s.removed_deletion_vectors.is_empty());
    if vectors && steps.len() > 1 {
        return Err(IcebergError::Other(format!(
            "commit_snapshots: deletion vectors in a commit of {} snapshots",
            steps.len()
        )));
    }
    table.check_deletes(&steps)?;
    let mut last = None;
    for step in steps {
        let id = table.next_snapshot_id;
        table.next_snapshot_id += 1;
        // Sim has no real Clock; timestamp is derived from the monotonic
        // snapshot id (multiplied by 1000 so retention-by-ms tests can
        // discriminate). Real catalogs surface iceberg's own
        // wall-clock millis here.
        let mut data_files = step.data_files;
        table.assign_row_ids(&mut data_files);
        table.snapshots.push(Snapshot {
            id,
            data_files,
            delete_files: step.equality_deletes,
            removed_paths: Vec::new(),
            timestamp_ms: id * 1000,
            expired: false,
            log_range: None,
            deletion_vectors: step.deletion_vectors,
            removed_deletion_vectors: step.removed_deletion_vectors,
        });
        table.metadata.current_snapshot_id = Some(id);
        last = Some(id);
    }
    if let Some(id) = last {
        table.record_log_ends(id, range);
    }
    Ok(last)
}

/// The table as a query engine reading its current snapshot sees it:
/// every committed snapshot's files, expired or not. A
/// [`pg2iceberg_iceberg::verify::DynCatalog`] whose `snapshots` is the full
/// history, so `read_materialized_state` over it is the ground truth that
/// `Catalog::snapshots` must replay to.
pub struct ReaderView<'a>(pub &'a MemoryCatalog);

#[async_trait]
impl pg2iceberg_iceberg::verify::DynCatalog for ReaderView<'_> {
    async fn snapshots(
        &self,
        ident: &TableIdent,
    ) -> pg2iceberg_iceberg::verify::Result<Vec<Snapshot>> {
        let s = self.0.state.lock().unwrap();
        Ok(s.tables
            .get(ident)
            .map(|t| t.snapshots.clone())
            .unwrap_or_default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pg2iceberg_core::typemap::IcebergType;
    use pg2iceberg_core::ColumnSchema;
    use pg2iceberg_iceberg::DataFile;
    use pollster::block_on;

    fn ident() -> TableIdent {
        TableIdent {
            namespace: Namespace(vec!["public".into()]),
            name: "t".into(),
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

    #[test]
    fn create_then_load_round_trips() {
        let c = MemoryCatalog::new();
        block_on(c.ensure_namespace(&ident().namespace)).unwrap();
        assert!(block_on(c.load_table(&ident())).unwrap().is_none());
        let meta = block_on(c.create_table(&schema())).unwrap();
        assert_eq!(meta.ident, ident());
        assert!(block_on(c.load_table(&ident())).unwrap().is_some());
    }

    #[test]
    fn create_table_in_unregistered_namespace_errors() {
        let c = MemoryCatalog::new();
        let err = block_on(c.create_table(&schema())).unwrap_err();
        assert!(matches!(err, IcebergError::NotFound(_)));
    }

    #[test]
    fn commit_appends_snapshots_with_increasing_ids() {
        let c = MemoryCatalog::new();
        block_on(c.ensure_namespace(&ident().namespace)).unwrap();
        block_on(c.create_table(&schema())).unwrap();

        for i in 0..3 {
            block_on(c.commit_snapshot(PreparedCommit {
                ident: ident(),
                data_files: vec![DataFile {
                    path: format!("s3://t/data-{i}.parquet"),
                    record_count: 1,
                    byte_size: 100,
                    equality_field_ids: vec![],
                    partition_values: Vec::new(),
                    sequence_number: None,
                    first_row_id: None,
                }],
                equality_deletes: vec![],
                deletion_vectors: Vec::new(),
                removed_deletion_vectors: Vec::new(),
            }))
            .unwrap();
        }
        let snaps = block_on(c.snapshots(&ident())).unwrap();
        assert_eq!(
            snaps.iter().map(|s| s.id).collect::<Vec<_>>(),
            vec![1, 2, 3]
        );
    }

    #[test]
    fn empty_prepared_commit_is_a_noop() {
        let c = MemoryCatalog::new();
        block_on(c.ensure_namespace(&ident().namespace)).unwrap();
        block_on(c.create_table(&schema())).unwrap();
        block_on(c.commit_snapshot(PreparedCommit {
            ident: ident(),
            data_files: vec![],
            equality_deletes: vec![],
            deletion_vectors: Vec::new(),
            removed_deletion_vectors: Vec::new(),
        }))
        .unwrap();
        assert!(block_on(c.snapshots(&ident())).unwrap().is_empty());
    }

    #[test]
    fn evolve_schema_add_column_appends_with_fresh_field_id() {
        let c = MemoryCatalog::new();
        block_on(c.ensure_namespace(&ident().namespace)).unwrap();
        block_on(c.create_table(&schema())).unwrap();
        let meta = block_on(c.evolve_schema(
            &ident(),
            vec![pg2iceberg_iceberg::SchemaChange::AddColumn {
                name: "qty".into(),
                ty: IcebergType::Long,
                nullable: true,
            }],
            BTreeMap::new(),
        ))
        .unwrap();
        assert_eq!(meta.schema.columns.len(), 2);
        let qty = meta
            .schema
            .columns
            .iter()
            .find(|c| c.name == "qty")
            .unwrap();
        assert_eq!(qty.field_id, 2);
        assert!(qty.nullable);
        assert!(!qty.is_primary_key);
    }

    #[test]
    fn evolve_schema_drop_column_is_soft_drop() {
        let c = MemoryCatalog::new();
        block_on(c.ensure_namespace(&ident().namespace)).unwrap();
        // Create a table where column 2 is non-nullable so we can observe
        // the soft-drop flipping it.
        let s = TableSchema {
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
            ],
            partition_spec: Vec::new(),
            pg_schema: None,
        };
        block_on(c.create_table(&s)).unwrap();
        let meta = block_on(c.evolve_schema(
            &ident(),
            vec![pg2iceberg_iceberg::SchemaChange::DropColumn { name: "qty".into() }],
            BTreeMap::new(),
        ))
        .unwrap();
        assert_eq!(meta.schema.columns.len(), 2);
        let qty = meta
            .schema
            .columns
            .iter()
            .find(|c| c.name == "qty")
            .unwrap();
        assert!(qty.nullable);
    }

    #[test]
    fn evolve_schema_on_missing_table_errors() {
        let c = MemoryCatalog::new();
        let err = block_on(c.evolve_schema(
            &ident(),
            vec![pg2iceberg_iceberg::SchemaChange::AddColumn {
                name: "x".into(),
                ty: IcebergType::Int,
                nullable: true,
            }],
            BTreeMap::new(),
        ))
        .unwrap_err();
        assert!(matches!(err, IcebergError::NotFound(_)));
    }

    #[test]
    fn properties_set_with_a_schema_change_and_removed_with_a_commit() {
        let c = MemoryCatalog::new();
        block_on(c.ensure_namespace(&ident().namespace)).unwrap();
        block_on(c.create_table(&schema())).unwrap();
        let add = |name: &str| pg2iceberg_iceberg::SchemaChange::AddColumn {
            name: name.into(),
            ty: IcebergType::Int,
            nullable: true,
        };
        let set = BTreeMap::from([("pg2iceberg.a".to_string(), "1".to_string())]);
        let meta = block_on(c.evolve_schema(&ident(), vec![add("x")], set.clone())).unwrap();
        assert_eq!(meta.properties, set);
        // A change that fails sets none.
        let other = BTreeMap::from([("pg2iceberg.b".to_string(), "2".to_string())]);
        block_on(c.evolve_schema(&ident(), vec![add("y"), add("x")], other)).unwrap_err();
        let meta = block_on(c.load_table(&ident())).unwrap().unwrap();
        assert_eq!(meta.properties, set);
        assert!(!meta.schema.columns.iter().any(|c| c.name == "y"));

        let meta = block_on(c.commit_snapshots(
            vec![PreparedCommit {
                ident: ident(),
                data_files: vec![DataFile {
                    path: "s3://t/data-0.parquet".into(),
                    record_count: 1,
                    byte_size: 100,
                    equality_field_ids: vec![],
                    partition_values: Vec::new(),
                    sequence_number: None,
                    first_row_id: None,
                }],
                equality_deletes: vec![],
                deletion_vectors: Vec::new(),
                removed_deletion_vectors: Vec::new(),
            }],
            None,
            BTreeSet::from(["pg2iceberg.a".to_string()]),
        ))
        .unwrap();
        assert!(meta.properties.is_empty());
        assert_eq!(meta.current_snapshot_id, Some(1));
    }

    // ── deletion vectors ─────────────────────────────────────────────

    fn data(path: &str) -> DataFile {
        DataFile {
            path: path.into(),
            record_count: 10,
            byte_size: 100,
            equality_field_ids: vec![],
            partition_values: Vec::new(),
            sequence_number: None,
            first_row_id: None,
        }
    }

    fn dv(puffin: &str, offset: u64, file: &str) -> pg2iceberg_iceberg::DeletionVector {
        pg2iceberg_iceberg::DeletionVector {
            path: puffin.into(),
            byte_size: 1000,
            offset,
            length: 40,
            referenced_data_file: file.into(),
            cardinality: 1,
            partition_values: Vec::new(),
            sequence_number: None,
        }
    }

    fn v3() -> MemoryCatalog {
        let c = MemoryCatalog::new().with_format_version(FormatVersion::V3);
        block_on(c.ensure_namespace(&ident().namespace)).unwrap();
        block_on(c.create_table(&schema())).unwrap();
        c
    }

    fn commit(
        c: &MemoryCatalog,
        data_files: Vec<DataFile>,
        vectors: Vec<pg2iceberg_iceberg::DeletionVector>,
        removed: Vec<&pg2iceberg_iceberg::DeletionVector>,
    ) -> Result<TableMetadata> {
        block_on(c.commit_snapshot(PreparedCommit {
            data_files,
            deletion_vectors: vectors,
            removed_deletion_vectors: removed.into_iter().map(|v| v.key()).collect(),
            ..PreparedCommit::empty(ident())
        }))
    }

    fn live(c: &MemoryCatalog) -> Vec<(String, DeletionVectorKey)> {
        let snaps = block_on(c.snapshots(&ident())).unwrap();
        live_deletion_vectors(&snaps)
            .into_iter()
            .map(|(file, (dv, _))| (file.to_string(), dv.key()))
            .collect()
    }

    fn conflict(r: Result<TableMetadata>, says: &str) {
        match r {
            Err(IcebergError::Conflict(msg)) => assert!(msg.contains(says), "{msg}"),
            other => panic!("expected a conflict saying {says:?}, got {other:?}"),
        }
    }

    #[test]
    fn a_deletion_vector_replaces_its_data_files_last() {
        let c = v3();
        let first = dv("p1.puffin", 4, "a.parquet");
        commit(&c, vec![data("a.parquet")], vec![first.clone()], vec![]).unwrap();
        let second = dv("p2.puffin", 4, "a.parquet");
        commit(&c, vec![], vec![second.clone()], vec![&first]).unwrap();
        assert_eq!(live(&c), vec![("a.parquet".into(), second.key())]);
        let snaps = block_on(c.snapshots(&ident())).unwrap();
        assert_eq!(snaps[1].removed_deletion_vectors, vec![first.key()]);
    }

    #[test]
    fn a_deletion_vector_for_a_file_that_has_one_must_replace_it() {
        let c = v3();
        let first = dv("p1.puffin", 4, "a.parquet");
        commit(&c, vec![data("a.parquet")], vec![first], vec![]).unwrap();
        conflict(
            commit(&c, vec![], vec![dv("p2.puffin", 4, "a.parquet")], vec![]),
            DELETION_VECTOR_CONFLICT,
        );
        // Nor two in one commit.
        let c = v3();
        conflict(
            commit(
                &c,
                vec![data("a.parquet")],
                vec![
                    dv("p1.puffin", 4, "a.parquet"),
                    dv("p1.puffin", 50, "a.parquet"),
                ],
                vec![],
            ),
            DELETION_VECTOR_CONFLICT,
        );
    }

    #[test]
    fn a_deletion_vector_replaced_meanwhile_cant_be_replaced() {
        let c = v3();
        let first = dv("p1.puffin", 4, "a.parquet");
        commit(&c, vec![data("a.parquet")], vec![first.clone()], vec![]).unwrap();
        commit(
            &c,
            vec![],
            vec![dv("p2.puffin", 4, "a.parquet")],
            vec![&first],
        )
        .unwrap();
        conflict(
            commit(
                &c,
                vec![],
                vec![dv("p3.puffin", 4, "a.parquet")],
                vec![&first],
            ),
            DELETION_VECTOR_CONFLICT,
        );
    }

    #[test]
    fn a_deletion_vector_must_name_a_live_data_file() {
        let c = v3();
        commit(&c, vec![data("a.parquet")], vec![], vec![]).unwrap();
        c.commit_foreign_removal(&ident(), vec!["a.parquet".into()])
            .unwrap();
        conflict(
            commit(&c, vec![], vec![dv("p1.puffin", 4, "a.parquet")], vec![]),
            MISSING_DATA_FILE_CONFLICT,
        );
        // One the same commit adds is live.
        commit(
            &c,
            vec![data("b.parquet")],
            vec![dv("p1.puffin", 4, "b.parquet")],
            vec![],
        )
        .unwrap();
    }

    #[test]
    fn a_v2_table_takes_no_deletion_vectors_and_a_v3_one_no_equality_deletes() {
        let c = MemoryCatalog::new();
        block_on(c.ensure_namespace(&ident().namespace)).unwrap();
        block_on(c.create_table(&schema())).unwrap();
        let err = commit(
            &c,
            vec![data("a.parquet")],
            vec![dv("p1.puffin", 4, "a.parquet")],
            vec![],
        )
        .unwrap_err();
        assert!(matches!(err, IcebergError::Other(_)), "{err:?}");

        let eq = PreparedCommit {
            equality_deletes: vec![DataFile {
                equality_field_ids: vec![1],
                ..data("d.parquet")
            }],
            ..PreparedCommit::empty(ident())
        };
        block_on(c.commit_snapshot(eq.clone())).unwrap();
        c.upgrade_format_version(&ident(), FormatVersion::V3)
            .unwrap();
        conflict(
            block_on(c.commit_snapshot(eq)),
            EQUALITY_DELETES_ON_V3_CONFLICT,
        );
    }

    #[test]
    fn a_commit_of_several_snapshots_takes_no_deletion_vectors() {
        let c = v3();
        let err = block_on(c.commit_snapshots(
            vec![
                PreparedCommit {
                    data_files: vec![data("a.parquet")],
                    ..PreparedCommit::empty(ident())
                },
                PreparedCommit {
                    deletion_vectors: vec![dv("p1.puffin", 4, "a.parquet")],
                    ..PreparedCommit::empty(ident())
                },
            ],
            None,
            BTreeSet::new(),
        ))
        .unwrap_err();
        assert!(matches!(err, IcebergError::Other(_)), "{err:?}");
        assert!(block_on(c.snapshots(&ident())).unwrap().is_empty());
        // One snapshot's vector names its own data file.
        commit(
            &c,
            vec![data("a.parquet")],
            vec![dv("p1.puffin", 4, "a.parquet")],
            vec![],
        )
        .unwrap();
        assert_eq!(live(&c).len(), 1);
    }

    fn rewrite(
        removed: &[&str],
        vectors: Vec<&pg2iceberg_iceberg::DeletionVector>,
        read: i64,
    ) -> PreparedCompaction {
        PreparedCompaction {
            ident: ident(),
            added_data_files: vec![data("out.parquet")],
            removed_paths: removed.iter().map(|p| p.to_string()).collect(),
            data_sequence_number: Some(read),
            stores_row_lineage: true,
            removed_deletion_vectors: vectors.into_iter().map(|v| v.key()).collect(),
            deletion_vectors: Vec::new(),
        }
    }

    #[test]
    fn a_rewrite_takes_its_inputs_vectors_with_them() {
        let c = v3();
        let first = dv("p1.puffin", 4, "a.parquet");
        commit(&c, vec![data("a.parquet")], vec![first.clone()], vec![]).unwrap();
        conflict(
            block_on(c.commit_compaction(rewrite(&["a.parquet"], vec![], 1))),
            DELETES_SINCE_REWRITE_CONFLICT,
        );
        block_on(c.commit_compaction(rewrite(&["a.parquet"], vec![&first], 1))).unwrap();
        assert!(live(&c).is_empty());
    }

    #[test]
    fn a_rewrite_fails_if_its_inputs_vectors_changed_since_it_read_them() {
        let c = v3();
        let first = dv("p1.puffin", 4, "a.parquet");
        commit(&c, vec![data("a.parquet")], vec![first.clone()], vec![]).unwrap();
        let second = dv("p2.puffin", 4, "a.parquet");
        commit(&c, vec![], vec![second.clone()], vec![&first]).unwrap();
        conflict(
            block_on(c.commit_compaction(rewrite(&["a.parquet"], vec![&first], 1))),
            DELETES_SINCE_REWRITE_CONFLICT,
        );
        // Carried over to its output, the rows stay deleted.
        let mut carried = rewrite(&["a.parquet"], vec![&second], 2);
        carried.deletion_vectors = vec![dv("p3.puffin", 4, "out.parquet")];
        block_on(c.commit_compaction(carried)).unwrap();
        assert_eq!(
            live(&c),
            vec![("out.parquet".into(), dv("p3.puffin", 4, "").key())]
        );
    }

    #[test]
    fn a_foreign_rewrite_removes_its_inputs_vectors_but_not_one_landed_since() {
        let c = v3();
        let first = dv("p1.puffin", 4, "a.parquet");
        commit(&c, vec![data("a.parquet")], vec![first.clone()], vec![]).unwrap();
        c.commit_foreign_rewrite(rewrite(&["a.parquet"], vec![], 2), false)
            .unwrap();
        assert!(live(&c).is_empty());

        let c = v3();
        commit(&c, vec![data("a.parquet")], vec![], vec![]).unwrap();
        commit(&c, vec![], vec![first.clone()], vec![]).unwrap();
        // It read the table before the vector landed.
        let err = c
            .commit_foreign_rewrite(rewrite(&["a.parquet"], vec![], 1), false)
            .unwrap_err();
        assert!(matches!(err, IcebergError::Conflict(_)), "{err:?}");
    }

    #[test]
    fn an_expired_snapshots_live_vectors_stay_listed() {
        let c = v3();
        let first = dv("p1.puffin", 4, "a.parquet");
        commit(&c, vec![data("a.parquet")], vec![first.clone()], vec![]).unwrap();
        commit(&c, vec![data("b.parquet")], vec![], vec![]).unwrap();
        commit(&c, vec![data("c.parquet")], vec![], vec![]).unwrap();
        assert_eq!(block_on(c.expire_snapshots(&ident(), 0)).unwrap(), 2);
        assert_eq!(live(&c), vec![("a.parquet".into(), first.key())]);
        // One replaced is gone with its snapshot.
        let second = dv("p2.puffin", 4, "a.parquet");
        commit(&c, vec![], vec![second.clone()], vec![&first]).unwrap();
        commit(&c, vec![data("d.parquet")], vec![], vec![]).unwrap();
        block_on(c.expire_snapshots(&ident(), 0)).unwrap();
        let snaps = block_on(c.snapshots(&ident())).unwrap();
        assert!(snaps
            .iter()
            .flat_map(|s| &s.deletion_vectors)
            .all(|dv| dv.key() == second.key()));
        assert_eq!(live(&c), vec![("a.parquet".into(), second.key())]);
    }
}
