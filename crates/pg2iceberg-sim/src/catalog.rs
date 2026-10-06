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

use async_trait::async_trait;
use pg2iceberg_core::{Namespace, TableIdent, TableSchema};
use pg2iceberg_iceberg::PreparedCommit;
use pg2iceberg_iceberg::{
    apply_schema_changes, merge_log_ends, Catalog, DataFile, IcebergError, LogRange, Result,
    SchemaChange, Snapshot, TableMetadata,
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
}

impl MemTable {
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

    /// Bring `metadata.log_ends` in line with the unexpired snapshots.
    fn refresh_log_ends(&mut self) {
        let mut ends = BTreeMap::new();
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
}

impl MemoryCatalog {
    pub fn new() -> Self {
        Self::default()
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
        let metadata = TableMetadata {
            ident: ident.clone(),
            schema: schema.clone(),
            current_snapshot_id: None,
            config: BTreeMap::new(),
            location: String::new(),
            log_ends: BTreeMap::new(),
            properties: BTreeMap::new(),
        };
        s.tables.insert(
            ident,
            MemTable {
                metadata: metadata.clone(),
                snapshots: Vec::new(),
                expired: BTreeSet::new(),
                next_snapshot_id: 1,
                log_ends: BTreeMap::new(),
            },
        );
        Ok(metadata)
    }

    async fn commit_snapshot(&self, prepared: PreparedCommit) -> Result<TableMetadata> {
        let mut s = self.state.lock().unwrap();
        let table = s
            .tables
            .get_mut(&prepared.ident)
            .ok_or_else(|| IcebergError::NotFound(format!("table: {}", prepared.ident)))?;

        // Skip empty commits (no data, no deletes). This matches the materializer's
        // behavior of not bumping the snapshot id when there's nothing to write.
        if prepared.data_files.is_empty() && prepared.equality_deletes.is_empty() {
            return Ok(table.metadata.clone());
        }

        let id = table.next_snapshot_id;
        table.next_snapshot_id += 1;
        // Sim has no real Clock; timestamp is derived from the monotonic
        // snapshot id (multiplied by 1000 so retention-by-ms tests can
        // discriminate). Real catalogs surface iceberg's own
        // wall-clock millis here.
        table.snapshots.push(Snapshot {
            id,
            data_files: prepared.data_files,
            delete_files: prepared.equality_deletes,
            removed_paths: Vec::new(),
            timestamp_ms: id * 1000,
            expired: false,
            log_range: None,
        });
        table.metadata.current_snapshot_id = Some(id);
        table.record_log_ends(id, None);
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
        let mut last = None;
        for step in steps {
            if step.data_files.is_empty() && step.equality_deletes.is_empty() {
                continue;
            }
            let id = table.next_snapshot_id;
            table.next_snapshot_id += 1;
            table.snapshots.push(Snapshot {
                id,
                data_files: step.data_files,
                delete_files: step.equality_deletes,
                removed_paths: Vec::new(),
                timestamp_ms: id * 1000,
                expired: false,
                log_range: None,
            });
            table.metadata.current_snapshot_id = Some(id);
            last = Some(id);
        }
        if let Some(id) = last {
            table.record_log_ends(id, log_range.as_ref());
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
        if prepared.added_data_files.is_empty() && prepared.removed_paths.is_empty() {
            return Ok(table.metadata.clone());
        }
        // Like iceberg-rust's rewrite: a file another pass already removed
        // fails the commit, or both passes' outputs would hold its rows.
        let removed: BTreeSet<&str> = table
            .snapshots
            .iter()
            .flat_map(|snap| snap.removed_paths.iter().map(String::as_str))
            .collect();
        let live: BTreeSet<&str> = table
            .snapshots
            .iter()
            .flat_map(|snap| snap.data_files.iter().chain(&snap.delete_files))
            .map(|f| f.path.as_str())
            .filter(|path| !removed.contains(path))
            .collect();
        let missing: Vec<&String> = prepared
            .removed_paths
            .iter()
            .filter(|path| !live.contains(path.as_str()))
            .collect();
        if !missing.is_empty() {
            return Err(IcebergError::Conflict(format!(
                "rewrite removes files no longer in the table: {missing:?}"
            )));
        }

        let id = table.next_snapshot_id;
        table.next_snapshot_id += 1;
        let added = prepared
            .added_data_files
            .into_iter()
            .map(|f| DataFile {
                sequence_number: prepared.data_sequence_number,
                ..f
            })
            .collect();
        table.snapshots.push(Snapshot {
            id,
            data_files: added,
            delete_files: Vec::new(),
            removed_paths: prepared.removed_paths,
            timestamp_ms: id * 1000,
            expired: false,
            log_range: None,
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
        Ok(t.snapshots
            .iter()
            .filter_map(|snap| {
                if !t.expired.contains(&snap.id) {
                    return Some(snap.clone());
                }
                // Like a real catalog's: what the snapshot removed is gone
                // with its metadata.
                let stand_in = Snapshot {
                    data_files: live(&snap.data_files),
                    delete_files: live(&snap.delete_files),
                    removed_paths: Vec::new(),
                    expired: true,
                    log_range: None,
                    ..snap.clone()
                };
                let empty = stand_in.data_files.is_empty() && stand_in.delete_files.is_empty();
                (!empty).then_some(stand_in)
            })
            .collect())
    }
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
                }],
                equality_deletes: vec![],
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
                }],
                equality_deletes: vec![],
            }],
            None,
            BTreeSet::from(["pg2iceberg.a".to_string()]),
        ))
        .unwrap();
        assert!(meta.properties.is_empty());
        assert_eq!(meta.current_snapshot_id, Some(1));
    }
}
