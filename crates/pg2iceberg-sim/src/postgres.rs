//! `SimPostgres`: a tiny in-process model of a Postgres source for DST.
//!
//! What it models:
//! - User tables with primary keys (rows keyed by canonical-JSON PK).
//! - A monotonic WAL where every event has a unique LSN.
//! - Transactions with all-or-nothing commit: a single tx commit produces
//!   `Begin / Change* / Commit` records with consecutive LSNs, atomically.
//! - Publications (table allowlist for a logical-replication stream).
//! - Replication slots with `restart_lsn` and `confirmed_flush_lsn`. A new
//!   stream resumes from `restart_lsn`; `send_standby` advances both.
//!
//! What it deliberately doesn't model (yet):
//! - Concurrent in-flight transactions. One tx at a time; tests serialize.
//! - WAL recycling / slot-blocks-recycling pressure.
//! - Two-phase commit, prepared transactions, in-progress (streaming) tx.
//! - The pgoutput wire protocol (sim emits `DecodedMessage` directly).
//!
//! These are the surfaces the plan calls out as "deferred" in §2; if DST
//! shows we need any of them, add behind a feature, not by rewriting the
//! happy path.

use crate::pgoutput::{self, OldTuple, RelationCol, ReplicaIdentity, TupleValue};
use async_trait::async_trait;
use bytes::Bytes;
use pg2iceberg_core::typemap::PgType;
use pg2iceberg_core::{
    ChangeEvent, ColumnName, ColumnSchema, Lsn, Op, PgValue, Row, TableIdent, TableSchema,
    Timestamp,
};
use pg2iceberg_pg::{
    DecodedMessage, PgClient, PgError, ReplicationStream, SlotMonitor, SnapshotId,
};
use pg2iceberg_snapshot::{SnapshotError, SnapshotSource};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::{Arc, Mutex};
use thiserror::Error;

#[derive(Clone, Debug, Error)]
pub enum SimError {
    #[error("table {0} does not exist")]
    UnknownTable(TableIdent),
    #[error("table {0} already exists")]
    DuplicateTable(TableIdent),
    #[error("publication {0} does not exist")]
    UnknownPublication(String),
    #[error("publication {0} already exists")]
    DuplicatePublication(String),
    #[error("slot {0} does not exist")]
    UnknownSlot(String),
    #[error("slot {0} already exists")]
    DuplicateSlot(String),
    #[error("replication slot \"{0}\" is active for another walsender")]
    SlotActive(String),
    #[error("primary key column {col} missing on insert into {table}")]
    MissingPkColumn { table: TableIdent, col: String },
    #[error("primary-key conflict on {table}: {detail}")]
    PkConflict { table: TableIdent, detail: String },
    #[error("row not found in {table} for {op}")]
    RowNotFound { table: TableIdent, op: &'static str },
    #[error("table {0} has no primary key")]
    NoPrimaryKey(TableIdent),
}

pub type Result<T> = std::result::Result<T, SimError>;

#[derive(Clone, Debug)]
struct TableData {
    schema: TableSchema,
    /// Rows keyed by canonical-JSON of the PK columns. BTreeMap keeps a
    /// deterministic iteration order, which matters for tests that scan.
    rows: BTreeMap<String, Row>,
    /// Sim-side mirror of `pg_class.oid`. Auto-assigned at
    /// `create_table` time from a monotonic counter; exposed via
    /// [`SimPostgres::table_oid`] so DST can model `DROP TABLE` +
    /// recreate (which yields a fresh oid).
    pg_oid: u32,
    /// `REPLICA IDENTITY`: what an UPDATE's or DELETE's old row carries.
    /// `Full` unless a test sets it, so a change always carries the
    /// whole old row by default.
    replica_identity: ReplicaIdentity,
    /// Postgres types of columns that don't have the default type for
    /// their Iceberg type ([`pgoutput::default_pg_type`]).
    pg_types: BTreeMap<String, PgType>,
}

impl TableData {
    /// `evt` as a walsender sends it given the table's replica identity:
    /// under `Default`, a DELETE's old row and a key-changing UPDATE's
    /// carry only the key (other columns `NULL`), and any other UPDATE
    /// carries none.
    fn shape_old_row(&self, mut evt: ChangeEvent) -> ChangeEvent {
        if self.replica_identity == ReplicaIdentity::Full {
            return evt;
        }
        let key_only = |row: &Row| -> Row {
            row.iter()
                .map(|(c, v)| {
                    let key = self
                        .schema
                        .columns
                        .iter()
                        .any(|s| s.name == c.0 && s.is_primary_key);
                    (c.clone(), if key { v.clone() } else { PgValue::Null })
                })
                .collect()
        };
        evt.before = match (evt.op, evt.before.as_ref(), evt.after.as_ref()) {
            (Op::Delete, Some(b), _) => Some(key_only(b)),
            (Op::Update, Some(b), Some(a)) if key_only(b) != key_only(a) => Some(key_only(b)),
            (Op::Update, _, _) => None,
            _ => evt.before,
        };
        evt
    }
}

impl TableData {
    fn pk_key(&self, row: &Row) -> Result<String> {
        let pk_cols: Vec<&ColumnSchema> = self.schema.primary_key_columns().collect();
        if pk_cols.is_empty() {
            return Err(SimError::NoPrimaryKey(self.schema.ident.clone()));
        }
        let mut parts: Vec<&PgValue> = Vec::with_capacity(pk_cols.len());
        for c in &pk_cols {
            let key = ColumnName(c.name.clone());
            let v = row.get(&key).ok_or_else(|| SimError::MissingPkColumn {
                table: self.schema.ident.clone(),
                col: c.name.clone(),
            })?;
            parts.push(v);
        }
        Ok(serde_json::to_string(&parts).expect("PgValue is serializable"))
    }
}

#[derive(Clone, Debug)]
struct Publication {
    tables: BTreeSet<TableIdent>,
}

#[derive(Clone, Debug)]
pub struct SlotState {
    pub publication: String,
    /// LSN where catch-up resumes when a stream reconnects. Bumps to the
    /// committed LSN on each `send_standby`.
    pub restart_lsn: Lsn,
    /// LSN the consumer has acknowledged as durably committed downstream.
    pub confirmed_flush_lsn: Lsn,
    /// Mirrors `pg_replication_slots.wal_status` (PG 13+). Defaults
    /// to [`SimWalStatus::Reserved`] (healthy). Tests can override
    /// via [`SimPostgres::set_slot_wal_status`] to model a slot
    /// transitioning toward `lost`.
    pub wal_status: SimWalStatus,
    /// Mirrors `pg_replication_slots.conflicting` (PG 16+). Tests
    /// can flip to `true` via
    /// [`SimPostgres::set_slot_conflicting`] to model a slot killed
    /// by physical-replication conflict.
    pub conflicting: bool,
    /// Mirrors `pg_replication_slots.safe_wal_size`. Defaults to a
    /// large positive value. Tests don't typically read this; it's
    /// here for the metric surface.
    pub safe_wal_size: i64,
}

/// Sim-side mirror of [`pg2iceberg_pg::WalStatus`]. Kept as a
/// separate type so the sim crate doesn't pull `pg2iceberg_pg` as
/// a non-test dep — the conversion happens at the
/// [`SimPgClient`] boundary.
#[derive(Copy, Clone, Eq, PartialEq, Debug, Default)]
pub enum SimWalStatus {
    #[default]
    Reserved,
    Extended,
    Unreserved,
    Lost,
}

#[derive(Clone, Debug)]
enum WalKind {
    Begin,
    Change(ChangeEvent),
    Commit,
    /// Schema published for a relation; emitted on `create_table` so existing
    /// streams pick it up, and on `alter_add_column` / `alter_drop_column`
    /// so the materializer can `evolve_schema` the Iceberg side. Carries a
    /// snapshot of the table's current columns at WAL-emit time.
    Relation {
        ident: TableIdent,
        columns: Vec<pg2iceberg_pg::RelationColumn>,
    },
}

#[derive(Clone, Debug)]
struct WalEntry {
    lsn: Lsn,
    xid: Option<u32>,
    kind: WalKind,
}

struct DbState {
    next_lsn: u64,
    next_xid: u32,
    /// Counter-allocated per `create_table`. Mirrors PG's behavior
    /// where each `CREATE TABLE` gets a fresh `pg_class.oid`. Real
    /// PG starts at much higher numbers; the sim starts at 16384
    /// (the conventional first user-defined oid) just to avoid
    /// conflating "0 = unknown" with a valid value.
    next_oid: u32,
    tables: BTreeMap<TableIdent, TableData>,
    publications: BTreeMap<String, Publication>,
    slots: BTreeMap<String, SlotState>,
    wal: Vec<WalEntry>,
    /// Each table's rows after every change to them (commit, ALTER), so a
    /// snapshot can read the table as of an earlier LSN.
    versions: BTreeMap<TableIdent, Vec<(Lsn, Rows)>>,
    /// The LSN an open snapshot reads at (see [`SimPostgres::begin_snapshot`]).
    snapshot_at: Option<Lsn>,
    /// Bumped by [`SimPostgres::terminate_walsenders`]: a stream started
    /// under an earlier value has lost its connection.
    walsender_generation: u64,
    /// `START_REPLICATION`s still to refuse per slot, held by a dead
    /// connection's walsender (see [`SimPostgres::hold_slot`]).
    slot_holds: BTreeMap<String, usize>,
}

/// A table's rows, keyed by canonical PK.
type Rows = BTreeMap<String, Row>;

impl DbState {
    /// Record `ident`'s current rows as of `lsn`.
    fn record_version(&mut self, ident: &TableIdent, lsn: Lsn) {
        if let Some(t) = self.tables.get(ident) {
            let rows = t.rows.clone();
            self.versions
                .entry(ident.clone())
                .or_default()
                .push((lsn, rows));
        }
    }

    /// `ident`'s rows as of `lsn`.
    fn rows_at(&self, ident: &TableIdent, lsn: Lsn) -> Rows {
        self.versions
            .get(ident)
            .and_then(|v| v.iter().rev().find(|(at, _)| *at <= lsn))
            .map(|(_, rows)| rows.clone())
            .unwrap_or_default()
    }
}

impl Default for DbState {
    fn default() -> Self {
        Self {
            next_lsn: 0,
            next_xid: 0,
            next_oid: 16384,
            tables: BTreeMap::new(),
            publications: BTreeMap::new(),
            slots: BTreeMap::new(),
            wal: Vec::new(),
            versions: BTreeMap::new(),
            snapshot_at: None,
            walsender_generation: 0,
            slot_holds: BTreeMap::new(),
        }
    }
}

/// Snapshot a TableSchema as the Vec<RelationColumn> the sim stream
/// emits in `WalKind::Relation`. Mirrors what a real pgoutput
/// Relation message would look like — but the sim stamps
/// `is_primary_key` from the schema (we know it explicitly) instead
/// of inferring from REPLICA IDENTITY flags like prod does.
fn relation_columns_from_schema(schema: &TableSchema) -> Vec<pg2iceberg_pg::RelationColumn> {
    schema
        .columns
        .iter()
        .map(|c| pg2iceberg_pg::RelationColumn {
            name: c.name.clone(),
            ty: c.ty,
            is_primary_key: c.is_primary_key,
            nullable: c.nullable,
        })
        .collect()
}

impl DbState {
    fn alloc_lsn(&mut self) -> Lsn {
        self.next_lsn += 1;
        Lsn(self.next_lsn)
    }
    fn alloc_xid(&mut self) -> u32 {
        self.next_xid += 1;
        self.next_xid
    }
    fn alloc_oid(&mut self) -> u32 {
        self.next_oid += 1;
        self.next_oid
    }
    /// The WAL insert position, as `pg_current_wal_lsn()` reports it:
    /// the end of the last record, where the next one starts. Like
    /// Postgres, positions (this, a slot's, a keepalive's `wal_end`)
    /// are record ends, while a transaction's commit LSN is where its
    /// commit record starts.
    fn current_lsn(&self) -> Lsn {
        Lsn(self.next_lsn + 1)
    }

    /// The stream cursor (entries at or before it are skipped) for
    /// decoding from position `from`, as Postgres decodes: it skips a
    /// transaction that committed before `from` and sends one that
    /// commits at or after it whole. Transactions sit whole in the WAL,
    /// so only one whose commit record starts exactly at `from` begins
    /// before it — a consumer that acks commit LSNs gets its last acked
    /// transaction again after reconnecting.
    fn decoding_cursor(&self, from: Lsn) -> Lsn {
        let commit = self
            .wal
            .iter()
            .find(|e| e.lsn == from && matches!(e.kind, WalKind::Commit));
        let first = commit
            .and_then(|c| {
                self.wal
                    .iter()
                    .find(|e| e.xid == c.xid && matches!(e.kind, WalKind::Begin))
            })
            .map_or(from, |begin| begin.lsn);
        Lsn(first.0.saturating_sub(1))
    }
}

#[derive(Default, Clone)]
pub struct SimPostgres {
    state: Arc<Mutex<DbState>>,
}

/// A table's relation as a walsender describes it at some WAL position.
struct WireRelation {
    rel_id: u32,
    namespace: String,
    name: String,
    identity: ReplicaIdentity,
    cols: Vec<RelationCol>,
}

impl WireRelation {
    fn message(&self) -> Bytes {
        pgoutput::relation(
            self.rel_id,
            &self.namespace,
            &self.name,
            self.identity,
            &self.cols,
        )
    }

    fn tuple(&self, row: &Row, unchanged: &[ColumnName]) -> Vec<TupleValue> {
        self.cols
            .iter()
            .map(|c| {
                let name = ColumnName(c.name.clone());
                if unchanged.contains(&name) {
                    return TupleValue::Unchanged;
                }
                match row.get(&name).and_then(pgoutput::pg_text) {
                    Some(text) => TupleValue::Text(text),
                    None => TupleValue::Null,
                }
            })
            .collect()
    }

    /// `old`, as the old-tuple kind the table's replica identity sends.
    fn old<'a>(&self, old: &'a [TupleValue]) -> OldTuple<'a> {
        match self.identity {
            ReplicaIdentity::Full => OldTuple::Full(old),
            ReplicaIdentity::Default => OldTuple::Key(old),
        }
    }

    /// `evt`, whose old row is already shaped by replica identity.
    fn change(&self, evt: &ChangeEvent) -> Bytes {
        let row = |r: &Option<Row>| r.clone().unwrap_or_default();
        match evt.op {
            Op::Insert => pgoutput::insert(self.rel_id, &self.tuple(&row(&evt.after), &[])),
            Op::Update => {
                let new = self.tuple(&row(&evt.after), &evt.unchanged_cols);
                let old = evt.before.as_ref().map(|b| self.tuple(b, &[]));
                pgoutput::update(self.rel_id, old.as_deref().map(|t| self.old(t)), &new)
            }
            Op::Delete => {
                let old = self.tuple(&row(&evt.before), &[]);
                pgoutput::delete(self.rel_id, self.old(&old))
            }
            Op::Truncate => pgoutput::truncate(&[self.rel_id]),
            // Schema changes travel as WAL Relation records, not changes.
            Op::Relation => self.message(),
        }
    }
}

impl DbState {
    /// The commit LSN and timestamp of transaction `xid`, whose WAL
    /// starts at or after `from`.
    fn commit_of(&self, xid: u32, from: Lsn) -> (Lsn, Timestamp) {
        let tx = self
            .wal
            .iter()
            .filter(|e| e.lsn >= from && e.xid == Some(xid));
        let ts = tx
            .clone()
            .find_map(|e| match &e.kind {
                WalKind::Change(c) => Some(c.commit_ts),
                _ => None,
            })
            .unwrap_or(Timestamp(0));
        let commit = tx
            .clone()
            .find(|e| matches!(e.kind, WalKind::Commit))
            .map_or(from, |e| e.lsn);
        (commit, ts)
    }

    /// `ident`'s columns from the latest Relation record at or before
    /// WAL position `at`.
    fn columns_at(&self, ident: &TableIdent, at: Lsn) -> Option<&[pg2iceberg_pg::RelationColumn]> {
        self.wal.iter().rev().find_map(|e| match &e.kind {
            WalKind::Relation { ident: i, columns } if i == ident && e.lsn <= at => {
                Some(columns.as_slice())
            }
            _ => None,
        })
    }

    /// `ident`'s Relation message as of `at`, as production decodes it:
    /// pgoutput's key flag marks replica-identity columns, which
    /// production reads as primary key.
    fn relation_columns(
        &self,
        ident: &TableIdent,
        at: Lsn,
    ) -> Option<Vec<pg2iceberg_pg::RelationColumn>> {
        let full = self.tables.get(ident)?.replica_identity == ReplicaIdentity::Full;
        let columns = self.columns_at(ident, at)?;
        Some(
            columns
                .iter()
                .map(|c| {
                    let key = full || c.is_primary_key;
                    pg2iceberg_pg::RelationColumn {
                        name: c.name.clone(),
                        ty: c.ty,
                        is_primary_key: key,
                        nullable: !key,
                    }
                })
                .collect(),
        )
    }

    /// `ident`'s relation as of WAL position `at`.
    fn relation(&self, ident: &TableIdent, at: Lsn) -> Option<WireRelation> {
        let t = self.tables.get(ident)?;
        let columns = self.columns_at(ident, at)?;
        Some(WireRelation {
            rel_id: t.pg_oid,
            namespace: ident.namespace.0.join("."),
            name: ident.name.clone(),
            identity: t.replica_identity,
            cols: columns
                .iter()
                .map(|c| RelationCol {
                    name: c.name.clone(),
                    // Every column is part of a FULL replica identity.
                    key: t.replica_identity == ReplicaIdentity::Full || c.is_primary_key,
                    pg_type: t
                        .pg_types
                        .get(&c.name)
                        .copied()
                        .unwrap_or_else(|| pgoutput::default_pg_type(c.ty)),
                })
                .collect(),
        })
    }
}

impl SimPostgres {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn current_lsn(&self) -> Lsn {
        self.state.lock().unwrap().current_lsn()
    }

    /// Creates a user table and emits a Relation record into the WAL.
    pub fn create_table(&self, schema: TableSchema) -> Result<()> {
        let mut s = self.state.lock().unwrap();
        if s.tables.contains_key(&schema.ident) {
            return Err(SimError::DuplicateTable(schema.ident));
        }
        let ident = schema.ident.clone();
        let columns = relation_columns_from_schema(&schema);
        let pg_oid = s.alloc_oid();
        s.tables.insert(
            ident.clone(),
            TableData {
                schema,
                rows: BTreeMap::new(),
                pg_oid,
                replica_identity: ReplicaIdentity::Full,
                pg_types: BTreeMap::new(),
            },
        );
        let lsn = s.alloc_lsn();
        s.wal.push(WalEntry {
            lsn,
            xid: None,
            kind: WalKind::Relation { ident, columns },
        });
        Ok(())
    }

    /// Test hook: `ALTER TABLE … ADD COLUMN`. Appends `col` to the
    /// table's schema and emits a fresh Relation WAL event so any
    /// active replication stream picks up the change. The column
    /// auto-allocates the next field id (matching Iceberg's
    /// monotonic-only field-id rule). Used by DST to drive schema
    /// evolution end-to-end.
    pub fn alter_add_column(
        &self,
        ident: &TableIdent,
        col: pg2iceberg_core::ColumnSchema,
    ) -> Result<()> {
        let mut s = self.state.lock().unwrap();
        let table = s
            .tables
            .get_mut(ident)
            .ok_or_else(|| SimError::UnknownTable(ident.clone()))?;
        // Field id must be unique within the table; auto-pick.
        let next_id = table
            .schema
            .columns
            .iter()
            .map(|c| c.field_id)
            .max()
            .unwrap_or(0)
            + 1;
        let mut new_col = col;
        new_col.field_id = next_id;
        // Existing rows read the new column as NULL (no DEFAULT).
        for row in table.rows.values_mut() {
            row.insert(ColumnName(new_col.name.clone()), PgValue::Null);
        }
        table.schema.columns.push(new_col);
        let columns = relation_columns_from_schema(&table.schema);
        let lsn = s.alloc_lsn();
        s.wal.push(WalEntry {
            lsn,
            xid: None,
            kind: WalKind::Relation {
                ident: ident.clone(),
                columns,
            },
        });
        s.record_version(ident, lsn);
        Ok(())
    }

    /// Test hook: `ALTER TABLE … DROP COLUMN`. Removes the named
    /// column from the table's schema and emits a fresh Relation
    /// event. Iceberg-side the column stays, renamed out of the way and
    /// nullable, so older data files keep their values.
    pub fn alter_drop_column(&self, ident: &TableIdent, col_name: &str) -> Result<()> {
        let mut s = self.state.lock().unwrap();
        let table = s
            .tables
            .get_mut(ident)
            .ok_or_else(|| SimError::UnknownTable(ident.clone()))?;
        table.schema.columns.retain(|c| c.name != col_name);
        // The column's data goes with it.
        for row in table.rows.values_mut() {
            row.remove(&ColumnName(col_name.to_string()));
        }
        let columns = relation_columns_from_schema(&table.schema);
        let lsn = s.alloc_lsn();
        s.wal.push(WalEntry {
            lsn,
            xid: None,
            kind: WalKind::Relation {
                ident: ident.clone(),
                columns,
            },
        });
        s.record_version(ident, lsn);
        Ok(())
    }

    /// Something that invalidates the table's relation cache entry
    /// without changing its columns — `CREATE INDEX`, `ANALYZE`,
    /// `ALTER PUBLICATION`, a `VACUUM` that updates `pg_class`. pgoutput
    /// resends the table's Relation, unchanged, before its next change.
    pub fn invalidate_relation(&self, ident: &TableIdent) -> Result<()> {
        let mut s = self.state.lock().unwrap();
        let table = s
            .tables
            .get(ident)
            .ok_or_else(|| SimError::UnknownTable(ident.clone()))?;
        let columns = relation_columns_from_schema(&table.schema);
        let lsn = s.alloc_lsn();
        s.wal.push(WalEntry {
            lsn,
            xid: None,
            kind: WalKind::Relation {
                ident: ident.clone(),
                columns,
            },
        });
        Ok(())
    }

    /// Test hook: `ALTER TABLE … ALTER COLUMN … TYPE …`. Mutates the
    /// named column's `IcebergType` in place (preserving field id and
    /// nullability) and emits a fresh Relation event so the
    /// materializer's schema diff sees the type change. Used
    /// by DST to drive the legal-promotion + illegal-narrowing paths.
    /// The sim doesn't validate the change itself (real PG would do its
    /// own type-cast checks); validation happens downstream in
    /// `reconcile_columns` / `apply_schema_changes`.
    pub fn alter_column_type(
        &self,
        ident: &TableIdent,
        col_name: &str,
        new_ty: pg2iceberg_core::IcebergType,
    ) -> Result<()> {
        let mut s = self.state.lock().unwrap();
        let table = s
            .tables
            .get_mut(ident)
            .ok_or_else(|| SimError::UnknownTable(ident.clone()))?;
        let col = table
            .schema
            .columns
            .iter_mut()
            .find(|c| c.name == col_name)
            .ok_or_else(|| SimError::UnknownTable(ident.clone()))?;
        col.ty = new_ty;
        let columns = relation_columns_from_schema(&table.schema);
        let lsn = s.alloc_lsn();
        s.wal.push(WalEntry {
            lsn,
            xid: None,
            kind: WalKind::Relation {
                ident: ident.clone(),
                columns,
            },
        });
        s.record_version(ident, lsn);
        Ok(())
    }

    /// Test hook: drop a table and (optionally) recreate it under
    /// the same identifier. Models PG's `DROP TABLE` + recreate
    /// flow that yields a fresh `pg_class.oid`. Used by DST to
    /// drive the `TableIdentityChanged` startup invariant.
    pub fn drop_and_recreate_table(&self, schema: TableSchema) -> Result<()> {
        let mut s = self.state.lock().unwrap();
        s.tables.remove(&schema.ident);
        let ident = schema.ident.clone();
        let pg_oid = s.alloc_oid();
        s.tables.insert(
            ident,
            TableData {
                schema,
                rows: BTreeMap::new(),
                pg_oid,
                replica_identity: ReplicaIdentity::Full,
                pg_types: BTreeMap::new(),
            },
        );
        Ok(())
    }

    /// Test hook: peek at a table's current oid. Real PG users get
    /// this from `pg_class.oid`; the sim mirrors it via
    /// [`SimPgClient::table_oid`].
    /// The table's current columns, as `information_schema` lists them.
    pub fn table_schema(&self, ident: &TableIdent) -> Option<TableSchema> {
        self.state
            .lock()
            .unwrap()
            .tables
            .get(ident)
            .map(|t| t.schema.clone())
    }

    /// Open a snapshot at the current LSN: until [`Self::end_snapshot`],
    /// snapshot reads see the tables as of now, however they change —
    /// one REPEATABLE READ transaction across every chunk, as production
    /// reads them.
    pub fn begin_snapshot(&self) -> Lsn {
        let mut s = self.state.lock().unwrap();
        let lsn = s.current_lsn();
        s.snapshot_at = Some(lsn);
        lsn
    }

    pub fn end_snapshot(&self) {
        self.state.lock().unwrap().snapshot_at = None;
    }

    /// `ALTER TABLE … REPLICA IDENTITY`.
    pub fn set_replica_identity(&self, ident: &TableIdent, identity: ReplicaIdentity) {
        if let Some(t) = self.state.lock().unwrap().tables.get_mut(ident) {
            t.replica_identity = identity;
        }
    }

    /// Declare `column`'s Postgres type, where it isn't the default for
    /// its Iceberg type (e.g. `smallint` for an Iceberg `int`).
    pub fn set_pg_type(&self, ident: &TableIdent, column: &str, ty: PgType) {
        if let Some(t) = self.state.lock().unwrap().tables.get_mut(ident) {
            t.pg_types.insert(column.to_string(), ty);
        }
    }

    pub fn table_oid(&self, ident: &TableIdent) -> Option<u32> {
        self.state
            .lock()
            .unwrap()
            .tables
            .get(ident)
            .map(|t| t.pg_oid)
    }

    /// Test hook: drop a table from a publication. Models the
    /// `ALTER PUBLICATION DROP TABLE` operator action that triggers
    /// the `TableMissingFromPublication` startup invariant.
    pub fn drop_table_from_publication(&self, publication: &str, ident: &TableIdent) -> Result<()> {
        let mut s = self.state.lock().unwrap();
        let pubrec = s
            .publications
            .get_mut(publication)
            .ok_or_else(|| SimError::UnknownPublication(publication.into()))?;
        pubrec.tables.remove(ident);
        Ok(())
    }

    /// Test hook: add a table to an existing publication. Models the
    /// `ALTER PUBLICATION ADD TABLE` operator action that an operator
    /// runs when extending an existing pg2iceberg deployment to cover
    /// a new source table. Mirrors PG semantics: the slot keeps
    /// running, and `SimReplicationStream::recv` picks up the new
    /// publication membership on its next call (events emitted
    /// *before* this call don't get re-streamed — only future events
    /// for the new table flow through).
    pub fn add_table_to_publication(&self, publication: &str, ident: &TableIdent) -> Result<()> {
        let mut s = self.state.lock().unwrap();
        if !s.tables.contains_key(ident) {
            return Err(SimError::UnknownTable(ident.clone()));
        }
        let pubrec = s
            .publications
            .get_mut(publication)
            .ok_or_else(|| SimError::UnknownPublication(publication.into()))?;
        pubrec.tables.insert(ident.clone());
        Ok(())
    }

    /// Test hook: list a publication's current tables. Mirrors
    /// `pg_publication_tables`.
    pub fn publication_tables(&self, publication: &str) -> Vec<TableIdent> {
        let s = self.state.lock().unwrap();
        s.publications
            .get(publication)
            .map(|p| p.tables.iter().cloned().collect())
            .unwrap_or_default()
    }

    /// Test hook: drop a replication slot. Idempotent — returns
    /// `Ok(())` if the slot doesn't exist, mirroring the prod
    /// [`PgClient::drop_slot`] semantics. The sim has no notion of
    /// "active" slots (no concurrent consumer model), so this never
    /// rejects on activeness.
    pub fn drop_slot(&self, name: &str) -> Result<()> {
        let mut s = self.state.lock().unwrap();
        s.slots.remove(name);
        Ok(())
    }

    /// `pg_terminate_backend` on every walsender — or anything else that
    /// cuts their connections (a restart, a failover, a network drop).
    /// Each stream started so far fails its client's next read or write
    /// (see [`AsyncSimStream`]); the slots keep their positions.
    pub fn terminate_walsenders(&self) {
        self.state.lock().unwrap().walsender_generation += 1;
    }

    /// The next `attempts` `START_REPLICATION`s on `slot` fail, as they
    /// do while a dead connection's walsender still holds the slot —
    /// until `wal_sender_timeout` or TCP keepalives notice it's gone:
    /// `replication slot "…" is active for PID …`.
    pub fn hold_slot(&self, slot: &str, attempts: usize) {
        let mut s = self.state.lock().unwrap();
        s.slot_holds.insert(slot.to_string(), attempts);
    }

    /// Test hook: drop a publication. Idempotent — `IF EXISTS`
    /// semantics, mirrors the prod [`PgClient::drop_publication`].
    pub fn drop_publication(&self, name: &str) -> Result<()> {
        let mut s = self.state.lock().unwrap();
        s.publications.remove(name);
        Ok(())
    }

    pub fn create_publication(&self, name: &str, tables: &[TableIdent]) -> Result<()> {
        let mut s = self.state.lock().unwrap();
        if s.publications.contains_key(name) {
            return Err(SimError::DuplicatePublication(name.to_string()));
        }
        for t in tables {
            if !s.tables.contains_key(t) {
                return Err(SimError::UnknownTable(t.clone()));
            }
        }
        s.publications.insert(
            name.to_string(),
            Publication {
                tables: tables.iter().cloned().collect(),
            },
        );
        Ok(())
    }

    /// Creates a logical replication slot bound to a publication. Returns the
    /// slot's initial `restart_lsn` (the current WAL position).
    pub fn create_slot(&self, name: &str, publication: &str) -> Result<Lsn> {
        let mut s = self.state.lock().unwrap();
        if s.slots.contains_key(name) {
            return Err(SimError::DuplicateSlot(name.to_string()));
        }
        if !s.publications.contains_key(publication) {
            return Err(SimError::UnknownPublication(publication.to_string()));
        }
        let lsn = s.current_lsn();
        s.slots.insert(
            name.to_string(),
            SlotState {
                publication: publication.to_string(),
                restart_lsn: lsn,
                confirmed_flush_lsn: lsn,
                wal_status: SimWalStatus::default(),
                conflicting: false,
                safe_wal_size: 1024 * 1024 * 1024,
            },
        );
        Ok(lsn)
    }

    /// Test hook: force a slot's `wal_status` to model PG's
    /// `unreserved` / `lost` transitions. Used by DST to drive
    /// startup-validation + watcher coverage of the slot-loss path.
    pub fn set_slot_wal_status(&self, name: &str, status: SimWalStatus) -> Result<()> {
        let mut s = self.state.lock().unwrap();
        let slot = s
            .slots
            .get_mut(name)
            .ok_or_else(|| SimError::UnknownSlot(name.to_string()))?;
        slot.wal_status = status;
        Ok(())
    }

    /// Test hook: flip a slot's `conflicting` flag. Models PG 16+'s
    /// physical-replication-conflict slot kill.
    pub fn set_slot_conflicting(&self, name: &str, conflicting: bool) -> Result<()> {
        let mut s = self.state.lock().unwrap();
        let slot = s
            .slots
            .get_mut(name)
            .ok_or_else(|| SimError::UnknownSlot(name.to_string()))?;
        slot.conflicting = conflicting;
        Ok(())
    }

    pub fn slot_state(&self, name: &str) -> Result<SlotState> {
        let s = self.state.lock().unwrap();
        s.slots
            .get(name)
            .cloned()
            .ok_or_else(|| SimError::UnknownSlot(name.to_string()))
    }

    pub fn begin_tx(&self) -> TxHandle {
        let xid = {
            let mut s = self.state.lock().unwrap();
            s.alloc_xid()
        };
        TxHandle {
            db: self.clone(),
            xid,
            ops: Vec::new(),
            done: false,
        }
    }

    /// Snapshot of a table's rows in PK order, for tests / verify.
    /// `ident`'s column names as of WAL position `at`.
    pub fn columns_at(&self, ident: &TableIdent, at: Lsn) -> Vec<String> {
        let s = self.state.lock().unwrap();
        s.columns_at(ident, at)
            .map(|cols| cols.iter().map(|c| c.name.clone()).collect())
            .unwrap_or_default()
    }

    /// The columns `ident` had at `from` that it kept through `to`: a
    /// column dropped in between isn't one, even if one with its name was
    /// added since.
    pub fn columns_kept(&self, ident: &TableIdent, from: Lsn, to: Lsn) -> Vec<String> {
        let s = self.state.lock().unwrap();
        let mut kept: Vec<String> = s
            .columns_at(ident, from)
            .map(|cols| cols.iter().map(|c| c.name.clone()).collect())
            .unwrap_or_default();
        for e in s.wal.iter().filter(|e| e.lsn > from && e.lsn <= to) {
            if let WalKind::Relation { ident: i, columns } = &e.kind {
                if i == ident {
                    kept.retain(|name| columns.iter().any(|c| &c.name == name));
                }
            }
        }
        kept
    }

    /// Whether `ident`'s relation changed (DDL, or an invalidation) after
    /// its last row change. pgoutput tells a consumer about a schema
    /// change only with the table's next change, so it hasn't told one.
    pub fn relation_changed_since_last_change(&self, ident: &TableIdent) -> bool {
        let s = self.state.lock().unwrap();
        let relation = s.wal.iter().rev().find_map(|e| match &e.kind {
            WalKind::Relation { ident: i, .. } if i == ident => Some(e.lsn),
            _ => None,
        });
        let change = s.wal.iter().rev().find_map(|e| match &e.kind {
            WalKind::Change(c) if &c.table == ident => Some(e.lsn),
            _ => None,
        });
        relation > change
    }

    pub fn read_table(&self, ident: &TableIdent) -> Result<Vec<Row>> {
        let s = self.state.lock().unwrap();
        let t = s
            .tables
            .get(ident)
            .ok_or_else(|| SimError::UnknownTable(ident.clone()))?;
        Ok(t.rows.values().cloned().collect())
    }

    /// Test-only: returns every committed change event for tables in the
    /// given publication, ordered by LSN. Used by DST invariant checks to
    /// compare the WAL "ground truth" against staged Parquet contents.
    /// [`Self::dump_change_events`] as a walsender sends them: old rows
    /// shaped by each table's replica identity.
    pub fn dump_change_events_as_sent(&self, publication: &str) -> Result<Vec<ChangeEvent>> {
        let events = self.dump_change_events(publication)?;
        let s = self.state.lock().unwrap();
        Ok(events
            .into_iter()
            .map(|e| match s.tables.get(&e.table) {
                Some(t) => t.shape_old_row(e),
                None => e,
            })
            .collect())
    }

    /// The LSN of transaction `xid`'s commit record.
    pub fn commit_lsn(&self, xid: u32) -> Lsn {
        self.state.lock().unwrap().commit_of(xid, Lsn::ZERO).0
    }

    pub fn dump_change_events(&self, publication: &str) -> Result<Vec<ChangeEvent>> {
        let s = self.state.lock().unwrap();
        let pub_tables = &s
            .publications
            .get(publication)
            .ok_or_else(|| SimError::UnknownPublication(publication.to_string()))?
            .tables;
        let mut out = Vec::new();
        for entry in &s.wal {
            if let WalKind::Change(ce) = &entry.kind {
                if pub_tables.contains(&ce.table) {
                    out.push(ce.clone());
                }
            }
        }
        Ok(out)
    }

    /// Open a replication stream for the slot. Cursor starts at the slot's
    /// `restart_lsn` (so reconnects replay from that point).
    pub fn start_replication(&self, slot: &str) -> Result<SimReplicationStream> {
        self.start_replication_at(slot, Lsn::ZERO)
    }

    /// `START_REPLICATION SLOT slot LOGICAL start`: decoding starts at
    /// the later of `start` and the slot's `confirmed_flush_lsn` (see
    /// [`DbState::decoding_cursor`] for which transactions that sends).
    pub fn start_replication_at(&self, slot: &str, start: Lsn) -> Result<SimReplicationStream> {
        let mut s = self.state.lock().unwrap();
        let slot_state = s
            .slots
            .get(slot)
            .cloned()
            .ok_or_else(|| SimError::UnknownSlot(slot.to_string()))?;
        if let Some(holds) = s.slot_holds.get_mut(slot).filter(|n| **n > 0) {
            *holds -= 1;
            return Err(SimError::SlotActive(slot.to_string()));
        }
        let cursor_lsn = s.decoding_cursor(slot_state.confirmed_flush_lsn.max(start));
        Ok(SimReplicationStream {
            db: self.clone(),
            slot: slot.to_string(),
            publication: slot_state.publication,
            cursor_lsn,
            keepalive_sent: Lsn::ZERO,
            pending: VecDeque::new(),
            wire_queue: VecDeque::new(),
            relations_sent: BTreeSet::new(),
            generation: s.walsender_generation,
        })
    }
}

pub struct TxHandle {
    db: SimPostgres,
    xid: u32,
    ops: Vec<TxOp>,
    done: bool,
}

#[derive(Clone, Debug)]
enum TxOp {
    Insert {
        table: TableIdent,
        row: Row,
    },
    Update {
        table: TableIdent,
        new_row: Row,
        unchanged_cols: Vec<ColumnName>,
    },
    /// PG `UPDATE` that changes the primary key. Emits an event with
    /// `before` = old-PK row, `after` = new full row. The sim removes
    /// the row at the old PK and inserts at the new PK. Mirrors what
    /// real PG sends with `REPLICA IDENTITY FULL` (or `DEFAULT` when
    /// the PK column itself changes).
    UpdatePkChange {
        table: TableIdent,
        before_row: Row,
        new_row: Row,
        unchanged_cols: Vec<ColumnName>,
    },
    Delete {
        table: TableIdent,
        pk_row: Row,
    },
    /// `TRUNCATE` — clears all rows and emits an `Op::Truncate`
    /// ChangeEvent with no before/after, matching pgoutput's
    /// behavior.
    Truncate {
        table: TableIdent,
        /// Part of the previous op's `TRUNCATE` statement: one WAL
        /// record, so one LSN and one pgoutput message for all of them.
        same_statement: bool,
    },
}

/// An UPDATE's `new_row` as the table stores it and as pgoutput sends
/// it, given the TOASTed columns it leaves `unchanged`: the table keeps
/// their `old` values; the WAL carries `NULL` in their place (the
/// unchanged marker decodes to `NULL` plus an `unchanged_cols` entry).
fn toast(new_row: Row, old: Option<&Row>, unchanged: &[ColumnName]) -> (Row, Row) {
    let mut stored = new_row.clone();
    let mut sent = new_row;
    for col in unchanged {
        match old.and_then(|o| o.get(col)) {
            Some(v) => stored.insert(col.clone(), v.clone()),
            None => stored.remove(col),
        };
        sent.insert(col.clone(), PgValue::Null);
    }
    (stored, sent)
}

impl TxHandle {
    pub fn xid(&self) -> u32 {
        self.xid
    }

    pub fn insert(&mut self, table: &TableIdent, row: Row) -> &mut Self {
        self.ops.push(TxOp::Insert {
            table: table.clone(),
            row,
        });
        self
    }

    pub fn update(&mut self, table: &TableIdent, new_row: Row) -> &mut Self {
        self.ops.push(TxOp::Update {
            table: table.clone(),
            new_row,
            unchanged_cols: Vec::new(),
        });
        self
    }

    /// `UPDATE` that leaves TOASTed columns `unchanged`, as pgoutput
    /// reports it: the table keeps their old values (whatever `new_row`
    /// holds for them is ignored), and the WAL row carries `NULL` plus an
    /// `unchanged_cols` marker in their place.
    pub fn update_with_unchanged(
        &mut self,
        table: &TableIdent,
        new_row: Row,
        unchanged: Vec<ColumnName>,
    ) -> &mut Self {
        self.ops.push(TxOp::Update {
            table: table.clone(),
            new_row,
            unchanged_cols: unchanged,
        });
        self
    }

    pub fn delete(&mut self, table: &TableIdent, pk_row: Row) -> &mut Self {
        self.ops.push(TxOp::Delete {
            table: table.clone(),
            pk_row,
        });
        self
    }

    /// `UPDATE` that changes the primary key. Use when the test
    /// needs to reproduce the "old PK becomes orphan in Iceberg"
    /// scenario. `before_row` should be the full old row (matches
    /// `REPLICA IDENTITY FULL`); `new_row` is the full new row.
    pub fn update_with_pk_change(
        &mut self,
        table: &TableIdent,
        before_row: Row,
        new_row: Row,
    ) -> &mut Self {
        self.update_with_pk_change_unchanged(table, before_row, new_row, Vec::new())
    }

    /// [`Self::update_with_pk_change`] that leaves TOASTed columns
    /// `unchanged`: pgoutput still sends them as unchanged markers when
    /// the key moves (see [`Self::update_with_unchanged`]).
    pub fn update_with_pk_change_unchanged(
        &mut self,
        table: &TableIdent,
        before_row: Row,
        new_row: Row,
        unchanged: Vec<ColumnName>,
    ) -> &mut Self {
        self.ops.push(TxOp::UpdatePkChange {
            table: table.clone(),
            before_row,
            new_row,
            unchanged_cols: unchanged,
        });
        self
    }

    /// `TRUNCATE` the table. Drops every row and emits a single
    /// `Op::Truncate` event with no payload, matching pgoutput.
    pub fn truncate(&mut self, table: &TableIdent) -> &mut Self {
        self.truncate_all(std::slice::from_ref(table))
    }

    /// One `TRUNCATE` statement naming several tables: one WAL record,
    /// which pgoutput sends as one Truncate message.
    pub fn truncate_all(&mut self, tables: &[TableIdent]) -> &mut Self {
        for (i, table) in tables.iter().enumerate() {
            self.ops.push(TxOp::Truncate {
                table: table.clone(),
                same_statement: i > 0,
            });
        }
        self
    }

    /// Atomically apply ops and append `Begin / Change* / Commit` to the WAL.
    /// Returns the commit LSN — the LSN that crosses the durability boundary.
    pub fn commit(mut self, commit_ts: Timestamp) -> Result<Lsn> {
        self.done = true;
        let mut s = self.db.state.lock().unwrap();

        // Pre-validate ops — in order, against a scratch copy of each
        // touched table's keys — so we either fully apply or fully bail
        // (PG's transaction semantics), and so later ops see earlier ones,
        // as in PG: a row this transaction inserted can be updated or
        // deleted by it.
        let mut keys: BTreeMap<TableIdent, BTreeSet<String>> = BTreeMap::new();
        for op in &self.ops {
            let table = match op {
                TxOp::Insert { table, .. }
                | TxOp::Update { table, .. }
                | TxOp::Delete { table, .. }
                | TxOp::UpdatePkChange { table, .. }
                | TxOp::Truncate { table, .. } => table,
            };
            let t = s
                .tables
                .get(table)
                .ok_or_else(|| SimError::UnknownTable(table.clone()))?;
            let live = keys
                .entry(table.clone())
                .or_insert_with(|| t.rows.keys().cloned().collect());
            match op {
                TxOp::Insert { row, .. } => {
                    let key = t.pk_key(row)?;
                    if !live.insert(key.clone()) {
                        return Err(SimError::PkConflict {
                            table: table.clone(),
                            detail: format!("duplicate pk {key}"),
                        });
                    }
                }
                TxOp::Update { new_row, .. } => {
                    if !live.contains(&t.pk_key(new_row)?) {
                        return Err(SimError::RowNotFound {
                            table: table.clone(),
                            op: "update",
                        });
                    }
                }
                TxOp::Delete { pk_row, .. } => {
                    if !live.remove(&t.pk_key(pk_row)?) {
                        return Err(SimError::RowNotFound {
                            table: table.clone(),
                            op: "delete",
                        });
                    }
                }
                TxOp::UpdatePkChange {
                    before_row,
                    new_row,
                    ..
                } => {
                    if !live.remove(&t.pk_key(before_row)?) {
                        return Err(SimError::RowNotFound {
                            table: table.clone(),
                            op: "update-pk-change(before)",
                        });
                    }
                    let new_key = t.pk_key(new_row)?;
                    if !live.insert(new_key.clone()) {
                        return Err(SimError::PkConflict {
                            table: table.clone(),
                            detail: format!("update-pk-change collides at {new_key}"),
                        });
                    }
                }
                TxOp::Truncate { .. } => live.clear(),
            }
        }

        // Allocate LSNs: Begin + one per change + Commit.
        let begin_lsn = s.alloc_lsn();
        s.wal.push(WalEntry {
            lsn: begin_lsn,
            xid: Some(self.xid),
            kind: WalKind::Begin,
        });

        for op in std::mem::take(&mut self.ops) {
            match op {
                TxOp::Insert { table, row } => {
                    let lsn = s.alloc_lsn();
                    let t = s.tables.get_mut(&table).expect("validated above");
                    let key = t.pk_key(&row)?;
                    t.rows.insert(key, row.clone());
                    s.wal.push(WalEntry {
                        lsn,
                        xid: Some(self.xid),
                        kind: WalKind::Change(ChangeEvent {
                            table,
                            op: Op::Insert,
                            lsn,
                            commit_ts,
                            xid: Some(self.xid),
                            before: None,
                            after: Some(row),
                            unchanged_cols: vec![],
                        }),
                    });
                }
                TxOp::Update {
                    table,
                    new_row,
                    unchanged_cols,
                } => {
                    let lsn = s.alloc_lsn();
                    let t = s.tables.get_mut(&table).expect("validated above");
                    let key = t.pk_key(&new_row)?;
                    let before = t.rows.get(&key).cloned();
                    let (stored, sent) = toast(new_row, before.as_ref(), &unchanged_cols);
                    t.rows.insert(key, stored);
                    s.wal.push(WalEntry {
                        lsn,
                        xid: Some(self.xid),
                        kind: WalKind::Change(ChangeEvent {
                            table,
                            op: Op::Update,
                            lsn,
                            commit_ts,
                            xid: Some(self.xid),
                            before,
                            after: Some(sent),
                            unchanged_cols,
                        }),
                    });
                }
                TxOp::Delete { table, pk_row } => {
                    let lsn = s.alloc_lsn();
                    let t = s.tables.get_mut(&table).expect("validated above");
                    let key = t.pk_key(&pk_row)?;
                    let before = t.rows.remove(&key);
                    s.wal.push(WalEntry {
                        lsn,
                        xid: Some(self.xid),
                        kind: WalKind::Change(ChangeEvent {
                            table,
                            op: Op::Delete,
                            lsn,
                            commit_ts,
                            xid: Some(self.xid),
                            before,
                            after: None,
                            unchanged_cols: vec![],
                        }),
                    });
                }
                TxOp::UpdatePkChange {
                    table,
                    before_row,
                    new_row,
                    unchanged_cols,
                } => {
                    let lsn = s.alloc_lsn();
                    let t = s.tables.get_mut(&table).expect("validated above");
                    let old_key = t.pk_key(&before_row)?;
                    let new_key = t.pk_key(&new_row)?;
                    let old_row = t.rows.remove(&old_key);
                    let (stored, sent) = toast(new_row, old_row.as_ref(), &unchanged_cols);
                    t.rows.insert(new_key, stored);
                    s.wal.push(WalEntry {
                        lsn,
                        xid: Some(self.xid),
                        kind: WalKind::Change(ChangeEvent {
                            table,
                            op: Op::Update,
                            lsn,
                            commit_ts,
                            xid: Some(self.xid),
                            before: Some(before_row),
                            after: Some(sent),
                            unchanged_cols,
                        }),
                    });
                }
                TxOp::Truncate {
                    table,
                    same_statement,
                } => {
                    let lsn = match s.wal.last() {
                        Some(prev) if same_statement => prev.lsn,
                        _ => s.alloc_lsn(),
                    };
                    let t = s.tables.get_mut(&table).expect("validated above");
                    t.rows.clear();
                    s.wal.push(WalEntry {
                        lsn,
                        xid: Some(self.xid),
                        kind: WalKind::Change(ChangeEvent {
                            table,
                            op: Op::Truncate,
                            lsn,
                            commit_ts,
                            xid: Some(self.xid),
                            before: None,
                            after: None,
                            unchanged_cols: vec![],
                        }),
                    });
                }
            }
        }

        let commit_lsn = s.alloc_lsn();
        s.wal.push(WalEntry {
            lsn: commit_lsn,
            xid: Some(self.xid),
            kind: WalKind::Commit,
        });
        let touched: BTreeSet<TableIdent> = s
            .wal
            .iter()
            .rev()
            .take_while(|e| e.xid == Some(self.xid))
            .filter_map(|e| match &e.kind {
                WalKind::Change(c) => Some(c.table.clone()),
                _ => None,
            })
            .collect();
        for t in touched {
            s.record_version(&t, commit_lsn);
        }

        Ok(commit_lsn)
    }

    /// Drop buffered ops without writing to the WAL. Real PG aborts roll back
    /// state changes but still consume some WAL bytes for bookkeeping; we
    /// don't model that — rollback is silent.
    pub fn rollback(mut self) {
        self.done = true;
        self.ops.clear();
    }
}

impl Drop for TxHandle {
    fn drop(&mut self) {
        if !self.done {
            // Implicit rollback. Useful for early-return paths in tests.
            self.ops.clear();
        }
    }
}

/// Sync replication stream over a SimPostgres slot.
///
/// `recv` returns `None` once the cursor reaches the end of the WAL — callers
/// poll until new entries appear. `send_standby` advances both
/// `confirmed_flush_lsn` and `restart_lsn`, mirroring how the production
/// pipeline acks the slot.
///
/// Models two pgoutput (PG 15+) behaviours that matter for slot
/// advancement: transactions with no change for the publication are
/// skipped entirely (no Begin/Commit), and once caught up the walsender
/// sends a keepalive carrying its WAL position if the slot hasn't
/// confirmed it. Together they're the only way the consumer learns it
/// may ack past WAL that carried nothing for it.
pub struct SimReplicationStream {
    db: SimPostgres,
    slot: String,
    publication: String,
    cursor_lsn: Lsn,
    /// `wal_end` of the last keepalive sent, so a caught-up stream
    /// sends one per new position instead of on every `recv`.
    keepalive_sent: Lsn,
    /// Decoded messages [`Self::recv`] still has to hand out.
    pending: VecDeque<DecodedMessage>,
    /// Encoded messages [`Self::recv_wire`] still has to hand out.
    wire_queue: VecDeque<WireMessage>,
    /// Tables whose Relation message this session has sent since their
    /// relation cache entry was last invalidated.
    relations_sent: BTreeSet<TableIdent>,
    /// [`DbState::walsender_generation`] when this stream started.
    generation: u64,
}

/// What a walsender sends: a pgoutput message (an `XLogData` payload),
/// or a keepalive.
#[derive(Clone, Debug)]
pub enum WireMessage {
    Pgoutput(Bytes),
    Keepalive { wal_end: Lsn, reply_requested: bool },
}

impl SimReplicationStream {
    pub fn slot_name(&self) -> &str {
        &self.slot
    }

    /// [`Self::recv`], encoded as a walsender sends it: pgoutput bytes,
    /// with values in Postgres text format, unchanged TOAST columns as
    /// markers, old rows shaped by replica identity, `Begin` carrying the
    /// commit record's LSN, and a multi-table TRUNCATE as one message.
    pub fn recv_wire(&mut self) -> Option<WireMessage> {
        if let Some(m) = self.wire_queue.pop_front() {
            return Some(m);
        }
        let record: Vec<DecodedMessage> = if self.pending.is_empty() {
            self.next_record()?
        } else {
            self.pending.drain(..).collect()
        };
        let s = self.db.state.lock().unwrap();
        let mut out: Vec<WireMessage> = Vec::new();
        // The tables of the record's TRUNCATE statement, sent last.
        let mut truncated = Vec::new();
        for msg in record {
            match msg {
                DecodedMessage::Begin { final_lsn, xid } => {
                    let (commit_lsn, ts) = s.commit_of(xid, final_lsn);
                    out.push(WireMessage::Pgoutput(pgoutput::begin(commit_lsn, ts, xid)));
                }
                DecodedMessage::Commit { commit_lsn, xid } => {
                    let (_, ts) = s.commit_of(xid, Lsn::ZERO);
                    out.push(WireMessage::Pgoutput(pgoutput::commit(
                        commit_lsn, commit_lsn, ts,
                    )));
                }
                DecodedMessage::Relation { ident, .. } => {
                    if let Some(rel) = s.relation(&ident, self.cursor_lsn) {
                        out.push(WireMessage::Pgoutput(rel.message()));
                    }
                }
                DecodedMessage::Change(evt) => {
                    let rel = s.relation(&evt.table, evt.lsn)?;
                    if matches!(evt.op, Op::Truncate) {
                        truncated.push(rel.rel_id);
                    } else {
                        out.push(WireMessage::Pgoutput(rel.change(&evt)));
                    }
                }
                DecodedMessage::Keepalive {
                    wal_end,
                    reply_requested,
                } => out.push(WireMessage::Keepalive {
                    wal_end,
                    reply_requested,
                }),
            }
        }
        if !truncated.is_empty() {
            out.push(WireMessage::Pgoutput(pgoutput::truncate(&truncated)));
        }
        drop(s);
        self.wire_queue.extend(out);
        self.wire_queue.pop_front()
    }

    /// Returns the next message at or after the cursor that's allowed by the
    /// publication. Cursor advances past whatever is returned.
    ///
    /// Like pgoutput, it sends a table's Relation message just before the
    /// first change to it since the session started or since its
    /// relation cache entry was last invalidated: by DDL (or anything
    /// else that invalidates it, such as `CREATE INDEX`), or by a
    /// TRUNCATE, which invalidates it before its own message and again
    /// at commit. Columns are flagged as key the way pgoutput flags
    /// them, which under `REPLICA IDENTITY FULL` is every column.
    pub fn recv(&mut self) -> Option<DecodedMessage> {
        if self.pending.is_empty() {
            let record = self.next_record()?;
            self.pending.extend(record);
        }
        self.pending.pop_front()
    }

    /// The messages for the next WAL record at or after the cursor that
    /// the publication sends; the cursor advances past it.
    fn next_record(&mut self) -> Option<Vec<DecodedMessage>> {
        let s = self.db.state.lock().unwrap();
        let pub_tables = &s
            .publications
            .get(&self.publication)
            .expect("slot points to existing publication")
            .tables;

        for (i, entry) in s.wal.iter().enumerate() {
            if entry.lsn <= self.cursor_lsn {
                continue;
            }
            match &entry.kind {
                WalKind::Begin => {
                    // pgoutput skips a transaction with nothing for the
                    // publication — no Begin/Commit at all. Jump the
                    // cursor to its Commit; only a later keepalive tells
                    // the consumer it can ack past it.
                    let tx = s.wal[i + 1..].iter().filter(|e| e.xid == entry.xid);
                    let publishes = tx.clone().any(|e| {
                        matches!(&e.kind, WalKind::Change(evt) if pub_tables.contains(&evt.table))
                    });
                    if !publishes {
                        if let Some(commit) =
                            tx.into_iter().find(|e| matches!(e.kind, WalKind::Commit))
                        {
                            self.cursor_lsn = commit.lsn;
                        }
                        continue;
                    }
                    self.cursor_lsn = entry.lsn;
                    return Some(vec![DecodedMessage::Begin {
                        final_lsn: entry.lsn,
                        xid: entry.xid.unwrap_or(0),
                    }]);
                }
                WalKind::Commit => {
                    self.cursor_lsn = entry.lsn;
                    // The transaction's own invalidations take effect at
                    // its commit: a table it truncated gets a fresh
                    // Relation before its next change.
                    for e in s.wal[..i].iter().rev().take_while(|e| e.xid == entry.xid) {
                        if let WalKind::Change(evt) = &e.kind {
                            if matches!(evt.op, Op::Truncate) {
                                self.relations_sent.remove(&evt.table);
                            }
                        }
                    }
                    return Some(vec![DecodedMessage::Commit {
                        commit_lsn: entry.lsn,
                        xid: entry.xid.unwrap_or(0),
                    }]);
                }
                WalKind::Relation { ident, .. } => {
                    // DDL changes no rows; it invalidates the table's
                    // relation cache entry, so the next change to it
                    // brings a fresh Relation.
                    self.relations_sent.remove(ident);
                    self.cursor_lsn = entry.lsn;
                }
                WalKind::Change(_) => {
                    // One WAL record: a multi-table TRUNCATE is several
                    // entries at one LSN.
                    let changes: Vec<&ChangeEvent> = s.wal[i..]
                        .iter()
                        .take_while(|e| e.lsn == entry.lsn)
                        .filter_map(|e| match &e.kind {
                            WalKind::Change(evt) if pub_tables.contains(&evt.table) => Some(evt),
                            _ => None,
                        })
                        .collect();
                    self.cursor_lsn = entry.lsn;
                    if changes.is_empty() {
                        continue;
                    }
                    for evt in &changes {
                        if matches!(evt.op, Op::Truncate) {
                            self.relations_sent.remove(&evt.table);
                        }
                    }
                    let mut out = Vec::new();
                    for evt in &changes {
                        if self.relations_sent.contains(&evt.table) {
                            continue;
                        }
                        if let Some(columns) = s.relation_columns(&evt.table, entry.lsn) {
                            self.relations_sent.insert(evt.table.clone());
                            out.push(DecodedMessage::Relation {
                                ident: evt.table.clone(),
                                columns,
                            });
                        }
                    }
                    for evt in changes {
                        let evt = match s.tables.get(&evt.table) {
                            Some(t) => t.shape_old_row(evt.clone()),
                            None => evt.clone(),
                        };
                        out.push(DecodedMessage::Change(evt));
                    }
                    return Some(out);
                }
            }
        }

        // Caught up. Like the walsender before it sleeps, send a
        // keepalive carrying the position decoded so far — the end of
        // the last record decoded — if the slot hasn't confirmed it;
        // once per position, so an idle stream still returns `None`.
        let confirmed = s
            .slots
            .get(&self.slot)
            .map(|slot| slot.confirmed_flush_lsn)
            .unwrap_or(Lsn::ZERO);
        let wal_end = Lsn(self.cursor_lsn.0 + 1);
        if wal_end > confirmed && wal_end > self.keepalive_sent {
            self.keepalive_sent = wal_end;
            return Some(vec![DecodedMessage::Keepalive {
                wal_end,
                reply_requested: false,
            }]);
        }
        None
    }

    /// Acknowledge that `flushed` is durably committed downstream. Advances
    /// the slot's `confirmed_flush_lsn` and `restart_lsn` (mirroring the
    /// pipeline's standby ack).
    pub fn send_standby(&mut self, flushed: Lsn) {
        let mut s = self.db.state.lock().unwrap();
        if let Some(slot) = s.slots.get_mut(&self.slot) {
            if flushed > slot.confirmed_flush_lsn {
                slot.confirmed_flush_lsn = flushed;
            }
            if flushed > slot.restart_lsn {
                slot.restart_lsn = flushed;
            }
        }
    }

    pub fn cursor_lsn(&self) -> Lsn {
        self.cursor_lsn
    }

    /// Whether this stream's walsender was terminated since it started
    /// (see [`SimPostgres::terminate_walsenders`]).
    pub fn is_terminated(&self) -> bool {
        self.db.state.lock().unwrap().walsender_generation != self.generation
    }
}

/// Wraps a `SimReplicationStream` so it satisfies the
/// [`pg2iceberg_pg::ReplicationStream`] trait. The async `recv`
/// returns immediately when a message is available; on empty queue,
/// it returns `Pending` so the binary's tokio `select!` picks the
/// timeout branch.
///
/// This lets the production main loop (in `pg2iceberg-validate`) be
/// reused unchanged with sim plumbing — the fault-DST exercises the
/// same code path as the binary.
pub struct AsyncSimStream {
    inner: SimReplicationStream,
}

impl AsyncSimStream {
    pub fn new(inner: SimReplicationStream) -> Self {
        Self { inner }
    }

    pub fn inner_mut(&mut self) -> &mut SimReplicationStream {
        &mut self.inner
    }
}

#[async_trait]
impl pg2iceberg_pg::ReplicationStream for AsyncSimStream {
    async fn recv(
        &mut self,
    ) -> std::result::Result<pg2iceberg_pg::DecodedMessage, pg2iceberg_pg::PgError> {
        if self.inner.is_terminated() {
            return Err(terminated());
        }
        match self.inner.recv() {
            Some(msg) => Ok(msg),
            None => {
                // Sim queue is drained. Return Pending so the binary's
                // `select!` picks the timer branch. In the binary
                // this means "wait for the next replication event or
                // timer tick" — same semantics as prod's blocking
                // recv.
                std::future::pending().await
            }
        }
    }

    async fn send_standby(
        &mut self,
        flushed: Lsn,
        _applied: Lsn,
    ) -> std::result::Result<(), pg2iceberg_pg::PgError> {
        if self.inner.is_terminated() {
            return Err(terminated());
        }
        self.inner.send_standby(flushed);
        Ok(())
    }
}

/// What the client of a terminated walsender reads: the server's FATAL,
/// then a closed connection.
fn terminated() -> pg2iceberg_pg::PgError {
    pg2iceberg_pg::PgError::Connection(
        "FATAL: terminating connection due to administrator command".into(),
    )
}

#[async_trait]
impl SnapshotSource for SimPostgres {
    async fn snapshot_lsn(&self) -> std::result::Result<Lsn, SnapshotError> {
        let s = self.state.lock().unwrap();
        Ok(s.snapshot_at.unwrap_or_else(|| s.current_lsn()))
    }

    async fn read_chunk(
        &self,
        ident: &TableIdent,
        chunk_size: usize,
        after_pk_key: Option<&str>,
    ) -> std::result::Result<Vec<Row>, SnapshotError> {
        let s = self.state.lock().unwrap();
        let table = s
            .tables
            .get(ident)
            .ok_or_else(|| SnapshotError::Source(format!("unknown table: {ident}")))?;

        // An open snapshot reads the table as of its LSN, like the
        // REPEATABLE READ transaction production's snapshot reads in.
        let at_snapshot;
        let rows = match s.snapshot_at {
            Some(lsn) => {
                at_snapshot = s.rows_at(ident, lsn);
                &at_snapshot
            }
            None => &table.rows,
        };
        // SimPostgres stores rows keyed by canonical PK in a BTreeMap, so
        // iteration is already sorted ASC by PK. Filter strictly above the
        // bound, then truncate.
        let chunk: Vec<Row> = rows
            .iter()
            .filter(|(k, _)| match after_pk_key {
                Some(after) => k.as_str() > after,
                None => true,
            })
            .take(chunk_size)
            .map(|(_, v)| v.clone())
            .collect();

        Ok(chunk)
    }
}

#[async_trait]
impl SlotMonitor for SimPostgres {
    async fn confirmed_flush_lsn(&self, slot: &str) -> std::result::Result<Option<Lsn>, PgError> {
        match self.slot_state(slot) {
            Ok(s) => Ok(Some(s.confirmed_flush_lsn)),
            Err(_) => Ok(None),
        }
    }
}

/// `SimPostgres`-backed [`PgClient`] for tests. Adapts the sim's
/// methods (which take a publication argument at create_slot time, and
/// don't have an `export_snapshot` concept) to the prod
/// `PgClient` trait surface.
///
/// Lets the same library lifecycle helper
/// (`pg2iceberg_validate::run_logical_lifecycle`) drive both prod and
/// sim end-to-end, so the fault-DST exercises slot creation,
/// publication creation, and start_replication semantics — not just
/// the loop body.
pub struct SimPgClient {
    db: SimPostgres,
    /// `PgClient::create_slot(slot)` doesn't take a publication; the
    /// sim's slot model requires one. We track the most-recently
    /// created publication here so create_slot can bind to it.
    /// Mirrors prod's loose coupling: prod's `CREATE_REPLICATION_SLOT`
    /// also doesn't bind a publication; the publication is supplied at
    /// `START_REPLICATION` time.
    pending_publication: std::sync::Mutex<Option<String>>,
}

impl SimPgClient {
    pub fn new(db: SimPostgres) -> Self {
        Self {
            db,
            pending_publication: std::sync::Mutex::new(None),
        }
    }

    pub fn db(&self) -> &SimPostgres {
        &self.db
    }
}

#[async_trait]
impl PgClient for SimPgClient {
    async fn create_publication(
        &self,
        name: &str,
        tables: &[TableIdent],
    ) -> std::result::Result<(), PgError> {
        self.db
            .create_publication(name, tables)
            .map_err(|e| PgError::Other(e.to_string()))?;
        *self.pending_publication.lock().unwrap() = Some(name.to_string());
        Ok(())
    }

    async fn create_slot(&self, slot: &str) -> std::result::Result<Lsn, PgError> {
        let pub_ = self
            .pending_publication
            .lock()
            .unwrap()
            .clone()
            .ok_or_else(|| {
                PgError::Other(
                    "SimPgClient::create_slot called before create_publication; \
                     prod expects an inverse order matching CREATE_REPLICATION_SLOT \
                     followed by START_REPLICATION ... publication_names ..., but \
                     the sim binds at create_slot time."
                        .into(),
                )
            })?;
        self.db
            .create_slot(slot, &pub_)
            .map_err(|e| PgError::Other(e.to_string()))
    }

    async fn slot_exists(&self, slot: &str) -> std::result::Result<bool, PgError> {
        Ok(self.db.slot_state(slot).is_ok())
    }

    async fn slot_restart_lsn(&self, slot: &str) -> std::result::Result<Option<Lsn>, PgError> {
        match self.db.slot_state(slot) {
            Ok(s) => Ok(Some(s.restart_lsn)),
            Err(_) => Ok(None),
        }
    }

    async fn slot_confirmed_flush_lsn(
        &self,
        slot: &str,
    ) -> std::result::Result<Option<Lsn>, PgError> {
        match self.db.slot_state(slot) {
            Ok(s) => Ok(Some(s.confirmed_flush_lsn)),
            Err(_) => Ok(None),
        }
    }

    async fn slot_health(
        &self,
        slot: &str,
    ) -> std::result::Result<Option<pg2iceberg_pg::SlotHealth>, PgError> {
        let s = match self.db.slot_state(slot) {
            Ok(s) => s,
            Err(_) => return Ok(None),
        };
        let wal_status = Some(match s.wal_status {
            crate::postgres::SimWalStatus::Reserved => pg2iceberg_pg::WalStatus::Reserved,
            crate::postgres::SimWalStatus::Extended => pg2iceberg_pg::WalStatus::Extended,
            crate::postgres::SimWalStatus::Unreserved => pg2iceberg_pg::WalStatus::Unreserved,
            crate::postgres::SimWalStatus::Lost => pg2iceberg_pg::WalStatus::Lost,
        });
        Ok(Some(pg2iceberg_pg::SlotHealth {
            exists: true,
            restart_lsn: s.restart_lsn,
            confirmed_flush_lsn: s.confirmed_flush_lsn,
            wal_status,
            conflicting: s.conflicting,
            safe_wal_size: Some(s.safe_wal_size),
        }))
    }

    async fn export_snapshot(&self) -> std::result::Result<SnapshotId, PgError> {
        // Sim doesn't have a notion of exported snapshot — the sim's
        // SnapshotSource impl reads at the current LSN directly.
        // Return a placeholder ID; callers that actually use the
        // string would be testing prod-only behavior.
        Ok(SnapshotId("sim-placeholder".into()))
    }

    async fn table_oid(
        &self,
        namespace: &str,
        name: &str,
    ) -> std::result::Result<Option<u32>, PgError> {
        let ident = TableIdent {
            namespace: pg2iceberg_core::Namespace(vec![namespace.into()]),
            name: name.into(),
        };
        Ok(self.db.table_oid(&ident))
    }

    async fn publication_tables(
        &self,
        publication_name: &str,
    ) -> std::result::Result<Vec<TableIdent>, PgError> {
        Ok(self.db.publication_tables(publication_name))
    }

    async fn identify_system_id(&self) -> std::result::Result<u64, PgError> {
        // Sim doesn't model multiple PG clusters, so the system_id
        // surface is effectively a no-op. Return `0` so the
        // lifecycle's sysid stamp/verify logic skips the cluster
        // fingerprint check (matching `connected_system_id == 0`
        // in `Checkpoint::verify`).
        Ok(0)
    }

    async fn server_version_num(&self) -> std::result::Result<i32, PgError> {
        // Sim doesn't model PG version differences. Return `0` so the
        // startup version check skips the assertion in DST runs.
        Ok(0)
    }

    async fn start_replication(
        &self,
        slot: &str,
        start: Lsn,
        _publication: &str,
    ) -> std::result::Result<Box<dyn ReplicationStream>, PgError> {
        let stream = self
            .db
            .start_replication_at(slot, start)
            .map_err(|e| PgError::Other(e.to_string()))?;
        Ok(Box::new(AsyncSimStream::new(stream)))
    }

    async fn drop_slot(&self, slot: &str) -> std::result::Result<(), PgError> {
        self.db
            .drop_slot(slot)
            .map_err(|e| PgError::Other(e.to_string()))
    }

    async fn drop_publication(&self, name: &str) -> std::result::Result<(), PgError> {
        self.db
            .drop_publication(name)
            .map_err(|e| PgError::Other(e.to_string()))
    }

    async fn alter_publication_add_table(
        &self,
        name: &str,
        ident: &TableIdent,
    ) -> std::result::Result<(), PgError> {
        self.db
            .add_table_to_publication(name, ident)
            .map_err(|e| PgError::Other(e.to_string()))
    }
}
