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

use async_trait::async_trait;
use pg2iceberg_core::{
    ChangeEvent, ColumnName, ColumnSchema, Lsn, Op, PgValue, Row, TableIdent, TableSchema,
    Timestamp,
};
use pg2iceberg_pg::{DecodedMessage, PgClient, PgError, ReplicationStream, SlotMonitor, SnapshotId};
use pg2iceberg_query::{watermark_compare, QueryError, WatermarkSource};
use pg2iceberg_snapshot::{SnapshotError, SnapshotSource};
use std::collections::{BTreeMap, BTreeSet};
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
    /// Mirrors `pg_replication_slots.conflicting` (PG 14+). Tests
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
    /// streams pick it up. (Real PG sends Relation lazily before the first
    /// change; close enough for the sim.)
    Relation(TableIdent),
}

#[derive(Clone, Debug)]
struct WalEntry {
    lsn: Lsn,
    xid: Option<u32>,
    kind: WalKind,
}

#[derive(Default)]
struct DbState {
    next_lsn: u64,
    next_xid: u32,
    tables: BTreeMap<TableIdent, TableData>,
    publications: BTreeMap<String, Publication>,
    slots: BTreeMap<String, SlotState>,
    wal: Vec<WalEntry>,
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
    fn current_lsn(&self) -> Lsn {
        Lsn(self.next_lsn)
    }
}

#[derive(Default, Clone)]
pub struct SimPostgres {
    state: Arc<Mutex<DbState>>,
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
        s.tables.insert(
            ident.clone(),
            TableData {
                schema,
                rows: BTreeMap::new(),
            },
        );
        let lsn = s.alloc_lsn();
        s.wal.push(WalEntry {
            lsn,
            xid: None,
            kind: WalKind::Relation(ident),
        });
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

    /// Test hook: flip a slot's `conflicting` flag. Models PG 14+'s
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
        let s = self.state.lock().unwrap();
        let slot_state = s
            .slots
            .get(slot)
            .cloned()
            .ok_or_else(|| SimError::UnknownSlot(slot.to_string()))?;
        Ok(SimReplicationStream {
            db: self.clone(),
            slot: slot.to_string(),
            publication: slot_state.publication,
            cursor_lsn: slot_state.restart_lsn,
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
    Delete {
        table: TableIdent,
        pk_row: Row,
    },
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

    /// Atomically apply ops and append `Begin / Change* / Commit` to the WAL.
    /// Returns the commit LSN — the LSN that crosses the durability boundary.
    pub fn commit(mut self, commit_ts: Timestamp) -> Result<Lsn> {
        self.done = true;
        let mut s = self.db.state.lock().unwrap();

        // Pre-validate ops against table state so we either fully apply or
        // fully bail. PG's transaction semantics demand atomic-or-nothing.
        for op in &self.ops {
            match op {
                TxOp::Insert { table, row } => {
                    let t = s
                        .tables
                        .get(table)
                        .ok_or_else(|| SimError::UnknownTable(table.clone()))?;
                    let key = t.pk_key(row)?;
                    if t.rows.contains_key(&key) {
                        return Err(SimError::PkConflict {
                            table: table.clone(),
                            detail: format!("duplicate pk {key}"),
                        });
                    }
                }
                TxOp::Update { table, new_row, .. } => {
                    let t = s
                        .tables
                        .get(table)
                        .ok_or_else(|| SimError::UnknownTable(table.clone()))?;
                    let key = t.pk_key(new_row)?;
                    if !t.rows.contains_key(&key) {
                        return Err(SimError::RowNotFound {
                            table: table.clone(),
                            op: "update",
                        });
                    }
                }
                TxOp::Delete { table, pk_row } => {
                    let t = s
                        .tables
                        .get(table)
                        .ok_or_else(|| SimError::UnknownTable(table.clone()))?;
                    let key = t.pk_key(pk_row)?;
                    if !t.rows.contains_key(&key) {
                        return Err(SimError::RowNotFound {
                            table: table.clone(),
                            op: "delete",
                        });
                    }
                }
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
                    t.rows.insert(key, new_row.clone());
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
                            after: Some(new_row),
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
            }
        }

        let commit_lsn = s.alloc_lsn();
        s.wal.push(WalEntry {
            lsn: commit_lsn,
            xid: Some(self.xid),
            kind: WalKind::Commit,
        });

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
pub struct SimReplicationStream {
    db: SimPostgres,
    slot: String,
    publication: String,
    cursor_lsn: Lsn,
}

impl SimReplicationStream {
    pub fn slot_name(&self) -> &str {
        &self.slot
    }

    /// Returns the next message at or after the cursor that's allowed by the
    /// publication. Cursor advances past whatever is returned.
    pub fn recv(&mut self) -> Option<DecodedMessage> {
        let s = self.db.state.lock().unwrap();
        let pub_tables = &s
            .publications
            .get(&self.publication)
            .expect("slot points to existing publication")
            .tables;

        for entry in &s.wal {
            if entry.lsn <= self.cursor_lsn {
                continue;
            }
            match &entry.kind {
                WalKind::Begin => {
                    self.cursor_lsn = entry.lsn;
                    return Some(DecodedMessage::Begin {
                        final_lsn: entry.lsn,
                        xid: entry.xid.unwrap_or(0),
                    });
                }
                WalKind::Commit => {
                    self.cursor_lsn = entry.lsn;
                    return Some(DecodedMessage::Commit {
                        commit_lsn: entry.lsn,
                        xid: entry.xid.unwrap_or(0),
                    });
                }
                WalKind::Relation(ident) => {
                    if !pub_tables.contains(ident) {
                        self.cursor_lsn = entry.lsn;
                        continue;
                    }
                    self.cursor_lsn = entry.lsn;
                    return Some(DecodedMessage::Relation {
                        ident: ident.clone(),
                    });
                }
                WalKind::Change(evt) => {
                    if !pub_tables.contains(&evt.table) {
                        self.cursor_lsn = entry.lsn;
                        continue;
                    }
                    self.cursor_lsn = entry.lsn;
                    return Some(DecodedMessage::Change(evt.clone()));
                }
            }
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

    /// Mimic the prod server-side snapshot↔CDC fence: skip emitting
    /// events whose LSN is at or below `lsn`. Used by
    /// [`SimPgClient::start_replication`] when the lifecycle helper
    /// passes `start = snap_lsn` after the snapshot phase.
    pub fn advance_cursor_to(&mut self, lsn: Lsn) {
        if lsn > self.cursor_lsn {
            self.cursor_lsn = lsn;
        }
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
        self.inner.send_standby(flushed);
        Ok(())
    }
}

#[async_trait]
impl SnapshotSource for SimPostgres {
    async fn snapshot_lsn(&self) -> std::result::Result<Lsn, SnapshotError> {
        Ok(self.current_lsn())
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

        // SimPostgres stores rows keyed by canonical PK in a BTreeMap, so
        // iteration is already sorted ASC by PK. Filter strictly above the
        // bound, then truncate.
        let chunk: Vec<Row> = table
            .rows
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
impl WatermarkSource for SimPostgres {
    async fn read_after(
        &self,
        ident: &TableIdent,
        watermark_col: &str,
        after: Option<&PgValue>,
        limit: Option<usize>,
    ) -> std::result::Result<Vec<Row>, QueryError> {
        let mut rows =
            SimPostgres::read_table(self, ident).map_err(|e| QueryError::Source(e.to_string()))?;
        let key = ColumnName(watermark_col.to_string());

        // Filter rows whose watermark > `after`. Rows missing the column are
        // skipped (matches PG behavior of `wm > NULL` excluding the row).
        if let Some(threshold) = after {
            rows.retain(|r| {
                r.get(&key)
                    .and_then(|v| watermark_compare(v, threshold).ok())
                    .is_some_and(|ord| ord == std::cmp::Ordering::Greater)
            });
        } else {
            rows.retain(|r| r.get(&key).is_some());
        }

        // Sort ASC by watermark. Rows with mismatched types fail the
        // comparison; we treat them as Equal (preserves stability without
        // panicking).
        rows.sort_by(|a, b| {
            let av = a.get(&key);
            let bv = b.get(&key);
            match (av, bv) {
                (Some(x), Some(y)) => watermark_compare(x, y).unwrap_or(std::cmp::Ordering::Equal),
                _ => std::cmp::Ordering::Equal,
            }
        });

        if let Some(n) = limit {
            rows.truncate(n);
        }
        Ok(rows)
    }
}

#[async_trait]
impl SlotMonitor for SimPostgres {
    async fn confirmed_flush_lsn(
        &self,
        slot: &str,
    ) -> std::result::Result<Option<Lsn>, PgError> {
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

    async fn identify_system_id(&self) -> std::result::Result<u64, PgError> {
        // Sim doesn't model multiple PG clusters, so the system_id
        // surface is effectively a no-op. Return `0` so the
        // lifecycle's sysid stamp/verify logic skips the cluster
        // fingerprint check (matching `connected_system_id == 0`
        // in `Checkpoint::verify`).
        Ok(0)
    }

    async fn start_replication(
        &self,
        slot: &str,
        start: Lsn,
        _publication: &str,
    ) -> std::result::Result<Box<dyn ReplicationStream>, PgError> {
        // Honor the snapshot↔CDC fence: when the lifecycle passes
        // `start = snap_lsn`, advance the stream's cursor so events
        // committed before the snapshot view aren't re-emitted. (In
        // prod, the server applies this fence; the sim emulates by
        // advancing its in-memory cursor.)
        let mut stream = self
            .db
            .start_replication(slot)
            .map_err(|e| PgError::Other(e.to_string()))?;
        stream.advance_cursor_to(start);
        Ok(Box::new(AsyncSimStream::new(stream)))
    }
}
