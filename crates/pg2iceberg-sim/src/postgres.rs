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

use pg2iceberg_core::{
    ChangeEvent, ColumnName, ColumnSchema, Lsn, Op, PgValue, Row, TableIdent, TableSchema,
    Timestamp,
};
use pg2iceberg_pg::DecodedMessage;
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
            },
        );
        Ok(lsn)
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
}
