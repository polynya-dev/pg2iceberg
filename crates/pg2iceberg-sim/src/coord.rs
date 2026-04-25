//! In-memory `Coordinator` impl for tests and the DST harness.
//!
//! Mirrors the semantics of `stream/coordinator_pg.go`. All state lives in a
//! single `Mutex<State>` so we get the same "single PG transaction" semantics
//! for free — the lock is held across the multi-step `claim_offsets`
//! algorithm.
//!
//! Time-dependent ops (consumer heartbeats, locks) consult an injected
//! [`Clock`] so tests advance time explicitly. No real `Instant::now()`.

use crate::clock::{duration_to_micros, TestClock};
use async_trait::async_trait;
use pg2iceberg_coord::{
    receipt, schema::CoordSchema, CommitBatch, CoordCommitReceipt, CoordError, Coordinator,
    LogEntry, OffsetGrant, Result,
};
use pg2iceberg_core::{Checkpoint, Clock, TableIdent, Timestamp, WorkerId};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// Snapshot of all coordinator state. Kept under one lock so multi-step
/// operations (the `claim_offsets` algorithm in particular) hold the same
/// PG-transaction-shaped invariants the prod impl gets from
/// `pgx.BeginTxFunc`.
#[derive(Default, Debug)]
struct State {
    /// `log_seq.next_offset`. Bumped by `claim_offsets`; never decreases.
    next_offset: BTreeMap<TableIdent, u64>,
    /// `log_index` rows in insert order. The `(table, end_offset)` PK is
    /// enforced on insert.
    log: Vec<LogEntry>,
    /// `mat_cursor.last_offset`. Default value when ensure_cursor inserts a
    /// row is `-1`, matching the Go DDL.
    cursors: BTreeMap<(String, TableIdent), i64>,
    /// `consumer.expires_at` per `(group, worker)`.
    consumers: BTreeMap<(String, WorkerId), Timestamp>,
    /// `lock` per table: holder + expiry.
    locks: BTreeMap<TableIdent, (WorkerId, Timestamp)>,
    /// `checkpoints` (one row in this design).
    checkpoint: Option<Checkpoint>,
}

#[derive(Clone)]
pub struct MemoryCoordinator {
    schema: CoordSchema,
    clock: Arc<dyn Clock>,
    state: Arc<Mutex<State>>,
}

impl MemoryCoordinator {
    pub fn new(schema: CoordSchema, clock: Arc<dyn Clock>) -> Self {
        Self {
            schema,
            clock,
            state: Arc::new(Mutex::new(State::default())),
        }
    }

    /// Convenience constructor for tests.
    pub fn with_test_clock(clock: TestClock) -> (Self, TestClock) {
        let arc: Arc<dyn Clock> = Arc::new(clock.clone());
        let coord = Self::new(CoordSchema::default_name(), arc);
        (coord, clock)
    }

    pub fn schema(&self) -> &CoordSchema {
        &self.schema
    }
}

fn ttl_expiry(now: Timestamp, ttl: Duration) -> Timestamp {
    Timestamp(now.0.saturating_add(duration_to_micros(ttl)))
}

#[async_trait]
impl Coordinator for MemoryCoordinator {
    async fn claim_offsets(&self, batch: &CommitBatch) -> Result<CoordCommitReceipt> {
        if batch.claims.is_empty() {
            return Ok(receipt::mint(batch.flushable_lsn, Vec::new()));
        }

        // Aggregate per-table totals (Go: tableOffsets map, lines 130-142).
        let mut totals: BTreeMap<TableIdent, u64> = BTreeMap::new();
        for c in &batch.claims {
            *totals.entry(c.table.clone()).or_default() += c.record_count;
        }

        let mut state = self.state.lock().unwrap();

        // Ensure log_seq row + atomically advance per-table next_offset.
        let mut start_offsets: BTreeMap<TableIdent, u64> = BTreeMap::new();
        for (table, total) in &totals {
            let entry = state.next_offset.entry(table.clone()).or_insert(0);
            let prev = *entry;
            *entry = entry
                .checked_add(*total)
                .ok_or_else(|| CoordError::Other(format!("log_seq overflow for {table}")))?;
            start_offsets.insert(table.clone(), prev);
        }

        // Walk claims in input order, emitting log_index rows. (Go: lines 169-191.)
        let mut running: BTreeMap<TableIdent, u64> = start_offsets.clone();
        let mut grants = Vec::with_capacity(batch.claims.len());
        for c in &batch.claims {
            let start = running.get(&c.table).copied().unwrap_or(0);
            let end = start + c.record_count;
            running.insert(c.table.clone(), end);

            let entry = LogEntry {
                table: c.table.clone(),
                start_offset: start,
                end_offset: end,
                s3_path: c.s3_path.clone(),
                record_count: c.record_count,
                byte_size: c.byte_size,
            };
            // PK violation check — should never fire because next_offset is
            // monotonic, but we surface it as a Conflict to mirror PG's
            // duplicate-key error.
            if state
                .log
                .iter()
                .any(|e| e.table == entry.table && e.end_offset == entry.end_offset)
            {
                return Err(CoordError::Conflict {
                    table: entry.table,
                    detail: format!("duplicate end_offset {}", entry.end_offset),
                });
            }
            state.log.push(entry.clone());
            grants.push(OffsetGrant {
                table: entry.table,
                start_offset: entry.start_offset,
                end_offset: entry.end_offset,
                s3_path: entry.s3_path,
            });
        }

        Ok(receipt::mint(batch.flushable_lsn, grants))
    }

    async fn read_log(
        &self,
        table: &TableIdent,
        after_offset: u64,
        limit: usize,
    ) -> Result<Vec<LogEntry>> {
        let state = self.state.lock().unwrap();
        let mut out: Vec<LogEntry> = state
            .log
            .iter()
            .filter(|e| &e.table == table && e.end_offset > after_offset)
            .cloned()
            .collect();
        out.sort_by_key(|e| e.end_offset);
        out.truncate(limit);
        Ok(out)
    }

    async fn truncate_log(&self, table: &TableIdent, before_offset: u64) -> Result<Vec<String>> {
        let mut state = self.state.lock().unwrap();
        let mut paths = Vec::new();
        let mut keep = Vec::with_capacity(state.log.len());
        for entry in std::mem::take(&mut state.log) {
            if &entry.table == table && entry.end_offset <= before_offset {
                paths.push(entry.s3_path);
            } else {
                keep.push(entry);
            }
        }
        state.log = keep;
        Ok(paths)
    }

    async fn ensure_cursor(&self, group: &str, table: &TableIdent) -> Result<()> {
        let mut state = self.state.lock().unwrap();
        state
            .cursors
            .entry((group.to_string(), table.clone()))
            .or_insert(-1);
        Ok(())
    }

    async fn get_cursor(&self, group: &str, table: &TableIdent) -> Result<Option<i64>> {
        let state = self.state.lock().unwrap();
        Ok(state
            .cursors
            .get(&(group.to_string(), table.clone()))
            .copied())
    }

    async fn set_cursor(&self, group: &str, table: &TableIdent, to_offset: i64) -> Result<()> {
        let mut state = self.state.lock().unwrap();
        let key = (group.to_string(), table.clone());
        match state.cursors.get_mut(&key) {
            Some(slot) => {
                *slot = to_offset;
                Ok(())
            }
            None => Err(CoordError::NotFound(format!("cursor for {group}/{table}"))),
        }
    }

    async fn register_consumer(&self, group: &str, worker: &WorkerId, ttl: Duration) -> Result<()> {
        let now = self.clock.now();
        let mut state = self.state.lock().unwrap();
        state
            .consumers
            .insert((group.to_string(), worker.clone()), ttl_expiry(now, ttl));
        Ok(())
    }

    async fn unregister_consumer(&self, group: &str, worker: &WorkerId) -> Result<()> {
        let mut state = self.state.lock().unwrap();
        state.consumers.remove(&(group.to_string(), worker.clone()));
        Ok(())
    }

    async fn active_consumers(&self, group: &str) -> Result<Vec<WorkerId>> {
        let now = self.clock.now();
        let mut state = self.state.lock().unwrap();
        // Expire stale entries first (matches Go: ActiveConsumers does the
        // sweep too, line 343).
        state.consumers.retain(|_, exp| *exp >= now);
        let mut out: Vec<WorkerId> = state
            .consumers
            .iter()
            .filter_map(|((g, w), _)| if g == group { Some(w.clone()) } else { None })
            .collect();
        out.sort();
        Ok(out)
    }

    async fn try_lock(&self, table: &TableIdent, worker: &WorkerId, ttl: Duration) -> Result<bool> {
        let now = self.clock.now();
        let expiry = ttl_expiry(now, ttl);
        let mut state = self.state.lock().unwrap();

        // Expire stale lock for this table first.
        if let Some((_, exp)) = state.locks.get(table) {
            if *exp < now {
                state.locks.remove(table);
            }
        }

        if state.locks.contains_key(table) {
            return Ok(false);
        }
        state.locks.insert(table.clone(), (worker.clone(), expiry));
        Ok(true)
    }

    async fn renew_lock(
        &self,
        table: &TableIdent,
        worker: &WorkerId,
        ttl: Duration,
    ) -> Result<bool> {
        let now = self.clock.now();
        let expiry = ttl_expiry(now, ttl);
        let mut state = self.state.lock().unwrap();
        match state.locks.get_mut(table) {
            Some((holder, exp)) if holder == worker => {
                *exp = expiry;
                Ok(true)
            }
            _ => Ok(false),
        }
    }

    async fn release_lock(&self, table: &TableIdent, worker: &WorkerId) -> Result<()> {
        let mut state = self.state.lock().unwrap();
        if let Some((holder, _)) = state.locks.get(table) {
            if holder == worker {
                state.locks.remove(table);
            }
        }
        Ok(())
    }

    async fn load_checkpoint(&self) -> Result<Option<Checkpoint>> {
        Ok(self.state.lock().unwrap().checkpoint.clone())
    }

    async fn save_checkpoint(&self, cp: &Checkpoint) -> Result<()> {
        self.state.lock().unwrap().checkpoint = Some(cp.clone());
        Ok(())
    }
}
