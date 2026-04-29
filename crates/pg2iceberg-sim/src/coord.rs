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
    LogEntry, MarkerInfo, OffsetGrant, Result,
};
use pg2iceberg_coord::TableSnapshotState;
use pg2iceberg_core::{Clock, Lsn, TableIdent, Timestamp, WorkerId};
use std::collections::BTreeSet;
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
    /// `pipeline_meta.system_identifier` (zero = unset).
    system_identifier: u64,
    /// `flushed_lsn.lsn` — highest LSN we've ever acked. Compared to
    /// `slot.confirmed_flush_lsn` at startup to catch external slot
    /// advancement.
    flushed_lsn: Lsn,
    /// Per-table snapshot status (`tables` table in the new schema).
    table_states: BTreeMap<TableIdent, TableSnapshotState>,
    /// Per-table mid-snapshot resume cursor (`snapshot_progress`).
    snapshot_progress: BTreeMap<TableIdent, String>,
    /// Per-table query-mode watermark (`query_watermarks`).
    query_watermarks: BTreeMap<TableIdent, pg2iceberg_core::PgValue>,
    /// Pending marker UUIDs observed by `claim_offsets`. Persisted
    /// alongside the log_index rows so the materializer can read
    /// them post-cycle (see [`Coordinator::pending_markers_for_table`]).
    /// Keyed by uuid for dedup; commit_lsn is the source-WAL LSN.
    pending_markers: BTreeMap<String, Lsn>,
    /// `(uuid, table)` pairs that have been emitted to Iceberg
    /// meta-marker tables. Emission is per-table because each
    /// pg2iceberg instance writes one meta-marker row per tracked
    /// user table per marker.
    marker_emissions: BTreeSet<(String, TableIdent)>,
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
            // No log_index rows to write — but markers still need
            // to be persisted (a marker-only flush has no claims).
            if !batch.markers.is_empty() {
                let mut state = self.state.lock().unwrap();
                for m in &batch.markers {
                    state
                        .pending_markers
                        .entry(m.uuid.clone())
                        .or_insert(m.commit_lsn);
                }
            }
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
                flushable_lsn: batch.flushable_lsn,
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

        // Markers are persisted atomically with the log_index rows
        // — same PG transaction in prod, same mutex section here. A
        // crash between staging Parquet and recording markers in
        // coord can't drop the marker because it's part of the same
        // claim_offsets call.
        for m in &batch.markers {
            // Idempotent: re-flushing a marker (e.g. on replay) is a
            // no-op. Keys by uuid mean the entry stays at its first
            // observed LSN.
            state
                .pending_markers
                .entry(m.uuid.clone())
                .or_insert(m.commit_lsn);
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

    async fn pipeline_system_identifier(&self) -> Result<u64> {
        Ok(self.state.lock().unwrap().system_identifier)
    }

    async fn set_pipeline_system_identifier(&self, sysid: u64) -> Result<()> {
        let mut state = self.state.lock().unwrap();
        if state.system_identifier != 0 && state.system_identifier != sysid {
            return Err(CoordError::SystemIdMismatch {
                stored: state.system_identifier,
                connected: sysid,
            });
        }
        state.system_identifier = sysid;
        Ok(())
    }

    async fn flushed_lsn(&self) -> Result<Lsn> {
        Ok(self.state.lock().unwrap().flushed_lsn)
    }

    async fn set_flushed_lsn(&self, lsn: Lsn) -> Result<()> {
        self.state.lock().unwrap().flushed_lsn = lsn;
        Ok(())
    }

    async fn table_state(&self, ident: &TableIdent) -> Result<Option<TableSnapshotState>> {
        Ok(self.state.lock().unwrap().table_states.get(ident).cloned())
    }

    async fn mark_table_snapshot_complete(
        &self,
        ident: &TableIdent,
        pg_oid: u32,
        snapshot_lsn: Lsn,
    ) -> Result<()> {
        let now_micros = self.clock.now().0;
        let mut state = self.state.lock().unwrap();
        state.table_states.insert(
            ident.clone(),
            TableSnapshotState {
                pg_oid,
                snapshot_complete: true,
                snapshot_lsn,
                completed_at_micros: Some(now_micros),
            },
        );
        Ok(())
    }

    async fn snapshot_progress(&self, ident: &TableIdent) -> Result<Option<String>> {
        Ok(self
            .state
            .lock()
            .unwrap()
            .snapshot_progress
            .get(ident)
            .cloned())
    }

    async fn set_snapshot_progress(
        &self,
        ident: &TableIdent,
        last_pk_key: &str,
    ) -> Result<()> {
        self.state
            .lock()
            .unwrap()
            .snapshot_progress
            .insert(ident.clone(), last_pk_key.to_string());
        Ok(())
    }

    async fn clear_snapshot_progress(&self, ident: &TableIdent) -> Result<()> {
        self.state
            .lock()
            .unwrap()
            .snapshot_progress
            .remove(ident);
        Ok(())
    }

    async fn query_watermark(&self, ident: &TableIdent) -> Result<Option<pg2iceberg_core::PgValue>> {
        Ok(self
            .state
            .lock()
            .unwrap()
            .query_watermarks
            .get(ident)
            .cloned())
    }

    async fn set_query_watermark(
        &self,
        ident: &TableIdent,
        watermark: &pg2iceberg_core::PgValue,
    ) -> Result<()> {
        self.state
            .lock()
            .unwrap()
            .query_watermarks
            .insert(ident.clone(), watermark.clone());
        Ok(())
    }

    async fn pending_markers_for_table(
        &self,
        table: &TableIdent,
        cursor: i64,
    ) -> Result<Vec<MarkerInfo>> {
        let state = self.state.lock().unwrap();
        let mut out: Vec<MarkerInfo> = state
            .pending_markers
            .iter()
            .filter(|(uuid, lsn)| {
                // Skip markers already emitted for this table.
                if state
                    .marker_emissions
                    .contains(&((*uuid).clone(), table.clone()))
                {
                    return false;
                }
                // Eligibility: every log_index entry for `table`
                // with flushable_lsn <= marker.commit_lsn must have
                // end_offset <= cursor. Otherwise the materializer
                // hasn't caught up to the marker's WAL point yet.
                let any_unprocessed_at_or_below = state
                    .log
                    .iter()
                    .filter(|e| e.table == *table)
                    .any(|e| e.flushable_lsn <= **lsn && (e.end_offset as i64) > cursor);
                !any_unprocessed_at_or_below
            })
            .map(|(uuid, lsn)| MarkerInfo {
                uuid: uuid.clone(),
                commit_lsn: *lsn,
            })
            .collect();
        // Deterministic order so emit order is stable.
        out.sort_by(|a, b| a.commit_lsn.cmp(&b.commit_lsn).then(a.uuid.cmp(&b.uuid)));
        Ok(out)
    }

    async fn record_marker_emitted(
        &self,
        uuid: &str,
        table: &TableIdent,
    ) -> Result<()> {
        self.state
            .lock()
            .unwrap()
            .marker_emissions
            .insert((uuid.to_string(), table.clone()));
        Ok(())
    }
}
