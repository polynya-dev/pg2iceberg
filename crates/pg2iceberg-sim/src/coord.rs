//! In-memory `Coordinator` impl for tests and the DST harness.
//!
//! Mirrors the semantics of `stream/coordinator_pg.go`. All state lives in a
//! single `Mutex<State>` so we get the same "single PG transaction" semantics
//! for free — the lock is held across the multi-step `claim_offsets`
//! algorithm.
//!
//! Time-dependent ops (consumer heartbeats) consult an injected
//! [`Clock`] so tests advance time explicitly. No real `Instant::now()`.

use crate::clock::{duration_to_micros, TestClock};
use async_trait::async_trait;
use pg2iceberg_coord::{
    receipt, schema::CoordSchema, CommitBatch, CoordCommitReceipt, CoordError, Coordinator,
    LogEntry, MarkerInfo, OffsetGrant, Result,
};
use pg2iceberg_coord::{StagedLogSize, TableSnapshotState, WriteInFlight};
use pg2iceberg_core::{Clock, Lsn, TableIdent, Timestamp, WorkerId};
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// Snapshot of all coordinator state. Kept under one lock so multi-step
/// operations (the `claim_offsets` algorithm in particular) hold the same
/// PG-transaction-shaped invariants the prod impl gets from
/// `pgx.BeginTxFunc`.
#[derive(Default, Debug, Clone, PartialEq)]
struct State {
    /// `log_seq.next_offset`. Bumped by `claim_offsets`; never decreases.
    next_offset: BTreeMap<TableIdent, u64>,
    /// `table_epoch.epoch`. Bumped by `bump_table_epoch`.
    table_epochs: BTreeMap<TableIdent, i64>,
    /// `writes_in_flight.since_ms` per `(table, writer)`.
    writes_in_flight: BTreeMap<(TableIdent, String), i64>,
    /// `log_index` rows in insert order. The `(table, end_offset)` PK is
    /// enforced on insert.
    log: Vec<LogEntry>,
    /// Every `log_index` row ever claimed, in insert order, truncated
    /// or not: what tests check staging against
    /// ([`MemoryCoordinator::claimed_log`]).
    claimed: Vec<LogEntry>,
    /// Groups a cursor write to fails
    /// ([`MemoryCoordinator::crash_on_cursor_write`]).
    crash_groups: BTreeSet<String>,
    /// `mat_cursor.last_offset`. Default value when ensure_cursor inserts a
    /// row is `-1` (matches the prod DDL default).
    cursors: BTreeMap<(String, TableIdent), i64>,
    /// `consumer.expires_at` per `(group, worker)`.
    consumers: BTreeMap<(String, WorkerId), Timestamp>,
    /// `pipeline_meta.system_identifier` (zero = unset).
    system_identifier: u64,
    /// `pipeline_meta.consumer_group` (`None` = unclaimed).
    consumer_group: Option<String>,
    /// `flushed_lsn.lsn` — highest LSN we've ever acked. Compared to
    /// `slot.confirmed_flush_lsn` at startup to catch external slot
    /// advancement.
    flushed_lsn: Lsn,
    /// `replicated_lsn.lsn` — how far replication is staged; raised by
    /// claims that carry [`CommitBatch::replicated_lsn`].
    replicated_lsn: Lsn,
    /// Per-table snapshot status (`tables` table in the new schema).
    table_states: BTreeMap<TableIdent, TableSnapshotState>,
    /// Per-table mid-snapshot resume cursor (`snapshot_progress`).
    snapshot_progress: BTreeMap<TableIdent, String>,
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

impl State {
    /// What the coordinator's tables hold: everything but the test
    /// hooks' bookkeeping (`claimed`, `crash_groups`), which no failover
    /// rolls back.
    fn durable(&self) -> State {
        let mut s = State {
            claimed: Vec::new(),
            crash_groups: BTreeSet::new(),
            ..State::default()
        };
        s.next_offset = self.next_offset.clone();
        s.table_epochs = self.table_epochs.clone();
        s.writes_in_flight = self.writes_in_flight.clone();
        s.log = self.log.clone();
        s.cursors = self.cursors.clone();
        s.consumers = self.consumers.clone();
        s.system_identifier = self.system_identifier;
        s.consumer_group = self.consumer_group.clone();
        s.flushed_lsn = self.flushed_lsn;
        s.replicated_lsn = self.replicated_lsn;
        s.table_states = self.table_states.clone();
        s.snapshot_progress = self.snapshot_progress.clone();
        s.pending_markers = self.pending_markers.clone();
        s.marker_emissions = self.marker_emissions.clone();
        s
    }

    /// Whether `other` holds what this does in the coordinator's tables.
    fn durably_eq(&self, other: &State) -> bool {
        self.next_offset == other.next_offset
            && self.table_epochs == other.table_epochs
            && self.writes_in_flight == other.writes_in_flight
            && self.log == other.log
            && self.cursors == other.cursors
            && self.consumers == other.consumers
            && self.system_identifier == other.system_identifier
            && self.consumer_group == other.consumer_group
            && self.flushed_lsn == other.flushed_lsn
            && self.replicated_lsn == other.replicated_lsn
            && self.table_states == other.table_states
            && self.snapshot_progress == other.snapshot_progress
            && self.pending_markers == other.pending_markers
            && self.marker_emissions == other.marker_emissions
    }
}

/// Work [`MemoryCoordinator::before_next_read_log`] runs.
type Interlude = Box<dyn FnOnce() + Send>;

#[derive(Clone)]
pub struct MemoryCoordinator {
    schema: CoordSchema,
    clock: Arc<dyn Clock>,
    state: Arc<Mutex<State>>,
    before_read_log: Arc<Mutex<Option<Interlude>>>,
    /// The source database this coordinator's tables live in, if they do
    /// (see [`MemoryCoordinator::colocate`]).
    colocation: Arc<Mutex<Option<Colocation>>>,
    /// The timeline that database was on when this handle connected, for
    /// a process's own handle ([`MemoryCoordinator::connection`]).
    connected_on: Option<u32>,
}

/// A coordinator living in the source database: each write to it is a
/// transaction in that database's WAL, which a failover rolls back to
/// what its standby had.
struct Colocation {
    db: crate::postgres::SimPostgres,
    /// The coordinator's tables as of each write's commit, oldest first;
    /// the first as of when they moved in.
    history: Vec<(Lsn, State)>,
}

/// The coordinator's state, locked; dropping it records a write in the
/// source's WAL if the coordinator lives there and its tables changed.
struct StateGuard<'a> {
    state: std::sync::MutexGuard<'a, State>,
    colocation: &'a Mutex<Option<Colocation>>,
}

impl std::ops::Deref for StateGuard<'_> {
    type Target = State;
    fn deref(&self) -> &State {
        &self.state
    }
}

impl std::ops::DerefMut for StateGuard<'_> {
    fn deref_mut(&mut self) -> &mut State {
        &mut self.state
    }
}

impl Drop for StateGuard<'_> {
    fn drop(&mut self) {
        let mut colocation = self.colocation.lock().unwrap();
        let Some(c) = colocation.as_mut() else {
            return;
        };
        let changed = c
            .history
            .last()
            .is_none_or(|(_, last)| !last.durably_eq(&self.state));
        if changed {
            let lsn = c.db.commit_unpublished();
            c.history.push((lsn, self.state.durable()));
        }
    }
}

impl MemoryCoordinator {
    pub fn new(schema: CoordSchema, clock: Arc<dyn Clock>) -> Self {
        Self {
            schema,
            clock,
            state: Arc::new(Mutex::new(State::default())),
            before_read_log: Arc::default(),
            colocation: Arc::default(),
            connected_on: None,
        }
    }

    /// A process's own handle on this coordinator, as on a connection of
    /// its own: once the database it lives in has failed over, the handle
    /// refuses every call ([`CoordError::FailedOver`]), as production's
    /// coordinator does when its connection comes back on another
    /// timeline. A new process takes a new handle.
    pub fn connection(&self) -> Self {
        Self {
            connected_on: self.timeline(),
            ..self.clone()
        }
    }

    /// The timeline of the database this coordinator lives in, if it lives
    /// in the source.
    fn timeline(&self) -> Option<u32> {
        let colocation = self.colocation.lock().unwrap();
        colocation.as_ref().map(|c| c.db.timeline())
    }

    /// The state, locked for a call — refused once the database the
    /// coordinator lives in failed over under this handle.
    fn lock(&self) -> Result<StateGuard<'_>> {
        if let (Some(from), Some(to)) = (self.connected_on, self.timeline()) {
            if from != to {
                return Err(CoordError::FailedOver { from, to });
            }
        }
        Ok(self.lock_unfenced())
    }

    /// The state, locked for a test hook.
    fn lock_unfenced(&self) -> StateGuard<'_> {
        StateGuard {
            state: self.state.lock().unwrap(),
            colocation: &self.colocation,
        }
    }

    /// From now on this coordinator's tables live in `db`, the source:
    /// each write to them is a transaction in its WAL, as production's
    /// state lives on the source's server. See [`Self::fail_over_to`].
    pub fn colocate(&self, db: &crate::postgres::SimPostgres) {
        let now = self.state.lock().unwrap().durable();
        *self.colocation.lock().unwrap() = Some(Colocation {
            db: db.clone(),
            history: vec![(Lsn::ZERO, now)],
        });
    }

    /// The source this coordinator lives in failed over, its WAL now
    /// ending at `end` ([`crate::postgres::SimPostgres::promote_standby`]):
    /// its tables are as of the last write the standby had. Does nothing
    /// for a coordinator that doesn't live in the source.
    pub fn fail_over_to(&self, end: Lsn) {
        let mut colocation = self.colocation.lock().unwrap();
        let Some(c) = colocation.as_mut() else {
            return;
        };
        c.history.retain(|(lsn, _)| *lsn < end);
        let (_, kept) = c
            .history
            .last()
            .expect("the state it moved in with is kept");
        let mut state = self.state.lock().unwrap();
        let claimed = std::mem::take(&mut state.claimed);
        let crash_groups = std::mem::take(&mut state.crash_groups);
        *state = State {
            claimed,
            crash_groups,
            ..kept.durable()
        };
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

    /// Test hook: run `f` just before the next `read_log` reads — as
    /// another process's work landing between a read of the cursor and of
    /// the log. `f` may call this coordinator.
    pub fn before_next_read_log(&self, f: impl FnOnce() + Send + 'static) {
        *self.before_read_log.lock().unwrap() = Some(Box::new(f));
    }

    /// Test hook: from now on, `set_cursor` for `groups` fails, writing
    /// nothing — as for a process that dies before its cursor write
    /// reaches the coordinator. An empty `groups` ends it.
    pub fn crash_on_cursor_write(&self, groups: &[&str]) {
        self.lock_unfenced().crash_groups = groups.iter().map(|g| g.to_string()).collect();
    }

    /// Test hook: every log entry ever claimed for `table`, by offset —
    /// those [`Coordinator::truncate_log`] dropped too.
    pub fn claimed_log(&self, table: &TableIdent) -> Vec<LogEntry> {
        let state = self.lock_unfenced();
        let mut out: Vec<LogEntry> = state
            .claimed
            .iter()
            .filter(|e| &e.table == table)
            .cloned()
            .collect();
        out.sort_by_key(|e| e.end_offset);
        out
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
            let mut state = self.lock()?;
            for m in &batch.markers {
                state
                    .pending_markers
                    .entry(m.uuid.clone())
                    .or_insert(m.commit_lsn);
            }
            if let Some(lsn) = batch.replicated_lsn {
                state.replicated_lsn = state.replicated_lsn.max(lsn);
            }
            return Ok(receipt::mint(batch.flushable_lsn, Vec::new()));
        }

        // Aggregate per-table totals (Go: tableOffsets map, lines 130-142).
        let mut totals: BTreeMap<TableIdent, u64> = BTreeMap::new();
        for c in &batch.claims {
            *totals.entry(c.table.clone()).or_default() += c.record_count;
        }

        let mut state = self.lock()?;

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
            state.claimed.push(entry.clone());
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
        if let Some(lsn) = batch.replicated_lsn {
            state.replicated_lsn = state.replicated_lsn.max(lsn);
        }

        Ok(receipt::mint(batch.flushable_lsn, grants))
    }

    async fn read_log(
        &self,
        table: &TableIdent,
        after_offset: u64,
        limit: usize,
    ) -> Result<Vec<LogEntry>> {
        // As Postgres's bigint parameters bound them in prod.
        if i64::try_from(after_offset).is_err() {
            return Err(CoordError::Other("after_offset > i64".into()));
        }
        if i64::try_from(limit).is_err() {
            return Err(CoordError::Other("limit > i64".into()));
        }
        let interlude = self.before_read_log.lock().unwrap().take();
        if let Some(f) = interlude {
            f();
        }
        let state = self.lock()?;
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

    async fn truncate_log(
        &self,
        table: &TableIdent,
        before_offset: u64,
        limit: usize,
    ) -> Result<Vec<String>> {
        // As Postgres's bigint parameters bound them in prod.
        if i64::try_from(before_offset).is_err() {
            return Err(CoordError::Other("before_offset > i64".into()));
        }
        if i64::try_from(limit).is_err() {
            return Err(CoordError::Other("limit > i64".into()));
        }
        let mut state = self.lock()?;
        // The first `limit` by offset.
        let mut doomed: Vec<u64> = state
            .log
            .iter()
            .filter(|e| &e.table == table && e.end_offset <= before_offset)
            .map(|e| e.end_offset)
            .collect();
        doomed.sort_unstable();
        doomed.truncate(limit);
        let doomed: BTreeSet<u64> = doomed.into_iter().collect();
        let mut paths = Vec::new();
        let mut keep = Vec::with_capacity(state.log.len());
        for entry in std::mem::take(&mut state.log) {
            if &entry.table == table && doomed.contains(&entry.end_offset) {
                paths.push(entry.s3_path);
            } else {
                keep.push(entry);
            }
        }
        state.log = keep;
        Ok(paths)
    }

    async fn staged_log_size(&self, table: &TableIdent) -> Result<StagedLogSize> {
        let state = self.lock()?;
        let mut size = StagedLogSize::default();
        for e in state.log.iter().filter(|e| &e.table == table) {
            size.files += 1;
            size.bytes += e.byte_size;
        }
        Ok(size)
    }

    async fn ensure_cursor(&self, group: &str, table: &TableIdent) -> Result<()> {
        let mut state = self.lock()?;
        state
            .cursors
            .entry((group.to_string(), table.clone()))
            .or_insert(-1);
        Ok(())
    }

    async fn get_cursor(&self, group: &str, table: &TableIdent) -> Result<Option<i64>> {
        let state = self.lock()?;
        Ok(state
            .cursors
            .get(&(group.to_string(), table.clone()))
            .copied())
    }

    async fn set_cursor(&self, group: &str, table: &TableIdent, to_offset: i64) -> Result<()> {
        let mut state = self.lock()?;
        if state.crash_groups.contains(group) {
            return Err(CoordError::Other(format!(
                "the process died before setting {group}'s cursor"
            )));
        }
        let key = (group.to_string(), table.clone());
        match state.cursors.get_mut(&key) {
            Some(slot) => {
                *slot = to_offset;
                Ok(())
            }
            None => Err(CoordError::NotFound(format!("cursor for {group}/{table}"))),
        }
    }

    async fn table_cursors(&self, table: &TableIdent) -> Result<Vec<(String, i64)>> {
        let state = self.lock()?;
        Ok(state
            .cursors
            .iter()
            .filter(|((_, t), _)| t == table)
            .map(|((group, _), offset)| (group.clone(), *offset))
            .collect())
    }

    async fn register_consumer(&self, group: &str, worker: &WorkerId, ttl: Duration) -> Result<()> {
        let now = self.clock.now();
        let mut state = self.lock()?;
        state
            .consumers
            .insert((group.to_string(), worker.clone()), ttl_expiry(now, ttl));
        Ok(())
    }

    async fn unregister_consumer(&self, group: &str, worker: &WorkerId) -> Result<()> {
        let mut state = self.lock()?;
        state.consumers.remove(&(group.to_string(), worker.clone()));
        Ok(())
    }

    async fn active_consumers(&self, group: &str) -> Result<Vec<WorkerId>> {
        let now = self.clock.now();
        let mut state = self.lock()?;
        // Expire stale entries first so the returned list is "live as
        // of now" — matches the prod impl's behaviour.
        state.consumers.retain(|_, exp| *exp >= now);
        let mut out: Vec<WorkerId> = state
            .consumers
            .iter()
            .filter_map(|((g, w), _)| if g == group { Some(w.clone()) } else { None })
            .collect();
        out.sort();
        Ok(out)
    }

    async fn pipeline_system_identifier(&self) -> Result<u64> {
        Ok(self.lock()?.system_identifier)
    }

    async fn set_pipeline_system_identifier(&self, sysid: u64) -> Result<()> {
        let mut state = self.lock()?;
        if state.system_identifier != 0 && state.system_identifier != sysid {
            return Err(CoordError::SystemIdMismatch {
                stored: state.system_identifier,
                connected: sysid,
            });
        }
        state.system_identifier = sysid;
        Ok(())
    }

    async fn claim_group(&self, group: &str) -> Result<()> {
        let mut state = self.lock()?;
        // Claimed by none yet: another group's cursors hold it anyway.
        let holder = state.consumer_group.clone().or_else(|| {
            state
                .cursors
                .keys()
                .map(|(g, _)| g.split('#').next().unwrap_or_default().to_string())
                .filter(|g| g != group)
                .min()
        });
        match holder {
            Some(holder) if holder != group => Err(CoordError::GroupMismatch {
                holder,
                configured: group.to_string(),
            }),
            _ => {
                state.consumer_group = Some(group.to_string());
                Ok(())
            }
        }
    }

    async fn flushed_lsn(&self) -> Result<Lsn> {
        Ok(self.lock()?.flushed_lsn)
    }

    async fn replicated_lsn(&self) -> Result<Lsn> {
        Ok(self.lock()?.replicated_lsn)
    }

    async fn set_flushed_lsn(&self, lsn: Lsn) -> Result<()> {
        self.lock()?.flushed_lsn = lsn;
        Ok(())
    }

    async fn table_state(&self, ident: &TableIdent) -> Result<Option<TableSnapshotState>> {
        Ok(self.lock()?.table_states.get(ident).cloned())
    }

    async fn mark_table_snapshot_complete(
        &self,
        ident: &TableIdent,
        pg_oid: u32,
        snapshot_lsn: Lsn,
    ) -> Result<()> {
        let now_micros = self.clock.now().0;
        let mut state = self.lock()?;
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

    async fn bump_table_epoch(&self, table: &TableIdent) -> Result<i64> {
        let mut s = self.lock()?;
        let epoch = s.table_epochs.entry(table.clone()).or_insert(0);
        *epoch += 1;
        Ok(*epoch)
    }

    async fn table_epochs(&self, tables: &[TableIdent]) -> Result<BTreeMap<TableIdent, i64>> {
        let s = self.lock()?;
        Ok(tables
            .iter()
            .filter_map(|t| s.table_epochs.get(t).map(|e| (t.clone(), *e)))
            .collect())
    }

    async fn begin_write(&self, table: &TableIdent, writer: &str, since_ms: i64) -> Result<()> {
        let mut s = self.lock()?;
        let since = s
            .writes_in_flight
            .entry((table.clone(), writer.to_string()))
            .or_insert(since_ms);
        *since = (*since).min(since_ms);
        Ok(())
    }

    async fn end_write(&self, table: &TableIdent, writer: &str) -> Result<()> {
        self.state
            .lock()
            .unwrap()
            .writes_in_flight
            .remove(&(table.clone(), writer.to_string()));
        Ok(())
    }

    async fn writes_in_flight(&self, table: &TableIdent) -> Result<Vec<WriteInFlight>> {
        let s = self.lock()?;
        Ok(s.writes_in_flight
            .iter()
            .filter(|((t, _), _)| t == table)
            .map(|((_, writer), since_ms)| WriteInFlight {
                writer: writer.clone(),
                since_ms: *since_ms,
            })
            .collect())
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

    async fn set_snapshot_progress(&self, ident: &TableIdent, last_pk_key: &str) -> Result<()> {
        self.state
            .lock()
            .unwrap()
            .snapshot_progress
            .insert(ident.clone(), last_pk_key.to_string());
        Ok(())
    }

    async fn clear_snapshot_progress(&self, ident: &TableIdent) -> Result<()> {
        self.lock()?.snapshot_progress.remove(ident);
        Ok(())
    }

    async fn pending_markers_for_table(
        &self,
        table: &TableIdent,
        cursor: i64,
    ) -> Result<Vec<MarkerInfo>> {
        let state = self.lock()?;
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

    async fn record_marker_emitted(&self, uuid: &str, table: &TableIdent) -> Result<()> {
        self.state
            .lock()
            .unwrap()
            .marker_emissions
            .insert((uuid.to_string(), table.clone()));
        Ok(())
    }
}
