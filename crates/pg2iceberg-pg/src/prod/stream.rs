//! [`ReplicationStreamImpl`]: production [`ReplicationStream`] wrapping
//! `postgres_replication::LogicalReplicationStream`.
//!
//! ## Why this owns a dedicated reader task
//!
//! The replication wire is a single bidirectional `CopyBoth` connection;
//! both `recv()` (decoded events) and `send_standby()` (slot acks) go
//! through the same socket. The earlier single-task design wrapped both
//! in the lifecycle's outer `tokio::select!`, which dropped the in-flight
//! `next()` future every time a tick won the race. Under tight tick
//! schedules that left no room for the wire to drain — the underlying
//! channel that backs `LogicalReplicationStream` filled up, the
//! tokio-postgres connection task stalled, and post-ack events stopped
//! being delivered (manifesting as missing schema-evolution + late-tx
//! events in tests).
//!
//! The fix is to give the stream its own task: that task owns the
//! `LogicalReplicationStream`, drains decoded events into a bounded
//! mpsc, and serves ack requests from a small command channel. The
//! outer `select!` no longer cancels mid-decode — `events_rx.recv()`
//! is fully cancel-safe.
//!
//! Translation rules (`LogicalReplicationMessage` → our
//! [`crate::DecodedMessage`]):
//!
//! - `Begin` → `DecodedMessage::Begin`. We stash `final_lsn`, `xid`, and
//!   `timestamp` so subsequent DML messages can be tagged with the
//!   correct commit LSN/xid/ts (pgoutput sends those once per txn,
//!   not per row).
//! - `Commit` → `DecodedMessage::Commit`. The xid is propagated from
//!   the most recent Begin.
//! - `Relation` → `DecodedMessage::Relation { ident }`, plus an
//!   internal cache update so DML decoders know the column types.
//!   The materializer queries the catalog separately for full schema;
//!   the cache here is wire-protocol-only.
//! - `Insert` / `Update` / `Delete` → `DecodedMessage::Change(...)`
//!   with `before` / `after` rows decoded via [`super::value_decode`].
//! - `Truncate` → emits one `Change` per truncated relation with
//!   `Op::Truncate` and no before/after row.
//! - `Origin` / `Type` / `Message` → silently skipped (we don't surface
//!   pgoutput protocol metadata to consumers).
//! - `PrimaryKeepAlive` → `DecodedMessage::Keepalive`.

use crate::prod::typemap::pg_type_from_oid;
use crate::prod::value_decode::decode_text;
use crate::{ChangeEvent, DecodedMessage, PgError, RelationColumn, ReplicationStream, Result};
use async_trait::async_trait;
use futures_util::StreamExt;
use pg2iceberg_core::typemap::PgType;
use pg2iceberg_core::Namespace;
use pg2iceberg_core::{ColumnName, Lsn, Op, PgValue, Row, TableIdent, Timestamp};
use postgres_replication::protocol::{
    LogicalReplicationMessage as PgMsg, ReplicationMessage as PgEnv, Tuple, TupleData,
};
use postgres_replication::LogicalReplicationStream;
use std::collections::{HashMap, VecDeque};
use std::pin::Pin;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio_postgres::types::PgLsn;

/// Postgres microsecond epoch starts at 2000-01-01; Unix epoch is
/// 1970-01-01. Difference in micros is constant and used both ways
/// for timestamp conversion.
const PG_EPOCH_OFFSET_MICROS: i64 = 946_684_800_000_000;

/// Bounded buffer for decoded events. 1000 is enough capacity that
/// transient main-loop stalls (materializer cycle, compaction tick)
/// don't backpressure the wire even under WAL bursts.
const EVENTS_CHANNEL_CAPACITY: usize = 1000;

/// Acks are infrequent (once per Standby tick + a final shutdown ack),
/// so a small queue is enough.
const CMD_CHANNEL_CAPACITY: usize = 8;

/// Cached relation metadata, populated by `Relation` messages and
/// consumed by DML decoders.
#[derive(Debug, Clone)]
struct RelationCache {
    ident: TableIdent,
    columns: Vec<ColumnInfo>,
}

#[derive(Debug, Clone)]
struct ColumnInfo {
    name: String,
    pg_type: Option<PgType>,
}

#[derive(Debug, Clone, Copy)]
struct TxnContext {
    final_lsn: Lsn,
    xid: u32,
    commit_ts: Timestamp,
}

/// Commands the reader task accepts on its control channel. Today only
/// `Standby` is needed; future shutdown / health probes can be added
/// here without touching the events path.
enum Cmd {
    Standby {
        flushed: Lsn,
        applied: Lsn,
        done: oneshot::Sender<Result<()>>,
    },
}

pub struct ReplicationStreamImpl {
    events_rx: mpsc::Receiver<Result<DecodedMessage>>,
    cmd_tx: mpsc::Sender<Cmd>,
    reader: JoinHandle<()>,
}

impl ReplicationStreamImpl {
    pub(crate) fn wrap(stream: LogicalReplicationStream) -> Self {
        let (events_tx, events_rx) = mpsc::channel(EVENTS_CHANNEL_CAPACITY);
        let (cmd_tx, cmd_rx) = mpsc::channel(CMD_CHANNEL_CAPACITY);
        let reader = tokio::spawn(reader_task(Box::pin(stream), events_tx, cmd_rx));
        Self {
            events_rx,
            cmd_tx,
            reader,
        }
    }
}

impl Drop for ReplicationStreamImpl {
    fn drop(&mut self) {
        // Channels alone usually wind the task down (events_tx blocks on
        // a closed receiver, cmd_rx returns None when the sender is
        // dropped), but the task may be parked in `stream.next()` with
        // no wire activity. Abort guarantees prompt teardown.
        self.reader.abort();
    }
}

#[async_trait]
impl ReplicationStream for ReplicationStreamImpl {
    async fn recv(&mut self) -> Result<DecodedMessage> {
        match self.events_rx.recv().await {
            Some(res) => res,
            None => Err(PgError::Connection("replication reader task exited".into())),
        }
    }

    async fn send_standby(&mut self, flushed: Lsn, applied: Lsn) -> Result<()> {
        let (done_tx, done_rx) = oneshot::channel();
        self.cmd_tx
            .send(Cmd::Standby {
                flushed,
                applied,
                done: done_tx,
            })
            .await
            .map_err(|_| PgError::Connection("replication reader task exited".into()))?;
        // Wait for the ack to actually hit the wire before returning.
        // Caller relies on this for the stamp-before-ack discipline:
        // when `send_standby` returns Ok, the slot has observed `lsn`.
        done_rx
            .await
            .map_err(|_| PgError::Connection("replication reader task dropped ack".into()))?
    }
}

#[derive(Default)]
struct ReaderState {
    relations: HashMap<u32, RelationCache>,
    /// Stashed per-txn metadata between `Begin` and `Commit`. Pgoutput
    /// only sends xid/commit_ts in the Begin record; DML messages
    /// inherit those via the surrounding txn.
    current_txn: Option<TxnContext>,
    /// Truncate carries N rel_ids; we emit one Change per rel_id.
    /// `VecDeque` so we drain in arrival order with `pop_front`.
    pending: VecDeque<DecodedMessage>,
}

impl ReaderState {
    fn handle(&mut self, env: PgEnv<PgMsg>) -> Result<Option<DecodedMessage>> {
        match env {
            // Keepalives are absorbed by the reader task — they're
            // protocol heartbeats, not consumer events. Forwarding them
            // through the bounded `events` channel is what created the
            // case-24 bug: under heavy WAL chatter on tables OUTSIDE the
            // publication, walsender emits one keepalive per skipped
            // record (we observed ~1500/s). At that rate the channel
            // fills, the reader blocks on `events_tx.send`, the inner
            // tokio-postgres CopyBoth channel backs up, and walsender
            // suspends decoding for our connection — so subsequent DML
            // for the publication's actual tables never reaches us.
            //
            // No downstream consumer uses `DecodedMessage::Keepalive`
            // (the pipeline's match arm is a noop, the sim impl doesn't
            // emit it), so dropping at the reader is purely an
            // optimization with no behavioural change. If we ever need
            // `reply_requested` handling we'll respond inline here
            // rather than forwarding.
            PgEnv::PrimaryKeepAlive(_) => Ok(None),
            PgEnv::XLogData(xlog) => self.handle_logical(xlog.into_data()),
            // The ReplicationMessage enum is `#[non_exhaustive]`; future
            // variants get treated as keepalive-style noops.
            _ => Ok(None),
        }
    }

    fn handle_logical(&mut self, msg: PgMsg) -> Result<Option<DecodedMessage>> {
        match msg {
            PgMsg::Begin(b) => {
                let final_lsn = Lsn(b.final_lsn());
                let xid = b.xid();
                let commit_ts = pg_micros_to_timestamp(b.timestamp());
                self.current_txn = Some(TxnContext {
                    final_lsn,
                    xid,
                    commit_ts,
                });
                Ok(Some(DecodedMessage::Begin { final_lsn, xid }))
            }
            PgMsg::Commit(c) => {
                let commit_lsn = Lsn(c.commit_lsn());
                let xid = self.current_txn.map(|t| t.xid).unwrap_or(0);
                self.current_txn = None;
                Ok(Some(DecodedMessage::Commit { commit_lsn, xid }))
            }
            PgMsg::Relation(r) => {
                let ident = build_table_ident(
                    r.namespace().map_err(io_to_pg)?,
                    r.name().map_err(io_to_pg)?,
                );
                let mut internal_columns: Vec<ColumnInfo> = Vec::with_capacity(r.columns().len());
                let mut decoded_columns: Vec<RelationColumn> =
                    Vec::with_capacity(r.columns().len());
                for c in r.columns() {
                    let name = c.name().map_err(io_to_pg)?.to_string();
                    let pg_type = pg_type_from_oid(c.type_id() as u32, c.type_modifier());
                    internal_columns.push(ColumnInfo {
                        name: name.clone(),
                        pg_type,
                    });
                    // pgoutput Relation column flags: bit 0 = column
                    // is part of REPLICA IDENTITY (PK or USING INDEX).
                    let is_pk = (c.flags() & 1) != 0;
                    // pgoutput doesn't carry nullability; we stamp
                    // nullable=true on every non-PK column. PG only
                    // permits adding non-nullable columns with a
                    // DEFAULT, and for that case `apply_relation`
                    // would still see a nullable add (which Iceberg
                    // tolerates).
                    let pg_type_unwrapped = pg_type.ok_or_else(|| {
                        PgError::Protocol(format!(
                            "unsupported PG type for column {name} at oid {} (modifier {})",
                            c.type_id(),
                            c.type_modifier()
                        ))
                    })?;
                    let ty = pg2iceberg_core::map_pg_to_iceberg(pg_type_unwrapped)
                        .map_err(|e| PgError::Protocol(format!("type map for {name}: {e}")))?
                        .iceberg;
                    decoded_columns.push(RelationColumn {
                        name,
                        ty,
                        is_primary_key: is_pk,
                        nullable: !is_pk,
                    });
                }
                self.relations.insert(
                    r.rel_id(),
                    RelationCache {
                        ident: ident.clone(),
                        columns: internal_columns,
                    },
                );
                Ok(Some(DecodedMessage::Relation {
                    ident,
                    columns: decoded_columns,
                }))
            }
            PgMsg::Insert(ins) => {
                let rel = self.lookup_relation(ins.rel_id())?;
                let (after, _) = decode_tuple(ins.tuple(), &rel.columns)?;
                Ok(Some(DecodedMessage::Change(self.change_event(
                    rel.ident.clone(),
                    Op::Insert,
                    None,
                    Some(after),
                    Vec::new(),
                ))))
            }
            PgMsg::Update(upd) => {
                let rel = self.lookup_relation(upd.rel_id())?;
                let before = match upd.old_tuple().or_else(|| upd.key_tuple()) {
                    Some(t) => Some(decode_tuple(t, &rel.columns)?.0),
                    None => None,
                };
                let (after, unchanged) = decode_tuple(upd.new_tuple(), &rel.columns)?;
                Ok(Some(DecodedMessage::Change(self.change_event(
                    rel.ident.clone(),
                    Op::Update,
                    before,
                    Some(after),
                    unchanged,
                ))))
            }
            PgMsg::Delete(del) => {
                let rel = self.lookup_relation(del.rel_id())?;
                // Either old_tuple (REPLICA IDENTITY FULL) or key_tuple
                // (REPLICA IDENTITY DEFAULT/INDEX) carries the deleted PK.
                let before = match del.old_tuple().or_else(|| del.key_tuple()) {
                    Some(t) => Some(decode_tuple(t, &rel.columns)?.0),
                    None => None,
                };
                Ok(Some(DecodedMessage::Change(self.change_event(
                    rel.ident.clone(),
                    Op::Delete,
                    before,
                    None,
                    Vec::new(),
                ))))
            }
            PgMsg::Truncate(t) => {
                // pgoutput sends one Truncate carrying N rel_ids. We
                // explode that into one ChangeEvent per rel and queue
                // the rest for subsequent recv() calls.
                let rel_ids = t.rel_ids().to_vec();
                let mut events: VecDeque<DecodedMessage> = VecDeque::new();
                for rel_id in rel_ids {
                    let rel = match self.relations.get(&rel_id) {
                        Some(r) => r.clone(),
                        // Truncate of an unknown relation is a noop —
                        // we never received its Relation message so we
                        // have nothing to materialize.
                        None => continue,
                    };
                    events.push_back(DecodedMessage::Change(self.change_event(
                        rel.ident,
                        Op::Truncate,
                        None,
                        None,
                        Vec::new(),
                    )));
                }
                let first = match events.pop_front() {
                    Some(e) => e,
                    None => return Ok(None),
                };
                // Append remaining in arrival order; reader_task drains
                // `pending` with `pop_front` before reading the next env.
                self.pending.extend(events);
                Ok(Some(first))
            }
            // Origin / Type / Message are protocol-level metadata we
            // don't surface to consumers.
            PgMsg::Origin(_) | PgMsg::Type(_) | PgMsg::Message(_) => Ok(None),
            _ => Ok(None),
        }
    }

    fn lookup_relation(&self, rel_id: u32) -> Result<RelationCache> {
        self.relations
            .get(&rel_id)
            .cloned()
            .ok_or_else(|| PgError::Protocol(format!("DML for unknown relation oid {rel_id}")))
    }

    fn change_event(
        &self,
        table: TableIdent,
        op: Op,
        before: Option<Row>,
        after: Option<Row>,
        unchanged_cols: Vec<ColumnName>,
    ) -> ChangeEvent {
        let txn = self.current_txn;
        ChangeEvent {
            table,
            op,
            // pgoutput delivers DML messages between Begin and Commit;
            // every row in a txn is tagged with the commit LSN, not
            // the per-row WAL position. This keeps cross-row ordering
            // consistent with the txn boundary downstream.
            lsn: txn.map(|t| t.final_lsn).unwrap_or(Lsn(0)),
            commit_ts: txn.map(|t| t.commit_ts).unwrap_or(Timestamp(0)),
            xid: txn.map(|t| t.xid),
            before,
            after,
            unchanged_cols,
        }
    }
}

/// Result of one outer-loop poll. The select! borrows `stream` to wait
/// on `next()`; restructuring as an enum lets the borrow end before we
/// run `state.handle()` or call `standby_status_update()` on the stream
/// in the cmd arm.
enum Branch {
    Event(Option<Result<DecodedMessage>>),
    Cmd(Option<Cmd>),
    /// PG sent a `PrimaryKeepAlive` with `reply_requested=1`. We must
    /// respond with a `standby_status_update` carrying any LSN —
    /// resending `last_ack` confirms liveness without advancing the
    /// slot beyond what the main loop has durably persisted via the
    /// cmd-channel ack path. Mirrors pgx's `pglogrepl` behaviour.
    KeepaliveReplyRequested,
}

/// One item read off the replication wire. Mirrors the subset of
/// `postgres_replication::ReplicationMessage` the reader acts on, but as
/// our own type — the wire-protocol structs have no public constructors,
/// so abstracting at this level is what lets [`reader_task`] be driven by
/// a mock in tests.
pub(crate) enum WireItem {
    /// Server heartbeat. `reply_requested` mirrors PG's
    /// `PrimaryKeepAlive.reply()`.
    Keepalive { reply_requested: bool },
    /// A logical-replication message to decode via [`ReaderState::handle`].
    Logical(PgEnv<PgMsg>),
    /// Stream ended cleanly.
    Closed,
    /// Wire/protocol error.
    Error(String),
}

/// The replication wire the reader owns: a source of [`WireItem`]s plus
/// the standby-ack write. Abstracted away from
/// `LogicalReplicationStream` so [`reader_task`] is unit-testable — the
/// deadlock it guards against (a `Cmd::Standby` starved by a keepalive
/// flood) is invisible to the DST sim, which replaces the whole reader.
#[async_trait]
pub(crate) trait ReplicationWire: Send {
    async fn next(&mut self) -> WireItem;
    async fn standby(&mut self, write: PgLsn, flush: PgLsn, apply: PgLsn) -> Result<()>;
}

#[async_trait]
impl ReplicationWire for Pin<Box<LogicalReplicationStream>> {
    async fn next(&mut self) -> WireItem {
        match StreamExt::next(self).await {
            None => WireItem::Closed,
            Some(Err(e)) => WireItem::Error(e.to_string()),
            Some(Ok(PgEnv::PrimaryKeepAlive(ka))) => WireItem::Keepalive {
                reply_requested: ka.reply() != 0,
            },
            Some(Ok(env)) => WireItem::Logical(env),
        }
    }

    async fn standby(&mut self, write: PgLsn, flush: PgLsn, apply: PgLsn) -> Result<()> {
        self.as_mut()
            .standby_status_update(write, flush, apply, 0, 0)
            .await
            .map_err(|e| PgError::Protocol(e.to_string()))
    }
}

/// Process a `Cmd` from the main loop. Returns `false` when the cmd
/// channel is closed (reader should exit).
///
/// Servicing a `Standby` here writes the ack to the wire and fulfils the
/// `done` oneshot the main loop is blocked on (`send_standby`). Doing
/// this even while the reader is *waiting for events-channel capacity*
/// (see [`send_servicing_cmds`]) is what prevents the reader↔main-loop
/// deadlock: the main loop's Standby tick blocks on `done` while it has
/// stopped draining the events channel, so if the reader could only ack
/// after a `send` completes, a full events channel would wedge both
/// sides permanently. Regression test: `tests::standby_acked_while_events_channel_full`.
async fn handle_cmd<W: ReplicationWire>(
    wire: &mut W,
    last_ack: &mut PgLsn,
    cmd: Option<Cmd>,
) -> bool {
    match cmd {
        None => false,
        Some(Cmd::Standby {
            flushed,
            applied,
            done,
        }) => {
            let write_lsn: PgLsn = flushed.0.into();
            let flush_lsn: PgLsn = flushed.0.into();
            let apply_lsn: PgLsn = applied.0.into();
            // Cache for keepalive `reply_requested=1` replies so the
            // reader can confirm liveness without going through the cmd
            // channel. Update *before* the wire write — even if it fails,
            // we still want the latest LSN cached for future replies.
            *last_ack = write_lsn;
            let res = wire.standby(write_lsn, flush_lsn, apply_lsn).await;
            // Caller may have given up (e.g. shutdown); ignore a closed rx.
            let _ = done.send(res);
            true
        }
    }
}

/// Send one decoded message to the consumer, but keep servicing `cmd_rx`
/// while waiting for events-channel capacity. Returns `false` if the
/// reader should exit (consumer gone or cmd channel closed).
///
/// A bare `events_tx.send(msg).await` would park the reader on a full
/// channel *without* polling `cmd_rx` — and the main loop, blocked in
/// `send_standby` awaiting an ack while no longer draining the channel,
/// would never free a slot. `select!`ing the send permit against
/// `cmd_rx` breaks that cycle: a `Standby` is acked, the main loop
/// resumes draining, a slot frees, and the send completes.
async fn send_servicing_cmds<W: ReplicationWire>(
    wire: &mut W,
    events_tx: &mpsc::Sender<Result<DecodedMessage>>,
    cmd_rx: &mut mpsc::Receiver<Cmd>,
    last_ack: &mut PgLsn,
    msg: Result<DecodedMessage>,
) -> bool {
    let mut msg = Some(msg);
    loop {
        tokio::select! {
            biased;
            // Ack a pending Standby even though we can't send yet.
            cmd = cmd_rx.recv() => {
                if !handle_cmd(wire, last_ack, cmd).await {
                    return false;
                }
            }
            permit = events_tx.reserve() => match permit {
                Ok(permit) => {
                    permit.send(msg.take().expect("msg sent exactly once"));
                    return true;
                }
                // Consumer dropped the receiver — reader should exit.
                Err(_) => return false,
            },
        }
    }
}

/// Reader task: sole owner of the replication [`ReplicationWire`]. Drains
/// decoded events into `events_tx` and serves ack requests from
/// `cmd_rx`. Exits cleanly when either channel side is dropped, the
/// stream errors, or `JoinHandle::abort` fires.
async fn reader_task<W: ReplicationWire>(
    mut wire: W,
    events_tx: mpsc::Sender<Result<DecodedMessage>>,
    mut cmd_rx: mpsc::Receiver<Cmd>,
) {
    let mut state = ReaderState::default();
    // Last LSN we acked back to PG via `Cmd::Standby`. Reused for
    // `reply_requested=1` keepalive replies to confirm liveness without
    // advancing the slot beyond what the main loop has durably persisted.
    // `0` until the first periodic Standby tick (or the post-snapshot
    // initial ack) lands; PG accepts that as "consumer is alive but at
    // unknown position".
    let mut last_ack: PgLsn = PgLsn::from(0u64);

    loop {
        // Drain queued events first (e.g. a Truncate that exploded into
        // multiple per-rel ChangeEvents). Order is preserved via
        // VecDeque::pop_front. Each send awaits if the events channel
        // is full — that's the backpressure path; main loop draining
        // unblocks us.
        if let Some(msg) = state.pending.pop_front() {
            if !send_servicing_cmds(&mut wire, &events_tx, &mut cmd_rx, &mut last_ack, Ok(msg))
                .await
            {
                return;
            }
            continue;
        }

        let branch: Branch = tokio::select! {
            biased;
            // Service standby acks FIRST. The slot only advances when the
            // main loop's Standby tick reaches the wire via `Cmd::Standby`
            // (which blocks on a oneshot until we ack here). If `wire.next()`
            // were biased ahead of this, a keepalive flood — exactly what PG
            // emits when the slot is pinned and there's heavy WAL churn on
            // tables OUTSIDE the publication (e.g. the `_pg2iceberg` coord
            // schema, written every main-loop iteration) — keeps `next`
            // perpetually ready, starves this arm, and `send_standby`
            // deadlocks: the ack that would advance the slot and stop the
            // flood can never be processed. Standby cmds are rare (~every
            // standby_interval), so checking them first costs nothing and
            // the event arm below still drains the wire. Regression test:
            // `tests::standby_is_serviced_under_keepalive_flood`.
            cmd = cmd_rx.recv() => Branch::Cmd(cmd),
            item = wire.next() => match item {
                WireItem::Closed => Branch::Event(Some(Err(PgError::Connection(
                    "replication stream closed".into(),
                )))),
                WireItem::Error(e) => Branch::Event(Some(Err(PgError::Protocol(e)))),
                // Keepalives never go through the events channel.
                WireItem::Keepalive { reply_requested } => {
                    if reply_requested {
                        Branch::KeepaliveReplyRequested
                    } else {
                        Branch::Event(None)
                    }
                }
                WireItem::Logical(env) => Branch::Event(state.handle(env).transpose()),
            },
        };

        match branch {
            Branch::Event(None) => {
                // state.handle returned Ok(None) — Origin/Type/Message
                // protocol metadata, or a dropped keepalive. Loop and
                // read more.
            }
            Branch::Event(Some(Ok(msg))) => {
                if !send_servicing_cmds(&mut wire, &events_tx, &mut cmd_rx, &mut last_ack, Ok(msg))
                    .await
                {
                    return;
                }
            }
            Branch::Event(Some(Err(e))) => {
                // Deliver the terminal error (best-effort, still acking a
                // pending Standby if the channel is full), then exit.
                let _ =
                    send_servicing_cmds(&mut wire, &events_tx, &mut cmd_rx, &mut last_ack, Err(e))
                        .await;
                return;
            }
            Branch::KeepaliveReplyRequested => {
                if let Err(e) = wire.standby(last_ack, last_ack, last_ack).await {
                    let _ = events_tx.send(Err(e)).await;
                    return;
                }
            }
            Branch::Cmd(cmd) => {
                if !handle_cmd(&mut wire, &mut last_ack, cmd).await {
                    return;
                }
            }
        }
    }
}

#[cfg(test)]
mod reader_tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Mutex};

    /// A wire that floods non-reply keepalives — each `next()` is
    /// immediately ready — until `stop` is set, then reports the stream
    /// closed so the reader exits. Records standby acks. Models PG
    /// emitting a continuous keepalive stream (slot pinned + heavy
    /// non-published WAL): every `next()` poll is ready, so a reader
    /// biased to the stream would never poll `cmd_rx`.
    struct FloodWire {
        standbys: Arc<Mutex<Vec<(PgLsn, PgLsn, PgLsn)>>>,
        stop: Arc<AtomicBool>,
    }

    #[async_trait]
    impl ReplicationWire for FloodWire {
        async fn next(&mut self) -> WireItem {
            if self.stop.load(Ordering::Relaxed) {
                WireItem::Closed
            } else {
                WireItem::Keepalive {
                    reply_requested: false,
                }
            }
        }
        async fn standby(&mut self, w: PgLsn, f: PgLsn, a: PgLsn) -> Result<()> {
            self.standbys.lock().unwrap().push((w, f, a));
            Ok(())
        }
    }

    /// Regression for the replication-reader deadlock: under a keepalive
    /// flood, a `Cmd::Standby` (the main loop's slot ack) must still be
    /// serviced. With the reader biased to the stream it never is — the
    /// slot never advances and CDC silently stops after the snapshot.
    /// Multi-thread so the flood (a busy, non-yielding reader) runs on one
    /// worker while this test drives the assertions + timeout on another.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn standby_is_serviced_under_keepalive_flood() {
        let standbys = Arc::new(Mutex::new(Vec::new()));
        let stop = Arc::new(AtomicBool::new(false));
        let wire = FloodWire {
            standbys: standbys.clone(),
            stop: stop.clone(),
        };
        let (events_tx, _events_rx) =
            mpsc::channel::<Result<DecodedMessage>>(EVENTS_CHANNEL_CAPACITY);
        let (cmd_tx, cmd_rx) = mpsc::channel::<Cmd>(CMD_CHANNEL_CAPACITY);
        let reader = tokio::spawn(reader_task(wire, events_tx, cmd_rx));

        // Issue a standby ack, exactly like the main loop's Standby tick.
        let (done_tx, done_rx) = oneshot::channel();
        cmd_tx
            .send(Cmd::Standby {
                flushed: Lsn(42),
                applied: Lsn(42),
                done: done_tx,
            })
            .await
            .expect("reader accepts the cmd");

        // New (cmd-first): resolves immediately. Old (stream-first): the
        // flood starves it and this times out.
        let res = tokio::time::timeout(std::time::Duration::from_secs(5), done_rx).await;

        // Stop the flood so the reader exits cleanly regardless of outcome
        // (it polls `next()` every iteration even when stream-biased).
        stop.store(true, Ordering::Relaxed);
        let _ = reader.await;

        let ack = res
            .expect("standby ack starved by keepalive flood — reader deadlock regressed")
            .expect("reader dropped the ack oneshot");
        assert!(ack.is_ok(), "standby should succeed, got {ack:?}");
        assert_eq!(
            standbys.lock().unwrap().as_slice(),
            &[(PgLsn::from(42u64), PgLsn::from(42u64), PgLsn::from(42u64))],
            "reader should have written exactly the requested standby LSN",
        );
    }

    /// Regression for the second reader deadlock (found via the AWS
    /// benchmark + a tokio task dump): when the events channel is FULL,
    /// the reader must still service a `Cmd::Standby`. A bare
    /// `events_tx.send().await` parks the reader on the full channel
    /// without polling `cmd_rx`; the main loop, blocked in `send_standby`
    /// awaiting that ack while it has stopped draining the channel, then
    /// never frees a slot → both wedge. `send_servicing_cmds` must ack
    /// the standby anyway, letting the main loop resume and drain.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn standby_acked_while_events_channel_full() {
        let standbys = Arc::new(Mutex::new(Vec::new()));
        let mut wire = FloodWire {
            standbys: standbys.clone(),
            stop: Arc::new(AtomicBool::new(true)),
        };
        // Capacity-1 events channel, pre-filled → the next send must wait.
        let (events_tx, mut events_rx) = mpsc::channel::<Result<DecodedMessage>>(1);
        events_tx
            .send(Ok(DecodedMessage::Begin {
                final_lsn: Lsn(1),
                xid: 1,
            }))
            .await
            .expect("prime the channel");
        let (cmd_tx, mut cmd_rx) = mpsc::channel::<Cmd>(CMD_CHANNEL_CAPACITY);
        let mut last_ack = PgLsn::from(0u64);

        // The main loop's slot ack, queued while the channel is full.
        let (done_tx, done_rx) = oneshot::channel();
        cmd_tx
            .send(Cmd::Standby {
                flushed: Lsn(42),
                applied: Lsn(42),
                done: done_tx,
            })
            .await
            .expect("queue the standby");

        let msg2 = Ok(DecodedMessage::Begin {
            final_lsn: Lsn(2),
            xid: 2,
        });
        let helper = send_servicing_cmds(&mut wire, &events_tx, &mut cmd_rx, &mut last_ack, msg2);

        let driver = async {
            // Fix: acked despite no free slot. Pre-fix: parked in send,
            // never acked → this times out.
            let ack = tokio::time::timeout(std::time::Duration::from_secs(5), done_rx)
                .await
                .expect("standby starved by a full events channel — deadlock regressed")
                .expect("reader dropped the ack oneshot");
            assert!(ack.is_ok(), "standby should succeed, got {ack:?}");
            // Free a slot so the queued send can complete.
            let first = events_rx.recv().await.expect("first message");
            assert!(matches!(first, Ok(DecodedMessage::Begin { xid: 1, .. })));
        };

        let (sent, ()) = tokio::join!(helper, driver);
        assert!(sent, "send should complete once a slot frees");
        let second = events_rx.recv().await.expect("second message");
        assert!(matches!(second, Ok(DecodedMessage::Begin { xid: 2, .. })));
        assert_eq!(
            standbys.lock().unwrap().as_slice(),
            &[(PgLsn::from(42u64), PgLsn::from(42u64), PgLsn::from(42u64))],
            "standby LSN should have reached the wire while the channel was full",
        );
    }
}

fn io_to_pg(e: std::io::Error) -> PgError {
    PgError::Protocol(format!("relation field decode: {e}"))
}

fn build_table_ident(namespace: &str, name: &str) -> TableIdent {
    TableIdent {
        namespace: Namespace(if namespace.is_empty() {
            Vec::new()
        } else {
            vec![namespace.to_string()]
        }),
        name: name.to_string(),
    }
}

/// Decode a wire `Tuple` into our `Row`, returning the row plus the list
/// of column names whose tuple data was `UnchangedToast` (for downstream
/// FileIndex resolution).
fn decode_tuple(tuple: &Tuple, columns: &[ColumnInfo]) -> Result<(Row, Vec<ColumnName>)> {
    let data = tuple.tuple_data();
    if data.len() != columns.len() {
        return Err(PgError::Protocol(format!(
            "tuple has {} columns, relation has {}",
            data.len(),
            columns.len()
        )));
    }
    let mut row: Row = Row::new();
    let mut unchanged: Vec<ColumnName> = Vec::new();
    for (col, td) in columns.iter().zip(data.iter()) {
        let key = ColumnName(col.name.clone());
        match td {
            TupleData::Null => {
                row.insert(key, PgValue::Null);
            }
            TupleData::UnchangedToast => {
                // Keep the row entry as Null so downstream code that
                // iterates the row doesn't trip; the unchanged_cols
                // vector tells the materializer which entries to
                // resolve from the FileIndex.
                row.insert(key.clone(), PgValue::Null);
                unchanged.push(key);
            }
            TupleData::Text(bytes) => {
                let pg_type = col.pg_type.ok_or_else(|| {
                    PgError::Protocol(format!("unsupported column type for {}", col.name))
                })?;
                let val = decode_text(pg_type, bytes)
                    .map_err(|e| PgError::Protocol(format!("column {}: {e}", col.name)))?;
                row.insert(key, val);
            }
            TupleData::Binary(_) => {
                return Err(PgError::Protocol(format!(
                    "binary tuple data for {} not supported (proto_version 4); \
                     pg2iceberg uses text mode",
                    col.name
                )));
            }
        }
    }
    Ok((row, unchanged))
}

fn pg_micros_to_timestamp(pg_micros_since_2k: i64) -> Timestamp {
    Timestamp(PG_EPOCH_OFFSET_MICROS + pg_micros_since_2k)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pg_micros_offsets_to_unix_correctly() {
        // PG epoch (2000-01-01) corresponds to PG-micros-since-2K = 0.
        // In unix micros: (2000 - 1970) years * (avg seconds) ≈ 946_684_800
        // seconds. Confirm exactly.
        assert_eq!(pg_micros_to_timestamp(0).0, 946_684_800_000_000);
        // 1 year later: 31_536_000 seconds = 31_536_000_000_000 micros.
        assert_eq!(
            pg_micros_to_timestamp(31_536_000_000_000).0,
            946_684_800_000_000 + 31_536_000_000_000
        );
    }

    #[test]
    fn build_table_ident_handles_empty_namespace() {
        let id = build_table_ident("", "t");
        assert!(id.namespace.0.is_empty());
        assert_eq!(id.name, "t");

        let id = build_table_ident("public", "orders");
        assert_eq!(id.namespace.0, vec!["public".to_string()]);
        assert_eq!(id.name, "orders");
    }
}
