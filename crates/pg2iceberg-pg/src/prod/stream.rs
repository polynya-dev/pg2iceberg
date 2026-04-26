//! [`ReplicationStreamImpl`]: production [`ReplicationStream`] wrapping
//! `postgres_replication::LogicalReplicationStream`.
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
//! - `Origin` / `Type` / `Message` → silently skipped (matches Go
//!   reference behavior).
//! - `PrimaryKeepAlive` → `DecodedMessage::Keepalive`.

use crate::prod::typemap::pg_type_from_oid;
use crate::prod::value_decode::decode_text;
use crate::{ChangeEvent, DecodedMessage, PgError, ReplicationStream, Result};
use async_trait::async_trait;
use futures_util::StreamExt;
use pg2iceberg_core::typemap::PgType;
use pg2iceberg_core::Namespace;
use pg2iceberg_core::{ColumnName, Lsn, Op, PgValue, Row, TableIdent, Timestamp};
use postgres_replication::protocol::{
    LogicalReplicationMessage as PgMsg, ReplicationMessage as PgEnv, Tuple, TupleData,
};
use postgres_replication::LogicalReplicationStream;
use std::collections::HashMap;
use std::pin::Pin;
use tokio_postgres::types::PgLsn;

/// Postgres microsecond epoch starts at 2000-01-01; Unix epoch is
/// 1970-01-01. Difference in micros is constant and used both ways
/// for timestamp conversion.
const PG_EPOCH_OFFSET_MICROS: i64 = 946_684_800_000_000;

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

pub struct ReplicationStreamImpl {
    stream: Pin<Box<LogicalReplicationStream>>,
    relations: HashMap<u32, RelationCache>,
    /// Stashed per-txn metadata between `Begin` and `Commit`. Pgoutput
    /// only sends xid/commit_ts in the Begin record; DML messages
    /// inherit those via the surrounding txn.
    current_txn: Option<TxnContext>,
    /// Truncate carries N rel_ids; we emit one Change per rel_id.
    /// Anything that didn't fit in a single message gets buffered here
    /// and drained on subsequent `recv()` calls.
    pending: Vec<DecodedMessage>,
}

#[derive(Debug, Clone, Copy)]
struct TxnContext {
    final_lsn: Lsn,
    xid: u32,
    commit_ts: Timestamp,
}

impl ReplicationStreamImpl {
    pub(crate) fn wrap(stream: LogicalReplicationStream) -> Self {
        Self {
            stream: Box::pin(stream),
            relations: HashMap::new(),
            current_txn: None,
            pending: Vec::new(),
        }
    }
}

#[async_trait]
impl ReplicationStream for ReplicationStreamImpl {
    async fn recv(&mut self) -> Result<DecodedMessage> {
        loop {
            if let Some(msg) = self.pending.pop() {
                return Ok(msg);
            }
            let next = self
                .stream
                .as_mut()
                .next()
                .await
                .ok_or_else(|| PgError::Connection("replication stream closed".into()))?
                .map_err(|e| PgError::Protocol(e.to_string()))?;
            if let Some(out) = self.handle(next)? {
                return Ok(out);
            }
            // None means we consumed an Origin/Type/Message/Relation-cache-only
            // event and should keep polling.
        }
    }

    async fn send_standby(&mut self, flushed: Lsn, applied: Lsn) -> Result<()> {
        let write_lsn: PgLsn = flushed.0.into();
        let flush_lsn: PgLsn = flushed.0.into();
        let apply_lsn: PgLsn = applied.0.into();
        // Server uses `ts` for measurement-only, not correctness; 0 is
        // accepted. `reply = 0` means "don't ask me to send another now".
        self.stream
            .as_mut()
            .standby_status_update(write_lsn, flush_lsn, apply_lsn, 0, 0)
            .await
            .map_err(|e| PgError::Protocol(e.to_string()))
    }
}

impl ReplicationStreamImpl {
    fn handle(&mut self, env: PgEnv<PgMsg>) -> Result<Option<DecodedMessage>> {
        match env {
            PgEnv::PrimaryKeepAlive(body) => Ok(Some(DecodedMessage::Keepalive {
                wal_end: Lsn(body.wal_end()),
                reply_requested: body.reply() != 0,
            })),
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
                let columns: Vec<ColumnInfo> = r
                    .columns()
                    .iter()
                    .map(|c| {
                        let name = c.name().map_err(io_to_pg)?.to_string();
                        let pg_type = pg_type_from_oid(c.type_id() as u32, c.type_modifier());
                        Ok::<_, PgError>(ColumnInfo { name, pg_type })
                    })
                    .collect::<Result<_>>()?;
                self.relations.insert(
                    r.rel_id(),
                    RelationCache {
                        ident: ident.clone(),
                        columns,
                    },
                );
                Ok(Some(DecodedMessage::Relation { ident }))
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
                let mut events: Vec<DecodedMessage> = Vec::new();
                for rel_id in rel_ids {
                    let rel = match self.relations.get(&rel_id) {
                        Some(r) => r.clone(),
                        // Truncate of an unknown relation is a noop —
                        // we never received its Relation message so we
                        // have nothing to materialize.
                        None => continue,
                    };
                    events.push(DecodedMessage::Change(self.change_event(
                        rel.ident,
                        Op::Truncate,
                        None,
                        None,
                        Vec::new(),
                    )));
                }
                if events.is_empty() {
                    return Ok(None);
                }
                let first = events.remove(0);
                // Buffer the rest in reverse so `pop()` returns them in
                // arrival order on subsequent recv() calls.
                events.reverse();
                self.pending.extend(events);
                Ok(Some(first))
            }
            // Origin / Type / Message are protocol-level metadata we
            // don't surface — matches the Go reference's behavior.
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
            // assigning the txn's final_lsn matches the Go reference's
            // semantics (every row in a txn is tagged with the commit
            // LSN, not the per-row WAL position).
            lsn: txn.map(|t| t.final_lsn).unwrap_or(Lsn(0)),
            commit_ts: txn.map(|t| t.commit_ts).unwrap_or(Timestamp(0)),
            xid: txn.map(|t| t.xid),
            before,
            after,
            unchanged_cols,
        }
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
