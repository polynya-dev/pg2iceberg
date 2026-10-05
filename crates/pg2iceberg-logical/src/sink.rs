//! Per-tx buffering + per-table rolling writers.
//!
//! Mirrors `logical/events.go`'s Sink. Behaviorally:
//! - `Begin` opens a tx buffer keyed by xid.
//! - DML events are buffered in their tx; events with no xid (snapshot rows)
//!   go directly into the per-table writer.
//! - `Commit` moves the buffer to the committed list; a flush then drains all
//!   committed buffers into the per-table writers and produces chunks.
//! - An open tx that grows too large is spilled: its buffered events are
//!   encoded into chunks for the pipeline to stage, so tx size never bounds
//!   memory (see [`Sink::spill_open_tx`]).
//! - `Rollback`-equivalent: a tx that never receives Commit but is replaced
//!   by a new Begin with the same xid is silently dropped. Real PG can't
//!   reuse an xid, so this only happens on testing fault paths.

use pg2iceberg_core::{ChangeEvent, Lsn, Op, TableIdent, Timestamp};
use pg2iceberg_stream::codec::{encode_chunk, EncodedChunk};
use pg2iceberg_stream::rolling::RollingWriter;
use pg2iceberg_stream::StreamError;
use std::collections::{BTreeMap, VecDeque};
use thiserror::Error;

#[derive(Clone, Debug, Error)]
pub enum SinkError {
    #[error("stream: {0}")]
    Stream(#[from] StreamError),
    #[error("event missing payload row for op {op:?}")]
    MissingPayload { op: Op },
}

pub type Result<T> = std::result::Result<T, SinkError>;

#[derive(Debug)]
pub(crate) struct TxBuffer {
    pub(crate) events: Vec<ChangeEvent>,
    pub(crate) commit_lsn: Option<Lsn>,
    pub(crate) commit_ts: Option<Timestamp>,
}

/// Output of a Sink flush: per-table chunk(s) ready for upload + claim.
#[derive(Debug)]
pub struct FlushOutput {
    pub chunks: Vec<TableChunk>,
    /// Highest LSN covered by this flush — what the pipeline hands to
    /// `CommitBatch::flushable_lsn` so the receipt advances `flushedLSN` to it.
    pub flushable_lsn: Lsn,
}

#[derive(Debug)]
pub struct TableChunk {
    pub table: TableIdent,
    pub chunk: EncodedChunk,
}

/// Sink. Owns per-table writers and per-tx buffers; not aware of coord/blob.
pub struct Sink {
    /// Per-table rolling writers, lazily created on first event.
    table_writers: BTreeMap<TableIdent, RollingWriter>,
    /// Chunks closed early, each because a relation event starts a new
    /// one ([`Self::append_event`]); the next flush emits them first.
    closed: Vec<TableChunk>,
    /// In-flight transactions keyed by xid.
    open_txns: BTreeMap<u32, TxBuffer>,
    /// Committed-but-unflushed transactions, in commit order.
    committed: VecDeque<TxBuffer>,
    /// Configurable target rows per chunk; passed to each table's RollingWriter.
    flush_threshold: usize,
}

impl Sink {
    pub fn new(flush_threshold: usize) -> Self {
        assert!(flush_threshold > 0);
        Self {
            table_writers: BTreeMap::new(),
            closed: Vec::new(),
            open_txns: BTreeMap::new(),
            committed: VecDeque::new(),
            flush_threshold,
        }
    }

    pub fn begin_tx(&mut self, xid: u32) {
        self.open_txns.insert(
            xid,
            TxBuffer {
                events: Vec::new(),
                commit_lsn: None,
                commit_ts: None,
            },
        );
    }

    pub fn record_change(&mut self, evt: ChangeEvent) -> Result<()> {
        // Sanity: DML events must have either before or after.
        match evt.op {
            Op::Insert | Op::Update if evt.after.is_none() => {
                return Err(SinkError::MissingPayload { op: evt.op });
            }
            Op::Delete if evt.before.is_none() => {
                return Err(SinkError::MissingPayload { op: evt.op });
            }
            _ => {}
        }

        match evt.xid {
            Some(xid) if self.open_txns.contains_key(&xid) => {
                let tx = self.open_txns.get_mut(&xid).expect("checked above");
                // A relation event carries no commit timestamp.
                if evt.op != Op::Relation {
                    tx.commit_ts = Some(evt.commit_ts);
                }
                tx.events.push(evt);
            }
            _ => {
                // No tx: e.g. a snapshot-phase event. Stage immediately.
                self.append_event(evt)?;
            }
        }
        Ok(())
    }

    pub fn commit_tx(&mut self, xid: u32, commit_lsn: Lsn) {
        if let Some(mut tx) = self.open_txns.remove(&xid) {
            tx.commit_lsn = Some(commit_lsn);
            self.committed.push_back(tx);
        }
    }

    pub fn has_committed(&self) -> bool {
        !self.committed.is_empty()
    }

    /// `true` while a transaction has begun but not yet committed.
    pub fn has_open_tx(&self) -> bool {
        !self.open_txns.is_empty()
    }

    /// Change events buffered for the open transaction `xid`.
    pub fn open_tx_rows(&self, xid: u32) -> usize {
        self.open_txns.get(&xid).map_or(0, |t| t.events.len())
    }

    /// Change events in committed-but-unflushed transactions.
    pub fn committed_rows(&self) -> usize {
        self.committed.iter().map(|t| t.events.len()).sum()
    }

    /// Encode the open transaction `xid`'s buffered events into one chunk
    /// per table and drop them from memory. The transaction stays open;
    /// the caller stages the chunks and claims them once it commits.
    pub fn spill_open_tx(&mut self, xid: u32) -> Result<Vec<TableChunk>> {
        let Some(tx) = self.open_txns.get_mut(&xid) else {
            return Ok(Vec::new());
        };
        let mut by_table: BTreeMap<TableIdent, Vec<ChangeEvent>> = BTreeMap::new();
        for evt in tx.events.drain(..) {
            by_table.entry(evt.table.clone()).or_default().push(evt);
        }
        let mut out = Vec::new();
        for (table, events) in by_table {
            // As in `append_event`: a relation event starts a chunk.
            let mut part: Vec<ChangeEvent> = Vec::new();
            for evt in events {
                if evt.op == Op::Relation && !part.is_empty() {
                    out.push(TableChunk {
                        table: table.clone(),
                        chunk: encode_chunk(&std::mem::take(&mut part))?,
                    });
                }
                part.push(evt);
            }
            out.push(TableChunk {
                table,
                chunk: encode_chunk(&part)?,
            });
        }
        Ok(out)
    }

    /// Change events currently held in memory: open transactions,
    /// committed-but-unflushed transactions, and per-table writers.
    pub fn buffered_rows(&self) -> usize {
        self.open_txns
            .values()
            .map(|t| t.events.len())
            .sum::<usize>()
            + self.committed.iter().map(|t| t.events.len()).sum::<usize>()
            + self.table_writers.values().map(|w| w.len()).sum::<usize>()
    }

    /// Drains every committed tx into per-table writers, flushes them, and
    /// returns the resulting chunks plus the highest commit_lsn covered.
    /// Returns `None` when nothing is ready.
    pub fn flush(&mut self) -> Result<Option<FlushOutput>> {
        if self.committed.is_empty() {
            return Ok(None);
        }

        // Fold committed events into per-table writers.
        let mut max_lsn = Lsn::ZERO;
        let txns: Vec<TxBuffer> = self.committed.drain(..).collect();
        for tx in txns {
            if let Some(lsn) = tx.commit_lsn {
                if lsn > max_lsn {
                    max_lsn = lsn;
                }
            }
            for evt in tx.events {
                self.append_event(evt)?;
            }
        }

        // Flush every writer that has buffered rows, after the chunks
        // closed early (which hold earlier events).
        let mut chunks = std::mem::take(&mut self.closed);
        for (table, writer) in self.table_writers.iter_mut() {
            if let Some(chunk) = writer.flush()? {
                chunks.push(TableChunk {
                    table: table.clone(),
                    chunk,
                });
            }
        }

        Ok(Some(FlushOutput {
            chunks,
            flushable_lsn: max_lsn,
        }))
    }

    /// A relation event (a schema change) starts a new chunk: the
    /// materializer applies it at a log entry's start, once the entries
    /// before it — rows written under the old schema — are committed.
    fn append_event(&mut self, evt: ChangeEvent) -> Result<()> {
        let table = evt.table.clone();
        let writer = self
            .table_writers
            .entry(table.clone())
            .or_insert_with(|| RollingWriter::new(self.flush_threshold));
        if evt.op == Op::Relation {
            if let Some(chunk) = writer.flush()? {
                self.closed.push(TableChunk { table, chunk });
            }
        }
        writer.append(evt);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pg2iceberg_core::{ColumnName, Namespace, PgValue, Row};
    use std::collections::BTreeMap;

    fn ident() -> TableIdent {
        TableIdent {
            namespace: Namespace(vec!["public".into()]),
            name: "t".into(),
        }
    }

    fn row(id: i32) -> Row {
        let mut r = BTreeMap::new();
        r.insert(ColumnName("id".into()), PgValue::Int4(id));
        r
    }

    fn relation(xid: u32, lsn: u64) -> ChangeEvent {
        ChangeEvent {
            op: Op::Relation,
            after: Some(crate::relation_event::encode(&vec![])),
            ..insert(xid, lsn, 0)
        }
    }

    /// Each chunk's ops, decoded.
    fn ops(chunks: &[TableChunk]) -> Vec<Vec<Op>> {
        chunks
            .iter()
            .map(|c| {
                pg2iceberg_stream::codec::decode_chunk(&c.chunk.bytes)
                    .unwrap()
                    .into_iter()
                    .map(|e| e.op)
                    .collect()
            })
            .collect()
    }

    #[test]
    fn a_relation_event_starts_a_chunk() {
        let mut sink = Sink::new(100);
        for (xid, lsn) in [(1, 10), (2, 20)] {
            sink.begin_tx(xid);
            sink.record_change(relation(xid, lsn)).unwrap();
            sink.record_change(insert(xid, lsn, xid as i32)).unwrap();
            sink.commit_tx(xid, Lsn(lsn));
        }
        let out = sink.flush().unwrap().unwrap();
        assert_eq!(
            ops(&out.chunks),
            [[Op::Relation, Op::Insert], [Op::Relation, Op::Insert]]
        );

        // Likewise when an open transaction is spilled.
        sink.begin_tx(3);
        sink.record_change(insert(3, 30, 3)).unwrap();
        sink.record_change(relation(3, 30)).unwrap();
        sink.record_change(insert(3, 30, 4)).unwrap();
        assert_eq!(
            ops(&sink.spill_open_tx(3).unwrap()),
            vec![vec![Op::Insert], vec![Op::Relation, Op::Insert]]
        );
    }

    fn insert(xid: u32, lsn: u64, id: i32) -> ChangeEvent {
        ChangeEvent {
            table: ident(),
            op: Op::Insert,
            lsn: Lsn(lsn),
            commit_ts: Timestamp(0),
            xid: Some(xid),
            before: None,
            after: Some(row(id)),
            unchanged_cols: vec![],
        }
    }

    #[test]
    fn flush_with_no_commits_returns_none() {
        let mut s = Sink::new(100);
        assert!(s.flush().unwrap().is_none());
    }

    #[test]
    fn open_tx_event_does_not_appear_until_commit() {
        let mut s = Sink::new(100);
        s.begin_tx(1);
        s.record_change(insert(1, 10, 1)).unwrap();
        // Not flushed yet — tx isn't committed.
        assert!(!s.has_committed());
        assert!(s.flush().unwrap().is_none());

        s.commit_tx(1, Lsn(11));
        assert!(s.has_committed());
        let out = s.flush().unwrap().expect("flush should produce output");
        assert_eq!(out.flushable_lsn, Lsn(11));
        assert_eq!(out.chunks.len(), 1);
        assert_eq!(out.chunks[0].chunk.record_count, 1);
    }

    #[test]
    fn flushable_lsn_is_max_commit_lsn_across_txns() {
        let mut s = Sink::new(100);
        s.begin_tx(1);
        s.record_change(insert(1, 10, 1)).unwrap();
        s.commit_tx(1, Lsn(11));

        s.begin_tx(2);
        s.record_change(insert(2, 20, 2)).unwrap();
        s.commit_tx(2, Lsn(21));

        let out = s.flush().unwrap().unwrap();
        assert_eq!(out.flushable_lsn, Lsn(21));
        // One table, both rows in one chunk.
        assert_eq!(out.chunks.len(), 1);
        assert_eq!(out.chunks[0].chunk.record_count, 2);
    }

    #[test]
    fn untracked_xid_event_stages_directly() {
        // Snapshot-phase events arrive without an open tx — they should still
        // make it into the per-table writer.
        let mut s = Sink::new(100);
        let mut evt = insert(0, 1, 1);
        evt.xid = None;
        s.record_change(evt).unwrap();

        // Trigger a flush by also committing a noop tx so flush() returns Some.
        s.begin_tx(99);
        s.commit_tx(99, Lsn(2));
        let out = s.flush().unwrap().unwrap();
        assert_eq!(out.chunks[0].chunk.record_count, 1);
        assert_eq!(out.flushable_lsn, Lsn(2));
    }

    #[test]
    fn missing_payload_row_is_an_error() {
        let mut s = Sink::new(100);
        let evt = ChangeEvent {
            table: ident(),
            op: Op::Insert,
            lsn: Lsn(1),
            commit_ts: Timestamp(0),
            xid: None,
            before: None,
            after: None,
            unchanged_cols: vec![],
        };
        let err = s.record_change(evt).unwrap_err();
        assert!(matches!(err, SinkError::MissingPayload { op: Op::Insert }));
    }
}
