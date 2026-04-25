//! Row-buffered writer that produces a Parquet chunk when a threshold trips.
//!
//! Mirrors the Go `RollingDataWriter` shape (`iceberg/writer.go`), but is much
//! simpler at this stage: events are buffered in memory and the codec encodes
//! once per [`flush`]. Streaming encode (per-row) and time/byte thresholds are
//! deferred until there's a reason — premature optimization here would just
//! complicate the DST surface.

use crate::codec::{encode_chunk, EncodedChunk};
use crate::Result;
use pg2iceberg_core::ChangeEvent;

#[derive(Debug)]
pub struct RollingWriter {
    max_rows: usize,
    events: Vec<ChangeEvent>,
}

impl RollingWriter {
    /// Construct a writer that flushes when `max_rows` events are buffered.
    /// Panics if `max_rows == 0` — a zero threshold has no useful meaning.
    pub fn new(max_rows: usize) -> Self {
        assert!(max_rows > 0, "RollingWriter::max_rows must be > 0");
        Self {
            max_rows,
            events: Vec::with_capacity(max_rows.min(1024)),
        }
    }

    pub fn append(&mut self, evt: ChangeEvent) {
        self.events.push(evt);
    }

    pub fn len(&self) -> usize {
        self.events.len()
    }

    pub fn is_empty(&self) -> bool {
        self.events.is_empty()
    }

    /// True once the buffer has reached the configured threshold. The pipeline
    /// reads this between transaction boundaries to decide whether to flush.
    pub fn should_flush(&self) -> bool {
        self.events.len() >= self.max_rows
    }

    /// Encode the buffered events into a Parquet chunk and clear the buffer.
    /// Returns `None` when the buffer is empty so callers can blindly call
    /// flush on every cycle without paying for an empty Parquet file.
    pub fn flush(&mut self) -> Result<Option<EncodedChunk>> {
        if self.events.is_empty() {
            return Ok(None);
        }
        let chunk = encode_chunk(&self.events)?;
        self.events.clear();
        Ok(Some(chunk))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::decode_chunk;
    use pg2iceberg_core::{ColumnName, Lsn, Namespace, Op, PgValue, Row, TableIdent, Timestamp};
    use std::collections::BTreeMap;

    fn ident() -> TableIdent {
        TableIdent {
            namespace: Namespace(vec!["public".into()]),
            name: "t".into(),
        }
    }

    fn row_with(id: i32) -> Row {
        let mut r = BTreeMap::new();
        r.insert(ColumnName("id".into()), PgValue::Int4(id));
        r
    }

    fn evt(lsn: u64) -> ChangeEvent {
        ChangeEvent {
            table: ident(),
            op: Op::Insert,
            lsn: Lsn(lsn),
            commit_ts: Timestamp(0),
            xid: Some(1),
            before: None,
            after: Some(row_with(lsn as i32)),
            unchanged_cols: vec![],
        }
    }

    #[test]
    fn empty_writer_flushes_to_none() {
        let mut w = RollingWriter::new(10);
        assert!(w.is_empty());
        assert_eq!(w.flush().unwrap(), None);
    }

    #[test]
    fn should_flush_trips_at_threshold() {
        let mut w = RollingWriter::new(3);
        assert!(!w.should_flush());
        w.append(evt(1));
        w.append(evt(2));
        assert!(!w.should_flush());
        w.append(evt(3));
        assert!(w.should_flush());
    }

    #[test]
    fn flush_clears_buffer_and_returns_chunk_with_events() {
        let mut w = RollingWriter::new(100);
        for i in 1..=5 {
            w.append(evt(i));
        }
        let chunk = w.flush().unwrap().expect("non-empty chunk");
        assert_eq!(chunk.record_count, 5);
        assert_eq!(chunk.max_lsn, Lsn(5));
        assert!(w.is_empty());

        let decoded = decode_chunk(&chunk.bytes).unwrap();
        assert_eq!(decoded.len(), 5);
        for (i, m) in decoded.iter().enumerate() {
            assert_eq!(m.lsn, Lsn((i + 1) as u64));
        }
    }

    #[test]
    fn second_flush_after_drain_is_none() {
        let mut w = RollingWriter::new(10);
        w.append(evt(1));
        let _ = w.flush().unwrap().unwrap();
        assert_eq!(w.flush().unwrap(), None);
    }

    #[test]
    #[should_panic(expected = "max_rows must be > 0")]
    fn zero_threshold_panics() {
        let _ = RollingWriter::new(0);
    }
}
