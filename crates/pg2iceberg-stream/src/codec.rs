//! Staged-event Parquet codec. Sync, pure-compute.
//!
//! Mirrors `iceberg/events_schema.go:30-42` (the Go source of truth):
//!
//! | Field ID | Name | Type | Null |
//! |---|---|---|---|
//! | 1 | `_op` | string | not null |
//! | 2 | `_lsn` | int64 | not null |
//! | 3 | `_ts` | timestamp(μs, UTC) | not null |
//! | 4 | `_unchanged_cols` | string | nullable |
//! | 5 | `_data` | string | not null |
//! | 6 | `_xid` | int64 | nullable |
//!
//! Field IDs are persisted in Parquet column metadata under `PARQUET:field_id`
//! so Iceberg readers (Go and Rust) resolve columns by ID, not name.
//!
//! The JSON inside `_data` is currently the serde-tagged form of `Row`. That
//! is *not yet* compatible with Go's natural-JSON encoding — switching is a
//! Phase 2.5 task tracked in the port plan.

use crate::{MatEvent, Result, StreamError};
use arrow_array::builder::{Int64Builder, StringBuilder, TimestampMicrosecondBuilder};
use arrow_array::{Array, Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use bytes::Bytes;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use pg2iceberg_core::{ChangeEvent, ColumnName, Lsn, Op, Row, Timestamp};
use std::collections::HashMap;
use std::sync::Arc;

const FIELD_ID_KEY: &str = "PARQUET:field_id";
const TIMESTAMP_TZ: &str = "UTC";

const COL_OP: &str = "_op";
const COL_LSN: &str = "_lsn";
const COL_TS: &str = "_ts";
const COL_UNCHANGED_COLS: &str = "_unchanged_cols";
const COL_DATA: &str = "_data";
const COL_XID: &str = "_xid";

/// Parquet schema for staged WAL files.
///
/// Returned as an Arc so the writer/reader can share without cloning.
pub fn staged_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        field_with_id(COL_OP, DataType::Utf8, false, 1),
        field_with_id(COL_LSN, DataType::Int64, false, 2),
        field_with_id(
            COL_TS,
            DataType::Timestamp(TimeUnit::Microsecond, Some(TIMESTAMP_TZ.into())),
            false,
            3,
        ),
        field_with_id(COL_UNCHANGED_COLS, DataType::Utf8, true, 4),
        field_with_id(COL_DATA, DataType::Utf8, false, 5),
        field_with_id(COL_XID, DataType::Int64, true, 6),
    ]))
}

fn field_with_id(name: &str, ty: DataType, nullable: bool, id: i32) -> Field {
    let mut md = HashMap::new();
    md.insert(FIELD_ID_KEY.to_string(), id.to_string());
    Field::new(name, ty, nullable).with_metadata(md)
}

/// Op encoded in the `_op` column. Matches Go's `OpString`.
fn op_to_str(op: Op) -> Option<&'static str> {
    match op {
        Op::Insert => Some("I"),
        Op::Update => Some("U"),
        Op::Delete => Some("D"),
        // Begin/Commit/Relation/Truncate aren't staged — they're control
        // events consumed by the pipeline before reaching the writer.
        Op::Relation | Op::Truncate => None,
    }
}

fn op_from_str(s: &str) -> Option<Op> {
    match s {
        "I" => Some(Op::Insert),
        "U" => Some(Op::Update),
        "D" => Some(Op::Delete),
        _ => None,
    }
}

/// Pick the row that gets staged for a given event: `after` for insert/update,
/// `before` for delete. Returns `None` for non-DML ops, which the caller
/// should filter before calling.
fn staged_row(evt: &ChangeEvent) -> Option<&Row> {
    match evt.op {
        Op::Insert | Op::Update => evt.after.as_ref(),
        Op::Delete => evt.before.as_ref(),
        Op::Relation | Op::Truncate => None,
    }
}

fn unchanged_cols_to_string(cols: &[ColumnName]) -> Option<String> {
    if cols.is_empty() {
        None
    } else {
        Some(
            cols.iter()
                .map(|c| c.0.as_str())
                .collect::<Vec<_>>()
                .join(","),
        )
    }
}

fn unchanged_cols_from_string(s: &str) -> Vec<ColumnName> {
    if s.is_empty() {
        Vec::new()
    } else {
        s.split(',').map(|c| ColumnName(c.to_string())).collect()
    }
}

/// Build a `RecordBatch` from a slice of `ChangeEvent`s. Skips non-DML events.
pub fn encode_batch(events: &[ChangeEvent]) -> Result<RecordBatch> {
    let schema = staged_schema();
    let cap = events.len();

    let mut op_b = StringBuilder::with_capacity(cap, cap * 2);
    let mut lsn_b = Int64Builder::with_capacity(cap);
    let mut ts_b = TimestampMicrosecondBuilder::with_capacity(cap).with_timezone(TIMESTAMP_TZ);
    let mut unchanged_b = StringBuilder::with_capacity(cap, cap * 16);
    let mut data_b = StringBuilder::with_capacity(cap, cap * 64);
    let mut xid_b = Int64Builder::with_capacity(cap);

    for evt in events {
        let Some(op_str) = op_to_str(evt.op) else {
            continue;
        };
        let Some(row) = staged_row(evt) else {
            return Err(StreamError::Encode(format!(
                "DML event {:?} has no row payload",
                evt.op
            )));
        };

        op_b.append_value(op_str);
        lsn_b.append_value(evt.lsn.0 as i64);
        ts_b.append_value(evt.commit_ts.0);
        match unchanged_cols_to_string(&evt.unchanged_cols) {
            Some(s) => unchanged_b.append_value(s),
            None => unchanged_b.append_null(),
        }
        let data_json = serde_json::to_string(row)
            .map_err(|e| StreamError::Encode(format!("encode _data: {e}")))?;
        data_b.append_value(data_json);
        match evt.xid {
            Some(xid) => xid_b.append_value(xid as i64),
            None => xid_b.append_null(),
        }
    }

    let cols: Vec<Arc<dyn Array>> = vec![
        Arc::new(op_b.finish()),
        Arc::new(lsn_b.finish()),
        Arc::new(ts_b.finish()),
        Arc::new(unchanged_b.finish()),
        Arc::new(data_b.finish()),
        Arc::new(xid_b.finish()),
    ];

    RecordBatch::try_new(schema, cols)
        .map_err(|e| StreamError::Encode(format!("build record batch: {e}")))
}

/// Encode events into a single Parquet file (one row group). Returns the
/// serialized bytes plus the highest LSN seen, which the caller hands to the
/// coordinator as `OffsetClaim::max_lsn`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EncodedChunk {
    pub bytes: Bytes,
    pub record_count: u64,
    pub max_lsn: Lsn,
}

pub fn encode_chunk(events: &[ChangeEvent]) -> Result<EncodedChunk> {
    let batch = encode_batch(events)?;
    let max_lsn = events
        .iter()
        .filter(|e| op_to_str(e.op).is_some())
        .map(|e| e.lsn)
        .max()
        .unwrap_or(Lsn::ZERO);
    let record_count = batch.num_rows() as u64;

    let mut buf = Vec::<u8>::new();
    let props = WriterProperties::builder().build();
    {
        let mut writer = ArrowWriter::try_new(&mut buf, batch.schema(), Some(props))
            .map_err(|e| StreamError::Encode(format!("parquet writer: {e}")))?;
        writer
            .write(&batch)
            .map_err(|e| StreamError::Encode(format!("parquet write: {e}")))?;
        writer
            .close()
            .map_err(|e| StreamError::Encode(format!("parquet close: {e}")))?;
    }

    Ok(EncodedChunk {
        bytes: Bytes::from(buf),
        record_count,
        max_lsn,
    })
}

/// Inverse of [`encode_chunk`]. Allocates a `Vec<MatEvent>` per call; suitable
/// for tests and the materializer's per-chunk read path.
pub fn decode_chunk(bytes: &[u8]) -> Result<Vec<MatEvent>> {
    let owned = Bytes::copy_from_slice(bytes);
    let builder = ParquetRecordBatchReaderBuilder::try_new(owned)
        .map_err(|e| StreamError::Decode(format!("parquet reader: {e}")))?;
    let reader = builder
        .build()
        .map_err(|e| StreamError::Decode(format!("parquet reader build: {e}")))?;

    let mut out = Vec::new();
    for batch in reader {
        let batch = batch.map_err(|e| StreamError::Decode(format!("read batch: {e}")))?;
        decode_batch_into(&batch, &mut out)?;
    }
    Ok(out)
}

fn decode_batch_into(batch: &RecordBatch, out: &mut Vec<MatEvent>) -> Result<()> {
    let n = batch.num_rows();

    let op_col = batch
        .column_by_name(COL_OP)
        .and_then(|a| a.as_any().downcast_ref::<StringArray>())
        .ok_or_else(|| StreamError::Decode(format!("missing or wrong type: {COL_OP}")))?;
    let lsn_col = batch
        .column_by_name(COL_LSN)
        .and_then(|a| a.as_any().downcast_ref::<Int64Array>())
        .ok_or_else(|| StreamError::Decode(format!("missing or wrong type: {COL_LSN}")))?;
    let ts_col = batch
        .column_by_name(COL_TS)
        .and_then(|a| a.as_any().downcast_ref::<TimestampMicrosecondArray>())
        .ok_or_else(|| StreamError::Decode(format!("missing or wrong type: {COL_TS}")))?;
    let unchanged_col = batch
        .column_by_name(COL_UNCHANGED_COLS)
        .and_then(|a| a.as_any().downcast_ref::<StringArray>())
        .ok_or_else(|| {
            StreamError::Decode(format!("missing or wrong type: {COL_UNCHANGED_COLS}"))
        })?;
    let data_col = batch
        .column_by_name(COL_DATA)
        .and_then(|a| a.as_any().downcast_ref::<StringArray>())
        .ok_or_else(|| StreamError::Decode(format!("missing or wrong type: {COL_DATA}")))?;
    let xid_col = batch
        .column_by_name(COL_XID)
        .and_then(|a| a.as_any().downcast_ref::<Int64Array>())
        .ok_or_else(|| StreamError::Decode(format!("missing or wrong type: {COL_XID}")))?;

    for i in 0..n {
        let op_str = op_col.value(i);
        let op = op_from_str(op_str)
            .ok_or_else(|| StreamError::Decode(format!("unknown op {op_str:?} at row {i}")))?;
        let lsn = Lsn(lsn_col.value(i) as u64);
        let commit_ts = Timestamp(ts_col.value(i));
        let unchanged = if unchanged_col.is_null(i) {
            Vec::new()
        } else {
            unchanged_cols_from_string(unchanged_col.value(i))
        };
        let row: Row = serde_json::from_str(data_col.value(i))
            .map_err(|e| StreamError::Decode(format!("decode _data at row {i}: {e}")))?;
        let xid = if xid_col.is_null(i) {
            None
        } else {
            Some(xid_col.value(i) as u32)
        };

        out.push(MatEvent {
            op,
            lsn,
            commit_ts,
            xid,
            unchanged_cols: unchanged,
            row,
        });
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use pg2iceberg_core::{Namespace, PgValue, TableIdent};
    use std::collections::BTreeMap;

    fn ident() -> TableIdent {
        TableIdent {
            namespace: Namespace(vec!["public".into()]),
            name: "orders".into(),
        }
    }

    fn row(pairs: &[(&str, PgValue)]) -> Row {
        let mut r = BTreeMap::new();
        for (k, v) in pairs {
            r.insert(ColumnName((*k).into()), v.clone());
        }
        r
    }

    #[test]
    fn schema_has_six_fields_with_field_ids_1_to_6() {
        let s = staged_schema();
        let names: Vec<&str> = s.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(
            names,
            vec!["_op", "_lsn", "_ts", "_unchanged_cols", "_data", "_xid"]
        );
        for (i, f) in s.fields().iter().enumerate() {
            let id = f
                .metadata()
                .get(FIELD_ID_KEY)
                .expect("field-id metadata present");
            assert_eq!(id, &(i as i32 + 1).to_string(), "field {}", f.name());
        }
        assert!(!s.field(0).is_nullable());
        assert!(!s.field(1).is_nullable());
        assert!(!s.field(2).is_nullable());
        assert!(s.field(3).is_nullable());
        assert!(!s.field(4).is_nullable());
        assert!(s.field(5).is_nullable());
    }

    #[test]
    fn round_trip_minimal_insert() {
        let evt = ChangeEvent {
            table: ident(),
            op: Op::Insert,
            lsn: Lsn(42),
            commit_ts: Timestamp(1_700_000_000_000_000),
            xid: Some(7),
            before: None,
            after: Some(row(&[
                ("id", PgValue::Int4(1)),
                ("name", PgValue::Text("alice".into())),
            ])),
            unchanged_cols: vec![],
        };
        let chunk = encode_chunk(std::slice::from_ref(&evt)).unwrap();
        assert_eq!(chunk.record_count, 1);
        assert_eq!(chunk.max_lsn, Lsn(42));
        let decoded = decode_chunk(&chunk.bytes).unwrap();
        assert_eq!(decoded.len(), 1);
        let m = &decoded[0];
        assert_eq!(m.op, Op::Insert);
        assert_eq!(m.lsn, Lsn(42));
        assert_eq!(m.commit_ts, Timestamp(1_700_000_000_000_000));
        assert_eq!(m.xid, Some(7));
        assert!(m.unchanged_cols.is_empty());
        assert_eq!(m.row, evt.after.unwrap());
    }

    #[test]
    fn delete_carries_before_row() {
        let before = row(&[("id", PgValue::Int4(2))]);
        let evt = ChangeEvent {
            table: ident(),
            op: Op::Delete,
            lsn: Lsn(10),
            commit_ts: Timestamp(0),
            xid: None,
            before: Some(before.clone()),
            after: None,
            unchanged_cols: vec![],
        };
        let chunk = encode_chunk(&[evt]).unwrap();
        let decoded = decode_chunk(&chunk.bytes).unwrap();
        assert_eq!(decoded[0].op, Op::Delete);
        assert_eq!(decoded[0].row, before);
        assert!(decoded[0].xid.is_none());
    }

    #[test]
    fn unchanged_cols_round_trip() {
        let evt = ChangeEvent {
            table: ident(),
            op: Op::Update,
            lsn: Lsn(99),
            commit_ts: Timestamp(0),
            xid: Some(1),
            before: None,
            after: Some(row(&[("id", PgValue::Int4(3))])),
            unchanged_cols: vec![ColumnName("blob".into()), ColumnName("doc".into())],
        };
        let chunk = encode_chunk(&[evt]).unwrap();
        let decoded = decode_chunk(&chunk.bytes).unwrap();
        assert_eq!(
            decoded[0].unchanged_cols,
            vec![ColumnName("blob".into()), ColumnName("doc".into())]
        );
    }

    #[test]
    fn non_dml_events_are_skipped() {
        let evts = vec![
            ChangeEvent {
                table: ident(),
                op: Op::Relation,
                lsn: Lsn(1),
                commit_ts: Timestamp(0),
                xid: None,
                before: None,
                after: None,
                unchanged_cols: vec![],
            },
            ChangeEvent {
                table: ident(),
                op: Op::Insert,
                lsn: Lsn(2),
                commit_ts: Timestamp(0),
                xid: None,
                before: None,
                after: Some(row(&[("id", PgValue::Int4(1))])),
                unchanged_cols: vec![],
            },
        ];
        let chunk = encode_chunk(&evts).unwrap();
        assert_eq!(chunk.record_count, 1);
        assert_eq!(chunk.max_lsn, Lsn(2));
    }

    #[test]
    fn empty_input_produces_empty_chunk() {
        let chunk = encode_chunk(&[]).unwrap();
        assert_eq!(chunk.record_count, 0);
        assert_eq!(chunk.max_lsn, Lsn::ZERO);
        let decoded = decode_chunk(&chunk.bytes).unwrap();
        assert!(decoded.is_empty());
    }
}
