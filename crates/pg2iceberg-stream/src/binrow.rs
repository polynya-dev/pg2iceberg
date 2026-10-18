//! The staged format's encoding of a row: each value by its column's index
//! into the chunk's column names, which the chunk holds once — rather than
//! a JSON object per row, naming every column in every row.
//!
//! A row is a count, then that many `(column index, value)` pairs; counts,
//! indices and lengths are LEB128 varints. A value is a tag byte and its
//! payload, fixed-width numbers little-endian.

use crate::{Result, StreamError};
use pg2iceberg_core::value::{DaysSinceEpoch, Decimal, TimeMicros, TimestampMicros};
use pg2iceberg_core::{ColumnName, PgValue, Row};
use std::collections::HashMap;

const NULL: u8 = 0;
const BOOL: u8 = 1;
const INT2: u8 = 2;
const INT4: u8 = 3;
const INT8: u8 = 4;
const FLOAT4: u8 = 5;
const FLOAT8: u8 = 6;
const NUMERIC: u8 = 7;
const TEXT: u8 = 8;
const BYTEA: u8 = 9;
const DATE: u8 = 10;
const TIME: u8 = 11;
const TIME_TZ: u8 = 12;
const TIMESTAMP: u8 = 13;
const TIMESTAMP_TZ: u8 = 14;
const UUID: u8 = 15;
const JSON: u8 = 16;
const JSONB: u8 = 17;

/// A chunk's column names, numbered in the order rows first use them.
#[derive(Default)]
pub(crate) struct ColumnNames<'a> {
    index: HashMap<&'a str, u64>,
    names: Vec<&'a str>,
}

impl<'a> ColumnNames<'a> {
    fn index(&mut self, name: &'a str) -> u64 {
        match self.index.get(name) {
            Some(&i) => i,
            None => {
                let i = self.names.len() as u64;
                self.index.insert(name, i);
                self.names.push(name);
                i
            }
        }
    }

    pub(crate) fn names(&self) -> &[&'a str] {
        &self.names
    }
}

/// Append `row` to `out`, numbering its columns in `columns`.
pub(crate) fn encode_row<'a>(row: &'a Row, columns: &mut ColumnNames<'a>, out: &mut Vec<u8>) {
    put_varint(out, row.len() as u64);
    for (name, value) in row {
        put_varint(out, columns.index(&name.0));
        encode_value(value, out);
    }
}

/// The row `bytes` encode, its columns numbered as in `names`.
pub(crate) fn decode_row(bytes: &[u8], names: &[ColumnName]) -> Result<Row> {
    let mut r = Reader(bytes);
    let mut row = Row::new();
    for _ in 0..r.varint()? {
        let i = r.varint()?;
        let name = usize::try_from(i)
            .ok()
            .and_then(|i| names.get(i))
            .ok_or_else(|| StreamError::Decode(format!("column {i} of {}", names.len())))?;
        row.insert(name.clone(), r.value()?);
    }
    if !r.0.is_empty() {
        return Err(StreamError::Decode(format!(
            "{} bytes past the row's end",
            r.0.len()
        )));
    }
    Ok(row)
}

fn encode_value(value: &PgValue, out: &mut Vec<u8>) {
    match value {
        PgValue::Null => out.push(NULL),
        PgValue::Bool(b) => out.extend([BOOL, *b as u8]),
        PgValue::Int2(n) => {
            out.push(INT2);
            out.extend(n.to_le_bytes());
        }
        PgValue::Int4(n) => {
            out.push(INT4);
            out.extend(n.to_le_bytes());
        }
        PgValue::Int8(n) => {
            out.push(INT8);
            out.extend(n.to_le_bytes());
        }
        PgValue::Float4(x) => {
            out.push(FLOAT4);
            out.extend(x.to_le_bytes());
        }
        PgValue::Float8(x) => {
            out.push(FLOAT8);
            out.extend(x.to_le_bytes());
        }
        PgValue::Numeric(d) => {
            out.extend([NUMERIC, d.scale]);
            put_bytes(out, &d.unscaled_be_bytes);
        }
        PgValue::Text(s) => {
            out.push(TEXT);
            put_bytes(out, s.as_bytes());
        }
        PgValue::Bytea(b) => {
            out.push(BYTEA);
            put_bytes(out, b);
        }
        PgValue::Date(d) => {
            out.push(DATE);
            out.extend(d.0.to_le_bytes());
        }
        PgValue::Time(t) => {
            out.push(TIME);
            out.extend(t.0.to_le_bytes());
        }
        PgValue::TimeTz { time, zone_secs } => {
            out.push(TIME_TZ);
            out.extend(time.0.to_le_bytes());
            out.extend(zone_secs.to_le_bytes());
        }
        PgValue::Timestamp(t) => {
            out.push(TIMESTAMP);
            out.extend(t.0.to_le_bytes());
        }
        PgValue::TimestampTz(t) => {
            out.push(TIMESTAMP_TZ);
            out.extend(t.0.to_le_bytes());
        }
        PgValue::Uuid(u) => {
            out.push(UUID);
            out.extend(u);
        }
        PgValue::Json(s) => {
            out.push(JSON);
            put_bytes(out, s.as_bytes());
        }
        PgValue::Jsonb(s) => {
            out.push(JSONB);
            put_bytes(out, s.as_bytes());
        }
    }
}

fn put_varint(out: &mut Vec<u8>, mut n: u64) {
    while n >= 0x80 {
        out.push(n as u8 | 0x80);
        n >>= 7;
    }
    out.push(n as u8);
}

fn put_bytes(out: &mut Vec<u8>, bytes: &[u8]) {
    put_varint(out, bytes.len() as u64);
    out.extend_from_slice(bytes);
}

/// What's left of an encoded row.
struct Reader<'a>(&'a [u8]);

impl<'a> Reader<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8]> {
        if self.0.len() < n {
            return Err(StreamError::Decode(format!(
                "row ends {} bytes short",
                n - self.0.len()
            )));
        }
        let (taken, rest) = self.0.split_at(n);
        self.0 = rest;
        Ok(taken)
    }

    fn array<const N: usize>(&mut self) -> Result<[u8; N]> {
        Ok(self.take(N)?.try_into().expect("took N bytes"))
    }

    fn varint(&mut self) -> Result<u64> {
        let mut n: u64 = 0;
        for shift in (0..64).step_by(7) {
            let [b] = self.array()?;
            n |= u64::from(b & 0x7f) << shift;
            if b & 0x80 == 0 {
                return Ok(n);
            }
        }
        Err(StreamError::Decode("varint too long".into()))
    }

    fn bytes(&mut self) -> Result<&'a [u8]> {
        let len = self.varint()?;
        let len = usize::try_from(len)
            .map_err(|_| StreamError::Decode(format!("length {len} too large")))?;
        self.take(len)
    }

    fn string(&mut self) -> Result<String> {
        String::from_utf8(self.bytes()?.to_vec())
            .map_err(|e| StreamError::Decode(format!("text: {e}")))
    }

    fn value(&mut self) -> Result<PgValue> {
        let [tag] = self.array()?;
        Ok(match tag {
            NULL => PgValue::Null,
            BOOL => PgValue::Bool(self.array::<1>()?[0] != 0),
            INT2 => PgValue::Int2(i16::from_le_bytes(self.array()?)),
            INT4 => PgValue::Int4(i32::from_le_bytes(self.array()?)),
            INT8 => PgValue::Int8(i64::from_le_bytes(self.array()?)),
            FLOAT4 => PgValue::Float4(f32::from_le_bytes(self.array()?)),
            FLOAT8 => PgValue::Float8(f64::from_le_bytes(self.array()?)),
            NUMERIC => {
                let [scale] = self.array()?;
                PgValue::Numeric(Decimal {
                    unscaled_be_bytes: self.bytes()?.to_vec(),
                    scale,
                })
            }
            TEXT => PgValue::Text(self.string()?),
            BYTEA => PgValue::Bytea(self.bytes()?.to_vec()),
            DATE => PgValue::Date(DaysSinceEpoch(i32::from_le_bytes(self.array()?))),
            TIME => PgValue::Time(TimeMicros(i64::from_le_bytes(self.array()?))),
            TIME_TZ => PgValue::TimeTz {
                time: TimeMicros(i64::from_le_bytes(self.array()?)),
                zone_secs: i32::from_le_bytes(self.array()?),
            },
            TIMESTAMP => PgValue::Timestamp(TimestampMicros(i64::from_le_bytes(self.array()?))),
            TIMESTAMP_TZ => {
                PgValue::TimestampTz(TimestampMicros(i64::from_le_bytes(self.array()?)))
            }
            UUID => PgValue::Uuid(self.array()?),
            JSON => PgValue::Json(self.string()?),
            JSONB => PgValue::Jsonb(self.string()?),
            other => return Err(StreamError::Decode(format!("value tag {other}"))),
        })
    }
}
