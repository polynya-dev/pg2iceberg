//! pgoutput (protocol v1) encoding, so a simulated walsender can send
//! production's decoder the bytes a real one would: relation messages
//! with type OIDs, tuples of text-format values, unchanged-TOAST
//! markers, key-only or full old tuples by replica identity.
//!
//! Message layouts follow PostgreSQL's "Logical Replication Message
//! Formats"; value text follows PostgreSQL's output functions (ISO
//! DateStyle, hex `bytea_output`).

use bytes::{BufMut, Bytes, BytesMut};
use pg2iceberg_core::typemap::{IcebergType, PgType};
use pg2iceberg_core::value::Decimal;
use pg2iceberg_core::{Lsn, PgValue, Timestamp};

/// Microseconds between the Unix epoch and PostgreSQL's (2000-01-01).
const PG_EPOCH_OFFSET_MICROS: i64 = 946_684_800_000_000;

/// A table's `REPLICA IDENTITY`: what pgoutput sends as an UPDATE's or
/// DELETE's old row.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ReplicaIdentity {
    /// Key columns only (non-key columns NULL); sent for a DELETE, and
    /// for an UPDATE only when it changes the key.
    #[default]
    Default,
    /// The whole old row, for every UPDATE and DELETE.
    Full,
}

impl ReplicaIdentity {
    fn byte(self) -> u8 {
        match self {
            Self::Default => b'd',
            Self::Full => b'f',
        }
    }
}

/// One column of a tuple as pgoutput sends it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TupleValue {
    Null,
    /// An unchanged TOASTed value: sent as a marker, without the value.
    Unchanged,
    Text(String),
}

/// One column of a relation message.
#[derive(Clone, Debug)]
pub struct RelationCol {
    pub name: String,
    /// Part of the replica identity (the key).
    pub key: bool,
    pub pg_type: PgType,
}

/// The PostgreSQL type a column of `ty` most naturally has.
pub fn default_pg_type(ty: IcebergType) -> PgType {
    match ty {
        IcebergType::Boolean => PgType::Bool,
        IcebergType::Int => PgType::Int4,
        IcebergType::Long => PgType::Int8,
        IcebergType::Float => PgType::Float4,
        IcebergType::Double => PgType::Float8,
        IcebergType::Decimal { precision, scale } => PgType::Numeric {
            precision: Some(precision),
            scale: Some(scale),
        },
        IcebergType::String => PgType::Text,
        IcebergType::Binary => PgType::Bytea,
        IcebergType::Date => PgType::Date,
        IcebergType::Time => PgType::Time,
        IcebergType::Timestamp => PgType::Timestamp,
        IcebergType::TimestampTz => PgType::TimestampTz,
        IcebergType::Uuid => PgType::Uuid,
    }
}

/// `(type oid, type modifier)` as `pg_attribute` reports them.
pub fn pg_type_oid(t: PgType) -> (u32, i32) {
    match t {
        PgType::Bool => (16, -1),
        PgType::Int2 => (21, -1),
        PgType::Int4 => (23, -1),
        PgType::Int8 => (20, -1),
        PgType::Oid => (26, -1),
        PgType::Float4 => (700, -1),
        PgType::Float8 => (701, -1),
        PgType::Numeric {
            precision: Some(p),
            scale,
        } => (
            1700,
            ((i32::from(p) << 16) | i32::from(scale.unwrap_or(0))) + 4,
        ),
        PgType::Numeric { .. } => (1700, -1),
        PgType::Text => (25, -1),
        PgType::Bytea => (17, -1),
        PgType::Date => (1082, -1),
        PgType::Time => (1083, -1),
        PgType::TimeTz => (1266, -1),
        PgType::Timestamp => (1114, -1),
        PgType::TimestampTz => (1184, -1),
        PgType::Uuid => (2950, -1),
        PgType::Json => (114, -1),
        PgType::Jsonb => (3802, -1),
    }
}

/// `v` as PostgreSQL's output function renders it; `None` for NULL.
pub fn pg_text(v: &PgValue) -> Option<String> {
    Some(match v {
        PgValue::Null => return None,
        PgValue::Bool(b) => if *b { "t" } else { "f" }.to_string(),
        PgValue::Int2(n) => n.to_string(),
        PgValue::Int4(n) => n.to_string(),
        PgValue::Int8(n) => n.to_string(),
        PgValue::Float4(x) => float_text(f64::from(*x), x.to_string()),
        PgValue::Float8(x) => float_text(*x, x.to_string()),
        PgValue::Numeric(d) => numeric_text(d),
        PgValue::Text(s) | PgValue::Json(s) | PgValue::Jsonb(s) => s.clone(),
        PgValue::Bytea(b) => format!("\\x{}", hex(b)),
        PgValue::Date(d) => date_text(i64::from(d.0)),
        PgValue::Time(t) => time_text(t.0),
        PgValue::TimeTz { time, zone_secs } => {
            format!("{}{}", time_text(time.0), offset_text(*zone_secs))
        }
        PgValue::Timestamp(t) => timestamp_text(t.0),
        PgValue::TimestampTz(t) => format!("{}+00", timestamp_text(t.0)),
        PgValue::Uuid(u) => {
            let h = hex(u);
            format!(
                "{}-{}-{}-{}-{}",
                &h[0..8],
                &h[8..12],
                &h[12..16],
                &h[16..20],
                &h[20..32]
            )
        }
    })
}

fn float_text(x: f64, display: String) -> String {
    if x.is_nan() {
        "NaN".into()
    } else if x.is_infinite() {
        if x > 0.0 { "Infinity" } else { "-Infinity" }.into()
    } else {
        display
    }
}

fn numeric_text(d: &Decimal) -> String {
    let b = d.normalized_be_bytes();
    let negative = b.first().is_some_and(|x| x & 0x80 != 0);
    let mut buf = if negative { [0xFF; 16] } else { [0; 16] };
    buf[16 - b.len().min(16)..].copy_from_slice(&b[b.len().saturating_sub(16)..]);
    let unscaled = i128::from_be_bytes(buf);
    let digits = unscaled.unsigned_abs().to_string();
    let scale = usize::from(d.scale);
    let sign = if unscaled < 0 { "-" } else { "" };
    if scale == 0 {
        return format!("{sign}{digits}");
    }
    let padded = format!("{digits:0>width$}", width = scale + 1);
    let (int, frac) = padded.split_at(padded.len() - scale);
    format!("{sign}{int}.{frac}")
}

/// Days since 1970-01-01 → `(year, month, day)` (proleptic Gregorian).
fn civil(days: i64) -> (i64, u32, u32) {
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    (yoe + era * 400 + i64::from(m <= 2), m, d)
}

fn date_text(days: i64) -> String {
    let (y, m, d) = civil(days);
    if y <= 0 {
        format!("{:04}-{m:02}-{d:02} BC", 1 - y)
    } else {
        format!("{y:04}-{m:02}-{d:02}")
    }
}

fn time_text(micros_of_day: i64) -> String {
    let secs = micros_of_day.div_euclid(1_000_000);
    let frac = micros_of_day.rem_euclid(1_000_000);
    let base = format!("{:02}:{:02}:{:02}", secs / 3600, secs / 60 % 60, secs % 60);
    if frac == 0 {
        base
    } else {
        format!("{base}.{}", format!("{frac:06}").trim_end_matches('0'))
    }
}

fn timestamp_text(unix_micros: i64) -> String {
    let days = unix_micros.div_euclid(86_400_000_000);
    let micros_of_day = unix_micros.rem_euclid(86_400_000_000);
    format!("{} {}", date_text(days), time_text(micros_of_day))
}

fn offset_text(zone_secs: i32) -> String {
    let sign = if zone_secs < 0 { '-' } else { '+' };
    let s = zone_secs.unsigned_abs();
    match (s / 60 % 60, s % 60) {
        (0, 0) => format!("{sign}{:02}", s / 3600),
        (m, 0) => format!("{sign}{:02}:{m:02}", s / 3600),
        (m, sec) => format!("{sign}{:02}:{m:02}:{sec:02}", s / 3600),
    }
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn pg_micros(ts: Timestamp) -> i64 {
    ts.0 - PG_EPOCH_OFFSET_MICROS
}

fn put_str(buf: &mut BytesMut, s: &str) {
    buf.put_slice(s.as_bytes());
    buf.put_u8(0);
}

fn put_tuple(buf: &mut BytesMut, tuple: &[TupleValue]) {
    buf.put_i16(tuple.len() as i16);
    for v in tuple {
        match v {
            TupleValue::Null => buf.put_u8(b'n'),
            TupleValue::Unchanged => buf.put_u8(b'u'),
            TupleValue::Text(s) => {
                buf.put_u8(b't');
                buf.put_i32(s.len() as i32);
                buf.put_slice(s.as_bytes());
            }
        }
    }
}

/// `Begin`: `final_lsn` is the LSN of the transaction's commit record.
pub fn begin(final_lsn: Lsn, commit_ts: Timestamp, xid: u32) -> Bytes {
    let mut b = BytesMut::new();
    b.put_u8(b'B');
    b.put_u64(final_lsn.0);
    b.put_i64(pg_micros(commit_ts));
    b.put_u32(xid);
    b.freeze()
}

pub fn commit(commit_lsn: Lsn, end_lsn: Lsn, commit_ts: Timestamp) -> Bytes {
    let mut b = BytesMut::new();
    b.put_u8(b'C');
    b.put_u8(0);
    b.put_u64(commit_lsn.0);
    b.put_u64(end_lsn.0);
    b.put_i64(pg_micros(commit_ts));
    b.freeze()
}

pub fn relation(
    rel_id: u32,
    namespace: &str,
    name: &str,
    identity: ReplicaIdentity,
    cols: &[RelationCol],
) -> Bytes {
    let mut b = BytesMut::new();
    b.put_u8(b'R');
    b.put_u32(rel_id);
    put_str(&mut b, namespace);
    put_str(&mut b, name);
    b.put_u8(identity.byte());
    b.put_i16(cols.len() as i16);
    for c in cols {
        b.put_u8(u8::from(c.key));
        put_str(&mut b, &c.name);
        let (oid, typmod) = pg_type_oid(c.pg_type);
        b.put_u32(oid);
        b.put_i32(typmod);
    }
    b.freeze()
}

pub fn insert(rel_id: u32, new: &[TupleValue]) -> Bytes {
    let mut b = BytesMut::new();
    b.put_u8(b'I');
    b.put_u32(rel_id);
    b.put_u8(b'N');
    put_tuple(&mut b, new);
    b.freeze()
}

/// An old tuple: `K` for key columns only, `O` for the whole row.
pub enum OldTuple<'a> {
    Key(&'a [TupleValue]),
    Full(&'a [TupleValue]),
}

fn put_old(b: &mut BytesMut, old: OldTuple) {
    match old {
        OldTuple::Key(t) => {
            b.put_u8(b'K');
            put_tuple(b, t);
        }
        OldTuple::Full(t) => {
            b.put_u8(b'O');
            put_tuple(b, t);
        }
    }
}

pub fn update(rel_id: u32, old: Option<OldTuple>, new: &[TupleValue]) -> Bytes {
    let mut b = BytesMut::new();
    b.put_u8(b'U');
    b.put_u32(rel_id);
    if let Some(old) = old {
        put_old(&mut b, old);
    }
    b.put_u8(b'N');
    put_tuple(&mut b, new);
    b.freeze()
}

pub fn delete(rel_id: u32, old: OldTuple) -> Bytes {
    let mut b = BytesMut::new();
    b.put_u8(b'D');
    b.put_u32(rel_id);
    put_old(&mut b, old);
    b.freeze()
}

pub fn truncate(rel_ids: &[u32]) -> Bytes {
    let mut b = BytesMut::new();
    b.put_u8(b'T');
    b.put_u32(rel_ids.len() as u32);
    b.put_u8(0);
    for id in rel_ids {
        b.put_u32(*id);
    }
    b.freeze()
}

#[cfg(test)]
mod tests {
    use super::*;
    use pg2iceberg_core::value::{DaysSinceEpoch, TimeMicros, TimestampMicros};

    #[test]
    fn values_render_like_postgres() {
        let cases: Vec<(PgValue, &str)> = vec![
            (PgValue::Bool(true), "t"),
            (PgValue::Int2(-7), "-7"),
            (PgValue::Float8(f64::NAN), "NaN"),
            (PgValue::Float4(f32::NEG_INFINITY), "-Infinity"),
            (
                PgValue::Numeric(Decimal {
                    unscaled_be_bytes: (-15i128).to_be_bytes().to_vec(),
                    scale: 2,
                }),
                "-0.15",
            ),
            (
                PgValue::Numeric(Decimal {
                    unscaled_be_bytes: vec![0x30, 0x39],
                    scale: 0,
                }),
                "12345",
            ),
            (PgValue::Bytea(vec![0xde, 0xad]), "\\xdead"),
            (PgValue::Date(DaysSinceEpoch(0)), "1970-01-01"),
            (PgValue::Date(DaysSinceEpoch(19_723)), "2024-01-01"),
            (PgValue::Time(TimeMicros(3_723_500_000)), "01:02:03.5"),
            (
                PgValue::Timestamp(TimestampMicros(1_704_067_200_000_001)),
                "2024-01-01 00:00:00.000001",
            ),
            (
                PgValue::TimestampTz(TimestampMicros(0)),
                "1970-01-01 00:00:00+00",
            ),
            (
                PgValue::TimeTz {
                    time: TimeMicros(0),
                    zone_secs: -19_800,
                },
                "00:00:00-05:30",
            ),
            (
                PgValue::Uuid([0x12; 16]),
                "12121212-1212-1212-1212-121212121212",
            ),
        ];
        for (v, want) in cases {
            assert_eq!(pg_text(&v).as_deref(), Some(want), "{v:?}");
        }
        assert_eq!(pg_text(&PgValue::Null), None);
    }
}
