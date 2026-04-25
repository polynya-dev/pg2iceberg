//! Value vocabulary for the type-mapping layer.
//!
//! These types capture the *shape* of a value at the PG and Iceberg sides.
//! They're used for property-test round-trips before we wire any actual
//! Parquet/Arrow encoders. Phase 2 will plug encoder/decoder behavior on top.

use serde::{Deserialize, Serialize};

/// Microsecond-precision timestamp, matching Postgres internal storage.
#[derive(Copy, Clone, Eq, Ord, PartialEq, PartialOrd, Debug, Serialize, Deserialize)]
pub struct TimestampMicros(pub i64);

/// Microsecond-of-day, 0..86_400_000_000.
#[derive(Copy, Clone, Eq, Ord, PartialEq, PartialOrd, Debug, Serialize, Deserialize)]
pub struct TimeMicros(pub i64);

/// Days since the Unix epoch (1970-01-01).
#[derive(Copy, Clone, Eq, Ord, PartialEq, PartialOrd, Debug, Serialize, Deserialize)]
pub struct DaysSinceEpoch(pub i32);

/// Decimal value as an unscaled big-endian two's-complement bytes plus the
/// scale. Big-decimal libraries can be plugged in later — this is only a
/// transport shape for round-trip tests.
#[derive(Clone, Eq, PartialEq, Debug, Serialize, Deserialize)]
pub struct Decimal {
    pub unscaled_be_bytes: Vec<u8>,
    pub scale: u8,
}

#[derive(Clone, PartialEq, Debug, Serialize, Deserialize)]
pub enum PgValue {
    Null,
    Bool(bool),
    Int2(i16),
    Int4(i32),
    Int8(i64),
    Float4(f32),
    Float8(f64),
    Numeric(Decimal),
    Text(String),
    Bytea(Vec<u8>),
    Date(DaysSinceEpoch),
    Time(TimeMicros),
    /// `time with time zone` — Postgres stores micros + offset; we keep the
    /// offset to match Go behavior and document the truncation at write time.
    TimeTz {
        time: TimeMicros,
        zone_secs: i32,
    },
    Timestamp(TimestampMicros),
    TimestampTz(TimestampMicros),
    Uuid([u8; 16]),
    Json(String),
    Jsonb(String),
}

#[derive(Clone, PartialEq, Debug, Serialize, Deserialize)]
pub enum IcebergValue {
    Null,
    Boolean(bool),
    Int(i32),
    Long(i64),
    Float(f32),
    Double(f64),
    Decimal(Decimal),
    String(String),
    Binary(Vec<u8>),
    Date(DaysSinceEpoch),
    Time(TimeMicros),
    Timestamp(TimestampMicros),
    TimestampTz(TimestampMicros),
    Uuid([u8; 16]),
}
