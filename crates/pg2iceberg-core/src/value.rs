//! Value vocabulary for the type-mapping layer.
//!
//! These types capture the *shape* of a value at the PG and Iceberg sides.
//! Used for property-test round-trips and consumed by the
//! Parquet/Arrow encoders.

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
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Decimal {
    pub unscaled_be_bytes: Vec<u8>,
    pub scale: u8,
}

impl Decimal {
    /// Minimal two's-complement big-endian form: strip redundant leading
    /// sign-extension bytes (`0x00` ahead of a non-negative byte, `0xFF`
    /// ahead of a negative byte). Postgres emits zero-padded unscaled
    /// bytes while Parquet emits the minimal form, so the same value
    /// arrives with different padding on the two sides.
    fn normalized_be_bytes(&self) -> &[u8] {
        let b = self.unscaled_be_bytes.as_slice();
        let mut i = 0;
        while i + 1 < b.len() {
            let next_negative = b[i + 1] & 0x80 != 0;
            match b[i] {
                0x00 if !next_negative => i += 1,
                0xFF if next_negative => i += 1,
                _ => break,
            }
        }
        &b[i..]
    }
}

/// Equality is by numeric value at the same scale, not by raw byte
/// representation: `{[0,0,1,236], scale:2}` and `{[1,236], scale:2}` are
/// the same number (4.92). Comparing the raw `Vec<u8>` (as a derived
/// `PartialEq` would) makes `verify` report spurious diffs on every
/// decimal column, since PG and Parquet pad the unscaled bytes
/// differently.
impl PartialEq for Decimal {
    fn eq(&self, other: &Self) -> bool {
        self.scale == other.scale && self.normalized_be_bytes() == other.normalized_be_bytes()
    }
}
impl Eq for Decimal {}

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

#[cfg(test)]
mod tests {
    use super::Decimal;

    fn d(bytes: &[u8], scale: u8) -> Decimal {
        Decimal {
            unscaled_be_bytes: bytes.to_vec(),
            scale,
        }
    }

    #[test]
    fn decimal_eq_ignores_leading_zero_padding() {
        // 4.92: PG zero-pads the unscaled bytes, Parquet emits minimal.
        assert_eq!(d(&[0, 0, 0, 0, 1, 236], 2), d(&[1, 236], 2));
        assert_eq!(d(&[0, 1], 0), d(&[1], 0));
    }

    #[test]
    fn decimal_eq_ignores_sign_extension_for_negatives() {
        // -20 (0xEC) sign-extended vs minimal.
        assert_eq!(d(&[0xFF, 0xFF, 0xEC], 0), d(&[0xEC], 0));
        // A leading 0x00 is NOT redundant when the next byte is negative:
        // 0x00EC (236) must stay distinct from 0xEC (-20).
        assert_ne!(d(&[0x00, 0xEC], 0), d(&[0xEC], 0));
    }

    #[test]
    fn decimal_distinguishes_value_and_scale() {
        assert_ne!(d(&[1, 236], 2), d(&[1, 237], 2)); // 4.92 vs 4.93
        assert_ne!(d(&[1, 236], 2), d(&[1, 236], 3)); // 4.92 vs 0.492
    }
}
