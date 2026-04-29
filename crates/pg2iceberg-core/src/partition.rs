//! Iceberg partition transforms + spec, expressed in our own type
//! system so the trait surface stays free of the heavy
//! `iceberg-rust` dep. Translation to `iceberg::spec::Transform` /
//! `UnboundPartitionSpec` lives in `pg2iceberg-iceberg/src/prod/`.
//!
//! Mirrors the Go reference's partition-expression grammar
//! (`config.example.yaml`):
//!
//! - `col_name` — identity
//! - `year(col)` — year-since-1970 int
//! - `month(col)` — months-since-1970-01 int
//! - `day(col)` — days-since-1970-01-01 int
//! - `hour(col)` — hours-since-1970-01-01-00 int
//! - `bucket[N](col)` — murmur3_x86_32(value) mod N
//! - `truncate[W](col)` — value rounded down to multiple of W (or first
//!   W code points for strings)

use crate::value::PgValue;
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Partition transform kind. Mirrors `iceberg::spec::Transform` minus
/// the `Void` / `Unknown` variants we don't expose.
#[derive(Copy, Clone, Eq, PartialEq, Debug, Serialize, Deserialize)]
pub enum Transform {
    Identity,
    Year,
    Month,
    Day,
    Hour,
    Bucket(u32),
    Truncate(u32),
}

impl Transform {
    /// Default partition-field name for a given source column. Matches
    /// Go's convention (`col`, `col_year`, `col_bucket`, `col_trunc`).
    /// Operators can override via the YAML field if we ever expose
    /// it; today the field name is always derived.
    pub fn partition_field_name(&self, source: &str) -> String {
        match self {
            Transform::Identity => source.to_string(),
            Transform::Year => format!("{source}_year"),
            Transform::Month => format!("{source}_month"),
            Transform::Day => format!("{source}_day"),
            Transform::Hour => format!("{source}_hour"),
            Transform::Bucket(_) => format!("{source}_bucket"),
            Transform::Truncate(_) => format!("{source}_trunc"),
        }
    }
}

/// One partition field — source column + transform. The Iceberg
/// `field_id` is allocated by the catalog when the table is created
/// (we don't pre-allocate).
#[derive(Clone, Eq, PartialEq, Debug, Serialize, Deserialize)]
pub struct PartitionField {
    pub source_column: String,
    pub name: String,
    pub transform: Transform,
}

#[derive(Clone, Eq, PartialEq, Debug, Error)]
pub enum PartitionError {
    #[error("invalid partition expression: {0}")]
    Invalid(String),
    #[error("unknown transform `{0}`; expected one of: identity, year, month, day, hour, bucket[N], truncate[W]")]
    UnknownTransform(String),
    #[error("transform `{transform}` requires a positive parameter; got `{got}`")]
    BadParameter { transform: String, got: String },
}

/// Parse a single partition expression like `day(created_at)` or
/// `bucket[16](id)` or just `region` (identity).
pub fn parse_partition_expr(expr: &str) -> Result<PartitionField, PartitionError> {
    let trimmed = expr.trim();
    if trimmed.is_empty() {
        return Err(PartitionError::Invalid("empty".into()));
    }

    // Identity: a bare column name (no parens, no brackets).
    if !trimmed.contains('(') {
        return Ok(PartitionField {
            source_column: trimmed.to_string(),
            name: Transform::Identity.partition_field_name(trimmed),
            transform: Transform::Identity,
        });
    }

    // Otherwise: `name [ '[' N ']' ] '(' col ')'`.
    let open = trimmed
        .find('(')
        .ok_or_else(|| PartitionError::Invalid(trimmed.to_string()))?;
    if !trimmed.ends_with(')') {
        return Err(PartitionError::Invalid(trimmed.to_string()));
    }
    let head = &trimmed[..open];
    let arg = &trimmed[open + 1..trimmed.len() - 1].trim();
    if arg.is_empty() {
        return Err(PartitionError::Invalid(trimmed.to_string()));
    }
    if arg.contains([' ', ',', '(']) {
        return Err(PartitionError::Invalid(format!(
            "{trimmed}: column name cannot contain space/comma/paren"
        )));
    }

    let (name_lower, param) = if let Some(lb) = head.find('[') {
        if !head.ends_with(']') {
            return Err(PartitionError::Invalid(trimmed.to_string()));
        }
        let n_str = &head[lb + 1..head.len() - 1];
        let n: u32 = n_str.parse().map_err(|_| PartitionError::BadParameter {
            transform: head[..lb].to_string(),
            got: n_str.to_string(),
        })?;
        if n == 0 {
            return Err(PartitionError::BadParameter {
                transform: head[..lb].to_string(),
                got: n_str.to_string(),
            });
        }
        (head[..lb].trim().to_ascii_lowercase(), Some(n))
    } else {
        (head.trim().to_ascii_lowercase(), None)
    };

    let transform = match (name_lower.as_str(), param) {
        ("identity", _) => Transform::Identity,
        ("year", _) => Transform::Year,
        ("month", _) => Transform::Month,
        ("day", _) => Transform::Day,
        ("hour", _) => Transform::Hour,
        ("bucket", Some(n)) => Transform::Bucket(n),
        ("truncate", Some(n)) => Transform::Truncate(n),
        ("bucket", None) | ("truncate", None) => {
            return Err(PartitionError::Invalid(format!(
                "{name_lower} requires `[N]`: e.g. bucket[16](col)"
            )))
        }
        _ => return Err(PartitionError::UnknownTransform(name_lower)),
    };

    Ok(PartitionField {
        source_column: arg.to_string(),
        name: transform.partition_field_name(arg),
        transform,
    })
}

/// Parse a list of partition expressions from YAML.
pub fn parse_partition_spec(exprs: &[String]) -> Result<Vec<PartitionField>, PartitionError> {
    exprs.iter().map(|s| parse_partition_expr(s)).collect()
}

// ── partition value computation ─────────────────────────────────────────

/// The result of applying a `Transform` to a `PgValue`. Always
/// integer-valued or string-valued — the iceberg spec defines exact
/// output types per transform. This wraps just what we need to
/// build an `iceberg::spec::Literal`.
#[derive(Clone, Eq, PartialEq, Debug)]
pub enum PartitionLiteral {
    /// `null` partition value (only ever emitted by identity on a
    /// nullable column). For year/month/day/hour the iceberg spec
    /// says null inputs produce null outputs too.
    Null,
    Int(i32),
    Long(i64),
    Float(f32_no_nan::F32),
    Double(f64_no_nan::F64),
    String(String),
    Boolean(bool),
    /// Fixed-width binary (e.g. UUID).
    Binary(Vec<u8>),
    /// Iceberg decimal partition value: unscaled i128 + scale. Iceberg
    /// caps decimal precision at 38 digits, which fits in i128.
    Decimal {
        unscaled: i128,
        scale: u8,
    },
}

/// Newtype wrappers that opt into Eq via NaN-rejection. Float and
/// Double partition values are unusual but technically allowed for
/// identity-partitioned columns; iceberg considers NaN ≠ NaN, so we
/// follow suit (NaN is treated as null).
pub mod f32_no_nan {
    use serde::{Deserialize, Serialize};
    #[derive(Copy, Clone, Debug, Serialize, Deserialize)]
    pub struct F32(pub f32);
    impl PartialEq for F32 {
        fn eq(&self, other: &Self) -> bool {
            self.0.to_bits() == other.0.to_bits()
        }
    }
    impl Eq for F32 {}
}
pub mod f64_no_nan {
    use serde::{Deserialize, Serialize};
    #[derive(Copy, Clone, Debug, Serialize, Deserialize)]
    pub struct F64(pub f64);
    impl PartialEq for F64 {
        fn eq(&self, other: &Self) -> bool {
            self.0.to_bits() == other.0.to_bits()
        }
    }
    impl Eq for F64 {}
}

/// Compute a partition literal for `value` under `transform`. Year /
/// Month / Day / Hour expect `PgValue::Date` or `Timestamp` /
/// `TimestampTz` inputs; identity passes the value through.
/// `Bucket(N)` hashes the value via murmur3_x86_32 per iceberg
/// [spec appendix B](https://iceberg.apache.org/spec/#appendix-b-32-bit-hash-requirements);
/// `Truncate(W)` floors integers/decimals to multiples of W and
/// truncates strings/binary to the first W code points/bytes.
pub fn apply_transform(
    value: &PgValue,
    transform: Transform,
) -> Result<PartitionLiteral, PartitionError> {
    if matches!(value, PgValue::Null) {
        return Ok(PartitionLiteral::Null);
    }
    match transform {
        Transform::Identity => identity_literal(value),
        Transform::Year => time_bucket(value, TimeUnit::Year),
        Transform::Month => time_bucket(value, TimeUnit::Month),
        Transform::Day => time_bucket(value, TimeUnit::Day),
        Transform::Hour => time_bucket(value, TimeUnit::Hour),
        Transform::Bucket(n) => bucket_value(value, n),
        Transform::Truncate(w) => truncate_value(value, w),
    }
}

#[derive(Copy, Clone)]
enum TimeUnit {
    Year,
    Month,
    Day,
    Hour,
}

const MICROS_PER_HOUR: i64 = 3_600_000_000;
const MICROS_PER_DAY: i64 = 86_400_000_000;

fn time_bucket(value: &PgValue, unit: TimeUnit) -> Result<PartitionLiteral, PartitionError> {
    let days_or_micros = match value {
        PgValue::Date(d) => DaysOrMicros::Days(d.0),
        PgValue::Timestamp(t) | PgValue::TimestampTz(t) => DaysOrMicros::Micros(t.0),
        _ => {
            return Err(PartitionError::Invalid(format!(
                "time-bucket transform requires date/timestamp, got {value:?}"
            )))
        }
    };
    let unit_value = match (unit, days_or_micros) {
        (TimeUnit::Year, DaysOrMicros::Days(d)) => year_from_days(d),
        (TimeUnit::Year, DaysOrMicros::Micros(m)) => year_from_days(days_from_micros(m)),
        (TimeUnit::Month, DaysOrMicros::Days(d)) => month_from_days(d),
        (TimeUnit::Month, DaysOrMicros::Micros(m)) => month_from_days(days_from_micros(m)),
        (TimeUnit::Day, DaysOrMicros::Days(d)) => d,
        (TimeUnit::Day, DaysOrMicros::Micros(m)) => days_from_micros(m),
        (TimeUnit::Hour, DaysOrMicros::Days(d)) => d * 24,
        (TimeUnit::Hour, DaysOrMicros::Micros(m)) => floor_div(m, MICROS_PER_HOUR) as i32,
    };
    Ok(PartitionLiteral::Int(unit_value))
}

#[derive(Copy, Clone)]
enum DaysOrMicros {
    Days(i32),
    Micros(i64),
}

fn floor_div(a: i64, b: i64) -> i64 {
    let q = a / b;
    if (a % b != 0) && ((a < 0) != (b < 0)) {
        q - 1
    } else {
        q
    }
}

fn days_from_micros(micros: i64) -> i32 {
    floor_div(micros, MICROS_PER_DAY) as i32
}

/// Year since 1970, computed by walking the calendar from epoch.
/// Iceberg's spec defines year-transform as "years from 1970", which
/// is a true calendar offset — not days/365 — to handle leap years.
fn year_from_days(days_since_epoch: i32) -> i32 {
    // Re-derive (year, month, day) from epoch days. Same algorithm
    // chrono uses; we inline it here so `core` stays chrono-free.
    let (year, _) = ymd_from_epoch_days(days_since_epoch);
    year - 1970
}

fn month_from_days(days_since_epoch: i32) -> i32 {
    let (year, month) = ymd_from_epoch_days(days_since_epoch);
    (year - 1970) * 12 + (month as i32 - 1)
}

/// Convert days-since-1970-01-01 to `(year, month_1_based)`. Adapted
/// from Howard Hinnant's `civil_from_days` algorithm.
fn ymd_from_epoch_days(days_since_epoch: i32) -> (i32, u32) {
    // Shift epoch to 0000-03-01 so leap years align with era boundaries.
    let days = days_since_epoch as i64 + 719_468;
    let era = days.div_euclid(146_097);
    let doe = days.rem_euclid(146_097) as u32; // [0, 146096]
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365; // [0, 399]
    let y = yoe as i32 + era as i32 * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // [0, 365]
    let mp = (5 * doy + 2) / 153; // [0, 11]
    let m = if mp < 10 { mp + 3 } else { mp - 9 }; // [1, 12]
    let y_final = if m <= 2 { y + 1 } else { y };
    (y_final, m)
}

// ── bucket[N] transform ─────────────────────────────────────────────────

fn bucket_value(value: &PgValue, n: u32) -> Result<PartitionLiteral, PartitionError> {
    if n == 0 {
        return Err(PartitionError::Invalid(
            "bucket modulus must be positive".into(),
        ));
    }
    let hash = bucket_hash(value)?;
    // Iceberg spec: `(hash & Integer.MAX_VALUE) % N`.
    let positive = hash & i32::MAX;
    let bucket = positive.rem_euclid(n as i32);
    Ok(PartitionLiteral::Int(bucket))
}

fn bucket_hash(value: &PgValue) -> Result<i32, PartitionError> {
    match value {
        // Per iceberg spec appendix B: int and date hash as i64-LE.
        PgValue::Int2(x) => Ok(hash_i64(*x as i64)),
        PgValue::Int4(x) => Ok(hash_i64(*x as i64)),
        PgValue::Int8(x) => Ok(hash_i64(*x)),
        PgValue::Date(d) => Ok(hash_i64(d.0 as i64)),
        PgValue::Time(t) | PgValue::TimeTz { time: t, .. } => Ok(hash_i64(t.0)),
        PgValue::Timestamp(t) | PgValue::TimestampTz(t) => Ok(hash_i64(t.0)),
        PgValue::Text(s) | PgValue::Json(s) | PgValue::Jsonb(s) => Ok(hash_bytes(s.as_bytes())),
        // UUID hashes as the canonical 16 big-endian bytes.
        PgValue::Uuid(b) => Ok(hash_bytes(b)),
        PgValue::Numeric(d) => Ok(hash_bytes(minimum_be_bytes(&d.unscaled_be_bytes))),
        // Per iceberg spec: bool / float / double / binary aren't valid
        // bucket inputs (booleans have only 2 values; floats hash NaN
        // ambiguously; binary uses identity). Surface a clear error.
        PgValue::Bool(_) | PgValue::Float4(_) | PgValue::Float8(_) | PgValue::Bytea(_) => {
            Err(PartitionError::Invalid(format!(
                "bucket transform does not accept value of type {value:?}; \
                 iceberg spec restricts bucket to int/long/string/uuid/decimal/date/time/timestamp"
            )))
        }
        PgValue::Null => Ok(0), // unreachable — apply_transform short-circuits Null
    }
}

#[inline]
fn hash_i64(v: i64) -> i32 {
    hash_bytes(&v.to_le_bytes())
}

fn hash_bytes(v: &[u8]) -> i32 {
    let mut cursor = std::io::Cursor::new(v);
    // murmur3_32 returns u32; iceberg's hash function is `(u32 as i32)`.
    murmur3::murmur3_32(&mut cursor, 0).expect("murmur3_32 cannot fail on Cursor<&[u8]>") as i32
}

/// Strip redundant sign-extension bytes from a big-endian two's-complement
/// representation. Iceberg's bucket hash for decimals operates on this
/// minimum-byte form, per [spec appendix B](https://iceberg.apache.org/spec/#appendix-b-32-bit-hash-requirements).
fn minimum_be_bytes(bytes: &[u8]) -> &[u8] {
    if bytes.len() <= 1 {
        return bytes;
    }
    let is_negative = bytes[0] & 0x80 != 0;
    let pad = if is_negative { 0xFF } else { 0x00 };
    let mut start = 0;
    while start < bytes.len() - 1 {
        if bytes[start] != pad {
            break;
        }
        // Stripping is safe only if the next byte preserves the original
        // sign (its MSB matches what we'd implicitly extend).
        let next_msb_set = bytes[start + 1] & 0x80 != 0;
        if is_negative != next_msb_set {
            break;
        }
        start += 1;
    }
    &bytes[start..]
}

// ── truncate[W] transform ───────────────────────────────────────────────

fn truncate_value(value: &PgValue, w: u32) -> Result<PartitionLiteral, PartitionError> {
    if w == 0 {
        return Err(PartitionError::Invalid(
            "truncate width must be positive".into(),
        ));
    }
    let w_i64 = w as i64;
    match value {
        PgValue::Int2(x) => Ok(PartitionLiteral::Int(truncate_i64(*x as i64, w_i64) as i32)),
        PgValue::Int4(x) => Ok(PartitionLiteral::Int(truncate_i64(*x as i64, w_i64) as i32)),
        PgValue::Int8(x) => Ok(PartitionLiteral::Long(truncate_i64(*x, w_i64))),
        PgValue::Text(s) | PgValue::Json(s) | PgValue::Jsonb(s) => Ok(PartitionLiteral::String(
            // Iceberg spec: truncate to L Unicode code points, NOT bytes.
            s.chars().take(w as usize).collect(),
        )),
        PgValue::Bytea(b) => Ok(PartitionLiteral::Binary(
            b.iter().take(w as usize).copied().collect(),
        )),
        PgValue::Numeric(d) => truncate_decimal(d, w),
        PgValue::Bool(_)
        | PgValue::Float4(_)
        | PgValue::Float8(_)
        | PgValue::Date(_)
        | PgValue::Time(_)
        | PgValue::TimeTz { .. }
        | PgValue::Timestamp(_)
        | PgValue::TimestampTz(_)
        | PgValue::Uuid(_) => Err(PartitionError::Invalid(format!(
            "truncate transform does not accept value of type {value:?}; \
             iceberg spec restricts truncate to int/long/decimal/string/binary"
        ))),
        PgValue::Null => unreachable!("apply_transform short-circuits Null"),
    }
}

/// Iceberg spec: `v - (((v % W) + W) % W)`. Equivalent to flooring `v`
/// toward negative infinity to a multiple of W.
#[inline]
fn truncate_i64(v: i64, w: i64) -> i64 {
    v - v.rem_euclid(w)
}

/// Decimal truncate per iceberg spec: applies `v - rem_euclid(v, W)` to
/// the *unscaled* integer value, preserving the original scale. Mirrors
/// the Go reference's `truncateDecimal` (which uses `big.Int` because Go
/// stores decimals as strings); we use `i128` since iceberg caps precision
/// at 38 digits and that fits.
fn truncate_decimal(d: &crate::value::Decimal, w: u32) -> Result<PartitionLiteral, PartitionError> {
    let unscaled = be_bytes_to_i128(&d.unscaled_be_bytes).ok_or_else(|| {
        PartitionError::Invalid(format!(
            "decimal unscaled value too wide ({} bytes); iceberg caps decimal \
             precision at 38 digits which fits in 16 bytes",
            d.unscaled_be_bytes.len()
        ))
    })?;
    let w = w as i128;
    let truncated = unscaled - unscaled.rem_euclid(w);
    Ok(PartitionLiteral::Decimal {
        unscaled: truncated,
        scale: d.scale,
    })
}

/// Sign-extend a variable-length big-endian two's-complement byte string
/// to an `i128`. Returns `None` if the input is empty or wider than 16
/// bytes (iceberg's 38-digit decimal cap fits in 16 bytes).
fn be_bytes_to_i128(bytes: &[u8]) -> Option<i128> {
    if bytes.is_empty() || bytes.len() > 16 {
        return None;
    }
    let is_negative = bytes[0] & 0x80 != 0;
    let mut buf = if is_negative { [0xFFu8; 16] } else { [0u8; 16] };
    let start = 16 - bytes.len();
    buf[start..].copy_from_slice(bytes);
    Some(i128::from_be_bytes(buf))
}

fn identity_literal(value: &PgValue) -> Result<PartitionLiteral, PartitionError> {
    Ok(match value {
        PgValue::Null => PartitionLiteral::Null,
        PgValue::Bool(b) => PartitionLiteral::Boolean(*b),
        PgValue::Int2(n) => PartitionLiteral::Int(*n as i32),
        PgValue::Int4(n) => PartitionLiteral::Int(*n),
        PgValue::Int8(n) => PartitionLiteral::Long(*n),
        PgValue::Float4(n) => PartitionLiteral::Float(f32_no_nan::F32(*n)),
        PgValue::Float8(n) => PartitionLiteral::Double(f64_no_nan::F64(*n)),
        PgValue::Text(s) | PgValue::Json(s) | PgValue::Jsonb(s) => {
            PartitionLiteral::String(s.clone())
        }
        PgValue::Bytea(b) => PartitionLiteral::Binary(b.clone()),
        PgValue::Date(d) => PartitionLiteral::Int(d.0),
        PgValue::Time(t) | PgValue::TimeTz { time: t, .. } => PartitionLiteral::Long(t.0),
        PgValue::Timestamp(t) | PgValue::TimestampTz(t) => PartitionLiteral::Long(t.0),
        PgValue::Uuid(b) => PartitionLiteral::Binary(b.to_vec()),
        // Numeric identity-partitioning is unusual; iceberg supports
        // it but our wire shape needs special handling. Defer.
        PgValue::Numeric(_) => {
            return Err(PartitionError::Invalid(
                "identity partition on numeric is not yet wired; \
                 use bucket or pre-truncate the column instead"
                    .into(),
            ))
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::value::{DaysSinceEpoch, TimestampMicros};

    #[test]
    fn identity_no_parens() {
        let f = parse_partition_expr("region").unwrap();
        assert_eq!(f.source_column, "region");
        assert_eq!(f.name, "region");
        assert_eq!(f.transform, Transform::Identity);
    }

    #[test]
    fn day_year_month_hour_parse() {
        let cases = [
            ("year(created_at)", Transform::Year),
            ("month(created_at)", Transform::Month),
            ("day(created_at)", Transform::Day),
            ("hour(created_at)", Transform::Hour),
        ];
        for (s, t) in cases {
            let f = parse_partition_expr(s).unwrap();
            assert_eq!(f.source_column, "created_at");
            assert_eq!(f.transform, t);
        }
    }

    #[test]
    fn bucket_and_truncate_carry_param() {
        let f = parse_partition_expr("bucket[16](id)").unwrap();
        assert_eq!(f.transform, Transform::Bucket(16));
        let f = parse_partition_expr("truncate[4](name)").unwrap();
        assert_eq!(f.transform, Transform::Truncate(4));
    }

    #[test]
    fn case_insensitive_transform_name() {
        let f = parse_partition_expr("DAY(created_at)").unwrap();
        assert_eq!(f.transform, Transform::Day);
    }

    #[test]
    fn rejects_bucket_without_param() {
        assert!(matches!(
            parse_partition_expr("bucket(id)"),
            Err(PartitionError::Invalid(_))
        ));
    }

    #[test]
    fn rejects_unknown_transform() {
        assert!(matches!(
            parse_partition_expr("hash(id)"),
            Err(PartitionError::UnknownTransform(_))
        ));
    }

    #[test]
    fn rejects_zero_param() {
        assert!(parse_partition_expr("bucket[0](id)").is_err());
    }

    #[test]
    fn day_transform_on_date_returns_days_since_epoch() {
        let v = PgValue::Date(DaysSinceEpoch(20_000));
        match apply_transform(&v, Transform::Day).unwrap() {
            PartitionLiteral::Int(n) => assert_eq!(n, 20_000),
            other => panic!("expected Int(20000), got {other:?}"),
        }
    }

    #[test]
    fn day_transform_on_timestamp_floors_to_day() {
        // 2024-01-01 00:00:00 UTC = 19_723 days since epoch.
        // Pick a moment a few hours into that day.
        let micros = 19_723_i64 * MICROS_PER_DAY + 5 * MICROS_PER_HOUR;
        let v = PgValue::Timestamp(TimestampMicros(micros));
        match apply_transform(&v, Transform::Day).unwrap() {
            PartitionLiteral::Int(n) => assert_eq!(n, 19_723),
            other => panic!("expected Int(19723), got {other:?}"),
        }
    }

    #[test]
    fn hour_transform_floors_correctly() {
        let micros = 19_723_i64 * MICROS_PER_DAY + 5 * MICROS_PER_HOUR + 1;
        let v = PgValue::TimestampTz(TimestampMicros(micros));
        match apply_transform(&v, Transform::Hour).unwrap() {
            PartitionLiteral::Int(n) => assert_eq!(n, 19_723 * 24 + 5),
            other => panic!("expected Int(19723*24 + 5), got {other:?}"),
        }
    }

    #[test]
    fn year_month_calendar_correct_for_known_dates() {
        // 1970-01-01 → year 0, month 0
        let v = PgValue::Date(DaysSinceEpoch(0));
        match apply_transform(&v, Transform::Year).unwrap() {
            PartitionLiteral::Int(0) => {}
            other => panic!("year(epoch) = {other:?}"),
        }
        match apply_transform(&v, Transform::Month).unwrap() {
            PartitionLiteral::Int(0) => {}
            other => panic!("month(epoch) = {other:?}"),
        }

        // 2024-01-01 → year 54, month 54*12+0 = 648
        let v = PgValue::Date(DaysSinceEpoch(19_723));
        match apply_transform(&v, Transform::Year).unwrap() {
            PartitionLiteral::Int(n) => assert_eq!(n, 54),
            other => panic!("year(2024-01-01) = {other:?}"),
        }
        match apply_transform(&v, Transform::Month).unwrap() {
            PartitionLiteral::Int(n) => assert_eq!(n, 54 * 12),
            other => panic!("month(2024-01-01) = {other:?}"),
        }
    }

    #[test]
    fn identity_on_int_passes_through() {
        match apply_transform(&PgValue::Int4(7), Transform::Identity).unwrap() {
            PartitionLiteral::Int(7) => {}
            other => panic!("got {other:?}"),
        }
        match apply_transform(&PgValue::Int8(42), Transform::Identity).unwrap() {
            PartitionLiteral::Long(42) => {}
            other => panic!("got {other:?}"),
        }
    }

    #[test]
    fn null_value_passes_through_to_null_literal() {
        for t in [
            Transform::Identity,
            Transform::Year,
            Transform::Day,
            Transform::Bucket(16),
        ] {
            match apply_transform(&PgValue::Null, t).unwrap() {
                PartitionLiteral::Null => {}
                other => panic!("expected Null for {t:?}, got {other:?}"),
            }
        }
    }

    // ── bucket apply ──────────────────────────────────────────────────

    fn bucket_unwrap(v: &PgValue, n: u32) -> i32 {
        match apply_transform(v, Transform::Bucket(n)).unwrap() {
            PartitionLiteral::Int(x) => x,
            other => panic!("expected Int from bucket, got {other:?}"),
        }
    }

    #[test]
    fn bucket_int_long_match_iceberg_reference_values() {
        // Cross-checked against iceberg-rust's own
        // `transform::bucket::tests`:
        // - bucket[10] of int(100) and long(100) both = 6
        //   (tests/transform/bucket.rs::test_projection_bucket_integer/long).
        assert_eq!(bucket_unwrap(&PgValue::Int4(100), 10), 6);
        assert_eq!(bucket_unwrap(&PgValue::Int8(100), 10), 6);
        // int and long with the same numeric value collide in iceberg's
        // bucket spec, since both hash via i64-LE.
        assert_eq!(
            bucket_unwrap(&PgValue::Int4(1), 16),
            bucket_unwrap(&PgValue::Int8(1), 16)
        );
    }

    #[test]
    fn bucket_string_hashes_utf8_bytes() {
        // Same string → same bucket regardless of how often we hash.
        let v = PgValue::Text("iceberg".into());
        let a = bucket_unwrap(&v, 16);
        let b = bucket_unwrap(&v, 16);
        assert_eq!(a, b);
        assert!((0..16).contains(&a));
        // Different strings should *generally* land in different buckets,
        // though collisions are possible. Just check the result is a valid
        // bucket index.
        let v2 = PgValue::Text("rust".into());
        let c = bucket_unwrap(&v2, 16);
        assert!((0..16).contains(&c));
    }

    #[test]
    fn bucket_result_is_always_in_range() {
        // Iceberg spec uses Java's int; bucket counts > i32::MAX aren't
        // representable. Test with the realistic range operators pick.
        for n in [1u32, 4, 16, 1000, 1_000_000] {
            for v in [
                PgValue::Int4(0),
                PgValue::Int4(-1),
                PgValue::Int4(i32::MIN),
                PgValue::Int8(i64::MAX),
                PgValue::Text("".into()),
                PgValue::Text("hello world".into()),
            ] {
                let bucket = bucket_unwrap(&v, n);
                assert!(
                    (0..n as i32).contains(&bucket),
                    "bucket[{n}] of {v:?} = {bucket}, out of range"
                );
            }
        }
    }

    #[test]
    fn bucket_negative_values_still_land_in_positive_range() {
        // (hash & i32::MAX) strips the sign bit; bucket must be ≥ 0.
        for v in [-1, -100, i32::MIN, -i32::MAX] {
            let bucket = bucket_unwrap(&PgValue::Int4(v), 16);
            assert!((0..16).contains(&bucket));
        }
    }

    #[test]
    fn bucket_uuid_hashes_canonical_bytes() {
        let v = PgValue::Uuid([0u8; 16]);
        let bucket = bucket_unwrap(&v, 16);
        assert!((0..16).contains(&bucket));
    }

    #[test]
    fn bucket_rejects_unsupported_types() {
        for v in [
            PgValue::Bool(true),
            PgValue::Float4(1.0),
            PgValue::Float8(1.0),
            PgValue::Bytea(vec![1, 2, 3]),
        ] {
            assert!(matches!(
                apply_transform(&v, Transform::Bucket(16)),
                Err(PartitionError::Invalid(_))
            ));
        }
    }

    #[test]
    fn bucket_decimal_uses_minimum_be_bytes() {
        use crate::value::Decimal;
        // 5 — same hash whether stored as [0x05], [0x00, 0x05], or [0x00, 0x00, 0x05].
        let a = bucket_unwrap(
            &PgValue::Numeric(Decimal {
                unscaled_be_bytes: vec![0x05],
                scale: 0,
            }),
            16,
        );
        let b = bucket_unwrap(
            &PgValue::Numeric(Decimal {
                unscaled_be_bytes: vec![0x00, 0x05],
                scale: 0,
            }),
            16,
        );
        let c = bucket_unwrap(
            &PgValue::Numeric(Decimal {
                unscaled_be_bytes: vec![0x00, 0x00, 0x05],
                scale: 0,
            }),
            16,
        );
        assert_eq!(a, b);
        assert_eq!(b, c);
    }

    #[test]
    fn bucket_zero_n_errors() {
        assert!(matches!(
            apply_transform(&PgValue::Int4(1), Transform::Bucket(0)),
            Err(PartitionError::Invalid(_))
        ));
    }

    // ── truncate apply ────────────────────────────────────────────────

    #[test]
    fn truncate_int_floors_to_multiple_of_w() {
        // Spec: `v - (((v % W) + W) % W)` → floor toward negative infinity.
        let cases: &[(i32, u32, i32)] = &[
            (0, 4, 0),
            (1, 4, 0),
            (3, 4, 0),
            (4, 4, 4),
            (13, 4, 12),
            (-1, 4, -4),
            (-4, 4, -4),
            (-5, 4, -8),
        ];
        for (v, w, expected) in cases {
            match apply_transform(&PgValue::Int4(*v), Transform::Truncate(*w)).unwrap() {
                PartitionLiteral::Int(got) => {
                    assert_eq!(
                        got, *expected,
                        "truncate[{w}]({v}) = {got}, want {expected}"
                    )
                }
                other => panic!("expected Int from truncate, got {other:?}"),
            }
        }
    }

    #[test]
    fn truncate_int8_returns_long_literal() {
        match apply_transform(&PgValue::Int8(1_000_000_000_007), Transform::Truncate(100)).unwrap()
        {
            PartitionLiteral::Long(got) => assert_eq!(got, 1_000_000_000_000),
            other => panic!("expected Long, got {other:?}"),
        }
    }

    #[test]
    fn truncate_string_takes_first_w_code_points() {
        match apply_transform(&PgValue::Text("hello world".into()), Transform::Truncate(5)).unwrap()
        {
            PartitionLiteral::String(s) => assert_eq!(s, "hello"),
            other => panic!("expected String, got {other:?}"),
        }
    }

    #[test]
    fn truncate_string_handles_multibyte_unicode() {
        // 4 code points but multi-byte. Iceberg spec is clear: count
        // characters, NOT bytes.
        let v = PgValue::Text("café☕".into()); // c, a, f, é, ☕  → 5 code points
        match apply_transform(&v, Transform::Truncate(4)).unwrap() {
            PartitionLiteral::String(s) => assert_eq!(s, "café"),
            other => panic!("expected String, got {other:?}"),
        }
        match apply_transform(&v, Transform::Truncate(10)).unwrap() {
            // Width >= length → return original unchanged.
            PartitionLiteral::String(s) => assert_eq!(s, "café☕"),
            other => panic!("expected String, got {other:?}"),
        }
    }

    #[test]
    fn truncate_binary_takes_first_w_bytes() {
        match apply_transform(
            &PgValue::Bytea(vec![1, 2, 3, 4, 5, 6]),
            Transform::Truncate(3),
        )
        .unwrap()
        {
            PartitionLiteral::Binary(b) => assert_eq!(b, vec![1, 2, 3]),
            other => panic!("expected Binary, got {other:?}"),
        }
    }

    #[test]
    fn truncate_rejects_unsupported_types() {
        for v in [
            PgValue::Bool(true),
            PgValue::Float4(1.0),
            PgValue::Date(crate::value::DaysSinceEpoch(0)),
            PgValue::Uuid([0u8; 16]),
        ] {
            assert!(matches!(
                apply_transform(&v, Transform::Truncate(4)),
                Err(PartitionError::Invalid(_))
            ));
        }
    }

    #[test]
    fn truncate_decimal_floors_unscaled_value_and_keeps_scale() {
        use crate::value::Decimal;
        // 10.65, scale=2, W=50 → unscaled=1065, 1065 - (1065 % 50) = 1050 → 10.50
        let d = Decimal {
            unscaled_be_bytes: 1065i128
                .to_be_bytes()
                .iter()
                .skip_while(|b| **b == 0)
                .copied()
                .collect(),
            scale: 2,
        };
        match apply_transform(&PgValue::Numeric(d), Transform::Truncate(50)).unwrap() {
            PartitionLiteral::Decimal { unscaled, scale } => {
                assert_eq!(unscaled, 1050);
                assert_eq!(scale, 2);
            }
            other => panic!("expected Decimal, got {other:?}"),
        }
    }

    #[test]
    fn truncate_decimal_handles_negative_unscaled() {
        use crate::value::Decimal;
        // -7, scale=0, W=4 → -7 - rem_euclid(-7, 4) = -7 - 1 = -8.
        let d = Decimal {
            unscaled_be_bytes: vec![0xF9], // -7 in 8-bit two's complement
            scale: 0,
        };
        match apply_transform(&PgValue::Numeric(d), Transform::Truncate(4)).unwrap() {
            PartitionLiteral::Decimal { unscaled, scale } => {
                assert_eq!(unscaled, -8);
                assert_eq!(scale, 0);
            }
            other => panic!("expected Decimal, got {other:?}"),
        }
    }

    #[test]
    fn truncate_decimal_preserves_arity_for_38_digit_unscaled() {
        use crate::value::Decimal;
        // 38-digit max value (10^38 - 1) ≈ 3.4e38, fits in i128.
        let unscaled: i128 = 99_999_999_999_999_999_999_999_999_999_999_999_999_i128;
        let d = Decimal {
            unscaled_be_bytes: unscaled.to_be_bytes().to_vec(),
            scale: 10,
        };
        let got = apply_transform(&PgValue::Numeric(d), Transform::Truncate(100)).unwrap();
        if let PartitionLiteral::Decimal { unscaled: u, scale } = got {
            assert_eq!(u, unscaled - unscaled.rem_euclid(100));
            assert_eq!(scale, 10);
        } else {
            panic!("expected Decimal, got {got:?}")
        }
    }

    #[test]
    fn truncate_decimal_rejects_unscaled_wider_than_16_bytes() {
        use crate::value::Decimal;
        let d = Decimal {
            unscaled_be_bytes: vec![0; 17],
            scale: 0,
        };
        assert!(matches!(
            apply_transform(&PgValue::Numeric(d), Transform::Truncate(10)),
            Err(PartitionError::Invalid(_))
        ));
    }

    #[test]
    fn truncate_zero_w_errors() {
        assert!(matches!(
            apply_transform(&PgValue::Int4(1), Transform::Truncate(0)),
            Err(PartitionError::Invalid(_))
        ));
    }

    #[test]
    fn parse_partition_spec_chains_errors() {
        let exprs = vec!["day(t)".to_string(), "bogus(x)".to_string()];
        assert!(parse_partition_spec(&exprs).is_err());
    }
}
