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
///
/// Bucket and Truncate currently return
/// `Err(PartitionError::Invalid("not yet wired"))` — partition spec
/// parses + table is created with the right transform on the
/// iceberg side, but compute-at-write is a follow-on. Identity +
/// time-bucket transforms cover the bulk of real partition layouts;
/// bucket / truncate land when we wire the murmur3 + truncate
/// functions over `PgValue` (small but separable work).
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
        Transform::Bucket(_) => Err(PartitionError::Invalid(
            "bucket transform is not yet wired in the writer; \
             partition spec is set on the table but per-row routing \
             needs the murmur3 application path"
                .into(),
        )),
        Transform::Truncate(_) => Err(PartitionError::Invalid(
            "truncate transform is not yet wired in the writer; \
             follows-on alongside bucket"
                .into(),
        )),
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

    #[test]
    fn bucket_and_truncate_error_at_apply_time() {
        let v = PgValue::Int4(7);
        assert!(matches!(
            apply_transform(&v, Transform::Bucket(16)),
            Err(PartitionError::Invalid(_))
        ));
        assert!(matches!(
            apply_transform(&v, Transform::Truncate(4)),
            Err(PartitionError::Invalid(_))
        ));
    }

    #[test]
    fn parse_partition_spec_chains_errors() {
        let exprs = vec!["day(t)".to_string(), "bogus(x)".to_string()];
        assert!(parse_partition_spec(&exprs).is_err());
    }
}
