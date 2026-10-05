//! Decode pgoutput tuple bytes into [`pg2iceberg_core::PgValue`].
//!
//! pgoutput emits column data in **text** format by default — the same
//! format you'd see from `SELECT col::text`. We parse that rather than
//! enabling binary mode (proto v4) so the decoder is parser-only and
//! easy to unit-test.

use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Timelike};
use pg2iceberg_core::typemap::PgType;
use pg2iceberg_core::value::{
    DaysSinceEpoch, Decimal as CoreDecimal, PgValue, TimeMicros, TimestampMicros,
};
use uuid::Uuid;

#[derive(Debug, thiserror::Error)]
pub enum DecodeError {
    #[error("invalid {ty:?} text-format value: {raw}")]
    Invalid { ty: PgType, raw: String },
}

/// Decode the text bytes from a pgoutput tuple column into a [`PgValue`].
/// The caller has already disambiguated null vs. unchanged (TOAST `'u'`
/// placeholder); by the time we get here `bytes` is the raw textual
/// encoding of a present, non-null value.
pub fn decode_text(ty: PgType, bytes: &[u8]) -> Result<PgValue, DecodeError> {
    let raw = std::str::from_utf8(bytes).map_err(|_| DecodeError::Invalid {
        ty,
        raw: format!("<{} non-utf8 bytes>", bytes.len()),
    })?;
    let bad = || DecodeError::Invalid {
        ty,
        raw: raw.to_string(),
    };

    Ok(match ty {
        PgType::Bool => match raw {
            "t" | "true" | "1" => PgValue::Bool(true),
            "f" | "false" | "0" => PgValue::Bool(false),
            _ => return Err(bad()),
        },
        PgType::Int2 => PgValue::Int2(raw.parse().map_err(|_| bad())?),
        PgType::Int4 => PgValue::Int4(raw.parse().map_err(|_| bad())?),
        // Unsigned 32-bit, so `long`: values past 2^31 don't fit `int`.
        PgType::Oid => PgValue::Int8(raw.parse::<u32>().map_err(|_| bad())?.into()),
        PgType::Int8 => PgValue::Int8(raw.parse().map_err(|_| bad())?),
        PgType::Float4 => PgValue::Float4(raw.parse().map_err(|_| bad())?),
        PgType::Float8 => PgValue::Float8(raw.parse().map_err(|_| bad())?),
        PgType::Numeric { .. } => PgValue::Numeric(decode_numeric(raw).ok_or_else(bad)?),
        PgType::Text => PgValue::Text(raw.to_string()),
        PgType::Json => PgValue::Json(raw.to_string()),
        PgType::Jsonb => PgValue::Jsonb(raw.to_string()),
        PgType::Bytea => PgValue::Bytea(decode_bytea(raw).ok_or_else(bad)?),
        PgType::Date => {
            let d = NaiveDate::parse_from_str(raw, "%Y-%m-%d").map_err(|_| bad())?;
            let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).expect("epoch");
            PgValue::Date(DaysSinceEpoch(
                (d - epoch).num_days().try_into().map_err(|_| bad())?,
            ))
        }
        PgType::Time => {
            let t = parse_pg_time(raw).ok_or_else(bad)?;
            PgValue::Time(TimeMicros(time_to_micros_of_day(t)))
        }
        PgType::TimeTz => {
            let (time_part, offset_part) = split_offset(raw).ok_or_else(bad)?;
            let t = parse_pg_time(time_part).ok_or_else(bad)?;
            let zone_secs = parse_pg_offset(offset_part).ok_or_else(bad)?;
            PgValue::TimeTz {
                time: TimeMicros(time_to_micros_of_day(t)),
                zone_secs,
            }
        }
        PgType::Timestamp => {
            let ts = parse_pg_timestamp(raw).ok_or_else(bad)?;
            PgValue::Timestamp(TimestampMicros(timestamp_to_micros(ts)))
        }
        PgType::TimestampTz => {
            // PG emits e.g. `2026-04-26 13:00:00+00` (offset may be 1, 2,
            // or 3 segments). Convert to UTC micros.
            let ts = DateTime::parse_from_str(raw, "%Y-%m-%d %H:%M:%S%.f%#z").map_err(|_| bad())?;
            PgValue::TimestampTz(TimestampMicros(ts.timestamp_micros()))
        }
        PgType::Uuid => PgValue::Uuid(*Uuid::parse_str(raw).map_err(|_| bad())?.as_bytes()),
    })
}

fn parse_pg_time(raw: &str) -> Option<NaiveTime> {
    NaiveTime::parse_from_str(raw, "%H:%M:%S%.f")
        .or_else(|_| NaiveTime::parse_from_str(raw, "%H:%M:%S"))
        .ok()
}

fn parse_pg_timestamp(raw: &str) -> Option<NaiveDateTime> {
    NaiveDateTime::parse_from_str(raw, "%Y-%m-%d %H:%M:%S%.f")
        .or_else(|_| NaiveDateTime::parse_from_str(raw, "%Y-%m-%d %H:%M:%S"))
        .ok()
}

fn time_to_micros_of_day(t: NaiveTime) -> i64 {
    t.num_seconds_from_midnight() as i64 * 1_000_000 + (t.nanosecond() as i64 / 1000)
}

fn timestamp_to_micros(ts: NaiveDateTime) -> i64 {
    ts.and_utc().timestamp_micros()
}

/// Split `12:34:56+02` → `("12:34:56", "+02")`. Looks for the rightmost
/// `+`/`-` (defensive against possible leading sign on time).
fn split_offset(raw: &str) -> Option<(&str, &str)> {
    let bytes = raw.as_bytes();
    for (i, &b) in bytes.iter().enumerate().rev() {
        if (b == b'+' || b == b'-') && i > 0 {
            return Some((&raw[..i], &raw[i..]));
        }
    }
    None
}

/// Parse `+02`, `-05:30`, `+02:00:15` → seconds east of UTC.
fn parse_pg_offset(raw: &str) -> Option<i32> {
    let (sign, rest) = match raw.as_bytes().first()? {
        b'+' => (1_i32, &raw[1..]),
        b'-' => (-1_i32, &raw[1..]),
        _ => return None,
    };
    let parts: Vec<&str> = rest.split(':').collect();
    let hours: i32 = parts.first()?.parse().ok()?;
    let mins: i32 = parts.get(1).map(|s| s.parse().ok()).unwrap_or(Some(0))?;
    let secs: i32 = parts.get(2).map(|s| s.parse().ok()).unwrap_or(Some(0))?;
    Some(sign * (hours * 3600 + mins * 60 + secs))
}

/// PG bytea text format `\xdeadbeef` → raw bytes. Modern PG defaults to
/// hex output (`bytea_output = 'hex'`); the older escape format isn't
/// emitted unless the operator opts out.
fn decode_bytea(raw: &str) -> Option<Vec<u8>> {
    let hex = raw.strip_prefix("\\x")?;
    if hex.len() % 2 != 0 {
        return None;
    }
    (0..hex.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).ok())
        .collect()
}

/// Parse `123.45` → our wire `Decimal { unscaled_be_bytes, scale }`.
///
/// We parse the digit string directly into an `i128` rather than
/// going through `rust_decimal` because `rust_decimal`'s mantissa is
/// 96-bit (precision ~28-29 digits), which can't represent
/// Iceberg-max `numeric(38, _)` values. PG itself can carry far more
/// (~131k digits), but Iceberg caps at precision 38, so the i128
/// budget — ~3.4 × 10^38, comfortably above 10^38 — is sufficient
/// for any value our pipeline can legally store.
fn decode_numeric(raw: &str) -> Option<CoreDecimal> {
    // Reject NaN / Inf / empty — PG can emit `NaN` for `numeric`,
    // but Iceberg has no representation for it. Drop the row
    // explicitly rather than silently round to 0.
    let raw = raw.trim();
    if raw.is_empty() || raw.eq_ignore_ascii_case("NaN") {
        return None;
    }
    let (sign, body) = match raw.as_bytes().first() {
        Some(b'-') => (-1i128, &raw[1..]),
        Some(b'+') => (1i128, &raw[1..]),
        _ => (1i128, raw),
    };
    // Optional exponent (PG's text form for `numeric` doesn't emit
    // one in practice, but we accept it for robustness against
    // user-supplied input that flowed through `to_char` / similar).
    let (digits_part, exponent) = match body.find(['e', 'E']) {
        Some(idx) => {
            let exp: i32 = body[idx + 1..].parse().ok()?;
            (&body[..idx], exp)
        }
        None => (body, 0),
    };
    // Split mantissa on the decimal point.
    let (int_part, frac_part) = match digits_part.find('.') {
        Some(i) => (&digits_part[..i], &digits_part[i + 1..]),
        None => (digits_part, ""),
    };
    // Strip leading zeros on int part for cleaner overflow checks
    // (the `i128::checked_mul` below would catch overflow anyway,
    // but losing leading zeros early keeps small values fast).
    let int_part = int_part.trim_start_matches('0');
    if !int_part.chars().all(|c| c.is_ascii_digit())
        || !frac_part.chars().all(|c| c.is_ascii_digit())
    {
        return None;
    }
    let combined = format!("{int_part}{frac_part}");
    let mantissa = if combined.is_empty() {
        0i128
    } else {
        combined.parse::<i128>().ok()? * sign
    };
    let raw_scale = frac_part.len() as i32;
    // Apply the exponent: a positive exponent shifts the decimal
    // point right (reduces scale), a negative one shifts left.
    let final_scale = raw_scale - exponent;
    if final_scale < 0 {
        // Negative scale ≡ trailing zeros on an integer mantissa.
        // Multiply mantissa to absorb it, scale becomes 0.
        let extra = (-final_scale) as u32;
        let factor = 10i128.checked_pow(extra)?;
        let m = mantissa.checked_mul(factor)?;
        return Some(CoreDecimal {
            unscaled_be_bytes: m.to_be_bytes().to_vec(),
            scale: 0,
        });
    }
    let scale: u8 = u8::try_from(final_scale).ok()?;
    Some(CoreDecimal {
        unscaled_be_bytes: mantissa.to_be_bytes().to_vec(),
        scale,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bool_decodes_short_and_long_forms() {
        assert!(matches!(
            decode_text(PgType::Bool, b"t"),
            Ok(PgValue::Bool(true))
        ));
        assert!(matches!(
            decode_text(PgType::Bool, b"f"),
            Ok(PgValue::Bool(false))
        ));
        assert!(matches!(
            decode_text(PgType::Bool, b"true"),
            Ok(PgValue::Bool(true))
        ));
        assert!(matches!(
            decode_text(PgType::Bool, b"false"),
            Ok(PgValue::Bool(false))
        ));
        assert!(decode_text(PgType::Bool, b"yes").is_err());
    }

    #[test]
    fn integers_decode() {
        assert!(matches!(
            decode_text(PgType::Int2, b"42"),
            Ok(PgValue::Int2(42))
        ));
        assert!(matches!(
            decode_text(PgType::Int4, b"-7"),
            Ok(PgValue::Int4(-7))
        ));
        assert!(matches!(
            decode_text(PgType::Int8, b"99999999999"),
            Ok(PgValue::Int8(99999999999))
        ));
    }

    #[test]
    fn numeric_decodes_with_correct_scale() {
        let v = decode_text(
            PgType::Numeric {
                precision: Some(10),
                scale: Some(2),
            },
            b"123.45",
        )
        .unwrap();
        let d = match v {
            PgValue::Numeric(d) => d,
            _ => panic!("expected Numeric"),
        };
        assert_eq!(d.scale, 2);
        // mantissa = 12345 as i128 → bytes [0..14] = 0, [14] = 0x30, [15] = 0x39.
        let mut expected = vec![0_u8; 16];
        let m = 12345_i128.to_be_bytes();
        expected.copy_from_slice(&m);
        assert_eq!(d.unscaled_be_bytes, expected);
    }

    #[test]
    fn numeric_negative() {
        let v = decode_text(
            PgType::Numeric {
                precision: Some(10),
                scale: Some(2),
            },
            b"-1.5",
        )
        .unwrap();
        let d = match v {
            PgValue::Numeric(d) => d,
            _ => panic!("expected Numeric"),
        };
        assert_eq!(d.scale, 1);
        // mantissa = -15
        let expected = (-15_i128).to_be_bytes().to_vec();
        assert_eq!(d.unscaled_be_bytes, expected);
    }

    #[test]
    fn text_decodes_passthrough() {
        let v = decode_text(PgType::Text, b"hello world").unwrap();
        assert!(matches!(v, PgValue::Text(s) if s == "hello world"));
    }

    #[test]
    fn bytea_decodes_hex_format() {
        let v = decode_text(PgType::Bytea, br"\xdeadbeef").unwrap();
        assert!(matches!(v, PgValue::Bytea(b) if b == vec![0xde, 0xad, 0xbe, 0xef]));
    }

    #[test]
    fn bytea_rejects_non_hex_format() {
        assert!(decode_text(PgType::Bytea, b"not hex").is_err());
        // Odd-length hex body — `\xa` has 1 hex char which can't form a byte.
        assert!(decode_text(PgType::Bytea, br"\xa").is_err());
        // Even-length but non-hex chars.
        assert!(decode_text(PgType::Bytea, br"\xZZ").is_err());
    }

    #[test]
    fn date_decodes_to_days_since_epoch() {
        let v = decode_text(PgType::Date, b"1970-01-02").unwrap();
        assert!(matches!(v, PgValue::Date(DaysSinceEpoch(1))));
        let v = decode_text(PgType::Date, b"1969-12-31").unwrap();
        assert!(matches!(v, PgValue::Date(DaysSinceEpoch(-1))));
        let v = decode_text(PgType::Date, b"2026-04-26").unwrap();
        match v {
            PgValue::Date(DaysSinceEpoch(d)) => assert!(d > 20_000),
            _ => panic!("expected Date"),
        }
    }

    #[test]
    fn time_decodes_with_and_without_fractional() {
        // 12:34:56 → 12*3600 + 34*60 + 56 = 45_296 seconds = 45_296_000_000 micros.
        let v = decode_text(PgType::Time, b"12:34:56").unwrap();
        assert!(matches!(v, PgValue::Time(TimeMicros(45_296_000_000))));

        // 12:34:56.789 → adds 789_000 micros.
        let v = decode_text(PgType::Time, b"12:34:56.789").unwrap();
        assert!(matches!(v, PgValue::Time(TimeMicros(45_296_789_000))));
    }

    #[test]
    fn timetz_decodes_with_offset() {
        let v = decode_text(PgType::TimeTz, b"12:34:56+02").unwrap();
        match v {
            PgValue::TimeTz { time, zone_secs } => {
                assert_eq!(time.0, 45_296_000_000);
                assert_eq!(zone_secs, 2 * 3600);
            }
            _ => panic!("expected TimeTz"),
        }
        let v = decode_text(PgType::TimeTz, b"12:34:56-05:30").unwrap();
        match v {
            PgValue::TimeTz { zone_secs, .. } => {
                assert_eq!(zone_secs, -(5 * 3600 + 30 * 60));
            }
            _ => panic!("expected TimeTz"),
        }
    }

    #[test]
    fn timestamp_decodes_to_micros() {
        let v = decode_text(PgType::Timestamp, b"1970-01-01 00:00:01").unwrap();
        assert!(matches!(v, PgValue::Timestamp(TimestampMicros(1_000_000))));
        let v = decode_text(PgType::Timestamp, b"1970-01-01 00:00:00.5").unwrap();
        assert!(matches!(v, PgValue::Timestamp(TimestampMicros(500_000))));
    }

    #[test]
    fn timestamptz_decodes_to_utc_micros() {
        // `2026-04-26 13:00:00+00` is a clean UTC ts.
        let v = decode_text(PgType::TimestampTz, b"2026-04-26 13:00:00+00").unwrap();
        let utc = match v {
            PgValue::TimestampTz(TimestampMicros(m)) => m,
            _ => panic!("expected TimestampTz"),
        };
        // `2026-04-26 11:00:00+02` should normalize to the same UTC instant.
        let v2 = decode_text(PgType::TimestampTz, b"2026-04-26 15:00:00+02").unwrap();
        match v2 {
            PgValue::TimestampTz(TimestampMicros(m)) => assert_eq!(m, utc),
            _ => panic!("expected TimestampTz"),
        }
    }

    #[test]
    fn uuid_decodes() {
        let v = decode_text(PgType::Uuid, b"550e8400-e29b-41d4-a716-446655440000").unwrap();
        match v {
            PgValue::Uuid(b) => {
                assert_eq!(b[0], 0x55);
                assert_eq!(b[15], 0x00);
            }
            _ => panic!("expected Uuid"),
        }
    }

    #[test]
    fn json_and_jsonb_round_trip_as_text() {
        let v = decode_text(PgType::Json, br#"{"a":1}"#).unwrap();
        assert!(matches!(v, PgValue::Json(s) if s == r#"{"a":1}"#));
        let v = decode_text(PgType::Jsonb, br#"{"b":[1,2]}"#).unwrap();
        assert!(matches!(v, PgValue::Jsonb(s) if s == r#"{"b":[1,2]}"#));
    }
}
