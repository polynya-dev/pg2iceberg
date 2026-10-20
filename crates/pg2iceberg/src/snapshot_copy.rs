//! A snapshot chunk read with `COPY (SELECT …) TO STDOUT (FORMAT binary)`:
//! each value in Postgres's binary form, which takes a few bytes' copy to
//! decode — where the text form takes parsing, a third of a snapshot's
//! CPU. Each column is cast to the type whose binary form [`decode`]
//! reads for its Iceberg type ([`select`]): an `int2` is read as an
//! `int4`, and so on. A string column of a type other than `text` or
//! `varchar` — `char(n)`, `inet`, an enum, an array — is read as its type
//! outputs it as text, as its text form was: a cast to `text` may differ
//! (`char(n)` drops its padding, `inet` adds its netmask).

use pg2iceberg_core::value::{
    DaysSinceEpoch, Decimal as CoreDecimal, PgValue, TimeMicros, TimestampMicros,
};
use pg2iceberg_core::IcebergType;

/// Micros from the Unix epoch to Postgres's, 2000-01-01.
const PG_EPOCH_MICROS: i64 = 946_684_800_000_000;
/// Days from the Unix epoch to Postgres's.
const PG_EPOCH_DAYS: i32 = 10_957;
/// A day's micros: a time of day is fewer.
const DAY_MICROS: i64 = 86_400_000_000;

/// What a chunk selects of `column`, of `ty`, to read it in binary —
/// given its Postgres type's name and output function, if known.
pub(crate) fn select(column: &str, ty: IcebergType, pg_type: Option<(&str, &str)>) -> String {
    let column = format!("\"{}\"", column.replace('"', "\"\""));
    match (ty, pg_type) {
        (IcebergType::String, Some(("text" | "varchar", _))) => column,
        (IcebergType::String, Some((_, output))) => format!("{output}({column})::text"),
        _ => format!("{column}::{}", cast(ty)),
    }
}

/// The type a column of `ty` is cast to, to be read in binary.
fn cast(ty: IcebergType) -> &'static str {
    match ty {
        IcebergType::Boolean => "bool",
        IcebergType::Int => "int4",
        IcebergType::Long => "int8",
        IcebergType::Float => "float4",
        IcebergType::Double => "float8",
        IcebergType::Decimal { .. } => "numeric",
        IcebergType::String => "text",
        IcebergType::Binary => "bytea",
        IcebergType::Date => "date",
        IcebergType::Time => "time",
        IcebergType::Timestamp => "timestamp",
        IcebergType::TimestampTz => "timestamptz",
        IcebergType::Uuid => "uuid",
    }
}

/// A value of a column of `ty`, cast to [`cast`]'s type, in its binary
/// form: the [`PgValue`] its text form decodes to.
pub(crate) fn decode(ty: IcebergType, b: &[u8]) -> Result<PgValue, String> {
    let bad = || format!("{} bytes aren't a binary {}", b.len(), cast(ty));
    Ok(match ty {
        IcebergType::Boolean => PgValue::Bool(fixed::<1>(b).ok_or_else(bad)?[0] != 0),
        IcebergType::Int => PgValue::Int4(i32::from_be_bytes(fixed(b).ok_or_else(bad)?)),
        IcebergType::Long => PgValue::Int8(i64::from_be_bytes(fixed(b).ok_or_else(bad)?)),
        IcebergType::Float => PgValue::Float4(f32::from_be_bytes(fixed(b).ok_or_else(bad)?)),
        IcebergType::Double => PgValue::Float8(f64::from_be_bytes(fixed(b).ok_or_else(bad)?)),
        IcebergType::Decimal { .. } => PgValue::Numeric(numeric(b).ok_or_else(bad)?),
        IcebergType::String => PgValue::Text(
            std::str::from_utf8(b)
                .map_err(|e| format!("text isn't UTF-8: {e}"))?
                .to_string(),
        ),
        IcebergType::Binary => PgValue::Bytea(b.to_vec()),
        IcebergType::Date => {
            let days = i32::from_be_bytes(fixed(b).ok_or_else(bad)?);
            // `±infinity` are the least and greatest: no day.
            if days == i32::MIN || days == i32::MAX {
                return Err("an infinite date".into());
            }
            PgValue::Date(DaysSinceEpoch(
                days.checked_add(PG_EPOCH_DAYS).ok_or_else(bad)?,
            ))
        }
        IcebergType::Time => {
            let micros = i64::from_be_bytes(fixed(b).ok_or_else(bad)?);
            if !(0..DAY_MICROS).contains(&micros) {
                return Err(format!("a time of day of {micros} micros"));
            }
            PgValue::Time(TimeMicros(micros))
        }
        IcebergType::Timestamp => PgValue::Timestamp(timestamp(b).ok_or_else(bad)?),
        IcebergType::TimestampTz => PgValue::TimestampTz(timestamp(b).ok_or_else(bad)?),
        IcebergType::Uuid => PgValue::Uuid(fixed(b).ok_or_else(bad)?),
    })
}

fn fixed<const N: usize>(b: &[u8]) -> Option<[u8; N]> {
    b.try_into().ok()
}

/// Micros since the Unix epoch; `±infinity`, the least and greatest, are
/// none.
fn timestamp(b: &[u8]) -> Option<TimestampMicros> {
    let micros = i64::from_be_bytes(fixed(b)?);
    if micros == i64::MIN || micros == i64::MAX {
        return None;
    }
    Some(TimestampMicros(micros.checked_add(PG_EPOCH_MICROS)?))
}

/// A `numeric`: its digit count, the power of 10000 of its first digit,
/// its sign, its scale, then its digits, base 10000, each a big-endian
/// `u16`. Of the scale it shows, as its text form decodes; none if it's
/// `NaN` or infinite, or its unscaled value is past an `i128`.
fn numeric(b: &[u8]) -> Option<CoreDecimal> {
    let word = |i: usize| -> Option<u16> {
        Some(u16::from_be_bytes(
            b.get(2 * i..2 * i + 2)?.try_into().ok()?,
        ))
    };
    let digits = usize::from(word(0)?);
    let weight = i32::from(word(1)? as i16);
    let negative = match word(2)? {
        0x0000 => false,
        0x4000 => true,
        // NaN, ±infinity.
        _ => return None,
    };
    let scale = word(3)?;
    if b.len() != 8 + 2 * digits {
        return None;
    }
    let mut unscaled: i128 = 0;
    for i in 0..digits {
        let digit = i128::from(word(4 + i)?);
        // The power of ten this digit's units stand at, unscaled.
        let at = (weight - i as i32) * 4 + i32::from(scale);
        let term = if at >= 0 {
            digit.checked_mul(10i128.checked_pow(at as u32)?)?
        } else {
            // Past the scale: zeros, but for those this digit drops.
            let unit = 10i128.checked_pow(at.unsigned_abs())?;
            if digit % unit != 0 {
                return None;
            }
            digit / unit
        };
        unscaled = unscaled.checked_add(term)?;
    }
    if negative {
        unscaled = -unscaled;
    }
    Some(CoreDecimal {
        unscaled_be_bytes: unscaled.to_be_bytes().to_vec(),
        scale: u8::try_from(scale).ok()?,
    })
}

/// The tuples of a binary `COPY`'s output, each a slice per column, of
/// its value's bytes, `None` if `NULL`.
pub(crate) struct Tuples<'a> {
    rest: &'a [u8],
    columns: usize,
}

/// A binary `COPY`'s signature, then its flags and its header
/// extension's length.
const SIGNATURE: &[u8] = b"PGCOPY\n\xff\r\n\0";

impl<'a> Tuples<'a> {
    /// The tuples of `copy`, of `columns` columns each.
    pub(crate) fn new(copy: &'a [u8], columns: usize) -> Result<Self, String> {
        let rest = copy
            .strip_prefix(SIGNATURE)
            .ok_or("not a binary COPY's output")?;
        let mut tuples = Self { rest, columns };
        let _flags = tuples.int()?;
        let extension = usize::try_from(tuples.int()?).map_err(|_| "a negative header")?;
        tuples.take(extension)?;
        Ok(tuples)
    }

    fn take(&mut self, n: usize) -> Result<&'a [u8], String> {
        if self.rest.len() < n {
            return Err(format!("COPY output cut short: {n} bytes past its end"));
        }
        let (taken, rest) = self.rest.split_at(n);
        self.rest = rest;
        Ok(taken)
    }

    fn int(&mut self) -> Result<i32, String> {
        Ok(i32::from_be_bytes(
            self.take(4)?.try_into().expect("4 bytes"),
        ))
    }

    /// The next tuple's values into `values`; false past the last.
    pub(crate) fn next_into(&mut self, values: &mut Vec<Option<&'a [u8]>>) -> Result<bool, String> {
        let count = i16::from_be_bytes(self.take(2)?.try_into().expect("2 bytes"));
        // The trailer.
        if count == -1 {
            return Ok(false);
        }
        if usize::try_from(count).ok() != Some(self.columns) {
            return Err(format!("a tuple of {count} values, not {}", self.columns));
        }
        values.clear();
        for _ in 0..self.columns {
            let len = self.int()?;
            values.push(match usize::try_from(len) {
                Ok(len) => Some(self.take(len)?),
                // -1: NULL.
                Err(_) => None,
            });
        }
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pg2iceberg_core::typemap::PgType;
    use pg2iceberg_pg::prod::value_decode::decode_text;

    /// `numeric`'s binary form of `text`, as Postgres sends it.
    fn binary_numeric(text: &str) -> Vec<u8> {
        let (negative, body) = match text.strip_prefix('-') {
            Some(body) => (true, body),
            None => (false, text),
        };
        let (int, frac) = body.split_once('.').unwrap_or((body, ""));
        let scale = frac.len();
        // Groups of four digits about the point: the integer part's padded
        // on the left, the fraction's on the right.
        let int = int.trim_start_matches('0');
        let pad = (4 - int.len() % 4) % 4;
        let int = format!("{}{int}", "0".repeat(pad));
        let frac = format!("{frac}{}", "0".repeat((4 - frac.len() % 4) % 4));
        let mut groups: Vec<u16> = int
            .as_bytes()
            .chunks(4)
            .chain(frac.as_bytes().chunks(4))
            .map(|g| std::str::from_utf8(g).unwrap().parse().unwrap())
            .collect();
        let mut weight = (int.len() / 4) as i16 - 1;
        // Leading and trailing zero groups aren't sent.
        while groups.first() == Some(&0) {
            groups.remove(0);
            weight -= 1;
        }
        while groups.last() == Some(&0) {
            groups.pop();
        }
        if groups.is_empty() {
            weight = 0;
        }
        let mut b = Vec::new();
        b.extend((groups.len() as u16).to_be_bytes());
        b.extend(weight.to_be_bytes());
        b.extend((if negative { 0x4000u16 } else { 0 }).to_be_bytes());
        b.extend((scale as u16).to_be_bytes());
        for g in groups {
            b.extend(g.to_be_bytes());
        }
        b
    }

    #[test]
    fn a_numeric_decodes_as_its_text_does() {
        for text in [
            "0",
            "0.00",
            "1",
            "-1",
            "12.30",
            "-12.30",
            "10000",
            "123456789.000001",
            "0.0001",
            "0.5",
            "-0.00012",
            "99999999999999999999999999999999999999",
            "1234567890123456789.123456789012345678",
        ] {
            let ty = IcebergType::Decimal {
                precision: 38,
                scale: 0,
            };
            assert_eq!(
                decode(ty, &binary_numeric(text)).unwrap(),
                decode_text(
                    PgType::Numeric {
                        precision: None,
                        scale: None
                    },
                    text.as_bytes()
                )
                .unwrap(),
                "{text}"
            );
        }
    }

    #[test]
    fn a_numeric_past_an_i128_or_not_a_number_is_none() {
        let ty = IcebergType::Decimal {
            precision: 38,
            scale: 0,
        };
        assert!(decode(ty, &binary_numeric(&"9".repeat(40))).is_err());
        let nan = [0u8, 0, 0, 0, 0xc0, 0, 0, 0];
        assert!(decode(ty, &nan).is_err());
    }

    #[test]
    fn times_count_from_postgres_epoch() {
        let ts = 1_000_000i64.to_be_bytes();
        assert_eq!(
            decode(IcebergType::TimestampTz, &ts).unwrap(),
            PgValue::TimestampTz(TimestampMicros(PG_EPOCH_MICROS + 1_000_000))
        );
        assert_eq!(
            decode(IcebergType::Date, &(-1i32).to_be_bytes()).unwrap(),
            PgValue::Date(DaysSinceEpoch(PG_EPOCH_DAYS - 1))
        );
        assert!(decode(IcebergType::Timestamp, &i64::MAX.to_be_bytes()).is_err());
        assert!(decode(IcebergType::Date, &i32::MIN.to_be_bytes()).is_err());
    }

    #[test]
    fn tuples_read_values_and_nulls_to_the_trailer() {
        let mut copy = SIGNATURE.to_vec();
        copy.extend(0i32.to_be_bytes());
        copy.extend(0i32.to_be_bytes());
        copy.extend(2i16.to_be_bytes());
        copy.extend(4i32.to_be_bytes());
        copy.extend(7i32.to_be_bytes());
        copy.extend((-1i32).to_be_bytes());
        copy.extend((-1i16).to_be_bytes());

        let mut tuples = Tuples::new(&copy, 2).unwrap();
        let mut values = Vec::new();
        assert!(tuples.next_into(&mut values).unwrap());
        assert_eq!(values, [Some(&7i32.to_be_bytes()[..]), None]);
        assert!(!tuples.next_into(&mut values).unwrap());
        assert!(Tuples::new(&copy[..copy.len() - 3], 2)
            .unwrap()
            .next_into(&mut values)
            .is_err());
    }
}
