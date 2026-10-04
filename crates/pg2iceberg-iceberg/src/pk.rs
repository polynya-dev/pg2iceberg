//! Compact primary-key identity for materialization.
//!
//! [`PkKey`] identifies a row by its primary-key values as Iceberg stores
//! them, so a key built from a decoded WAL event equals the key built
//! from the same row read back out of a data file. The two sides carry
//! different [`PgValue`] variants for the same stored value — `smallint`
//! arrives as `Int2` but reads back as `Int4`, `jsonb` arrives as
//! `Jsonb` but reads back as `Text`, a numeric arrives at its own scale
//! but reads back at the column's — so the key encodes the value, not
//! the variant.
//!
//! It is also small: the encoded values are stored inline up to
//! [`INLINE`] bytes (an integer, a UUID, a short string), which is what
//! keeps the materializer's [`crate::FileIndex`] — one key per live row
//! — compact.
//!
//! [`crate::pk_key`] is a different, JSON key: a stable text form used
//! where a key is persisted or compared as text (snapshot resume
//! cursors, `verify` paging).

use pg2iceberg_core::value::{DaysSinceEpoch, Decimal, TimeMicros, TimestampMicros};
use pg2iceberg_core::{ColumnName, ColumnSchema, IcebergType, PgValue, Row};
use std::cmp::Ordering;
use std::fmt;
use std::hash::{Hash, Hasher};

/// Encoded keys up to this many bytes are stored inline.
pub const INLINE: usize = 22;

/// A row's primary-key values, canonically encoded. See the module docs.
#[derive(Clone)]
pub struct PkKey(Repr);

#[derive(Clone)]
enum Repr {
    Inline { len: u8, buf: [u8; INLINE] },
    Heap(Box<[u8]>),
}

const _: () = assert!(std::mem::size_of::<PkKey>() == 24);

// One tag byte per value, then its payload.
const NULL: u8 = 0;
const BOOL: u8 = 1;
/// `Int2`/`Int4`/`Int8`, as an `i64`.
const INT: u8 = 2;
/// `Float4`/`Float8`, as the bits of an `f64`.
const FLOAT: u8 = 3;
/// A numeric that fits an `i128` unscaled: trailing zeros stripped, then
/// the 16-byte unscaled value and the scale.
const DECIMAL: u8 = 4;
/// A numeric too wide for `i128`: scale, length, minimal unscaled bytes.
const WIDE_DECIMAL: u8 = 5;
/// `Text`/`Json`/`Jsonb`: length, UTF-8 bytes.
const STRING: u8 = 6;
const BINARY: u8 = 7;
const DATE: u8 = 8;
const TIME: u8 = 9;
const TIME_TZ: u8 = 10;
const TIMESTAMP: u8 = 11;
const TIMESTAMP_TZ: u8 = 12;
const UUID: u8 = 13;

impl PkKey {
    /// The key of `row`'s primary key. Like [`crate::pk_key`], PK columns
    /// missing from the row are skipped.
    pub fn from_row(row: &Row, pk_cols: &[ColumnName]) -> Self {
        let mut buf = Vec::with_capacity(INLINE);
        for col in pk_cols {
            if let Some(v) = row.get(col) {
                encode(v, &mut buf);
            }
        }
        Self::from_bytes(&buf)
    }

    fn from_bytes(bytes: &[u8]) -> Self {
        if bytes.len() <= INLINE {
            let mut buf = [0u8; INLINE];
            buf[..bytes.len()].copy_from_slice(bytes);
            Self(Repr::Inline {
                len: bytes.len() as u8,
                buf,
            })
        } else {
            Self(Repr::Heap(bytes.into()))
        }
    }

    pub fn as_bytes(&self) -> &[u8] {
        match &self.0 {
            Repr::Inline { len, buf } => &buf[..*len as usize],
            Repr::Heap(b) => b,
        }
    }

    /// The PK-only row this key was built from, with each value in the
    /// variant a data-file read of `pk_cols` produces. `None` if the key
    /// doesn't hold one value per column, or a value doesn't fit its
    /// column's type.
    pub fn to_row(&self, pk_cols: &[ColumnSchema]) -> Option<Row> {
        let mut values = Values(self.as_bytes());
        let mut row = Row::new();
        for col in pk_cols {
            let v = values.next()??;
            row.insert(ColumnName(col.name.clone()), as_column_type(v, col.ty)?);
        }
        values.0.is_empty().then_some(row)
    }
}

impl PartialEq for PkKey {
    fn eq(&self, other: &Self) -> bool {
        self.as_bytes() == other.as_bytes()
    }
}

impl Eq for PkKey {}

impl Hash for PkKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.as_bytes().hash(state);
    }
}

impl PartialOrd for PkKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for PkKey {
    fn cmp(&self, other: &Self) -> Ordering {
        self.as_bytes().cmp(other.as_bytes())
    }
}

/// The key's values, comma-separated: `42`, `"abc"`, `1.50`.
impl fmt::Display for PkKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (i, v) in Values(self.as_bytes()).enumerate() {
            if i > 0 {
                f.write_str(", ")?;
            }
            match v {
                Some(v) => write_value(f, &v)?,
                None => f.write_str("<malformed>")?,
            }
        }
        Ok(())
    }
}

impl fmt::Debug for PkKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "PkKey({self})")
    }
}

fn encode(v: &PgValue, out: &mut Vec<u8>) {
    match v {
        PgValue::Null => out.push(NULL),
        PgValue::Bool(b) => out.extend([BOOL, *b as u8]),
        PgValue::Int2(n) => encode_int(i64::from(*n), out),
        PgValue::Int4(n) => encode_int(i64::from(*n), out),
        PgValue::Int8(n) => encode_int(*n, out),
        PgValue::Float4(x) => encode_float(f64::from(*x), out),
        PgValue::Float8(x) => encode_float(*x, out),
        PgValue::Numeric(d) => encode_decimal(d, out),
        PgValue::Text(s) | PgValue::Json(s) | PgValue::Jsonb(s) => {
            out.push(STRING);
            encode_len_prefixed(s.as_bytes(), out);
        }
        PgValue::Bytea(b) => {
            out.push(BINARY);
            encode_len_prefixed(b, out);
        }
        PgValue::Date(d) => {
            out.push(DATE);
            out.extend(d.0.to_be_bytes());
        }
        PgValue::Time(t) => {
            out.push(TIME);
            out.extend(t.0.to_be_bytes());
        }
        PgValue::TimeTz { time, zone_secs } => {
            out.push(TIME_TZ);
            out.extend(time.0.to_be_bytes());
            out.extend(zone_secs.to_be_bytes());
        }
        PgValue::Timestamp(t) => {
            out.push(TIMESTAMP);
            out.extend(t.0.to_be_bytes());
        }
        PgValue::TimestampTz(t) => {
            out.push(TIMESTAMP_TZ);
            out.extend(t.0.to_be_bytes());
        }
        PgValue::Uuid(u) => {
            out.push(UUID);
            out.extend(u);
        }
    }
}

fn encode_int(n: i64, out: &mut Vec<u8>) {
    out.push(INT);
    out.extend(n.to_be_bytes());
}

fn encode_float(x: f64, out: &mut Vec<u8>) {
    out.push(FLOAT);
    out.extend(x.to_bits().to_be_bytes());
}

/// Numerically canonical: `1.5` at scale 1 and `1.50` at scale 2 are one
/// key, as are differently padded unscaled bytes.
fn encode_decimal(d: &Decimal, out: &mut Vec<u8>) {
    let bytes = d.normalized_be_bytes();
    match i128_from_be(bytes) {
        Some(mut unscaled) => {
            let mut scale = d.scale;
            while scale > 0 && unscaled % 10 == 0 {
                unscaled /= 10;
                scale -= 1;
            }
            out.push(DECIMAL);
            out.extend(unscaled.to_be_bytes());
            out.push(scale);
        }
        None => {
            out.extend([WIDE_DECIMAL, d.scale]);
            encode_len_prefixed(bytes, out);
        }
    }
}

/// Sign-extend minimal two's-complement big-endian bytes to an `i128`.
fn i128_from_be(bytes: &[u8]) -> Option<i128> {
    if bytes.len() > 16 {
        return None;
    }
    let negative = bytes.first().is_some_and(|b| b & 0x80 != 0);
    let mut buf = if negative { [0xFF; 16] } else { [0; 16] };
    buf[16 - bytes.len()..].copy_from_slice(bytes);
    Some(i128::from_be_bytes(buf))
}

fn encode_len_prefixed(bytes: &[u8], out: &mut Vec<u8>) {
    // LEB128 length.
    let mut n = bytes.len();
    loop {
        let byte = (n & 0x7F) as u8;
        n >>= 7;
        if n == 0 {
            out.push(byte);
            break;
        }
        out.push(byte | 0x80);
    }
    out.extend(bytes);
}

/// A decoded key value, before it is fit to a column type.
enum Value {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    Decimal { unscaled: i128, scale: u8 },
    WideDecimal(Decimal),
    String(String),
    Binary(Vec<u8>),
    Date(i32),
    Time(i64),
    TimeTz(i64, i32),
    Timestamp(i64),
    TimestampTz(i64),
    Uuid([u8; 16]),
}

/// Iterates the values of an encoded key; yields `None` once on a
/// malformed value, then stops.
struct Values<'a>(&'a [u8]);

impl Iterator for Values<'_> {
    type Item = Option<Value>;

    fn next(&mut self) -> Option<Self::Item> {
        let (&tag, rest) = self.0.split_first()?;
        self.0 = rest;
        let v = self.decode(tag);
        if v.is_none() {
            self.0 = &[];
        }
        Some(v)
    }
}

impl Values<'_> {
    fn take<const N: usize>(&mut self) -> Option<[u8; N]> {
        let (head, rest) = self.0.split_first_chunk::<N>()?;
        self.0 = rest;
        Some(*head)
    }

    fn take_len_prefixed(&mut self) -> Option<Vec<u8>> {
        let mut len = 0usize;
        let mut shift = 0;
        loop {
            let [byte] = self.take::<1>()?;
            len |= usize::from(byte & 0x7F).checked_shl(shift)?;
            if byte & 0x80 == 0 {
                break;
            }
            shift += 7;
        }
        if len > self.0.len() {
            return None;
        }
        let (head, rest) = self.0.split_at(len);
        self.0 = rest;
        Some(head.to_vec())
    }

    fn decode(&mut self, tag: u8) -> Option<Value> {
        Some(match tag {
            NULL => Value::Null,
            BOOL => Value::Bool(self.take::<1>()?[0] != 0),
            INT => Value::Int(i64::from_be_bytes(self.take()?)),
            FLOAT => Value::Float(f64::from_bits(u64::from_be_bytes(self.take()?))),
            DECIMAL => {
                let unscaled = i128::from_be_bytes(self.take()?);
                let [scale] = self.take()?;
                Value::Decimal { unscaled, scale }
            }
            WIDE_DECIMAL => {
                let [scale] = self.take()?;
                Value::WideDecimal(Decimal {
                    unscaled_be_bytes: self.take_len_prefixed()?,
                    scale,
                })
            }
            STRING => Value::String(String::from_utf8(self.take_len_prefixed()?).ok()?),
            BINARY => Value::Binary(self.take_len_prefixed()?),
            DATE => Value::Date(i32::from_be_bytes(self.take()?)),
            TIME => Value::Time(i64::from_be_bytes(self.take()?)),
            TIME_TZ => {
                let time = i64::from_be_bytes(self.take()?);
                Value::TimeTz(time, i32::from_be_bytes(self.take()?))
            }
            TIMESTAMP => Value::Timestamp(i64::from_be_bytes(self.take()?)),
            TIMESTAMP_TZ => Value::TimestampTz(i64::from_be_bytes(self.take()?)),
            UUID => Value::Uuid(self.take()?),
            _ => return None,
        })
    }
}

/// `v` as the variant [`crate::reader`] produces for a column of `ty`.
fn as_column_type(v: Value, ty: IcebergType) -> Option<PgValue> {
    Some(match (v, ty) {
        (Value::Null, _) => PgValue::Null,
        (Value::Bool(b), IcebergType::Boolean) => PgValue::Bool(b),
        (Value::Int(n), IcebergType::Int) => PgValue::Int4(n.try_into().ok()?),
        (Value::Int(n), IcebergType::Long) => PgValue::Int8(n),
        (Value::Float(x), IcebergType::Float) => PgValue::Float4(x as f32),
        (Value::Float(x), IcebergType::Double) => PgValue::Float8(x),
        (Value::Decimal { unscaled, scale }, IcebergType::Decimal { scale: col, .. }) => {
            // Back to the column's scale, as stored.
            let unscaled =
                unscaled.checked_mul(10i128.checked_pow(u32::from(col.checked_sub(scale)?))?)?;
            PgValue::Numeric(Decimal {
                unscaled_be_bytes: unscaled.to_be_bytes().to_vec(),
                scale: col,
            })
        }
        (Value::WideDecimal(d), IcebergType::Decimal { .. }) => PgValue::Numeric(d),
        (Value::String(s), IcebergType::String) => PgValue::Text(s),
        (Value::Binary(b), IcebergType::Binary) => PgValue::Bytea(b),
        (Value::Date(d), IcebergType::Date) => PgValue::Date(DaysSinceEpoch(d)),
        (Value::Time(t), IcebergType::Time) => PgValue::Time(TimeMicros(t)),
        (Value::TimeTz(t, zone_secs), IcebergType::Time) => PgValue::TimeTz {
            time: TimeMicros(t),
            zone_secs,
        },
        (Value::Timestamp(t), IcebergType::Timestamp) => PgValue::Timestamp(TimestampMicros(t)),
        (Value::TimestampTz(t), IcebergType::TimestampTz) => {
            PgValue::TimestampTz(TimestampMicros(t))
        }
        (Value::Uuid(u), IcebergType::Uuid) => PgValue::Uuid(u),
        _ => return None,
    })
}

fn write_value(f: &mut fmt::Formatter<'_>, v: &Value) -> fmt::Result {
    match v {
        Value::Null => f.write_str("null"),
        Value::Bool(b) => write!(f, "{b}"),
        Value::Int(n) => write!(f, "{n}"),
        Value::Float(x) => write!(f, "{x}"),
        Value::Decimal { unscaled, scale } => write!(f, "{unscaled}e-{scale}"),
        Value::WideDecimal(d) => write!(f, "0x{}e-{}", hex(&d.unscaled_be_bytes), d.scale),
        Value::String(s) => write!(f, "{s:?}"),
        Value::Binary(b) => write!(f, "0x{}", hex(b)),
        Value::Date(d) => write!(f, "date {d}"),
        Value::Time(t) => write!(f, "time {t}"),
        Value::TimeTz(t, z) => write!(f, "timetz {t}{z:+}"),
        Value::Timestamp(t) => write!(f, "timestamp {t}"),
        Value::TimestampTz(t) => write!(f, "timestamptz {t}"),
        Value::Uuid(u) => write!(f, "uuid {}", hex(u)),
    }
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    fn key(values: &[PgValue]) -> PkKey {
        let cols: Vec<ColumnName> = (0..values.len())
            .map(|i| ColumnName(format!("c{i}")))
            .collect();
        let row: Row = cols.iter().cloned().zip(values.iter().cloned()).collect();
        PkKey::from_row(&row, &cols)
    }

    fn col(i: usize, ty: IcebergType) -> ColumnSchema {
        ColumnSchema {
            name: format!("c{i}"),
            field_id: i as i32 + 1,
            ty,
            nullable: false,
            is_primary_key: true,
        }
    }

    fn decimal(unscaled: i128, scale: u8, width: usize) -> PgValue {
        PgValue::Numeric(Decimal {
            unscaled_be_bytes: unscaled.to_be_bytes()[16 - width..].to_vec(),
            scale,
        })
    }

    #[test]
    fn a_value_has_one_key_whichever_variant_carries_it() {
        // What a WAL event carries vs. what a data-file read returns.
        assert_eq!(key(&[PgValue::Int2(7)]), key(&[PgValue::Int4(7)]));
        assert_eq!(key(&[PgValue::Int4(7)]), key(&[PgValue::Int8(7)]));
        assert_eq!(
            key(&[PgValue::Jsonb("{}".into())]),
            key(&[PgValue::Text("{}".into())])
        );
        assert_eq!(key(&[PgValue::Float4(1.5)]), key(&[PgValue::Float8(1.5)]));
        // PG pads the unscaled bytes and keeps the value's own scale;
        // a data file holds minimal bytes at the column's scale.
        assert_eq!(key(&[decimal(-15, 1, 16)]), key(&[decimal(-150, 2, 2)]));
        assert_eq!(key(&[decimal(0, 3, 16)]), key(&[decimal(0, 0, 1)]));
    }

    #[test]
    fn different_values_have_different_keys() {
        assert_ne!(key(&[PgValue::Int4(1)]), key(&[PgValue::Int4(2)]));
        assert_ne!(key(&[PgValue::Int4(1)]), key(&[PgValue::Text("1".into())]));
        assert_ne!(key(&[decimal(15, 1, 2)]), key(&[decimal(15, 2, 2)]));
        assert_ne!(
            key(&[PgValue::Timestamp(TimestampMicros(1))]),
            key(&[PgValue::TimestampTz(TimestampMicros(1))])
        );
        // Composite keys don't run together.
        assert_ne!(
            key(&[PgValue::Text("ab".into()), PgValue::Text("c".into())]),
            key(&[PgValue::Text("a".into()), PgValue::Text("bc".into())])
        );
    }

    #[test]
    fn common_keys_are_stored_inline() {
        for k in [
            key(&[PgValue::Int8(i64::MIN)]),
            key(&[PgValue::Uuid([0xAB; 16])]),
            key(&[PgValue::Int4(1), PgValue::Int4(2)]),
            key(&[PgValue::Text("x".repeat(INLINE - 2))]),
        ] {
            assert!(
                matches!(k.0, Repr::Inline { .. }),
                "{k:?} spilled to the heap"
            );
        }
        let long = key(&[PgValue::Text("x".repeat(100))]);
        assert!(matches!(long.0, Repr::Heap(_)));
        assert_eq!(long, long.clone());
    }

    #[test]
    fn to_row_returns_what_a_data_file_read_returns() {
        let cases: Vec<(PgValue, IcebergType, PgValue)> = vec![
            (PgValue::Int2(-3), IcebergType::Int, PgValue::Int4(-3)),
            (PgValue::Int4(5), IcebergType::Long, PgValue::Int8(5)),
            (
                PgValue::Jsonb("{}".into()),
                IcebergType::String,
                PgValue::Text("{}".into()),
            ),
            (
                PgValue::Float4(2.5),
                IcebergType::Float,
                PgValue::Float4(2.5),
            ),
            (
                PgValue::Bytea(vec![0, 1, 2]),
                IcebergType::Binary,
                PgValue::Bytea(vec![0, 1, 2]),
            ),
            (
                PgValue::Uuid([7; 16]),
                IcebergType::Uuid,
                PgValue::Uuid([7; 16]),
            ),
            (
                PgValue::Date(DaysSinceEpoch(-1)),
                IcebergType::Date,
                PgValue::Date(DaysSinceEpoch(-1)),
            ),
            (
                PgValue::TimestampTz(TimestampMicros(9)),
                IcebergType::TimestampTz,
                PgValue::TimestampTz(TimestampMicros(9)),
            ),
            (
                PgValue::Text("é".repeat(40)),
                IcebergType::String,
                PgValue::Text("é".repeat(40)),
            ),
        ];
        for (from_wal, ty, from_file) in cases {
            let row = key(std::slice::from_ref(&from_wal))
                .to_row(&[col(0, ty)])
                .unwrap();
            assert_eq!(
                row[&ColumnName("c0".into())],
                from_file,
                "{from_wal:?} as {ty:?}"
            );
        }
        // Decimals come back at the column's scale.
        let ty = IcebergType::Decimal {
            precision: 10,
            scale: 2,
        };
        let row = key(&[decimal(-15, 1, 16)]).to_row(&[col(0, ty)]).unwrap();
        assert_eq!(row[&ColumnName("c0".into())], decimal(-150, 2, 16));
    }

    #[test]
    fn to_row_rejects_keys_that_dont_fit_the_columns() {
        let k = key(&[PgValue::Int8(i64::MAX)]);
        assert!(k.to_row(&[col(0, IcebergType::Int)]).is_none());
        assert!(k.to_row(&[col(0, IcebergType::String)]).is_none());
        // Too few or too many values.
        assert!(k
            .to_row(&[col(0, IcebergType::Long), col(1, IcebergType::Long)])
            .is_none());
        assert!(key(&[PgValue::Int8(1), PgValue::Int8(2)])
            .to_row(&[col(0, IcebergType::Long)])
            .is_none());
        // Truncated bytes.
        let bytes = k.as_bytes();
        assert!(PkKey::from_bytes(&bytes[..bytes.len() - 1])
            .to_row(&[col(0, IcebergType::Long)])
            .is_none());
    }

    #[test]
    fn display_shows_the_values() {
        let k = key(&[PgValue::Int4(99), PgValue::Text("a".into())]);
        assert_eq!(k.to_string(), r#"99, "a""#);
    }

    #[test]
    fn missing_pk_columns_are_skipped_like_pk_key() {
        let row: Row = BTreeMap::from([(ColumnName("a".into()), PgValue::Int4(1))]);
        let with_missing = PkKey::from_row(&row, &[ColumnName("a".into()), ColumnName("b".into())]);
        assert_eq!(with_missing, key(&[PgValue::Int4(1)]));
    }
}
