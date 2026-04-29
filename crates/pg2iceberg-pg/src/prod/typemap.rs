//! Postgres OID + type-modifier → [`pg2iceberg_core::PgType`].
//!
//! Mirrors `postgres/schema.go:38-88` (the `ParseType` table) and
//! `postgres/schema.go:104-138` (numeric typmod handling). Unknown OIDs
//! are reported as `None`; the caller decides whether to fail or fall
//! back to `Text` (we choose the latter at the source-side, matching
//! Go's behavior for forward-compat with new PG types).

use pg2iceberg_core::typemap::PgType;
use postgres_types::Type;

/// Decode the `(precision, scale)` pair carried by `pg_attribute.atttypmod`
/// for `numeric` columns. The wire format is documented at
/// <https://www.postgresql.org/docs/current/datatype-numeric.html>:
///
/// - `typmod < 0` (typically `-1`): unspecified; precision/scale not
///   declared by DDL. We surface this as `None`/`None` and let the
///   Iceberg side default to `decimal(38, 18)` with a warning.
/// - `typmod ≥ 0`: encodes `((precision << 16) | scale) + VARHDRSZ`,
///   where `VARHDRSZ = 4`.
fn parse_numeric_typmod(type_modifier: i32) -> (Option<u8>, Option<u8>) {
    if type_modifier < 0 {
        return (None, None);
    }
    let raw = (type_modifier - 4) as u32;
    let precision = ((raw >> 16) & 0xffff) as u16;
    let scale = (raw & 0xffff) as u16;
    // Cap to u8 to match `pg2iceberg_core::PgType::Numeric`'s field width.
    // Iceberg's max precision is 38; PG allows up to 1000 in theory but in
    // practice precision >38 is rejected at type-mapping time anyway. If
    // either field overflows u8, we surface as `None` so the validator
    // catches it rather than silently truncating here.
    let precision = u8::try_from(precision).ok();
    let scale = u8::try_from(scale).ok();
    (precision, scale)
}

/// Map a Postgres OID + type-modifier to our [`PgType`]. Returns `None`
/// for OIDs we don't model (callers usually fall back to `PgType::Text`,
/// matching the Go reference).
pub fn pg_type_from_oid(oid: u32, type_modifier: i32) -> Option<PgType> {
    Some(match oid {
        // Booleans + integers.
        x if x == Type::BOOL.oid() => PgType::Bool,
        x if x == Type::INT2.oid() => PgType::Int2,
        x if x == Type::INT4.oid() => PgType::Int4,
        x if x == Type::INT8.oid() => PgType::Int8,
        x if x == Type::OID.oid() => PgType::Oid,

        // Floats + numeric.
        x if x == Type::FLOAT4.oid() => PgType::Float4,
        x if x == Type::FLOAT8.oid() => PgType::Float8,
        x if x == Type::NUMERIC.oid() => {
            let (precision, scale) = parse_numeric_typmod(type_modifier);
            PgType::Numeric { precision, scale }
        }

        // String-like — varchar/bpchar/name all collapse to Text per the
        // Go reference (postgres/schema.go:120). Iceberg has no
        // fixed-length character type and the distinction doesn't
        // survive replication anyway.
        x if x == Type::TEXT.oid()
            || x == Type::VARCHAR.oid()
            || x == Type::BPCHAR.oid()
            || x == Type::NAME.oid() =>
        {
            PgType::Text
        }

        x if x == Type::BYTEA.oid() => PgType::Bytea,

        // Date / time — note the timetz / time distinction is preserved
        // here even though `value_map::pg_to_iceberg` collapses both to
        // Iceberg `Time` (with the zone offset deliberately dropped).
        x if x == Type::DATE.oid() => PgType::Date,
        x if x == Type::TIME.oid() => PgType::Time,
        x if x == Type::TIMETZ.oid() => PgType::TimeTz,
        x if x == Type::TIMESTAMP.oid() => PgType::Timestamp,
        x if x == Type::TIMESTAMPTZ.oid() => PgType::TimestampTz,

        x if x == Type::UUID.oid() => PgType::Uuid,

        // JSON / JSONB. Both collapse to string in Iceberg via the
        // forward type mapper; we keep them distinct here so the value
        // decoder can pick the right wire format.
        x if x == Type::JSON.oid() => PgType::Json,
        x if x == Type::JSONB.oid() => PgType::Jsonb,

        _ => return None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn well_known_oids_map_correctly() {
        assert_eq!(pg_type_from_oid(Type::BOOL.oid(), -1), Some(PgType::Bool));
        assert_eq!(pg_type_from_oid(Type::INT2.oid(), -1), Some(PgType::Int2));
        assert_eq!(pg_type_from_oid(Type::INT4.oid(), -1), Some(PgType::Int4));
        assert_eq!(pg_type_from_oid(Type::INT8.oid(), -1), Some(PgType::Int8));
        assert_eq!(pg_type_from_oid(Type::OID.oid(), -1), Some(PgType::Oid));
        assert_eq!(
            pg_type_from_oid(Type::FLOAT4.oid(), -1),
            Some(PgType::Float4)
        );
        assert_eq!(
            pg_type_from_oid(Type::FLOAT8.oid(), -1),
            Some(PgType::Float8)
        );
        assert_eq!(pg_type_from_oid(Type::TEXT.oid(), -1), Some(PgType::Text));
        assert_eq!(
            pg_type_from_oid(Type::VARCHAR.oid(), -1),
            Some(PgType::Text)
        );
        assert_eq!(pg_type_from_oid(Type::BPCHAR.oid(), -1), Some(PgType::Text));
        assert_eq!(pg_type_from_oid(Type::NAME.oid(), -1), Some(PgType::Text));
        assert_eq!(pg_type_from_oid(Type::BYTEA.oid(), -1), Some(PgType::Bytea));
        assert_eq!(pg_type_from_oid(Type::DATE.oid(), -1), Some(PgType::Date));
        assert_eq!(pg_type_from_oid(Type::TIME.oid(), -1), Some(PgType::Time));
        assert_eq!(
            pg_type_from_oid(Type::TIMETZ.oid(), -1),
            Some(PgType::TimeTz)
        );
        assert_eq!(
            pg_type_from_oid(Type::TIMESTAMP.oid(), -1),
            Some(PgType::Timestamp)
        );
        assert_eq!(
            pg_type_from_oid(Type::TIMESTAMPTZ.oid(), -1),
            Some(PgType::TimestampTz)
        );
        assert_eq!(pg_type_from_oid(Type::UUID.oid(), -1), Some(PgType::Uuid));
        assert_eq!(pg_type_from_oid(Type::JSON.oid(), -1), Some(PgType::Json));
        assert_eq!(pg_type_from_oid(Type::JSONB.oid(), -1), Some(PgType::Jsonb));
    }

    #[test]
    fn unknown_oid_returns_none() {
        assert_eq!(pg_type_from_oid(99999, -1), None);
    }

    #[test]
    fn numeric_with_unspecified_typmod_returns_none_precision() {
        let pg = pg_type_from_oid(Type::NUMERIC.oid(), -1).unwrap();
        assert_eq!(
            pg,
            PgType::Numeric {
                precision: None,
                scale: None
            }
        );
    }

    #[test]
    fn numeric_typmod_decodes_precision_and_scale() {
        // numeric(10, 2) → typmod = ((10 << 16) | 2) + 4 = 655_366.
        let typmod = ((10_i32) << 16) | 2_i32;
        let pg = pg_type_from_oid(Type::NUMERIC.oid(), typmod + 4).unwrap();
        assert_eq!(
            pg,
            PgType::Numeric {
                precision: Some(10),
                scale: Some(2),
            }
        );
    }

    #[test]
    fn numeric_with_zero_scale_decodes() {
        // numeric(38, 0) — common shape for integer-replacement decimals.
        let typmod = (38_i32) << 16;
        let pg = pg_type_from_oid(Type::NUMERIC.oid(), typmod + 4).unwrap();
        assert_eq!(
            pg,
            PgType::Numeric {
                precision: Some(38),
                scale: Some(0),
            }
        );
    }
}
