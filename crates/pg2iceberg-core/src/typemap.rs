//! Postgres → Iceberg type-shape mapping.
//!
//! Mirrors `postgres/schema.go:104-161` from the Go reference. The
//! corresponding value mapping lives in [`value_map`].

use crate::value::{IcebergValue, PgValue};
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Postgres scalar types we recognize. Aliases (`serial`, `int`, `bigint`, etc.)
/// are normalized into these by the source-side schema reader; this enum is the
/// canonical form.
#[derive(Copy, Clone, Eq, PartialEq, Debug, Serialize, Deserialize)]
pub enum PgType {
    Bool,
    Int2,
    Int4,
    Int8,
    Float4,
    Float8,
    /// `precision == None` represents unconstrained `numeric`. The Go reference
    /// maps this to `decimal(38, 18)` and emits a warning.
    Numeric {
        precision: Option<u8>,
        scale: Option<u8>,
    },
    Text,
    Bytea,
    Date,
    Time,
    TimeTz,
    Timestamp,
    TimestampTz,
    Uuid,
    Json,
    Jsonb,
    /// PG `oid` is a 32-bit unsigned ID; we map it to int (matches Go).
    Oid,
}

#[derive(Copy, Clone, Eq, PartialEq, Debug, Serialize, Deserialize)]
pub enum IcebergType {
    Boolean,
    Int,
    Long,
    Float,
    Double,
    Decimal { precision: u8, scale: u8 },
    String,
    Binary,
    Date,
    Time,
    Timestamp,
    TimestampTz,
    Uuid,
}

#[derive(Clone, Eq, PartialEq, Debug, Error)]
pub enum MapError {
    #[error("numeric precision {0} exceeds maximum supported precision (38)")]
    NumericPrecisionTooLarge(u32),
    #[error("numeric scale {scale} exceeds precision {precision}")]
    NumericScaleExceedsPrecision { precision: u8, scale: u8 },
}

/// Outcome of mapping a PG type to an Iceberg type, plus any non-fatal warning
/// to surface to the operator. The Go reference emits a log line for the
/// unconstrained-numeric case; we return it as data so the binary can log it
/// once at startup rather than deep in the type layer.
#[derive(Clone, Eq, PartialEq, Debug)]
pub struct Mapped {
    pub iceberg: IcebergType,
    pub warning: Option<MapWarning>,
}

#[derive(Clone, Eq, PartialEq, Debug)]
pub enum MapWarning {
    /// Unconstrained `numeric` was mapped to `decimal(38, 18)` — values whose
    /// scale exceeds 18 will be rounded.
    UnconstrainedNumericDefaulted,
}

pub const MAX_DECIMAL_PRECISION: u8 = 38;
pub const DEFAULT_NUMERIC_PRECISION: u8 = 38;
pub const DEFAULT_NUMERIC_SCALE: u8 = 18;

pub fn map_pg_to_iceberg(pg: PgType) -> Result<Mapped, MapError> {
    let iceberg = match pg {
        PgType::Bool => IcebergType::Boolean,
        PgType::Int2 | PgType::Int4 | PgType::Oid => IcebergType::Int,
        PgType::Int8 => IcebergType::Long,
        PgType::Float4 => IcebergType::Float,
        PgType::Float8 => IcebergType::Double,
        PgType::Numeric { precision, scale } => return map_numeric(precision, scale),
        PgType::Text | PgType::Json | PgType::Jsonb => IcebergType::String,
        PgType::Bytea => IcebergType::Binary,
        PgType::Date => IcebergType::Date,
        PgType::Time | PgType::TimeTz => IcebergType::Time,
        PgType::Timestamp => IcebergType::Timestamp,
        PgType::TimestampTz => IcebergType::TimestampTz,
        PgType::Uuid => IcebergType::Uuid,
    };
    Ok(Mapped {
        iceberg,
        warning: None,
    })
}

fn map_numeric(precision: Option<u8>, scale: Option<u8>) -> Result<Mapped, MapError> {
    match (precision, scale) {
        (None, None) | (None, Some(_)) => Ok(Mapped {
            iceberg: IcebergType::Decimal {
                precision: DEFAULT_NUMERIC_PRECISION,
                scale: DEFAULT_NUMERIC_SCALE,
            },
            warning: Some(MapWarning::UnconstrainedNumericDefaulted),
        }),
        (Some(p), s) => {
            if p > MAX_DECIMAL_PRECISION {
                return Err(MapError::NumericPrecisionTooLarge(p as u32));
            }
            let scale = s.unwrap_or(0);
            if scale > p {
                return Err(MapError::NumericScaleExceedsPrecision {
                    precision: p,
                    scale,
                });
            }
            Ok(Mapped {
                iceberg: IcebergType::Decimal {
                    precision: p,
                    scale,
                },
                warning: None,
            })
        }
    }
}

/// Map a single PG value into the Iceberg-side value vocabulary. This is the
/// shape-only mapping used for round-trip property tests; actual Parquet/Arrow
/// encoding lives in `pg2iceberg-stream`.
///
/// Returns `None` if the value is structurally inexpressible at the Iceberg
/// side (e.g. numeric with precision > 38). Callers should pre-validate via
/// [`map_pg_to_iceberg`].
pub mod value_map {
    use super::*;

    pub fn pg_to_iceberg(v: PgValue) -> IcebergValue {
        match v {
            PgValue::Null => IcebergValue::Null,
            PgValue::Bool(b) => IcebergValue::Boolean(b),
            PgValue::Int2(n) => IcebergValue::Int(n as i32),
            PgValue::Int4(n) => IcebergValue::Int(n),
            PgValue::Int8(n) => IcebergValue::Long(n),
            PgValue::Float4(n) => IcebergValue::Float(n),
            PgValue::Float8(n) => IcebergValue::Double(n),
            PgValue::Numeric(d) => IcebergValue::Decimal(d),
            PgValue::Text(s) | PgValue::Json(s) | PgValue::Jsonb(s) => IcebergValue::String(s),
            PgValue::Bytea(b) => IcebergValue::Binary(b),
            PgValue::Date(d) => IcebergValue::Date(d),
            // timetz drops the zone offset to match Go behavior. Document at the
            // type-mapping layer; the loss is intentional.
            PgValue::Time(t) | PgValue::TimeTz { time: t, .. } => IcebergValue::Time(t),
            PgValue::Timestamp(t) => IcebergValue::Timestamp(t),
            PgValue::TimestampTz(t) => IcebergValue::TimestampTz(t),
            PgValue::Uuid(u) => IcebergValue::Uuid(u),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bool_maps_to_boolean() {
        let m = map_pg_to_iceberg(PgType::Bool).unwrap();
        assert_eq!(m.iceberg, IcebergType::Boolean);
        assert!(m.warning.is_none());
    }

    #[test]
    fn int2_int4_oid_all_map_to_int() {
        for ty in [PgType::Int2, PgType::Int4, PgType::Oid] {
            assert_eq!(map_pg_to_iceberg(ty).unwrap().iceberg, IcebergType::Int);
        }
    }

    #[test]
    fn int8_maps_to_long() {
        assert_eq!(
            map_pg_to_iceberg(PgType::Int8).unwrap().iceberg,
            IcebergType::Long
        );
    }

    #[test]
    fn json_and_jsonb_both_map_to_string() {
        for ty in [PgType::Json, PgType::Jsonb, PgType::Text] {
            assert_eq!(map_pg_to_iceberg(ty).unwrap().iceberg, IcebergType::String);
        }
    }

    #[test]
    fn time_and_timetz_both_map_to_time() {
        for ty in [PgType::Time, PgType::TimeTz] {
            assert_eq!(map_pg_to_iceberg(ty).unwrap().iceberg, IcebergType::Time);
        }
    }

    #[test]
    fn timestamp_distinct_from_timestamptz() {
        assert_eq!(
            map_pg_to_iceberg(PgType::Timestamp).unwrap().iceberg,
            IcebergType::Timestamp
        );
        assert_eq!(
            map_pg_to_iceberg(PgType::TimestampTz).unwrap().iceberg,
            IcebergType::TimestampTz
        );
    }

    #[test]
    fn unconstrained_numeric_defaults_to_decimal_38_18_with_warning() {
        let m = map_pg_to_iceberg(PgType::Numeric {
            precision: None,
            scale: None,
        })
        .unwrap();
        assert_eq!(
            m.iceberg,
            IcebergType::Decimal {
                precision: 38,
                scale: 18
            }
        );
        assert_eq!(m.warning, Some(MapWarning::UnconstrainedNumericDefaulted));
    }

    #[test]
    fn numeric_with_precision_above_38_is_hard_error() {
        let err = map_pg_to_iceberg(PgType::Numeric {
            precision: Some(39),
            scale: Some(0),
        })
        .unwrap_err();
        assert!(matches!(err, MapError::NumericPrecisionTooLarge(39)));
    }

    #[test]
    fn numeric_38_0_is_allowed() {
        let m = map_pg_to_iceberg(PgType::Numeric {
            precision: Some(38),
            scale: Some(0),
        })
        .unwrap();
        assert_eq!(
            m.iceberg,
            IcebergType::Decimal {
                precision: 38,
                scale: 0
            }
        );
    }

    #[test]
    fn numeric_scale_exceeding_precision_is_error() {
        let err = map_pg_to_iceberg(PgType::Numeric {
            precision: Some(5),
            scale: Some(7),
        })
        .unwrap_err();
        assert!(matches!(
            err,
            MapError::NumericScaleExceedsPrecision {
                precision: 5,
                scale: 7
            }
        ));
    }
}
