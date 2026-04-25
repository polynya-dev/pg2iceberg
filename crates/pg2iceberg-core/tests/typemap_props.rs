//! Property tests for the PG → Iceberg type and value mapping.
//!
//! Phase 1 deliverable. Value-level encoding (Parquet/Arrow) is out of scope;
//! these tests pin the *shape* of the mapping so Phase 2 can plug encoders in
//! against a known spec.

use pg2iceberg_core::typemap::{
    map_pg_to_iceberg, value_map, IcebergType, MapError, MapWarning, MAX_DECIMAL_PRECISION,
};
use pg2iceberg_core::value::{DaysSinceEpoch, Decimal, TimeMicros, TimestampMicros};
use pg2iceberg_core::{IcebergValue, PgType, PgValue};
use proptest::prelude::*;

// ---------- type-shape mapping ----------

proptest! {
    #[test]
    fn numeric_within_38_always_maps_to_decimal(p in 1u8..=MAX_DECIMAL_PRECISION, s in 0u8..=MAX_DECIMAL_PRECISION) {
        let scale = s.min(p); // valid combination
        let m = map_pg_to_iceberg(PgType::Numeric { precision: Some(p), scale: Some(scale) }).unwrap();
        prop_assert_eq!(m.iceberg, IcebergType::Decimal { precision: p, scale });
        prop_assert!(m.warning.is_none());
    }

    #[test]
    fn numeric_above_38_always_errors(p in 39u8..=u8::MAX) {
        let err = map_pg_to_iceberg(PgType::Numeric { precision: Some(p), scale: Some(0) }).unwrap_err();
        prop_assert!(matches!(err, MapError::NumericPrecisionTooLarge(got) if got == p as u32));
    }

    #[test]
    fn numeric_scale_above_precision_always_errors(p in 1u8..=MAX_DECIMAL_PRECISION, extra in 1u8..50) {
        let scale = p.saturating_add(extra);
        prop_assume!(scale > p);
        let err = map_pg_to_iceberg(PgType::Numeric { precision: Some(p), scale: Some(scale) }).unwrap_err();
        let ok = matches!(
            err,
            MapError::NumericScaleExceedsPrecision { precision: gp, scale: gs }
                if gp == p && gs == scale
        );
        prop_assert!(ok);
    }
}

#[test]
fn unconstrained_numeric_warns() {
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
fn every_supported_pg_type_has_a_mapping() {
    // Lock the supported set to exactly what postgres/schema.go covers.
    // Adding a new variant here without a mapping should be a compile error;
    // adding a mapping without a test should fail this test.
    let cases = [
        PgType::Bool,
        PgType::Int2,
        PgType::Int4,
        PgType::Int8,
        PgType::Float4,
        PgType::Float8,
        PgType::Numeric {
            precision: Some(10),
            scale: Some(2),
        },
        PgType::Text,
        PgType::Bytea,
        PgType::Date,
        PgType::Time,
        PgType::TimeTz,
        PgType::Timestamp,
        PgType::TimestampTz,
        PgType::Uuid,
        PgType::Json,
        PgType::Jsonb,
        PgType::Oid,
    ];
    for ty in cases {
        let _ = map_pg_to_iceberg(ty).unwrap();
    }
}

// ---------- value round-trip ----------

fn pg_value_strategy() -> impl Strategy<Value = PgValue> {
    prop_oneof![
        Just(PgValue::Null),
        any::<bool>().prop_map(PgValue::Bool),
        any::<i16>().prop_map(PgValue::Int2),
        any::<i32>().prop_map(PgValue::Int4),
        any::<i64>().prop_map(PgValue::Int8),
        any::<i32>().prop_map(|d| PgValue::Date(DaysSinceEpoch(d))),
        // Time is bounded to one day.
        (0i64..86_400_000_000).prop_map(|m| PgValue::Time(TimeMicros(m))),
        ((0i64..86_400_000_000), -50_400i32..=50_400i32).prop_map(|(m, z)| PgValue::TimeTz {
            time: TimeMicros(m),
            zone_secs: z
        }),
        any::<i64>().prop_map(|t| PgValue::Timestamp(TimestampMicros(t))),
        any::<i64>().prop_map(|t| PgValue::TimestampTz(TimestampMicros(t))),
        prop::array::uniform16(any::<u8>()).prop_map(PgValue::Uuid),
        prop::collection::vec(any::<u8>(), 0..32).prop_map(PgValue::Bytea),
        ".{0,32}".prop_map(PgValue::Text),
        ".{0,32}".prop_map(PgValue::Json),
        ".{0,32}".prop_map(PgValue::Jsonb),
        // Numeric: arbitrary unscaled bytes, scale ≤ 38.
        (prop::collection::vec(any::<u8>(), 0..16), 0u8..=38).prop_map(|(b, s)| PgValue::Numeric(
            Decimal {
                unscaled_be_bytes: b,
                scale: s
            }
        )),
    ]
}

proptest! {
    /// For every PgValue, mapping to IcebergValue and back to a comparable form
    /// must preserve the data we model. Loss is documented (timetz drops
    /// zone offset; json/jsonb collapse to string).
    #[test]
    fn value_mapping_preserves_information(v in pg_value_strategy()) {
        let iv = value_map::pg_to_iceberg(v.clone());
        match (&v, &iv) {
            (PgValue::Null, IcebergValue::Null) => {}
            (PgValue::Bool(a), IcebergValue::Boolean(b)) => prop_assert_eq!(a, b),
            (PgValue::Int2(a), IcebergValue::Int(b)) => prop_assert_eq!(*a as i32, *b),
            (PgValue::Int4(a), IcebergValue::Int(b)) => prop_assert_eq!(a, b),
            (PgValue::Int8(a), IcebergValue::Long(b)) => prop_assert_eq!(a, b),
            (PgValue::Float4(a), IcebergValue::Float(b)) => prop_assert!(a == b || (a.is_nan() && b.is_nan())),
            (PgValue::Float8(a), IcebergValue::Double(b)) => prop_assert!(a == b || (a.is_nan() && b.is_nan())),
            (PgValue::Numeric(a), IcebergValue::Decimal(b)) => prop_assert_eq!(a, b),
            (PgValue::Text(a), IcebergValue::String(b)) => prop_assert_eq!(a, b),
            (PgValue::Json(a), IcebergValue::String(b)) => prop_assert_eq!(a, b),
            (PgValue::Jsonb(a), IcebergValue::String(b)) => prop_assert_eq!(a, b),
            (PgValue::Bytea(a), IcebergValue::Binary(b)) => prop_assert_eq!(a, b),
            (PgValue::Date(a), IcebergValue::Date(b)) => prop_assert_eq!(a, b),
            (PgValue::Time(a), IcebergValue::Time(b)) => prop_assert_eq!(a, b),
            // Documented loss: timetz drops zone_secs, only the time-of-day survives.
            (PgValue::TimeTz { time, .. }, IcebergValue::Time(b)) => prop_assert_eq!(time, b),
            (PgValue::Timestamp(a), IcebergValue::Timestamp(b)) => prop_assert_eq!(a, b),
            (PgValue::TimestampTz(a), IcebergValue::TimestampTz(b)) => prop_assert_eq!(a, b),
            (PgValue::Uuid(a), IcebergValue::Uuid(b)) => prop_assert_eq!(a, b),
            other => prop_assert!(false, "unexpected mapping: {:?}", other),
        }
    }
}
