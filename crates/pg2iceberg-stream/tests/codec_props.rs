//! Round-trip property test for the staged-event codec.
//!
//! For every random sequence of DML `ChangeEvent`s, encoding to Parquet and
//! decoding back must preserve op / lsn / commit_ts / xid / unchanged_cols /
//! row, in order. Phase 2 deliverable.

use pg2iceberg_core::value::{DaysSinceEpoch, Decimal, TimeMicros, TimestampMicros};
use pg2iceberg_core::{
    ChangeEvent, ColumnName, Lsn, Namespace, Op, PgValue, TableIdent, Timestamp,
};
use pg2iceberg_stream::codec::{decode_chunk, encode_chunk};
use proptest::prelude::*;
use std::collections::BTreeMap;

fn ident() -> TableIdent {
    TableIdent {
        namespace: Namespace(vec!["public".into()]),
        name: "t".into(),
    }
}

fn pg_value() -> impl Strategy<Value = PgValue> {
    prop_oneof![
        Just(PgValue::Null),
        any::<bool>().prop_map(PgValue::Bool),
        any::<i16>().prop_map(PgValue::Int2),
        any::<i32>().prop_map(PgValue::Int4),
        any::<i64>().prop_map(PgValue::Int8),
        any::<i32>().prop_map(|d| PgValue::Date(DaysSinceEpoch(d))),
        (0i64..86_400_000_000).prop_map(|m| PgValue::Time(TimeMicros(m))),
        ((0i64..86_400_000_000), -50_400i32..=50_400i32).prop_map(|(m, z)| PgValue::TimeTz {
            time: TimeMicros(m),
            zone_secs: z
        }),
        any::<i64>().prop_map(|t| PgValue::Timestamp(TimestampMicros(t))),
        any::<i64>().prop_map(|t| PgValue::TimestampTz(TimestampMicros(t))),
        prop::array::uniform16(any::<u8>()).prop_map(PgValue::Uuid),
        prop::collection::vec(any::<u8>(), 0..16).prop_map(PgValue::Bytea),
        "[a-zA-Z0-9_]{0,16}".prop_map(PgValue::Text),
        "[a-zA-Z0-9_]{0,16}".prop_map(PgValue::Json),
        "[a-zA-Z0-9_]{0,16}".prop_map(PgValue::Jsonb),
        (prop::collection::vec(any::<u8>(), 0..8), 0u8..=18).prop_map(|(b, s)| PgValue::Numeric(
            Decimal {
                unscaled_be_bytes: b,
                scale: s,
            }
        )),
    ]
}

fn row() -> impl Strategy<Value = std::collections::BTreeMap<ColumnName, PgValue>> {
    // Column names from a small alphabet so we get duplicates across rows.
    let key = "[a-z_]{1,8}".prop_map(ColumnName);
    prop::collection::btree_map(key, pg_value(), 0..6)
}

fn dml_op() -> impl Strategy<Value = Op> {
    prop_oneof![Just(Op::Insert), Just(Op::Update), Just(Op::Delete)]
}

fn change_event() -> impl Strategy<Value = ChangeEvent> {
    (
        dml_op(),
        any::<u64>(),
        any::<i64>(),
        prop::option::of(any::<u32>()),
        row(),
        row(),
        prop::collection::vec("[a-z_]{1,8}".prop_map(ColumnName), 0..4),
    )
        .prop_map(|(op, lsn, ts, xid, before, after, unchanged)| {
            // Ensure the op-specific row is populated; otherwise encode_chunk errors.
            let (before_opt, after_opt) = match op {
                Op::Insert | Op::Update => (None, Some(after)),
                Op::Delete => (Some(before), None),
                _ => unreachable!(),
            };
            ChangeEvent {
                table: ident(),
                op,
                lsn: Lsn(lsn),
                commit_ts: Timestamp(ts),
                xid,
                before: before_opt,
                after: after_opt,
                unchanged_cols: unchanged,
            }
        })
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    #[test]
    fn round_trip_preserves_dml_events(events in prop::collection::vec(change_event(), 0..16)) {
        let chunk = encode_chunk(&events).unwrap();
        prop_assert_eq!(chunk.record_count as usize, events.len());

        let decoded = decode_chunk(&chunk.bytes).unwrap();
        prop_assert_eq!(decoded.len(), events.len());

        for (orig, dec) in events.iter().zip(decoded.iter()) {
            prop_assert_eq!(dec.op, orig.op);
            prop_assert_eq!(dec.lsn, orig.lsn);
            prop_assert_eq!(dec.commit_ts, orig.commit_ts);
            prop_assert_eq!(dec.xid, orig.xid);
            prop_assert_eq!(&dec.unchanged_cols, &orig.unchanged_cols);

            let expected_row: &BTreeMap<_, _> = match orig.op {
                Op::Insert | Op::Update => orig.after.as_ref().unwrap(),
                Op::Delete => orig.before.as_ref().unwrap(),
                _ => unreachable!(),
            };
            prop_assert_eq!(&dec.row, expected_row);
        }

        // max_lsn invariant.
        let expected_max = events.iter().map(|e| e.lsn).max().unwrap_or(Lsn::ZERO);
        prop_assert_eq!(chunk.max_lsn, expected_max);
    }
}
