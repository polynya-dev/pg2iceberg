//! Materialization fold: collapse a sequence of `MatEvent`s into the final
//! per-PK row state.
//!
//! Mirrors `logical/materializer.go:343+` (`FoldEvents`). Walks events in LSN
//! order; for each PK keeps the most recent (op, row, unchanged_cols).
//!
//! Why fold matters: the materializer can stage many events per PK in one
//! cycle (insert + N updates + maybe delete). Iceberg merge-on-read only
//! needs the final state of each PK to compute the equality-delete + data
//! file output. Folding upfront cuts both Parquet rows written and PG
//! coord-update churn.

use pg2iceberg_core::{ColumnName, Op, PgValue, Row};
use pg2iceberg_stream::MatEvent;
use std::collections::BTreeMap;

/// Final state for one PK after the fold.
#[derive(Clone, Debug, PartialEq)]
pub struct MaterializedRow {
    pub op: Op,
    /// Full row for `Insert`/`Update`; PK-only for `Delete`.
    pub row: Row,
    pub unchanged_cols: Vec<ColumnName>,
}

/// Build a canonical PK key from a row given the PK column list.
/// Mirrors Go's `BuildPKKey`. Used for grouping during the fold and as the
/// FileIndex key in Phase 7.5.
pub fn pk_key(row: &Row, pk_cols: &[ColumnName]) -> String {
    let parts: Vec<&PgValue> = pk_cols.iter().filter_map(|c| row.get(c)).collect();
    serde_json::to_string(&parts).expect("PgValue is Serializable")
}

/// Fold events into per-PK final state. Returns rows in PK-key order so the
/// output is deterministic regardless of input event interleaving.
///
/// `events` is consumed; events are assumed already sorted by LSN.
///
/// Op transitions:
/// - `I` → `(Insert, after, [])`
/// - `U` → `(Update, after, unchanged_cols)`
/// - `D` → `(Delete, before-or-pk-only-row, [])`
///
/// Multiple events for the same PK collapse to the *last* event's state.
/// Caveat: if events are `I`-then-`D` for the same PK, the output is `Delete`
/// with the row from the `D` event (which carries before-row). Iceberg MoR
/// applies an equality-delete that's a no-op if no prior data exists, so this
/// is safe even when the row was never persisted to a data file.
pub fn fold_events(events: Vec<MatEvent>, pk_cols: &[ColumnName]) -> Vec<MaterializedRow> {
    let mut by_pk: BTreeMap<String, MaterializedRow> = BTreeMap::new();
    for evt in events {
        let key = pk_key(&evt.row, pk_cols);
        by_pk.insert(
            key,
            MaterializedRow {
                op: evt.op,
                row: evt.row,
                unchanged_cols: evt.unchanged_cols,
            },
        );
    }
    by_pk.into_values().collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use pg2iceberg_core::{Lsn, Timestamp};

    fn col(name: &str) -> ColumnName {
        ColumnName(name.into())
    }

    fn row(id: i32, qty: i32) -> Row {
        let mut r = BTreeMap::new();
        r.insert(col("id"), PgValue::Int4(id));
        r.insert(col("qty"), PgValue::Int4(qty));
        r
    }

    fn pk_only(id: i32) -> Row {
        let mut r = BTreeMap::new();
        r.insert(col("id"), PgValue::Int4(id));
        r
    }

    fn evt(op: Op, lsn: u64, r: Row) -> MatEvent {
        MatEvent {
            op,
            lsn: Lsn(lsn),
            commit_ts: Timestamp(0),
            xid: Some(1),
            unchanged_cols: vec![],
            row: r,
        }
    }

    #[test]
    fn empty_input_yields_empty_output() {
        let out = fold_events(vec![], &[col("id")]);
        assert!(out.is_empty());
    }

    #[test]
    fn single_insert_passes_through() {
        let out = fold_events(vec![evt(Op::Insert, 1, row(1, 10))], &[col("id")]);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].op, Op::Insert);
        assert_eq!(out[0].row, row(1, 10));
    }

    #[test]
    fn insert_then_update_collapses_to_update() {
        let out = fold_events(
            vec![
                evt(Op::Insert, 1, row(1, 10)),
                evt(Op::Update, 2, row(1, 20)),
            ],
            &[col("id")],
        );
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].op, Op::Update);
        assert_eq!(out[0].row, row(1, 20));
    }

    #[test]
    fn insert_then_delete_collapses_to_delete() {
        let out = fold_events(
            vec![
                evt(Op::Insert, 1, row(1, 10)),
                evt(Op::Delete, 2, pk_only(1)),
            ],
            &[col("id")],
        );
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].op, Op::Delete);
        assert_eq!(out[0].row, pk_only(1));
    }

    #[test]
    fn delete_then_insert_collapses_to_insert_with_new_row() {
        // D-then-I exercises the "row was deleted then re-inserted" path.
        // Iceberg MoR is correct because the materializer emits an equality
        // delete on the PK *and* a fresh data row — see TableWriter::prepare.
        let out = fold_events(
            vec![
                evt(Op::Delete, 1, pk_only(1)),
                evt(Op::Insert, 2, row(1, 99)),
            ],
            &[col("id")],
        );
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].op, Op::Insert);
        assert_eq!(out[0].row, row(1, 99));
    }

    #[test]
    fn distinct_pks_kept_separate_and_pk_ordered() {
        let out = fold_events(
            vec![
                evt(Op::Insert, 3, row(3, 30)),
                evt(Op::Insert, 1, row(1, 10)),
                evt(Op::Insert, 2, row(2, 20)),
            ],
            &[col("id")],
        );
        assert_eq!(out.len(), 3);
        let ids: Vec<i32> = out
            .iter()
            .map(|m| match m.row.get(&col("id")) {
                Some(PgValue::Int4(n)) => *n,
                _ => panic!(),
            })
            .collect();
        // BTreeMap output is ordered by PK-key string. JSON of `[Int4(1)]`
        // sorts before `[Int4(2)]` etc. — check ascending.
        let mut sorted = ids.clone();
        sorted.sort();
        assert_eq!(ids, sorted);
    }

    #[test]
    fn unchanged_cols_propagate_from_last_event() {
        let mut e = evt(Op::Update, 2, row(1, 20));
        e.unchanged_cols = vec![col("blob"), col("doc")];
        let out = fold_events(vec![evt(Op::Insert, 1, row(1, 10)), e], &[col("id")]);
        assert_eq!(out[0].unchanged_cols, vec![col("blob"), col("doc")]);
    }

    #[test]
    fn pk_key_serializes_pk_columns_only() {
        let r = row(42, 7);
        let single = pk_key(&r, &[col("id")]);
        let composite = pk_key(&r, &[col("id"), col("qty")]);
        assert_ne!(single, composite);
        // Same PK columns → same key, regardless of non-PK values.
        assert_eq!(
            pk_key(&row(42, 1), &[col("id")]),
            pk_key(&row(42, 999), &[col("id")])
        );
    }
}
