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

use crate::pk::PkKey;
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
    /// The key whose committed row holds the `unchanged_cols` values, when
    /// it isn't this row's own: the row moved here from that key.
    pub unchanged_from: Option<PkKey>,
}

/// The JSON text form of a row's PK: a stable string for keys that are
/// persisted or compared as text (snapshot resume cursors, `verify`
/// paging). Materialization keys rows by [`PkKey`], which unlike this
/// treats a value the same whichever `PgValue` variant carries it.
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
    let mut by_pk: BTreeMap<PkKey, MaterializedRow> = BTreeMap::new();
    // Each key's state just before this cycle deleted it, for an UPDATE
    // that moves a row from that key.
    let mut deleted: BTreeMap<PkKey, MaterializedRow> = BTreeMap::new();
    for evt in events {
        let key = PkKey::from_row(&evt.row, pk_cols);
        let mut row = MaterializedRow {
            op: evt.op,
            row: evt.row,
            unchanged_cols: evt.unchanged_cols,
            unchanged_from: None,
        };
        // pgoutput emits `'u'` (unchanged sentinel) for TOAST columns
        // whose value didn't move on an UPDATE. Take their values from
        // the row's earlier state in this cycle — its own key's, or for a
        // moved row its old key's — so an INSERT-then-UPDATE inside one
        // cycle resolves without a committed data file. What that state
        // doesn't have stays unchanged, to resolve from the key whose
        // committed row holds it.
        if !row.unchanged_cols.is_empty() {
            match evt.moved_from {
                Some(old) => {
                    let old_key = PkKey::from_row(&old, pk_cols);
                    match deleted.get(&old_key) {
                        Some(prev) => row.inherit(prev, Some(old_key)),
                        None => row.unchanged_from = Some(old_key),
                    }
                }
                None => {
                    if let Some(prev) = by_pk.get(&key) {
                        row.inherit(prev, None);
                    }
                }
            }
        }
        let prev = by_pk.insert(key.clone(), row);
        if evt.op == Op::Delete {
            match prev {
                Some(p) if p.op != Op::Delete => {
                    deleted.insert(key, p);
                }
                _ => {}
            }
        }
    }
    by_pk.into_values().collect()
}

impl MaterializedRow {
    /// Fill this row's unchanged columns from `prev`, the row's earlier
    /// state in the cycle — under `moved_from`'s key when the row moved.
    /// A column `prev` also left unchanged stays unchanged, resolving
    /// from wherever `prev`'s would.
    fn inherit(&mut self, prev: &MaterializedRow, moved_from: Option<PkKey>) {
        self.unchanged_cols.retain(|col| match prev.row.get(col) {
            Some(v) if !prev.unchanged_cols.contains(col) => {
                self.row.insert(col.clone(), v.clone());
                false
            }
            _ => true,
        });
        if !self.unchanged_cols.is_empty() {
            self.unchanged_from = prev.unchanged_from.clone().or(moved_from);
        }
    }
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
            moved_from: None,
        }
    }

    fn noted(id: i32, note: PgValue) -> Row {
        let mut r = pk_only(id);
        r.insert(col("note"), note);
        r
    }

    /// An UPDATE of `id` that leaves `note` unchanged (sent as NULL).
    fn toast_update(id: i32, lsn: u64) -> MatEvent {
        let mut e = evt(Op::Update, lsn, noted(id, PgValue::Null));
        e.unchanged_cols = vec![col("note")];
        e
    }

    /// The two halves the pipeline stages for `UPDATE … SET id = to`
    /// leaving `note` unchanged under REPLICA IDENTITY DEFAULT.
    fn move_key(from: i32, to: i32, lsn: u64) -> [MatEvent; 2] {
        let mut upd = toast_update(to, lsn);
        upd.moved_from = Some(pk_only(from));
        [evt(Op::Delete, lsn, pk_only(from)), upd]
    }

    fn key(id: i32) -> PkKey {
        PkKey::from_row(&pk_only(id), &[col("id")])
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
    fn repeated_toast_updates_stay_unchanged() {
        // Neither update has the value: the second must not take the
        // first's NULL placeholder for it.
        let out = fold_events(vec![toast_update(1, 1), toast_update(1, 2)], &[col("id")]);
        assert_eq!(out[0].unchanged_cols, vec![col("note")]);
        assert_eq!(out[0].unchanged_from, None);
    }

    #[test]
    fn toast_update_after_an_update_with_the_value_takes_it() {
        let set = evt(Op::Update, 1, noted(1, PgValue::Text("big".into())));
        let out = fold_events(vec![set, toast_update(1, 2)], &[col("id")]);
        assert!(out[0].unchanged_cols.is_empty());
        assert_eq!(out[0].row[&col("note")], PgValue::Text("big".into()));
    }

    #[test]
    fn moved_row_resolves_from_its_old_key() {
        let out = fold_events(move_key(1, 2, 1).into(), &[col("id")]);
        assert_eq!(out.len(), 2);
        assert_eq!(out[0].op, Op::Delete);
        assert_eq!(out[1].unchanged_cols, vec![col("note")]);
        assert_eq!(out[1].unchanged_from, Some(key(1)));
    }

    #[test]
    fn moved_row_takes_values_its_old_key_got_this_cycle() {
        let mut events = vec![evt(Op::Insert, 1, noted(1, PgValue::Text("big".into())))];
        events.extend(move_key(1, 2, 2));
        let out = fold_events(events, &[col("id")]);
        assert!(out[1].unchanged_cols.is_empty());
        assert_eq!(out[1].row[&col("note")], PgValue::Text("big".into()));
    }

    #[test]
    fn moves_and_updates_resolve_from_the_first_key() {
        let mut events: Vec<MatEvent> = move_key(1, 2, 1).into();
        events.extend(move_key(2, 3, 2));
        events.push(toast_update(3, 3));
        let out = fold_events(events, &[col("id")]);
        let last = out.last().unwrap();
        assert_eq!(last.row[&col("id")], PgValue::Int4(3));
        assert_eq!(last.unchanged_cols, vec![col("note")]);
        assert_eq!(last.unchanged_from, Some(key(1)));
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
