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

use crate::pk::{KeyReader, PkKey};
use pg2iceberg_core::{ColumnName, Op, PgValue, Row};
use pg2iceberg_stream::MatEvent;
use std::collections::HashMap;

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
    fold_events_with_sources(events, pk_cols).0
}

/// [`fold_events`], with each folded row's source: the key whose
/// committed row it continues — an update keeps that row's id (row
/// lineage). Its own key's, unless it moved from another key; `None` for
/// a delete, and a row the cycle inserted after deleting its key, or
/// moved from one it inserted that way — a new row. An insert not after
/// a delete of its key continues the row its key holds, if any: applied
/// again, it's the same row (a backfill's copy of a row its changes
/// already wrote, say).
pub fn fold_events_with_sources(
    events: Vec<MatEvent>,
    pk_cols: &[ColumnName],
) -> (Vec<MaterializedRow>, Vec<Option<PkKey>>) {
    let mut keys = KeyReader::new(pk_cols);
    let mut by_pk: HashMap<PkKey, (MaterializedRow, Option<PkKey>)> =
        HashMap::with_capacity(events.len());
    // Each key's state just before this cycle deleted it, with its
    // source, for an UPDATE that moves a row from that key.
    let mut deleted: HashMap<PkKey, (MaterializedRow, Option<PkKey>)> = HashMap::new();
    for evt in events {
        let key = keys.key(&evt.row);
        let moved_from = evt.moved_from.as_ref().map(|old| keys.key(old));
        let source = match (evt.op, &moved_from, by_pk.get(&key)) {
            (Op::Delete, _, _) => None,
            (Op::Update, Some(old), _) => match deleted.get(old) {
                Some((_, source)) => source.clone(),
                None => Some(old.clone()),
            },
            (_, _, None) => Some(key.clone()),
            (_, _, Some((prev, _))) if prev.op == Op::Delete => None,
            (_, _, Some((_, source))) => source.clone(),
        };
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
            match moved_from {
                Some(old_key) => match deleted.get(&old_key) {
                    Some((prev, _)) => row.inherit(prev, Some(old_key)),
                    None => row.unchanged_from = Some(old_key),
                },
                None => {
                    if let Some((prev, _)) = by_pk.get(&key) {
                        row.inherit(prev, None);
                    }
                }
            }
        }
        let prev = by_pk.insert(key.clone(), (row, source));
        if evt.op == Op::Delete {
            match prev {
                Some(p) if p.0.op != Op::Delete => {
                    deleted.insert(key, p);
                }
                _ => {}
            }
        }
    }
    let mut folded: Vec<(PkKey, (MaterializedRow, Option<PkKey>))> = by_pk.into_iter().collect();
    folded.sort_unstable_by(|a, b| a.0.cmp(&b.0));
    folded.into_iter().map(|(_, row)| row).unzip()
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
        let mut r = Row::new();
        r.insert(col("id"), PgValue::Int4(id));
        r.insert(col("qty"), PgValue::Int4(qty));
        r
    }

    fn pk_only(id: i32) -> Row {
        let mut r = Row::new();
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

    /// Each folded row's source, by its key.
    fn sources(events: Vec<MatEvent>) -> Vec<(i32, Option<PkKey>)> {
        let (rows, sources) = fold_events_with_sources(events, &[col("id")]);
        rows.iter()
            .map(|r| match r.row[&col("id")] {
                PgValue::Int4(id) => id,
                _ => unreachable!(),
            })
            .zip(sources)
            .collect()
    }

    #[test]
    fn an_update_continues_its_keys_row() {
        let got = sources(vec![
            evt(Op::Update, 1, row(1, 10)),
            evt(Op::Update, 2, row(1, 11)),
            evt(Op::Delete, 3, pk_only(2)),
        ]);
        assert_eq!(got, [(1, Some(key(1))), (2, None)]);
    }

    /// An insert continues the row its key holds, if any — the same row
    /// applied again — unless the cycle deleted the key first: then it's
    /// a new row, and so are its updates.
    #[test]
    fn an_insert_after_a_delete_is_a_new_row() {
        let got = sources(vec![
            evt(Op::Insert, 1, row(1, 10)),
            evt(Op::Delete, 2, pk_only(2)),
            evt(Op::Insert, 3, row(2, 20)),
            evt(Op::Update, 4, row(2, 21)),
        ]);
        assert_eq!(got, [(1, Some(key(1))), (2, None)]);
    }

    /// A moved row continues its old key's row — or what that row
    /// continued this cycle, through several moves. A row inserted at a
    /// key one moved from is new.
    #[test]
    fn a_moved_row_continues_its_old_keys_row() {
        let mut events: Vec<MatEvent> = move_key(1, 2, 1).into();
        events.extend(move_key(2, 3, 2));
        events.push(evt(Op::Insert, 3, row(1, 10)));
        let got = sources(events);
        assert_eq!(got, [(1, None), (2, None), (3, Some(key(1)))]);

        // Moved from a key the cycle inserted after deleting it: new.
        let mut events = vec![
            evt(Op::Delete, 1, pk_only(1)),
            evt(Op::Insert, 2, row(1, 10)),
        ];
        events.extend(move_key(1, 2, 3));
        assert_eq!(sources(events), [(1, None), (2, None)]);
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
