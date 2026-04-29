//! In-memory PK-dedup buffer for query mode.
//!
//! Mirrors `query/buffer.go`. The poller upserts rows; only the latest row
//! per PK survives. On flush, all rows drain as `Op::Insert` — the
//! materializer-style `promote_re_inserts` against the FileIndex later
//! demotes already-known PKs to `Update` so MoR readers see the new row,
//! not duplicates.

use pg2iceberg_core::Op;
use pg2iceberg_core::{ColumnName, Row};
use pg2iceberg_iceberg::{pk_key, MaterializedRow};
use std::collections::BTreeMap;

#[derive(Debug)]
pub struct Buffer {
    pk_cols: Vec<ColumnName>,
    rows: BTreeMap<String, Row>,
}

impl Buffer {
    pub fn new(pk_cols: Vec<ColumnName>) -> Self {
        Self {
            pk_cols,
            rows: BTreeMap::new(),
        }
    }

    /// Upsert a row keyed by its PK. Replaces any prior entry — newer wins.
    pub fn upsert(&mut self, row: Row) {
        let key = pk_key(&row, &self.pk_cols);
        self.rows.insert(key, row);
    }

    pub fn len(&self) -> usize {
        self.rows.len()
    }

    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    /// Drain into `MaterializedRow`s with `Op::Insert`. The query-mode
    /// pipeline runs them through `promote_re_inserts` before handing to
    /// `TableWriter::prepare`.
    pub fn drain(&mut self) -> Vec<MaterializedRow> {
        std::mem::take(&mut self.rows)
            .into_values()
            .map(|row| MaterializedRow {
                op: Op::Insert,
                row,
                unchanged_cols: vec![],
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pg2iceberg_core::PgValue;

    fn col(n: &str) -> ColumnName {
        ColumnName(n.into())
    }

    fn row(id: i32, qty: i32) -> Row {
        let mut r = BTreeMap::new();
        r.insert(col("id"), PgValue::Int4(id));
        r.insert(col("qty"), PgValue::Int4(qty));
        r
    }

    #[test]
    fn upsert_dedups_by_pk_with_newer_wins() {
        let mut b = Buffer::new(vec![col("id")]);
        b.upsert(row(1, 10));
        b.upsert(row(1, 99));
        b.upsert(row(2, 20));
        assert_eq!(b.len(), 2);
        let drained = b.drain();
        assert_eq!(drained.len(), 2);
        // Drained order is PK-key-sorted by BTreeMap. Find each by id.
        let by_id: BTreeMap<i32, &Row> = drained
            .iter()
            .map(|m| match m.row.get(&col("id")) {
                Some(PgValue::Int4(n)) => (*n, &m.row),
                _ => panic!(),
            })
            .collect();
        assert_eq!(
            by_id.get(&1).unwrap().get(&col("qty")),
            Some(&PgValue::Int4(99))
        );
        assert_eq!(
            by_id.get(&2).unwrap().get(&col("qty")),
            Some(&PgValue::Int4(20))
        );
    }

    #[test]
    fn drain_clears_buffer() {
        let mut b = Buffer::new(vec![col("id")]);
        b.upsert(row(1, 10));
        let _ = b.drain();
        assert!(b.is_empty());
    }

    #[test]
    fn drain_on_empty_buffer_yields_empty_vec() {
        let mut b = Buffer::new(vec![col("id")]);
        assert!(b.drain().is_empty());
    }

    #[test]
    fn every_drained_row_has_op_insert() {
        let mut b = Buffer::new(vec![col("id")]);
        b.upsert(row(1, 10));
        b.upsert(row(2, 20));
        for m in b.drain() {
            assert_eq!(m.op, Op::Insert);
            assert!(m.unchanged_cols.is_empty());
        }
    }
}
