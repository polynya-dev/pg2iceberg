//! Materialization helpers: TOAST resolution + re-insert promotion.
//!
//! Sits between [`crate::fold::fold_events`] and
//! [`crate::writer::TableWriter::prepare`]. Both functions are sync; the
//! materializer (Phase 8) handles the async `BlobStore.get` calls before
//! invoking [`resolve_unchanged_cols`].

use crate::file_index::FileIndex;
use crate::fold::{pk_key, MaterializedRow};
use crate::writer::WriterError;
use pg2iceberg_core::{ColumnName, Op, Row};
use std::collections::BTreeMap;

pub type Result<T> = std::result::Result<T, WriterError>;

/// Promote `Insert` ops to `Update` when their PK already exists in the
/// FileIndex. The downstream `TableWriter` then emits an equality delete on
/// that PK so readers see only the new row, not both.
///
/// Required after a `D`-then-`I` sequence in the same batch *or* an `I` for a
/// PK that lives in a prior committed data file. Mirrors the implicit
/// behavior of Go's path: `tablewriter.go:301` only emits equality deletes
/// for `U`/`D`, but the materializer routes re-inserts through Update earlier
/// in the pipeline.
pub fn promote_re_inserts(
    rows: &mut [MaterializedRow],
    file_index: &FileIndex,
    pk_cols: &[ColumnName],
) {
    for r in rows {
        if r.op == Op::Insert {
            let key = pk_key(&r.row, pk_cols);
            if file_index.contains_pk(&key) {
                r.op = Op::Update;
            }
        }
    }
}

/// Fill in `unchanged_cols` placeholders by reading the prior data file.
///
/// For each row with non-empty `unchanged_cols`:
/// - Look up the file path containing its PK via `FileIndex`.
/// - Find the corresponding row in `prior_rows_by_path` (caller pre-fetched
///   and decoded the file via `BlobStore` + [`crate::reader::read_data_file`]).
/// - Copy each unchanged column's value into the row, then clear
///   `unchanged_cols`.
///
/// Errors when:
/// - The PK isn't in any indexed file (TOAST UPDATE on a row we don't have
///   prior data for — the source publication may be new, or replica identity
///   isn't FULL).
/// - The expected file wasn't pre-fetched into `prior_rows_by_path`.
/// - The file was pre-fetched but doesn't actually contain the PK.
pub fn resolve_unchanged_cols(
    rows: &mut [MaterializedRow],
    pk_cols: &[ColumnName],
    file_index: &FileIndex,
    prior_rows_by_path: &BTreeMap<String, Vec<Row>>,
) -> Result<()> {
    for r in rows {
        if r.unchanged_cols.is_empty() {
            continue;
        }
        let key = pk_key(&r.row, pk_cols);
        let path = file_index.lookup(&key).ok_or_else(|| {
            WriterError::Encode(format!(
                "TOAST resolution failed: PK {key} is not in any indexed data file"
            ))
        })?;
        let priors = prior_rows_by_path.get(path).ok_or_else(|| {
            WriterError::Encode(format!(
                "TOAST resolution failed: file {path} was not pre-fetched by the materializer"
            ))
        })?;
        let prior = priors
            .iter()
            .find(|p| pk_key(p, pk_cols) == key)
            .ok_or_else(|| {
                WriterError::Encode(format!(
                    "TOAST resolution failed: PK {key} expected in file {path} but not found"
                ))
            })?;
        for col in &r.unchanged_cols {
            if let Some(v) = prior.get(col) {
                r.row.insert(col.clone(), v.clone());
            }
        }
        r.unchanged_cols.clear();
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use pg2iceberg_core::PgValue;

    fn c(name: &str) -> ColumnName {
        ColumnName(name.into())
    }

    fn row(id: i32, qty: i32) -> Row {
        let mut r = BTreeMap::new();
        r.insert(c("id"), PgValue::Int4(id));
        r.insert(c("qty"), PgValue::Int4(qty));
        r
    }

    fn pk_only(id: i32) -> Row {
        let mut r = BTreeMap::new();
        r.insert(c("id"), PgValue::Int4(id));
        r
    }

    fn mat(op: Op, r: Row, unchanged: Vec<&str>) -> MaterializedRow {
        MaterializedRow {
            op,
            row: r,
            unchanged_cols: unchanged.into_iter().map(c).collect(),
        }
    }

    // ---------- promote_re_inserts ----------

    #[test]
    fn insert_with_pk_in_index_becomes_update() {
        let mut fi = FileIndex::new();
        let pk_cols = vec![c("id")];
        let key = pk_key(&row(1, 0), &pk_cols);
        fi.add_file("p0".into(), vec![key], Vec::new());

        let mut rows = vec![mat(Op::Insert, row(1, 99), vec![])];
        promote_re_inserts(&mut rows, &fi, &pk_cols);
        assert_eq!(rows[0].op, Op::Update);
    }

    #[test]
    fn fresh_insert_stays_insert() {
        let fi = FileIndex::new();
        let pk_cols = vec![c("id")];
        let mut rows = vec![mat(Op::Insert, row(1, 99), vec![])];
        promote_re_inserts(&mut rows, &fi, &pk_cols);
        assert_eq!(rows[0].op, Op::Insert);
    }

    #[test]
    fn update_and_delete_are_left_alone() {
        let mut fi = FileIndex::new();
        let pk_cols = vec![c("id")];
        let key = pk_key(&row(1, 0), &pk_cols);
        fi.add_file("p0".into(), vec![key], Vec::new());

        let mut rows = vec![
            mat(Op::Update, row(1, 99), vec![]),
            mat(Op::Delete, pk_only(1), vec![]),
        ];
        promote_re_inserts(&mut rows, &fi, &pk_cols);
        assert_eq!(rows[0].op, Op::Update);
        assert_eq!(rows[1].op, Op::Delete);
    }

    // ---------- resolve_unchanged_cols ----------

    #[test]
    fn unchanged_col_filled_from_prior_data_file() {
        let pk_cols = vec![c("id")];
        let mut fi = FileIndex::new();
        let key = pk_key(&row(1, 0), &pk_cols);
        fi.add_file("p0".into(), vec![key], Vec::new());

        // Prior row had qty=42; the staged update marks `qty` as unchanged
        // (TOAST placeholder) and we want resolve to fill it back in.
        let mut prior_rows = BTreeMap::new();
        prior_rows.insert("p0".into(), vec![row(1, 42)]);

        // The current update payload has a placeholder PgValue::Null for qty.
        let mut placeholder = pk_only(1);
        placeholder.insert(c("qty"), PgValue::Null);
        let mut rows = vec![mat(Op::Update, placeholder, vec!["qty"])];

        resolve_unchanged_cols(&mut rows, &pk_cols, &fi, &prior_rows).unwrap();
        assert_eq!(rows[0].row.get(&c("qty")), Some(&PgValue::Int4(42)));
        assert!(rows[0].unchanged_cols.is_empty());
    }

    #[test]
    fn no_unchanged_cols_is_a_noop() {
        let pk_cols = vec![c("id")];
        let fi = FileIndex::new();
        let prior = BTreeMap::new();
        let mut rows = vec![mat(Op::Update, row(1, 5), vec![])];
        resolve_unchanged_cols(&mut rows, &pk_cols, &fi, &prior).unwrap();
        assert_eq!(rows[0].row.get(&c("qty")), Some(&PgValue::Int4(5)));
    }

    #[test]
    fn missing_pk_in_index_errors() {
        let pk_cols = vec![c("id")];
        let fi = FileIndex::new();
        let prior = BTreeMap::new();
        let mut rows = vec![mat(Op::Update, row(1, 0), vec!["qty"])];
        let err = resolve_unchanged_cols(&mut rows, &pk_cols, &fi, &prior).unwrap_err();
        assert!(matches!(err, WriterError::Encode(ref s) if s.contains("not in any indexed")));
    }

    #[test]
    fn file_not_prefetched_errors() {
        let pk_cols = vec![c("id")];
        let mut fi = FileIndex::new();
        let key = pk_key(&row(1, 0), &pk_cols);
        fi.add_file("p0".into(), vec![key], Vec::new());
        // prior_rows_by_path is empty — caller forgot to fetch p0.
        let prior = BTreeMap::new();
        let mut rows = vec![mat(Op::Update, row(1, 0), vec!["qty"])];
        let err = resolve_unchanged_cols(&mut rows, &pk_cols, &fi, &prior).unwrap_err();
        assert!(matches!(err, WriterError::Encode(ref s) if s.contains("not pre-fetched")));
    }
}
