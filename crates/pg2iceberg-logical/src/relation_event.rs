//! A source table's columns, as staged in the change log.
//!
//! The pipeline stages each pgoutput Relation message that changes a
//! table's columns as an `Op::Relation` event, in stream order, so the
//! materializer applies schema changes between the rows staged before
//! and after them. Its row carries the column list under one key, in the
//! source's column order (which re-add detection reads — see
//! [`pg2iceberg_iceberg::reconcile_columns`]).

use pg2iceberg_core::{ColumnName, IcebergType, PgValue, Row};

const COLUMNS: &str = "columns";

/// A table's columns: name and type, in the source's order.
pub type Columns = Vec<(String, IcebergType)>;

pub fn encode(columns: &Columns) -> Row {
    let json = serde_json::to_string(columns).expect("columns serialize");
    Row::from([(ColumnName(COLUMNS.into()), PgValue::Json(json))])
}

/// `None` if `row` isn't an encoded column list.
pub fn decode(row: &Row) -> Option<Columns> {
    match row.get(&ColumnName(COLUMNS.into()))? {
        PgValue::Json(json) => serde_json::from_str(json).ok(),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn columns_round_trip_in_order() {
        let cols: Columns = vec![
            ("id".into(), IcebergType::Int),
            ("qty".into(), IcebergType::Long),
            ("note".into(), IcebergType::String),
        ];
        assert_eq!(decode(&encode(&cols)), Some(cols));
    }
}
