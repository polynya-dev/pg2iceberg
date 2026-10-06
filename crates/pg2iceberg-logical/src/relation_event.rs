//! A source table's columns, as staged in the change log.
//!
//! The pipeline stages each pgoutput Relation message that changes a
//! table's columns as an `Op::Relation` event, in stream order, so the
//! materializer applies schema changes between the rows staged before
//! and after them. Its row carries the column list under one key, in the
//! source's column order (which re-add detection reads — see
//! [`pg2iceberg_iceberg::reconcile_columns`]), and the columns' defaults
//! under another.

use pg2iceberg_core::{ColumnName, IcebergType, PgValue, Row};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

const COLUMNS: &str = "columns";
const DEFAULTS: &str = "defaults";

/// A table's columns: name and type, in the source's order.
pub type Columns = Vec<(String, IcebergType)>;

/// What Postgres's catalog said, when the event was staged, of the value
/// a column's rows that predate it read (see
/// [`pg2iceberg_pg::ColumnDefault`]). The catalog has no history: a change
/// since the column was added may have taken the value away.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum DefaultValue {
    /// The value Postgres stores for them.
    Stored(PgValue),
    /// The column has a default Postgres doesn't store for them: a
    /// volatile one, which gave each row its own, or the table was
    /// rewritten since.
    NotStored,
    /// The catalog no longer has the column: dropped, or renamed, since.
    ColumnGone,
}

/// The table's columns with a [`DefaultValue`], by name; a column without
/// a default has none.
pub type Defaults = BTreeMap<String, DefaultValue>;

/// A relation event's contents.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Relation {
    pub columns: Columns,
    pub defaults: Defaults,
}

pub fn encode(relation: &Relation) -> Row {
    let columns = serde_json::to_string(&relation.columns).expect("columns serialize");
    let mut row = Row::from([(ColumnName(COLUMNS.into()), PgValue::Json(columns))]);
    if !relation.defaults.is_empty() {
        let defaults = serde_json::to_string(&relation.defaults).expect("defaults serialize");
        row.insert(ColumnName(DEFAULTS.into()), PgValue::Json(defaults));
    }
    row
}

/// `None` if `row` isn't an encoded relation. Events staged before
/// defaults were have none.
pub fn decode(row: &Row) -> Option<Relation> {
    let json = |key: &str| match row.get(&ColumnName(key.into())) {
        Some(PgValue::Json(json)) => Some(json),
        _ => None,
    };
    let columns = serde_json::from_str(json(COLUMNS)?).ok()?;
    let defaults = match json(DEFAULTS) {
        Some(json) => serde_json::from_str(json).ok()?,
        None => Defaults::new(),
    };
    Some(Relation { columns, defaults })
}

/// `value` as JSON, if it reads back the same: not a NaN, say, which JSON
/// can't hold.
pub fn value_to_json(value: &PgValue) -> Option<String> {
    let json = serde_json::to_string(value).ok()?;
    (value_from_json(&json).as_ref() == Some(value)).then_some(json)
}

pub fn value_from_json(json: &str) -> Option<PgValue> {
    serde_json::from_str(json).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn columns() -> Columns {
        vec![
            ("id".into(), IcebergType::Int),
            ("qty".into(), IcebergType::Long),
            ("note".into(), IcebergType::String),
        ]
    }

    #[test]
    fn columns_round_trip_in_order() {
        let relation = Relation {
            columns: columns(),
            defaults: Defaults::new(),
        };
        assert_eq!(decode(&encode(&relation)), Some(relation));
    }

    #[test]
    fn defaults_round_trip() {
        let relation = Relation {
            columns: columns(),
            defaults: Defaults::from([
                ("id".into(), DefaultValue::ColumnGone),
                ("qty".into(), DefaultValue::Stored(PgValue::Int8(7))),
                ("note".into(), DefaultValue::NotStored),
            ]),
        };
        assert_eq!(decode(&encode(&relation)), Some(relation));
    }

    #[test]
    fn values_json_cant_hold_are_refused() {
        assert_eq!(value_to_json(&PgValue::Float8(f64::NAN)), None);
        let json = value_to_json(&PgValue::Text("on".into())).unwrap();
        assert_eq!(value_from_json(&json), Some(PgValue::Text("on".into())));
    }
}
