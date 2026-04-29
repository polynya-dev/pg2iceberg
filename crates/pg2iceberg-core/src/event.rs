use crate::io::Timestamp;
use crate::lsn::Lsn;
use crate::schema::TableIdent;
use crate::value::PgValue;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Clone, Eq, Ord, PartialEq, PartialOrd, Hash, Debug, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ColumnName(pub String);

#[derive(Copy, Clone, Eq, PartialEq, Debug, Serialize, Deserialize)]
pub enum Op {
    Insert,
    Update,
    Delete,
    /// DDL on the source table, captured as a `Relation` message in pgoutput.
    Relation,
    Truncate,
}

/// User-row representation. Ordered by column name so BTreeMap iteration is
/// deterministic.
pub type Row = BTreeMap<ColumnName, PgValue>;

#[derive(Clone, PartialEq, Debug, Serialize, Deserialize)]
pub struct ChangeEvent {
    pub table: TableIdent,
    pub op: Op,
    pub lsn: Lsn,
    pub commit_ts: Timestamp,
    /// Source-PG transaction ID. `None` for events emitted outside a tx
    /// (initial-snapshot rows).
    pub xid: Option<u32>,
    pub before: Option<Row>,
    pub after: Option<Row>,
    /// Columns whose value the publisher said are unchanged (TOAST `'u'` placeholder).
    /// The materializer resolves these against the FileIndex of prior data.
    pub unchanged_cols: Vec<ColumnName>,
}
