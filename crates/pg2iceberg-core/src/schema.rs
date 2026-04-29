use crate::partition::PartitionField;
use crate::typemap::IcebergType;
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Clone, Default, Eq, Ord, PartialEq, PartialOrd, Hash, Debug, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Namespace(pub Vec<String>);

impl fmt::Display for Namespace {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0.join("."))
    }
}

#[derive(Clone, Default, Eq, Ord, PartialEq, PartialOrd, Hash, Debug, Serialize, Deserialize)]
pub struct TableIdent {
    pub namespace: Namespace,
    pub name: String,
}

impl fmt::Display for TableIdent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.namespace.0.is_empty() {
            f.write_str(&self.name)
        } else {
            write!(f, "{}.{}", self.namespace, self.name)
        }
    }
}

#[derive(Clone, Eq, PartialEq, Debug, Serialize, Deserialize)]
pub struct ColumnSchema {
    pub name: String,
    /// Iceberg field id. Stable across renames; required by Iceberg readers
    /// for column resolution. The first column in a fresh table starts at 1
    /// and increments; new columns added via schema evolution take the next
    /// unused id.
    pub field_id: i32,
    pub ty: IcebergType,
    pub nullable: bool,
    pub is_primary_key: bool,
}

#[derive(Clone, Eq, PartialEq, Debug, Serialize, Deserialize)]
pub struct TableSchema {
    pub ident: TableIdent,
    pub columns: Vec<ColumnSchema>,
    /// Iceberg partition spec. Empty = unpartitioned.
    /// Source columns must reference names that exist in `columns`.
    #[serde(default)]
    pub partition_spec: Vec<PartitionField>,
}

impl TableSchema {
    pub fn primary_key_columns(&self) -> impl Iterator<Item = &ColumnSchema> {
        self.columns.iter().filter(|c| c.is_primary_key)
    }

    pub fn is_partitioned(&self) -> bool {
        !self.partition_spec.is_empty()
    }

    /// Look up a column's `field_id` by name. Used to resolve
    /// `partition_spec[].source_column` to a Iceberg `source_id`.
    pub fn field_id_for(&self, column_name: &str) -> Option<i32> {
        self.columns
            .iter()
            .find(|c| c.name == column_name)
            .map(|c| c.field_id)
    }

    /// Look up a column by name.
    pub fn column(&self, name: &str) -> Option<&ColumnSchema> {
        self.columns.iter().find(|c| c.name == name)
    }
}
