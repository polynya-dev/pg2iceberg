//! Core types and IO traits for pg2iceberg.
//!
//! This crate has zero IO dependencies. It owns the type vocabulary and the
//! traits that production impls and the simulation impls both implement.

pub mod event;
pub mod io;
pub mod lsn;
pub mod metrics;
pub mod partition;
pub mod schema;
pub mod typemap;
pub mod value;

use serde::{Deserialize, Serialize};

pub use metrics::{InMemoryMetrics, Labels, Metrics, NoopMetrics};

pub use event::{ChangeEvent, ColumnName, Op, Row};
pub use io::{Clock, IdGen, Spawner, Timestamp, WorkerId};
pub use lsn::Lsn;
pub use partition::{
    apply_transform, parse_partition_expr, parse_partition_spec, PartitionField, PartitionLiteral,
    Transform,
};
pub use schema::{ColumnSchema, Namespace, TableIdent, TableSchema};
pub use typemap::{map_pg_to_iceberg, IcebergType, MapError, PgType};
pub use value::{IcebergValue, PgValue};

/// The pipeline-mode tag. Configured per-deployment from
/// `source.mode` in YAML; not persisted (the coord doesn't store it
/// and `pg2iceberg cleanup` doesn't need to know about it).
///
/// Used at runtime by:
/// - `pg2iceberg-validate::validate_startup` to gate logical-only
///   invariants (e.g. `SnapshotCompleteButLsnZero`).
/// - The CLI dispatcher in `pg2iceberg-rust/crates/pg2iceberg/src/run.rs`.
#[derive(Copy, Clone, Default, Eq, PartialEq, Debug, Serialize, Deserialize)]
pub enum Mode {
    #[default]
    Logical,
    Query,
}

impl Mode {
    pub fn as_str(self) -> &'static str {
        match self {
            Mode::Logical => "logical",
            Mode::Query => "query",
        }
    }
}
