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

pub use metrics::{InMemoryMetrics, Labels, Metrics, NoopMetrics, Phase, Registry};

pub use event::{is_snapshot_xid, ChangeEvent, ColumnName, Op, Row, SNAPSHOT_XID_BASE};
pub use io::{Clock, IdGen, Spawner, Timestamp, WorkerId};
pub use lsn::Lsn;
pub use partition::{
    apply_transform, parse_partition_expr, parse_partition_spec, PartitionField, PartitionLiteral,
    Transform,
};
pub use schema::{ColumnSchema, Namespace, TableIdent, TableSchema};
pub use typemap::{map_pg_to_iceberg, IcebergType, MapError, PgType};
pub use value::{IcebergValue, PgValue};
