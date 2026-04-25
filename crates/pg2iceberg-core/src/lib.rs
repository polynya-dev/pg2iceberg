//! Core types and IO traits for pg2iceberg.
//!
//! This crate has zero IO dependencies. It owns the type vocabulary and the
//! traits that production impls and the simulation impls both implement.

pub mod checkpoint;
pub mod event;
pub mod io;
pub mod lsn;
pub mod metrics;
pub mod schema;
pub mod typemap;
pub mod value;

pub use metrics::{InMemoryMetrics, Labels, Metrics, NoopMetrics};

pub use checkpoint::{Checkpoint, Mode, SnapshotState};
pub use event::{ChangeEvent, ColumnName, Op, Row};
pub use io::{Clock, IdGen, Spawner, Timestamp, WorkerId};
pub use lsn::Lsn;
pub use schema::{ColumnSchema, Namespace, TableIdent, TableSchema};
pub use typemap::{map_pg_to_iceberg, IcebergType, MapError, PgType};
pub use value::{IcebergValue, PgValue};
