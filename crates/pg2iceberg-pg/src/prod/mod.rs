//! Production-impl glue for the [`crate::PgClient`] +
//! [`crate::ReplicationStream`] traits, built on `tokio-postgres` and
//! `postgres-replication` (Supabase etl's fork — see workspace
//! `Cargo.toml` for context).
//!
//! Allowlisted to use real IO via `crates/pg2iceberg-pg/src/prod/`
//! per the banned-calls CI script.
//!
//! Module map:
//!
//! - [`typemap`] — Postgres OID + type-modifier → our [`pg2iceberg_core::PgType`].
//! - [`value_decode`] — text-format pgoutput tuple bytes →
//!   [`pg2iceberg_core::PgValue`] per column type.
//! - [`client`] — [`PgClientImpl`] driving non-replication SQL: publication
//!   create/check, slot create/inspect, snapshot export.
//! - [`stream`] — [`ReplicationStreamImpl`] wrapping
//!   `postgres_replication::LogicalReplicationStream` and translating
//!   `LogicalReplicationMessage` → our [`crate::DecodedMessage`].

pub mod client;
pub mod stream;
pub mod typemap;
pub mod value_decode;

pub use client::PgClientImpl;
pub use stream::ReplicationStreamImpl;
