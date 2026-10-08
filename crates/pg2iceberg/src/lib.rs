//! Library entrypoints for the `pg2iceberg` binary crate.
//!
//! These modules contain the prod-component wiring (`setup` builds
//! the `LogicalLifecycle` from a YAML `Config`, `run` drives the
//! lifecycle helper in `pg2iceberg-validate`).
//! They live behind a tiny `lib.rs` so integration tests can re-use
//! the exact wiring the binary uses, instead of duplicating it.
//!
//! `main.rs` is the CLI entrypoint and re-uses these via the lib.

pub mod config;
pub mod init;
pub mod realio;
pub mod run;
pub mod setup;
pub mod snapshot_src;
pub mod tables;
pub mod telemetry;
