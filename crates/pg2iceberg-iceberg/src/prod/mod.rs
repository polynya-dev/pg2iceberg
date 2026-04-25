//! Production-impl glue for the [`crate::Catalog`] trait, wrapping the
//! upstream Apache `iceberg-rust` 0.9 crate.
//!
//! Allowlisted to use real IO via `crates/pg2iceberg-iceberg/src/prod/`
//! per the banned-calls CI script.
//!
//! ## Status (post-Phase 13)
//!
//! Append-only commits, namespace + table CRUD, and snapshot reads are
//! all wired through [`IcebergRustCatalog`]. See [`gap_audit`] for the
//! full method-by-method status. The two remaining gaps:
//!
//! - **Equality-delete commits** — upstream-blocked on iceberg-rust 0.9.
//!   Surfaced as a clear `IcebergError::Other` so callers know the path
//!   is unsupported rather than silently broken.
//! - **Schema evolution** — `evolve_schema` returns "not yet wired".
//!   Needs an `UpdateSchema` action wiring; tracked alongside CDC-side
//!   DDL handling.
//!
//! Smoke tests (`smoke.rs`) exercise the upstream surface directly to
//! catch breakage on `iceberg-rust` upgrades. Per-method translation
//! tests in [`catalog::tests`](self::catalog) exercise our adapter.
//! Both run under `--features prod`.

pub mod catalog;
pub mod gap_audit;

pub use catalog::IcebergRustCatalog;

#[cfg(test)]
mod smoke;
