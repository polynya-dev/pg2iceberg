//! Production-impl glue for the [`crate::Catalog`] trait, wrapping the
//! upstream Apache `iceberg-rust` crate (pinned to our
//! `polynya-dev/iceberg-rust` fork — see [`gap_audit`] for what's
//! patched and why).
//!
//! Allowlisted to use real IO via `crates/pg2iceberg-iceberg/src/prod/`
//! per the banned-calls CI script.
//!
//! ## Status
//!
//! Append-only commits, namespace + table CRUD, snapshot reads, and
//! schema evolution are all wired through [`IcebergRustCatalog`]. See
//! [`gap_audit`] for the full method-by-method status.
//!
//! One remaining gap:
//!
//! - **Equality-delete commits** — upstream's `FastAppendAction` rejects
//!   non-Data files and there's no public delete-aware action. Surfaced
//!   as a clear `IcebergError::Other` so callers know the path is
//!   unsupported rather than silently broken. Closing this is the next
//!   patch on the fork.
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
