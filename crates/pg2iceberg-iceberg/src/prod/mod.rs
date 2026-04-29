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
//! All trait methods on [`IcebergRustCatalog`] are wired end-to-end:
//! namespace + table CRUD, append-only commits, **equality-delete
//! commits**, schema evolution, and snapshot reads. See [`gap_audit`]
//! for the method-by-method status and the list of fork patches we
//! depend on.
//!
//! Smoke tests (`smoke.rs`) exercise the upstream surface directly to
//! catch breakage on `iceberg-rust` upgrades. Per-method translation
//! tests in [`catalog::tests`](self::catalog) exercise our adapter.
//! Both run under `--features prod`.

pub mod catalog;
pub mod gap_audit;
pub mod vended;

pub use catalog::IcebergRustCatalog;
pub use vended::{
    build_per_table_object_store, bucket_from_location, extract_creds, warehouse_relative_path,
    VendedBlobStoreRouter, VendedCreds, VendedRouterConfig, DEFAULT_REFRESH_BUFFER,
    DEFAULT_VENDED_TTL,
};

#[cfg(test)]
mod smoke;
