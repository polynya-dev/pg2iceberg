//! Production-impl glue for the `Catalog` trait, wrapping the upstream
//! Apache `iceberg-rust` crate.
//!
//! Allowlisted to use real IO via `crates/pg2iceberg-iceberg/src/prod/`
//! per the banned-calls CI script.
//!
//! ## Status
//!
//! This module is in the **gap-audit phase** (per port-plan §10). Built
//! against `iceberg-rust 0.9`. The smoke test confirms the primitive type
//! surface we need for the `Catalog` wrap:
//!
//! - `Schema::builder().with_fields(...)` accepts the field-id +
//!   nested-field shape our `pg2iceberg_core::ColumnSchema` produces.
//! - All Iceberg primitives we emit (Bool/Int/Long/Float/Double/String/
//!   Date/Timestamp/TimestampTz) round-trip through `PrimitiveType`.
//! - `NamespaceIdent` / `TableIdent` accept our string-based namespace
//!   shape, including nested namespaces.
//!
//! What we deliberately **don't yet exercise**, with rationale in
//! [`gap_audit`]:
//!
//! - **A real catalog backend.** `iceberg-catalog-memory` exists only in
//!   the upstream `apache/iceberg-rust` git source (not on crates.io).
//!   `iceberg-catalog-rest` / `glue` / `sql` all require a live backing
//!   service. Wiring those is a Phase 13.5 task — likely we'll vendor
//!   the memory catalog or pull it via a git path dep for tests.
//! - **Equality-delete writes**. We need a way to register pre-built
//!   data files (with `content = 2` and `equality_field_ids`) into a
//!   snapshot. Uncertain whether the public Transaction API in 0.9
//!   exposes that surface.
//! - **Schema evolution.** Add column / drop column / type widening
//!   through `Transaction`. Unverified.
//! - **REST/Glue catalog auth flavors.** Plumbing exists in the separate
//!   catalog crates; not exercised here.
//!
//! Once the gaps are resolved a follow-on phase will land an
//! `IcebergRustCatalog` impl of our `Catalog` trait.

pub mod gap_audit;

#[cfg(test)]
mod smoke;
