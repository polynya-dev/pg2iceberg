//! What `iceberg-rust` 0.9 supports vs. what `pg2iceberg-iceberg`'s
//! `Catalog` trait needs. Updated as we close gaps.
//!
//! ## Methods we need to translate
//!
//! ```text
//! ensure_namespace          → iceberg::Catalog::create_namespace               ✅ wired
//! load_table                → iceberg::Catalog::load_table                     ✅ wired
//! create_table              → iceberg::Catalog::create_table                   ✅ wired
//! commit_snapshot (append)  → iceberg::transaction::Transaction::fast_append   ✅ wired
//! commit_snapshot (deletes) → no public action accepts non-Data files          🔴 upstream-blocked
//! evolve_schema             → iceberg::transaction::* (UpdateSchema)           🟡 not yet wired
//! snapshots                 → table.metadata().snapshots() + manifest walk     ✅ wired
//! ```
//!
//! See [`super::catalog::IcebergRustCatalog`] for the trait impl, and
//! `prod/smoke.rs` + `prod/catalog.rs::tests` for the proofs.
//!
//! ## Equality-delete write path is upstream-blocked
//!
//! - `iceberg::transaction::FastAppendAction::validate_added_data_files`
//!   in 0.9 explicitly rejects any `DataFile` whose `content_type()` is
//!   not `DataContentType::Data` with the error
//!   `"Only data content type is allowed for fast append"`. There is no
//!   other public commit action in 0.9 — `RowDelta` / `MergeAppend` /
//!   delete-aware actions don't exist.
//! - The lower-level escape hatch
//!   `iceberg::TableCommit::builder()` is **`pub(crate)`** in 0.9, so we
//!   cannot construct a `TableCommit` with a hand-built
//!   `TableUpdate::AddSnapshot` from outside the crate.
//! - There IS an `iceberg::writer::base_writer::EqualityDeleteFileWriter`
//!   that *writes* a delete file (returning a `DataFile` with
//!   `content = EqualityDeletes`) — but no way to commit it via the
//!   public Catalog/Transaction surface.
//!
//! Consequences for `IcebergRustCatalog::commit_snapshot`:
//!
//! - When `prepared.equality_deletes` is empty, we use `FastAppendAction`
//!   and the commit succeeds.
//! - When `prepared.equality_deletes` is non-empty, we return
//!   `IcebergError::Other("equality-delete commits are not yet supported
//!   by the iceberg-rust prod backend...")`. Callers that need
//!   upserts/deletes today must run against the sim catalog.
//!
//! Two options for closing this:
//!
//! 1. **Wait for upstream.** Track the iceberg-rust transaction module
//!    for delete-aware actions (`RowDelta`, `MergeAppend`, etc.).
//! 2. **Vendor a slim fork.** Vendor `apache/iceberg-rust` as a path dep
//!    and patch `TableCommit::builder` visibility to `pub`. Construct
//!    `TableUpdate::AddSnapshot { snapshot }` with a hand-built
//!    `Snapshot` that references both data and delete manifests.
//!    Doable but invasive — defer until we see real demand.
//!
//! ## Schema evolution
//!
//! `evolve_schema` returns `IcebergError::Other("not yet wired")`.
//! `iceberg-rust` exposes `Transaction::update_table_properties` and
//! related builders, but the materializer doesn't drive DDL through the
//! prod catalog yet. When we add CDC-side DDL handling we'll need to
//! map our `SchemaChange` variants onto upstream `UpdateSchema` action(s).
//!
//! ## Snapshot ID semantics
//!
//! `iceberg-rust` generates `snapshot_id` as a random 63-bit value (UUID
//! ⊕ timestamp). Our `Snapshot.id` is used by the verifier and FileIndex
//! for monotonic ordering (`delete.id > data.id`), so our wrapper
//! reports `sequence_number` (which **is** monotonic) as `Snapshot.id`
//! and as `TableMetadata::current_snapshot_id`. The internal iceberg
//! `snapshot_id` is only used to filter manifest entries by
//! `added_snapshot_id` during the walk.
//!
//! ## Read-path
//!
//! `read_materialized_state` currently reads our own Parquet files via
//! `pg2iceberg-iceberg::reader::read_data_file`, applying our own MoR
//! semantics (delete file at snap N voids data files at snap < N). Once
//! the prod catalog is the default in the binary we can swap the
//! verifier to iceberg-rust's reader for parity with other Iceberg
//! consumers — that's a follow-on.
//!
//! ## Catalog backends
//!
//! `IcebergRustCatalog<C>` is generic over any `iceberg::Catalog`. The
//! tests use `iceberg::memory::MemoryCatalog` (now public on crates.io
//! at 0.9 — earlier audits assumed it was git-only). Other backends
//! published at 0.9:
//!
//! - `iceberg-catalog-rest` — covers AWS Glue, Polaris, Tabular,
//!   Snowflake-managed-catalog. Auth flavors: bearer, sigv4, oauth2.
//! - `iceberg-catalog-glue` — direct AWS Glue.
//! - `iceberg-catalog-sql` — SQL DB (Postgres/SQLite) as metadata
//!   backing. Useful for self-hosted setups.
//! - `iceberg-catalog-s3tables` — AWS S3 Tables (managed Iceberg).
//! - `iceberg-catalog-hms` — Hive Metastore.
//!
//! All five are drop-in candidates for the `inner` field of
//! `IcebergRustCatalog` once we wire them in the binary.
//!
//! ## Remaining items for the prod path
//!
//! 1. Drive an end-to-end materializer flush through
//!    `IcebergRustCatalog<MemoryCatalog>` in an integration test (proves
//!    real BlobStore + catalog interop, not just per-method translation).
//! 2. Schema evolution (`evolve_schema` → `UpdateSchema`).
//! 3. Equality-delete commit path — track upstream.
//! 4. Wire `IcebergRustCatalog` into the binary, replacing the sim
//!    catalog as the default at runtime.

#![allow(clippy::doc_markdown)]
