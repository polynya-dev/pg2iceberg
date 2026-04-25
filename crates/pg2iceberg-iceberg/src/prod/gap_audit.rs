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
//! evolve_schema             → iceberg::transaction::Transaction::update_schema ✅ wired (via fork)
//! snapshots                 → table.metadata().snapshots() + manifest walk     ✅ wired
//! ```
//!
//! See [`super::catalog::IcebergRustCatalog`] for the trait impl, and
//! `prod/smoke.rs` + `prod/catalog.rs::tests` for the proofs.
//!
//! ## We're on a patched fork of iceberg-rust
//!
//! The workspace `Cargo.toml` pins `iceberg` to
//! `polynya-dev/iceberg-rust` (branch `polynya-patches`), based on
//! upstream `v0.9.0`. The fork carries two non-invasive patches:
//!
//! - `TransactionAction` trait (and `BoxedTransactionAction`) flipped
//!   from `pub(crate)` to `pub`. Lets us author custom transaction
//!   actions in downstream crates.
//! - New `UpdateSchemaAction`. Takes a target `Schema` and emits
//!   `TableUpdate::AddSchema` + `TableUpdate::SetCurrentSchema(-1)` plus
//!   `UuidMatch` / `CurrentSchemaIdMatch` / `LastAssignedFieldIdMatch`
//!   requirements. Wrapped by a `Transaction::update_schema()`
//!   convenience method matching upstream conventions.
//!
//! Both changes are intended to be upstreamed — once they land we drop
//! the fork and pin to a fresh crates.io release.
//!
//! ## Schema evolution wiring
//!
//! `IcebergRustCatalog::evolve_schema` translates our `Vec<SchemaChange>`
//! onto upstream by:
//!
//! 1. Loading the current iceberg `Schema`, translating it back to our
//!    `TableSchema` via `from_iceberg_schema`.
//! 2. Calling [`crate::apply_schema_changes`] (shared with the sim
//!    catalog) — `AddColumn` appends with `field_id = max + 1`,
//!    `DropColumn` is a soft drop that flips `nullable = true`.
//! 3. Translating the evolved `TableSchema` back to an iceberg `Schema`
//!    and submitting it via `Transaction::update_schema()` from the fork.
//!
//! Both catalogs share `apply_schema_changes`, so the two backends never
//! drift on field-id allocation or soft-drop semantics. The
//! `evolve_schema_*` tests in `prod/catalog.rs::tests` and
//! `pg2iceberg-sim::catalog::tests` exercise the same change set against
//! both.
//!
//! ## Equality-delete write path is upstream-blocked
//!
//! - `iceberg::transaction::FastAppendAction::validate_added_data_files`
//!   in 0.9 explicitly rejects any `DataFile` whose `content_type()` is
//!   not `DataContentType::Data` with the error
//!   `"Only data content type is allowed for fast append"`. There is no
//!   other public commit action in 0.9 — `RowDelta` / `MergeAppend` /
//!   delete-aware actions don't exist upstream.
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
//! Closing this is a **follow-on patch on the same fork** — author a
//! `RowDeltaAction` (or relax `FastAppendAction` to accept all content
//! types, splitting manifests by content). Doable, deferred until we
//! tackle equality deletes specifically.
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
//! tests use `iceberg::memory::MemoryCatalog`. Other backends published
//! at 0.9 (drop-in for the `inner` field):
//!
//! - `iceberg-catalog-rest` — covers AWS Glue, Polaris, Tabular,
//!   Snowflake-managed-catalog. Auth flavors: bearer, sigv4, oauth2.
//! - `iceberg-catalog-glue` — direct AWS Glue.
//! - `iceberg-catalog-sql` — SQL DB (Postgres/SQLite) as metadata
//!   backing. Useful for self-hosted setups.
//! - `iceberg-catalog-s3tables` — AWS S3 Tables (managed Iceberg).
//! - `iceberg-catalog-hms` — Hive Metastore.
//!
//! ## Remaining items for the prod path
//!
//! 1. Equality-delete commit path — author a delete-aware action on the
//!    fork (`RowDeltaAction`).
//! 2. Drive an end-to-end materializer flush through
//!    `IcebergRustCatalog<MemoryCatalog>` in an integration test (proves
//!    real BlobStore + catalog interop, not just per-method translation).
//! 3. Wire `IcebergRustCatalog` into the binary, replacing the sim
//!    catalog as the default at runtime.

#![allow(clippy::doc_markdown)]
