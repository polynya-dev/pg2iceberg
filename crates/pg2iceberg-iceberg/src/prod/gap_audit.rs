//! What `iceberg-rust` (our fork) supports vs. what `pg2iceberg-iceberg`'s
//! `Catalog` trait needs. Updated as we close gaps.
//!
//! ## Methods we need to translate
//!
//! ```text
//! ensure_namespace          → iceberg::Catalog::create_namespace               ✅ wired
//! load_table                → iceberg::Catalog::load_table                     ✅ wired
//! create_table              → iceberg::Catalog::create_table                   ✅ wired
//! commit_snapshot (append)  → iceberg::transaction::Transaction::fast_append   ✅ wired
//! commit_snapshot (deletes) → fork-patched FastAppendAction routes by type     ✅ wired (via fork)
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
//! upstream `v0.9.0`. The fork carries three non-invasive patches:
//!
//! 1. **`TransactionAction` visibility.** The trait (and
//!    `BoxedTransactionAction`) flipped from `pub(crate)` to `pub`. Lets
//!    downstream crates author custom transaction actions.
//! 2. **`UpdateSchemaAction`.** Takes a target `Schema`, emits
//!    `TableUpdate::AddSchema` + `TableUpdate::SetCurrentSchema(-1)` plus
//!    `UuidMatch` / `CurrentSchemaIdMatch` / `LastAssignedFieldIdMatch`
//!    requirements. Wrapped by a `Transaction::update_schema()`
//!    convenience method matching upstream conventions.
//! 3. **`FastAppendAction` accepts mixed content types.** Files passed to
//!    `add_data_files` are routed by `content_type()` into
//!    `added_data_files` (Data) or `added_delete_files`
//!    (EqualityDeletes / PositionDeletes). `SnapshotProducer` writes them
//!    to separate manifests (`build_v{2,3}_data` vs
//!    `build_v{2,3}_deletes`) per Iceberg-spec rules. The old
//!    "Only data content type is allowed for fast append" check is gone.
//!
//! All three are intended to be upstreamed as separate PRs; once they
//! land we drop the fork and pin to a fresh crates.io release.
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
//! ## Equality-delete commit wiring
//!
//! `IcebergRustCatalog::commit_snapshot` builds a single
//! `Vec<iceberg::spec::DataFile>` from `prepared.data_files` (Data) and
//! `prepared.equality_deletes` (EqualityDeletes), and hands the lot to
//! `FastAppendAction::add_data_files`. The fork's action splits by
//! `content_type()` and `SnapshotProducer` writes two manifests — one
//! data, one deletes — both attached to the same snapshot.
//!
//! Pre-flight check: equality-delete files with empty
//! `equality_field_ids` are rejected before submission, since they would
//! match either no rows or every row depending on reader and silently
//! corrupt the table either way.
//!
//! Position deletes: not used yet. Our `TableWriter` only emits equality
//! deletes today. RisingWave's `DeltaWriter` shows the optimization path
//! (in-batch self-cancellation via position deletes), but it's a separate
//! optimization — read-side correctness already works with equality
//! deletes only. Revisit if profiling shows in-batch insert/delete churn
//! is hot.
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
//! 1. Drive an end-to-end materializer flush through
//!    `IcebergRustCatalog<MemoryCatalog>` in an integration test (proves
//!    real BlobStore + catalog interop, not just per-method translation).
//! 2. Wire `IcebergRustCatalog` into the binary, replacing the sim
//!    catalog as the default at runtime.
//! 3. (Optional optimization) Adopt RisingWave-style position deletes
//!    for in-batch self-cancellation. Equality deletes alone are correct.

#![allow(clippy::doc_markdown)]
