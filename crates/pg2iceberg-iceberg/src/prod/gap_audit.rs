//! What `iceberg-rust` 0.9 supports vs. what `pg2iceberg-iceberg`'s
//! `Catalog` trait needs. Updated as we close gaps.
//!
//! ## Methods we need to translate
//!
//! ```text
//! ensure_namespace          → iceberg::Catalog::create_namespace               ✅ supported
//! load_table                → iceberg::Catalog::load_table                     ✅ supported
//! create_table              → iceberg::Catalog::create_table                   ✅ supported
//! commit_snapshot           → iceberg::transaction::Transaction::commit        🟡 partial
//! evolve_schema             → iceberg::transaction::* (UpdateSchema?)          🔴 unverified
//! snapshots                 → table.metadata().snapshots() (read-only)         ✅ supported
//! ```
//!
//! ## Write-path open questions
//!
//! - **Equality delete files.** Our `PreparedCommit::equality_deletes`
//!   carries Parquet bytes already produced by `TableWriter::prepare`. To
//!   surface them as Iceberg manifest entries we need a way to register
//!   pre-built data files (with `content = 2` and `equality_field_ids`)
//!   into a snapshot. iceberg-rust's `DataFileBuilder` may or may not
//!   expose `content_type` and `equality_ids` setters at the public
//!   surface in 0.9. Needs spike.
//!
//! - **Position deletes.** Out of scope for pg2iceberg (we only do
//!   equality deletes), but the same registration question applies.
//!
//! - **Append-only optimization.** When the materializer flushes a
//!   batch with no equality deletes (initial-snapshot path), we should
//!   be able to use a faster code path. Not yet investigated.
//!
//! ## Read-path
//!
//! `read_materialized_state` currently reads our own Parquet files via
//! `pg2iceberg-iceberg::reader::read_data_file`, applying our own MoR
//! semantics (delete file at snap N voids data files at snap < N). Once
//! iceberg-rust catalog is wired in, we can swap to iceberg-rust's
//! reader for parity with other Iceberg consumers — that's a Phase 13.5
//! task.
//!
//! ## Catalog backends
//!
//! Published on crates.io at 0.9:
//! - `iceberg-catalog-rest` — covers AWS Glue, Polaris, Tabular,
//!   Snowflake-managed-catalog. Auth flavors: bearer, sigv4, oauth2.
//! - `iceberg-catalog-glue` — direct AWS Glue.
//! - `iceberg-catalog-sql` — uses a SQL DB (Postgres/SQLite) as the
//!   metadata backing. Useful for self-hosted setups.
//! - `iceberg-catalog-s3tables` — AWS S3 Tables (managed Iceberg).
//! - `iceberg-catalog-hms` — Hive Metastore.
//!
//! Not on crates.io (only in apache/iceberg-rust git source):
//! - `iceberg-catalog-memory` — in-process, in-memory metadata. Useful
//!   for tests but requires a git path dep until they publish it.
//!
//! ## Action items for Phase 13.5
//!
//! 1. Vendor or git-dep `iceberg-catalog-memory` so we can unit-test
//!    against an in-memory catalog without a network service.
//! 2. Spike a minimal end-to-end write: create table, append data file,
//!    commit snapshot. If `iceberg::transaction::Transaction` exposes
//!    `append_files` / equivalent, we're 80% done. If not, evaluate
//!    writing manifests ourselves on top of iceberg-rust's
//!    metadata-only support.
//! 3. Audit equality-delete write path. File upstream issue if blocked.
//! 4. Wire `IcebergRustCatalog` impl of our `Catalog` trait, replacing
//!    `MemoryCatalog` in the binary.

#![allow(clippy::doc_markdown)]
