//! `pg2iceberg run`: assemble all four prod surfaces and drive the
//! pipeline.
//!
//! Loop shape (mirrors `pg2iceberg-logical::runner` doctest):
//!
//! 1. `select!` between `stream.recv()`, the ticker timeout, and
//!    SIGINT.
//! 2. Each `recv` yields a `DecodedMessage` we feed to
//!    `Pipeline::process`.
//! 3. When the ticker fires, we run the due handlers in stable order:
//!    Flush → Standby → Materialize → Watcher. Watcher runs the
//!    `pg2iceberg-validate` invariant checks against live coord +
//!    pipeline + slot state and logs/counts any violations.
//! 4. SIGINT calls `Pipeline::shutdown` and exits cleanly.

use crate::config::Config;
use crate::snapshot_src::PgSnapshotSource;
use anyhow::{Context, Result};
use async_trait::async_trait;
use pg2iceberg_coord::{
    prod::{connect_with as coord_connect_with, PostgresCoordinator, TlsMode as CoordTls},
    schema::CoordSchema,
    Coordinator,
};
use pg2iceberg_core::{IdGen, TableSchema};
use pg2iceberg_iceberg::prod::IcebergRustCatalog;
use pg2iceberg_logical::{
    materializer::CounterMaterializerNamer, pipeline::BlobNamer, Materializer,
};
use pg2iceberg_pg::prod::{PgClientImpl, TlsMode as PgTls};
use pg2iceberg_stream::{prod::ObjectStoreBlobStore, BlobStore};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::signal::unix::{signal, SignalKind};

/// Production blob namer. Uses an [`IdGen`]-supplied UUID per blob so
/// uploaded paths never collide across processes.
struct UuidBlobNamer<I: IdGen> {
    id_gen: Arc<I>,
    base: String,
}

impl<I: IdGen> UuidBlobNamer<I> {
    fn new(id_gen: Arc<I>, base: impl Into<String>) -> Self {
        Self {
            id_gen,
            base: base.into(),
        }
    }
}

#[async_trait]
impl<I: IdGen + 'static> BlobNamer for UuidBlobNamer<I> {
    async fn next_blob_path(&self, table: &str) -> String {
        let bytes = self.id_gen.new_uuid();
        let hex = bytes.iter().map(|b| format!("{b:02x}")).collect::<String>();
        format!(
            "{}/{}/{}.parquet",
            self.base.trim_end_matches('/'),
            table,
            hex
        )
    }
}

pub async fn run(cfg: Config) -> Result<()> {
    let blob = build_blob(&cfg).context("build blob store")?;
    if cfg.sink.catalog_uri.is_empty() {
        anyhow::bail!("sink.catalog_uri is required");
    }
    let catalog = build_rest_catalog(&cfg).await?;
    let catalog = IcebergRustCatalog::new(Arc::new(catalog));
    match cfg.source.mode.as_str() {
        "" | "logical" => run_inner(cfg, catalog, blob).await,
        "query" => run_query(cfg, catalog, blob).await,
        other => anyhow::bail!(
            "unknown source.mode {other:?}; expected one of: logical, query"
        ),
    }
}

/// Build a REST `iceberg::Catalog` from sink config. We default to the
/// REST flavor because that's the only iceberg-rust catalog backend
/// that covers the common cloud / managed-Iceberg deployments today
/// (Polaris, Tabular, Snowflake, Iceberg-REST reference). Other
/// flavors are follow-ons.
pub async fn build_rest_catalog(cfg: &Config) -> Result<iceberg_catalog_rest::RestCatalog> {
    use iceberg::CatalogBuilder;
    use iceberg_catalog_rest::RestCatalogBuilder;
    use iceberg_storage_opendal::OpenDalStorageFactory;
    let props: HashMap<String, String> = cfg.rest_catalog_props().into_iter().collect();
    // iceberg-rust 0.9's REST catalog requires a StorageFactory or
    // every file-IO operation panics with "StorageFactory must be
    // provided". OpenDAL's S3 backend is what the upstream
    // integration tests use; it picks up endpoint / credentials
    // from the catalog config response (REST `/v1/config`) plus
    // any overrides we pass through `props`. Surfaced by the
    // testcontainers integration test.
    RestCatalogBuilder::default()
        .with_storage_factory(Arc::new(OpenDalStorageFactory::S3 {
            configured_scheme: "s3".to_string(),
            customized_credential_load: None,
        }))
        .load("pg2iceberg", props)
        .await
        .context("RestCatalog load")
}

fn build_blob(cfg: &Config) -> Result<Arc<dyn BlobStore>> {
    match cfg.sink.credential_mode.as_str() {
        "static" => build_s3_static(cfg),
        "iam" => build_s3_iam(cfg),
        "vended" => Err(anyhow::anyhow!(
            "credential_mode=vended is not yet wired in the Rust port; \
             see plan §Phase 7 vended-credentials S3 router"
        )),
        other => Err(anyhow::anyhow!(
            "unknown credential_mode {other:?}; expected one of: static, vended, iam"
        )),
    }
}

fn build_s3_static(cfg: &Config) -> Result<Arc<dyn BlobStore>> {
    if cfg.sink.warehouse.is_empty() {
        anyhow::bail!("sink.warehouse is required for credential_mode=static");
    }
    if cfg.sink.s3_endpoint.is_empty() {
        anyhow::bail!("sink.s3_endpoint is required for credential_mode=static");
    }
    let bucket = bucket_from_warehouse(&cfg.sink.warehouse)?;
    let prefix = prefix_from_warehouse(&cfg.sink.warehouse);
    let inner = object_store::aws::AmazonS3Builder::new()
        .with_bucket_name(&bucket)
        .with_region(&cfg.sink.s3_region)
        .with_endpoint(&cfg.sink.s3_endpoint)
        .with_access_key_id(&cfg.sink.s3_access_key)
        .with_secret_access_key(&cfg.sink.s3_secret_key)
        // Path-style is the safe default for non-AWS S3 (MinIO, LocalStack).
        // AWS itself accepts both; it's only newer endpoints that are
        // virtual-hosted-only, and we'd flip this when we hit one.
        .with_virtual_hosted_style_request(false)
        .build()
        .context("AmazonS3Builder build")?;
    let store: Arc<dyn object_store::ObjectStore> = if let Some(p) = prefix {
        Arc::new(object_store::prefix::PrefixStore::new(inner, p))
    } else {
        Arc::new(inner)
    };
    Ok(Arc::new(ObjectStoreBlobStore::new(store)))
}

fn build_s3_iam(cfg: &Config) -> Result<Arc<dyn BlobStore>> {
    if cfg.sink.warehouse.is_empty() {
        anyhow::bail!("sink.warehouse is required for credential_mode=iam");
    }
    let bucket = bucket_from_warehouse(&cfg.sink.warehouse)?;
    let prefix = prefix_from_warehouse(&cfg.sink.warehouse);
    let mut builder = object_store::aws::AmazonS3Builder::from_env()
        .with_bucket_name(&bucket)
        .with_region(&cfg.sink.s3_region);
    if !cfg.sink.s3_endpoint.is_empty() {
        builder = builder.with_endpoint(&cfg.sink.s3_endpoint);
    }
    let inner = builder.build().context("AmazonS3Builder build")?;
    let store: Arc<dyn object_store::ObjectStore> = if let Some(p) = prefix {
        Arc::new(object_store::prefix::PrefixStore::new(inner, p))
    } else {
        Arc::new(inner)
    };
    Ok(Arc::new(ObjectStoreBlobStore::new(store)))
}

/// Extract the bucket name from `s3://bucket/path` or `s3a://bucket/path`.
fn bucket_from_warehouse(warehouse: &str) -> Result<String> {
    let stripped = warehouse
        .strip_prefix("s3://")
        .or_else(|| warehouse.strip_prefix("s3a://"))
        .with_context(|| format!("warehouse must start with s3:// or s3a://, got {warehouse:?}"))?;
    let bucket = stripped
        .split('/')
        .next()
        .filter(|s| !s.is_empty())
        .with_context(|| format!("warehouse missing bucket: {warehouse:?}"))?;
    Ok(bucket.to_string())
}

/// Extract the path-prefix portion of `s3://bucket/prefix/...`. Returns
/// `None` if the warehouse points directly at the bucket root.
fn prefix_from_warehouse(warehouse: &str) -> Option<String> {
    let stripped = warehouse
        .strip_prefix("s3://")
        .or_else(|| warehouse.strip_prefix("s3a://"))?;
    let mut parts = stripped.splitn(2, '/');
    parts.next()?;
    let p = parts.next()?.trim_matches('/');
    if p.is_empty() {
        None
    } else {
        Some(p.to_string())
    }
}

async fn run_inner<C>(
    cfg: Config,
    catalog: IcebergRustCatalog<C>,
    blob: Arc<dyn BlobStore>,
) -> Result<()>
where
    C: iceberg::Catalog + Send + Sync + 'static,
{
    // Build prod components (PG connection, coord migrate, schema
    // discovery, blob namer) and assemble the `LogicalLifecycle`.
    // Everything past this is library code in
    // `pg2iceberg_validate::run_logical_lifecycle` — slot creation,
    // table registration, consumer heartbeat, snapshot decision,
    // main loop, drain. The fault-DST exercises the same lifecycle
    // helper with sim plumbing, so any change to lifecycle behavior
    // gets fault-tested.
    let id_gen = Arc::new(crate::realio::RealIdGen::new());
    let blob_namer: Arc<dyn pg2iceberg_logical::pipeline::BlobNamer> =
        Arc::new(UuidBlobNamer::new(id_gen.clone(), "staged"));
    let lifecycle = crate::setup::build_logical_lifecycle(&cfg, catalog, blob, blob_namer)
        .await
        .context("build logical lifecycle")?;

    let mut sigint = signal(SignalKind::interrupt()).context("install SIGINT handler")?;
    let mut sigterm = signal(SignalKind::terminate()).context("install SIGTERM handler")?;
    let shutdown = async move {
        tokio::select! {
            biased;
            _ = sigint.recv() => tracing::info!("SIGINT received, shutting down"),
            _ = sigterm.recv() => tracing::info!("SIGTERM received, shutting down"),
        }
    };

    tracing::info!("entering replication lifecycle");
    pg2iceberg_validate::run_logical_lifecycle(lifecycle, Box::pin(shutdown))
        .await
        .context("logical lifecycle")?;
    Ok(())
}


/// Query-mode driver. Polls each table for rows whose watermark
/// column has advanced past the stored cursor, dedups by PK, writes
/// directly to the materialized Iceberg table — no replication slot,
/// no staging Parquet, no coord log_seq/log_index. Persistence of the
/// per-table watermark goes into the same `_pg2iceberg.checkpoints`
/// table (`Checkpoint::query_watermarks`) so a restarted process
/// resumes from where the prior left off.
///
/// Each table needs `watermark_column` set in YAML. Watermark column
/// must be int/long/date/timestamp/timestamptz; other types are
/// rejected at startup.
async fn run_query<C>(
    cfg: Config,
    catalog: IcebergRustCatalog<C>,
    blob: Arc<dyn BlobStore>,
) -> Result<()>
where
    C: iceberg::Catalog + Send + Sync + 'static,
{
    // Build prod components, then hand off to
    // `pg2iceberg_query::run_query_lifecycle`. Same pattern as
    // logical mode: construction here, lifecycle orchestration in
    // library code so the fault-DST exercises the same path.
    let lifecycle = crate::setup::build_query_lifecycle(&cfg, catalog, blob).await?;

    let mut sigint = signal(SignalKind::interrupt()).context("install SIGINT handler")?;
    let mut sigterm = signal(SignalKind::terminate()).context("install SIGTERM handler")?;
    let shutdown = async move {
        tokio::select! {
            biased;
            _ = sigint.recv() => tracing::info!("SIGINT received, query-mode shutting down"),
            _ = sigterm.recv() => tracing::info!("SIGTERM received, query-mode shutting down"),
        }
    };

    pg2iceberg_query::run_query_lifecycle(lifecycle, Box::pin(shutdown))
        .await
        .context("query lifecycle")?;
    Ok(())
}

/// One-shot: run a compaction pass over every configured table, log
/// outcomes, exit. Mirrors Go's `pg2iceberg compact`.
pub async fn run_compact(cfg: Config) -> Result<()> {
    if cfg.sink.target_file_size == 0 {
        anyhow::bail!(
            "sink.target_file_size is 0 — compaction is disabled. \
             Set a non-zero value (e.g. 134217728 for 128 MiB) to enable."
        );
    }
    let mut materializer = build_one_shot_materializer(cfg.clone()).await?;
    let cfg_compact = cfg.sink.compaction_config();
    let outcomes = materializer
        .compact_cycle(&cfg_compact)
        .await
        .context("compact_cycle")?;
    if outcomes.is_empty() {
        tracing::info!("compaction: every table below threshold, nothing rewritten");
    } else {
        for (ident, o) in &outcomes {
            tracing::info!(
                table = %ident,
                in_data = o.input_data_files,
                in_del = o.input_delete_files,
                out_data = o.output_data_files,
                rows = o.rows_rewritten,
                rows_removed = o.rows_removed_by_deletes,
                bytes_before = o.bytes_before,
                bytes_after = o.bytes_after,
                "compacted"
            );
        }
    }
    Ok(())
}

/// One-shot: run maintenance over every configured table — snapshot
/// expiry first, then orphan-file cleanup — and exit. Mirrors Go's
/// `pg2iceberg maintain` (which does both phases in sequence).
///
/// Either retention or grace is required; the other is treated as a
/// no-op for that phase if blank.
///
/// CLI `--retention` (e.g. `168h`) overrides
/// `sink.maintenance_retention`. Cleanup grace and the materialized
/// prefix come from `sink.maintenance_grace` /
/// `sink.materialized_prefix`.
pub async fn run_maintain(cfg: Config, retention_override: Option<String>) -> Result<()> {
    let retention_str = retention_override
        .clone()
        .unwrap_or_else(|| cfg.sink.maintenance_retention.clone());
    let grace_str = cfg.sink.maintenance_grace.clone();
    if retention_str.is_empty() && grace_str.is_empty() {
        anyhow::bail!(
            "no maintenance work configured: set at least one of \
             `sink.maintenance_retention`, `sink.maintenance_grace`, or \
             pass `--retention 168h`"
        );
    }

    let mut materializer = build_one_shot_materializer(cfg.clone()).await?;

    // Phase 1: snapshot expiry.
    if !retention_str.is_empty() {
        let dur = humantime::parse_duration(&retention_str)
            .with_context(|| format!("parse retention `{retention_str}`"))?;
        let retention_ms: i64 = dur.as_millis().try_into().unwrap_or(i64::MAX);
        let outcomes = materializer
            .expire_cycle(retention_ms)
            .await
            .context("expire_cycle")?;
        if outcomes.is_empty() {
            tracing::info!(retention = %retention_str, "maintain: no snapshots to expire");
        } else {
            for (ident, n) in &outcomes {
                tracing::info!(table = %ident, expired = n, "expired snapshots");
            }
        }
    }

    // Phase 2: orphan-file cleanup.
    if !grace_str.is_empty() {
        let dur = humantime::parse_duration(&grace_str)
            .with_context(|| format!("parse maintenance_grace `{grace_str}`"))?;
        let grace_ms: i64 = dur.as_millis().try_into().unwrap_or(i64::MAX);
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis().try_into().unwrap_or(i64::MAX))
            .unwrap_or(i64::MAX);
        let outcomes = materializer
            .cleanup_orphans_cycle(&cfg.sink.materialized_prefix, now_ms, grace_ms)
            .await
            .context("cleanup_orphans_cycle")?;
        if outcomes.is_empty() {
            tracing::info!(
                grace = %grace_str,
                prefix = %cfg.sink.materialized_prefix,
                "maintain: no orphan files found"
            );
        } else {
            for (ident, o) in &outcomes {
                tracing::info!(
                    table = %ident,
                    deleted = o.deleted,
                    bytes_freed = o.bytes_freed,
                    grace_protected = o.grace_protected,
                    "cleanup orphans"
                );
            }
        }
    }

    Ok(())
}

/// Set up the same Materializer the main run loop builds, but without
/// opening the replication slot or installing signal handlers — for
/// one-shot subcommands (`compact`, `maintain`).
async fn build_one_shot_materializer(
    cfg: Config,
) -> Result<Materializer<IcebergRustCatalog<iceberg_catalog_rest::RestCatalog>>> {
    let blob = build_blob(&cfg).context("build blob store")?;
    if cfg.sink.catalog_uri.is_empty() {
        anyhow::bail!("sink.catalog_uri is required");
    }
    let catalog = Arc::new(IcebergRustCatalog::new(Arc::new(
        build_rest_catalog(&cfg).await?,
    )));

    let coord_dsn = cfg.coord_dsn();
    let coord_tls = match cfg.source.postgres.tls_label() {
        "webpki" => CoordTls::Webpki,
        _ => CoordTls::Disable,
    };
    let coord_conn = coord_connect_with(&coord_dsn, coord_tls)
        .await
        .context("coord connect")?;
    let coord_schema = CoordSchema::sanitize(&cfg.state.coordinator_schema);
    let coord: Arc<dyn Coordinator> = Arc::new(PostgresCoordinator::new(coord_conn, coord_schema));

    // Resolve schemas. Discovery requires PG; tables with explicit
    // columns can skip it.
    let needs_pg = cfg.tables.iter().any(|t| !t.has_explicit_columns());
    let pg_for_discovery = if needs_pg {
        let pg_tls = match cfg.source.postgres.tls_label() {
            "webpki" => PgTls::Webpki,
            _ => PgTls::Disable,
        };
        Some(
            PgClientImpl::connect_with(&cfg.source.postgres.dsn(), pg_tls)
                .await
                .context("PG connect for schema discovery")?,
        )
    } else {
        None
    };

    let mut resolved_schemas: Vec<pg2iceberg_core::TableSchema> =
        Vec::with_capacity(cfg.tables.len());
    for t in &cfg.tables {
        let schema = if t.has_explicit_columns() {
            t.to_table_schema()?
        } else {
            let (ns, name) = t.qualified()?;
            let pg = pg_for_discovery
                .as_ref()
                .expect("needs_pg above guards this");
            let mut s = pg
                .discover_schema(&ns, &name)
                .await
                .with_context(|| format!("discover schema for {}", t.name))?;
            if !t.primary_key.is_empty() {
                let pk_set: std::collections::BTreeSet<&str> =
                    t.primary_key.iter().map(String::as_str).collect();
                for col in &mut s.columns {
                    let now_pk = pk_set.contains(col.name.as_str());
                    col.is_primary_key = now_pk;
                    if now_pk {
                        col.nullable = false;
                    }
                }
            }
            s.partition_spec = pg2iceberg_core::parse_partition_spec(&t.iceberg.partition)
                .map_err(|e| anyhow::anyhow!("partition spec for {}: {e}", t.name))?;
            s
        };
        resolved_schemas.push(schema);
    }

    let mat_namer = Arc::new(CounterMaterializerNamer::new("materialized"));
    let mut materializer: Materializer<IcebergRustCatalog<iceberg_catalog_rest::RestCatalog>> =
        Materializer::new(coord, blob, catalog, mat_namer, &cfg.state.group, 64);
    for s in &resolved_schemas {
        materializer
            .register_table(s.clone())
            .await
            .with_context(|| format!("register {}", s.ident))?;
    }
    Ok(materializer)
}

/// One-shot: diff every configured table against PG ground truth.
/// Mirrors Go's `pg2iceberg verify` semantics: open a snapshot tx
/// against the source, read each table's rows at that view, read the
/// materialized Iceberg state, compare PK-by-PK. Returns non-zero
/// exit on any non-empty diff so CI / cron can detect drift.
pub async fn run_verify(cfg: Config, chunk_size: usize) -> Result<()> {
    let blob = build_blob(&cfg).context("build blob store")?;
    if cfg.sink.catalog_uri.is_empty() {
        anyhow::bail!("sink.catalog_uri is required");
    }
    let catalog: Arc<IcebergRustCatalog<iceberg_catalog_rest::RestCatalog>> = Arc::new(
        IcebergRustCatalog::new(Arc::new(build_rest_catalog(&cfg).await?)),
    );

    // Resolve schemas (mirrors `build_one_shot_materializer`). Verify
    // needs the same column-aware schema as the snapshot to know
    // which columns to SELECT and decode.
    let needs_pg = cfg.tables.iter().any(|t| !t.has_explicit_columns());
    let pg_for_discovery = if needs_pg {
        let pg_tls = match cfg.source.postgres.tls_label() {
            "webpki" => PgTls::Webpki,
            _ => PgTls::Disable,
        };
        Some(
            PgClientImpl::connect_with(&cfg.source.postgres.dsn(), pg_tls)
                .await
                .context("PG connect for schema discovery")?,
        )
    } else {
        None
    };
    let mut schemas: Vec<TableSchema> = Vec::with_capacity(cfg.tables.len());
    for t in &cfg.tables {
        let schema = if t.has_explicit_columns() {
            t.to_table_schema()?
        } else {
            let (ns, name) = t.qualified()?;
            let pg = pg_for_discovery
                .as_ref()
                .expect("needs_pg above guards this");
            let mut s = pg
                .discover_schema(&ns, &name)
                .await
                .with_context(|| format!("discover schema for {}", t.name))?;
            if !t.primary_key.is_empty() {
                let pk_set: std::collections::BTreeSet<&str> =
                    t.primary_key.iter().map(String::as_str).collect();
                for col in &mut s.columns {
                    let now_pk = pk_set.contains(col.name.as_str());
                    col.is_primary_key = now_pk;
                    if now_pk {
                        col.nullable = false;
                    }
                }
            }
            s.partition_spec = pg2iceberg_core::parse_partition_spec(&t.iceberg.partition)
                .map_err(|e| anyhow::anyhow!("partition spec for {}: {e}", t.name))?;
            s
        };
        if !schema.columns.iter().any(|c| c.is_primary_key) {
            anyhow::bail!(
                "table {} has no primary key; verify requires PKs to compare rows",
                t.name
            );
        }
        schemas.push(schema);
    }

    let source = PgSnapshotSource::open(&cfg.source.postgres, &schemas)
        .await
        .context("open verify source")?;

    let mut total_diffs = 0usize;
    for schema in &schemas {
        let diff = pg2iceberg_validate::verify::verify_table(
            &source,
            catalog.as_ref(),
            blob.as_ref(),
            schema,
            chunk_size,
        )
        .await
        .with_context(|| format!("verify {}", schema.ident))?;
        let n = diff.total_diffs();
        total_diffs += n;
        if diff.is_empty() {
            tracing::info!(table = %schema.ident, "verify: ok (no diffs)");
        } else {
            tracing::warn!(
                table = %schema.ident,
                pg_only = diff.pg_only.len(),
                iceberg_only = diff.iceberg_only.len(),
                mismatched = diff.mismatched.len(),
                "verify: diff detected",
            );
            for r in diff.pg_only.iter().take(5) {
                tracing::warn!(table = %schema.ident, ?r, "pg_only");
            }
            for r in diff.iceberg_only.iter().take(5) {
                tracing::warn!(table = %schema.ident, ?r, "iceberg_only");
            }
            for (pg_r, ice_r) in diff.mismatched.iter().take(5) {
                tracing::warn!(table = %schema.ident, ?pg_r, ?ice_r, "mismatched");
            }
        }
    }
    if total_diffs > 0 {
        anyhow::bail!("verify: {total_diffs} total row-level diffs across all tables");
    }
    println!("OK: {} table(s) match PG ground truth", schemas.len());
    Ok(())
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bucket_from_warehouse_strips_scheme() {
        assert_eq!(
            bucket_from_warehouse("s3://my-bucket").unwrap(),
            "my-bucket"
        );
        assert_eq!(
            bucket_from_warehouse("s3://my-bucket/sub/dir").unwrap(),
            "my-bucket"
        );
        assert_eq!(
            bucket_from_warehouse("s3a://my-bucket/").unwrap(),
            "my-bucket"
        );
    }

    #[test]
    fn bucket_from_warehouse_rejects_other_schemes() {
        assert!(bucket_from_warehouse("gs://x").is_err());
        assert!(bucket_from_warehouse("just/a/path").is_err());
    }

    #[test]
    fn prefix_from_warehouse_returns_path_or_none() {
        assert_eq!(prefix_from_warehouse("s3://b"), None);
        assert_eq!(prefix_from_warehouse("s3://b/"), None);
        assert_eq!(prefix_from_warehouse("s3://b/x"), Some("x".to_string()));
        assert_eq!(
            prefix_from_warehouse("s3://b/staged/data/"),
            Some("staged/data".to_string())
        );
    }
}
