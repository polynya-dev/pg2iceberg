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
    if cfg.sink.catalog_uri.is_empty() {
        anyhow::bail!("sink.catalog_uri is required");
    }
    let catalog = build_rest_catalog(&cfg).await?;
    let catalog = IcebergRustCatalog::new(Arc::new(catalog));
    let blob = build_blob_for_run(&cfg, &catalog)
        .await
        .context("build blob store")?;
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
            "credential_mode=vended requires async catalog access — call \
             build_blob_for_run instead. (Static/iam paths are sync because \
             they don't need the catalog.)"
        )),
        other => Err(anyhow::anyhow!(
            "unknown credential_mode {other:?}; expected one of: static, vended, iam"
        )),
    }
}

/// Async blob-store builder used by [`run`] (logical + query modes).
/// Routes to [`build_blob`] for `static` / `iam` modes, and to the
/// vended-credentials path for `vended`. The latter requires the
/// catalog because it has to load each registered table to obtain
/// per-table S3 credentials.
async fn build_blob_for_run<C>(
    cfg: &Config,
    catalog: &IcebergRustCatalog<C>,
) -> Result<Arc<dyn BlobStore>>
where
    C: iceberg::Catalog + Send + Sync + 'static,
{
    if cfg.sink.credential_mode != "vended" {
        return build_blob(cfg);
    }
    use pg2iceberg_iceberg::prod::{VendedBlobStoreRouter, VendedRouterConfig};
    use pg2iceberg_iceberg::Catalog;

    // Resolve table idents from YAML — we need each table's
    // namespace/name to call `load_table` and extract per-table
    // creds. Discovery isn't required at this level; only the
    // operator-supplied identity matters.
    let mut idents: Vec<pg2iceberg_core::TableIdent> = Vec::with_capacity(cfg.tables.len());
    for t in &cfg.tables {
        let (ns, name) = t.qualified()?;
        idents.push(pg2iceberg_core::TableIdent {
            namespace: pg2iceberg_core::Namespace(vec![ns]),
            name,
        });
    }
    if idents.is_empty() {
        anyhow::bail!(
            "credential_mode=vended requires at least one configured table; \
             tables: [] in YAML"
        );
    }

    let mut router_cfg = VendedRouterConfig::default();
    if !cfg.sink.s3_region.is_empty() {
        router_cfg.default_region = cfg.sink.s3_region.clone();
    }
    let arc_catalog: Arc<dyn Catalog> = Arc::new(catalog.clone());
    let router = VendedBlobStoreRouter::build(arc_catalog, &idents, router_cfg)
        .await
        .context("build vended-credentials S3 router")?;
    tracing::info!(
        tables = idents.len(),
        "vended-credentials S3 router built (per-table object stores)"
    );
    Ok(Arc::new(router))
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

/// Distributed mode: WAL writer only. Same as [`run`] in logical mode,
/// but disables the materializer cycle handler so a paired
/// `materializer-only` process (or several, round-robined across
/// tables) does the catalog commits.
///
/// Implementation: run the regular logical lifecycle but stamp the
/// `Schedule::materialize` interval to a duration that effectively
/// never fires. Flush + Standby + Watcher still run on their normal
/// cadences so the slot stays advanced and invariants stay
/// monitored.
pub async fn run_stream_only(cfg: Config) -> Result<()> {
    if cfg.sink.catalog_uri.is_empty() {
        anyhow::bail!("sink.catalog_uri is required");
    }
    let catalog = build_rest_catalog(&cfg).await?;
    let catalog = IcebergRustCatalog::new(Arc::new(catalog));
    let blob = build_blob_for_run(&cfg, &catalog)
        .await
        .context("build blob store")?;
    if cfg.source.mode != "" && cfg.source.mode != "logical" {
        anyhow::bail!(
            "stream-only is logical-mode only; got source.mode={:?}",
            cfg.source.mode
        );
    }
    run_inner_with_schedule(
        cfg,
        catalog,
        blob,
        // 100 years — fire_due never matches, so the materializer
        // handler is effectively disabled. Picked over Duration::MAX
        // because some duration math saturates to MAX and would
        // never sleep.
        Some(std::time::Duration::from_secs(100 * 365 * 24 * 60 * 60)),
    )
    .await
}

/// Distributed mode: materializer worker only. Builds catalog + coord
/// + materializer, registers tables, enables distributed mode with
/// the operator-supplied `worker_id`, and loops `cycle()` on the
/// configured interval. **No PG replication slot is opened** —
/// staged parquet comes from the coord log written by a paired
/// `stream-only` process.
///
/// Multiple workers under the same `state.group` heartbeat into
/// `_pg2iceberg.consumers` and round-robin tables across themselves
/// deterministically (sorted tables → sorted workers → `[i % N]`).
/// Joins and leaves rebalance on the next cycle automatically.
pub async fn run_materializer_only(cfg: Config, worker_id: String) -> Result<()> {
    use pg2iceberg_core::WorkerId;

    if worker_id.is_empty() {
        anyhow::bail!("--worker-id is required for materializer-only mode");
    }
    let mut materializer = build_one_shot_materializer(cfg.clone()).await?;

    // Heartbeat TTL mirrors Go's `lockTTL = 30 * time.Second` in
    // `logical/materializer.go:222`. The lifecycle's main-loop
    // ticker fires `cycle()` (and thus the heartbeat refresh)
    // typically every 10s, so a 30s TTL gives ~3 missed cycles
    // before a worker drops out of the active list.
    let consumer_ttl = std::time::Duration::from_secs(30);
    materializer.enable_distributed_mode(WorkerId(worker_id.clone()), consumer_ttl);

    // Cycle interval. Mirrors Go's `MaterializerInterval` (default 10s).
    let cycle_interval = if cfg.sink.materializer_interval.is_empty() {
        std::time::Duration::from_secs(10)
    } else {
        humantime::parse_duration(&cfg.sink.materializer_interval).with_context(|| {
            format!(
                "parse sink.materializer_interval `{}`",
                cfg.sink.materializer_interval
            )
        })?
    };

    let mut sigint = signal(SignalKind::interrupt()).context("install SIGINT handler")?;
    let mut sigterm = signal(SignalKind::terminate()).context("install SIGTERM handler")?;

    tracing::info!(
        worker = %worker_id,
        group = %cfg.state.group,
        interval = ?cycle_interval,
        "materializer-only worker starting"
    );

    loop {
        tokio::select! {
            biased;
            _ = sigint.recv() => {
                tracing::info!("SIGINT received, materializer-only shutting down");
                break;
            }
            _ = sigterm.recv() => {
                tracing::info!("SIGTERM received, materializer-only shutting down");
                break;
            }
            _ = tokio::time::sleep(cycle_interval) => {
                if let Err(e) = materializer.cycle().await {
                    // Log but don't fail loudly: a transient blob /
                    // coord error shouldn't take the worker out of
                    // the rotation. Persistent errors will surface
                    // via metrics + the next operator-side check.
                    tracing::warn!(error = %e, "materializer cycle failed; will retry next interval");
                }
            }
        }
    }

    materializer.shutdown_distributed().await;
    Ok(())
}

async fn run_inner<C>(
    cfg: Config,
    catalog: IcebergRustCatalog<C>,
    blob: Arc<dyn BlobStore>,
) -> Result<()>
where
    C: iceberg::Catalog + Send + Sync + 'static,
{
    run_inner_with_schedule(cfg, catalog, blob, None).await
}

async fn run_inner_with_schedule<C>(
    cfg: Config,
    catalog: IcebergRustCatalog<C>,
    blob: Arc<dyn BlobStore>,
    materialize_override: Option<std::time::Duration>,
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
    let mut lifecycle =
        crate::setup::build_logical_lifecycle(&cfg, catalog, blob, blob_namer)
            .await
            .context("build logical lifecycle")?;
    if let Some(d) = materialize_override {
        // Stream-only: bump the materialize handler interval so far
        // out it never fires. Keeps the shape of the lifecycle the
        // same — slot management, snapshot phase, flush, standby,
        // watcher all run normally.
        lifecycle.schedule.materialize = d;
        tracing::info!(
            "stream-only mode: materializer cycle disabled (interval set to {d:?})"
        );
    }

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


/// One-shot: drop the replication slot, drop the publication, and
/// drop the coordinator schema (CASCADE). Mirrors Go's
/// `--cleanup`. Idempotent per resource — a missing slot /
/// publication / schema is silently skipped — but errors out if the
/// slot is still active (i.e. some consumer is connected). Stop the
/// running process before invoking cleanup.
///
/// Each step opens a *fresh* connection because the source PG and
/// the coord PG can live on different hosts (separate state DSN).
/// In single-DB setups they happen to be the same, and the two
/// connections just fan out to the same instance.
pub async fn run_cleanup(cfg: Config) -> Result<()> {
    use pg2iceberg_pg::PgClient;

    // ── 1. Drop slot + publication on the *source* PG. ─────────────
    let pg_tls = match cfg.source.postgres.tls_label() {
        "webpki" => PgTls::Webpki,
        _ => PgTls::Disable,
    };
    let pg = PgClientImpl::connect_with(&cfg.source.postgres.dsn(), pg_tls)
        .await
        .context("source PG connect")?;
    let slot = &cfg.source.logical.slot_name;
    let pub_name = &cfg.source.logical.publication_name;
    if !slot.is_empty() {
        pg.drop_slot(slot)
            .await
            .with_context(|| format!("drop replication slot {slot:?}"))?;
        tracing::info!(slot = %slot, "replication slot dropped (or absent)");
    }
    if !pub_name.is_empty() {
        pg.drop_publication(pub_name)
            .await
            .with_context(|| format!("drop publication {pub_name:?}"))?;
        tracing::info!(publication = %pub_name, "publication dropped (or absent)");
    }
    drop(pg);

    // ── 2. Drop coord schema CASCADE on the *coord* PG. ────────────
    // This wipes every coordinator table the migration created,
    // including log_index, log_seq, cursors, consumers, locks,
    // checkpoints, and pending_markers / marker_emissions.
    let coord_dsn = cfg.coord_dsn();
    let coord_tls = match cfg.source.postgres.tls_label() {
        "webpki" => CoordTls::Webpki,
        _ => CoordTls::Disable,
    };
    let coord_conn = coord_connect_with(&coord_dsn, coord_tls)
        .await
        .context("coord connect")?;
    let coord_schema = CoordSchema::sanitize(&cfg.state.coordinator_schema);
    let coord = PostgresCoordinator::new(coord_conn, coord_schema.clone());
    coord
        .teardown()
        .await
        .with_context(|| format!("drop coordinator schema {coord_schema:?}"))?;
    tracing::info!(schema = %coord_schema, "coordinator schema dropped (CASCADE)");

    println!(
        "OK: cleanup complete. \
         Materialized Iceberg tables remain — drop them via the catalog if you \
         intend a full re-bootstrap."
    );
    Ok(())
}

/// One-shot: run the initial snapshot phase for every configured
/// table, mark `snapshot_complete = true` in the checkpoint, and
/// exit. Mirrors Go's `--snapshot-only`.
///
/// The replication slot is created here if missing — that pins the
/// WAL from `consistent_point` onward, so a subsequent `run` doesn't
/// lose any commits between snapshot completion and CDC start. This
/// is **safer than Go's behavior**, which doesn't create the slot
/// in `--snapshot-only` mode and assumes the operator has already
/// done so out-of-band.
///
/// On a checkpoint that already says `snapshot_complete = true`,
/// returns `Ok(())` immediately without touching PG or the catalog.
pub async fn run_snapshot_only(cfg: Config) -> Result<()> {
    use pg2iceberg_logical::pipeline::Pipeline;
    use pg2iceberg_logical::Materializer;
    use pg2iceberg_pg::PgClient;
    use pg2iceberg_snapshot::{run_snapshot_phase, SnapshotPhaseOutcome};
    use pg2iceberg_validate::LifecycleError;

    let id_gen = Arc::new(crate::realio::RealIdGen::new());
    let blob = build_blob(&cfg).context("build blob store")?;
    if cfg.sink.catalog_uri.is_empty() {
        anyhow::bail!("sink.catalog_uri is required");
    }
    let catalog = Arc::new(IcebergRustCatalog::new(Arc::new(
        build_rest_catalog(&cfg).await?,
    )));

    // ── coord ──────────────────────────────────────────────────────
    let coord_dsn = cfg.coord_dsn();
    let coord_tls = match cfg.source.postgres.tls_label() {
        "webpki" => CoordTls::Webpki,
        _ => CoordTls::Disable,
    };
    let coord_conn = coord_connect_with(&coord_dsn, coord_tls)
        .await
        .context("coord connect")?;
    let coord_schema = CoordSchema::sanitize(&cfg.state.coordinator_schema);
    let coord_concrete = Arc::new(PostgresCoordinator::new(coord_conn, coord_schema));
    coord_concrete.migrate().await.context("coord migrate")?;
    let coord: Arc<dyn Coordinator> = coord_concrete;

    // ── early exit: snapshot already done ──────────────────────────
    // Use system_id=0 to skip the cluster fingerprint check — the
    // first run hasn't stamped one yet and we don't want
    // snapshot-only to fail loudly on a brand-new install. The full
    // `Run` lifecycle re-validates on every start.
    if let Some(cp) = coord
        .load_checkpoint(0)
        .await
        .context("load checkpoint")?
    {
        if cp.snapshot_complete {
            println!(
                "OK: snapshot already complete (per checkpoint at LSN {:?}); nothing to do",
                cp.flushed_lsn
            );
            return Ok(());
        }
    }

    // ── pg + slot ──────────────────────────────────────────────────
    let pg_tls = match cfg.source.postgres.tls_label() {
        "webpki" => PgTls::Webpki,
        _ => PgTls::Disable,
    };
    let pg = Arc::new(
        PgClientImpl::connect_with(&cfg.source.postgres.dsn(), pg_tls)
            .await
            .context("source PG connect")?,
    );
    let schemas = crate::setup::__discover_schemas_for_snapshot(&cfg.tables, pg.as_ref())
        .await
        .context("discover schemas")?;

    // Auto-create slot + publication if missing. This pins the WAL
    // from `consistent_point` onward so a later `Run` doesn't lose
    // any commits between snapshot and CDC start.
    let slot = &cfg.source.logical.slot_name;
    let pub_name = &cfg.source.logical.publication_name;
    if pg.slot_exists(slot).await.context("check slot")? {
        tracing::info!(slot = %slot, "replication slot exists, reusing");
    } else {
        let table_idents: Vec<pg2iceberg_core::TableIdent> =
            schemas.iter().map(|s| s.ident.clone()).collect();
        if let Err(e) = pg.create_publication(pub_name, &table_idents).await {
            tracing::warn!(
                error = %e,
                publication = %pub_name,
                "create_publication failed; assuming it exists"
            );
        }
        let cp = pg.create_slot(slot).await.context("create slot")?;
        tracing::info!(
            slot = %slot,
            consistent_point = ?cp,
            "replication slot created (WAL pinned for later Run)"
        );
    }

    // ── pipeline + materializer ────────────────────────────────────
    let blob_namer: Arc<dyn pg2iceberg_logical::pipeline::BlobNamer> =
        Arc::new(UuidBlobNamer::new(id_gen.clone(), "staged"));
    let mut pipeline: Pipeline<dyn Coordinator> = Pipeline::new(
        Arc::clone(&coord),
        Arc::clone(&blob),
        blob_namer,
        cfg.sink.flush_rows,
    );
    for s in &schemas {
        let pk_cols: Vec<pg2iceberg_core::ColumnName> = s
            .primary_key_columns()
            .map(|c| pg2iceberg_core::ColumnName(c.name.clone()))
            .collect();
        if !pk_cols.is_empty() {
            pipeline.register_primary_keys(s.ident.clone(), pk_cols);
        }
    }

    let mat_namer = Arc::new(
        pg2iceberg_logical::materializer::CounterMaterializerNamer::new("materialized"),
    );
    let mut materializer: Materializer<IcebergRustCatalog<iceberg_catalog_rest::RestCatalog>> =
        Materializer::new(
            Arc::clone(&coord),
            Arc::clone(&blob),
            Arc::clone(&catalog),
            mat_namer,
            &cfg.state.group,
            64,
        );
    for schema in &schemas {
        materializer
            .register_table(schema.clone())
            .await
            .with_context(|| format!("register {}", schema.ident))?;
    }

    // ── run snapshot phase ─────────────────────────────────────────
    let pg_cfg = cfg.source.postgres.clone();
    let to_snapshot = schemas.clone();
    let source = crate::snapshot_src::PgSnapshotSource::open(&pg_cfg, &to_snapshot)
        .await
        .map_err(|e| {
            anyhow::Error::from(LifecycleError::Snapshot(
                pg2iceberg_snapshot::SnapshotError::Source(format!("{e:#}")),
            ))
        })
        .context("open snapshot source")?;

    let mut table_oids: std::collections::BTreeMap<pg2iceberg_core::TableIdent, u32> =
        std::collections::BTreeMap::new();
    for s in &schemas {
        if let Some(v) = pg
            .table_oid(&s.ident.namespace.0.join("."), &s.ident.name)
            .await
            .with_context(|| format!("table_oid for {}", s.ident))?
        {
            table_oids.insert(s.ident.clone(), v);
        }
    }

    let outcome = run_snapshot_phase(
        &source,
        Arc::clone(&coord),
        &schemas,
        &std::collections::BTreeSet::new(),
        &table_oids,
        &mut pipeline,
        pg2iceberg_snapshot::DEFAULT_CHUNK_SIZE,
    )
    .await
    .context("run_snapshot_phase")?;

    match outcome {
        SnapshotPhaseOutcome::Skipped => {
            println!("OK: snapshot already complete; nothing to do");
        }
        SnapshotPhaseOutcome::Completed { snapshot_lsn } => {
            // Publish staged snapshot rows to Iceberg via one
            // materializer cycle. Without this, the staged Parquet
            // sits in coord but no Iceberg snapshot exists, and a
            // later `Run` would commit them — which works, but
            // surprises the operator who'd expect snapshot-only to
            // produce visible Iceberg state.
            materializer
                .cycle()
                .await
                .context("materialize snapshot rows")?;
            println!("OK: snapshot complete at LSN {snapshot_lsn:?}");
        }
    }

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
