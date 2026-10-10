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
use pg2iceberg_core::metrics::{labels, names};
use pg2iceberg_core::{Clock, IdGen, Metrics, NoopMetrics, Phase, TableIdent, TableSchema};
use pg2iceberg_iceberg::prod::{IcebergRustCatalog, VendedBlobStoreRouter};
use pg2iceberg_logical::{
    materializer::{MaterializerNamer, UuidMaterializerNamer},
    pipeline::BlobNamer,
    Materializer,
};
use pg2iceberg_pg::prod::{PgClientImpl, TlsMode as PgTls};
use pg2iceberg_stream::{prod::ObjectStoreBlobStore, BlobStore};
use pg2iceberg_validate::Instrumented;
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
    async fn next_blob_path(&self, table: &TableIdent) -> String {
        format!(
            "{}/{}/{}.parquet",
            self.base.trim_end_matches('/'),
            table.name,
            uuid_hex(self.id_gen.as_ref())
        )
    }
}

fn uuid_hex(id_gen: &dyn IdGen) -> String {
    id_gen
        .new_uuid()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

/// Where pg2iceberg's files go: the blob store, and how staged and
/// materialized files are named in it.
pub struct Storage {
    pub blob: Arc<dyn BlobStore>,
    pub blob_namer: Arc<dyn BlobNamer>,
    pub materializer_namer: Arc<dyn MaterializerNamer>,
}

/// The storage `cfg` describes. With static or AWS-chain credentials,
/// files go under `sink.warehouse`. With vended credentials they go
/// under each table's location, as its catalog has it — what the
/// catalog vends credentials for (see [`LocationNamer`]).
pub async fn build_storage<C>(cfg: &Config, catalog: &IcebergRustCatalog<C>) -> Result<Storage>
where
    C: iceberg::Catalog + Send + Sync + 'static,
{
    let id_gen = Arc::new(crate::realio::RealIdGen::new());
    if cfg.sink.resolved_credential_mode() != "vended" {
        let warehouse = cfg.sink.warehouse.trim_end_matches('/');
        return Ok(Storage {
            blob: build_blob(cfg)?,
            blob_namer: Arc::new(UuidBlobNamer::new(
                id_gen.clone(),
                format!("{warehouse}/staged"),
            )),
            materializer_namer: Arc::new(UuidMaterializerNamer::new(
                id_gen,
                format!("{warehouse}/materialized"),
            )),
        });
    }
    let router = Arc::new(build_vended_router(cfg, catalog).await?);
    let namer = Arc::new(LocationNamer {
        router: Arc::clone(&router),
        id_gen,
    });
    Ok(Storage {
        blob: router,
        blob_namer: namer.clone(),
        materializer_namer: namer,
    })
}

/// Names files under each table's location (for vended credentials,
/// which the catalog scopes to it): staged chunks in `staged/`, data,
/// delete and compacted files in `data/` — orphan cleanup's scope, which
/// leaves the catalog's `metadata/` alone.
struct LocationNamer {
    router: Arc<VendedBlobStoreRouter>,
    id_gen: Arc<crate::realio::RealIdGen>,
}

impl LocationNamer {
    /// `table`'s location. It's registered with the router before
    /// anything writes to it; failing that, a path no store serves, so
    /// the write fails naming it.
    async fn location(&self, table: &TableIdent) -> String {
        self.router.table_location(table).await.unwrap_or_else(|e| {
            tracing::error!(%table, error = %e, "no catalog location for the table");
            format!("s3://no-location-for/{table}")
        })
    }
}

#[async_trait]
impl BlobNamer for LocationNamer {
    async fn next_blob_path(&self, table: &TableIdent) -> String {
        staged_path(&self.location(table).await, &uuid_hex(self.id_gen.as_ref()))
    }
}

#[async_trait]
impl MaterializerNamer for LocationNamer {
    async fn next_path(&self, table: &TableIdent, kind: &str, partition_segment: &str) -> String {
        let id = uuid_hex(self.id_gen.as_ref());
        data_path(&self.location(table).await, kind, partition_segment, &id)
    }

    async fn table_dir(&self, table: &TableIdent) -> String {
        data_dir(&self.location(table).await)
    }
}

/// A staged chunk's path under a table's `location`.
fn staged_path(location: &str, id: &str) -> String {
    format!("{location}/staged/{id}.parquet")
}

/// Where everything materialized for a table at `location` goes.
fn data_dir(location: &str) -> String {
    format!("{location}/data")
}

/// A materialized file's path under a table's `location`: data and
/// delete files by partition, other kinds (compaction output, meta and
/// marker rows) in a directory of their own.
fn data_path(location: &str, kind: &str, partition_segment: &str, id: &str) -> String {
    let dir = data_dir(location);
    match (kind, partition_segment) {
        ("data" | "eq-delete", "") => format!("{dir}/{kind}-{id}.parquet"),
        ("data" | "eq-delete", segment) => format!("{dir}/{segment}/{kind}-{id}.parquet"),
        (other, _) => format!("{dir}/{other}/{other}-{id}.parquet"),
    }
}

pub async fn run(cfg: Config, metrics: Arc<dyn Metrics>) -> Result<()> {
    cfg.require_catalog()?;
    let catalog = build_rest_catalog(&cfg).await?;
    let catalog = IcebergRustCatalog::new(Arc::new(catalog));
    let storage = build_storage(&cfg, &catalog)
        .await
        .context("build blob store")?;
    run_inner(cfg, catalog, storage, metrics).await
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
    // iceberg-rust's REST catalog requires a StorageFactory or
    // every file-IO operation panics with "StorageFactory must be
    // provided". OpenDAL's S3 backend is what the upstream
    // integration tests use; it picks up endpoint / credentials
    // from the catalog config response (REST `/v1/config`) plus
    // any overrides we pass through `props`. Surfaced by the
    // testcontainers integration test.
    RestCatalogBuilder::default()
        .with_storage_factory(Arc::new(OpenDalStorageFactory::S3 {
            customized_credential_load: None,
        }))
        .load("pg2iceberg", props)
        .await
        .context("RestCatalog load")
}

fn build_blob(cfg: &Config) -> Result<Arc<dyn BlobStore>> {
    match cfg.sink.resolved_credential_mode() {
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

/// The blob store [`build_storage`] builds.
///
/// `pub` so the integration tests can drive the exact same path the
/// binary takes for vended credentials, rather than reconstructing
/// the wiring inline.
pub async fn build_blob_for_run<C>(
    cfg: &Config,
    catalog: &IcebergRustCatalog<C>,
) -> Result<Arc<dyn BlobStore>>
where
    C: iceberg::Catalog + Send + Sync + 'static,
{
    Ok(build_storage(cfg, catalog).await?.blob)
}

/// The router over each table's vended credentials. It loads each
/// table from the catalog for them.
async fn build_vended_router<C>(
    cfg: &Config,
    catalog: &IcebergRustCatalog<C>,
) -> Result<VendedBlobStoreRouter>
where
    C: iceberg::Catalog + Send + Sync + 'static,
{
    use pg2iceberg_iceberg::prod::VendedRouterConfig;
    use pg2iceberg_iceberg::Catalog;

    // Each table's Iceberg identity, to `load_table` it and extract its
    // credentials. Discovery isn't required at this level.
    let mut idents: Vec<TableIdent> = Vec::with_capacity(cfg.tables.len());
    for t in &cfg.tables {
        idents.push(t.iceberg_ident(&cfg.sink.namespace)?);
    }
    if idents.is_empty() {
        anyhow::bail!("credential_mode=vended requires at least one table to replicate");
    }

    let router_cfg = VendedRouterConfig {
        default_region: cfg.sink.resolved_region(),
        // For a catalog whose credentials don't say where the storage is.
        default_endpoint: (!cfg.sink.s3_endpoint.is_empty()).then(|| cfg.sink.s3_endpoint.clone()),
        ..VendedRouterConfig::default()
    };
    let arc_catalog: Arc<dyn Catalog> = Arc::new(catalog.clone());
    let router = VendedBlobStoreRouter::build(arc_catalog, &idents, router_cfg)
        .await
        .context("build vended-credentials S3 router")?;
    tracing::info!(
        tables = idents.len(),
        "vended-credentials S3 router built (per-table object stores)"
    );
    Ok(router)
}

fn build_s3_static(cfg: &Config) -> Result<Arc<dyn BlobStore>> {
    if cfg.sink.warehouse.is_empty() {
        anyhow::bail!("sink.warehouse (ICEBERG_WAREHOUSE) is required for credential_mode=static");
    }
    let bucket = bucket_from_warehouse(&cfg.sink.warehouse)?;
    // object_store defaults `allow_http=false`. With the default,
    // reqwest is built with `https_only(true)` and refuses `http://`
    // requests at send time with "builder error for url (...)" — no
    // network call is made. We allow plain HTTP when the operator
    // configured an `http://` endpoint (MinIO, LocalStack, on-prem
    // S3-compatible setups). HTTPS endpoints retain the strict default.
    let mut builder = object_store::aws::AmazonS3Builder::new()
        .with_bucket_name(&bucket)
        .with_region(cfg.sink.resolved_region())
        .with_access_key_id(&cfg.sink.s3_access_key)
        .with_secret_access_key(&cfg.sink.s3_secret_key);
    if !cfg.sink.s3_endpoint.is_empty() {
        let allow_http = cfg.sink.s3_endpoint.starts_with("http://");
        builder = builder
            .with_endpoint(&cfg.sink.s3_endpoint)
            .with_allow_http(allow_http)
            // Path-style is the safe default for non-AWS S3 (MinIO,
            // LocalStack). Without an endpoint it's AWS, and its default.
            .with_virtual_hosted_style_request(false);
    }
    let inner = builder.build().context("AmazonS3Builder build")?;
    // No `PrefixStore`: namers and the Iceberg catalog emit full
    // `s3://<bucket>/<warehouse-subpath>/...` paths, and `parse_path`
    // strips only `s3://<bucket>/` — so the bucket-relative key already
    // carries the warehouse subpath. Wrapping in a PrefixStore keyed on
    // that same subpath would apply it twice (`warehouse/warehouse/...`),
    // writing data files where the manifest's absolute paths don't point,
    // so external readers (Athena/Spark/...) can't find them.
    Ok(Arc::new(ObjectStoreBlobStore::new(Arc::new(inner))))
}

fn build_s3_iam(cfg: &Config) -> Result<Arc<dyn BlobStore>> {
    if cfg.sink.warehouse.is_empty() {
        anyhow::bail!("sink.warehouse (ICEBERG_WAREHOUSE) is required for credential_mode=iam");
    }
    let bucket = bucket_from_warehouse(&cfg.sink.warehouse)?;
    let mut builder = object_store::aws::AmazonS3Builder::from_env()
        .with_bucket_name(&bucket)
        .with_region(cfg.sink.resolved_region());
    if !cfg.sink.s3_endpoint.is_empty() {
        // See the matching comment in `build_s3_static` for why
        // `allow_http` flips with the endpoint scheme.
        let allow_http = cfg.sink.s3_endpoint.starts_with("http://");
        builder = builder
            .with_endpoint(&cfg.sink.s3_endpoint)
            .with_allow_http(allow_http);
    }
    let inner = builder.build().context("AmazonS3Builder build")?;
    // See `build_s3_static` for why there is no `PrefixStore` here.
    Ok(Arc::new(ObjectStoreBlobStore::new(Arc::new(inner))))
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
pub async fn run_stream_only(cfg: Config, metrics: Arc<dyn Metrics>) -> Result<()> {
    cfg.require_catalog()?;
    let catalog = build_rest_catalog(&cfg).await?;
    let catalog = IcebergRustCatalog::new(Arc::new(catalog));
    let storage = build_storage(&cfg, &catalog)
        .await
        .context("build blob store")?;
    run_inner_with_schedule(
        cfg,
        catalog,
        storage,
        // 100 years — fire_due never matches, so the materializer
        // handler is effectively disabled. Picked over Duration::MAX
        // because some duration math saturates to MAX and would
        // never sleep.
        Some(std::time::Duration::from_secs(100 * 365 * 24 * 60 * 60)),
        metrics,
    )
    .await
}

/// Distributed mode: materializer worker only. Builds catalog, coord,
/// and materializer, registers tables, enables distributed mode with
/// the operator-supplied `worker_id`, and loops `cycle()` on the
/// configured interval. **No PG replication slot is opened** —
/// staged parquet comes from the coord log written by a paired
/// `stream-only` process.
///
/// Multiple workers under the same `state.group` heartbeat into
/// `_pg2iceberg.consumers` and round-robin tables across themselves
/// deterministically (sorted tables → sorted workers → `[i % N]`).
/// Joins and leaves rebalance on the next cycle automatically.
pub async fn run_materializer_only(
    cfg: Config,
    worker_id: String,
    metrics: Arc<dyn Metrics>,
) -> Result<()> {
    use pg2iceberg_core::WorkerId;

    if worker_id.is_empty() {
        anyhow::bail!("--worker-id is required for materializer-only mode");
    }
    let mut materializer = build_one_shot_materializer(cfg.clone(), metrics.clone()).await?;
    materializer.set_catalog_cache_ttl(
        Arc::new(crate::realio::RealClock),
        pg2iceberg_logical::CachingCatalog::<OneShotCatalog>::DEFAULT_TTL,
    );

    // Heartbeat TTL mirrors Go's `lockTTL = 30 * time.Second` in
    // `logical/materializer.go:222`. The lifecycle's main-loop
    // ticker fires `cycle()` (and thus the heartbeat refresh)
    // typically every 10s, so a 30s TTL gives ~3 missed cycles
    // before a worker drops out of the active list.
    let consumer_ttl = std::time::Duration::from_secs(30);
    materializer.enable_distributed_mode(WorkerId(worker_id.clone()), consumer_ttl);

    let cycle_interval = cfg.sink.schedule()?.materialize;

    let mut sigint = signal(SignalKind::interrupt()).context("install SIGINT handler")?;
    let mut sigterm = signal(SignalKind::terminate()).context("install SIGTERM handler")?;

    tracing::info!(
        worker = %worker_id,
        group = %cfg.state.group,
        interval = ?cycle_interval,
        "materializer-only worker starting"
    );

    // Sleep between cycles only once caught up: while a backlog remains,
    // each cycle (bounded by its row budget) is followed straight away by
    // the next.
    let mut pause = cycle_interval;
    Phase::Running.set(metrics.as_ref());
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
            _ = tokio::time::sleep(pause) => {
                let cycled = materializer.cycle().await;
                if cycled.is_ok() {
                    metrics.gauge(
                        names::LAST_SUCCESS,
                        &labels([("stage", "materialize")]),
                        crate::realio::RealClock.now().0 as f64 / 1e6,
                    );
                }
                pause = match cycled {
                    Ok(0) => cycle_interval,
                    Ok(_) => std::time::Duration::ZERO,
                    Err(e) => {
                        // Log but don't fail loudly: a transient blob /
                        // coord error shouldn't take the worker out of
                        // the rotation. Persistent errors will surface
                        // via metrics + the next operator-side check.
                        tracing::warn!(error = %e, "materializer cycle failed; will retry next interval");
                        cycle_interval
                    }
                };
            }
        }
    }

    Phase::Stopping.set(metrics.as_ref());
    materializer.shutdown_distributed().await;
    Ok(())
}

async fn run_inner<C>(
    cfg: Config,
    catalog: IcebergRustCatalog<C>,
    storage: Storage,
    metrics: Arc<dyn Metrics>,
) -> Result<()>
where
    C: iceberg::Catalog + Send + Sync + 'static,
{
    run_inner_with_schedule(cfg, catalog, storage, None, metrics).await
}

async fn run_inner_with_schedule<C>(
    cfg: Config,
    catalog: IcebergRustCatalog<C>,
    storage: Storage,
    materialize_override: Option<std::time::Duration>,
    metrics: Arc<dyn Metrics>,
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
    let mut lifecycle = crate::setup::build_logical_lifecycle(&cfg, catalog, storage, metrics)
        .await
        .context("build logical lifecycle")?;
    if let Some(d) = materialize_override {
        // Stream-only: bump the materialize handler interval so far
        // out it never fires. Keeps the shape of the lifecycle the
        // same — slot management, snapshot phase, flush, standby,
        // watcher all run normally.
        lifecycle.schedule.materialize = d;
        tracing::info!("stream-only mode: materializer cycle disabled (interval set to {d:?})");
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

/// One-shot: run a compaction pass over every configured table, log
/// outcomes, exit.
pub async fn run_compact(cfg: Config) -> Result<()> {
    if cfg.sink.target_file_size == 0 {
        anyhow::bail!(
            "sink.target_file_size is 0 — compaction is disabled. \
             Set a non-zero value (e.g. 134217728 for 128 MiB) to enable."
        );
    }
    let mut materializer = build_one_shot_materializer(cfg.clone(), Arc::new(NoopMetrics)).await?;
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
/// expiry first, then orphan-file cleanup — and exit.
///
/// Either retention or grace is required; the other is treated as a
/// no-op for that step if blank.
///
/// CLI `--retention` (e.g. `168h`) overrides
/// `sink.maintenance_retention`. Cleanup grace comes from
/// `sink.maintenance_grace`; each table's cleanup scope is the
/// directory the materializer writes its files to.
pub async fn run_maintain(cfg: Config, retention_override: Option<String>) -> Result<()> {
    if cfg.sink.resolved_maintenance()? == pg2iceberg_logical::Maintenance::Managed {
        // Its orphan cleanup would delete the catalog's own writes in
        // flight: only the catalog knows which unreferenced files those are.
        anyhow::bail!(
            "sink.maintenance is \"managed\": the catalog expires the tables' snapshots \
             and removes their orphan files, so `pg2iceberg maintain` leaves them alone"
        );
    }
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

    let mut materializer = build_one_shot_materializer(cfg.clone(), Arc::new(NoopMetrics)).await?;

    // Step 1: snapshot expiry.
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

    // Step 2: orphan-file cleanup.
    if !grace_str.is_empty() {
        let dur = humantime::parse_duration(&grace_str)
            .with_context(|| format!("parse maintenance_grace `{grace_str}`"))?;
        let grace_ms: i64 = dur.as_millis().try_into().unwrap_or(i64::MAX);
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis().try_into().unwrap_or(i64::MAX))
            .unwrap_or(i64::MAX);
        let outcomes = materializer
            .cleanup_orphans_cycle(now_ms, grace_ms)
            .await
            .context("cleanup_orphans_cycle")?;
        if outcomes.is_empty() {
            tracing::info!(
                grace = %grace_str,
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

/// The catalog of a materializer outside the lifecycle, its requests
/// recorded as the lifecycle records its own.
type OneShotCatalog = Instrumented<IcebergRustCatalog<iceberg_catalog_rest::RestCatalog>>;

/// `catalog`, `blob` and `coord`, recording their requests into `metrics`
/// (see [`Instrumented`]).
fn instrument(
    catalog: IcebergRustCatalog<iceberg_catalog_rest::RestCatalog>,
    blob: Arc<dyn BlobStore>,
    coord: Arc<dyn Coordinator>,
    metrics: &Arc<dyn Metrics>,
) -> (
    Arc<OneShotCatalog>,
    Arc<dyn BlobStore>,
    Arc<dyn Coordinator>,
) {
    let clock: Arc<dyn Clock> = Arc::new(crate::realio::RealClock);
    let catalog = Arc::new(Instrumented::new(
        Arc::new(catalog),
        metrics.clone(),
        clock.clone(),
    ));
    let blob: Arc<dyn BlobStore> =
        Arc::new(Instrumented::new(blob, metrics.clone(), clock.clone()));
    let coord: Arc<dyn Coordinator> = Arc::new(Instrumented::new(coord, metrics.clone(), clock));
    (catalog, blob, coord)
}

/// Set up the same Materializer the main run loop builds, but without
/// opening the replication slot or installing signal handlers — for
/// one-shot subcommands (`compact`, `maintain`) and `materializer-only`,
/// recording into `metrics`.
async fn build_one_shot_materializer(
    cfg: Config,
    metrics: Arc<dyn Metrics>,
) -> Result<Materializer<OneShotCatalog>> {
    cfg.require_catalog()?;
    let catalog = IcebergRustCatalog::new(Arc::new(build_rest_catalog(&cfg).await?));
    let storage = build_storage(&cfg, &catalog)
        .await
        .context("build blob store")?;

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
    let (catalog, blob, coord) = instrument(catalog, storage.blob, coord, &metrics);

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
            // The Iceberg namespace comes from `sink.namespace`, mirroring
            // `discover_schemas` in setup.rs (which the streaming
            // materializer uses to create the tables). Without this remap
            // these one-shot paths resolve the PG-schema-named table
            // (e.g. `public.riders`) instead of the materialized
            // `<sink.namespace>.riders` and fail with "table does not
            // exist" against every catalog.
            s.ident = t.iceberg_ident(&cfg.sink.namespace)?;
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

    let mut materializer: Materializer<OneShotCatalog> = Materializer::with_metrics(
        coord,
        blob,
        catalog,
        storage.materializer_namer,
        &cfg.state.group,
        cfg.sink.materializer_batch_rows,
        metrics,
    );
    materializer.set_maintenance(cfg.sink.resolved_maintenance()?);
    for s in &resolved_schemas {
        materializer
            .register_table(s.clone())
            .await
            .with_context(|| format!("register {}", s.ident))?;
    }
    Ok(materializer)
}

/// One-shot: diff every configured table against PG ground truth.
/// Opens a snapshot tx against the source, reads each table's rows
/// at that view, reads the materialized Iceberg state, compares
/// PK-by-PK. Returns non-zero exit on any non-empty diff so CI /
/// cron can detect drift.
pub async fn run_verify(cfg: Config, chunk_size: usize) -> Result<()> {
    cfg.require_catalog()?;
    let catalog: Arc<IcebergRustCatalog<iceberg_catalog_rest::RestCatalog>> = Arc::new(
        IcebergRustCatalog::new(Arc::new(build_rest_catalog(&cfg).await?)),
    );
    let blob = build_storage(&cfg, &catalog)
        .await
        .context("build blob store")?
        .blob;

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
            // The Iceberg namespace comes from `sink.namespace`, mirroring
            // `discover_schemas` in setup.rs (which the streaming
            // materializer uses to create the tables). Without this remap
            // these one-shot paths resolve the PG-schema-named table
            // (e.g. `public.riders`) instead of the materialized
            // `<sink.namespace>.riders` and fail with "table does not
            // exist" against every catalog.
            s.ident = t.iceberg_ident(&cfg.sink.namespace)?;
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
/// drop the coordinator schema (CASCADE). Idempotent per resource —
/// a missing slot / publication / schema is silently skipped — but
/// errors out if the slot is still active (i.e. some consumer is
/// connected). Stop the running process before invoking cleanup.
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
/// exit.
///
/// The replication slot is created here if missing — that pins the
/// WAL from `consistent_point` onward, so a subsequent `run` doesn't
/// lose any commits between snapshot completion and CDC start. The
/// alternative — operator creates the slot manually first — risks
/// the operator forgetting and losing data.
///
/// On a checkpoint that already says `snapshot_complete = true`,
/// returns `Ok(())` immediately without touching PG or the catalog.
pub async fn run_snapshot_only(cfg: Config, metrics: Arc<dyn Metrics>) -> Result<()> {
    use pg2iceberg_logical::pipeline::Pipeline;
    use pg2iceberg_logical::Materializer;
    use pg2iceberg_pg::PgClient;
    use pg2iceberg_snapshot::{run_snapshot_phase, SnapshotPhaseOutcome};
    use pg2iceberg_validate::LifecycleError;

    cfg.require_catalog()?;
    let catalog = IcebergRustCatalog::new(Arc::new(build_rest_catalog(&cfg).await?));
    let Storage {
        blob,
        blob_namer,
        materializer_namer,
    } = build_storage(&cfg, &catalog)
        .await
        .context("build blob store")?;

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
    let (catalog, blob, coord) = instrument(catalog, blob, coord_concrete, &metrics);

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
    let schemas = crate::setup::__discover_schemas_for_snapshot(
        &cfg.tables,
        pg.as_ref(),
        &cfg.sink.namespace,
    )
    .await
    .context("discover schemas")?;

    // ── early exit: every configured table already snapshotted ─────
    // Per-table state in `_pg2iceberg.tables` replaces the old
    // single-blob `snapshot_complete` flag; a missing row means
    // "fresh" (snapshot still needed).
    let mut all_done = !schemas.is_empty();
    let mut last_lsn = pg2iceberg_core::Lsn::ZERO;
    for s in &schemas {
        match coord
            .table_state(&s.ident)
            .await
            .context("load table_state")?
        {
            Some(state) if state.snapshot_complete => {
                if state.snapshot_lsn > last_lsn {
                    last_lsn = state.snapshot_lsn;
                }
            }
            _ => {
                all_done = false;
                break;
            }
        }
    }
    if all_done {
        println!(
            "OK: snapshot already complete for all {} configured table(s) \
             (highest snapshot LSN {:?}); nothing to do",
            schemas.len(),
            last_lsn
        );
        return Ok(());
    }

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
    let mut pipeline: Pipeline<dyn Coordinator> = Pipeline::with_metrics(
        Arc::clone(&coord),
        Arc::clone(&blob),
        blob_namer,
        cfg.sink.flush_rows,
        metrics.clone(),
    );
    for s in &schemas {
        let pk_cols: Vec<pg2iceberg_core::ColumnName> = s
            .primary_key_columns()
            .map(|c| pg2iceberg_core::ColumnName(c.name.clone()))
            .collect();
        if !pk_cols.is_empty() {
            pipeline.register_primary_keys(s.ident.clone(), pk_cols);
        }
        // PG → Iceberg ident translation. Mirrors the same call in
        // `run_logical_lifecycle`; needed because this path drives
        // its own pipeline + materializer outside the lifecycle.
        let pg_ident = s.pg_ident();
        if pg_ident != s.ident {
            pipeline.register_table_translation(pg_ident, s.ident.clone());
        }
    }

    let mut materializer: Materializer<OneShotCatalog> = Materializer::with_metrics(
        Arc::clone(&coord),
        Arc::clone(&blob),
        Arc::clone(&catalog),
        materializer_namer,
        &cfg.state.group,
        cfg.sink.materializer_batch_rows,
        metrics.clone(),
    );
    materializer.set_maintenance(cfg.sink.resolved_maintenance()?);
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
            .table_oid(s.pg_schema(), &s.ident.name)
            .await
            .with_context(|| format!("table_oid for {}", s.ident))?
        {
            table_oids.insert(s.ident.clone(), v);
        }
    }

    Phase::Snapshotting.set(metrics.as_ref());
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

    /// With vended credentials, a table's files go under its location:
    /// materialized files under `data/` — orphan cleanup's scope, which
    /// must never reach the catalog's `metadata/` — and staged chunks
    /// beside it, where cleanup doesn't look.
    #[test]
    fn location_layout_keeps_data_apart_from_staging_and_metadata() {
        let location = "s3://bucket/__r2_data_catalog/abc";
        let dir = data_dir(location);
        assert_eq!(dir, "s3://bucket/__r2_data_catalog/abc/data");
        let paths = [
            data_path(location, "data", "", "1"),
            data_path(location, "eq-delete", "status=active", "2"),
            data_path(location, "compact", "", "3"),
        ];
        assert_eq!(
            paths,
            [
                "s3://bucket/__r2_data_catalog/abc/data/data-1.parquet",
                "s3://bucket/__r2_data_catalog/abc/data/status=active/eq-delete-2.parquet",
                "s3://bucket/__r2_data_catalog/abc/data/compact/compact-3.parquet",
            ]
        );
        for path in &paths {
            assert!(path.starts_with(&format!("{dir}/")), "{path}");
        }
        let staged = staged_path(location, "4");
        assert_eq!(staged, "s3://bucket/__r2_data_catalog/abc/staged/4.parquet");
        assert!(!staged.starts_with(&format!("{dir}/")));
    }

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
}
