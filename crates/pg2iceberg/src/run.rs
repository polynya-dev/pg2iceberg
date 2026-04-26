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
//!    Flush → Standby → Materialize → Watcher (Watcher is a no-op
//!    today; the watcher crate isn't wired in).
//! 4. SIGINT calls `Pipeline::shutdown` and exits cleanly.

use crate::config::Config;
use crate::realio::{RealClock, RealIdGen};
use anyhow::{Context, Result};
use async_trait::async_trait;
use pg2iceberg_coord::{
    prod::{connect_with as coord_connect_with, PostgresCoordinator, TlsMode as CoordTls},
    schema::CoordSchema,
    Coordinator,
};
use pg2iceberg_core::{Clock, IdGen, Lsn};
use pg2iceberg_iceberg::prod::IcebergRustCatalog;
use pg2iceberg_logical::{
    materializer::CounterMaterializerNamer, pipeline::BlobNamer, Handler, Materializer, Pipeline,
    Schedule, Ticker,
};
use pg2iceberg_pg::{
    prod::{PgClientImpl, TlsMode as PgTls},
    PgClient,
};
use pg2iceberg_stream::{prod::ObjectStoreBlobStore, BlobStore};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
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
    run_inner(cfg, IcebergRustCatalog::new(Arc::new(catalog)), blob).await
}

/// Build a REST `iceberg::Catalog` from sink config. We default to the
/// REST flavor because that's the only iceberg-rust catalog backend
/// that covers the common cloud / managed-Iceberg deployments today
/// (Polaris, Tabular, Snowflake, Iceberg-REST reference). Other
/// flavors are follow-ons.
pub(crate) async fn build_rest_catalog(cfg: &Config) -> Result<iceberg_catalog_rest::RestCatalog> {
    use iceberg::CatalogBuilder;
    use iceberg_catalog_rest::RestCatalogBuilder;
    let props: HashMap<String, String> = cfg.rest_catalog_props().into_iter().collect();
    RestCatalogBuilder::default()
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
    let clock = Arc::new(RealClock);
    let id_gen = Arc::new(RealIdGen::new());
    let worker_id = id_gen.worker_id();
    tracing::info!(worker = %worker_id.0, "starting pg2iceberg");

    // ── coord ──────────────────────────────────────────────────────────
    let coord_dsn = cfg.coord_dsn();
    let coord_tls = match cfg.source.postgres.tls_label() {
        // The coord shares the source PG's TLS choice unless a
        // separate `state.postgres_url` is set with its own
        // sslmode in the URL — for that, libpq parses the
        // sslmode from the URL itself and `tokio-postgres` honors
        // it via the conn-string parser.
        "webpki" => CoordTls::Webpki,
        _ => CoordTls::Disable,
    };
    let coord_conn = coord_connect_with(&coord_dsn, coord_tls)
        .await
        .context("coord connect")?;
    let coord_schema = CoordSchema::sanitize(&cfg.state.coordinator_schema);
    let coord = Arc::new(PostgresCoordinator::new(coord_conn, coord_schema));
    coord.migrate().await.context("coord migrate")?;

    let catalog = Arc::new(catalog);

    // ── pg source ──────────────────────────────────────────────────────
    let pg_tls = match cfg.source.postgres.tls_label() {
        "webpki" => PgTls::Webpki,
        _ => PgTls::Disable,
    };
    let pg = PgClientImpl::connect_with(&cfg.source.postgres.dsn(), pg_tls)
        .await
        .context("PG connect")?;

    // Resolve each table's schema: explicit `columns:` block in YAML
    // wins; otherwise we discover from `information_schema.columns` +
    // `pg_index`. Discovery requires the source PG to already have
    // the table — this fails clearly at startup if a typo or missing
    // table is in config.
    let mut resolved_schemas: Vec<pg2iceberg_core::TableSchema> =
        Vec::with_capacity(cfg.tables.len());
    for t in &cfg.tables {
        let schema = if t.has_explicit_columns() {
            t.to_table_schema()?
        } else {
            let (ns, name) = t.qualified()?;
            tracing::info!(table = %t.name, "discovering schema from PG");
            let mut s = pg
                .discover_schema(&ns, &name)
                .await
                .with_context(|| format!("discover schema for {}", t.name))?;
            // Operator-supplied PK overrides whatever discovery
            // found — useful when the source table has no PK index
            // but operators know the natural identity columns.
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
            // Partition spec is always YAML-driven — discovery
            // doesn't infer it from the source PG (which has no
            // notion of "Iceberg partitioning").
            s.partition_spec = pg2iceberg_core::parse_partition_spec(&t.iceberg.partition)
                .map_err(|e| anyhow::anyhow!("partition spec for {}: {e}", t.name))?;
            s
        };
        if !schema.columns.iter().any(|c| c.is_primary_key) {
            anyhow::bail!(
                "table {} has no primary key; pg2iceberg requires a PK on every \
                 replicated table — set one in PG or supply `primary_key:` in YAML",
                t.name
            );
        }
        resolved_schemas.push(schema);
    }
    let table_idents: Vec<_> = resolved_schemas.iter().map(|s| s.ident.clone()).collect();
    let slot_name = &cfg.source.logical.slot_name;
    let publication_name = &cfg.source.logical.publication_name;
    if !pg
        .slot_exists(slot_name)
        .await
        .context("slot exists check")?
    {
        if let Err(e) = pg.create_publication(publication_name, &table_idents).await {
            tracing::warn!(error = %e, "create_publication failed; assuming it exists");
        }
        let cp = pg.create_slot(slot_name).await.context("create_slot")?;
        tracing::info!(slot = %slot_name, ?cp, "replication slot created");
    } else {
        tracing::info!(slot = %slot_name, "replication slot exists, resuming");
    }

    let mut stream = pg
        .start_replication(slot_name, Lsn(0), publication_name)
        .await
        .context("START_REPLICATION")?;

    // ── pipeline + materializer ────────────────────────────────────────
    let blob_namer = Arc::new(UuidBlobNamer::new(id_gen.clone(), "staged"));
    let mut pipeline = Pipeline::new(coord.clone(), blob.clone(), blob_namer, cfg.sink.flush_rows);

    let mat_namer = Arc::new(CounterMaterializerNamer::new("materialized"));
    let mut materializer: Materializer<IcebergRustCatalog<C>> = Materializer::new(
        coord.clone() as Arc<dyn Coordinator>,
        blob.clone(),
        catalog.clone(),
        mat_namer,
        &cfg.state.group,
        // Materializer cycle limit isn't in the Go YAML; we use 64
        // for now and surface as a knob if profiling shows we need it.
        64,
    );

    for schema in &resolved_schemas {
        materializer
            .register_table(schema.clone())
            .await
            .context("register table")?;
    }
    tracing::info!(count = resolved_schemas.len(), "tables registered");

    coord
        .register_consumer(&cfg.state.group, &worker_id, Duration::from_secs(60))
        .await
        .context("register_consumer")?;

    // ── main loop ──────────────────────────────────────────────────────
    let mut ticker = Ticker::new(clock.now(), Schedule::default());
    let mut sigint = signal(SignalKind::interrupt()).context("install SIGINT handler")?;
    let mut sigterm = signal(SignalKind::terminate()).context("install SIGTERM handler")?;

    tracing::info!("entering replication loop; SIGINT or SIGTERM to stop");

    loop {
        let now = clock.now();
        let next = ticker.next_due();
        let until_next = (next.0.saturating_sub(now.0)).max(0) as u64;
        let tick_sleep = Duration::from_micros(until_next.min(1_000_000));

        tokio::select! {
            biased;
            _ = sigint.recv() => {
                tracing::info!("SIGINT received, shutting down");
                break;
            }
            _ = sigterm.recv() => {
                tracing::info!("SIGTERM received, shutting down");
                break;
            }
            res = stream.recv() => {
                let msg = res.context("replication recv")?;
                pipeline.process(msg).await.context("pipeline.process")?;
            }
            _ = tokio::time::sleep(tick_sleep) => {}
        }

        for h in ticker.fire_due(clock.now()) {
            match h {
                Handler::Flush => {
                    if let Err(e) = pipeline.flush().await {
                        tracing::error!(error = %e, "pipeline.flush failed");
                        return Err(anyhow::anyhow!(e));
                    }
                }
                Handler::Standby => {
                    let lsn = pipeline.flushed_lsn();
                    if let Err(e) = stream.send_standby(lsn, lsn).await {
                        tracing::warn!(error = %e, "send_standby failed");
                    }
                }
                Handler::Materialize => {
                    if let Err(e) = materializer.cycle().await {
                        tracing::error!(error = %e, "materializer.cycle failed");
                        return Err(anyhow::anyhow!(e));
                    }
                }
                Handler::Watcher => {}
            }
        }

        let _heartbeat = coord
            .register_consumer(&cfg.state.group, &worker_id, Duration::from_secs(60))
            .await;
    }

    // ── shutdown ───────────────────────────────────────────────────────
    tracing::info!("draining pipeline before exit");
    if let Err(e) = pipeline.flush().await {
        tracing::warn!(error = %e, "final flush failed");
    }
    let final_lsn = pipeline.flushed_lsn();
    if let Err(e) = stream.send_standby(final_lsn, final_lsn).await {
        tracing::warn!(error = %e, "final send_standby failed");
    }
    let _ = coord
        .unregister_consumer(&cfg.state.group, &worker_id)
        .await;
    tracing::info!(?final_lsn, "exited cleanly");
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
