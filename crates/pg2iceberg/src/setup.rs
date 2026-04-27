//! Binary-only setup: construct the prod components the lifecycle
//! helper expects.
//!
//! Stays in the binary (not the library) because every step here
//! touches a *prod-specific* type — `PgClientImpl`,
//! `PostgresCoordinator`, `IcebergRustCatalog<RestCatalog>`,
//! `ObjectStoreBlobStore` — that the sim doesn't have. The orchestration
//! once components are built lives in `pg2iceberg_validate::run_logical_lifecycle`.

use crate::config::{Config, TableConfig};
use crate::realio::{RealClock, RealIdGen};
use crate::snapshot_src::PgSnapshotSource;
use anyhow::{Context, Result};
use pg2iceberg_coord::{
    prod::{connect_with as coord_connect_with, PostgresCoordinator, TlsMode as CoordTls},
    schema::CoordSchema,
    Coordinator,
};
use pg2iceberg_core::{InMemoryMetrics, Mode, TableIdent, TableSchema};
use pg2iceberg_iceberg::prod::IcebergRustCatalog;
use pg2iceberg_logical::{materializer::CounterMaterializerNamer, pipeline::BlobNamer};
use pg2iceberg_pg::{
    prod::{PgClientImpl, TlsMode as PgTls},
    PgClient, SlotMonitor,
};
use pg2iceberg_stream::BlobStore;
use pg2iceberg_validate::{LifecycleError, LogicalLifecycle, SnapshotSourceFactoryFut};
use std::sync::Arc;
use std::time::Duration;

/// Build the [`LogicalLifecycle`] inputs from a YAML config and
/// already-opened catalog/blob handles. After this returns, the
/// caller hands the struct to
/// `pg2iceberg_validate::run_logical_lifecycle` and the rest of the
/// loop runs in library code.
pub async fn build_logical_lifecycle<C>(
    cfg: &Config,
    catalog: IcebergRustCatalog<C>,
    blob: Arc<dyn BlobStore>,
    blob_namer: Arc<dyn BlobNamer>,
) -> Result<LogicalLifecycle<IcebergRustCatalog<C>>>
where
    C: iceberg::Catalog + Send + Sync + 'static,
{
    let clock = Arc::new(RealClock);
    let id_gen = Arc::new(RealIdGen::new());

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
    coord_concrete
        .migrate()
        .await
        .context("coord migrate")?;
    let coord: Arc<dyn Coordinator> = coord_concrete;

    // ── pg source ──────────────────────────────────────────────────
    let pg_tls = match cfg.source.postgres.tls_label() {
        "webpki" => PgTls::Webpki,
        _ => PgTls::Disable,
    };
    let pg_concrete = Arc::new(
        PgClientImpl::connect_with(&cfg.source.postgres.dsn(), pg_tls)
            .await
            .context("PG connect")?,
    );
    let pg: Arc<dyn PgClient> = pg_concrete.clone();
    let slot_monitor: Arc<dyn SlotMonitor> = pg_concrete.clone();

    // ── schema discovery (PG-specific) ─────────────────────────────
    let schemas = discover_schemas(&cfg.tables, pg_concrete.as_ref()).await?;

    // ── skip-snapshot opt-out ──────────────────────────────────────
    let skip_snapshot_idents: std::collections::BTreeSet<TableIdent> = cfg
        .tables
        .iter()
        .filter(|t| t.skip_snapshot)
        .filter_map(|t| {
            let (ns, name) = t.qualified().ok()?;
            Some(TableIdent {
                namespace: pg2iceberg_core::Namespace(vec![ns]),
                name,
            })
        })
        .collect();

    // ── snapshot source factory ────────────────────────────────────
    // Closure captures the YAML config so opening the SnapshotSource
    // (REPEATABLE READ tx + LSN capture) only happens if the snapshot
    // phase actually needs to run.
    let pg_cfg = cfg.source.postgres.clone();
    let snapshot_source_factory = Box::new(move |to_snapshot: &[TableSchema]| {
        let pg_cfg = pg_cfg.clone();
        let to_snapshot = to_snapshot.to_vec();
        Box::pin(async move {
            let source = PgSnapshotSource::open(&pg_cfg, &to_snapshot)
                .await
                .map_err(|e| LifecycleError::Snapshot(
                    pg2iceberg_snapshot::SnapshotError::Source(format!("{e:#}")),
                ))?;
            Ok::<Box<dyn pg2iceberg_snapshot::SnapshotSource>, LifecycleError>(Box::new(source))
        }) as SnapshotSourceFactoryFut
    });

    Ok(LogicalLifecycle {
        pg,
        slot_monitor,
        coord,
        catalog: Arc::new(catalog),
        blob,
        clock,
        id_gen,
        schemas,
        skip_snapshot_idents,
        slot_name: cfg.source.logical.slot_name.clone(),
        publication_name: cfg.source.logical.publication_name.clone(),
        group: cfg.state.group.clone(),
        schedule: pg2iceberg_logical::Schedule::default(),
        compaction: if cfg.sink.target_file_size > 0 {
            Some(cfg.sink.compaction_config())
        } else {
            None
        },
        flush_rows: cfg.sink.flush_rows,
        mat_cycle_limit: 64,
        consumer_ttl: Duration::from_secs(60),
        snapshot_source_factory,
        materializer_namer: Arc::new(CounterMaterializerNamer::new("materialized")),
        blob_namer,
        metrics: Arc::new(InMemoryMetrics::new()),
        mode: Mode::Logical,
        // Blue-green marker mode. Operators set
        // `sink.meta_namespace: "_pg2iceberg_<env>"` in YAML to
        // opt in.
        meta_namespace: if cfg.sink.meta_namespace.is_empty() {
            None
        } else {
            Some(cfg.sink.meta_namespace.clone())
        },
    })
}

/// Query-mode equivalent of [`build_logical_lifecycle`]. Builds the
/// prod components and packs them into a [`QueryLifecycle`] for the
/// library helper to drive.
pub async fn build_query_lifecycle<C>(
    cfg: &Config,
    catalog: IcebergRustCatalog<C>,
    blob: Arc<dyn BlobStore>,
) -> Result<pg2iceberg_query::QueryLifecycle<IcebergRustCatalog<C>>>
where
    C: iceberg::Catalog + Send + Sync + 'static,
{
    use pg2iceberg_query::{QueryLifecycle, QueryLifecycleError};

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
    coord_concrete
        .migrate()
        .await
        .context("coord migrate")?;
    let coord: Arc<dyn Coordinator> = coord_concrete;

    let pg_tls = match cfg.source.postgres.tls_label() {
        "webpki" => PgTls::Webpki,
        _ => PgTls::Disable,
    };
    let pg = PgClientImpl::connect_with(&cfg.source.postgres.dsn(), pg_tls)
        .await
        .context("PG connect")?;

    // Resolve schemas + watermark column per table.
    let mut tables: Vec<(TableSchema, String)> = Vec::with_capacity(cfg.tables.len());
    for t in &cfg.tables {
        if t.watermark_column.is_empty() {
            anyhow::bail!(
                "query mode requires `watermark_column` on every table; missing on {}",
                t.name
            );
        }
        let schema = if t.has_explicit_columns() {
            t.to_table_schema()?
        } else {
            let (ns, name) = t.qualified()?;
            tracing::info!(table = %t.name, "discovering schema from PG");
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
                "table {} has no primary key; query mode requires PKs for dedup",
                t.name
            );
        }
        tables.push((schema, t.watermark_column.clone()));
    }

    let pg_cfg = cfg.source.postgres.clone();
    let source_factory: Box<
        dyn FnOnce(
                &[(TableSchema, String)],
            ) -> std::pin::Pin<
                Box<
                    dyn std::future::Future<
                            Output = std::result::Result<
                                Box<dyn pg2iceberg_query::WatermarkSource>,
                                QueryLifecycleError,
                            >,
                        > + Send,
                >,
            > + Send,
    > = Box::new(move |tables| {
        let pg_cfg = pg_cfg.clone();
        let tables = tables.to_vec();
        Box::pin(async move {
            let source = crate::snapshot_src::PgWatermarkSource::open(&pg_cfg, &tables)
                .await
                .map_err(|e| QueryLifecycleError::Config(format!("{e:#}")))?;
            Ok::<Box<dyn pg2iceberg_query::WatermarkSource>, QueryLifecycleError>(Box::new(source))
        })
    });

    let poll_interval = if cfg.source.query.poll_interval.is_empty() {
        Duration::from_secs(30)
    } else {
        humantime::parse_duration(&cfg.source.query.poll_interval)
            .with_context(|| {
                format!(
                    "parse query.poll_interval `{}`",
                    cfg.source.query.poll_interval
                )
            })?
    };

    Ok(QueryLifecycle {
        coord,
        catalog: Arc::new(catalog),
        blob,
        materializer_namer: Arc::new(CounterMaterializerNamer::new("materialized")),
        tables,
        source_factory,
        poll_interval,
    })
}

async fn discover_schemas(
    tables: &[TableConfig],
    pg: &PgClientImpl,
) -> Result<Vec<TableSchema>> {
    let mut resolved: Vec<TableSchema> = Vec::with_capacity(tables.len());
    for t in tables {
        let schema = if t.has_explicit_columns() {
            t.to_table_schema()?
        } else {
            let (ns, name) = t.qualified()?;
            tracing::info!(table = %t.name, "discovering schema from PG");
            let mut s = pg
                .discover_schema(&ns, &name)
                .await
                .with_context(|| format!("discover schema for {}", t.name))?;
            // Operator-supplied PK overrides discovery.
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
            // Partition spec is YAML-driven.
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
        resolved.push(schema);
    }
    Ok(resolved)
}
