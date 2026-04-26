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
use crate::realio::{RealClock, RealIdGen};
use anyhow::{Context, Result};
use async_trait::async_trait;
use pg2iceberg_coord::{
    prod::{connect_with as coord_connect_with, PostgresCoordinator, TlsMode as CoordTls},
    schema::CoordSchema,
    Coordinator,
};
use crate::snapshot_src::PgSnapshotSource;
use pg2iceberg_core::{Clock, IdGen, InMemoryMetrics, Lsn, Metrics, Mode, TableSchema};
use pg2iceberg_snapshot::{run_snapshot_phase, SnapshotPhaseOutcome};
use pg2iceberg_iceberg::{prod::IcebergRustCatalog, Catalog as _};
use pg2iceberg_logical::{
    materializer::CounterMaterializerNamer, pipeline::BlobNamer, Handler, Materializer, Pipeline,
    Schedule, Ticker,
};
use pg2iceberg_pg::{
    prod::{PgClientImpl, TlsMode as PgTls},
    PgClient,
};
use pg2iceberg_stream::{prod::ObjectStoreBlobStore, BlobStore};
use pg2iceberg_validate::{
    validate_startup, InvariantWatcher, SlotState, StartupValidation, TableExistence,
    WatcherInputs,
};
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

    // ── startup validation (Go's `pipeline/validate.go`) ───────────────
    // Inspect coord checkpoint, source PG slot, and Iceberg table state
    // for the 8 invariants from the Go reference. Refuse to start (with
    // an actionable error) when something is off — e.g. a fresh
    // checkpoint paired with an existing slot, or a slot that has
    // recycled past the last-flushed LSN. Runs before we'd auto-create
    // any state, so an operator who forgot to drop a slot doesn't end
    // up silently re-snapshotting on top of one.
    run_startup_validation(&pg, catalog.as_ref(), &coord, &resolved_schemas, slot_name)
        .await
        .context("startup validation")?;

    let slot_was_fresh = !pg
        .slot_exists(slot_name)
        .await
        .context("slot exists check")?;
    if slot_was_fresh {
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

    // ── snapshot phase (fresh-slot bootstrap) ──────────────────────────
    // pg2iceberg is a *mirror*, not a CDC tool — Iceberg must reflect
    // 100% of PG state. Replication only emits events for changes
    // *after* slot creation, so without a snapshot we'd lose every row
    // that existed beforehand. The orchestration logic lives in
    // `pg2iceberg_snapshot::run_snapshot_phase` so the fault-DST
    // exercises the *exact* code path the binary runs.
    if slot_was_fresh {
        // Per-table `skip_snapshot: true` opt-out, mapped to TableIdents.
        let skip_idents: std::collections::BTreeSet<pg2iceberg_core::TableIdent> = cfg
            .tables
            .iter()
            .filter(|t| t.skip_snapshot)
            .filter_map(|t| {
                let (ns, name) = t.qualified().ok()?;
                Some(pg2iceberg_core::TableIdent {
                    namespace: pg2iceberg_core::Namespace(vec![ns]),
                    name,
                })
            })
            .collect();
        let to_snapshot: Vec<TableSchema> = resolved_schemas
            .iter()
            .filter(|s| !skip_idents.contains(&s.ident))
            .cloned()
            .collect();
        if to_snapshot.is_empty() {
            tracing::info!("snapshot phase: every configured table is skip_snapshot=true; nothing to do");
        } else {
            tracing::info!(
                count = to_snapshot.len(),
                "snapshot phase starting (fresh slot bootstrap)"
            );
            let source = PgSnapshotSource::open(&cfg.source.postgres, &to_snapshot)
                .await
                .context("open snapshot source")?;
            tracing::info!(snap_lsn = ?source.snapshot_lsn(), "snapshot tx LSN captured");
            // Resumable orchestration. Returns Skipped if checkpoint
            // already says Complete; otherwise drives the resumable
            // Snapshotter and persists progress per chunk so a
            // mid-snapshot crash + restart resumes at the next chunk.
            match run_snapshot_phase(
                &source,
                coord.clone() as Arc<dyn Coordinator>,
                &resolved_schemas,
                &skip_idents,
                &mut pipeline,
                pg2iceberg_snapshot::DEFAULT_CHUNK_SIZE,
            )
            .await
            .context("run_snapshot_phase")?
            {
                SnapshotPhaseOutcome::Skipped => {
                    tracing::info!("snapshot phase: skipped (checkpoint says complete)");
                }
                SnapshotPhaseOutcome::Completed { snapshot_lsn } => {
                    // Ack the slot — PG advances `confirmed_flush_lsn`;
                    // replication resumes from snap_lsn onward.
                    stream
                        .send_standby(snapshot_lsn, snapshot_lsn)
                        .await
                        .context("post-snapshot send_standby")?;
                    // Materialize once so snapshot rows land in Iceberg
                    // before the live-replication loop. Subsequent live
                    // events show up via the Materialize handler.
                    materializer
                        .cycle()
                        .await
                        .context("post-snapshot materializer cycle")?;
                    tracing::info!(?snapshot_lsn, "snapshot phase complete");
                }
            }
        }
    }

    // ── invariant watcher ──────────────────────────────────────────────
    // Plan §9 invariants checked at every Watcher tick:
    //   1. pipeline.flushed_lsn ≤ slot.confirmed_flush_lsn
    //   2. mat_cursor[t] ≤ max(log_index.end_offset[t])
    //   3. pipeline.flushed_lsn monotonic
    // Violations are logged + counted on the metrics surface; we don't
    // exit because they're observability signals, not operator-fatal
    // state. (A real prod metrics backend lands when we wire Prometheus
    // — for now `InMemoryMetrics` keeps the surface complete without an
    // exporter dep.)
    let metrics: Arc<dyn Metrics> = Arc::new(InMemoryMetrics::new());
    let watcher = InvariantWatcher::new(coord.clone() as Arc<dyn Coordinator>, metrics.clone());
    let watched_tables: Vec<pg2iceberg_core::TableIdent> = resolved_schemas
        .iter()
        .map(|s| s.ident.clone())
        .collect();

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
                    // Compaction runs at the end of every materializer
                    // cycle, gated by file-count thresholds. Failures
                    // are non-fatal — replication keeps progressing,
                    // the next cycle retries. Mirrors Go's behavior.
                    if cfg.sink.target_file_size > 0 {
                        let cfg_compact = cfg.sink.compaction_config();
                        match materializer.compact_cycle(&cfg_compact).await {
                            Ok(outcomes) if !outcomes.is_empty() => {
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
                                        "compaction"
                                    );
                                }
                            }
                            Ok(_) => {} // every table below threshold; quiet
                            Err(e) => {
                                tracing::warn!(error = %e, "materializer.compact_cycle failed");
                            }
                        }
                    }
                }
                Handler::Watcher => {
                    let confirmed = pg
                        .slot_confirmed_flush_lsn(slot_name)
                        .await
                        .ok()
                        .flatten()
                        .unwrap_or(Lsn::ZERO);
                    let inputs = WatcherInputs {
                        pipeline_flushed_lsn: pipeline.flushed_lsn(),
                        slot_confirmed_flush_lsn: confirmed,
                        group: cfg.state.group.clone(),
                        watched_tables: watched_tables.clone(),
                    };
                    let violations = watcher.check(&inputs).await;
                    for v in &violations {
                        tracing::warn!(violation = %v, "invariant violation");
                    }
                }
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
    use crate::snapshot_src::PgWatermarkSource;
    use pg2iceberg_logical::materializer::CounterMaterializerNamer;
    use pg2iceberg_query::QueryPipeline;

    let clock = Arc::new(RealClock);
    let id_gen = Arc::new(RealIdGen::new());
    tracing::info!(worker = %id_gen.worker_id().0, "starting pg2iceberg in query mode");

    // ── coord (still needed for checkpoint watermark persistence) ────
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
    let coord: Arc<dyn Coordinator> = coord_concrete.clone();

    let catalog = Arc::new(catalog);

    // ── pg connection for schema discovery (replication-mode is fine
    // ── here; we only run SELECTs against pg_catalog).
    let pg_tls = match cfg.source.postgres.tls_label() {
        "webpki" => PgTls::Webpki,
        _ => PgTls::Disable,
    };
    let pg = PgClientImpl::connect_with(&cfg.source.postgres.dsn(), pg_tls)
        .await
        .context("PG connect")?;

    // Resolve schemas + watermark columns.
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

    let source = PgWatermarkSource::open(&cfg.source.postgres, &tables)
        .await
        .context("open watermark source")?;

    let mat_namer = Arc::new(CounterMaterializerNamer::new("materialized"));
    let mut pipeline: QueryPipeline<IcebergRustCatalog<C>> =
        QueryPipeline::new(coord.clone(), catalog.clone(), blob.clone(), mat_namer);
    for (schema, wm_col) in &tables {
        pipeline
            .register_table(schema.clone(), wm_col.clone())
            .await
            .with_context(|| format!("register {}", schema.ident))?;
    }
    tracing::info!(count = tables.len(), "query-mode tables registered");

    // Poll interval — Go uses `query.poll_interval`; default to 30s.
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
    tracing::info!(?poll_interval, "query-mode poll interval");

    let _ = clock;

    let mut sigint = signal(SignalKind::interrupt()).context("install SIGINT handler")?;
    let mut sigterm = signal(SignalKind::terminate()).context("install SIGTERM handler")?;

    loop {
        // First-cycle no-wait so a fresh deploy doesn't sit idle.
        match pipeline.poll(&source).await {
            Ok(n) if n > 0 => tracing::info!(rows = n, "query-mode poll"),
            Ok(_) => {} // quiet on no-op cycles
            Err(e) => tracing::warn!(error = %e, "query-mode poll failed"),
        }
        match pipeline.flush().await {
            Ok(n) if n > 0 => tracing::info!(rows = n, "query-mode flush"),
            Ok(_) => {}
            Err(e) => tracing::error!(error = %e, "query-mode flush failed"),
        }

        tokio::select! {
            biased;
            _ = sigint.recv() => {
                tracing::info!("SIGINT received, query-mode shutting down");
                break;
            }
            _ = sigterm.recv() => {
                tracing::info!("SIGTERM received, query-mode shutting down");
                break;
            }
            _ = tokio::time::sleep(poll_interval) => {}
        }
    }

    // Final flush on exit.
    if let Err(e) = pipeline.flush().await {
        tracing::warn!(error = %e, "final query-mode flush failed");
    }
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

/// Run the 8 plan-§Phase-12 startup validations from
/// `pg2iceberg-validate`. Reads the coord checkpoint, queries each
/// configured table's existence + current snapshot id from the
/// catalog, and reads the slot's `restart_lsn` /
/// `confirmed_flush_lsn` from the source PG. Refuses to start on any
/// violation with an actionable error message.
async fn run_startup_validation<C>(
    pg: &PgClientImpl,
    catalog: &IcebergRustCatalog<C>,
    coord: &Arc<PostgresCoordinator>,
    schemas: &[TableSchema],
    slot_name: &str,
) -> Result<()>
where
    C: iceberg::Catalog + Send + Sync + 'static,
{
    let checkpoint = coord.load_checkpoint().await.context("load checkpoint")?;

    let mut tables: Vec<TableExistence> = Vec::with_capacity(schemas.len());
    for schema in schemas {
        let meta = catalog
            .load_table(&schema.ident)
            .await
            .with_context(|| format!("load_table({}) for validation", schema.ident))?;
        let iceberg_name = format!("{}.{}", schema.ident.namespace, schema.ident.name);
        tables.push(TableExistence {
            pg_table: schema.ident.clone(),
            iceberg_name,
            existed: meta.is_some(),
            current_snapshot_id: meta.and_then(|m| m.current_snapshot_id),
        });
    }

    let slot_exists = pg
        .slot_exists(slot_name)
        .await
        .context("slot_exists for validation")?;
    let slot = if slot_exists {
        let restart_lsn = pg
            .slot_restart_lsn(slot_name)
            .await
            .context("slot_restart_lsn for validation")?
            .unwrap_or(Lsn::ZERO);
        let confirmed_flush_lsn = pg
            .slot_confirmed_flush_lsn(slot_name)
            .await
            .context("slot_confirmed_flush_lsn for validation")?
            .unwrap_or(Lsn::ZERO);
        Some(SlotState {
            exists: true,
            restart_lsn,
            confirmed_flush_lsn,
        })
    } else {
        Some(SlotState {
            exists: false,
            restart_lsn: Lsn::ZERO,
            confirmed_flush_lsn: Lsn::ZERO,
        })
    };

    let v = StartupValidation {
        checkpoint,
        tables,
        slot,
        config_mode: Mode::Logical,
        slot_name: slot_name.to_string(),
    };
    validate_startup(&v).map_err(|e| anyhow::anyhow!(e))
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
