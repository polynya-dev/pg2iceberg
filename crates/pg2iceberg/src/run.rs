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

use crate::config::{BlobConfig, Config, IcebergConfig};
use crate::realio::{RealClock, RealIdGen};
use anyhow::{Context, Result};
use async_trait::async_trait;
use pg2iceberg_coord::{
    prod::{connect as coord_connect, PostgresCoordinator},
    schema::CoordSchema,
    Coordinator,
};
use pg2iceberg_core::{Clock, IdGen, Lsn};
use pg2iceberg_iceberg::prod::IcebergRustCatalog;
use pg2iceberg_logical::{
    materializer::CounterMaterializerNamer, pipeline::BlobNamer, Handler, Materializer, Pipeline,
    Schedule, Ticker,
};
use pg2iceberg_pg::{prod::PgClientImpl, PgClient};
use pg2iceberg_stream::{prod::ObjectStoreBlobStore, BlobStore};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
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
    let clock = Arc::new(RealClock);
    let id_gen = match cfg.runtime.worker_id.as_deref() {
        Some(w) => Arc::new(RealIdGen::with_worker_id(w)),
        None => Arc::new(RealIdGen::new()),
    };
    let worker_id = id_gen.worker_id();
    tracing::info!(worker = %worker_id.0, "starting pg2iceberg");

    // ── coord ──────────────────────────────────────────────────────────
    let coord_conn = coord_connect(&cfg.coord.conn)
        .await
        .context("coord connect")?;
    let coord_schema = CoordSchema::sanitize(&cfg.coord.schema);
    let coord = Arc::new(PostgresCoordinator::new(coord_conn, coord_schema));
    coord.migrate().await.context("coord migrate")?;

    // ── catalog ────────────────────────────────────────────────────────
    let warehouse = match &cfg.iceberg {
        IcebergConfig::Memory { warehouse } => warehouse.clone(),
    };
    let inner_catalog: iceberg::memory::MemoryCatalog =
        <iceberg::memory::MemoryCatalogBuilder as iceberg::CatalogBuilder>::load(
            iceberg::memory::MemoryCatalogBuilder::default(),
            "pg2iceberg",
            HashMap::from([(
                iceberg::memory::MEMORY_CATALOG_WAREHOUSE.to_string(),
                warehouse.clone(),
            )]),
        )
        .await
        .context("MemoryCatalog load")?;
    let catalog = Arc::new(IcebergRustCatalog::new(Arc::new(inner_catalog)));

    // ── blob store ─────────────────────────────────────────────────────
    let blob: Arc<dyn BlobStore> = match &cfg.blob {
        BlobConfig::Memory => Arc::new(ObjectStoreBlobStore::new(Arc::new(
            object_store::memory::InMemory::new(),
        ))),
    };

    // ── pg source ──────────────────────────────────────────────────────
    let pg = PgClientImpl::connect(&cfg.pg.conn)
        .await
        .context("PG connect")?;
    let table_idents: Vec<_> = cfg
        .tables
        .iter()
        .map(|t| t.to_table_schema().map(|s| s.ident))
        .collect::<Result<Vec<_>, _>>()?;
    if !pg
        .slot_exists(&cfg.pg.slot)
        .await
        .context("slot exists check")?
    {
        // First run — create the publication and the slot. Both are
        // idempotent in the sense that we check for existence before
        // creation, so re-running this once shouldn't double-fail.
        if let Err(e) = pg
            .create_publication(&cfg.pg.publication, &table_idents)
            .await
        {
            tracing::warn!(error = %e, "create_publication failed; assuming it exists");
        }
        let cp = pg.create_slot(&cfg.pg.slot).await.context("create_slot")?;
        tracing::info!(slot = %cfg.pg.slot, ?cp, "replication slot created");
    } else {
        tracing::info!(slot = %cfg.pg.slot, "replication slot exists, resuming");
    }

    // PG resumes from the slot's `confirmed_flush_lsn` server-side when
    // we pass `0/0` here. No need to track our own start LSN.
    let mut stream = pg
        .start_replication(&cfg.pg.slot, Lsn(0), &cfg.pg.publication)
        .await
        .context("START_REPLICATION")?;

    // ── pipeline + materializer ────────────────────────────────────────
    let blob_namer = Arc::new(UuidBlobNamer::new(id_gen.clone(), "staged"));
    let mut pipeline = Pipeline::new(
        coord.clone(),
        blob.clone(),
        blob_namer,
        cfg.runtime.flush_threshold,
    );

    let mat_namer = Arc::new(CounterMaterializerNamer::new("materialized"));
    let mut materializer: Materializer<IcebergRustCatalog<iceberg::memory::MemoryCatalog>> =
        Materializer::new(
            coord.clone() as Arc<dyn Coordinator>,
            blob.clone(),
            catalog.clone(),
            mat_namer,
            &cfg.coord.group,
            cfg.runtime.cycle_limit,
        );

    for t in &cfg.tables {
        let schema = t.to_table_schema()?;
        materializer
            .register_table(schema)
            .await
            .context("register table")?;
    }
    tracing::info!(count = cfg.tables.len(), "tables registered");

    // Initial consumer heartbeat. The ticker handles renewal alongside
    // every standby tick.
    coord
        .register_consumer(&cfg.coord.group, &worker_id, Duration::from_secs(60))
        .await
        .context("register_consumer")?;

    // ── main loop ──────────────────────────────────────────────────────
    let mut ticker = Ticker::new(clock.now(), Schedule::default());
    let mut sigint = signal(SignalKind::interrupt()).context("install SIGINT handler")?;
    let mut sigterm = signal(SignalKind::terminate()).context("install SIGTERM handler")?;
    let last_flush = Arc::new(AtomicU64::new(clock.now().0 as u64));
    let _ = &last_flush;

    tracing::info!("entering replication loop; SIGINT or SIGTERM to stop");

    loop {
        // Compute the time until the next ticker handler fires. We use
        // it as a cap on the recv-or-tick select.
        let now = clock.now();
        let next = ticker.next_due();
        let until_next = (next.0.saturating_sub(now.0)).max(0) as u64;
        let tick_sleep = Duration::from_micros(until_next.min(1_000_000)); // cap at 1s

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
            _ = tokio::time::sleep(tick_sleep) => {
                // Fall through to fire any due handlers below.
            }
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
                Handler::Watcher => {
                    // The invariant watcher (`pg2iceberg-validate::watcher`)
                    // isn't wired into the binary yet; that's a follow-on.
                }
            }
        }

        // Periodically renew the consumer heartbeat. We piggy-back on
        // ticker firings rather than running a third task.
        let _heartbeat = coord
            .register_consumer(&cfg.coord.group, &worker_id, Duration::from_secs(60))
            .await;
        // Fence usage of `last_flush` so the AtomicU64 isn't dead code.
        last_flush.store(clock.now().0 as u64, Ordering::Relaxed);
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
        .unregister_consumer(&cfg.coord.group, &worker_id)
        .await;
    tracing::info!(?final_lsn, "exited cleanly");
    Ok(())
}
