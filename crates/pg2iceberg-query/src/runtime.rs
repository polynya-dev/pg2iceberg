//! Query-mode lifecycle. Mirrors
//! `pg2iceberg_validate::run_logical_lifecycle` for logical mode —
//! everything from coord migrate through main poll loop to drain
//! lives here, so the binary's `run_query` is just construction +
//! one library call. Fault-DST tests the lifecycle end-to-end.

use crate::{QueryPipeline, WatermarkSource};
use async_trait::async_trait;
use pg2iceberg_coord::Coordinator;
use pg2iceberg_core::TableSchema;
use pg2iceberg_iceberg::Catalog;
use pg2iceberg_logical::materializer::MaterializerNamer;
use pg2iceberg_stream::BlobStore;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum QueryLifecycleError {
    #[error("query pipeline: {0}")]
    Pipeline(#[from] crate::QueryPipelineError),
    #[error("config: {0}")]
    Config(String),
}

/// Inputs for [`run_query_lifecycle`]. Binary builds with prod types;
/// fault-DST builds with sim types — same lifecycle function runs.
#[allow(clippy::type_complexity)]
pub struct QueryLifecycle<Cat: Catalog + 'static> {
    pub coord: Arc<dyn Coordinator>,
    pub catalog: Arc<Cat>,
    pub blob: Arc<dyn BlobStore>,
    pub materializer_namer: Arc<dyn MaterializerNamer>,
    /// `(schema, watermark_column)` for each registered table.
    pub tables: Vec<(TableSchema, String)>,
    /// Built lazily — only invoked once the pipeline has its tables
    /// registered and is ready to poll. Binary supplies a closure
    /// that opens a fresh `tokio_postgres` conn; fault-DST supplies
    /// a sim-backed source.
    pub source_factory: Box<
        dyn FnOnce(
                &[(TableSchema, String)],
            ) -> std::pin::Pin<
                Box<
                    dyn Future<Output = Result<Box<dyn WatermarkSource>, QueryLifecycleError>>
                        + Send,
                >,
            > + Send,
    >,
    pub poll_interval: Duration,
}

/// Run the complete query-mode lifecycle:
///
/// 1. Build [`QueryPipeline`] from coord/catalog/blob.
/// 2. Register every table with its watermark column.
/// 3. Open the watermark source via the factory.
/// 4. Loop: poll → flush → sleep, until shutdown.
/// 5. Final flush on exit.
///
/// **Fault-DST coverage:** because the binary's `run_query` calls
/// this function, every step above is exercised by fault tests with
/// scripted faults at the coord/catalog/blob layer.
pub async fn run_query_lifecycle<Cat, F>(
    lc: QueryLifecycle<Cat>,
    shutdown: F,
) -> Result<(), QueryLifecycleError>
where
    Cat: Catalog + 'static,
    F: Future<Output = ()> + Unpin + Send,
{
    let mut pipeline: QueryPipeline<Cat> = QueryPipeline::new(
        Arc::clone(&lc.coord),
        Arc::clone(&lc.catalog),
        Arc::clone(&lc.blob),
        Arc::clone(&lc.materializer_namer),
    );
    for (schema, wm_col) in &lc.tables {
        pipeline
            .register_table(schema.clone(), wm_col.clone())
            .await?;
    }
    tracing::info!(count = lc.tables.len(), "query-mode tables registered");

    let source = (lc.source_factory)(&lc.tables).await?;
    tracing::info!(?lc.poll_interval, "query-mode poll interval");

    let mut shutdown = shutdown;
    let mut shutdown_pinned = std::pin::Pin::new(&mut shutdown);

    loop {
        // First-cycle no-wait so a fresh deploy doesn't sit idle.
        match pipeline.poll(source.as_ref()).await {
            Ok(n) if n > 0 => tracing::info!(rows = n, "query-mode poll"),
            Ok(_) => {}
            Err(e) => tracing::warn!(error = %e, "query-mode poll failed"),
        }
        match pipeline.flush().await {
            Ok(n) if n > 0 => tracing::info!(rows = n, "query-mode flush"),
            Ok(_) => {}
            Err(e) => tracing::error!(error = %e, "query-mode flush failed"),
        }

        tokio::select! {
            biased;
            _ = &mut shutdown_pinned => break,
            _ = tokio::time::sleep(lc.poll_interval) => {}
        }
    }

    if let Err(e) = pipeline.flush().await {
        tracing::warn!(error = %e, "final query-mode flush failed");
    }
    Ok(())
}

/// Wrapper to satisfy `Box<dyn WatermarkSource>` — gives the source a
/// concrete type when needed.
pub struct DynWatermarkSource(pub Box<dyn WatermarkSource>);

#[async_trait]
impl WatermarkSource for DynWatermarkSource {
    async fn read_after(
        &self,
        ident: &pg2iceberg_core::TableIdent,
        watermark_col: &str,
        after: Option<&pg2iceberg_core::PgValue>,
        limit: Option<usize>,
    ) -> crate::Result<Vec<pg2iceberg_core::Row>> {
        self.0.read_after(ident, watermark_col, after, limit).await
    }
}
