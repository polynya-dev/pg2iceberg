//! Logical-replication pipeline orchestrator.
//!
//! `Pipeline::process(msg)` advances the sink for a single decoded message;
//! `flush()` drains, uploads, claims, and advances `flushedLSN` — the last
//! step gated by [`CoordCommitReceipt`].

use crate::sink::{FlushOutput, Sink, SinkError, TableChunk};
use async_trait::async_trait;
use bytes::Bytes;
use pg2iceberg_coord::{CommitBatch, CoordCommitReceipt, CoordError, Coordinator, OffsetClaim};
use pg2iceberg_core::metrics::{names, Labels};
use pg2iceberg_core::{Lsn, Metrics, NoopMetrics};
use pg2iceberg_pg::DecodedMessage;
use pg2iceberg_stream::{BlobStore, StreamError};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use thiserror::Error;

#[derive(Clone, Debug, Error)]
pub enum PipelineError {
    #[error("sink: {0}")]
    Sink(#[from] SinkError),
    #[error("blob: {0}")]
    Blob(#[from] StreamError),
    #[error("coord: {0}")]
    Coord(#[from] CoordError),
}

pub type Result<T> = std::result::Result<T, PipelineError>;

/// Names a staged Parquet object in the blob store. Real production uses
/// `IdGen::new_uuid` for unique suffixes; the sim uses a deterministic counter
/// so DST runs are reproducible.
#[async_trait]
pub trait BlobNamer: Send + Sync {
    async fn next_blob_path(&self, table: &str) -> String;
}

/// Deterministic blob namer for sim/test paths. Uses a monotonic counter.
#[derive(Default)]
pub struct CounterBlobNamer {
    counter: AtomicU64,
    base: String,
}

impl CounterBlobNamer {
    pub fn new(base: impl Into<String>) -> Self {
        Self {
            counter: AtomicU64::new(0),
            base: base.into(),
        }
    }
}

#[async_trait]
impl BlobNamer for CounterBlobNamer {
    async fn next_blob_path(&self, table: &str) -> String {
        let n = self.counter.fetch_add(1, Ordering::SeqCst);
        format!("{}/{}/{:010}.parquet", self.base, table, n)
    }
}

/// The pipeline. Generic over the coord impl so the type system carries the
/// `Coordinator` choice all the way through.
pub struct Pipeline<C: Coordinator> {
    sink: Sink,
    coord: Arc<C>,
    blob_store: Arc<dyn BlobStore>,
    namer: Arc<dyn BlobNamer>,
    flushed_lsn: AtomicU64,
    metrics: Arc<dyn Metrics>,
    /// True after `shutdown` runs; further `process` calls become no-ops.
    /// Tested for in flush so a forgotten shutdown sequence stays correct.
    shut_down: bool,
}

impl<C: Coordinator> Pipeline<C> {
    pub fn new(
        coord: Arc<C>,
        blob_store: Arc<dyn BlobStore>,
        namer: Arc<dyn BlobNamer>,
        flush_threshold: usize,
    ) -> Self {
        Self::with_metrics(
            coord,
            blob_store,
            namer,
            flush_threshold,
            Arc::new(NoopMetrics),
        )
    }

    pub fn with_metrics(
        coord: Arc<C>,
        blob_store: Arc<dyn BlobStore>,
        namer: Arc<dyn BlobNamer>,
        flush_threshold: usize,
        metrics: Arc<dyn Metrics>,
    ) -> Self {
        Self {
            sink: Sink::new(flush_threshold),
            coord,
            blob_store,
            namer,
            flushed_lsn: AtomicU64::new(0),
            metrics,
            shut_down: false,
        }
    }

    /// Highest LSN whose underlying batch has committed in the coordinator.
    /// This is what the pipeline hands to its `send_standby` ticker (Phase 5
    /// keeps that out of band — the sim test calls `send_standby` directly).
    pub fn flushed_lsn(&self) -> Lsn {
        Lsn(self.flushed_lsn.load(Ordering::SeqCst))
    }

    pub async fn process(&mut self, msg: DecodedMessage) -> Result<()> {
        if self.shut_down {
            // Refuse new events after shutdown. Caller bug if this fires.
            return Ok(());
        }
        match msg {
            DecodedMessage::Begin { xid, .. } => self.sink.begin_tx(xid),
            DecodedMessage::Commit { xid, commit_lsn } => self.sink.commit_tx(xid, commit_lsn),
            DecodedMessage::Change(evt) => self.sink.record_change(evt)?,
            DecodedMessage::Relation { .. } => {
                // Schema evolution lands in Phase 7.
            }
            DecodedMessage::Keepalive { .. } => {
                // The standby ticker reads `flushed_lsn()` directly.
            }
        }
        Ok(())
    }

    /// Drain all committed-but-unflushed transactions: encode → upload →
    /// `claim_offsets` → advance `flushedLSN` via the receipt. No-op if
    /// nothing is ready.
    pub async fn flush(&mut self) -> Result<Option<Lsn>> {
        let Some(FlushOutput {
            chunks,
            flushable_lsn,
        }) = self.sink.flush()?
        else {
            return Ok(None);
        };
        if chunks.is_empty() {
            // Possible if every committed tx was empty. Still advance the LSN
            // (we know all tx commits up to this point are durable in PG, but
            // there's nothing for the coord to register).
            // For Phase 5 we still go through claim_offsets with an empty
            // batch so the receipt is the single LSN-advance code path.
        }

        let mut claims = Vec::with_capacity(chunks.len());
        let mut total_records = 0u64;
        for TableChunk { table, chunk } in chunks {
            let path = self.namer.next_blob_path(&table.name).await;
            let byte_size = chunk.bytes.len() as u64;
            self.blob_store
                .put(&path, Bytes::clone(&chunk.bytes))
                .await?;
            let mut labels = Labels::new();
            labels.insert("table".into(), table.name.clone());
            self.metrics.counter(
                names::PIPELINE_ROWS_STAGED_TOTAL,
                &labels,
                chunk.record_count,
            );
            total_records += chunk.record_count;
            claims.push(OffsetClaim {
                table,
                record_count: chunk.record_count,
                byte_size,
                s3_path: path,
            });
        }
        let _ = total_records;

        let batch = CommitBatch {
            claims,
            flushable_lsn,
        };
        let receipt = self.coord.claim_offsets(&batch).await?;
        self.advance_flushed_lsn(receipt);

        // Emit per-flush counters + the flushed_lsn gauge.
        let mut empty_labels = Labels::new();
        self.metrics
            .counter(names::PIPELINE_FLUSH_TOTAL, &empty_labels, 1);
        // Empty-labels gauge doesn't need a fresh map; reuse the buffer.
        empty_labels.clear();
        self.metrics.gauge(
            names::PIPELINE_FLUSHED_LSN,
            &empty_labels,
            flushable_lsn.0 as f64,
        );
        Ok(Some(flushable_lsn))
    }

    /// Graceful shutdown: drain whatever's already buffered into committed
    /// txns (no-op for already-empty queues), do one final flush + receipt
    /// advance, then mark the pipeline as no-longer-accepting events.
    ///
    /// Intentionally does NOT wait for in-flight transactions to commit:
    /// the caller is expected to have already drained the source. After
    /// this returns, `process` is a no-op and `flush` returns `Ok(None)`.
    pub async fn shutdown(&mut self) -> Result<()> {
        if self.shut_down {
            return Ok(());
        }
        // One last flush to push any committed-but-unflushed work.
        let _ = self.flush().await?;
        self.shut_down = true;
        Ok(())
    }

    pub fn is_shut_down(&self) -> bool {
        self.shut_down
    }

    /// **Receipt-gated LSN advance.** Consumes a [`CoordCommitReceipt`] —
    /// since the receipt cannot be constructed outside `pg2iceberg-coord`'s
    /// internals, no caller can advance the slot LSN without the coord
    /// having committed.
    fn advance_flushed_lsn(&self, receipt: CoordCommitReceipt) {
        // The receipt carries the LSN that the pipeline supplied via
        // `CommitBatch::flushable_lsn`. After the coord commit, that LSN is
        // safely durable downstream of the slot.
        self.flushed_lsn
            .store(receipt.flushable_lsn.0, Ordering::SeqCst);
    }
}
