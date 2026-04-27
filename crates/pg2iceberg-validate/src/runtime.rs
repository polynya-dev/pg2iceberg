//! Production handler-tick orchestration, lifted from the binary so
//! the fault-DST exercises the *same* code paths as `pg2iceberg run`.
//!
//! The binary's main `select!` loop owns SIGINT/SIGTERM/sleep
//! plumbing — that part stays in the binary. Everything inside the
//! `for h in ticker.fire_due(now)` block lives here, so changes to
//! handler semantics (compaction policy, watcher cadence, etc.) are
//! tested by the fault-DST automatically.
//!
//! ## Provided ticks
//!
//! - [`run_materialize_tick`] — `Materializer::cycle` plus inline
//!   compaction (when `CompactionConfig` is supplied), with the same
//!   "warn-and-continue on compact_cycle Err" behavior the binary
//!   uses. Returns counts so callers can log.
//! - [`run_watcher_tick`] — builds [`WatcherInputs`] and calls
//!   `InvariantWatcher::check`. Returns the violations vec.
//!
//! Both are async free functions, generic over the Pipeline's
//! Coordinator and Materializer's Catalog. Crash-and-restart, signal
//! handling, and the `select!` itself stay in the binary.

use crate::watcher::{InvariantViolation, InvariantWatcher, WatcherInputs};
use crate::{validate_startup, SlotState, StartupValidation, TableExistence};
use pg2iceberg_coord::Coordinator;
use pg2iceberg_core::{
    Clock, IdGen, Lsn, Mode, TableIdent, TableSchema, Timestamp, WorkerId,
};
use pg2iceberg_iceberg::{Catalog, CompactionConfig, CompactionOutcome};
use pg2iceberg_logical::{
    materializer::MaterializerNamer,
    pipeline::BlobNamer,
    runner::{Handler, Schedule, Ticker},
    Materializer, MaterializerError, Pipeline, PipelineError,
};
use pg2iceberg_pg::{PgClient, ReplicationStream, SlotMonitor};
use pg2iceberg_snapshot::{run_snapshot_phase, SnapshotPhaseOutcome, SnapshotSource};
use pg2iceberg_stream::BlobStore;
use std::collections::BTreeSet;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;

/// Outcome of one [`run_materialize_tick`] call. The binary logs
/// these counts; the fault-DST asserts on them under faults.
#[derive(Debug, Default)]
pub struct MaterializeTickOutcome {
    /// Rows materialized to Iceberg in this cycle.
    pub rows_committed: usize,
    /// Per-table compaction outcomes, in the order tables were walked.
    /// Empty when compaction was disabled or every table was below
    /// threshold.
    pub compaction: Vec<(TableIdent, CompactionOutcome)>,
    /// Set when compaction failed mid-cycle. The binary logs at warn
    /// (compaction is non-fatal — replication keeps progressing).
    pub compaction_error: Option<String>,
}

/// Run one materializer tick — `cycle()` plus optional inline
/// compaction. Mirrors the binary's `Handler::Materialize` arm.
///
/// **Failure semantics:**
/// - `cycle()` failure → propagates `MaterializerError`. The binary
///   exits on this; pipeline is unrecoverable in-process.
/// - `compact_cycle()` failure → captured in
///   `MaterializeTickOutcome::compaction_error` and returned `Ok`.
///   Mirrors the Go reference: compaction is best-effort.
pub async fn run_materialize_tick<Cat>(
    materializer: &mut Materializer<Cat>,
    compaction: Option<&CompactionConfig>,
) -> std::result::Result<MaterializeTickOutcome, MaterializerError>
where
    Cat: Catalog + 'static,
{
    let rows_committed = materializer.cycle().await?;

    let mut outcome = MaterializeTickOutcome {
        rows_committed,
        compaction: Vec::new(),
        compaction_error: None,
    };
    if let Some(cfg) = compaction {
        match materializer.compact_cycle(cfg).await {
            Ok(c) => outcome.compaction = c,
            Err(e) => outcome.compaction_error = Some(e.to_string()),
        }
    }
    Ok(outcome)
}

/// Run one watcher tick — builds [`WatcherInputs`] and calls
/// [`InvariantWatcher::check`]. Mirrors the binary's
/// `Handler::Watcher` arm.
#[allow(clippy::too_many_arguments)]
pub async fn run_watcher_tick(
    watcher: &InvariantWatcher,
    pipeline_flushed_lsn: Lsn,
    slot_confirmed_flush_lsn: Lsn,
    slot_wal_status: Option<pg2iceberg_pg::WalStatus>,
    slot_safe_wal_size: Option<i64>,
    slot_restart_lsn: Lsn,
    slot_conflicting: bool,
    slot_name: &str,
    group: &str,
    watched_tables: &[TableIdent],
) -> Vec<InvariantViolation> {
    let inputs = WatcherInputs {
        pipeline_flushed_lsn,
        slot_confirmed_flush_lsn,
        group: group.to_string(),
        watched_tables: watched_tables.to_vec(),
        slot_wal_status,
        slot_safe_wal_size,
        slot_name: slot_name.to_string(),
        slot_restart_lsn,
        slot_conflicting,
    };
    watcher.check(&inputs).await
}

// ── full lifecycle ────────────────────────────────────────────────

#[derive(Debug, Error)]
pub enum LifecycleError {
    #[error("startup validation: {0}")]
    Validation(String),
    #[error("pipeline.flush: {0}")]
    Flush(#[from] PipelineError),
    #[error("materializer.cycle: {0}")]
    Materialize(#[from] MaterializerError),
    #[error("snapshot phase: {0}")]
    Snapshot(#[from] pg2iceberg_snapshot::SnapshotError),
    #[error("replication recv: {0}")]
    Recv(String),
    #[error("pipeline.process: {0}")]
    Process(String),
    #[error("pg client: {0}")]
    Pg(#[from] pg2iceberg_pg::PgError),
    #[error("coord: {0}")]
    Coord(String),
    #[error("catalog: {0}")]
    Catalog(String),
    #[error("config: {0}")]
    Config(String),
    /// Mid-run slot health regression that's unrecoverable from the
    /// pipeline's perspective. Distinct from `Validation` (startup)
    /// so operators can tell "I started cleanly but the slot got
    /// killed under me" from "I refused to start." The watcher
    /// surfaces this when `wal_status` transitions to `Lost` or
    /// `conflicting` flips to true; the main loop propagates it.
    #[error("slot health regressed: {0}")]
    SlotHealth(String),
}

/// Type alias for backwards-compat — the previous loop-only error
/// variant is now subsumed by [`LifecycleError`].
pub type MainLoopError = LifecycleError;

/// Inputs for [`run_logical_lifecycle`]. The binary builds this with
/// prod components; the fault-DST builds it with sim components — both
/// then call the same lifecycle function. Slot setup, table
/// registration, consumer heartbeat, snapshot phase, main loop, and
/// shutdown drain are *all* in the helper.
pub struct LogicalLifecycle<Cat: Catalog + 'static> {
    /// Source PG. Used for slot existence check, publication creation,
    /// slot creation, and `start_replication`.
    pub pg: Arc<dyn PgClient>,
    /// Slot inspection for the watcher. Same impl as `pg` for prod /
    /// sim, but threaded separately so callers don't pay for trait
    /// upcasting (still flaky in stable Rust through 1.86 for
    /// non-supertrait-cast scenarios).
    pub slot_monitor: Arc<dyn SlotMonitor>,
    /// Coordinator. Already migrated by the caller (since `migrate()`
    /// is on the concrete type, not the trait).
    pub coord: Arc<dyn Coordinator>,
    pub catalog: Arc<Cat>,
    pub blob: Arc<dyn BlobStore>,
    pub clock: Arc<dyn Clock>,
    pub id_gen: Arc<dyn IdGen>,
    /// Pre-resolved table schemas. The binary resolves via
    /// `PgClient::discover_schema` (PG-specific); the DST passes
    /// schemas directly. Either way, the lifecycle takes them
    /// pre-resolved.
    pub schemas: Vec<TableSchema>,
    /// Subset of `schemas` whose tables should NOT be snapshotted
    /// (per-table `skip_snapshot: true` in YAML).
    pub skip_snapshot_idents: BTreeSet<TableIdent>,
    /// Slot name + publication name. The lifecycle creates the
    /// publication + slot if absent, then opens replication.
    pub slot_name: String,
    pub publication_name: String,
    pub group: String,
    pub schedule: Schedule,
    pub compaction: Option<CompactionConfig>,
    pub flush_rows: usize,
    /// Materializer cycle limit (entries-per-cycle cap).
    pub mat_cycle_limit: usize,
    pub consumer_ttl: Duration,
    /// Built lazily — only invoked if the snapshot phase actually
    /// needs to run (fresh slot, snapshot not already complete). The
    /// binary's factory builds a `PgSnapshotSource`; the DST's factory
    /// returns the harness's `SimPostgres` clone wrapped as a trait
    /// object.
    pub snapshot_source_factory:
        Box<dyn FnOnce(&[TableSchema]) -> SnapshotSourceFactoryFut + Send>,
    /// Materializer namer. Prod uses `CounterMaterializerNamer`
    /// (counter-suffixed); tests can pass a deterministic namer.
    pub materializer_namer: Arc<dyn MaterializerNamer>,
    /// Pipeline blob namer.
    pub blob_namer: Arc<dyn BlobNamer>,
    /// Metrics surface. `InMemoryMetrics` for tests + the binary;
    /// follow-on Prometheus exporter wraps the same trait.
    pub metrics: Arc<dyn pg2iceberg_core::Metrics>,
    pub mode: Mode,
    /// Blue-green marker mode. When `Some(meta_namespace)`,
    /// pg2iceberg watches `_pg2iceberg.markers` in the source PG
    /// (auto-included in the publication) and emits
    /// `(uuid, table_name, snapshot_id)` rows to a meta-marker
    /// Iceberg table at `<meta_namespace>.markers`. Each marker
    /// observation also triggers an immediate flush+materialize so
    /// the meta-marker rows on blue and green converge to the same
    /// WAL point regardless of timer skew.
    ///
    /// `None` disables marker mode (default).
    pub meta_namespace: Option<String>,
}

/// Boxed future returning the snapshot source. Awkward but unavoidable
/// — we want the factory to be async (PG snapshot tx setup is async)
/// AND to be `dyn`-callable.
pub type SnapshotSourceFactoryFut = std::pin::Pin<
    Box<
        dyn Future<Output = Result<Box<dyn SnapshotSource>, LifecycleError>>
            + Send,
    >,
>;

/// Run the *complete* logical-replication lifecycle from already-built
/// IO components to clean shutdown. Steps:
///
/// 1. Validate startup (Go's 8 invariants from `pipeline/validate.go`).
/// 2. Check slot existence; if absent, create publication + slot.
/// 3. `start_replication`.
/// 4. Build pipeline + materializer + watcher.
/// 5. Register every schema on the materializer.
/// 6. Register the consumer (heartbeat for distributed-worker quorum).
/// 7. If slot was fresh, run the snapshot phase via
///    [`pg2iceberg_snapshot::run_snapshot_phase`] using the supplied
///    factory; ack the slot at the snapshot LSN; run a materialize
///    cycle to publish snapshot rows to Iceberg.
/// 8. Run the main loop until `shutdown` resolves (handler dispatch +
///    heartbeat).
/// 9. Drain: final flush, send_standby, unregister consumer.
///
/// **Fault-DST coverage:** every step above is exercised by sim
/// plumbing. Bugs in any step (e.g. forgetting to create the
/// publication, advancing the slot before the snapshot is durable)
/// surface in fault tests automatically.
pub async fn run_logical_lifecycle<Cat, F>(
    mut lc: LogicalLifecycle<Cat>,
    shutdown: F,
) -> Result<(), LifecycleError>
where
    Cat: Catalog + 'static,
    F: Future<Output = ()> + Unpin + Send,
{
    // 1. Fingerprint the source cluster. `IDENTIFY_SYSTEM`'s systemid
    //    is unique per `initdb` and survives clone/replication, so a
    //    stale checkpoint pointed at the wrong cluster (accidental
    //    DSN swap, or blue/green mid-cutover) gets caught by
    //    `Checkpoint::verify` instead of silently re-using a
    //    cluster's LSN against a different cluster's WAL. Sim's
    //    `identify_system_id` returns 0, which makes
    //    `verify`/load_checkpoint skip the cluster check.
    let connected_system_id = lc.pg.identify_system_id().await?;
    if connected_system_id != 0 {
        tracing::info!(system_id = connected_system_id, "source cluster fingerprinted");
        // Wrap the coord so every subsequent `save_checkpoint` stamps
        // the connected cluster's fingerprint into the payload.
        // Mirrors Go's `pipeline.WithSystemIdentifier`.
        lc.coord = Arc::new(pg2iceberg_coord::StampingCoordinator::new(
            Arc::clone(&lc.coord),
            connected_system_id,
        ));
    }

    // 2. Startup validation. Refuses to start on any of the 8
    //    invariants Go's `pipeline/validate.go` enforces. Loads the
    //    checkpoint with the connected systemid so a cluster
    //    fingerprint mismatch fails fast here.
    run_startup_validation(
        lc.pg.as_ref(),
        lc.coord.as_ref(),
        lc.catalog.as_ref(),
        &lc.schemas,
        &lc.slot_name,
        &lc.publication_name,
        lc.mode,
        connected_system_id,
    )
    .await?;

    // 2. Slot setup. Don't `start_replication` yet — we want the
    //    snapshot phase to run first so we can pass `snap_lsn` as
    //    the stream's start LSN. That way the server-side fence
    //    skips events in `[consistent_point, snap_lsn]` which are
    //    already covered by the snapshot view. Mirrors the
    //    snapshot↔CDC marker-row fence pattern from Go's
    //    `pg2iceberg`, but uses the snapshot tx's
    //    `pg_current_wal_lsn()` directly instead of a real WAL
    //    marker row — same correctness guarantee, simpler wire
    //    format.
    let slot_was_fresh = !lc.pg.slot_exists(&lc.slot_name).await?;
    let table_idents: Vec<TableIdent> = lc.schemas.iter().map(|s| s.ident.clone()).collect();
    // Auto-include `_pg2iceberg.markers` in the publication when
    // marker mode is enabled. The bluegreen replication
    // publication (between blue and green PGs) must ALSO include
    // it — that's the operator's responsibility, documented in
    // the blue-green example README.
    let markers_table_ident = TableIdent {
        namespace: pg2iceberg_core::Namespace(vec!["_pg2iceberg".into()]),
        name: "markers".into(),
    };
    let mut pub_table_idents = table_idents.clone();
    if lc.meta_namespace.is_some() {
        pub_table_idents.push(markers_table_ident.clone());
    }
    if slot_was_fresh {
        if let Err(e) = lc
            .pg
            .create_publication(&lc.publication_name, &pub_table_idents)
            .await
        {
            tracing::warn!(error = %e, "create_publication failed; assuming it exists");
        }
        let cp = lc.pg.create_slot(&lc.slot_name).await?;
        tracing::info!(slot = %lc.slot_name, ?cp, "replication slot created");
    } else {
        tracing::info!(slot = %lc.slot_name, "replication slot exists, resuming");
    }

    // 3. Build pipeline + materializer + register tables.
    let mut pipeline = Pipeline::new(
        Arc::clone(&lc.coord),
        Arc::clone(&lc.blob),
        Arc::clone(&lc.blob_namer),
        lc.flush_rows,
    );
    if lc.meta_namespace.is_some() {
        pipeline.enable_markers(markers_table_ident.clone());
    }
    let mut materializer: Materializer<Cat> = Materializer::new(
        Arc::clone(&lc.coord),
        Arc::clone(&lc.blob),
        Arc::clone(&lc.catalog),
        Arc::clone(&lc.materializer_namer),
        &lc.group,
        lc.mat_cycle_limit,
    );
    for schema in &lc.schemas {
        materializer
            .register_table(schema.clone())
            .await
            .map_err(|e| LifecycleError::Catalog(e.to_string()))?;
    }
    if let Some(meta_ns) = &lc.meta_namespace {
        // Enable meta-marker emission. The materializer creates the
        // meta-marker Iceberg table in `<meta_namespace>.markers`
        // if missing.
        let meta_schema = pg2iceberg_logical::materializer::meta_marker_table_schema(
            meta_ns,
            "markers",
        );
        materializer
            .enable_meta_markers(meta_schema)
            .await
            .map_err(|e| LifecycleError::Catalog(e.to_string()))?;
    }
    tracing::info!(count = lc.schemas.len(), "tables registered");

    // 4. Consumer heartbeat.
    let worker_id = WorkerId(format!(
        "lifecycle-{}",
        lc.id_gen.new_uuid().iter().take(4).map(|b| format!("{b:02x}")).collect::<String>()
    ));
    lc.coord
        .register_consumer(&lc.group, &worker_id, lc.consumer_ttl)
        .await
        .map_err(|e| LifecycleError::Coord(e.to_string()))?;

    // 5. Snapshot phase (only on fresh slot). Returns the snapshot
    //    LSN we'll use as the replication start LSN — this is the
    //    snapshot↔CDC fence.
    let snap_lsn: Option<Lsn> = if slot_was_fresh {
        let to_snapshot: Vec<TableSchema> = lc
            .schemas
            .iter()
            .filter(|s| !lc.skip_snapshot_idents.contains(&s.ident))
            .cloned()
            .collect();
        if to_snapshot.is_empty() {
            tracing::info!(
                "snapshot phase: every configured table is skip_snapshot=true; nothing to do"
            );
            None
        } else {
            let source = (lc.snapshot_source_factory)(&to_snapshot).await?;
            tracing::info!(
                count = to_snapshot.len(),
                snap_lsn = ?source.snapshot_lsn().await?,
                "snapshot phase starting"
            );
            // Look up `pg_class.oid` for each schema so the snapshotter
            // can stamp `cp.snapshoted_table_oids` on completion. This
            // is what powers startup invariant 11 (TableIdentityChanged)
            // on subsequent runs.
            let mut table_oids: std::collections::BTreeMap<TableIdent, u32> =
                std::collections::BTreeMap::new();
            for s in &lc.schemas {
                let oid = lc
                    .pg
                    .table_oid(&s.ident.namespace.0.join("."), &s.ident.name)
                    .await?;
                if let Some(v) = oid {
                    table_oids.insert(s.ident.clone(), v);
                }
            }
            match run_snapshot_phase(
                source.as_ref(),
                Arc::clone(&lc.coord),
                &lc.schemas,
                &lc.skip_snapshot_idents,
                &table_oids,
                &mut pipeline,
                pg2iceberg_snapshot::DEFAULT_CHUNK_SIZE,
            )
            .await?
            {
                SnapshotPhaseOutcome::Skipped => {
                    tracing::info!("snapshot phase: skipped (checkpoint says complete)");
                    None
                }
                SnapshotPhaseOutcome::Completed { snapshot_lsn } => {
                    run_materialize_tick(&mut materializer, None).await?;
                    tracing::info!(?snapshot_lsn, "snapshot phase complete");
                    Some(snapshot_lsn)
                }
            }
        }
    } else {
        None
    };

    // 6. start_replication AT the right LSN. Snapshot↔CDC fence:
    //    - Fresh slot + snapshot ran: start at snap_lsn. Server
    //      skips events in [consistent_point, snap_lsn] which are
    //      already covered by the snapshot. No duplicates.
    //    - Fresh slot + snapshot skipped: start at consistent_point
    //      (Lsn 0 = "use confirmed_flush_lsn", which is the slot's
    //      initial position).
    //    - Resuming slot: start at confirmed_flush_lsn (Lsn 0
    //      semantics).
    let start_lsn = snap_lsn.unwrap_or(Lsn::ZERO);
    let mut stream = lc
        .pg
        .start_replication(&lc.slot_name, start_lsn, &lc.publication_name)
        .await?;
    // 7. Initial standby ack so the slot's confirmed_flush_lsn
    //    advances to start_lsn — server can recycle WAL behind it.
    if let Some(snapshot_lsn) = snap_lsn {
        stream.send_standby(snapshot_lsn, snapshot_lsn).await?;
    }

    // 8-9. Main loop + drain.
    let watched_tables = table_idents.clone();
    let watcher = InvariantWatcher::new(Arc::clone(&lc.coord), Arc::clone(&lc.metrics));
    let slot_monitor = lc.slot_monitor.clone();
    let loop_state = LogicalLoop {
        pipeline,
        materializer,
        stream,
        coord: Arc::clone(&lc.coord),
        slot_monitor,
        watcher,
        clock: Arc::clone(&lc.clock),
        watched_tables,
        group: lc.group,
        slot_name: lc.slot_name,
        schedule: lc.schedule,
        compaction: lc.compaction,
        worker_id,
        consumer_ttl: lc.consumer_ttl,
    };
    run_logical_main_loop(loop_state, shutdown).await
}

/// Bridge between [`crate::validate_startup`] (sync) and the lifecycle
/// helper. Loads coord checkpoint, queries each table's catalog state
/// + slot state, runs the 8 invariants. Errors with
/// [`LifecycleError::Validation`] on any violation.
#[allow(clippy::too_many_arguments)]
async fn run_startup_validation<Cat: Catalog + ?Sized>(
    pg: &dyn PgClient,
    coord: &dyn Coordinator,
    catalog: &Cat,
    schemas: &[TableSchema],
    slot_name: &str,
    publication_name: &str,
    mode: Mode,
    connected_system_id: u64,
) -> Result<(), LifecycleError> {
    let checkpoint = coord
        .load_checkpoint(connected_system_id)
        .await
        .map_err(|e| LifecycleError::Coord(e.to_string()))?;
    // One round-trip lookup of the publication's current tables;
    // we'll cross-reference each schema below. Empty list when the
    // publication doesn't exist (covered by the OrphanedSlot /
    // create_publication paths in step 2 of the lifecycle).
    let pub_members: std::collections::BTreeSet<TableIdent> = pg
        .publication_tables(publication_name)
        .await
        .map(|v| v.into_iter().collect())
        .unwrap_or_default();
    let mut tables: Vec<TableExistence> = Vec::with_capacity(schemas.len());
    for schema in schemas {
        let meta = catalog
            .load_table(&schema.ident)
            .await
            .map_err(|e| LifecycleError::Catalog(e.to_string()))?;
        let iceberg_name = format!("{}.{}", schema.ident.namespace, schema.ident.name);
        let pg_oid = pg
            .table_oid(
                &schema.ident.namespace.0.join("."),
                &schema.ident.name,
            )
            .await?;
        tables.push(TableExistence {
            pg_table: schema.ident.clone(),
            iceberg_name,
            existed: meta.is_some(),
            current_snapshot_id: meta.and_then(|m| m.current_snapshot_id),
            current_pg_oid: pg_oid,
            in_publication: pub_members.contains(&schema.ident),
        });
    }

    // (3) Hygiene warning — surface tables previously snapshotted
    // but no longer in YAML. log_index for these still accumulates;
    // operator should clean up. Doesn't block startup.
    if let Some(cp) = &checkpoint {
        let yaml_keys: std::collections::BTreeSet<String> =
            schemas.iter().map(|s| s.ident.to_string()).collect();
        for stale in cp
            .snapshoted_tables
            .keys()
            .filter(|k| !yaml_keys.contains(k.as_str()))
        {
            tracing::warn!(
                table = %stale,
                "table previously snapshotted but no longer in YAML config; \
                 its log_index entries will accumulate. drop the table from \
                 the publication and clear its `snapshoted_tables` entry to \
                 stop tracking it."
            );
        }
    }
    // One round-trip combined slot probe — covers exists,
    // restart_lsn, confirmed_flush_lsn, wal_status, conflicting,
    // safe_wal_size. The two new health fields drive invariants 9
    // and 10 in `validate_startup`.
    let slot = match pg.slot_health(slot_name).await? {
        Some(h) => Some(SlotState {
            exists: true,
            restart_lsn: h.restart_lsn,
            confirmed_flush_lsn: h.confirmed_flush_lsn,
            wal_status: h.wal_status,
            conflicting: h.conflicting,
        }),
        None => Some(SlotState {
            exists: false,
            restart_lsn: Lsn::ZERO,
            confirmed_flush_lsn: Lsn::ZERO,
            wal_status: None,
            conflicting: false,
        }),
    };
    let v = StartupValidation {
        checkpoint,
        tables,
        slot,
        config_mode: mode,
        slot_name: slot_name.to_string(),
        publication_name: publication_name.to_string(),
    };
    validate_startup(&v).map_err(|e| LifecycleError::Validation(e.to_string()))
}

/// Owned bundle of the post-construction state the main loop runs
/// against. The binary builds this with prod components; the
/// fault-DST builds it with sim components — both then call
/// [`run_logical_main_loop`] to execute the same handler-dispatch +
/// heartbeat + drain logic.
pub struct LogicalLoop<C: Coordinator + ?Sized + 'static, Cat: Catalog + 'static> {
    pub pipeline: Pipeline<C>,
    pub materializer: Materializer<Cat>,
    pub stream: Box<dyn ReplicationStream>,
    pub coord: Arc<dyn Coordinator>,
    pub slot_monitor: Arc<dyn SlotMonitor>,
    pub watcher: InvariantWatcher,
    pub clock: Arc<dyn Clock>,
    pub watched_tables: Vec<TableIdent>,
    pub group: String,
    pub slot_name: String,
    pub schedule: Schedule,
    pub compaction: Option<CompactionConfig>,
    pub worker_id: WorkerId,
    pub consumer_ttl: Duration,
}

/// Run the full replication main loop until `shutdown` resolves.
///
/// Drives the same select-loop pattern as the binary used to:
///
/// 1. `select!` on shutdown / replication recv / next-tick sleep.
/// 2. On replication event → [`Pipeline::process`].
/// 3. On tick → fire due handlers via [`run_materialize_tick`] /
///    [`run_watcher_tick`] (the helpers the binary used to inline).
/// 4. Heartbeat the coord.
/// 5. On shutdown → final flush + send_standby + unregister.
///
/// **Why this is a library function:** the production binary's
/// `run_inner` used to inline this whole loop (~80 lines), which
/// meant the fault-DST had to maintain a parallel hand-rolled copy.
/// Extracting it here means binary and DST exercise the *same code
/// path*. Bugs in the loop surface in fault-injection automatically.
pub async fn run_logical_main_loop<C, Cat, F>(
    mut loop_state: LogicalLoop<C, Cat>,
    shutdown: F,
) -> Result<(), MainLoopError>
where
    C: Coordinator + ?Sized + 'static,
    Cat: Catalog + 'static,
    F: Future<Output = ()> + Unpin + Send,
{
    let mut ticker = Ticker::new(loop_state.clock.now(), loop_state.schedule.clone());
    tokio::pin!(shutdown);

    loop {
        let now = loop_state.clock.now();
        let next = ticker.next_due();
        let tick_sleep = micros_to_duration(next, now);

        tokio::select! {
            biased;
            _ = &mut shutdown => break,
            res = loop_state.stream.recv() => {
                let msg = res.map_err(|e| MainLoopError::Recv(e.to_string()))?;
                loop_state
                    .pipeline
                    .process(msg)
                    .await
                    .map_err(|e| MainLoopError::Process(e.to_string()))?;
            }
            _ = tokio::time::sleep(tick_sleep) => {}
        }

        // Blue-green marker fast-path. When the pipeline observes a
        // marker INSERT in a committed tx, drive flush + materialize
        // immediately — don't wait for the next periodic Flush /
        // Materialize tick. This is what aligns blue and green
        // pg2iceberg instances even when their materializer
        // intervals differ wildly: each one acts on the marker as
        // soon as its WAL stream delivers it, and the meta-marker
        // emission ends up pointing at the same logical
        // (uuid, table, snapshot) tuple on both sides.
        if loop_state.pipeline.has_pending_marker() {
            dispatch_handler(&mut loop_state, Handler::Flush).await?;
            dispatch_handler(&mut loop_state, Handler::Materialize).await?;
        }

        let now = loop_state.clock.now();
        for h in ticker.fire_due(now) {
            dispatch_handler(&mut loop_state, h).await?;
        }

        // Heartbeat (best-effort — coord errors are logged at the
        // call site in the binary; here we swallow because the
        // watcher will surface any cursor-vs-log_index drift).
        let _ = loop_state
            .coord
            .register_consumer(
                &loop_state.group,
                &loop_state.worker_id,
                loop_state.consumer_ttl,
            )
            .await;
    }

    drain_and_shutdown(loop_state).await
}

/// Dispatch one Handler arm. Pure function — no select loop, no
/// shutdown plumbing — so the fault-DST can call it directly with
/// scripted faults.
async fn dispatch_handler<C, Cat>(
    loop_state: &mut LogicalLoop<C, Cat>,
    h: Handler,
) -> Result<(), MainLoopError>
where
    C: Coordinator + ?Sized + 'static,
    Cat: Catalog + 'static,
{
    match h {
        Handler::Flush => {
            loop_state.pipeline.flush().await?;
        }
        Handler::Standby => {
            let lsn = loop_state.pipeline.flushed_lsn();
            let _ = loop_state.stream.send_standby(lsn, lsn).await;
        }
        Handler::Materialize => {
            // `cycle` failure is fatal (propagates); compaction
            // failure is captured in the outcome and only logged.
            let outcome = run_materialize_tick(
                &mut loop_state.materializer,
                loop_state.compaction.as_ref(),
            )
            .await?;
            if let Some(err) = outcome.compaction_error {
                tracing::warn!(error = %err, "materializer.compact_cycle failed");
            }
        }
        Handler::Watcher => {
            // One combined slot probe per tick covers every health
            // field the watcher needs:
            //   - confirmed_flush_lsn → invariant 1 (PipelineAheadOfSlot)
            //   - wal_status=Unreserved → invariant 4 (warn-only)
            //   - wal_status=Lost → invariant 5 (fatal)
            //   - conflicting=true → invariant 6 (fatal)
            // Fatal violations break the loop with
            // `LifecycleError::SlotHealth` so the operator gets the
            // actionable message instead of a confusing
            // `recv()` error a few ticks later.
            let health = loop_state
                .slot_monitor
                .slot_health_for_watcher(&loop_state.slot_name)
                .await
                .ok()
                .flatten();
            let confirmed = health
                .as_ref()
                .map(|h| h.confirmed_flush_lsn)
                .unwrap_or(Lsn::ZERO);
            let wal_status = health.as_ref().and_then(|h| h.wal_status);
            let safe_wal_size = health.as_ref().and_then(|h| h.safe_wal_size);
            let restart_lsn = health
                .as_ref()
                .map(|h| h.restart_lsn)
                .unwrap_or(Lsn::ZERO);
            let conflicting = health.as_ref().map(|h| h.conflicting).unwrap_or(false);
            let violations = run_watcher_tick(
                &loop_state.watcher,
                loop_state.pipeline.flushed_lsn(),
                confirmed,
                wal_status,
                safe_wal_size,
                restart_lsn,
                conflicting,
                &loop_state.slot_name,
                &loop_state.group,
                &loop_state.watched_tables,
            )
            .await;
            // Log all, then fail fast on the first fatal one.
            for v in &violations {
                tracing::warn!(violation = %v, "invariant violation");
            }
            if let Some(fatal) = violations.iter().find(|v| v.is_fatal()) {
                return Err(MainLoopError::SlotHealth(fatal.to_string()));
            }
        }
    }
    Ok(())
}

/// Final flush + send_standby + unregister. Used by
/// [`run_logical_main_loop`] on shutdown; also exposed so callers can
/// drive shutdown directly.
pub async fn drain_and_shutdown<C, Cat>(
    mut loop_state: LogicalLoop<C, Cat>,
) -> Result<(), MainLoopError>
where
    C: Coordinator + ?Sized + 'static,
    Cat: Catalog + 'static,
{
    if let Err(e) = loop_state.pipeline.flush().await {
        tracing::warn!(error = %e, "final flush failed");
    }
    let final_lsn = loop_state.pipeline.flushed_lsn();
    if let Err(e) = loop_state.stream.send_standby(final_lsn, final_lsn).await {
        tracing::warn!(error = %e, "final send_standby failed");
    }
    let _ = loop_state
        .coord
        .unregister_consumer(&loop_state.group, &loop_state.worker_id)
        .await;
    tracing::info!(?final_lsn, "main loop exited cleanly");
    Ok(())
}

fn micros_to_duration(next: Timestamp, now: Timestamp) -> Duration {
    let until_next = (next.0.saturating_sub(now.0)).max(0) as u64;
    Duration::from_micros(until_next.min(1_000_000))
}
