//! pg2iceberg CLI binary.
//!
//! Subcommands:
//!
//! - `connect-pg` — open a replication-mode connection to the source PG
//!   and report slots / publications. Connectivity smoke test for the
//!   PG prod path.
//! - `connect-iceberg` — open the Iceberg catalog from config and list
//!   namespaces. Connectivity smoke test for the Iceberg prod path.
//! - `migrate-coord` — run the coordinator's idempotent schema
//!   migration. First step of any greenfield deployment.
//! - `run` — assemble the full pipeline (PG client + coord + catalog +
//!   blob store) and run it until SIGINT.
//! - `init` — inspect the source database and write a config for it.
//!
//! Every subcommand reads its config from environment variables and an
//! optional YAML file (see [`Config::load`]).

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use pg2iceberg::config::{self, Config, DEFAULT_CONFIG_PATH};
use pg2iceberg::telemetry::Telemetry;
use pg2iceberg::{init, run, tables};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Cli {
    /// Config file. Default: `PG2ICEBERG_CONFIG`, else `pg2iceberg.yaml` if
    /// present, else environment variables alone.
    #[arg(long, global = true)]
    config: Option<PathBuf>,
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Inspect the source database (`POSTGRES_URL`) and write a config for
    /// it: every table pg2iceberg can replicate, the settings the
    /// environment gives, secrets as `${VAR}` references. Checks what
    /// replication needs of the database too.
    Init {
        /// Where to write it; `-` prints it.
        #[arg(long, default_value = DEFAULT_CONFIG_PATH)]
        output: String,
        /// Overwrite an existing file.
        #[arg(long)]
        force: bool,
    },
    /// Smoke-test the PG replication-mode connection.
    ConnectPg,
    /// Smoke-test the Iceberg catalog connection.
    ConnectIceberg,
    /// Run the coordinator's idempotent schema migration. Safe to run
    /// repeatedly — every statement is `CREATE … IF NOT EXISTS`.
    MigrateCoord,
    /// Run the full pipeline.
    Run,
    /// One-shot: run a single compaction pass over every configured
    /// table and exit. Useful for cron / k8s CronJob deployments where
    /// compaction runs out-of-band from the replication loop.
    Compact,
    /// One-shot: run snapshot expiry over every configured table and
    /// exit. Reads `sink.maintenance_retention` from YAML; CLI
    /// override takes precedence when supplied.
    Maintain {
        /// Override `sink.maintenance_retention` (e.g. `168h`, `30m`).
        /// Parsed by `humantime`.
        #[arg(long)]
        retention: Option<String>,
    },
    /// Diff PG ground truth against Iceberg materialized state for
    /// every configured table. Prints per-table counts of `pg_only`,
    /// `iceberg_only`, and `mismatched` rows. Exits non-zero if any
    /// table reports a non-empty diff. Day-2 confidence check: "is
    /// my mirror correct?"
    Verify {
        /// Per-PG-read chunk cap. Tradeoff: larger chunks = fewer
        /// round-trips, larger peak memory. 1024 mirrors the snapshot
        /// phase default.
        #[arg(long, default_value_t = 1024)]
        chunk_size: usize,
    },
    /// One-shot: run the initial snapshot phase for every configured
    /// table, persist `snapshot_complete` in the checkpoint, and exit.
    /// A subsequent `run` invocation will skip the snapshot and start
    /// CDC from `flushed_lsn`.
    ///
    /// Creates the replication slot before snapshotting if it doesn't
    /// exist yet — that pins the WAL from `consistent_point` onward so
    /// a later `run` doesn't lose any data committed between snapshot
    /// completion and CDC start.
    Snapshot,
    /// One-shot: drop the replication slot, drop the publication, and
    /// drop the coordinator schema (CASCADE). Used to tear down all
    /// PG-side state created by a pg2iceberg pipeline ahead of a
    /// re-bootstrap or final retirement.
    ///
    /// The slot must be inactive — stop any running consumer first.
    /// Idempotent in the per-resource sense (missing slot /
    /// publication / schema are skipped silently), but does **not**
    /// delete the materialized Iceberg tables — that has to be done
    /// out-of-band against the catalog.
    Cleanup,
    /// Distributed mode: WAL writer only. Captures pgoutput, stages
    /// parquet to S3, advances the slot. **Does not run the
    /// materializer cycle** — pair with one or more
    /// `materializer-only` workers reading from the same coord.
    ///
    /// Only one stream-only process per slot (PG enforces single
    /// consumer); scale the materializer side instead.
    StreamOnly,
    /// Distributed mode: materializer worker only. Reads staged
    /// parquet from coord + S3 and writes to Iceberg. **Does not
    /// open a replication slot**. Multiple workers register under
    /// the same `consumer_group` and round-robin tables across
    /// themselves; rebalances automatically on join/leave.
    ///
    /// The `--worker-id` must be process-unique (e.g. a k8s pod name)
    /// and stable across restarts of the same process.
    MaterializerOnly {
        /// Process-unique worker identity. Two workers claiming the
        /// same id will trample each other's heartbeat row in
        /// `_pg2iceberg.consumers` and produce undefined assignment.
        #[arg(long)]
        worker_id: String,
    },
}

fn main() -> Result<()> {
    // Dropped after the runtime: the spans not yet exported are, outside
    // async code.
    let _subscriber = pg2iceberg::subscriber::init(&config::process_env)?;
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .context("start the async runtime")?
        .block_on(run_command())
}

async fn run_command() -> Result<()> {
    let cli = Cli::parse();
    let config = cli.config;
    match cli.command {
        Command::Init { output, force } => {
            if config.is_some() {
                anyhow::bail!("init reads the environment, not a config file: drop --config");
            }
            init::run(&config::process_env, &output, force).await
        }
        Command::ConnectPg => connect_pg(&source(config)?).await,
        Command::ConnectIceberg => connect_iceberg(&Config::load(config.as_deref())?).await,
        Command::MigrateCoord => migrate_coord(&source(config)?).await,
        Command::Run => {
            let cfg = replicated(config).await?;
            let telemetry = serve(&cfg, Duration::ZERO).await?;
            run::run(cfg, telemetry.metrics()).await
        }
        Command::Compact => run::run_compact(replicated(config).await?).await,
        Command::Maintain { retention } => {
            run::run_maintain(replicated(config).await?, retention).await
        }
        Command::Verify { chunk_size } => {
            run::run_verify(replicated(config).await?, chunk_size).await
        }
        Command::Snapshot => {
            let cfg = replicated(config).await?;
            let telemetry = serve(&cfg, Duration::ZERO).await?;
            run::run_snapshot_only(cfg, telemetry.metrics()).await
        }
        Command::Cleanup => run::run_cleanup(source(config)?).await,
        Command::StreamOnly => {
            let cfg = replicated(config).await?;
            let telemetry = serve(&cfg, Duration::ZERO).await?;
            run::run_stream_only(cfg, telemetry.metrics()).await
        }
        Command::MaterializerOnly { worker_id } => {
            let cfg = replicated(config).await?;
            // An idle worker completes nothing between cycles.
            let between_cycles = cfg.sink.schedule()?.materialize * 3;
            let telemetry = serve(&cfg, between_cycles).await?;
            run::run_materializer_only(cfg, worker_id, telemetry.metrics()).await
        }
    }
}

/// Serve a long-running subcommand's `/metrics`, `/healthz` and
/// `/readyz`, its liveness timeout at least `at_least`.
async fn serve(cfg: &Config, at_least: Duration) -> Result<Arc<Telemetry>> {
    Telemetry::start(cfg, cfg.liveness_timeout()?.max(at_least)).await
}

/// The config, for a subcommand that needs the source database.
fn source(config: Option<PathBuf>) -> Result<Config> {
    let cfg = Config::load(config.as_deref())?;
    cfg.require_source()?;
    Ok(cfg)
}

/// The config, with the tables to replicate resolved (see
/// [`tables::resolve`]).
async fn replicated(config: Option<PathBuf>) -> Result<Config> {
    let mut cfg = source(config)?;
    tables::resolve(&mut cfg).await?;
    Ok(cfg)
}

// ── connect-pg ──────────────────────────────────────────────────────────

async fn connect_pg(cfg: &Config) -> Result<()> {
    use pg2iceberg_pg::{
        prod::{PgClientImpl, TlsMode},
        PgClient,
    };
    let tls = match cfg.source.postgres.tls_label() {
        "webpki" => TlsMode::Webpki,
        _ => TlsMode::Disable,
    };
    tracing::info!(
        slot = %cfg.source.logical.slot_name,
        publication = %cfg.source.logical.publication_name,
        ?tls,
        "connecting to source PG",
    );
    let client = PgClientImpl::connect_with(&cfg.source.postgres.dsn(), tls)
        .await
        .context("PG connect")?;
    let slot = &cfg.source.logical.slot_name;
    let exists = client.slot_exists(slot).await.context("slot lookup")?;
    if exists {
        let lsn = client
            .slot_restart_lsn(slot)
            .await
            .context("slot restart_lsn")?;
        tracing::info!(slot = %slot, ?lsn, "slot exists");
    } else {
        tracing::info!(slot = %slot, "slot does not exist; would be created on first run");
    }
    println!("OK: PG replication-mode connection established");
    Ok(())
}

// ── connect-iceberg ────────────────────────────────────────────────────

async fn connect_iceberg(cfg: &Config) -> Result<()> {
    use iceberg::Catalog as _;
    use pg2iceberg_iceberg::prod::IcebergRustCatalog;
    use std::sync::Arc;
    cfg.require_catalog()?;
    tracing::info!(uri = %cfg.sink.catalog_uri, warehouse = %cfg.sink.warehouse, "opening Iceberg REST catalog");
    let inner = run::build_rest_catalog(cfg).await?;
    let namespaces = inner
        .list_namespaces(None)
        .await
        .context("list the catalog's namespaces")?;
    tracing::info!(count = namespaces.len(), "catalog namespaces listed");
    let catalog = IcebergRustCatalog::new(Arc::new(inner));
    ensure_namespaces(&catalog, cfg).await?;
    println!("OK: Iceberg catalog connection established");
    Ok(())
}

/// Ensure the namespaces of the tables the config names.
async fn ensure_namespaces<C: iceberg::Catalog + Send + Sync + 'static>(
    catalog: &pg2iceberg_iceberg::prod::IcebergRustCatalog<C>,
    cfg: &Config,
) -> Result<()> {
    use pg2iceberg_iceberg::Catalog as _;
    for t in cfg.tables.iter().filter(|t| !t.is_pattern()) {
        let ident = t
            .iceberg_ident(&cfg.sink.namespace)
            .with_context(|| format!("parse table name {}", t.name))?;
        catalog
            .ensure_namespace(&ident.namespace)
            .await
            .with_context(|| format!("ensure namespace for {ident}"))?;
        tracing::info!(table = %t.name, "namespace ready");
    }
    Ok(())
}

// ── migrate-coord ──────────────────────────────────────────────────────

async fn migrate_coord(cfg: &Config) -> Result<()> {
    use pg2iceberg_coord::{
        prod::{connect_with, PostgresCoordinator, TlsMode},
        schema::CoordSchema,
    };
    let tls = match cfg.source.postgres.tls_label() {
        "webpki" => TlsMode::Webpki,
        _ => TlsMode::Disable,
    };
    tracing::info!(schema = %cfg.state.coordinator_schema, ?tls, "connecting to coord PG");
    let conn = connect_with(&cfg.coord_dsn(), tls)
        .await
        .context("coord connect")?;
    let schema = CoordSchema::sanitize(&cfg.state.coordinator_schema);
    let coord = PostgresCoordinator::new(conn, schema);
    coord.migrate().await.context("coord migrate")?;
    println!("OK: coordinator schema migrated");
    Ok(())
}
