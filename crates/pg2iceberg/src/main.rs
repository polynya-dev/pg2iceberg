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

mod config;
mod realio;
mod run;
mod snapshot_src;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use config::Config;
use std::path::PathBuf;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Smoke-test the PG replication-mode connection.
    ConnectPg {
        #[arg(long)]
        config: PathBuf,
    },
    /// Smoke-test the Iceberg catalog connection.
    ConnectIceberg {
        #[arg(long)]
        config: PathBuf,
    },
    /// Run the coordinator's idempotent schema migration. Safe to run
    /// repeatedly — every statement is `CREATE … IF NOT EXISTS`.
    MigrateCoord {
        #[arg(long)]
        config: PathBuf,
    },
    /// Run the full pipeline.
    Run {
        #[arg(long)]
        config: PathBuf,
    },
    /// One-shot: run a single compaction pass over every configured
    /// table and exit. Mirrors Go's `pg2iceberg compact` — for cron /
    /// k8s CronJob deployments where compaction runs out-of-band from
    /// the replication loop.
    Compact {
        #[arg(long)]
        config: PathBuf,
    },
    /// One-shot: run snapshot expiry over every configured table and
    /// exit. Mirrors Go's `pg2iceberg maintain`. Reads
    /// `sink.maintenance_retention` from YAML; CLI override takes
    /// precedence when supplied.
    Maintain {
        #[arg(long)]
        config: PathBuf,
        /// Override `sink.maintenance_retention`. Format matches Go's
        /// `time.ParseDuration` (e.g. `168h`, `7d`, `30m`).
        #[arg(long)]
        retention: Option<String>,
    },
    /// Diff PG ground truth against Iceberg materialized state for
    /// every configured table. Prints per-table counts of `pg_only`,
    /// `iceberg_only`, and `mismatched` rows. Exits non-zero if any
    /// table reports a non-empty diff. Day-2 confidence check: "is
    /// my mirror correct?"
    Verify {
        #[arg(long)]
        config: PathBuf,
        /// Per-PG-read chunk cap. Tradeoff: larger chunks = fewer
        /// round-trips, larger peak memory. 1024 mirrors the snapshot
        /// phase default.
        #[arg(long, default_value_t = 1024)]
        chunk_size: usize,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,pg2iceberg=debug".into()),
        )
        .init();

    let cli = Cli::parse();
    match cli.command {
        Command::ConnectPg { config } => connect_pg(&Config::load_from(config)?).await,
        Command::ConnectIceberg { config } => connect_iceberg(&Config::load_from(config)?).await,
        Command::MigrateCoord { config } => migrate_coord(&Config::load_from(config)?).await,
        Command::Run { config } => run::run(Config::load_from(config)?).await,
        Command::Compact { config } => run::run_compact(Config::load_from(config)?).await,
        Command::Maintain { config, retention } => {
            run::run_maintain(Config::load_from(config)?, retention).await
        }
        Command::Verify { config, chunk_size } => {
            run::run_verify(Config::load_from(config)?, chunk_size).await
        }
    }
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
    use pg2iceberg_iceberg::prod::IcebergRustCatalog;
    use std::sync::Arc;
    tracing::info!(uri = %cfg.sink.catalog_uri, warehouse = %cfg.sink.warehouse, "opening Iceberg REST catalog");
    let inner = run::build_rest_catalog(cfg).await?;
    let catalog = IcebergRustCatalog::new(Arc::new(inner));
    ensure_namespaces(&catalog, &cfg.tables).await?;
    println!("OK: Iceberg catalog connection established");
    Ok(())
}

async fn ensure_namespaces<C: iceberg::Catalog + Send + Sync + 'static>(
    catalog: &pg2iceberg_iceberg::prod::IcebergRustCatalog<C>,
    tables: &[config::TableConfig],
) -> Result<()> {
    use pg2iceberg_core::Namespace;
    use pg2iceberg_iceberg::Catalog as _;
    // `connect-iceberg` is a connectivity smoke test — we don't
    // need full column metadata here, just the parsed
    // (schema, table) so we know which namespaces to ensure.
    for t in tables {
        let (ns, name) = t
            .qualified()
            .with_context(|| format!("parse table name {}", t.name))?;
        let namespace = Namespace(vec![ns.clone()]);
        catalog
            .ensure_namespace(&namespace)
            .await
            .with_context(|| format!("ensure namespace for {ns}.{name}"))?;
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
