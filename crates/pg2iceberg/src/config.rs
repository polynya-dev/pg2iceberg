//! Binary configuration.
//!
//! Sections: `tables` (list), `source.{postgres, logical}`,
//! `sink` (catalog + storage + credential mode + flush knobs),
//! `state` (coordinator location).
//!
//! Every section is optional: a YAML file (see [`Config::load`]) and
//! environment variables (see [`Config::apply_env`]) each supply what
//! they set, and what's left out is inferred where it can be — the
//! tables (every one with a primary key), the catalog's auth, the
//! storage credentials, the region.
//!
//! Some fields are accepted by the deserializer but not yet consumed
//! at runtime (materializer cycle knobs,
//! control-plane metadata, etc.). They're carried in the schema so
//! configs round-trip cleanly while features land. Hence the
//! crate-level `dead_code` allow on the config structs — *fields*,
//! not types.

#![allow(dead_code)]

use anyhow::{Context, Result};
use pg2iceberg_core::{ColumnSchema, IcebergType, Namespace, PgType, TableIdent, TableSchema};
use serde::Deserialize;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

/// The config file read when neither `--config` nor `PG2ICEBERG_CONFIG`
/// names one, if it exists.
pub const DEFAULT_CONFIG_PATH: &str = "pg2iceberg.yaml";

/// Environment-variable lookup: the process environment in production,
/// a map in tests. Unset and empty variables both read as `None`.
pub type Env<'a> = &'a dyn Fn(&str) -> Option<String>;

/// The process environment, as an [`Env`].
pub fn process_env(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|v| !v.is_empty())
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct Config {
    /// Tables to replicate. Empty: every table with a primary key (see
    /// [`TableConfig::is_pattern`] for `schema.*`).
    #[serde(default)]
    pub tables: Vec<TableConfig>,
    #[serde(default)]
    pub source: SourceConfig,
    #[serde(default)]
    pub sink: SinkConfig,
    #[serde(default)]
    pub state: StateConfig,
    #[serde(default)]
    pub metrics_addr: String,
    #[serde(default)]
    pub snapshot_only: bool,
}

#[derive(Debug, Clone, Deserialize)]
pub struct TableConfig {
    /// Fully-qualified table name `"schema.name"`, or `"schema.*"` for
    /// every table with a primary key in the schema.
    pub name: String,
    #[serde(default)]
    pub skip_snapshot: bool,
    /// Operator-supplied PK columns. When non-empty, overrides whatever
    /// schema discovery found in the source `pg_index`.
    #[serde(default)]
    pub primary_key: Vec<String>,
    /// Optional column declarations. When provided, these override
    /// schema discovery; useful for tables where the source columns
    /// don't match what you want to materialize, or for testing.
    #[serde(default, rename = "columns")]
    pub columns: Vec<ColumnConfig>,
    /// Iceberg-specific per-table settings. Currently just partition.
    #[serde(default)]
    pub iceberg: IcebergTableConfig,
}

/// Iceberg per-table options. Just `partition` for now; sort orders
/// and other knobs are
/// follow-ons.
#[derive(Debug, Clone, Deserialize, Default)]
pub struct IcebergTableConfig {
    /// Partition expressions, one per partition field. Examples:
    /// `["day(created_at)"]`, `["region", "bucket[16](id)"]`. See
    /// [`pg2iceberg_core::parse_partition_expr`] for the grammar.
    #[serde(default)]
    pub partition: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ColumnConfig {
    pub name: String,
    pub pg_type: String,
    #[serde(default)]
    pub nullable: bool,
}

#[derive(Debug, Clone, Deserialize)]
pub struct SourceConfig {
    /// Replication mode. `"logical"` (the default) is the only one;
    /// kept so existing configs that spell it out still parse.
    #[serde(default = "default_mode")]
    pub mode: String,
    /// The source database as one connection string — a URL
    /// (`postgres://user:password@host:5432/db?sslmode=require`) or
    /// libpq `key=value` pairs — in place of `postgres`'s fields.
    /// Moved into [`PostgresConfig::url`] on load.
    #[serde(default)]
    pub postgres_url: String,
    #[serde(default)]
    pub postgres: PostgresConfig,
    #[serde(default)]
    pub logical: LogicalConfig,
}

impl Default for SourceConfig {
    fn default() -> Self {
        Self {
            mode: default_mode(),
            postgres_url: String::new(),
            postgres: PostgresConfig::default(),
            logical: LogicalConfig::default(),
        }
    }
}

fn default_mode() -> String {
    "logical".into()
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct PostgresConfig {
    /// A connection string (see [`SourceConfig::postgres_url`]). When
    /// set, it's used as is, and the fields below are ignored.
    #[serde(default)]
    pub url: String,
    #[serde(default)]
    pub host: String,
    #[serde(default = "default_pg_port")]
    pub port: u16,
    #[serde(default)]
    pub database: String,
    #[serde(default)]
    pub user: String,
    #[serde(default)]
    pub password: String,
    /// `"disable"` (default), `"require"`, `"verify-ca"`, `"verify-full"`.
    /// Maps to `TlsMode::Disable` (`"disable"`) or `TlsMode::Webpki`
    /// (`"require"` / `"verify-ca"` / `"verify-full"`); custom CA and
    /// hostname verification flavors aren't yet differentiated.
    #[serde(default)]
    pub sslmode: String,
}

fn default_pg_port() -> u16 {
    5432
}

impl PostgresConfig {
    /// The connection string: [`Self::url`], or the fields as libpq-style
    /// `key=value` pairs.
    pub fn dsn(&self) -> String {
        if !self.url.is_empty() {
            return self.url.clone();
        }
        let sslmode = if self.sslmode.is_empty() {
            "disable"
        } else {
            self.sslmode.as_str()
        };
        format!(
            "host={} port={} dbname={} user={} password={} sslmode={}",
            self.host, self.port, self.database, self.user, self.password, sslmode
        )
    }

    /// Map the Postgres `sslmode` to our [`pg2iceberg_pg::prod::TlsMode`].
    /// `disable` → `Disable`; everything else maps to `Webpki` since
    /// `tokio-postgres-rustls` always verifies server certs against
    /// the configured roots. Hostname-only / CA-only differentiation
    /// is a follow-on (see plan §"Remaining items for the binary").
    ///
    /// With a [`Self::url`], its own `sslmode` decides; left out, TLS is
    /// off, as with the `sslmode` field.
    pub fn tls_label(&self) -> &str {
        let sslmode = if self.url.is_empty() {
            self.sslmode.as_str()
        } else {
            sslmode_in(&self.url).unwrap_or("")
        };
        match sslmode {
            "" | "disable" | "off" | "false" => "disable",
            _ => "webpki",
        }
    }

    /// Whether a database to connect to is configured at all.
    pub fn is_configured(&self) -> bool {
        !self.url.is_empty() || !self.host.is_empty()
    }
}

/// The `sslmode` a connection string sets: a URL's query parameter, or a
/// `key=value` pair.
fn sslmode_in(dsn: &str) -> Option<&str> {
    let params: Vec<&str> = match dsn.split_once('?') {
        Some((_, query)) if dsn.contains("://") => query.split('&').collect(),
        _ => dsn.split_whitespace().collect(),
    };
    params
        .into_iter()
        .filter_map(|p| p.split_once('='))
        .find(|(k, _)| *k == "sslmode")
        .map(|(_, v)| v)
}

#[derive(Debug, Clone, Deserialize)]
pub struct LogicalConfig {
    #[serde(default = "default_publication")]
    pub publication_name: String,
    #[serde(default = "default_slot")]
    pub slot_name: String,
    #[serde(default)]
    pub standby_interval: String,
}

impl Default for LogicalConfig {
    fn default() -> Self {
        Self {
            publication_name: default_publication(),
            slot_name: default_slot(),
            standby_interval: String::new(),
        }
    }
}

fn default_publication() -> String {
    "pg2iceberg_pub".into()
}

fn default_slot() -> String {
    "pg2iceberg_slot".into()
}

#[derive(Debug, Clone, Deserialize)]
pub struct SinkConfig {
    #[serde(default)]
    pub catalog_uri: String,
    /// `"none"` / `"sigv4"` / `"bearer"` / `"oauth2"`. Empty: inferred
    /// (see [`Self::resolved_catalog_auth`]).
    #[serde(default)]
    pub catalog_auth: String,
    #[serde(default)]
    pub catalog_token: String,
    #[serde(default)]
    pub catalog_client_id: String,
    #[serde(default)]
    pub catalog_client_secret: String,

    /// `"static"` — explicit S3 keys below.
    /// `"vended"` — temporary credentials from the catalog's
    /// `LoadTable` response.
    /// `"iam"` — the AWS default credential chain: `AWS_*` environment
    /// variables, a profile, an instance or task role.
    /// Empty: inferred (see [`Self::resolved_credential_mode`]).
    #[serde(default)]
    pub credential_mode: String,

    #[serde(default)]
    pub warehouse: String,
    /// Iceberg namespace for every table. Empty: each table's PG schema.
    #[serde(default)]
    pub namespace: String,
    /// S3 endpoint for S3-compatible storage (MinIO, R2, ...). Empty:
    /// AWS's own.
    #[serde(default)]
    pub s3_endpoint: String,
    #[serde(default)]
    pub s3_access_key: String,
    #[serde(default)]
    pub s3_secret_key: String,
    /// Empty: inferred (see [`Self::resolved_region`]).
    #[serde(default)]
    pub s3_region: String,

    /// How often `run` stages buffered changes. Empty = 10s. Staging
    /// also happens sooner, once `flush_rows` changes are buffered.
    #[serde(default)]
    pub flush_interval: String,
    /// Most change events the WAL writer holds in memory before staging
    /// them; also the staged chunk size for larger transactions.
    #[serde(default = "default_flush_rows")]
    pub flush_rows: usize,
    /// Most change events the materializer folds into one snapshot step —
    /// the bound on its memory. A transaction spanning several steps is
    /// still committed atomically.
    #[serde(default = "default_materializer_batch_rows")]
    pub materializer_batch_rows: usize,
    /// How often the materializer commits staged changes to Iceberg —
    /// in `run` and `materializer-only`. Matches Go's
    /// `materializer_interval`. Empty = 10s.
    #[serde(default)]
    pub materializer_interval: String,

    /// Compaction file-count thresholds. Names match Go
    /// (`compaction_data_files`, `compaction_delete_files`). Compaction
    /// runs as part of every materializer cycle, gated by these
    /// thresholds — so most cycles do no compaction work.
    #[serde(default = "default_compaction_data_files")]
    pub compaction_data_files: usize,
    #[serde(default = "default_compaction_delete_files")]
    pub compaction_delete_files: usize,
    /// Target output file size in bytes for compaction. Files smaller
    /// than `target_file_size / 2` are eligible for rewrite. Matches
    /// Go's `target_file_size`. 0 = disable compaction (no files are
    /// ever rewritten).
    #[serde(default = "default_target_file_size")]
    pub target_file_size: u64,

    /// Snapshot retention as a duration string (e.g. `"168h"` for 7
    /// days). Matches Go's `maintenance_retention`. Empty means no
    /// expiry. Used by the `pg2iceberg maintain` subcommand.
    #[serde(default)]
    pub maintenance_retention: String,

    /// Grace period for orphan-file cleanup (e.g. `"30m"`). Files
    /// younger than this are protected from deletion even if
    /// unreferenced — it gives in-flight commits a window before
    /// cleanup races them. Default `30m`.
    #[serde(default = "default_maintenance_grace")]
    pub maintenance_grace: String,

    /// Free-form REST-catalog props passthrough. Useful for
    /// vendor-specific settings (Polaris OAuth2
    /// server URI, etc.) without us having to enumerate every quirk.
    #[serde(default, rename = "catalog_props")]
    pub catalog_props: BTreeMap<String, String>,

    /// Blue-green replica-alignment marker mode (per Go's
    /// `examples/blue-green/`). When set, pg2iceberg watches
    /// `_pg2iceberg.markers` in the source PG, includes it in the
    /// publication, and emits `(uuid, table_name, snapshot_id)` rows
    /// to a meta-marker Iceberg table at `<meta_namespace>.markers`.
    /// External `iceberg-diff` joins blue's and green's tables on
    /// `marker_uuid` to verify replica equivalence at WAL points.
    /// Empty → marker mode disabled (default).
    ///
    /// **Operator precondition**: the bluegreen PG↔PG replication
    /// publication must also include `_pg2iceberg.markers` so the
    /// marker INSERT is replicated to green. pg2iceberg can't
    /// enforce this — see `examples/blue-green/` from the Go
    /// reference for the bootstrap.
    #[serde(default)]
    pub meta_namespace: String,
}

impl Default for SinkConfig {
    fn default() -> Self {
        Self {
            catalog_uri: String::new(),
            catalog_auth: String::new(),
            catalog_token: String::new(),
            catalog_client_id: String::new(),
            catalog_client_secret: String::new(),
            credential_mode: String::new(),
            warehouse: String::new(),
            namespace: String::new(),
            s3_endpoint: String::new(),
            s3_access_key: String::new(),
            s3_secret_key: String::new(),
            s3_region: String::new(),
            flush_interval: String::new(),
            flush_rows: default_flush_rows(),
            materializer_batch_rows: default_materializer_batch_rows(),
            materializer_interval: String::new(),
            compaction_data_files: default_compaction_data_files(),
            compaction_delete_files: default_compaction_delete_files(),
            target_file_size: default_target_file_size(),
            maintenance_retention: String::new(),
            maintenance_grace: default_maintenance_grace(),
            catalog_props: BTreeMap::new(),
            meta_namespace: String::new(),
        }
    }
}

impl SinkConfig {
    /// `credential_mode`, or the one the rest implies: explicit S3 keys
    /// mean `static`; an `s3://` warehouse, the AWS default credential
    /// chain (`iam`); otherwise — no warehouse, or a catalog-side name
    /// for one (Polaris, Lakekeeper, R2) — the catalog vends credentials.
    pub fn resolved_credential_mode(&self) -> &str {
        if !self.credential_mode.is_empty() {
            &self.credential_mode
        } else if !self.s3_access_key.is_empty() {
            "static"
        } else if self.warehouse.starts_with("s3://") || self.warehouse.starts_with("s3a://") {
            "iam"
        } else {
            "vended"
        }
    }

    /// `catalog_auth`, or the one the rest implies: a token means
    /// `bearer`, client credentials `oauth2`, and an AWS endpoint (S3
    /// Tables, Glue) `sigv4`.
    pub fn resolved_catalog_auth(&self) -> &str {
        if !self.catalog_auth.is_empty() {
            &self.catalog_auth
        } else if !self.catalog_token.is_empty() {
            "bearer"
        } else if !self.catalog_client_id.is_empty() {
            "oauth2"
        } else if aws_endpoint(&self.catalog_uri).is_some() {
            "sigv4"
        } else {
            "none"
        }
    }

    /// `s3_region`, else an AWS catalog endpoint's, else `us-east-1`.
    pub fn resolved_region(&self) -> String {
        if !self.s3_region.is_empty() {
            return self.s3_region.clone();
        }
        aws_endpoint(&self.catalog_uri)
            .map(|(_, region)| region)
            .unwrap_or_else(|| "us-east-1".into())
    }

    /// Translate the Go-shaped sink fields into the iceberg crate's
    /// `CompactionConfig` shape. Used on every materializer cycle.
    pub fn compaction_config(&self) -> pg2iceberg_iceberg::CompactionConfig {
        pg2iceberg_iceberg::CompactionConfig {
            data_file_threshold: self.compaction_data_files,
            delete_file_threshold: self.compaction_delete_files,
            target_size_bytes: self.target_file_size,
            max_input_bytes_per_pass: self
                .target_file_size
                .saturating_mul(pg2iceberg_iceberg::CompactionConfig::DEFAULT_PASS_TARGET_FILES),
        }
    }

    /// The main loop's cadence: `flush_interval` for staging,
    /// `materializer_interval` for committing; each 10s when empty.
    pub fn schedule(&self) -> Result<pg2iceberg_logical::Schedule> {
        let default = pg2iceberg_logical::Schedule::default();
        Ok(pg2iceberg_logical::Schedule {
            flush: interval("flush_interval", &self.flush_interval, default.flush)?,
            materialize: interval(
                "materializer_interval",
                &self.materializer_interval,
                default.materialize,
            )?,
            ..default
        })
    }

    /// Parse `maintenance_retention` ("168h", "7d", "30m", etc.) into
    /// milliseconds. Returns `Ok(None)` when the field is empty.
    pub fn maintenance_retention_ms(&self) -> Result<Option<i64>> {
        if self.maintenance_retention.is_empty() {
            return Ok(None);
        }
        let dur = humantime::parse_duration(&self.maintenance_retention).with_context(|| {
            format!(
                "parse maintenance_retention `{}`",
                self.maintenance_retention
            )
        })?;
        Ok(Some(dur.as_millis().try_into().unwrap_or(i64::MAX)))
    }
}

/// The interval `value` ("10s", "1m", ...) of the setting `name`, or
/// `default` when empty.
fn interval(name: &str, value: &str, default: std::time::Duration) -> Result<std::time::Duration> {
    if value.is_empty() {
        return Ok(default);
    }
    let d =
        humantime::parse_duration(value).with_context(|| format!("parse sink.{name} `{value}`"))?;
    if d.is_zero() {
        anyhow::bail!("sink.{name} must be longer than zero");
    }
    Ok(d)
}

/// `(service, region)` of an AWS endpoint URL, such as
/// `https://s3tables.us-east-1.amazonaws.com/iceberg`: the SigV4 signing
/// name and region.
fn aws_endpoint(uri: &str) -> Option<(String, String)> {
    let host = uri.split_once("://").map_or(uri, |(_, rest)| rest);
    let host = host.split(['/', ':']).next()?;
    let labels: Vec<&str> = host.strip_suffix(".amazonaws.com")?.split('.').collect();
    match labels.as_slice() {
        [service, region] => Some((service.to_string(), region.to_string())),
        _ => None,
    }
}

fn default_flush_rows() -> usize {
    10_000
}

fn default_materializer_batch_rows() -> usize {
    50_000
}

fn default_compaction_data_files() -> usize {
    8
}

fn default_compaction_delete_files() -> usize {
    4
}

fn default_target_file_size() -> u64 {
    128 * 1024 * 1024
}

fn default_maintenance_grace() -> String {
    "30m".into()
}

#[derive(Debug, Clone, Deserialize)]
pub struct StateConfig {
    /// File-backed state path (sim-only / dev). Not honored in the
    /// Rust prod path — leave empty.
    #[serde(default)]
    pub path: String,
    /// Postgres URL for the coordinator. If absent, the source PG
    /// hosts the `_pg2iceberg.*` schema.
    #[serde(default)]
    pub postgres_url: String,
    #[serde(default = "default_coord_schema")]
    pub coordinator_schema: String,
    /// Materialization group name (consumer / mat_cursor key).
    /// Defaults to `"default"`.
    #[serde(default = "default_group")]
    pub group: String,
}

impl Default for StateConfig {
    fn default() -> Self {
        Self {
            path: String::new(),
            postgres_url: String::new(),
            coordinator_schema: default_coord_schema(),
            group: default_group(),
        }
    }
}

fn default_coord_schema() -> String {
    "_pg2iceberg".into()
}

fn default_group() -> String {
    "default".into()
}

impl Config {
    /// Load the config from the environment, and a YAML file if there is
    /// one: `path`, else `PG2ICEBERG_CONFIG`, else `pg2iceberg.yaml` if it
    /// exists. Environment variables override the file (see
    /// [`Self::apply_env`]).
    pub fn load(path: Option<&Path>) -> Result<Self> {
        Self::load_with(path, &process_env)
    }

    /// [`Self::load`], with `env` for the environment.
    pub fn load_with(path: Option<&Path>, env: Env) -> Result<Self> {
        let path: Option<PathBuf> = match path {
            Some(path) => Some(path.to_path_buf()),
            None => env("PG2ICEBERG_CONFIG").map(PathBuf::from).or_else(|| {
                let default = Path::new(DEFAULT_CONFIG_PATH);
                default.exists().then(|| default.to_path_buf())
            }),
        };
        let cfg = match &path {
            Some(path) => {
                tracing::info!(path = %path.display(), "reading config");
                let raw = std::fs::read_to_string(path)
                    .with_context(|| format!("read config from {}", path.display()))?;
                Self::parse(&raw, env)
                    .with_context(|| format!("parse config at {}", path.display()))?
            }
            None => Config::default(),
        };
        cfg.finish(env)
    }

    /// The config the environment alone gives, without reading any file.
    pub fn from_env(env: Env) -> Result<Self> {
        Config::default().finish(env)
    }

    fn finish(mut self, env: Env) -> Result<Self> {
        self.apply_env(env)?;
        if !self.source.postgres_url.is_empty() {
            self.source.postgres.url = std::mem::take(&mut self.source.postgres_url);
        }
        self.validate_mode()?;
        self.validate_tables()?;
        self.sink.schedule()?;
        Ok(self)
    }

    /// Parse YAML, with `${NAME}` in its string values replaced by the
    /// environment variable `NAME`.
    pub fn parse(yaml: &str, env: Env) -> Result<Self> {
        let mut value: serde_yaml::Value = serde_yaml::from_str(yaml)?;
        if value.is_null() {
            return Ok(Config::default());
        }
        interpolate(&mut value, env)?;
        Ok(serde_yaml::from_value(value)?)
    }

    /// Environment variables, over whatever the file set:
    ///
    /// | Variable | Field |
    /// |---|---|
    /// | `POSTGRES_URL` | `source.postgres_url` |
    /// | `PG2ICEBERG_TABLES` | `tables`: comma-separated `schema.table` or `schema.*` |
    /// | `PG2ICEBERG_SLOT` / `PG2ICEBERG_PUBLICATION` | `source.logical.slot_name` / `publication_name` |
    /// | `PG2ICEBERG_STATE_URL` | `state.postgres_url` |
    /// | `ICEBERG_CATALOG_URL` | `sink.catalog_uri` |
    /// | `ICEBERG_CATALOG_AUTH` | `sink.catalog_auth` |
    /// | `ICEBERG_CATALOG_TOKEN` | `sink.catalog_token` |
    /// | `ICEBERG_CATALOG_CLIENT_ID` / `_SECRET` | `sink.catalog_client_id` / `_secret` |
    /// | `ICEBERG_WAREHOUSE` | `sink.warehouse` |
    /// | `ICEBERG_NAMESPACE` | `sink.namespace` |
    /// | `ICEBERG_CREDENTIAL_MODE` | `sink.credential_mode` |
    ///
    /// AWS's own variables fill in only what the file leaves out:
    /// `AWS_REGION` / `AWS_DEFAULT_REGION` for `sink.s3_region`, and
    /// `AWS_ENDPOINT_URL_S3` / `AWS_ENDPOINT_URL` for `sink.s3_endpoint`.
    /// Credentials (`AWS_ACCESS_KEY_ID`, ...) are read by the AWS
    /// default credential chain itself (`credential_mode: iam`).
    fn apply_env(&mut self, env: Env) -> Result<()> {
        let set = |field: &mut String, name: &str| {
            if let Some(value) = env(name) {
                *field = value;
            }
        };
        set(&mut self.source.postgres_url, "POSTGRES_URL");
        set(&mut self.source.logical.slot_name, "PG2ICEBERG_SLOT");
        set(
            &mut self.source.logical.publication_name,
            "PG2ICEBERG_PUBLICATION",
        );
        set(&mut self.state.postgres_url, "PG2ICEBERG_STATE_URL");
        set(&mut self.sink.catalog_uri, "ICEBERG_CATALOG_URL");
        set(&mut self.sink.catalog_auth, "ICEBERG_CATALOG_AUTH");
        set(&mut self.sink.catalog_token, "ICEBERG_CATALOG_TOKEN");
        set(
            &mut self.sink.catalog_client_id,
            "ICEBERG_CATALOG_CLIENT_ID",
        );
        set(
            &mut self.sink.catalog_client_secret,
            "ICEBERG_CATALOG_CLIENT_SECRET",
        );
        set(&mut self.sink.warehouse, "ICEBERG_WAREHOUSE");
        set(&mut self.sink.namespace, "ICEBERG_NAMESPACE");
        set(&mut self.sink.credential_mode, "ICEBERG_CREDENTIAL_MODE");
        if let Some(tables) = env("PG2ICEBERG_TABLES") {
            self.tables = tables
                .split(|c: char| c == ',' || c.is_whitespace())
                .filter(|name| !name.is_empty())
                .map(TableConfig::named)
                .collect();
        }
        let fill = |field: &mut String, names: &[&str]| {
            if field.is_empty() {
                if let Some(value) = names.iter().find_map(|name| env(name)) {
                    *field = value;
                }
            }
        };
        fill(
            &mut self.sink.s3_region,
            &["AWS_REGION", "AWS_DEFAULT_REGION"],
        );
        fill(
            &mut self.sink.s3_endpoint,
            &["AWS_ENDPOINT_URL_S3", "AWS_ENDPOINT_URL"],
        );
        Ok(())
    }

    /// A source database is configured.
    pub fn require_source(&self) -> Result<()> {
        let pg = &self.source.postgres;
        if !pg.url.is_empty()
            || !(pg.host.is_empty() || pg.database.is_empty() || pg.user.is_empty())
        {
            return Ok(());
        }
        anyhow::bail!(
            "no source database configured: set POSTGRES_URL \
             (postgres://user:password@host:5432/database), or source.postgres_url in {DEFAULT_CONFIG_PATH}"
        )
    }

    /// An Iceberg catalog is configured.
    pub fn require_catalog(&self) -> Result<()> {
        if !self.sink.catalog_uri.is_empty() {
            return Ok(());
        }
        anyhow::bail!(
            "no Iceberg catalog configured: set ICEBERG_CATALOG_URL (a REST catalog, such as \
             http://localhost:8181), or sink.catalog_uri in {DEFAULT_CONFIG_PATH}"
        )
    }

    /// Every table needs an Iceberg table of its own. `sink.namespace`
    /// replaces the PG schema, so `public.orders` and `sales.orders`
    /// would both land in `<namespace>.orders`, their rows mixed.
    /// `schema.*` stands for tables yet to be found, and can't carry
    /// settings for a particular one.
    pub fn validate_tables(&self) -> Result<()> {
        let mut targets: BTreeMap<TableIdent, &str> = BTreeMap::new();
        for t in &self.tables {
            if t.is_pattern() {
                if !t.columns.is_empty()
                    || !t.primary_key.is_empty()
                    || !t.iceberg.partition.is_empty()
                {
                    anyhow::bail!(
                        "{} can't set columns, primary_key or iceberg.partition: list the \
                         tables that need them by name",
                        t.name
                    );
                }
                continue;
            }
            let ident = t.iceberg_ident(&self.sink.namespace)?;
            match targets.insert(ident.clone(), &t.name) {
                Some(other) if other == t.name => {
                    anyhow::bail!("table {other} is listed twice in `tables`")
                }
                Some(other) => anyhow::bail!(
                    "tables {other} and {} both map to Iceberg table {ident}: sink.namespace \
                     {:?} replaces their PG schemas, so their rows would mix in one table. \
                     Replicate only one of them, or leave sink.namespace unset so each PG \
                     schema is its own Iceberg namespace",
                    t.name,
                    self.sink.namespace
                ),
                None => {}
            }
        }
        Ok(())
    }

    /// Reject anything but logical replication up front, so every
    /// subcommand fails fast with a pointer instead of half-starting.
    fn validate_mode(&self) -> Result<()> {
        match self.source.mode.as_str() {
            "" | "logical" => Ok(()),
            "query" => anyhow::bail!(
                "source.mode \"query\" is no longer supported: pg2iceberg only replicates \
                 via logical replication. Set source.mode to \"logical\" (or remove it) and \
                 enable wal_level=logical on the source"
            ),
            other => anyhow::bail!("unknown source.mode {other:?}; expected \"logical\""),
        }
    }

    /// Connection string for the coordinator. Defaults to the source
    /// PG when `state.postgres_url` is unset.
    pub fn coord_dsn(&self) -> String {
        if !self.state.postgres_url.is_empty() {
            self.state.postgres_url.clone()
        } else {
            self.source.postgres.dsn()
        }
    }

    /// Bag of REST catalog props derived from sink config. Keys we
    /// pass: `uri`, `warehouse`, plus auth-flavor-specific bits, plus
    /// S3 storage-factory props (`s3.*`) so iceberg-rust's REST
    /// catalog can sign + send catalog-side IO. The catalog server's
    /// `/v1/config` response can also carry these, but only the
    /// endpoint/region/path-style/region — credentials almost always
    /// have to come from the client side. `catalog_props` from YAML
    /// is layered on top.
    pub fn rest_catalog_props(&self) -> BTreeMap<String, String> {
        let mut props: BTreeMap<String, String> = BTreeMap::new();
        props.insert("uri".into(), self.sink.catalog_uri.clone());
        if !self.sink.warehouse.is_empty() {
            props.insert("warehouse".into(), self.sink.warehouse.clone());
        }
        let region = self.sink.resolved_region();
        match self.sink.resolved_catalog_auth() {
            "bearer" if !self.sink.catalog_token.is_empty() => {
                props.insert("token".into(), self.sink.catalog_token.clone());
            }
            "oauth2" if !self.sink.catalog_client_id.is_empty() => {
                props.insert(
                    "oauth2-server-uri".into(),
                    format!(
                        "{}/v1/oauth/tokens",
                        self.sink.catalog_uri.trim_end_matches('/')
                    ),
                );
                props.insert(
                    "credential".into(),
                    format!(
                        "{}:{}",
                        self.sink.catalog_client_id, self.sink.catalog_client_secret
                    ),
                );
            }
            // Requests signed with the AWS default credential chain's
            // credentials, for the service the endpoint names (S3
            // Tables' `s3tables`, Glue's `glue`).
            "sigv4" => {
                props.insert("rest.sigv4-enabled".into(), "true".into());
                props.insert("rest.signing-region".into(), region.clone());
                if let Some((service, _)) = aws_endpoint(&self.sink.catalog_uri) {
                    props.insert("rest.signing-name".into(), service);
                }
            }
            _ => {}
        }
        let credential_mode = self.sink.resolved_credential_mode();
        // Vended-credentials mode requires the
        // `X-Iceberg-Access-Delegation: vended-credentials` header on
        // every catalog request. iceberg-rust's REST client already
        // forwards `header.<name>: <value>` props through to outgoing
        // requests, so we set it here when credential_mode=vended.
        // Operators can override via explicit `catalog_props` if they
        // need a different delegation type (e.g. `remote-signing`).
        if credential_mode == "vended" {
            props.insert(
                "header.x-iceberg-access-delegation".into(),
                "vended-credentials".into(),
            );
        }
        // S3 props for iceberg-rust's StorageFactory. Only set
        // credentials when credential_mode=static — for `iam` we let
        // OpenDAL pick them up from env/IMDS/EC2 metadata. Endpoint
        // + region + path-style are always set when configured.
        if !self.sink.s3_endpoint.is_empty() {
            props.insert("s3.endpoint".into(), self.sink.s3_endpoint.clone());
            // S3-compatible storage (MinIO, LocalStack) takes path-style
            // requests, whatever the credentials. The file IO's default is
            // virtual-hosted, addressing `<bucket>.<endpoint host>`, which
            // MinIO answers with a 404. Mirrors the blob store.
            props.insert("s3.path-style-access".into(), "true".into());
        }
        props.insert("s3.region".into(), region);
        if credential_mode == "static" {
            if !self.sink.s3_access_key.is_empty() {
                props.insert("s3.access-key-id".into(), self.sink.s3_access_key.clone());
            }
            if !self.sink.s3_secret_key.is_empty() {
                props.insert(
                    "s3.secret-access-key".into(),
                    self.sink.s3_secret_key.clone(),
                );
            }
            // Skip EC2 IMDS so unrelated AWS credential lookups don't
            // surface as cryptic "169.254.169.254 unreachable" errors
            // in non-AWS environments. Surfaced by the testcontainers
            // integration test against MinIO + apache/iceberg-rest.
            props.insert("s3.disable-ec2-metadata".into(), "true".into());
            props.insert("s3.disable-config-load".into(), "true".into());
        }
        for (k, v) in &self.sink.catalog_props {
            props.insert(k.clone(), v.clone());
        }
        props
    }
}

impl TableConfig {
    /// A table by name alone, every other setting its default.
    pub fn named(name: &str) -> Self {
        Self {
            name: name.to_string(),
            skip_snapshot: false,
            primary_key: Vec::new(),
            columns: Vec::new(),
            iceberg: IcebergTableConfig::default(),
        }
    }

    /// Whether this names every table in a schema (`schema.*`) rather
    /// than one.
    pub fn is_pattern(&self) -> bool {
        self.name.ends_with(".*")
    }

    /// `(schema, table)` parsed from the YAML `name`.
    pub fn qualified(&self) -> Result<(String, String)> {
        parse_qualified_name(&self.name)
    }

    /// The Iceberg table this table materializes to: in `sink_namespace`
    /// when set, else in its PG schema. Tables with explicit `columns:`
    /// keep their PG schema.
    pub fn iceberg_ident(&self, sink_namespace: &str) -> Result<TableIdent> {
        let (schema, name) = self.qualified()?;
        let namespace = if sink_namespace.is_empty() || self.has_explicit_columns() {
            schema
        } else {
            sink_namespace.to_string()
        };
        Ok(TableIdent {
            namespace: Namespace(vec![namespace]),
            name,
        })
    }

    /// `True` when the operator has explicitly declared columns in
    /// YAML. When `false`, the binary discovers schema from
    /// `information_schema.columns` + `pg_index` at startup.
    pub fn has_explicit_columns(&self) -> bool {
        !self.columns.is_empty()
    }

    /// Convert to our [`TableSchema`]. Splits `name` on `.` to
    /// recover the (namespace, table) pair. Errors if `columns:` is
    /// empty; callers should branch via [`Self::has_explicit_columns`]
    /// and use schema discovery instead in that case.
    pub fn to_table_schema(&self) -> Result<TableSchema> {
        let (ns, name) = parse_qualified_name(&self.name)?;
        if self.columns.is_empty() {
            anyhow::bail!(
                "table {ns}.{name} has no explicit columns; \
                 the binary uses live schema discovery in this case — \
                 callers should branch on TableConfig::has_explicit_columns() \
                 instead of calling to_table_schema() directly"
            );
        }
        let pk_set: std::collections::BTreeSet<&str> =
            self.primary_key.iter().map(String::as_str).collect();
        let mut columns: Vec<ColumnSchema> = Vec::with_capacity(self.columns.len());
        for (idx, c) in self.columns.iter().enumerate() {
            let pg = parse_pg_type(&c.pg_type)
                .with_context(|| format!("unknown pg_type for column {}: {}", c.name, c.pg_type))?;
            let mapped = pg2iceberg_core::map_pg_to_iceberg(pg)
                .with_context(|| format!("map column {} ({:?}) to Iceberg", c.name, pg))?;
            columns.push(ColumnSchema {
                name: c.name.clone(),
                field_id: (idx + 1) as i32,
                ty: mapped.iceberg,
                nullable: c.nullable && !pk_set.contains(c.name.as_str()),
                is_primary_key: pk_set.contains(c.name.as_str()),
            });
        }
        let partition_spec = pg2iceberg_core::parse_partition_spec(&self.iceberg.partition)
            .map_err(|e| anyhow::anyhow!("partition spec for {}: {e}", self.name))?;
        Ok(TableSchema {
            ident: TableIdent {
                namespace: Namespace(vec![ns]),
                name,
            },
            columns,
            partition_spec,
            pg_schema: None,
        })
    }
}

/// Replace `${NAME}` in every string in `value` with the environment
/// variable `NAME`; `$${` is a literal `${`. Comments are left alone, so
/// a commented-out line can name a variable that isn't set.
fn interpolate(value: &mut serde_yaml::Value, env: Env) -> Result<()> {
    use serde_yaml::Value;
    match value {
        Value::String(s) => *s = interpolate_str(s, env)?,
        Value::Sequence(items) => {
            for item in items {
                interpolate(item, env)?;
            }
        }
        Value::Mapping(map) => {
            for (_, item) in map.iter_mut() {
                interpolate(item, env)?;
            }
        }
        Value::Tagged(tagged) => interpolate(&mut tagged.value, env)?,
        Value::Null | Value::Bool(_) | Value::Number(_) => {}
    }
    Ok(())
}

fn interpolate_str(s: &str, env: Env) -> Result<String> {
    let mut out = String::with_capacity(s.len());
    let mut rest = s;
    while let Some(at) = rest.find('$') {
        out.push_str(&rest[..at]);
        let after = &rest[at + 1..];
        if let Some(tail) = after.strip_prefix("${") {
            out.push_str("${");
            rest = tail;
        } else if let Some(tail) = after.strip_prefix('{') {
            let end = tail
                .find('}')
                .with_context(|| format!("unterminated `${{` in {s:?}"))?;
            let name = &tail[..end];
            let value = env(name)
                .with_context(|| format!("the config uses ${{{name}}}, which isn't set"))?;
            out.push_str(&value);
            rest = &tail[end + 1..];
        } else {
            out.push('$');
            rest = after;
        }
    }
    out.push_str(rest);
    Ok(out)
}

/// Split `"schema.table"` → `("schema", "table")`. Errors on
/// missing dot.
fn parse_qualified_name(qualified: &str) -> Result<(String, String)> {
    let (ns, name) = qualified
        .split_once('.')
        .with_context(|| format!("table name must be \"schema.name\": {qualified}"))?;
    if ns.is_empty() || name.is_empty() {
        anyhow::bail!("table name must be \"schema.name\": {qualified}");
    }
    Ok((ns.to_string(), name.to_string()))
}

/// Parse a Postgres type name (case-insensitive) into [`PgType`].
fn parse_pg_type(name: &str) -> Result<PgType> {
    let lower = name.to_ascii_lowercase();
    Ok(match lower.as_str() {
        "bool" | "boolean" => PgType::Bool,
        "int2" | "smallint" => PgType::Int2,
        "int4" | "integer" | "int" | "serial" => PgType::Int4,
        "int8" | "bigint" | "bigserial" => PgType::Int8,
        "float4" | "real" => PgType::Float4,
        "float8" | "double precision" | "double" => PgType::Float8,
        "numeric" | "decimal" => PgType::Numeric {
            precision: None,
            scale: None,
        },
        "text" | "varchar" | "character varying" | "bpchar" | "char" | "character" | "name" => {
            PgType::Text
        }
        "bytea" => PgType::Bytea,
        "date" => PgType::Date,
        "time" | "time without time zone" => PgType::Time,
        "timetz" | "time with time zone" => PgType::TimeTz,
        "timestamp" | "timestamp without time zone" => PgType::Timestamp,
        "timestamptz" | "timestamp with time zone" => PgType::TimestampTz,
        "uuid" => PgType::Uuid,
        "json" => PgType::Json,
        "jsonb" => PgType::Jsonb,
        "oid" => PgType::Oid,
        _ => anyhow::bail!("unknown pg_type: {name}"),
    })
}

/// Suppress unused-import warning when `IcebergType` is exported but
/// nothing in this module references it directly.
#[allow(dead_code)]
fn _types_keep_alive(_: IcebergType) {}

#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLE: &str = r#"
tables:
  - name: public.orders
    primary_key: [id]
    columns:
      - name: id
        pg_type: int4
      - name: qty
        pg_type: int8
        nullable: true

source:
  mode: logical
  postgres:
    host: localhost
    port: 5432
    database: src
    user: postgres
    password: secret
    sslmode: require
  logical:
    publication_name: p2i_pub
    slot_name: p2i_slot

sink:
  catalog_uri: http://localhost:8181
  catalog_auth: bearer
  catalog_token: REDACTED
  credential_mode: static
  warehouse: s3://warehouse/
  namespace: public
  s3_endpoint: http://localhost:9000
  s3_access_key: admin
  s3_secret_key: password
  s3_region: us-east-1
  flush_interval: 10s
  flush_rows: 1000

state:
  coordinator_schema: _pg2iceberg
"#;

    #[test]
    fn query_mode_is_rejected_with_a_pointer_to_logical() {
        let mut cfg: Config = serde_yaml::from_str(SAMPLE).unwrap();
        cfg.source.mode = "query".into();
        let err = cfg.validate_mode().unwrap_err().to_string();
        assert!(
            err.contains("no longer supported") && err.contains("wal_level=logical"),
            "{err}"
        );
    }

    #[test]
    fn logical_or_unset_mode_is_accepted() {
        let mut cfg: Config = serde_yaml::from_str(SAMPLE).unwrap();
        for mode in ["", "logical"] {
            cfg.source.mode = mode.into();
            cfg.validate_mode().unwrap();
        }
    }

    /// A config replicating `tables` (discovered, no explicit columns)
    /// under `sink.namespace`.
    fn with_tables(tables: &[&str], sink_namespace: &str) -> Config {
        let mut cfg: Config = serde_yaml::from_str(SAMPLE).unwrap();
        cfg.tables = tables
            .iter()
            .map(|t| serde_yaml::from_str(&format!("name: {t}")).unwrap())
            .collect();
        cfg.sink.namespace = sink_namespace.into();
        cfg
    }

    #[test]
    fn same_named_tables_under_one_sink_namespace_are_refused() {
        let err = with_tables(&["public.orders", "sales.orders"], "analytics")
            .validate_tables()
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("public.orders and sales.orders")
                && err.contains("Iceberg table analytics.orders"),
            "{err}"
        );
    }

    #[test]
    fn same_named_tables_in_their_own_namespaces_are_accepted() {
        with_tables(&["public.orders", "sales.orders"], "")
            .validate_tables()
            .unwrap();
        with_tables(&["public.orders", "sales.customers"], "analytics")
            .validate_tables()
            .unwrap();
    }

    #[test]
    fn a_table_listed_twice_is_refused() {
        let err = with_tables(&["public.orders", "public.orders"], "")
            .validate_tables()
            .unwrap_err()
            .to_string();
        assert!(err.contains("listed twice"), "{err}");
    }

    #[test]
    fn explicit_columns_keep_their_pg_schema() {
        // SAMPLE's `public.orders` declares its columns.
        let cfg: Config = serde_yaml::from_str(SAMPLE).unwrap();
        assert_eq!(
            cfg.tables[0].iceberg_ident("analytics").unwrap(),
            TableIdent {
                namespace: Namespace(vec!["public".into()]),
                name: "orders".into(),
            }
        );
    }

    #[test]
    fn leftover_query_mode_keys_still_parse() {
        // Configs written for the removed query mode may still carry
        // its keys after switching to logical; they're ignored.
        let yaml = SAMPLE
            .replace(
                "    primary_key: [id]\n",
                "    primary_key: [id]\n    watermark_column: updated_at\n",
            )
            .replace(
                "  logical:\n",
                "  query:\n    poll_interval: 30s\n  logical:\n",
            );
        let cfg: Config = serde_yaml::from_str(&yaml).unwrap();
        cfg.validate_mode().unwrap();
        assert_eq!(cfg.tables[0].name, "public.orders");
    }

    #[test]
    fn parses_go_shaped_yaml() {
        let cfg: Config = serde_yaml::from_str(SAMPLE).unwrap();
        assert_eq!(cfg.tables.len(), 1);
        assert_eq!(cfg.tables[0].name, "public.orders");
        assert_eq!(cfg.tables[0].primary_key, vec!["id".to_string()]);
        assert_eq!(cfg.tables[0].columns.len(), 2);
        assert_eq!(cfg.source.mode, "logical");
        assert_eq!(cfg.source.postgres.host, "localhost");
        assert_eq!(cfg.source.postgres.port, 5432);
        assert_eq!(cfg.source.postgres.tls_label(), "webpki");
        assert_eq!(cfg.source.logical.slot_name, "p2i_slot");
        assert_eq!(cfg.sink.catalog_uri, "http://localhost:8181");
        assert_eq!(cfg.sink.credential_mode, "static");
        assert_eq!(cfg.sink.s3_endpoint, "http://localhost:9000");
        assert_eq!(cfg.sink.namespace, "public");
        assert_eq!(cfg.state.coordinator_schema, "_pg2iceberg");
    }

    #[test]
    fn dsn_renders_all_fields() {
        let cfg: Config = serde_yaml::from_str(SAMPLE).unwrap();
        let dsn = cfg.source.postgres.dsn();
        assert!(dsn.contains("host=localhost"));
        assert!(dsn.contains("dbname=src"));
        assert!(dsn.contains("user=postgres"));
        assert!(dsn.contains("sslmode=require"));
    }

    #[test]
    fn dsn_defaults_sslmode_disable_when_unset() {
        let cfg: Config = serde_yaml::from_str(
            r#"
tables: []
source:
  postgres:
    host: localhost
    database: x
    user: y
sink:
  catalog_uri: http://localhost
  namespace: x
"#,
        )
        .unwrap();
        let dsn = cfg.source.postgres.dsn();
        assert!(dsn.contains("sslmode=disable"));
        assert_eq!(cfg.source.postgres.tls_label(), "disable");
    }

    #[test]
    fn parse_qualified_name_splits_on_dot() {
        let (ns, n) = parse_qualified_name("public.orders").unwrap();
        assert_eq!(ns, "public");
        assert_eq!(n, "orders");
    }

    #[test]
    fn parse_qualified_name_rejects_unqualified() {
        assert!(parse_qualified_name("orders").is_err());
        assert!(parse_qualified_name(".orders").is_err());
        assert!(parse_qualified_name("public.").is_err());
    }

    #[test]
    fn rest_catalog_props_includes_bearer_token_when_configured() {
        let cfg: Config = serde_yaml::from_str(SAMPLE).unwrap();
        let props = cfg.rest_catalog_props();
        assert_eq!(
            props.get("uri").map(String::as_str),
            Some("http://localhost:8181"),
        );
        assert_eq!(props.get("token").map(String::as_str), Some("REDACTED"));
        assert_eq!(
            props.get("warehouse").map(String::as_str),
            Some("s3://warehouse/"),
        );
    }

    #[test]
    fn rest_catalog_props_sets_access_delegation_header_in_vended_mode() {
        // Polaris/Tabular/Snowflake only return per-table vended creds
        // when the request carries this header. Without it, the
        // catalog returns metadata-only and the vended router would
        // see empty `s3.access-key-id` props.
        let cfg: Config = serde_yaml::from_str(
            r#"
tables: []
source:
  postgres:
    host: h
    database: d
    user: u
sink:
  catalog_uri: http://polaris
  namespace: ns
  warehouse: s3://wh/
  credential_mode: vended
"#,
        )
        .unwrap();
        let props = cfg.rest_catalog_props();
        assert_eq!(
            props
                .get("header.x-iceberg-access-delegation")
                .map(String::as_str),
            Some("vended-credentials"),
            "vended mode must set the access-delegation header"
        );
    }

    /// S3-compatible storage takes path-style requests, whatever the
    /// credentials: addressed virtual-hosted, as `<bucket>.<host>`, MinIO
    /// answers 404.
    #[test]
    fn rest_catalog_props_use_path_style_with_a_custom_endpoint_in_every_mode() {
        for mode in ["static", "iam", "vended"] {
            let mut cfg: Config = serde_yaml::from_str(SAMPLE).unwrap();
            cfg.sink.credential_mode = mode.into();
            let props = cfg.rest_catalog_props();
            assert_eq!(
                props.get("s3.path-style-access").map(String::as_str),
                Some("true"),
                "{mode}"
            );
        }
    }

    #[test]
    fn rest_catalog_props_omits_access_delegation_header_in_static_mode() {
        let cfg: Config = serde_yaml::from_str(SAMPLE).unwrap();
        // SAMPLE uses credential_mode=static.
        let props = cfg.rest_catalog_props();
        assert!(
            !props.contains_key("header.x-iceberg-access-delegation"),
            "static mode must not request vended creds"
        );
    }

    #[test]
    fn defaults_kick_in_for_missing_fields() {
        let cfg: Config = serde_yaml::from_str(
            r#"
tables: []
source:
  postgres:
    host: h
    database: d
    user: u
sink:
  catalog_uri: x
  namespace: ns
"#,
        )
        .unwrap();
        assert_eq!(cfg.source.mode, "logical");
        assert_eq!(cfg.source.postgres.port, 5432);
        assert_eq!(cfg.source.logical.slot_name, "pg2iceberg_slot");
        assert_eq!(cfg.source.logical.publication_name, "pg2iceberg_pub");
        // No warehouse: the catalog vends credentials.
        assert_eq!(cfg.sink.resolved_credential_mode(), "vended");
        assert_eq!(cfg.sink.resolved_catalog_auth(), "none");
        assert_eq!(cfg.sink.resolved_region(), "us-east-1");
        assert_eq!(cfg.sink.flush_rows, 10_000);
        assert_eq!(cfg.sink.materializer_batch_rows, 50_000);
        assert_eq!(cfg.state.coordinator_schema, "_pg2iceberg");
        assert_eq!(cfg.state.group, "default");
    }

    const INTERVALS: &str = r#"
tables: []
source:
  postgres:
    host: h
    database: d
    user: u
sink:
  catalog_uri: x
  namespace: ns
  flush_interval: 2s
  materializer_interval: 1m
"#;

    #[test]
    fn run_stages_and_commits_on_the_configured_intervals() {
        let cfg = Config::parse(INTERVALS, &vars(&[])).unwrap();
        let schedule = cfg.sink.schedule().unwrap();
        assert_eq!(schedule.flush, std::time::Duration::from_secs(2));
        assert_eq!(schedule.materialize, std::time::Duration::from_secs(60));
    }

    #[test]
    fn intervals_left_out_are_ten_seconds() {
        let yaml = INTERVALS
            .replace("  flush_interval: 2s\n", "")
            .replace("  materializer_interval: 1m\n", "");
        let cfg = Config::parse(&yaml, &vars(&[])).unwrap();
        let schedule = cfg.sink.schedule().unwrap();
        assert_eq!(schedule.flush, std::time::Duration::from_secs(10));
        assert_eq!(schedule.materialize, std::time::Duration::from_secs(10));
    }

    #[test]
    fn a_bad_interval_fails_the_config_load() {
        for (line, bad) in [
            ("flush_interval: 2s", "flush_interval: soon"),
            ("materializer_interval: 1m", "materializer_interval: 0s"),
        ] {
            let env = vars(&[]);
            let err = Config::parse(&INTERVALS.replace(line, bad), &env)
                .and_then(|cfg| cfg.finish(&env))
                .unwrap_err();
            let field = line.split(':').next().unwrap();
            assert!(format!("{err:#}").contains(field), "{bad}: {err:#}");
        }
    }

    fn vars(pairs: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> {
        let map: BTreeMap<String, String> = pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        move |name: &str| map.get(name).cloned()
    }

    #[test]
    fn the_environment_alone_is_a_config() {
        let env = vars(&[
            ("POSTGRES_URL", "postgres://u:p@db:5432/app?sslmode=require"),
            ("PG2ICEBERG_TABLES", "public.orders, sales.*"),
            ("PG2ICEBERG_SLOT", "my_slot"),
            ("ICEBERG_CATALOG_URL", "https://catalog.example.com"),
            ("ICEBERG_CATALOG_TOKEN", "t0ken"),
            ("ICEBERG_WAREHOUSE", "s3://lake/"),
            ("ICEBERG_NAMESPACE", "analytics"),
            ("AWS_REGION", "eu-west-1"),
        ]);
        let cfg = Config::from_env(&env).unwrap();
        cfg.require_source().unwrap();
        cfg.require_catalog().unwrap();
        assert_eq!(
            cfg.source.postgres.dsn(),
            "postgres://u:p@db:5432/app?sslmode=require"
        );
        assert_eq!(cfg.source.postgres.tls_label(), "webpki");
        assert_eq!(
            cfg.tables
                .iter()
                .map(|t| t.name.as_str())
                .collect::<Vec<_>>(),
            ["public.orders", "sales.*"]
        );
        assert!(cfg.tables[1].is_pattern());
        assert_eq!(cfg.source.logical.slot_name, "my_slot");
        assert_eq!(cfg.source.logical.publication_name, "pg2iceberg_pub");
        assert_eq!(cfg.sink.namespace, "analytics");
        assert_eq!(cfg.sink.resolved_catalog_auth(), "bearer");
        // No keys given: the AWS default credential chain.
        assert_eq!(cfg.sink.resolved_credential_mode(), "iam");
        assert_eq!(cfg.sink.resolved_region(), "eu-west-1");
        let props = cfg.rest_catalog_props();
        assert_eq!(props.get("token").map(String::as_str), Some("t0ken"));
        assert_eq!(
            props.get("s3.region").map(String::as_str),
            Some("eu-west-1")
        );
    }

    #[test]
    fn the_environment_overrides_the_file_but_aws_variables_only_fill_in() {
        let yaml = SAMPLE.to_string();
        let env = vars(&[
            ("ICEBERG_CATALOG_URL", "http://other:8181"),
            ("AWS_REGION", "eu-west-1"),
            ("AWS_ENDPOINT_URL_S3", "http://elsewhere:9000"),
        ]);
        let mut cfg = Config::parse(&yaml, &env).unwrap();
        cfg.apply_env(&env).unwrap();
        assert_eq!(cfg.sink.catalog_uri, "http://other:8181");
        assert_eq!(cfg.sink.s3_region, "us-east-1");
        assert_eq!(cfg.sink.s3_endpoint, "http://localhost:9000");
    }

    #[test]
    fn missing_settings_name_the_variables_to_set() {
        let cfg = Config::from_env(&vars(&[])).unwrap();
        let source = cfg.require_source().unwrap_err().to_string();
        assert!(source.contains("POSTGRES_URL"), "{source}");
        let catalog = cfg.require_catalog().unwrap_err().to_string();
        assert!(catalog.contains("ICEBERG_CATALOG_URL"), "{catalog}");
    }

    #[test]
    fn strings_in_the_file_read_variables() {
        let env = vars(&[("TOKEN", "t0ken"), ("HOST", "db")]);
        let yaml = r#"
# A comment naming ${UNSET} is left alone.
source:
  postgres_url: postgres://u@${HOST}/app
sink:
  catalog_uri: http://catalog
  catalog_token: ${TOKEN}
  catalog_props:
    "header.x-price": "$5 and $${literal}"
"#;
        let cfg = Config::parse(yaml, &env).unwrap();
        assert_eq!(cfg.source.postgres_url, "postgres://u@db/app");
        assert_eq!(cfg.sink.catalog_token, "t0ken");
        assert_eq!(
            cfg.sink
                .catalog_props
                .get("header.x-price")
                .map(String::as_str),
            Some("$5 and ${literal}")
        );
        let err = Config::parse("sink:\n  catalog_token: ${NOPE}\n", &env)
            .unwrap_err()
            .to_string();
        assert!(err.contains("${NOPE}"), "{err}");
        // An empty file is an empty config.
        assert!(Config::parse("", &env).unwrap().tables.is_empty());
    }

    #[test]
    fn a_connection_strings_sslmode_decides_tls() {
        let pg = |url: &str| PostgresConfig {
            url: url.into(),
            ..PostgresConfig::default()
        };
        assert_eq!(pg("postgres://u@db/app").tls_label(), "disable");
        assert_eq!(
            pg("postgres://u@db/app?sslmode=disable").tls_label(),
            "disable"
        );
        assert_eq!(
            pg("postgres://u@db/app?application_name=x&sslmode=verify-full").tls_label(),
            "webpki"
        );
        assert_eq!(
            pg("host=db dbname=app sslmode=require").tls_label(),
            "webpki"
        );
        assert_eq!(pg("host=db dbname=app").tls_label(), "disable");
    }

    #[test]
    fn the_credential_mode_follows_from_what_is_given() {
        let sink = |keys: bool, warehouse: &str, explicit: &str| SinkConfig {
            s3_access_key: if keys { "k".into() } else { String::new() },
            warehouse: warehouse.into(),
            credential_mode: explicit.into(),
            ..SinkConfig::default()
        };
        assert_eq!(
            sink(true, "s3://w/", "").resolved_credential_mode(),
            "static"
        );
        assert_eq!(sink(false, "s3://w/", "").resolved_credential_mode(), "iam");
        assert_eq!(sink(false, "", "").resolved_credential_mode(), "vended");
        assert_eq!(
            sink(false, "my_catalog", "").resolved_credential_mode(),
            "vended"
        );
        assert_eq!(sink(true, "", "iam").resolved_credential_mode(), "iam");
    }

    #[test]
    fn an_aws_catalog_is_signed_for_its_service_and_region() {
        let sink = SinkConfig {
            catalog_uri: "https://s3tables.ap-southeast-1.amazonaws.com/iceberg".into(),
            warehouse: "arn:aws:s3tables:ap-southeast-1:123456789012:bucket/b".into(),
            ..SinkConfig::default()
        };
        assert_eq!(sink.resolved_catalog_auth(), "sigv4");
        assert_eq!(sink.resolved_region(), "ap-southeast-1");
        let cfg = Config {
            sink,
            ..Config::default()
        };
        let props = cfg.rest_catalog_props();
        assert_eq!(
            props.get("rest.sigv4-enabled").map(String::as_str),
            Some("true")
        );
        assert_eq!(
            props.get("rest.signing-region").map(String::as_str),
            Some("ap-southeast-1")
        );
        assert_eq!(
            props.get("rest.signing-name").map(String::as_str),
            Some("s3tables")
        );
        assert_eq!(
            aws_endpoint("https://glue.us-west-2.amazonaws.com/iceberg"),
            Some(("glue".to_string(), "us-west-2".to_string()))
        );
        assert_eq!(aws_endpoint("http://localhost:8181"), None);
    }

    #[test]
    fn client_credentials_mean_oauth2() {
        let sink = SinkConfig {
            catalog_uri: "https://polaris".into(),
            catalog_client_id: "id".into(),
            catalog_client_secret: "secret".into(),
            ..SinkConfig::default()
        };
        assert_eq!(sink.resolved_catalog_auth(), "oauth2");
    }

    #[test]
    fn static_keys_on_aws_itself_use_virtual_hosted_requests() {
        let cfg = Config {
            sink: SinkConfig {
                catalog_uri: "http://catalog".into(),
                warehouse: "s3://w/".into(),
                s3_access_key: "k".into(),
                s3_secret_key: "s".into(),
                ..SinkConfig::default()
            },
            ..Config::default()
        };
        let props = cfg.rest_catalog_props();
        assert_eq!(props.get("s3.access-key-id").map(String::as_str), Some("k"));
        assert!(!props.contains_key("s3.path-style-access"));
        assert!(!props.contains_key("s3.endpoint"));
    }

    #[test]
    fn the_example_configs_load() {
        let env = vars(&[("CATALOG_TOKEN", "t"), ("S3_SECRET_KEY", "s")]);
        let example = Config::parse(include_str!("../../../config.example.yaml"), &env).unwrap();
        example.validate_tables().unwrap();
        assert_eq!(example.sink.catalog_token, "t");
        assert_eq!(example.sink.s3_secret_key, "s");
        let single =
            Config::parse(include_str!("../../../example/single/config.yaml"), &env).unwrap();
        single.validate_tables().unwrap();
        assert_eq!(single.tables.len(), 5);
    }

    #[test]
    fn a_schema_pattern_cant_carry_one_tables_settings() {
        let mut cfg = Config {
            tables: vec![TableConfig {
                primary_key: vec!["id".into()],
                ..TableConfig::named("public.*")
            }],
            ..Config::default()
        };
        let err = cfg.validate_tables().unwrap_err().to_string();
        assert!(err.contains("public.*"), "{err}");
        cfg.tables = vec![TableConfig::named("public.*")];
        cfg.validate_tables().unwrap();
    }
}
