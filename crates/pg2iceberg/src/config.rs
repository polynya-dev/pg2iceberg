//! Binary configuration. Mirrors the Go reference's
//! `config/config.go` shape so operators can reuse their existing
//! `pg2iceberg.yaml` without translation.
//!
//! Sections: `tables` (list), `source.{postgres, logical, query}`,
//! `sink` (catalog + storage + credential mode + flush knobs),
//! `state` (coordinator location).
//!
//! Some fields are intentionally not yet consumed by the Rust port
//! (query-mode settings, materializer cycle knobs, control-plane
//! metadata, etc.). They're carried in the schema so existing Go
//! configs deserialize cleanly; we wire them as the corresponding
//! features land. Hence the crate-level `dead_code` allow on the
//! config structs — *fields*, not types.

#![allow(dead_code)]

use anyhow::{Context, Result};
use pg2iceberg_core::{ColumnSchema, IcebergType, Namespace, PgType, TableIdent, TableSchema};
use serde::Deserialize;
use std::collections::BTreeMap;
use std::path::Path;

#[derive(Debug, Clone, Deserialize, Default)]
pub struct Config {
    #[serde(default)]
    pub tables: Vec<TableConfig>,
    pub source: SourceConfig,
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
    /// Fully-qualified table name `"schema.name"`. Mirrors Go.
    pub name: String,
    #[serde(default)]
    pub skip_snapshot: bool,
    /// Operator-supplied PK columns. When non-empty, overrides whatever
    /// schema discovery found in the source `pg_index`.
    #[serde(default)]
    pub primary_key: Vec<String>,
    #[serde(default)]
    pub watermark_column: String,
    /// Optional column declarations. When provided, these override
    /// schema discovery; useful for tables where the source columns
    /// don't match what you want to materialize, or for testing.
    #[serde(default, rename = "columns")]
    pub columns: Vec<ColumnConfig>,
    /// Iceberg-specific per-table settings. Currently just partition.
    #[serde(default)]
    pub iceberg: IcebergTableConfig,
}

/// Iceberg per-table options. Mirrors Go's `IcebergTableConfig` —
/// just `partition` for now; sort orders and other knobs are
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
    /// `"logical"` (default) or `"query"`.
    #[serde(default = "default_mode")]
    pub mode: String,
    pub postgres: PostgresConfig,
    #[serde(default)]
    pub logical: LogicalConfig,
    #[serde(default)]
    pub query: QueryConfig,
}

impl Default for SourceConfig {
    fn default() -> Self {
        Self {
            mode: default_mode(),
            postgres: PostgresConfig::default(),
            logical: LogicalConfig::default(),
            query: QueryConfig::default(),
        }
    }
}

fn default_mode() -> String {
    "logical".into()
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct PostgresConfig {
    pub host: String,
    #[serde(default = "default_pg_port")]
    pub port: u16,
    pub database: String,
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
    /// Render as a libpq-style `key=value` connection string.
    pub fn dsn(&self) -> String {
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
    pub fn tls_label(&self) -> &str {
        match self.sslmode.as_str() {
            "" | "disable" | "off" | "false" => "disable",
            _ => "webpki",
        }
    }
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

#[derive(Debug, Clone, Deserialize, Default)]
pub struct QueryConfig {
    #[serde(default)]
    pub poll_interval: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct SinkConfig {
    pub catalog_uri: String,
    /// `""` / `"none"` / `"sigv4"` / `"bearer"` / `"oauth2"`.
    #[serde(default)]
    pub catalog_auth: String,
    #[serde(default)]
    pub catalog_token: String,
    #[serde(default)]
    pub catalog_client_id: String,
    #[serde(default)]
    pub catalog_client_secret: String,

    /// `"static"` (default) — explicit S3 keys below.
    /// `"vended"` — temporary credentials from the catalog's
    /// `LoadTable` response. NOT YET WIRED in the Rust port; will
    /// error at startup. Plan §Phase 7.
    /// `"iam"` — instance profile / AWS SSO / env-var chain.
    #[serde(default = "default_credential_mode")]
    pub credential_mode: String,

    #[serde(default)]
    pub warehouse: String,
    pub namespace: String,
    #[serde(default)]
    pub s3_endpoint: String,
    #[serde(default)]
    pub s3_access_key: String,
    #[serde(default)]
    pub s3_secret_key: String,
    #[serde(default = "default_region")]
    pub s3_region: String,

    #[serde(default)]
    pub flush_interval: String,
    #[serde(default = "default_flush_rows")]
    pub flush_rows: usize,
    /// Materializer cycle interval. Matches Go's `materializer_interval`.
    /// Empty = use the lifecycle's default (10s). Only consulted by the
    /// `materializer-only` subcommand; the integrated `run` mode uses
    /// `Schedule::materialize` from the runner module.
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
    /// cleanup races them. Default `30m` matches Go.
    #[serde(default = "default_maintenance_grace")]
    pub maintenance_grace: String,

    /// Blob-store prefix where the materializer writes data files.
    /// Orphan cleanup operates only under this prefix. Should match
    /// the path scheme `CounterMaterializerNamer` produces in
    /// `run.rs`. Default `materialized/` mirrors what the binary uses.
    #[serde(default = "default_materialized_prefix")]
    pub materialized_prefix: String,

    /// Free-form REST-catalog props passthrough. Not in the Go YAML
    /// shape but useful for vendor-specific settings (Polaris OAuth2
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
            credential_mode: default_credential_mode(),
            warehouse: String::new(),
            namespace: String::new(),
            s3_endpoint: String::new(),
            s3_access_key: String::new(),
            s3_secret_key: String::new(),
            s3_region: default_region(),
            flush_interval: String::new(),
            flush_rows: default_flush_rows(),
            materializer_interval: String::new(),
            compaction_data_files: default_compaction_data_files(),
            compaction_delete_files: default_compaction_delete_files(),
            target_file_size: default_target_file_size(),
            maintenance_retention: String::new(),
            maintenance_grace: default_maintenance_grace(),
            materialized_prefix: default_materialized_prefix(),
            catalog_props: BTreeMap::new(),
            meta_namespace: String::new(),
        }
    }
}

impl SinkConfig {
    /// Translate the Go-shaped sink fields into the iceberg crate's
    /// `CompactionConfig` shape. Used on every materializer cycle.
    pub fn compaction_config(&self) -> pg2iceberg_iceberg::CompactionConfig {
        pg2iceberg_iceberg::CompactionConfig {
            data_file_threshold: self.compaction_data_files,
            delete_file_threshold: self.compaction_delete_files,
            target_size_bytes: self.target_file_size,
        }
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

fn default_credential_mode() -> String {
    "static".into()
}

fn default_region() -> String {
    "us-east-1".into()
}

fn default_flush_rows() -> usize {
    1000
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

fn default_materialized_prefix() -> String {
    "materialized/".into()
}

#[derive(Debug, Clone, Deserialize)]
pub struct StateConfig {
    /// File-backed state path (sim-only / dev). Not honored in the
    /// Rust prod path — leave empty.
    #[serde(default)]
    pub path: String,
    /// Postgres URL for the coordinator. If absent, the source PG
    /// hosts the `_pg2iceberg.*` schema (matches Go).
    #[serde(default)]
    pub postgres_url: String,
    #[serde(default = "default_coord_schema")]
    pub coordinator_schema: String,
    /// Materialization group name (consumer / mat_cursor key).
    /// Not in the Go YAML — defaults to `"default"`.
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
    /// Read + parse a YAML file from disk.
    pub fn load_from<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path = path.as_ref();
        let raw = std::fs::read_to_string(path)
            .with_context(|| format!("read config from {}", path.display()))?;
        let cfg: Config = serde_yaml::from_str(&raw)
            .with_context(|| format!("parse config at {}", path.display()))?;
        Ok(cfg)
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
        match self.sink.catalog_auth.as_str() {
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
            _ => {}
        }
        // Vended-credentials mode requires the
        // `X-Iceberg-Access-Delegation: vended-credentials` header on
        // every catalog request. iceberg-rust's REST client already
        // forwards `header.<name>: <value>` props through to outgoing
        // requests, so we set it here when credential_mode=vended.
        // Operators can override via explicit `catalog_props` if they
        // need a different delegation type (e.g. `remote-signing`).
        if self.sink.credential_mode == "vended" {
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
        }
        if !self.sink.s3_region.is_empty() {
            props.insert("s3.region".into(), self.sink.s3_region.clone());
        }
        if self.sink.credential_mode == "static" {
            if !self.sink.s3_access_key.is_empty() {
                props.insert("s3.access-key-id".into(), self.sink.s3_access_key.clone());
            }
            if !self.sink.s3_secret_key.is_empty() {
                props.insert(
                    "s3.secret-access-key".into(),
                    self.sink.s3_secret_key.clone(),
                );
            }
            // Path-style access is the safe default for non-AWS S3
            // (MinIO, LocalStack). Mirrors what `build_s3_static`
            // does for the client-side blob store.
            props.insert("s3.path-style-access".into(), "true".into());
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
    /// `(schema, table)` parsed from the YAML `name`.
    pub fn qualified(&self) -> Result<(String, String)> {
        parse_qualified_name(&self.name)
    }

    /// `True` when the operator has explicitly declared columns in
    /// YAML. When `false`, the binary discovers schema from
    /// `information_schema.columns` + `pg_index` at startup.
    pub fn has_explicit_columns(&self) -> bool {
        !self.columns.is_empty()
    }

    /// Convert to our [`TableSchema`]. Splits `name` on `.` for the
    /// namespace + table — same form Go's reference uses.
    /// Errors if `columns:` is empty; callers should branch via
    /// [`Self::has_explicit_columns`] and use schema discovery
    /// instead in that case.
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
        })
    }
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
        assert_eq!(cfg.sink.credential_mode, "static");
        assert_eq!(cfg.sink.s3_region, "us-east-1");
        assert_eq!(cfg.sink.flush_rows, 1000);
        assert_eq!(cfg.state.coordinator_schema, "_pg2iceberg");
        assert_eq!(cfg.state.group, "default");
    }
}
