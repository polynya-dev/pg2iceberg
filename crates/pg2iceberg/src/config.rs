//! Binary configuration: TOML schema + loader.
//!
//! The TOML shape is intentionally flat. Each prod surface
//! (PG source, coordinator, Iceberg catalog, blob store) gets its
//! own section so an operator can swap any one without touching the
//! others. Tables are listed individually with their PK columns.

use anyhow::{Context, Result};
use pg2iceberg_core::{ColumnSchema, IcebergType, Namespace, PgType, TableIdent, TableSchema};
use serde::Deserialize;
use std::path::Path;

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    pub pg: PgConfig,
    pub coord: CoordConfig,
    pub iceberg: IcebergConfig,
    pub blob: BlobConfig,
    /// Tables to mirror. Order matters — registration happens in this
    /// order at startup.
    #[serde(rename = "table", default)]
    pub tables: Vec<TableConfig>,
    #[serde(default)]
    pub runtime: RuntimeConfig,
}

#[derive(Debug, Clone, Deserialize)]
pub struct PgConfig {
    /// libpq-style connection string for the source database
    /// (`host=... user=... dbname=...`). Replication mode is added
    /// automatically by `PgClientImpl::connect`.
    pub conn: String,
    /// Replication slot name. Created on first run; reused across
    /// restarts.
    pub slot: String,
    /// Publication name. Created on first run from the `tables` list.
    pub publication: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct CoordConfig {
    /// Connection string for the coord PG. Often the same database as
    /// the source; can be a separate one. Regular (non-replication)
    /// mode.
    pub conn: String,
    /// Schema name for the `_pg2iceberg.*` tables. Default
    /// `_pg2iceberg`.
    #[serde(default = "default_coord_schema")]
    pub schema: String,
    /// Materialization group name (used in `mat_cursor` /
    /// `consumer` tables). Default `default`.
    #[serde(default = "default_group")]
    pub group: String,
}

fn default_coord_schema() -> String {
    "_pg2iceberg".into()
}

fn default_group() -> String {
    "default".into()
}

/// Iceberg catalog choice. Today only `memory` (in-process
/// `MemoryCatalog`) is wired. REST/Glue/SQL/HMS are follow-ons.
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "kebab-case")]
pub enum IcebergConfig {
    Memory {
        /// Warehouse URI, e.g. `memory:///warehouse`.
        warehouse: String,
    },
}

/// Blob store choice. Today only `memory` (in-process
/// `object_store::memory::InMemory`). S3/GCS/Azure are follow-ons
/// with their own auth-config sections.
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "kebab-case")]
pub enum BlobConfig {
    Memory,
}

#[derive(Debug, Clone, Deserialize)]
pub struct TableConfig {
    /// Postgres schema name. `public` is the usual default.
    pub namespace: String,
    pub name: String,
    /// PK columns, in declaration order. Required (pg2iceberg
    /// refuses tables without a PK).
    pub pk: Vec<String>,
    /// Column declarations. The order here matches the column order
    /// in Iceberg; `field_id` is auto-assigned (1-based).
    #[serde(default, rename = "column")]
    pub columns: Vec<ColumnConfig>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ColumnConfig {
    pub name: String,
    /// Postgres type name (`int4`, `text`, etc.) — see
    /// `pg2iceberg_core::PgType`'s `ParseType` for the accepted names.
    pub pg_type: String,
    #[serde(default)]
    pub nullable: bool,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct RuntimeConfig {
    /// Pipeline `flush_threshold` (rows). Default 10_000.
    #[serde(default = "default_flush_threshold")]
    pub flush_threshold: usize,
    /// Materializer `cycle_limit` (snapshots per cycle). Default 64.
    #[serde(default = "default_cycle_limit")]
    pub cycle_limit: usize,
    /// Optional operator-supplied worker id. If unset, generated from
    /// `Uuid::new_v4()` at startup.
    pub worker_id: Option<String>,
}

fn default_flush_threshold() -> usize {
    10_000
}

fn default_cycle_limit() -> usize {
    64
}

impl Config {
    /// Read + parse a TOML file from disk.
    pub fn load_from<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path = path.as_ref();
        let raw = std::fs::read_to_string(path)
            .with_context(|| format!("read config from {}", path.display()))?;
        let cfg: Config =
            toml::from_str(&raw).with_context(|| format!("parse config at {}", path.display()))?;
        Ok(cfg)
    }
}

impl TableConfig {
    /// Convert to the workspace's [`TableSchema`] shape, assigning
    /// 1-based field ids in declaration order. Errors out on unknown
    /// PG type names — we surface those at config-load time rather
    /// than at materialization-runtime.
    pub fn to_table_schema(&self) -> Result<TableSchema> {
        let pk_set: std::collections::BTreeSet<&str> = self.pk.iter().map(String::as_str).collect();
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
        Ok(TableSchema {
            ident: TableIdent {
                namespace: Namespace(vec![self.namespace.clone()]),
                name: self.name.clone(),
            },
            columns,
        })
    }
}

/// Parse a Postgres type name (case-insensitive) into [`PgType`].
/// Mirrors the Go reference's `postgres.ParseType` table.
fn parse_pg_type(name: &str) -> Result<PgType> {
    let lower = name.to_ascii_lowercase();
    Ok(match lower.as_str() {
        "bool" | "boolean" => PgType::Bool,
        "int2" | "smallint" => PgType::Int2,
        "int4" | "integer" | "int" | "serial" => PgType::Int4,
        "int8" | "bigint" | "bigserial" => PgType::Int8,
        "float4" | "real" => PgType::Float4,
        "float8" | "double precision" | "double" => PgType::Float8,
        // For `numeric` without explicit (precision, scale) on the
        // config side we use the unconstrained form; the warning is
        // surfaced via `map_pg_to_iceberg`.
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

// Suppress unused-import warning when `IcebergType` is exported but
// nothing in this module references it directly. Keeping the re-export
// shape consistent with the rest of the workspace.
#[allow(dead_code)]
fn _types_keep_alive(_: IcebergType) {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_known_type_aliases() {
        assert_eq!(parse_pg_type("int4").unwrap(), PgType::Int4);
        assert_eq!(parse_pg_type("INTEGER").unwrap(), PgType::Int4);
        assert_eq!(parse_pg_type("text").unwrap(), PgType::Text);
        assert_eq!(parse_pg_type("VARCHAR").unwrap(), PgType::Text);
        assert!(matches!(
            parse_pg_type("numeric").unwrap(),
            PgType::Numeric { .. }
        ));
    }

    #[test]
    fn unknown_type_errors() {
        assert!(parse_pg_type("polygon").is_err());
    }

    #[test]
    fn table_to_schema_assigns_1_based_field_ids_and_marks_pk() {
        let t = TableConfig {
            namespace: "public".into(),
            name: "orders".into(),
            pk: vec!["id".into()],
            columns: vec![
                ColumnConfig {
                    name: "id".into(),
                    pg_type: "int4".into(),
                    nullable: false,
                },
                ColumnConfig {
                    name: "qty".into(),
                    pg_type: "int8".into(),
                    nullable: true,
                },
            ],
        };
        let s = t.to_table_schema().unwrap();
        assert_eq!(s.columns.len(), 2);
        assert_eq!(s.columns[0].name, "id");
        assert_eq!(s.columns[0].field_id, 1);
        assert!(s.columns[0].is_primary_key);
        assert!(!s.columns[0].nullable);
        assert_eq!(s.columns[1].name, "qty");
        assert_eq!(s.columns[1].field_id, 2);
        assert!(!s.columns[1].is_primary_key);
        assert!(s.columns[1].nullable);
    }

    #[test]
    fn parses_minimal_toml() {
        let toml = r#"
[pg]
conn = "host=localhost user=test dbname=src"
slot = "p2i_slot"
publication = "p2i_pub"

[coord]
conn = "host=localhost user=test dbname=src"

[iceberg]
type = "memory"
warehouse = "memory:///warehouse"

[blob]
type = "memory"

[[table]]
namespace = "public"
name = "orders"
pk = ["id"]
[[table.column]]
name = "id"
pg_type = "int4"
[[table.column]]
name = "qty"
pg_type = "int8"
nullable = true
"#;
        let cfg: Config = toml::from_str(toml).unwrap();
        assert_eq!(cfg.pg.slot, "p2i_slot");
        assert_eq!(cfg.coord.schema, "_pg2iceberg");
        assert_eq!(cfg.coord.group, "default");
        assert_eq!(cfg.tables.len(), 1);
        assert_eq!(cfg.tables[0].name, "orders");
        assert_eq!(cfg.tables[0].columns.len(), 2);
    }
}
