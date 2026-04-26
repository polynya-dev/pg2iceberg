//! Source-side schema discovery: query `pg_attribute` /
//! `information_schema.columns` / `pg_index` to learn a table's
//! columns and primary key without needing the operator to declare
//! them in YAML.
//!
//! Mirrors `postgres/schema.go::DiscoverSchema` from the Go reference.
//! Two `simple_query` calls per table:
//!
//! 1. **Column metadata** from `information_schema.columns` —
//!    name, `udt_name`, nullability, numeric precision/scale.
//! 2. **Primary key columns** from `pg_index` joined with
//!    `pg_attribute`, ordered by `indkey` position so PK column
//!    order matches the index definition.
//!
//! Both run in replication mode via `simple_query`. We don't use
//! the extended (parameterized) protocol because replication-mode
//! connections don't support it; instead we quote-literal the
//! schema + table names per Postgres conventions.

use crate::{PgError, Result};
use pg2iceberg_core::{ColumnSchema, IcebergType, Namespace, PgType, TableIdent, TableSchema};
use tokio_postgres::{Client, SimpleQueryMessage};

/// Discover a table's full schema (columns + PK + Iceberg type
/// mapping) by querying the source PG.
///
/// `schema` and `table` must be the unquoted names; we wrap them in
/// quoted-literal form internally.
pub(crate) async fn discover_schema(
    client: &Client,
    schema: &str,
    table: &str,
) -> Result<TableSchema> {
    let cols = fetch_columns(client, schema, table).await?;
    if cols.is_empty() {
        return Err(PgError::Other(format!(
            "table {schema}.{table} not found or has no columns"
        )));
    }
    let pk_cols = fetch_primary_key(client, schema, table).await?;
    let pk_set: std::collections::BTreeSet<&str> = pk_cols.iter().map(String::as_str).collect();

    let mut columns: Vec<ColumnSchema> = Vec::with_capacity(cols.len());
    for (idx, raw) in cols.into_iter().enumerate() {
        let pg_type = map_udt_to_pg_type(&raw.udt_name, raw.precision, raw.scale)?;
        let mapped = pg2iceberg_core::map_pg_to_iceberg(pg_type)
            .map_err(|e| PgError::Other(format!("type-map column {}: {e}", raw.name)))?;
        // PG columns from `information_schema.columns` are nullable
        // unless `is_nullable = 'NO'`. PK columns are always
        // non-nullable in our materialized output regardless of the
        // PG side, since equality-delete files use them as identity.
        let is_pk = pk_set.contains(raw.name.as_str());
        columns.push(ColumnSchema {
            name: raw.name,
            field_id: (idx + 1) as i32,
            ty: mapped.iceberg,
            nullable: raw.is_nullable && !is_pk,
            is_primary_key: is_pk,
        });
    }
    Ok(TableSchema {
        ident: TableIdent {
            namespace: Namespace(vec![schema.to_string()]),
            name: table.to_string(),
        },
        columns,
        // PG-side discovery doesn't carry partition info — that's
        // declared in YAML (`tables[].iceberg.partition`) and merged
        // by the binary before handing the schema to the catalog.
        partition_spec: Vec::new(),
    })
}

#[derive(Debug)]
struct RawColumn {
    name: String,
    udt_name: String,
    is_nullable: bool,
    precision: Option<u32>,
    scale: Option<u32>,
}

async fn fetch_columns(client: &Client, schema: &str, table: &str) -> Result<Vec<RawColumn>> {
    let q = format!(
        "SELECT column_name, udt_name, is_nullable, \
         COALESCE(numeric_precision::text, ''), \
         COALESCE(numeric_scale::text, '') \
         FROM information_schema.columns \
         WHERE table_schema = {schema} AND table_name = {table} \
         ORDER BY ordinal_position",
        schema = quote_lit(schema),
        table = quote_lit(table),
    );
    let rows = client
        .simple_query(&q)
        .await
        .map_err(|e| PgError::Protocol(format!("discover columns: {e}")))?;
    let mut out = Vec::new();
    for msg in rows {
        if let SimpleQueryMessage::Row(row) = msg {
            let name = row
                .try_get(0)
                .map_err(|e| PgError::Protocol(e.to_string()))?
                .ok_or_else(|| PgError::Protocol("column_name was NULL".into()))?
                .to_string();
            let udt_name = row
                .try_get(1)
                .map_err(|e| PgError::Protocol(e.to_string()))?
                .ok_or_else(|| PgError::Protocol("udt_name was NULL".into()))?
                .to_string();
            let is_nullable = row
                .try_get(2)
                .map_err(|e| PgError::Protocol(e.to_string()))?
                .map(|s| s.eq_ignore_ascii_case("YES"))
                .unwrap_or(false);
            let precision = row
                .try_get(3)
                .map_err(|e| PgError::Protocol(e.to_string()))?
                .and_then(|s| if s.is_empty() { None } else { s.parse().ok() });
            let scale = row
                .try_get(4)
                .map_err(|e| PgError::Protocol(e.to_string()))?
                .and_then(|s| if s.is_empty() { None } else { s.parse().ok() });
            out.push(RawColumn {
                name,
                udt_name,
                is_nullable,
                precision,
                scale,
            });
        }
    }
    Ok(out)
}

async fn fetch_primary_key(client: &Client, schema: &str, table: &str) -> Result<Vec<String>> {
    // `regclass` resolves the schema-qualified name; we have to
    // build the literal from a single quoted identifier so PG's
    // parser accepts both quoted-schema and quoted-name forms.
    let q = format!(
        "SELECT a.attname \
         FROM pg_index i \
         JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey) \
         WHERE i.indrelid = {qualified}::regclass AND i.indisprimary \
         ORDER BY array_position(i.indkey, a.attnum)",
        qualified = quote_lit(&format!("{}.{}", quote_ident(schema), quote_ident(table))),
    );
    let rows = client
        .simple_query(&q)
        .await
        .map_err(|e| PgError::Protocol(format!("discover primary key: {e}")))?;
    let mut out = Vec::new();
    for msg in rows {
        if let SimpleQueryMessage::Row(row) = msg {
            let v = row
                .try_get(0)
                .map_err(|e| PgError::Protocol(e.to_string()))?
                .ok_or_else(|| PgError::Protocol("attname was NULL".into()))?
                .to_string();
            out.push(v);
        }
    }
    Ok(out)
}

/// Map a Postgres `udt_name` (the canonical type name from
/// `information_schema.columns.udt_name`) to our [`PgType`].
/// Mirrors `postgres/schema.go::ParseType` from the Go reference.
fn map_udt_to_pg_type(
    udt_name: &str,
    precision: Option<u32>,
    scale: Option<u32>,
) -> Result<PgType> {
    Ok(match udt_name.to_ascii_lowercase().as_str() {
        "bool" => PgType::Bool,
        "int2" => PgType::Int2,
        "int4" => PgType::Int4,
        "int8" => PgType::Int8,
        "float4" => PgType::Float4,
        "float8" => PgType::Float8,
        "numeric" => PgType::Numeric {
            // information_schema reports precision/scale as `0` (not
            // null) when undeclared; the COALESCE in the query
            // returns the empty string in that case, which becomes
            // None here. Map None → unconstrained numeric.
            precision: precision.and_then(|p| u8::try_from(p).ok()),
            scale: scale.and_then(|s| u8::try_from(s).ok()),
        },
        // Single character or fixed-length char — collapse to text.
        "text" | "varchar" | "bpchar" | "name" | "char" => PgType::Text,
        "bytea" => PgType::Bytea,
        "date" => PgType::Date,
        "time" => PgType::Time,
        "timetz" => PgType::TimeTz,
        "timestamp" => PgType::Timestamp,
        "timestamptz" => PgType::TimestampTz,
        "uuid" => PgType::Uuid,
        "json" => PgType::Json,
        "jsonb" => PgType::Jsonb,
        "oid" => PgType::Oid,
        // Forward-compat: unknown types (network, range, geometric,
        // arrays, custom enums, ...) collapse to Text. The Go
        // reference does the same. If a column we care about ends up
        // here in production, surface it as a follow-on rather than
        // failing the whole startup.
        _ => PgType::Text,
    })
}

/// Postgres-style identifier quoting: wrap in double quotes; embedded
/// double quotes are doubled. Used to build a `regclass`-friendly
/// literal of the qualified table name.
fn quote_ident(name: &str) -> String {
    let escaped = name.replace('"', "\"\"");
    format!("\"{escaped}\"")
}

/// Postgres-style literal quoting: wrap in single quotes; embedded
/// single quotes are doubled.
fn quote_lit(value: &str) -> String {
    let escaped = value.replace('\'', "''");
    format!("'{escaped}'")
}

/// Suppress unused-warning when `IcebergType` is exported in the
/// crate root but nothing in this module references it directly.
#[allow(dead_code)]
fn _types_keep_alive(_: IcebergType) {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maps_well_known_udt_names() {
        assert_eq!(
            map_udt_to_pg_type("int4", None, None).unwrap(),
            PgType::Int4
        );
        assert_eq!(
            map_udt_to_pg_type("INT4", None, None).unwrap(),
            PgType::Int4
        );
        assert_eq!(
            map_udt_to_pg_type("text", None, None).unwrap(),
            PgType::Text
        );
        assert_eq!(
            map_udt_to_pg_type("varchar", None, None).unwrap(),
            PgType::Text
        );
        assert_eq!(
            map_udt_to_pg_type("bpchar", None, None).unwrap(),
            PgType::Text
        );
        assert_eq!(
            map_udt_to_pg_type("name", None, None).unwrap(),
            PgType::Text
        );
        assert_eq!(
            map_udt_to_pg_type("timestamptz", None, None).unwrap(),
            PgType::TimestampTz
        );
        assert_eq!(
            map_udt_to_pg_type("uuid", None, None).unwrap(),
            PgType::Uuid
        );
        assert_eq!(
            map_udt_to_pg_type("jsonb", None, None).unwrap(),
            PgType::Jsonb
        );
    }

    #[test]
    fn numeric_carries_precision_and_scale() {
        match map_udt_to_pg_type("numeric", Some(10), Some(2)).unwrap() {
            PgType::Numeric { precision, scale } => {
                assert_eq!(precision, Some(10));
                assert_eq!(scale, Some(2));
            }
            _ => panic!("expected Numeric"),
        }
    }

    #[test]
    fn unknown_udt_falls_back_to_text() {
        assert_eq!(
            map_udt_to_pg_type("polygon", None, None).unwrap(),
            PgType::Text
        );
        assert_eq!(
            map_udt_to_pg_type("custom_enum", None, None).unwrap(),
            PgType::Text
        );
    }

    #[test]
    fn quote_ident_doubles_quotes() {
        assert_eq!(quote_ident("foo"), "\"foo\"");
        assert_eq!(quote_ident("a\"b"), "\"a\"\"b\"");
    }

    #[test]
    fn quote_lit_doubles_quotes() {
        assert_eq!(quote_lit("foo"), "'foo'");
        assert_eq!(quote_lit("a'b"), "'a''b'");
    }
}
