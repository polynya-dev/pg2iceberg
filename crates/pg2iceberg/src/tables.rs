//! Which tables to replicate, when the config doesn't name them all: no
//! tables at all means every table with a primary key, and `schema.*`
//! every such table in the schema.

use crate::config::{Config, TableConfig};
use anyhow::{Context, Result};
use pg2iceberg_coord::schema::CoordSchema;
use pg2iceberg_pg::prod::{PgClientImpl, SourceTable, TlsMode};
use std::collections::BTreeSet;
use std::fmt;

/// Why a table discovery found isn't replicated.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Skipped {
    NoPrimaryKey,
    Unreadable,
    /// A column Iceberg can't hold, say.
    Unsupported(String),
}

impl fmt::Display for Skipped {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Skipped::NoPrimaryKey => write!(
                f,
                "no primary key (add one, or list the table with `primary_key:` in the config)"
            ),
            Skipped::Unreadable => write!(f, "this role can't read it (GRANT SELECT)"),
            Skipped::Unsupported(why) => write!(f, "{why}"),
        }
    }
}

/// The tables to replicate, and those left out.
#[derive(Debug, Default)]
pub struct Selection {
    pub tables: Vec<TableConfig>,
    pub skipped: Vec<(String, Skipped)>,
}

/// The tables `listed` names, against those the database has (`found`):
/// one listed by name as it is; `schema.*` every table with a primary key
/// in the schema, with the entry's settings; nothing listed, every such
/// table. Schemas in `excluded` hold pg2iceberg's own tables.
pub fn select(listed: &[TableConfig], found: &[SourceTable], excluded: &[String]) -> Selection {
    let by_name: BTreeSet<&str> = listed
        .iter()
        .filter(|t| !t.is_pattern())
        .map(|t| t.name.as_str())
        .collect();
    let everything = [TableConfig::named("*")];
    let entries: &[TableConfig] = if listed.is_empty() {
        &everything
    } else {
        listed
    };
    let mut selection = Selection::default();
    let mut taken: BTreeSet<String> = BTreeSet::new();
    for entry in entries {
        let schema = match entry.name.strip_suffix(".*") {
            Some(schema) => Some(schema),
            None if listed.is_empty() => None,
            None => {
                selection.tables.push(entry.clone());
                continue;
            }
        };
        for t in found {
            let name = format!("{}.{}", t.schema, t.name);
            if schema.is_some_and(|s| s != t.schema)
                || excluded.contains(&t.schema)
                || by_name.contains(name.as_str())
                || !taken.insert(name.clone())
            {
                continue;
            }
            if !t.has_primary_key {
                selection.skipped.push((name, Skipped::NoPrimaryKey));
            } else if !t.readable {
                selection.skipped.push((name, Skipped::Unreadable));
            } else {
                selection.tables.push(TableConfig {
                    name,
                    ..entry.clone()
                });
            }
        }
    }
    selection
}

/// Schemas holding pg2iceberg's own tables, never replicated.
pub fn own_schemas(cfg: &Config) -> Vec<String> {
    let coord = CoordSchema::sanitize(&cfg.state.coordinator_schema)
        .as_str()
        .to_string();
    let mut schemas = vec![coord];
    if !schemas.iter().any(|s| s == "_pg2iceberg") {
        // Blue-green markers.
        schemas.push("_pg2iceberg".into());
    }
    schemas
}

/// Connect to the source database for queries.
pub async fn connect_source(cfg: &Config) -> Result<PgClientImpl> {
    let tls = match cfg.source.postgres.tls_label() {
        "webpki" => TlsMode::Webpki,
        _ => TlsMode::Disable,
    };
    PgClientImpl::connect_with(&cfg.source.postgres.dsn(), tls)
        .await
        .context("connect to the source database")
}

/// Expand `cfg.tables` (see [`select`]) against the source database.
/// A table the config names by name stays as it is: a problem with it
/// fails startup. One found by discovery is left out instead, with a
/// warning, when pg2iceberg can't replicate it.
pub async fn resolve(cfg: &mut Config) -> Result<()> {
    if !cfg.tables.is_empty() && !cfg.tables.iter().any(TableConfig::is_pattern) {
        return Ok(());
    }
    cfg.require_source()?;
    let pg = connect_source(cfg).await?;
    let found = pg
        .list_tables()
        .await
        .context("list the source database's tables")?;
    let by_name: BTreeSet<String> = cfg
        .tables
        .iter()
        .filter(|t| !t.is_pattern())
        .map(|t| t.name.clone())
        .collect();
    let Selection {
        tables: selected,
        mut skipped,
    } = select(&cfg.tables, &found, &own_schemas(cfg));
    let mut tables = Vec::with_capacity(selected.len());
    for t in selected {
        if !by_name.contains(&t.name) {
            let (schema, name) = t.qualified()?;
            if let Err(e) = pg.discover_schema(&schema, &name).await {
                skipped.push((t.name.clone(), Skipped::Unsupported(e.to_string())));
                continue;
            }
        }
        tables.push(t);
    }
    for (table, why) in &skipped {
        tracing::warn!(table = %table, "not replicating: {why}");
    }
    if tables.is_empty() {
        anyhow::bail!(
            "no tables to replicate: the source database has no table pg2iceberg can \
             replicate{} (see the warnings above), and the config names none",
            if cfg.tables.is_empty() {
                ""
            } else {
                " in the listed schemas"
            }
        );
    }
    tracing::info!(
        count = tables.len(),
        tables = %tables.iter().map(|t| t.name.as_str()).collect::<Vec<_>>().join(", "),
        "replicating"
    );
    cfg.tables = tables;
    cfg.validate_tables()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn table(schema: &str, name: &str) -> SourceTable {
        SourceTable {
            schema: schema.into(),
            name: name.into(),
            has_primary_key: true,
            partitioned: false,
            readable: true,
            row_estimate: 0,
        }
    }

    fn names(selection: &Selection) -> Vec<&str> {
        selection.tables.iter().map(|t| t.name.as_str()).collect()
    }

    fn found() -> Vec<SourceTable> {
        vec![
            table("_pg2iceberg", "markers"),
            table("public", "orders"),
            SourceTable {
                has_primary_key: false,
                ..table("public", "events")
            },
            SourceTable {
                readable: false,
                ..table("public", "secrets")
            },
            table("sales", "invoices"),
        ]
    }

    #[test]
    fn nothing_listed_means_every_table_with_a_primary_key() {
        let selection = select(&[], &found(), &["_pg2iceberg".into()]);
        assert_eq!(names(&selection), ["public.orders", "sales.invoices"]);
        assert_eq!(
            selection.skipped,
            [
                ("public.events".to_string(), Skipped::NoPrimaryKey),
                ("public.secrets".to_string(), Skipped::Unreadable),
            ]
        );
    }

    #[test]
    fn a_schema_pattern_takes_its_schemas_tables_with_its_settings() {
        let listed = [TableConfig {
            skip_snapshot: true,
            ..TableConfig::named("sales.*")
        }];
        let selection = select(&listed, &found(), &[]);
        assert_eq!(names(&selection), ["sales.invoices"]);
        assert!(selection.tables[0].skip_snapshot);
        assert!(selection.skipped.is_empty());
    }

    #[test]
    fn a_table_listed_by_name_stays_as_it_is() {
        let listed = [
            TableConfig {
                primary_key: vec!["id".into()],
                ..TableConfig::named("public.events")
            },
            TableConfig::named("public.*"),
        ];
        let selection = select(&listed, &found(), &[]);
        assert_eq!(names(&selection), ["public.events", "public.orders"]);
        assert_eq!(selection.tables[0].primary_key, ["id"]);
        assert_eq!(
            selection.skipped,
            [("public.secrets".to_string(), Skipped::Unreadable)]
        );
    }

    #[test]
    fn overlapping_patterns_take_a_table_once() {
        let listed = [TableConfig::named("sales.*"), TableConfig::named("sales.*")];
        assert_eq!(names(&select(&listed, &found(), &[])), ["sales.invoices"]);
    }
}
