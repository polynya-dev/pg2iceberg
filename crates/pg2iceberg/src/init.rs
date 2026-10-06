//! `pg2iceberg init`: inspect the source database and write a config for
//! it — every table pg2iceberg can replicate, the settings the
//! environment gives, secrets as `${VAR}` references — after checking
//! what replication needs of the database.

use crate::config::{Config, Env, DEFAULT_CONFIG_PATH};
use crate::tables::{self, Skipped};
use anyhow::{Context, Result};
use pg2iceberg_coord::prod::{connect_with, TlsMode};
use pg2iceberg_coord::schema::CoordSchema;
use pg2iceberg_pg::prod::SourceTable;
use std::collections::BTreeSet;
use std::fmt::Write as _;
use std::path::Path;

/// A prerequisite for replication, checked.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Check {
    pub what: String,
    /// What to do about it, if it fails.
    pub fix: Option<String>,
}

/// What `init` found in the source database.
#[derive(Clone, Debug, Default)]
pub struct Inspection {
    pub database: String,
    pub checks: Vec<Check>,
    /// The tables to replicate, with the database's view of them.
    pub tables: Vec<SourceTable>,
    pub skipped: Vec<(String, Skipped)>,
}

/// Inspect the database, write the config to `output` (`-`: stdout), and
/// report on stderr.
pub async fn run(env: Env<'_>, output: &str, force: bool) -> Result<()> {
    let cfg = Config::from_env(env)?;
    cfg.require_source()?;
    if output != "-" && !force && Path::new(output).exists() {
        anyhow::bail!("{output} exists; pass --force to overwrite it, or --output - to print");
    }
    let inspection = inspect(&cfg).await?;
    let yaml = render(&cfg, env, &inspection);
    // It must load as written.
    Config::parse(&yaml, &|name| env(name).or_else(|| Some(String::new())))
        .context("the generated config doesn't parse")?;
    if output == "-" {
        print!("{yaml}");
    } else {
        std::fs::write(output, &yaml).with_context(|| format!("write {output}"))?;
    }
    report(&inspection, output);
    Ok(())
}

async fn inspect(cfg: &Config) -> Result<Inspection> {
    let pg = tables::connect_source(cfg).await?;
    let tls = match cfg.source.postgres.tls_label() {
        "webpki" => TlsMode::Webpki,
        _ => TlsMode::Disable,
    };
    let conn = connect_with(&cfg.source.postgres.dsn(), tls)
        .await
        .context("connect to the source database")?;
    let client = &conn.client;
    let found = pg
        .list_tables()
        .await
        .context("list the source database's tables")?;
    let own = tables::own_schemas(cfg);
    let selection = tables::select(&[], &found, &own);
    let mut skipped = selection.skipped;
    let mut picked = Vec::new();
    for t in selection.tables {
        let (schema, name) = t.qualified()?;
        match pg.discover_schema(&schema, &name).await {
            Ok(_) => picked.push(t.name),
            Err(e) => skipped.push((t.name, Skipped::Unsupported(e.to_string()))),
        }
    }
    let tables: Vec<SourceTable> = found
        .into_iter()
        .filter(|t| picked.contains(&format!("{}.{}", t.schema, t.name)))
        .collect();

    let one = |sql: String| async move {
        let row = client
            .query_one(sql.as_str(), &[])
            .await
            .with_context(|| format!("query {sql}"))?;
        Ok::<_, anyhow::Error>(row)
    };
    let database: String = one("SELECT current_database()".into()).await?.get(0);
    let user: String = one("SELECT current_user::text".into()).await?.get(0);
    let mut checks = Vec::new();

    let wal_level: String = one("SHOW wal_level".into()).await?.get(0);
    checks.push(Check {
        what: format!("wal_level is {wal_level}"),
        fix: (wal_level != "logical").then(|| {
            "set wal_level = logical and restart Postgres (RDS / Aurora: \
             rds.logical_replication = 1 in the parameter group; Cloud SQL: the \
             cloudsql.logical_decoding flag)"
                .into()
        }),
    });

    let can_replicate: bool = one("SELECT r.rolsuper OR r.rolreplication OR EXISTS ( \
             SELECT 1 FROM pg_roles g \
             WHERE g.rolname = 'rds_replication' AND pg_has_role(r.oid, g.oid, 'member')) \
         FROM pg_roles r WHERE r.rolname = current_user"
        .into())
    .await?
    .get(0);
    checks.push(Check {
        what: format!("role {user} can replicate"),
        fix: (!can_replicate).then(|| {
            format!("ALTER ROLE {user} WITH REPLICATION (RDS / Aurora: GRANT rds_replication TO {user})")
        }),
    });

    let slot = &cfg.source.logical.slot_name;
    let slot_row = one(format!(
        "SELECT EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = {}), \
                current_setting('max_replication_slots')::int \
                    - (SELECT count(*) FROM pg_replication_slots)::int",
        literal(slot)
    ))
    .await?;
    let (slot_exists, free_slots): (bool, i32) = (slot_row.get(0), slot_row.get(1));
    checks.push(if slot_exists {
        Check {
            what: format!("replication slot {slot} exists: pg2iceberg resumes from it"),
            fix: None,
        }
    } else {
        Check {
            what: format!("{free_slots} replication slot(s) free for {slot}"),
            fix: (free_slots < 1)
                .then(|| "raise max_replication_slots and restart Postgres".into()),
        }
    });

    let coord = CoordSchema::sanitize(&cfg.state.coordinator_schema);
    if cfg.state.postgres_url.is_empty() {
        let can_create: bool = one(format!(
            "SELECT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = {}) \
                 OR has_database_privilege(current_database(), 'CREATE')",
            literal(coord.as_str())
        ))
        .await?
        .get(0);
        checks.push(Check {
            what: format!("pg2iceberg can keep its state in schema {}", coord.as_str()),
            fix: (!can_create).then(|| {
                format!(
                    "GRANT CREATE ON DATABASE {database} TO {user}, or keep the state in \
                     another database (PG2ICEBERG_STATE_URL)"
                )
            }),
        });
    }

    // pg2iceberg creates the publication itself, and adding a table to
    // one takes owning it.
    let publication = &cfg.source.logical.publication_name;
    let publication_exists: bool = one(format!(
        "SELECT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = {})",
        literal(publication)
    ))
    .await?
    .get(0);
    let others: BTreeSet<String> = client
        .query(
            "SELECT n.nspname || '.' || c.relname FROM pg_class c \
             JOIN pg_namespace n ON n.oid = c.relnamespace \
             WHERE c.relkind IN ('r', 'p') AND NOT pg_has_role(c.relowner, 'USAGE')",
            &[],
        )
        .await
        .context("query table owners")?
        .iter()
        .map(|row| row.get(0))
        .collect();
    let not_owned: Vec<String> = picked
        .iter()
        .filter(|name| others.contains(*name))
        .cloned()
        .collect();
    checks.push(Check {
        what: if publication_exists {
            format!("publication {publication} exists")
        } else {
            format!("pg2iceberg can create publication {publication}")
        },
        fix: (!not_owned.is_empty()).then(|| {
            format!(
                "{user} doesn't own {}: have their owner run CREATE PUBLICATION {publication} \
                 FOR TABLE {} WITH (publish_via_partition_root = true)",
                not_owned.join(", "),
                picked.join(", ")
            )
        }),
    });

    Ok(Inspection {
        database,
        checks,
        tables,
        skipped,
    })
}

/// The config file for `inspection`. Settings the environment gives are
/// written out, secrets as `${VAR}` references; the tables are listed.
pub fn render(cfg: &Config, env: Env, inspection: &Inspection) -> String {
    let mut out = String::new();
    let line = |out: &mut String, text: &str| {
        out.push_str(text);
        out.push('\n');
    };
    let _ = writeln!(
        out,
        "# pg2iceberg configuration for database {:?}, written by `pg2iceberg init`.",
        inspection.database
    );
    line(&mut out, "#");
    line(
        &mut out,
        "# `pg2iceberg run` reads it from the current directory. Environment",
    );
    line(
        &mut out,
        "# variables override it, and ${VAR} reads one: secrets stay there.",
    );
    line(&mut out, "");

    line(&mut out, "source:");
    line(&mut out, "  postgres_url: ${POSTGRES_URL}");
    let logical = &cfg.source.logical;
    let default_logical = crate::config::LogicalConfig::default();
    if logical.slot_name != default_logical.slot_name
        || logical.publication_name != default_logical.publication_name
    {
        line(&mut out, "  logical:");
        let _ = writeln!(out, "    slot_name: {}", scalar(&logical.slot_name));
        let _ = writeln!(
            out,
            "    publication_name: {}",
            scalar(&logical.publication_name)
        );
    }
    line(&mut out, "");

    let sink = &cfg.sink;
    line(&mut out, "sink:");
    if sink.catalog_uri.is_empty() {
        line(
            &mut out,
            "  # catalog_uri: http://localhost:8181  # your Iceberg REST catalog",
        );
    } else {
        let _ = writeln!(out, "  catalog_uri: {}", scalar(&sink.catalog_uri));
    }
    let secret = |out: &mut String, key: &str, var: &str| {
        if env(var).is_some() {
            let _ = writeln!(out, "  {key}: ${{{var}}}");
        }
    };
    secret(&mut out, "catalog_token", "ICEBERG_CATALOG_TOKEN");
    secret(&mut out, "catalog_client_id", "ICEBERG_CATALOG_CLIENT_ID");
    secret(
        &mut out,
        "catalog_client_secret",
        "ICEBERG_CATALOG_CLIENT_SECRET",
    );
    for (key, value) in [
        ("catalog_auth", &sink.catalog_auth),
        ("credential_mode", &sink.credential_mode),
        ("warehouse", &sink.warehouse),
    ] {
        if !value.is_empty() {
            let _ = writeln!(out, "  {key}: {}", scalar(value));
        }
    }
    if sink.namespace.is_empty() {
        line(
            &mut out,
            "  # namespace: analytics  # one Iceberg namespace for every table; \
             default: each table's Postgres schema",
        );
    } else {
        let _ = writeln!(out, "  namespace: {}", scalar(&sink.namespace));
    }
    for (key, value) in [
        ("s3_endpoint", &sink.s3_endpoint),
        ("s3_region", &sink.s3_region),
    ] {
        if !value.is_empty() {
            let _ = writeln!(out, "  {key}: {}", scalar(value));
        }
    }
    line(&mut out, "");

    line(
        &mut out,
        "# Every table with a primary key, as of now. Without `tables:`, pg2iceberg",
    );
    line(
        &mut out,
        "# replicates every such table when it starts, including ones created later.",
    );
    line(&mut out, "tables:");
    for t in &inspection.tables {
        let name = format!("{}.{}", t.schema, t.name);
        let mut notes = vec![format!("~{} rows", count(t.row_estimate))];
        if t.partitioned {
            notes.push("partitioned".into());
        }
        let _ = writeln!(out, "  - name: {}  # {}", scalar(&name), notes.join(", "));
    }
    if inspection.tables.is_empty() {
        line(&mut out, "  []  # none found");
    }
    if !inspection.skipped.is_empty() {
        line(&mut out, "  # Not replicated:");
        for (name, why) in &inspection.skipped {
            let _ = writeln!(out, "  # - name: {}  # {why}", scalar(name));
        }
    }
    out
}

/// Summary on stderr.
fn report(inspection: &Inspection, output: &str) {
    for check in &inspection.checks {
        match &check.fix {
            None => eprintln!("✓ {}", check.what),
            Some(fix) => eprintln!("✗ {} — {fix}", check.what),
        }
    }
    eprintln!(
        "{} table(s) to replicate, {} left out",
        inspection.tables.len(),
        inspection.skipped.len()
    );
    for (name, why) in &inspection.skipped {
        eprintln!("  {name}: {why}");
    }
    if output != "-" {
        let run = if output == DEFAULT_CONFIG_PATH {
            "pg2iceberg run".to_string()
        } else {
            format!("pg2iceberg run --config {output}")
        };
        eprintln!("wrote {output}; next: {run}");
    }
    if inspection.checks.iter().any(|c| c.fix.is_some()) {
        eprintln!("fix what's marked ✗ before starting pg2iceberg");
    }
}

/// `s` as a YAML scalar: plain if it reads back as the same string,
/// else quoted.
fn scalar(s: &str) -> String {
    let plain = !s.is_empty()
        && s.chars()
            .all(|c| c.is_ascii_alphanumeric() || "_-./:@*".contains(c))
        && !s.starts_with(['-', ':', '@', '*'])
        && matches!(
            serde_yaml::from_str::<serde_yaml::Value>(s),
            Ok(serde_yaml::Value::String(read)) if read == s
        );
    if plain {
        s.to_string()
    } else {
        serde_yaml::to_string(s)
            .map(|y| y.trim_end().to_string())
            .unwrap_or_else(|_| format!("{s:?}"))
    }
}

/// A SQL string literal.
fn literal(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

/// A row count, roughly: `340`, `12k`, `1.2M`.
fn count(n: i64) -> String {
    match n {
        n if n >= 1_000_000_000 => format!("{:.1}B", n as f64 / 1e9),
        n if n >= 1_000_000 => format!("{:.1}M", n as f64 / 1e6),
        n if n >= 1_000 => format!("{}k", n / 1_000),
        n => n.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    fn table(schema: &str, name: &str, rows: i64, partitioned: bool) -> SourceTable {
        SourceTable {
            schema: schema.into(),
            name: name.into(),
            has_primary_key: true,
            partitioned,
            readable: true,
            row_estimate: rows,
        }
    }

    fn inspection() -> Inspection {
        Inspection {
            database: "app".into(),
            checks: vec![],
            tables: vec![
                table("public", "orders", 1_204_331, false),
                table("sales", "invoices", 42, true),
            ],
            skipped: vec![("public.events".into(), Skipped::NoPrimaryKey)],
        }
    }

    #[test]
    fn writes_the_tables_and_keeps_secrets_in_the_environment() {
        let vars = BTreeMap::from([
            ("POSTGRES_URL", "postgres://u:hunter2@db/app"),
            ("ICEBERG_CATALOG_URL", "https://catalog.example.com"),
            ("ICEBERG_CATALOG_TOKEN", "t0ken"),
            ("ICEBERG_WAREHOUSE", "s3://lake/"),
        ]);
        let env = |name: &str| vars.get(name).map(|v| v.to_string());
        let cfg = Config::from_env(&env).unwrap();
        let yaml = render(&cfg, &env, &inspection());
        assert!(
            !yaml.contains("hunter2") && !yaml.contains("t0ken"),
            "{yaml}"
        );
        assert!(yaml.contains("postgres_url: ${POSTGRES_URL}"), "{yaml}");
        assert!(
            yaml.contains("catalog_token: ${ICEBERG_CATALOG_TOKEN}"),
            "{yaml}"
        );
        assert!(
            yaml.contains("# - name: public.events  # no primary key"),
            "{yaml}"
        );

        // It loads back, with the environment, into the same settings.
        let loaded = Config::parse(&yaml, &env).unwrap();
        assert_eq!(
            loaded
                .tables
                .iter()
                .map(|t| t.name.as_str())
                .collect::<Vec<_>>(),
            ["public.orders", "sales.invoices"]
        );
        assert_eq!(loaded.source.postgres_url, "postgres://u:hunter2@db/app");
        assert_eq!(loaded.sink.catalog_uri, "https://catalog.example.com");
        assert_eq!(loaded.sink.catalog_token, "t0ken");
        assert_eq!(loaded.sink.warehouse, "s3://lake/");
        assert!(loaded.sink.namespace.is_empty());
    }

    #[test]
    fn names_that_need_quoting_are_quoted() {
        assert_eq!(scalar("public.orders"), "public.orders");
        assert_eq!(scalar("s3://lake/"), "s3://lake/");
        // Whatever it takes, each reads back as itself.
        for name in [
            "true",
            "1.5",
            "null",
            "public.My Table",
            "odd: name",
            "-x",
            "# no",
            "",
        ] {
            let written = scalar(name);
            let read: serde_yaml::Value = serde_yaml::from_str(&written).unwrap();
            assert_eq!(read, serde_yaml::Value::String(name.into()), "{written}");
        }
        assert_eq!(scalar("true"), "'true'");
    }

    #[test]
    fn counts_read_roughly() {
        assert_eq!(count(340), "340");
        assert_eq!(count(12_345), "12k");
        assert_eq!(count(1_204_331), "1.2M");
    }
}
