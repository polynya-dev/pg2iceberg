//! [`PgClientImpl`]: production [`PgClient`] backed by `tokio-postgres`
//! in **logical replication mode**.
//!
//! Replication mode supports a subset of SQL via the simple-query
//! protocol — enough for the catalog-style queries we need
//! (`CREATE PUBLICATION`, slot CRUD, `pg_export_snapshot`) plus the
//! `START_REPLICATION` / `CREATE_REPLICATION_SLOT` replication commands
//! that aren't available in regular mode.
//!
//! # Connection lifetime
//!
//! `tokio-postgres` returns the `Connection` future separately from the
//! `Client`. We spawn the connection on a tokio task and abort it when
//! the connection is dropped. A [`PgClientImpl`] keeps one connection
//! for its queries, reopened on next use once it has closed (Postgres
//! restarted, the network dropped it): the operation in flight fails,
//! the next one runs on a new connection. Each `start_replication`
//! opens a connection of its own, which the stream owns — `COPY BOTH`
//! takes a connection over for good.

use crate::prod::tls::{build_rustls_connector, TlsMode};
use crate::prod::typemap::{column_type, Domain};
use crate::prod::value_decode::decode_text;
use crate::{
    ColumnDefault, DecodedMessage, PgClient, PgError, ReplicationStream, Result, SlotHealth,
    SnapshotId, WalStatus,
};
use async_trait::async_trait;
use pg2iceberg_core::{Lsn, TableIdent};
use postgres_replication::LogicalReplicationStream;
use std::collections::HashMap;
use tokio::sync::{MappedMutexGuard, Mutex, MutexGuard};
use tokio::task::AbortHandle;
use tokio_postgres::{config::ReplicationMode, Client, NoTls, SimpleQueryMessage};

/// A `tokio_postgres` client in logical replication mode, on a
/// connection reopened when it closes (see the module docs).
pub struct PgClientImpl {
    conn: Mutex<Conn>,
    config: tokio_postgres::Config,
    tls: TlsMode,
}

/// A connection: its client, and the background task driving it,
/// aborted when this is dropped.
pub(crate) struct Conn {
    client: Client,
    task: AbortHandle,
}

impl Conn {
    async fn open(config: &tokio_postgres::Config, tls: TlsMode) -> Result<Self> {
        match tls {
            TlsMode::Disable => Self::finish_open(config, NoTls).await,
            TlsMode::Webpki => {
                let connector = build_rustls_connector()?;
                Self::finish_open(config, connector).await
            }
        }
    }

    async fn finish_open<T>(config: &tokio_postgres::Config, tls: T) -> Result<Self>
    where
        T: tokio_postgres::tls::MakeTlsConnect<tokio_postgres::Socket>
            + Clone
            + Send
            + Sync
            + 'static,
        T::Stream: Send,
        T::TlsConnect: Send,
        <T::TlsConnect as tokio_postgres::tls::TlsConnect<tokio_postgres::Socket>>::Future: Send,
    {
        let (client, connection) = config
            .connect(tls)
            .await
            .map_err(|e| PgError::Connection(e.to_string()))?;

        // Spawn the connection task. If it errors, the next op on
        // `client` surfaces the error; we don't try to log here to
        // keep the prod module dep-light.
        let handle = tokio::spawn(async move {
            let _ = connection.await;
        });

        Ok(Self {
            client,
            task: handle.abort_handle(),
        })
    }
}

impl Drop for Conn {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl PgClientImpl {
    /// Connect to Postgres in **logical replication mode** with TLS
    /// disabled. Convenience wrapper for tests / sample configs that
    /// point at a local Postgres that doesn't require encryption.
    /// Production-managed Postgres almost always wants `connect_with`
    /// + `TlsMode::Webpki`.
    pub async fn connect(conn_str: &str) -> Result<Self> {
        Self::connect_with(conn_str, TlsMode::Disable).await
    }

    /// Connect with the configured [`TlsMode`]. The conn string follows
    /// libpq URI/keyword format; the caller is responsible for setting
    /// `dbname` (required by `START_REPLICATION`). Logical-replication
    /// mode is added implicitly.
    pub async fn connect_with(conn_str: &str, tls: TlsMode) -> Result<Self> {
        let mut config: tokio_postgres::Config = conn_str
            .parse()
            .map_err(|e: tokio_postgres::Error| PgError::Connection(e.to_string()))?;
        config.replication_mode(ReplicationMode::Logical);
        let conn = Conn::open(&config, tls).await?;
        Ok(Self {
            conn: Mutex::new(conn),
            config,
            tls,
        })
    }

    /// The client, on a new connection if the last one closed. A new
    /// connection serves as well: the only state left on one is
    /// `export_snapshot`'s transaction, which a closed one has lost.
    async fn client(&self) -> Result<MappedMutexGuard<'_, Client>> {
        let mut conn = self.conn.lock().await;
        if conn.client.is_closed() {
            *conn = Conn::open(&self.config, self.tls).await?;
        }
        Ok(MutexGuard::map(conn, |c| &mut c.client))
    }
}

impl PgClientImpl {
    /// Discover a table's columns + primary key from the source PG.
    /// Mirrors `postgres/schema.go::DiscoverSchema` in the Go
    /// reference. Operators can omit `columns:` from YAML and we'll
    /// query `information_schema.columns` + `pg_index` at startup.
    ///
    /// Returns a [`pg2iceberg_core::TableSchema`] with auto-assigned
    /// 1-based field ids, PK columns marked, and types mapped to
    /// our [`PgType`] enum.
    ///
    /// [`PgType`]: pg2iceberg_core::typemap::PgType
    pub async fn discover_schema(
        &self,
        schema: &str,
        table: &str,
    ) -> Result<pg2iceberg_core::TableSchema> {
        super::discover::discover_schema(&*self.client().await?, schema, table).await
    }

    /// The source's domains, keyed by oid. pgoutput names a domain
    /// column's type by the domain's oid; the decoder types it as the
    /// domain's base type, as discovery does.
    pub async fn domains(&self) -> Result<HashMap<u32, Domain>> {
        domains_of(&*self.client().await?).await
    }

    /// Every table a publication can hold — an ordinary or partitioned
    /// table, not a partition (its parent replicates it), temporary,
    /// unlogged, an extension's own, or in a system schema — by schema
    /// and name.
    pub async fn list_tables(&self) -> Result<Vec<SourceTable>> {
        let rows = self
            .client()
            .await?
            .simple_query(
                "SELECT n.nspname, c.relname, \
                        EXISTS (SELECT 1 FROM pg_index i \
                                WHERE i.indrelid = c.oid AND i.indisprimary), \
                        c.relkind = 'p', \
                        has_table_privilege(c.oid, 'SELECT'), \
                        (CASE WHEN c.relkind = 'p' THEN \
                            (SELECT coalesce(sum(greatest(l.reltuples, 0)), 0) \
                             FROM pg_partition_tree(c.oid) t JOIN pg_class l ON l.oid = t.relid \
                             WHERE t.isleaf) \
                         ELSE greatest(c.reltuples, 0) END)::int8 \
                 FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace \
                 WHERE c.relkind IN ('r', 'p') AND NOT c.relispartition \
                   AND c.relpersistence = 'p' \
                   AND n.nspname <> 'information_schema' AND n.nspname NOT LIKE 'pg\\_%' \
                   AND NOT EXISTS (SELECT 1 FROM pg_depend d \
                                   WHERE d.classid = 'pg_class'::regclass \
                                     AND d.objid = c.oid AND d.deptype = 'e') \
                 ORDER BY 1, 2",
            )
            .await
            .map_err(|e| PgError::Protocol(format!("list tables: {e}")))?;
        let mut out = Vec::new();
        for msg in rows {
            let SimpleQueryMessage::Row(row) = msg else {
                continue;
            };
            let text = |i: usize| -> Result<&str> {
                row.try_get(i)
                    .map_err(|e| PgError::Protocol(e.to_string()))?
                    .ok_or_else(|| PgError::Protocol(format!("list tables: column {i} is NULL")))
            };
            out.push(SourceTable {
                schema: text(0)?.to_string(),
                name: text(1)?.to_string(),
                has_primary_key: text(2)? == "t",
                partitioned: text(3)? == "t",
                readable: text(4)? == "t",
                row_estimate: text(5)?.parse().unwrap_or(0),
            });
        }
        Ok(out)
    }
}

/// A table in the source database (see [`PgClientImpl::list_tables`]).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SourceTable {
    pub schema: String,
    pub name: String,
    pub has_primary_key: bool,
    /// Declaratively partitioned: its partitions replicate as it.
    pub partitioned: bool,
    /// Whether the connecting role can read it, as the initial snapshot
    /// does.
    pub readable: bool,
    /// Rows, as the planner's statistics estimate them.
    pub row_estimate: i64,
}

async fn domains_of(client: &Client) -> Result<HashMap<u32, Domain>> {
    let rows = client
        .simple_query("SELECT oid, typbasetype, typtypmod FROM pg_type WHERE typtype = 'd'")
        .await
        .map_err(|e| PgError::Protocol(e.to_string()))?;
    let mut out = HashMap::new();
    for msg in rows {
        if let SimpleQueryMessage::Row(row) = msg {
            let field = |i: usize| -> Result<&str> {
                row.try_get(i)
                    .map_err(|e| PgError::Protocol(e.to_string()))?
                    .ok_or_else(|| PgError::Protocol("pg_type returned NULL".into()))
            };
            let parse = |i: usize| -> Result<i64> {
                let v = field(i)?;
                v.parse()
                    .map_err(|e| PgError::Protocol(format!("parse pg_type value {v:?}: {e}")))
            };
            out.insert(
                parse(0)? as u32,
                Domain {
                    base_oid: parse(1)? as u32,
                    typmod: parse(2)? as i32,
                },
            );
        }
    }
    Ok(out)
}

#[async_trait]
impl PgClient for PgClientImpl {
    async fn create_publication(&self, name: &str, tables: &[TableIdent]) -> Result<()> {
        // `FOR TABLE` requires at least one table; we error rather than
        // emit `FOR ALL TABLES`, which is a different (and broader)
        // semantic that the materializer doesn't expect.
        if tables.is_empty() {
            return Err(PgError::Other(
                "create_publication: tables must not be empty".into(),
            ));
        }
        let table_list = tables
            .iter()
            .map(|t| {
                if t.namespace.0.is_empty() {
                    quote_ident(&t.name)
                } else {
                    format!(
                        "{}.{}",
                        quote_ident(&t.namespace.0.join(".")),
                        quote_ident(&t.name)
                    )
                }
            })
            .collect::<Vec<_>>()
            .join(", ");
        // `publish_via_partition_root = true` makes pgoutput tag DML
        // events on partition children with the *parent* table's relid
        // and name. Without it, INSERT into a `PARTITION BY RANGE`
        // parent fans out to children physically, and pgoutput emits
        // Relation/Insert/Update/Delete against the child relid — so
        // the consumer would see N untracked tables (`<parent>_2024`,
        // `<parent>_2025`, …) instead of the one configured `<parent>`.
        // PG 13+ supports this option (we require 14+, so no version
        // gate here). Mirrors `pg2iceberg/logical/logical.go`
        // `ensurePublication`.
        let q = format!(
            "CREATE PUBLICATION {} FOR TABLE {} WITH (publish_via_partition_root = true)",
            quote_ident(name),
            table_list
        );
        simple_exec(&*self.client().await?, &q).await
    }

    async fn create_slot(&self, slot: &str) -> Result<Lsn> {
        // `NOEXPORT_SNAPSHOT` keeps creation simple — no transactional
        // snapshot is exposed to the caller. If we later need
        // initial-snapshot consistency we'll switch to `USE_SNAPSHOT`
        // inside a `BEGIN ... ` and add a separate API.
        let q = format!(
            "CREATE_REPLICATION_SLOT {} LOGICAL pgoutput NOEXPORT_SNAPSHOT",
            quote_ident(slot)
        );
        let rows = self
            .client()
            .await?
            .simple_query(&q)
            .await
            .map_err(|e| PgError::Protocol(e.to_string()))?;
        for msg in rows {
            if let SimpleQueryMessage::Row(row) = msg {
                let cp = row
                    .try_get("consistent_point")
                    .map_err(|e| PgError::Protocol(e.to_string()))?
                    .ok_or_else(|| {
                        PgError::Protocol(
                            "CREATE_REPLICATION_SLOT returned no consistent_point".into(),
                        )
                    })?;
                return parse_lsn(cp);
            }
        }
        Err(PgError::Protocol(
            "CREATE_REPLICATION_SLOT returned no rows".into(),
        ))
    }

    async fn slot_exists(&self, slot: &str) -> Result<bool> {
        let q = format!(
            "SELECT slot_name FROM pg_replication_slots WHERE slot_name = {}",
            quote_lit(slot)
        );
        let rows = self
            .client()
            .await?
            .simple_query(&q)
            .await
            .map_err(|e| PgError::Protocol(e.to_string()))?;
        Ok(rows.iter().any(|m| matches!(m, SimpleQueryMessage::Row(_))))
    }

    async fn slot_restart_lsn(&self, slot: &str) -> Result<Option<Lsn>> {
        let q = format!(
            "SELECT restart_lsn::text FROM pg_replication_slots WHERE slot_name = {}",
            quote_lit(slot)
        );
        let rows = self
            .client()
            .await?
            .simple_query(&q)
            .await
            .map_err(|e| PgError::Protocol(e.to_string()))?;
        for msg in rows {
            if let SimpleQueryMessage::Row(row) = msg {
                let v: Option<&str> = row
                    .try_get("restart_lsn")
                    .map_err(|e| PgError::Protocol(e.to_string()))?;
                return match v {
                    Some(s) => Ok(Some(parse_lsn(s)?)),
                    None => Ok(None),
                };
            }
        }
        Err(PgError::SlotNotFound(slot.to_string()))
    }

    async fn slot_confirmed_flush_lsn(&self, slot: &str) -> Result<Option<Lsn>> {
        let q = format!(
            "SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name = {}",
            quote_lit(slot)
        );
        let rows = self
            .client()
            .await?
            .simple_query(&q)
            .await
            .map_err(|e| PgError::Protocol(e.to_string()))?;
        for msg in rows {
            if let SimpleQueryMessage::Row(row) = msg {
                let v: Option<&str> = row
                    .try_get("confirmed_flush_lsn")
                    .map_err(|e| PgError::Protocol(e.to_string()))?;
                return match v {
                    Some(s) => Ok(Some(parse_lsn(s)?)),
                    None => Ok(Some(Lsn(0))),
                };
            }
        }
        Ok(None)
    }

    async fn slot_health(&self, slot: &str) -> Result<Option<SlotHealth>> {
        // One query covering every slot field we use: restart_lsn,
        // confirmed_flush_lsn, wal_status, safe_wal_size, conflicting.
        //
        // pg2iceberg requires PG 14+ (validated at startup via
        // `server_version_num`), so `wal_status` and `safe_wal_size`
        // are always direct columns. `conflicting` was added in PG
        // 16, so we read it via `to_jsonb` and degrade to `false` on
        // older versions.
        let q = format!(
            "SELECT \
                restart_lsn::text AS restart_lsn, \
                confirmed_flush_lsn::text AS confirmed_flush_lsn, \
                wal_status::text AS wal_status, \
                safe_wal_size::text AS safe_wal_size, \
                COALESCE((to_jsonb(pg_replication_slots.*) ->> 'conflicting')::boolean, false) AS conflicting \
             FROM pg_replication_slots \
             WHERE slot_name = {}",
            quote_lit(slot)
        );
        let rows = self
            .client()
            .await?
            .simple_query(&q)
            .await
            .map_err(|e| PgError::Protocol(e.to_string()))?;
        for msg in rows {
            if let SimpleQueryMessage::Row(row) = msg {
                let restart_text: Option<&str> = row
                    .try_get("restart_lsn")
                    .map_err(|e| PgError::Protocol(e.to_string()))?;
                let confirmed_text: Option<&str> = row
                    .try_get("confirmed_flush_lsn")
                    .map_err(|e| PgError::Protocol(e.to_string()))?;
                let wal_status_text: Option<&str> = row
                    .try_get("wal_status")
                    .map_err(|e| PgError::Protocol(e.to_string()))?;
                let safe_wal_size_text: Option<&str> = row
                    .try_get("safe_wal_size")
                    .map_err(|e| PgError::Protocol(e.to_string()))?;
                let conflicting_text: Option<&str> = row
                    .try_get("conflicting")
                    .map_err(|e| PgError::Protocol(e.to_string()))?;

                let restart_lsn = restart_text.map(parse_lsn).transpose()?.unwrap_or(Lsn(0));
                let confirmed_flush_lsn =
                    confirmed_text.map(parse_lsn).transpose()?.unwrap_or(Lsn(0));
                let wal_status = wal_status_text.map(WalStatus::parse).transpose()?;
                let safe_wal_size = safe_wal_size_text.and_then(|s| s.parse::<i64>().ok());
                let conflicting = conflicting_text
                    .map(|s| s == "t" || s == "true")
                    .unwrap_or(false);

                return Ok(Some(SlotHealth {
                    exists: true,
                    restart_lsn,
                    confirmed_flush_lsn,
                    wal_status,
                    conflicting,
                    safe_wal_size,
                }));
            }
        }
        Ok(None)
    }

    async fn table_oid(&self, namespace: &str, name: &str) -> Result<Option<u32>> {
        // `pg_class.oid` joined to `pg_namespace.nspname` for the
        // (schema, table) tuple. Returns NULL when the table doesn't
        // exist; we map that to None.
        let q = format!(
            "SELECT c.oid::int8 AS oid \
             FROM pg_class c \
             JOIN pg_namespace n ON n.oid = c.relnamespace \
             WHERE n.nspname = {} AND c.relname = {}",
            quote_lit(namespace),
            quote_lit(name),
        );
        let rows = self
            .client()
            .await?
            .simple_query(&q)
            .await
            .map_err(|e| PgError::Protocol(e.to_string()))?;
        for msg in rows {
            if let SimpleQueryMessage::Row(row) = msg {
                let v: Option<&str> = row
                    .try_get("oid")
                    .map_err(|e| PgError::Protocol(e.to_string()))?;
                return match v {
                    Some(s) => s
                        .parse::<u32>()
                        .map(Some)
                        .map_err(|e| PgError::Protocol(format!("parse oid {s:?}: {e}"))),
                    None => Ok(None),
                };
            }
        }
        Ok(None)
    }

    async fn publication_tables(&self, publication_name: &str) -> Result<Vec<TableIdent>> {
        let q = format!(
            "SELECT schemaname, tablename \
             FROM pg_publication_tables \
             WHERE pubname = {} \
             ORDER BY schemaname, tablename",
            quote_lit(publication_name)
        );
        let rows = self
            .client()
            .await?
            .simple_query(&q)
            .await
            .map_err(|e| PgError::Protocol(e.to_string()))?;
        let mut out = Vec::new();
        for msg in rows {
            if let SimpleQueryMessage::Row(row) = msg {
                let schema: &str = row
                    .try_get("schemaname")
                    .map_err(|e| PgError::Protocol(e.to_string()))?
                    .ok_or_else(|| PgError::Protocol("schemaname returned NULL".into()))?;
                let name: &str = row
                    .try_get("tablename")
                    .map_err(|e| PgError::Protocol(e.to_string()))?
                    .ok_or_else(|| PgError::Protocol("tablename returned NULL".into()))?;
                out.push(TableIdent {
                    namespace: pg2iceberg_core::Namespace(vec![schema.into()]),
                    name: name.into(),
                });
            }
        }
        Ok(out)
    }

    async fn export_snapshot(&self) -> Result<SnapshotId> {
        // Begin a REPEATABLE READ transaction and call
        // `pg_export_snapshot()`. The caller is responsible for closing
        // out the transaction when they no longer need the snapshot.
        // Today we leave it open implicitly — the snapshot stays valid
        // as long as this connection lives.
        let client = self.client().await?;
        client
            .simple_query("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
            .await
            .map_err(|e| PgError::Protocol(e.to_string()))?;
        let rows = client
            .simple_query("SELECT pg_export_snapshot()")
            .await
            .map_err(|e| PgError::Protocol(e.to_string()))?;
        for msg in rows {
            if let SimpleQueryMessage::Row(row) = msg {
                let id = row
                    .try_get("pg_export_snapshot")
                    .map_err(|e| PgError::Protocol(e.to_string()))?
                    .ok_or_else(|| {
                        PgError::Protocol("pg_export_snapshot() returned NULL".into())
                    })?;
                return Ok(SnapshotId(id.to_string()));
            }
        }
        Err(PgError::Protocol(
            "pg_export_snapshot() returned no rows".into(),
        ))
    }

    async fn identify_system_id(&self) -> Result<u64> {
        // `IDENTIFY_SYSTEM` is a replication-mode command that
        // returns four columns: systemid (text), timeline (int),
        // xlogpos (text), dbname (text or NULL). systemid is the
        // 19-digit cluster fingerprint we want.
        let rows = self
            .client()
            .await?
            .simple_query("IDENTIFY_SYSTEM")
            .await
            .map_err(|e| PgError::Protocol(e.to_string()))?;
        for msg in rows {
            if let SimpleQueryMessage::Row(row) = msg {
                let id = row
                    .try_get("systemid")
                    .map_err(|e| PgError::Protocol(e.to_string()))?
                    .ok_or_else(|| {
                        PgError::Protocol("IDENTIFY_SYSTEM returned NULL systemid".into())
                    })?;
                return id
                    .parse::<u64>()
                    .map_err(|e| PgError::Protocol(format!("parse systemid {id:?}: {e}")));
            }
        }
        Err(PgError::Protocol("IDENTIFY_SYSTEM returned no rows".into()))
    }

    async fn server_version_num(&self) -> Result<i32> {
        // `SHOW server_version_num` returns the encoded version string
        // (e.g. "160004" for PG 16.4). Available on every supported PG.
        let rows = self
            .client()
            .await?
            .simple_query("SHOW server_version_num")
            .await
            .map_err(|e| PgError::Protocol(e.to_string()))?;
        for msg in rows {
            if let SimpleQueryMessage::Row(row) = msg {
                let v = row
                    .try_get(0)
                    .map_err(|e| PgError::Protocol(e.to_string()))?
                    .ok_or_else(|| {
                        PgError::Protocol("SHOW server_version_num returned NULL".into())
                    })?;
                return v.parse::<i32>().map_err(|e| {
                    PgError::Protocol(format!("parse server_version_num {v:?}: {e}"))
                });
            }
        }
        Err(PgError::Protocol(
            "SHOW server_version_num returned no rows".into(),
        ))
    }

    async fn start_replication(
        &self,
        slot: &str,
        start: Lsn,
        publication: &str,
    ) -> Result<Box<dyn ReplicationStream>> {
        // `START_REPLICATION SLOT <slot> LOGICAL <lsn> (
        //     "proto_version" '1',
        //     "publication_names" '<pub>'
        // )`
        //
        // proto_version '1' keeps us in text-mode pgoutput (matches the
        // value-decoder in `value_decode.rs`). v2 enables streaming
        // in-progress txns; v3 adds two-phase commit; v4 switches to
        // binary-format tuples. Stick with v1 until DST shows we need
        // streaming.
        let opts = format!(
            r#"("proto_version" '1', "publication_names" {})"#,
            // Inner literal must itself be a quoted-identifier-as-text:
            // the publication name (after identifier quoting) is then
            // wrapped as a SQL string.
            quote_lit(&quote_ident(publication))
        );
        let q = format!(
            "START_REPLICATION SLOT {} LOGICAL {} {}",
            quote_ident(slot),
            format_lsn(start),
            opts
        );
        // On a connection of its own, which the stream owns: streaming
        // takes it over for good. When it ends — Postgres restarting,
        // the network dropping it — calling this again reconnects.
        let conn = Conn::open(&self.config, self.tls).await?;
        let domains = domains_of(&conn.client).await?;
        let copy_stream = conn
            .client
            .copy_both_simple::<bytes::Bytes>(&q)
            .await
            .map_err(|e| PgError::Protocol(e.to_string()))?;
        Ok(Box::new(super::ReplicationStreamImpl::wrap(
            LogicalReplicationStream::new(copy_stream),
            domains,
            conn,
        )))
    }

    async fn drop_slot(&self, slot: &str) -> Result<()> {
        // Probe `pg_replication_slots` first so we can: (a) treat
        // missing slots as a no-op (idempotent) and (b) refuse to
        // drop an active slot — `pg_drop_replication_slot` would
        // error in that case anyway, but we surface a clearer
        // message and skip the round-trip.
        let probe = format!(
            "SELECT active FROM pg_replication_slots WHERE slot_name = {}",
            quote_lit(slot)
        );
        let rows = self
            .client()
            .await?
            .simple_query(&probe)
            .await
            .map_err(|e| PgError::Protocol(e.to_string()))?;
        let mut found = false;
        let mut active = false;
        for msg in rows {
            if let SimpleQueryMessage::Row(row) = msg {
                found = true;
                if let Some(s) = row
                    .try_get(0)
                    .map_err(|e| PgError::Protocol(e.to_string()))?
                {
                    // Boolean comes back as "t"/"f" in simple-query mode.
                    active = s == "t" || s == "true";
                }
            }
        }
        if !found {
            return Ok(());
        }
        if active {
            return Err(PgError::Other(format!(
                "replication slot {slot:?} is still active; \
                 stop the consumer (e.g. by terminating the running \
                 pg2iceberg process) before running cleanup"
            )));
        }
        let q = format!("SELECT pg_drop_replication_slot({})", quote_lit(slot));
        simple_exec(&*self.client().await?, &q).await
    }

    async fn drop_publication(&self, name: &str) -> Result<()> {
        let q = format!("DROP PUBLICATION IF EXISTS {}", quote_ident(name));
        simple_exec(&*self.client().await?, &q).await
    }

    async fn alter_publication_add_table(&self, name: &str, ident: &TableIdent) -> Result<()> {
        let qualified = if ident.namespace.0.is_empty() {
            quote_ident(&ident.name)
        } else {
            format!(
                "{}.{}",
                quote_ident(&ident.namespace.0.join(".")),
                quote_ident(&ident.name)
            )
        };
        let q = format!(
            "ALTER PUBLICATION {} ADD TABLE {}",
            quote_ident(name),
            qualified
        );
        match simple_exec(&*self.client().await?, &q).await {
            Ok(()) => Ok(()),
            // Idempotency: PG raises SQLSTATE 42710 ("relation … is
            // already member of publication") when the table is
            // already in the publication. Treat as success so a
            // crash-restart between ALTER and downstream state
            // updates can re-enter cleanly.
            Err(PgError::Protocol(msg)) if msg.contains("is already member of publication") => {
                Ok(())
            }
            Err(e) => Err(e),
        }
    }

    async fn column_defaults(&self, rel_id: u32) -> Result<Vec<ColumnDefault>> {
        // By oid, as pgoutput names the table: its name may be another
        // table's by now. `attmissingval` is a one-element array of the
        // column's type; its element's text form is what pgoutput sends
        // for the value. (An array column's would be an array of arrays:
        // left unread.)
        let q = format!(
            "SELECT a.attname, a.atttypid::int8, a.atttypmod, a.atthasdef, \
                    a.atthasmissing AND t.typcategory <> 'A', \
                    array_to_string(a.attmissingval, '') \
             FROM pg_attribute a JOIN pg_type t ON t.oid = a.atttypid \
             WHERE a.attrelid = {rel_id} AND a.attnum > 0 AND NOT a.attisdropped \
             ORDER BY a.attnum"
        );
        let client = self.client().await?;
        let rows = client
            .simple_query(&q)
            .await
            .map_err(|e| PgError::Protocol(e.to_string()))?;
        let mut domains = None;
        let mut out = Vec::new();
        for msg in rows {
            let SimpleQueryMessage::Row(row) = msg else {
                continue;
            };
            let field = |i: usize| -> Result<Option<&str>> {
                row.try_get(i).map_err(|e| PgError::Protocol(e.to_string()))
            };
            let number = |i: usize| -> Result<i64> {
                let v = field(i)?.unwrap_or("0");
                v.parse()
                    .map_err(|e| PgError::Protocol(format!("parse pg_attribute value {v:?}: {e}")))
            };
            let name = field(0)?.unwrap_or_default().to_string();
            let stored = match (field(4)?, field(5)?) {
                (Some("t"), Some(text)) => {
                    if domains.is_none() {
                        domains = Some(domains_of(&client).await?);
                    }
                    let ty = column_type(
                        number(1)? as u32,
                        number(2)? as i32,
                        domains.as_ref().expect("just read"),
                    );
                    let value = decode_text(ty, text.as_bytes()).map_err(|e| {
                        PgError::Protocol(format!("decode {name}'s stored default: {e}"))
                    })?;
                    Some(value)
                }
                _ => None,
            };
            out.push(ColumnDefault {
                name,
                stored,
                has_default: field(3)? == Some("t"),
            });
        }
        Ok(out)
    }
}

async fn simple_exec(client: &Client, q: &str) -> Result<()> {
    client
        .simple_query(q)
        .await
        .map_err(|e| PgError::Protocol(e.to_string()))?;
    Ok(())
}

/// Postgres-style identifier quoting: wrap in double quotes; embedded
/// double quotes are doubled.
fn quote_ident(name: &str) -> String {
    let escaped = name.replace('"', "\"\"");
    format!("\"{escaped}\"")
}

/// Postgres-style literal quoting: wrap in single quotes; embedded
/// single quotes and backslashes are doubled.
fn quote_lit(value: &str) -> String {
    let escaped = value.replace('\'', "''");
    format!("'{escaped}'")
}

/// Format an [`Lsn`] as `XXXXXXXX/XXXXXXXX` (the canonical
/// hex-with-slash form that Postgres replication commands accept).
fn format_lsn(lsn: Lsn) -> String {
    let bits: u64 = lsn.0;
    format!("{:X}/{:X}", bits >> 32, bits & 0xffff_ffff)
}

/// Parse `XXXXXXXX/XXXXXXXX` (or `0/0`) into [`Lsn`]. Accepts the form
/// that `pg_replication_slots.restart_lsn::text` and the
/// `consistent_point` column emit.
fn parse_lsn(s: &str) -> Result<Lsn> {
    let (hi, lo) = s
        .split_once('/')
        .ok_or_else(|| PgError::Protocol(format!("malformed LSN: {s}")))?;
    let hi = u64::from_str_radix(hi, 16)
        .map_err(|_| PgError::Protocol(format!("malformed LSN hi: {s}")))?;
    let lo = u64::from_str_radix(lo, 16)
        .map_err(|_| PgError::Protocol(format!("malformed LSN lo: {s}")))?;
    Ok(Lsn((hi << 32) | lo))
}

// Workaround: `DecodedMessage` is `Send` but our trait method's box
// type also needs to be Send. The compiler enforces that, but if it
// ever complains, we'd add `+ Send` to the dyn here. Documented for
// future readers.
const _: () = {
    fn _assert<T: Send>() {}
    fn _assert_decoded() {
        _assert::<DecodedMessage>();
    }
};

#[cfg(test)]
mod tests {
    use super::*;

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

    #[test]
    fn lsn_round_trip() {
        let cases = [
            (0u64, "0/0"),
            (1, "0/1"),
            (0x1234_5678_9ABC_DEF0, "12345678/9ABCDEF0"),
        ];
        for (n, s) in cases {
            assert_eq!(format_lsn(Lsn(n)), s);
            assert_eq!(parse_lsn(s).unwrap().0, n);
        }
    }

    #[test]
    fn parse_lsn_rejects_garbage() {
        assert!(parse_lsn("oops").is_err());
        assert!(parse_lsn("/0").is_err());
        assert!(parse_lsn("XYZ/0").is_err());
    }
}
