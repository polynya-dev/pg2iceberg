//! [`PgClientImpl`]: production [`PgClient`] backed by `tokio-postgres`
//! in **logical replication mode**.
//!
//! Replication mode supports a subset of SQL via the simple-query
//! protocol — enough for the catalog-style queries we need
//! (`CREATE PUBLICATION`, slot CRUD, `pg_export_snapshot`) plus the
//! `START_REPLICATION` / `CREATE_REPLICATION_SLOT` replication commands
//! that aren't available in regular mode. Following Supabase etl's
//! design, we keep a single replication-mode connection per pipeline
//! rather than juggling two clients.
//!
//! # Connection lifetime
//!
//! `tokio-postgres` returns the `Connection` future separately from the
//! `Client`. We spawn the connection on a tokio task and store its
//! `AbortHandle` so dropping the [`PgClientImpl`] also tears down the
//! background task. If the connection errors, the next operation on
//! the client surfaces that error — we don't try to surface mid-call
//! disconnects through a side channel today.

use crate::prod::tls::{build_rustls_connector, TlsMode};
use crate::{
    DecodedMessage, PgClient, PgError, ReplicationStream, Result, SlotHealth, SnapshotId, WalStatus,
};
use async_trait::async_trait;
use pg2iceberg_core::{Lsn, TableIdent};
use postgres_replication::LogicalReplicationStream;
use tokio::task::AbortHandle;
use tokio_postgres::{config::ReplicationMode, Client, NoTls, SimpleQueryMessage};

/// Owns a `tokio_postgres::Client` configured for logical replication
/// plus the abort handle of the background connection task.
pub struct PgClientImpl {
    client: Client,
    _conn_abort: AbortHandle,
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

        match tls {
            TlsMode::Disable => Self::finish_connect(&config, NoTls).await,
            TlsMode::Webpki => {
                let connector = build_rustls_connector()?;
                Self::finish_connect(&config, connector).await
            }
        }
    }

    async fn finish_connect<T>(config: &tokio_postgres::Config, tls: T) -> Result<Self>
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
            _conn_abort: handle.abort_handle(),
        })
    }
}

impl Drop for PgClientImpl {
    fn drop(&mut self) {
        self._conn_abort.abort();
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
        super::discover::discover_schema(&self.client, schema, table).await
    }
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
        let q = format!(
            "CREATE PUBLICATION {} FOR TABLE {}",
            quote_ident(name),
            table_list
        );
        simple_exec(&self.client, &q).await
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
            .client
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
            .client
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
            .client
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
            .client
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
        // We use `to_jsonb(pg_replication_slots.*)` for `wal_status`,
        // `safe_wal_size`, and `conflicting` so that PG versions that
        // don't have those columns return NULL instead of a parse
        // error. Specifically:
        //   - PG ≤ 12: no `wal_status` / `safe_wal_size`.
        //   - PG ≤ 13: no `conflicting`.
        // The base columns (`restart_lsn`, `confirmed_flush_lsn`)
        // exist on every supported PG version, so we read them
        // directly. Result: a single SQL works on PG 12+; older
        // versions just surface the new fields as None/false.
        let q = format!(
            "SELECT \
                restart_lsn::text AS restart_lsn, \
                confirmed_flush_lsn::text AS confirmed_flush_lsn, \
                (to_jsonb(pg_replication_slots.*) ->> 'wal_status') AS wal_status, \
                NULLIF(to_jsonb(pg_replication_slots.*) ->> 'safe_wal_size', '') AS safe_wal_size, \
                COALESCE((to_jsonb(pg_replication_slots.*) ->> 'conflicting')::boolean, false) AS conflicting \
             FROM pg_replication_slots \
             WHERE slot_name = {}",
            quote_lit(slot)
        );
        let rows = self
            .client
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
            .client
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
            .client
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
        self.client
            .simple_query("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
            .await
            .map_err(|e| PgError::Protocol(e.to_string()))?;
        let rows = self
            .client
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
            .client
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
        let copy_stream = self
            .client
            .copy_both_simple::<bytes::Bytes>(&q)
            .await
            .map_err(|e| PgError::Protocol(e.to_string()))?;
        Ok(Box::new(super::ReplicationStreamImpl::wrap(
            LogicalReplicationStream::new(copy_stream),
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
            .client
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
        simple_exec(&self.client, &q).await
    }

    async fn drop_publication(&self, name: &str) -> Result<()> {
        let q = format!("DROP PUBLICATION IF EXISTS {}", quote_ident(name));
        simple_exec(&self.client, &q).await
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
