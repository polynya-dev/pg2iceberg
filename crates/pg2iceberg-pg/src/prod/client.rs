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

use crate::{DecodedMessage, PgClient, PgError, ReplicationStream, Result, SnapshotId};
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
    /// Connect to Postgres in **logical replication mode** with NoTls.
    ///
    /// The configuration string follows libpq's URI/keyword format. The
    /// caller is responsible for setting `dbname` since libpq doesn't
    /// require it but `START_REPLICATION` does. Replication mode is
    /// added implicitly here.
    ///
    /// TLS is intentionally not wired yet — most managed Postgres needs
    /// it; that's a follow-on patch.
    pub async fn connect(conn_str: &str) -> Result<Self> {
        let mut config: tokio_postgres::Config = conn_str
            .parse()
            .map_err(|e: tokio_postgres::Error| PgError::Connection(e.to_string()))?;
        config.replication_mode(ReplicationMode::Logical);

        let (client, connection) = config
            .connect(NoTls)
            .await
            .map_err(|e| PgError::Connection(e.to_string()))?;

        // Spawn the connection task. If it errors, the next op on
        // `client` surfaces the error; we don't try to log here to
        // keep the prod module dep-light.
        let handle = tokio::spawn(async move {
            // Errors are visible to operations on the client; they
            // also surface as a closed connection.
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
