//! Connection helper for [`super::PostgresCoordinator`].
//!
//! Symmetric to `pg2iceberg-pg/src/prod/client.rs`'s `connect()` but
//! **without** logical-replication mode — the coordinator uses the
//! extended query protocol (parameterized queries, transactions) which
//! isn't available in replication-mode connections.
//!
//! Supports either plaintext (`TlsMode::Disable`) or rustls + Mozilla
//! webpki roots (`TlsMode::Webpki`), with SCRAM channel binding (see
//! `pg2iceberg_pg`'s TLS module). Custom CA bundles and mTLS are
//! deferred.

use crate::CoordError;
use tokio::task::AbortHandle;
use tokio_postgres::{Client, NoTls};

/// TLS mode for the coord connection. Mirrors the surface of
/// `pg2iceberg_pg::prod::TlsMode`.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum TlsMode {
    /// `NoTls`. Use only against a local Postgres without `ssl = on`.
    #[default]
    Disable,
    /// Server-cert verification against Mozilla `webpki-roots`.
    Webpki,
}

impl TlsMode {
    pub fn parse(s: &str) -> Result<Self, CoordError> {
        match s.to_ascii_lowercase().as_str() {
            "disable" | "off" | "false" => Ok(Self::Disable),
            "webpki" | "on" | "true" => Ok(Self::Webpki),
            other => Err(CoordError::Other(format!(
                "unknown tls mode {other:?}; expected one of: disable, webpki"
            ))),
        }
    }
}

pub struct PgConn {
    pub client: Client,
    pub abort: AbortHandle,
    /// What it connected with, to [`Self::reopen`] it.
    conn_str: String,
    tls: TlsMode,
}

impl PgConn {
    /// A new connection to the same server.
    pub async fn reopen(&self) -> Result<PgConn, CoordError> {
        connect_with(&self.conn_str, self.tls).await
    }
}

/// Backwards-compatible NoTls connect, kept for callers that don't
/// care about TLS yet (the existing `migrate-coord` smoke flow).
pub async fn connect(conn_str: &str) -> Result<PgConn, CoordError> {
    connect_with(conn_str, TlsMode::Disable).await
}

/// Open a regular-mode `tokio_postgres::Client` to `conn_str` using
/// the configured [`TlsMode`].
pub async fn connect_with(conn_str: &str, tls: TlsMode) -> Result<PgConn, CoordError> {
    match tls {
        TlsMode::Disable => finish(conn_str, tls, NoTls).await,
        TlsMode::Webpki => {
            let connector = build_rustls_connector()?;
            finish(conn_str, tls, connector).await
        }
    }
}

async fn finish<T>(conn_str: &str, mode: TlsMode, tls: T) -> Result<PgConn, CoordError>
where
    T: tokio_postgres::tls::MakeTlsConnect<tokio_postgres::Socket> + Send + 'static,
    T::Stream: Send,
    T::TlsConnect: Send,
    <T::TlsConnect as tokio_postgres::tls::TlsConnect<tokio_postgres::Socket>>::Future: Send,
{
    let (client, connection) = tokio_postgres::connect(conn_str, tls)
        .await
        .map_err(|e| CoordError::Pg(e.to_string()))?;
    let handle = tokio::spawn(async move {
        let _ = connection.await;
    });
    Ok(PgConn {
        client,
        abort: handle.abort_handle(),
        conn_str: conn_str.to_string(),
        tls: mode,
    })
}

fn build_rustls_connector() -> Result<tokio_postgres_rustls::MakeRustlsConnect, CoordError> {
    // `install_default` is idempotent across calls — Err just means a
    // provider is already installed (e.g. by pg2iceberg-pg).
    let _ = rustls::crypto::ring::default_provider().install_default();

    let mut roots = rustls::RootCertStore::empty();
    roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    let config = rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    Ok(tokio_postgres_rustls::MakeRustlsConnect::new(config))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_known_modes() {
        assert_eq!(TlsMode::parse("disable").unwrap(), TlsMode::Disable);
        assert_eq!(TlsMode::parse("webpki").unwrap(), TlsMode::Webpki);
        assert!(TlsMode::parse("bogus").is_err());
    }

    #[test]
    fn build_connector_succeeds() {
        let _ = build_rustls_connector().unwrap();
    }
}
