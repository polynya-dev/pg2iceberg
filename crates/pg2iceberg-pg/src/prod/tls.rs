//! TLS configuration for the prod Postgres clients.
//!
//! We expose two modes:
//!
//! - [`TlsMode::Disable`] — `NoTls`. Local Postgres without
//!   `ssl = on`; never use against managed PG.
//! - [`TlsMode::Webpki`] — server-cert verification against the
//!   Mozilla `webpki-roots` bundle. Covers AWS RDS / Aurora,
//!   Supabase, Cloud SQL, Azure Database, Neon, etc.
//!
//! Either way with TLS, SCRAM channel binding works: `tokio-postgres-rustls`
//! gives tokio-postgres the server certificate's hash
//! (`tls-server-end-point`), which `channel_binding=require` — in Neon's
//! connection strings, for one — insists on. (0.13 never did: it parsed
//! the certificate as its inner TBS structure, which always failed.)
//!
//! Custom CA bundles and mTLS are deferred. They land in this module when
//! we wire a deployment that needs them.
//!
//! Why `ring` and not `aws-lc-rs`: we enable `tokio-postgres-rustls`'s
//! `ring` feature, so anything we install for rustls has to match.

use crate::PgError;

/// TLS mode for the prod PG client.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum TlsMode {
    /// No TLS. Plaintext socket. Use only against a local PG that has
    /// `ssl = off`.
    #[default]
    Disable,
    /// Server-cert verification against Mozilla `webpki-roots`. Most
    /// managed Postgres deployments work out of the box with this.
    Webpki,
}

/// Build a `MakeRustlsConnect` trusting the Mozilla webpki roots.
pub(crate) fn build_rustls_connector() -> crate::Result<tokio_postgres_rustls::MakeRustlsConnect> {
    let mut roots = rustls::RootCertStore::empty();
    roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    Ok(connector_trusting(roots))
}

fn connector_trusting(roots: rustls::RootCertStore) -> tokio_postgres_rustls::MakeRustlsConnect {
    // Install the ring crypto provider lazily. `install_default` returns
    // Err if a provider is already installed, which we treat as "fine,
    // somebody else got there first."
    let _ = rustls::crypto::ring::default_provider().install_default();

    let config = rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    tokio_postgres_rustls::MakeRustlsConnect::new(config)
}

impl TlsMode {
    /// Parse from a config string (`"disable" | "webpki"`).
    pub fn parse(s: &str) -> Result<Self, PgError> {
        match s.to_ascii_lowercase().as_str() {
            "disable" | "off" | "false" => Ok(Self::Disable),
            "webpki" | "on" | "true" => Ok(Self::Webpki),
            other => Err(PgError::Other(format!(
                "unknown tls mode {other:?}; expected one of: disable, webpki"
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rustls::pki_types::pem::PemObject;
    use rustls::pki_types::{CertificateDer, PrivateKeyDer};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    /// A Postgres server, as far as offering SCRAM-SHA-256-PLUS over TLS:
    /// the mechanism the client picks, or `None` if it hangs up first.
    async fn sasl_mechanism_chosen(listener: tokio::net::TcpListener) -> Option<String> {
        let cert =
            CertificateDer::from_pem_slice(include_bytes!("testdata/localhost.pem")).unwrap();
        let key = PrivateKeyDer::from_pem_slice(include_bytes!("testdata/localhost.key")).unwrap();
        let config = rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(vec![cert], key)
            .unwrap();
        let (mut tcp, _) = listener.accept().await.unwrap();
        let mut ssl_request = [0u8; 8];
        tcp.read_exact(&mut ssl_request).await.unwrap();
        tcp.write_all(b"S").await.unwrap();
        let acceptor = tokio_rustls::TlsAcceptor::from(std::sync::Arc::new(config));
        let mut tls = acceptor.accept(tcp).await.unwrap();

        let len = tls.read_i32().await.unwrap();
        let mut startup = vec![0u8; len as usize - 4];
        tls.read_exact(&mut startup).await.unwrap();

        let mechanisms = b"SCRAM-SHA-256-PLUS\0SCRAM-SHA-256\0\0";
        let mut auth = vec![b'R'];
        auth.extend_from_slice(&(8 + mechanisms.len() as i32).to_be_bytes());
        auth.extend_from_slice(&10i32.to_be_bytes()); // AuthenticationSASL
        auth.extend_from_slice(mechanisms);
        tls.write_all(&auth).await.unwrap();
        tls.flush().await.unwrap();

        // SASLInitialResponse: 'p', length, the mechanism's name, ...
        if tls.read_u8().await.ok()? != b'p' {
            return None;
        }
        let len = tls.read_i32().await.ok()?;
        let mut body = vec![0u8; len as usize - 4];
        tls.read_exact(&mut body).await.ok()?;
        let name = body.split(|b| *b == 0).next()?;
        Some(String::from_utf8_lossy(name).into_owned())
    }

    /// `channel_binding=require` — in the connection strings Neon hands
    /// out — needs the TLS layer to give tokio-postgres the server
    /// certificate's hash, or it refuses: "server did not use channel
    /// binding".
    #[tokio::test]
    async fn the_connector_supports_channel_binding() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = tokio::spawn(sasl_mechanism_chosen(listener));

        let mut roots = rustls::RootCertStore::empty();
        roots
            .add(CertificateDer::from_pem_slice(include_bytes!("testdata/ca.pem")).unwrap())
            .unwrap();
        let dsn = format!(
            "host=localhost hostaddr=127.0.0.1 port={port} user=u password=p dbname=d \
             sslmode=require channel_binding=require"
        );
        let client = tokio_postgres::connect(&dsn, connector_trusting(roots)).await;

        let chosen = server.await.unwrap();
        assert_eq!(
            chosen.as_deref(),
            Some("SCRAM-SHA-256-PLUS"),
            "client: {:?}",
            client.err()
        );
    }

    #[test]
    fn parse_known_modes() {
        assert_eq!(TlsMode::parse("disable").unwrap(), TlsMode::Disable);
        assert_eq!(TlsMode::parse("DISABLE").unwrap(), TlsMode::Disable);
        assert_eq!(TlsMode::parse("webpki").unwrap(), TlsMode::Webpki);
        assert_eq!(TlsMode::parse("on").unwrap(), TlsMode::Webpki);
    }

    #[test]
    fn parse_unknown_errors() {
        assert!(TlsMode::parse("bogus").is_err());
    }

    #[test]
    fn build_connector_succeeds() {
        // Smoke test that the rustls + webpki-roots build chain
        // compiles and returns a usable connector. We don't actually
        // connect — that needs a live PG.
        let _ = build_rustls_connector().unwrap();
    }
}
