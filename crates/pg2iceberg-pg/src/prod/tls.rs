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
//! Custom CA bundles, mTLS, and channel binding are deferred. They
//! land in this module when we wire a deployment that needs them.
//!
//! Why `ring` and not `aws-lc-rs`: `tokio-postgres-rustls` 0.13 hard-pins
//! the ring crypto provider, so anything we install for rustls has to
//! match. If we ever fork or upgrade beyond a release that switches
//! providers, swap the `default_provider()` call below.

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
    // Install the ring crypto provider lazily. `install_default` returns
    // Err if a provider is already installed, which we treat as "fine,
    // somebody else got there first."
    let _ = rustls::crypto::ring::default_provider().install_default();

    let mut roots = rustls::RootCertStore::empty();
    roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    let config = rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    Ok(tokio_postgres_rustls::MakeRustlsConnect::new(config))
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
