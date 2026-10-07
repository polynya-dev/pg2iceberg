Test-only TLS material for `tls.rs`'s tests: a CA and a `localhost`
certificate it signed (ECDSA P-256, SHA-256), with the certificate's key.
Nothing trusts them outside those tests.
