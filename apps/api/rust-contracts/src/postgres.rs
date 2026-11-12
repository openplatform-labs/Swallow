//! Shared PostgreSQL TLS configuration for experimental services.
use std::{error::Error, fs::File, io::BufReader, sync::Arc};
use tokio_postgres::{Config, config::SslMode};
pub use tokio_postgres_rustls::MakeRustlsConnect;

/// Require certificate- and hostname-verified TLS unless insecure development
/// access is explicitly enabled. A PEM CA bundle can augment public roots.
pub fn configure(url: &str) -> Result<(Config, MakeRustlsConnect), Box<dyn Error + Send + Sync>> {
    let mut config: Config = url.parse()?;
    let insecure = std::env::var("NOVU_POSTGRES_ALLOW_INSECURE").as_deref() == Ok("true");
    config.ssl_mode(if insecure {
        SslMode::Disable
    } else {
        SslMode::Require
    });
    let mut roots = rustls::RootCertStore {
        roots: webpki_roots::TLS_SERVER_ROOTS.to_vec(),
    };
    if let Ok(path) = std::env::var("NOVU_POSTGRES_CA_FILE") {
        let mut reader = BufReader::new(File::open(path)?);
        let mut count = 0;
        for certificate in rustls_pemfile::certs(&mut reader) {
            roots.add(certificate?)?;
            count += 1;
        }
        if count == 0 {
            return Err("PostgreSQL CA file contains no certificates".into());
        }
    }
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let tls = rustls::ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()?
        .with_root_certificates(roots)
        .with_no_client_auth();
    Ok((config, MakeRustlsConnect::new(tls)))
}
