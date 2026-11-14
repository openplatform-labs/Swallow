//! Shared PostgreSQL TLS configuration for experimental services.
use std::{
    error::Error,
    fs::File,
    io::{BufReader, Read},
    path::Path,
    sync::Arc,
};
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
    let certificate = std::env::var_os("NOVU_POSTGRES_CLIENT_CERT_FILE");
    let key = std::env::var_os("NOVU_POSTGRES_CLIENT_KEY_FILE");
    if certificate.is_some() != key.is_some() || (insecure && certificate.is_some()) {
        return Err("invalid PostgreSQL client certificate configuration".into());
    }
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
    let builder = rustls::ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()?
        .with_root_certificates(roots);
    let tls = match (certificate, key) {
        (Some(cert_path), Some(key_path)) => {
            // Bounded reads and generic errors avoid exposing paths or key material.
            let cert_bytes = read_identity(Path::new(&cert_path))?;
            let key_bytes = read_identity(Path::new(&key_path))?;
            let certs = rustls_pemfile::certs(&mut cert_bytes.as_slice())
                .collect::<Result<Vec<_>, _>>()
                .map_err(|_| "invalid PostgreSQL client certificate")?;
            if certs.is_empty() {
                return Err("invalid PostgreSQL client certificate".into());
            }
            let private_key = rustls_pemfile::private_key(&mut key_bytes.as_slice())
                .map_err(|_| "invalid PostgreSQL client key")?
                .ok_or("invalid PostgreSQL client key")?;
            builder
                .with_client_auth_cert(certs, private_key)
                .map_err(|_| "invalid PostgreSQL client identity")?
        }
        (None, None) => builder.with_no_client_auth(),
        _ => unreachable!(),
    };
    Ok((config, MakeRustlsConnect::new(tls)))
}

fn read_identity(path: &Path) -> Result<Vec<u8>, Box<dyn Error + Send + Sync>> {
    const MAX_BYTES: u64 = 1_048_576;
    let mut bytes = Vec::new();
    File::open(path)
        .map_err(|_| "PostgreSQL client identity file unavailable")?
        .take(MAX_BYTES + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| "PostgreSQL client identity file unavailable")?;
    if bytes.is_empty() || bytes.len() as u64 > MAX_BYTES {
        return Err("invalid PostgreSQL client identity file".into());
    }
    Ok(bytes)
}
