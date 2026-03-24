//! TLS configuration for the daemon server.

use std::sync::Arc;

use crate::error::{Error, Result};
use axum_server::tls_rustls::RustlsConfig;
use rcgen::{
    CertificateParams, DistinguishedName, DnType, ExtendedKeyUsagePurpose, KeyPair, KeyUsagePurpose,
};
use rustls::server::ServerConfig;
use rustls_pki_types::pem::PemObject;
use rustls_pki_types::{CertificateDer, PrivateKeyDer};
use time::{Duration, OffsetDateTime};

/// ALPN protocols: h2 for gRPC, http/1.1 for REST. Matches Go server NextProtos.
const ALPN_PROTOCOLS: &[&[u8]] = &[b"h2", b"http/1.1"];

/// Builds a self-signed TLS config for the daemon (same port for HTTP/1.1 and gRPC over h2).
/// ALPN is set to ["h2", "http/1.1"] to match the Go daemon server.
pub(crate) async fn build_rustls_config() -> Result<RustlsConfig> {
    let mut params = CertificateParams::new(vec!["localhost".to_string()]).map_err(|e| {
        Error::TlsConfiguration(format!("Failed to create certificate parameters: {}", e))
    })?;

    let mut dn = DistinguishedName::new();
    dn.push(DnType::OrganizationName, "Numaproj");
    params.distinguished_name = dn;

    let not_before = OffsetDateTime::now_utc();
    params.not_before = not_before;
    params.not_after = not_before + Duration::days(365);

    params.key_usages = vec![
        KeyUsagePurpose::KeyEncipherment,
        KeyUsagePurpose::DigitalSignature,
    ];
    params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];

    let signing_key = KeyPair::generate()
        .map_err(|e| Error::TlsConfiguration(format!("Failed to generate signing key: {}", e)))?;
    let cert = params.self_signed(&signing_key).map_err(|e| {
        Error::TlsConfiguration(format!("Failed to generate self-signed certificate: {}", e))
    })?;

    let cert_pem = cert.pem();
    let key_pem = signing_key.serialize_pem();

    let cert = CertificateDer::from_pem_slice(cert_pem.as_bytes())
        .map_err(|e| Error::TlsConfiguration(format!("Failed to parse certificate PEM: {}", e)))?;
    let certs = vec![cert];
    let key = PrivateKeyDer::from_pem_slice(key_pem.as_bytes())
        .map_err(|e| Error::TlsConfiguration(format!("Failed to parse key PEM: {}", e)))?;

    let mut server_config = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .map_err(|e| Error::TlsConfiguration(format!("Failed to build ServerConfig: {}", e)))?;

    server_config.alpn_protocols = ALPN_PROTOCOLS.iter().map(|p| p.to_vec()).collect();

    Ok(RustlsConfig::from_config(Arc::new(server_config)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn build_rustls_config_succeeds() {
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

        let result = build_rustls_config().await;
        assert!(
            result.is_ok(),
            "build_rustls_config should succeed: {:?}",
            result.err()
        );
    }
}
