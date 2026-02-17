//! TLS configuration for the daemon server.

use crate::error::{Error, Result};
use axum_server::tls_rustls::RustlsConfig;
use rcgen::{
    CertificateParams, DistinguishedName, DnType, ExtendedKeyUsagePurpose, KeyPair, KeyUsagePurpose,
};
use time::{Duration, OffsetDateTime};

/// Builds a self-signed TLS config for the daemon (same port for HTTP/1.1 and gRPC over h2).
/// ALPN is handled by the server stack; both protocols are advertised.
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

    RustlsConfig::from_pem(cert_pem.into(), key_pem.into())
        .await
        .map_err(|e| Error::TlsConfiguration(format!("Failed to build RustlsConfig: {}", e)))
}
