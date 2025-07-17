use crate::Error;
use crate::Result as NatsResult;
use crate::TlsClientAuthCerts;
use crate::TlsConfig;
use async_nats::ConnectOptions;
use async_nats::rustls::client::danger::{
    HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier,
};
use async_nats::rustls::{DigitallySignedStruct, Error as TLSError, SignatureScheme};
use rustls_pki_types::pem::PemObject;
use rustls_pki_types::{CertificateDer, PrivateKeyDer};
use rustls_pki_types::{ServerName, UnixTime};
use std::sync::Arc;

#[derive(Debug)]
pub(crate) struct NoVerifier;

impl ServerCertVerifier for NoVerifier {
    fn verify_server_cert(
        &self,
        _end_entity: &rustls_pki_types::CertificateDer<'_>,
        _intermediates: &[rustls_pki_types::CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, TLSError> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &rustls_pki_types::CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, TLSError> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &rustls_pki_types::CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, TLSError> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        vec![
            SignatureScheme::RSA_PKCS1_SHA1,
            SignatureScheme::ECDSA_SHA1_Legacy,
            SignatureScheme::RSA_PKCS1_SHA256,
            SignatureScheme::ECDSA_NISTP256_SHA256,
            SignatureScheme::RSA_PKCS1_SHA384,
            SignatureScheme::ECDSA_NISTP384_SHA384,
            SignatureScheme::RSA_PKCS1_SHA512,
            SignatureScheme::ECDSA_NISTP521_SHA512,
            SignatureScheme::RSA_PSS_SHA256,
            SignatureScheme::RSA_PSS_SHA384,
            SignatureScheme::RSA_PSS_SHA512,
            SignatureScheme::ED25519,
            SignatureScheme::ED448,
        ]
    }
}

pub(crate) fn configure_tls(
    mut conn_opts: ConnectOptions,
    tls_config: TlsConfig,
) -> NatsResult<ConnectOptions> {
    if tls_config.insecure_skip_verify {
        tracing::warn!(
            "'insecureSkipVerify' is set to true, certificate validation will not be performed when connecting to NATS server"
        );
        let tls_client_config = rustls::ClientConfig::builder()
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(NoVerifier))
            .with_no_client_auth();
        conn_opts = conn_opts
            .require_tls(true)
            .tls_client_config(tls_client_config);
    } else {
        let root_store = load_root_store(tls_config.ca_cert)?;
        let tls_client = configure_client_auth(tls_config.client_auth, root_store)?;
        conn_opts = conn_opts.require_tls(true).tls_client_config(tls_client);
    }
    Ok(conn_opts)
}

fn load_root_store(ca_cert: Option<String>) -> NatsResult<rustls::RootCertStore> {
    let mut root_store = rustls::RootCertStore::empty();
    let native_certs = rustls_native_certs::load_native_certs();
    if !native_certs.errors.is_empty() {
        return Err(Error::Other(format!(
            "Loading native certs from certificate store: {:?}",
            native_certs.errors
        )));
    }
    root_store.add_parsable_certificates(native_certs.unwrap());
    if let Some(ca_cert) = ca_cert {
        let cert = CertificateDer::from_pem_slice(ca_cert.as_bytes())
            .map_err(|err| Error::Other(format!("Parsing CA cert: {err:?}")))?;
        root_store.add(cert).map_err(|err| {
            Error::Other(format!("Adding CA cert to in-memory cert store: {err:?}"))
        })?;
    }
    Ok(root_store)
}

fn configure_client_auth(
    client_auth: Option<TlsClientAuthCerts>,
    root_store: rustls::RootCertStore,
) -> NatsResult<rustls::ClientConfig> {
    match client_auth {
        Some(client_auth) => {
            let client_cert = CertificateDer::from_pem_slice(client_auth.client_cert.as_bytes())
                .map_err(|err| Error::Other(format!("Parsing client tls certificate: {err:?}")))?;
            let client_key =
                PrivateKeyDer::from_pem_slice(client_auth.client_cert_private_key.as_bytes())
                    .map_err(|err| {
                        Error::Other(format!("Parsing client tls private key: {err:?}"))
                    })?;
            rustls::ClientConfig::builder()
                .with_root_certificates(root_store)
                .with_client_auth_cert(vec![client_cert], client_key)
                .map_err(|err| Error::Other(format!("Client TLS private key is invalid: {err:?}")))
        }
        None => Ok(rustls::ClientConfig::builder()
            .with_root_certificates(root_store)
            .with_no_client_auth()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_nats::rustls::SignatureScheme;
    use rustls_pki_types::{CertificateDer, UnixTime};
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn test_verify_server_cert() {
        let verifier = NoVerifier;
        let cert = CertificateDer::from(vec![]);
        let intermediates = vec![];
        let server_name = ServerName::try_from("localhost").unwrap();
        let ocsp_response = vec![];
        let now = UnixTime::since_unix_epoch(SystemTime::now().duration_since(UNIX_EPOCH).unwrap());

        let result =
            verifier.verify_server_cert(&cert, &intermediates, &server_name, &ocsp_response, now);
        assert!(result.is_ok());
    }

    #[test]
    fn test_supported_verify_schemes() {
        let verifier = NoVerifier;
        let schemes = verifier.supported_verify_schemes();
        assert!(schemes.contains(&SignatureScheme::RSA_PKCS1_SHA256));
        assert!(schemes.contains(&SignatureScheme::ECDSA_NISTP256_SHA256));
        assert!(schemes.contains(&SignatureScheme::ED25519));
    }
}
