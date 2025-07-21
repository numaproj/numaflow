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

    #[tokio::test]
    async fn test_configure_tls_insecure_skip_verify() {
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
        let tls_config = TlsConfig {
            insecure_skip_verify: true,
            ca_cert: None,
            client_auth: None,
        };

        let conn_opts = ConnectOptions::new();
        let result = configure_tls(conn_opts, tls_config);

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_configure_tls_invalid_ca_cert() {
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
        let tls_config = TlsConfig {
            insecure_skip_verify: false,
            ca_cert: Some("-----BEGIN CERTIFICATE-----\n...".to_string()),
            client_auth: None,
        };

        let conn_opts = ConnectOptions::new();
        let result = configure_tls(conn_opts, tls_config);

        assert!(result.is_err());
        let err = result.unwrap_err();
        let Error::Other(cert_err) = err else {
            panic!("unexpected error variant");
        };
        assert!(cert_err.starts_with("Parsing CA cert"));
    }

    #[tokio::test]
    async fn test_configure_tls_with_client_auth() {
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
        let tls_config = TlsConfig {
            insecure_skip_verify: false,
            ca_cert: Some(
                r#"-----BEGIN CERTIFICATE-----
MIIEmTCCAwGgAwIBAgIQOnQDenOU6LzG5RbfGNGlkjANBgkqhkiG9w0BAQsFADBl
MR4wHAYDVQQKExVta2NlcnQgZGV2ZWxvcG1lbnQgQ0ExHTAbBgNVBAsMFHJvb3RA
YnVpbGRraXRzYW5kYm94MSQwIgYDVQQDDBtta2NlcnQgcm9vdEBidWlsZGtpdHNh
bmRib3gwHhcNMjUwMzI1MDkyMTM1WhcNMzUwMzI1MDkyMTM1WjBlMR4wHAYDVQQK
ExVta2NlcnQgZGV2ZWxvcG1lbnQgQ0ExHTAbBgNVBAsMFHJvb3RAYnVpbGRraXRz
YW5kYm94MSQwIgYDVQQDDBtta2NlcnQgcm9vdEBidWlsZGtpdHNhbmRib3gwggGi
MA0GCSqGSIb3DQEBAQUAA4IBjwAwggGKAoIBgQDPWLCmd0BW0Ht7xin/DdGig6ak
Q00IaWnFOL5ZkjOFGq19nObepztwUELtTu04Up0OdS9gJMGnlZWnDAfMxL7BeyGP
WQ6kkph9c58xAWzcNT3LJPbLvhozuvCF8epaK8fJjyUH2ZOkuUyxbWxGHotrcuqg
12l/kv45gCYkiRcpw1np05JNr0LfHTtB2bC6L+rT9XkhcCTgT6Jn6mAV7DrTAFE4
itrB+7A3unsaetBFH8psHWxfUb0wEBCw1D0jiAwd6gyvKWgzdvRyyLofSAXECTt2
8PcA+3LnAgIm9KlpIxDUcM19hW8k4dm2uSINnZtOQUPO+p9lb8dobnIkGfzaLMb8
fKas+A24B17IyN7Pd1oEbx8CdLA1/hFeoR/rOMuZZG+DPP2au19+J2irz9YYaQSU
d0PMlMhdex5HFSKx0VwXbm0Q5LL4l6D+RYAEFxNeq0NL/GdxEg6wn01rh3sQdPns
WmLeY2KJKD/GABZhGoDsfNt2V6bXDG8gE3RyYvMCAwEAAaNFMEMwDgYDVR0PAQH/
BAQDAgIEMBIGA1UdEwEB/wQIMAYBAf8CAQAwHQYDVR0OBBYEFDrbgKJvJRAgdVwc
hLoJAJbpJbxWMA0GCSqGSIb3DQEBCwUAA4IBgQAbkgEOuQPxmm7UsU84PXihHYPQ
pLBTSO0NIlESwJxpSAg04UxjTwQepPWvriYHTYunc3LKknjbplm6SZpV+s0ofhxV
vP0eO6NH/Ip5bdWWC1IrILbsL6l2ceNutQDWlvXUMmKETuTbT4TmXS/TB9O4ZtVr
1awcc6jvB0Ds+cnd7AZZPMQfPXQXUN6roPF1rIamXU3qrN2kAanGDzNmR740Lnfb
WeTIHnjMq4Pllo14TbDK0YNpCtuoneBERp40JOlOKPa+sWH2AFG0STiXuoY0m9TN
XguD4005MJq0gi3uy2HUdO1L0v8bYzqwvbrlCdyTrcsE7gUjiWUsicAYISfVOj7H
9h89s7CHH3I3CWFNvAFWV7Frug9iqKyxYr7+0eGpx2NR+4cdS4co3C+G6NjlE2ET
aVpDuM0QKJTM/Xs+h0htNcosO1ti7fNAjv4z5DZO635hP4Y3QqsD8aELcihP0CBq
5mXTljzZ334dfFcyLlbWZrLPHpcCWbWGQYdIdF4=
-----END CERTIFICATE-----"#
                    .into(),
            ),
            client_auth: Some(TlsClientAuthCerts {
                client_cert: "-----BEGIN CERTIFICATE-----
MIIETjCCAragAwIBAgIRAKmqnT/hu7Ewsl5V4Le6RdUwDQYJKoZIhvcNAQELBQAw
ZTEeMBwGA1UEChMVbWtjZXJ0IGRldmVsb3BtZW50IENBMR0wGwYDVQQLDBRyb290
QGJ1aWxka2l0c2FuZGJveDEkMCIGA1UEAwwbbWtjZXJ0IHJvb3RAYnVpbGRraXRz
YW5kYm94MB4XDTI1MDMyNTExMzMxMFoXDTI3MDYyNTExMzMxMFowSDEnMCUGA1UE
ChMebWtjZXJ0IGRldmVsb3BtZW50IGNlcnRpZmljYXRlMR0wGwYDVQQLDBRyb290
QGJ1aWxka2l0c2FuZGJveDCCASIwDQYJKoZIhvcNAQEBBQADggEPADCCAQoCggEB
AJuqarht8aU3oY0rUKLPxsAXgRUkby2I9nxpLiVvMX3z8uOR7k8peiJ0NgOWXsRb
fD681AB/W/6aUcnV1FHMkF2SlAUNr+iyyQ7JUr26/r0cgFcfLO+fRY++qZzLrAMG
LLSzoI3/m7dbI7WZYBwBoXwv0QyZk4H1TL1E8IkCJ5yLEprFZEBh2UX437KNu3gj
ipwrk+IC3ehT9G9r0udpU3TTlk0uS9GcHVOkC4khNMkXCAGjRcdZOWfDJXKYfR2a
QE4a+OYeEvOZJgdzxKK4D5AFLa5z1bbOfpKzFixGnsrNBVVEOURajVv6rRhfe938
98i1w3KWSHlIruhXeHAUWOUCAwEAAaOBlTCBkjAOBgNVHQ8BAf8EBAMCBaAwJwYD
VR0lBCAwHgYIKwYBBQUHAwIGCCsGAQUFBwMBBggrBgEFBQcDBDAfBgNVHSMEGDAW
gBQ624CibyUQIHVcHIS6CQCW6SW8VjA2BgNVHREELzAtgglsb2NhbGhvc3SCD25h
dHMtc2VydmVyLXRsc4EPZW1haWxAbG9jYWxob3N0MA0GCSqGSIb3DQEBCwUAA4IB
gQCtzNu/i5q2zHpnrCXSL+1Dd8Sc7l0hGA0bUX2sgAJoo2mOywOAH2xNHac5Y2Wm
1ZXxlim0DfYgnPEbPO14CiRHN3Ho2lJqkDWvPjUnudLO4R9fqtKqMLWjqggYLhvY
35h3Eou8xxXTzlNgm8y/4DzPwZt+Ta+4AmT2niYHhYl6aQTCfBkBTuy4+OdE9cgc
w1Zd4hiRUXiydoZnaNdis/7v6yL+sWB8mP0HYY4qpq0FSQxSDsXaVnfW9Ly4vUu0
/KryRQbnGbCKDSP8YycEgRPrmZoQmbwGY21L3RWw+6cM+t9cjWaw5bnlo6rH2nqM
pD7OKMmhWWFyHIOJlP55bT23I8+KzMpfEb8TkXeSLPj1Kp//vapuWO5++dat5vLc
7uA4lagnnbG9Z0T0YOvMUv2n/+RV9UpfyzATUVCWih8zCrpW3EJDRHsHq3blgUdB
dl84hz7cZ26/EMHN4OaQmFvr5Haqm8SNn9YiUPDQWKfcqAQtts/+F0TFbndwX/H/
qaw=
-----END CERTIFICATE-----"
                    .to_string(),
                client_cert_private_key: "-----BEGIN PRIVATE KEY-----
MIIEvwIBADANBgkqhkiG9w0BAQEFAASCBKkwggSlAgEAAoIBAQCbqmq4bfGlN6GN
K1Ciz8bAF4EVJG8tiPZ8aS4lbzF98/Ljke5PKXoidDYDll7EW3w+vNQAf1v+mlHJ
1dRRzJBdkpQFDa/osskOyVK9uv69HIBXHyzvn0WPvqmcy6wDBiy0s6CN/5u3WyO1
mWAcAaF8L9EMmZOB9Uy9RPCJAiecixKaxWRAYdlF+N+yjbt4I4qcK5PiAt3oU/Rv
a9LnaVN005ZNLkvRnB1TpAuJITTJFwgBo0XHWTlnwyVymH0dmkBOGvjmHhLzmSYH
c8SiuA+QBS2uc9W2zn6SsxYsRp7KzQVVRDlEWo1b+q0YX3vd/PfItcNylkh5SK7o
V3hwFFjlAgMBAAECggEBAITmKdbrhrMXm8V6hY169qRpbLswL/sgQ9BTvCIZnJd7
/pJrtJnBP5TKwpKtfpsFwD2P2S/TjCcCeXFoLazoiRhaXCU0WkrA2QbWut1hGLs7
6hZ3d1XdFPZ0QTqsaF7slp5v/VM+NyODnCkemP9QJR5xdHjdZSI+2xV8Fh/ixw7O
GGJ9IBKHHaQeXWFh/fpb2zAvOs0vZQSv+jOV+bze3fE0AI7XPYKt8Sa54KPBhYls
aHsHXznhnm19cSXW/x+cloWhJu3FXKzQltxxiHCaJrgYWI8mp40MYQ225RkcyhJ4
uWTpuho6ePqlySdeq7zw0r4jRRjCy6xE7TPdX+8XjsECgYEAyZ3mM+htcNy2H9dm
XUFf9qxPE8Lx5t6Jen4vczZ+BK0hCPB1NXJxtmVULGEb051/0z2eQLk7IxZpfuXq
bhR9qDNcqzeVNPILnPjFudxk/YlbltpyMo3nN9U/vwRa0i/KXnbRTKkfCdOM/2XZ
/lzIJfQ8rS603PdXmfCHrUOr2g8CgYEAxad9J6I5u5g0LLbGAesY+8K4ESh2kHVh
W7EuHGbxMBtKFGndRITjJ85qxxUfw6eqTddYuFR22Pm98tCT1BMpFZGYGxoEdrP3
qqwbUNKcdk6zeAwooBF1q397dBJ2lR55joCVxk0/YQeXm6Dqkq5j0bA1biFKmJtE
o63An2mlocsCgYB7YAyGpyyRa/5m7cDOQDshD8A0L48n3/Xw51bSAf6LjgYxGjQf
SLEdFFS185a7oB2gfoxgvvjZN9XGuZsDUbazPvruK0064QMKQ5F7csq5+1v4rCRF
m2BqYixoD5okFOqZc8wQRU2hDbuybflAFjbEQvj+YR58OT96DB56gHahMQKBgQCW
D175pDRotFmISQtzkWXaXi8Y97tsWXGdB5uWfKFIgK9xaB5RUwKSyihPFT6UcMrf
Zks5RwckHBeWLbzOGe3rLippCQuyg1fY/+mNJxkayQ2Aatq9DARmO9cifJIDDKwF
AKK1dxhTNkxoH3d3/WZTYJBwGF5mFhu6mMPRQ4g4mQKBgQDCCiHGuBUDscDiLbSq
fn0n2cmujcxhFYzHvE9ahNTBpJErEsLG0RSp77ZK5gETYUvmuI0BrfAYgU/xaGCz
t0Rqav1kVzZH9j/hd0tavIIS1kfDL/whj/UdVeZjTiIEY9AZLcuwUHOcQesWursk
XdvExDsAdjbkBG7ynn9pmMgIJg==
-----END PRIVATE KEY-----"
                    .to_string(),
            }),
        };

        let conn_opts = ConnectOptions::new();
        let result = configure_tls(conn_opts, tls_config);

        assert!(result.is_ok());
    }
}
