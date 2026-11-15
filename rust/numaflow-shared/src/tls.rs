use secrecy::SecretString;

/// Client certificates presented for mutual TLS (mTLS).
#[derive(Debug, Clone)]
pub struct TlsClientAuthCerts {
    pub client_cert: SecretString,
    pub client_cert_private_key: SecretString,
}

/// TLS settings for connections that only authenticate the server (one-way TLS).
///
/// Use this for clients whose underlying transport cannot present a client
/// certificate. Connections that support mutual TLS should use
/// [`TlsConfigWithClientAuth`] instead.
#[derive(Debug, Clone)]
pub struct TlsConfig {
    /// Whether to skip server certificate verification.
    ///
    /// When set to `true`, the client will not verify the server's certificate.
    pub insecure_skip_verify: bool,
    /// CA certificate used to verify the server's certificate.
    /// Note: This will contain the actual certificate, not the path to the certificate.
    pub ca_cert: Option<SecretString>,
}

/// TLS settings for connections that may also authenticate the client (mTLS).
#[derive(Debug, Clone)]
pub struct TlsConfigWithClientAuth {
    /// Whether to skip server certificate verification.
    ///
    /// When set to `true`, the client will not verify the server's certificate.
    pub insecure_skip_verify: bool,
    /// CA certificate used to verify the server's certificate.
    /// Note: This will contain the actual certificate, not the path to the certificate.
    pub ca_cert: Option<SecretString>,
    /// Client authentication certificates for mutual TLS (mTLS).
    ///
    /// If `Some`, client authentication will be enabled using the provided certificates.
    /// If `None`, client authentication will be disabled.
    pub client_auth: Option<TlsClientAuthCerts>,
}

#[cfg(test)]
mod tests {
    use super::*;

    const CA_CERT: &str = "secret-ca-certificate-material";
    const CLIENT_CERT: &str = "secret-client-certificate-material";
    const CLIENT_KEY: &str = "secret-client-private-key-material";

    #[test]
    fn tls_config_debug_redacts_ca_cert() {
        let config = TlsConfig {
            insecure_skip_verify: false,
            ca_cert: Some(CA_CERT.into()),
        };
        for output in [format!("{config:?}"), format!("{config:#?}")] {
            assert!(!output.contains(CA_CERT));
            assert!(output.contains("REDACTED"));
            assert!(output.contains("insecure_skip_verify"));
        }
    }

    #[test]
    fn tls_config_with_client_auth_debug_redacts_every_secret() {
        let config = TlsConfigWithClientAuth {
            insecure_skip_verify: true,
            ca_cert: Some(CA_CERT.into()),
            client_auth: Some(TlsClientAuthCerts {
                client_cert: CLIENT_CERT.into(),
                client_cert_private_key: CLIENT_KEY.into(),
            }),
        };
        for output in [format!("{config:?}"), format!("{config:#?}")] {
            for secret in [CA_CERT, CLIENT_CERT, CLIENT_KEY] {
                assert!(!output.contains(secret));
            }
            assert!(output.contains("REDACTED"));
        }
    }
}
