use rdkafka::config::ClientConfig;
use tracing::{error, warn};

pub mod sink;
pub mod source;

const KAFKA_TOPIC_HEADER_KEY: &str = "X-NF-Kafka-TopicName";

pub type Result<T> = core::result::Result<T, Error>;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Connecting to Kafka {server} - {error}")]
    Connection { server: String, error: String },

    #[error("Kafka - {0}")]
    Kafka(String),

    #[error("{0}")]
    Other(String),
}

/// Represents the SASL authentication mechanism for connecting to Kafka.
///
/// See the [Kafka Security documentation](https://kafka.apache.org/documentation/#security_sasl) for more details on each mechanism.
#[derive(Debug, Clone, PartialEq)]
pub enum KafkaSaslAuth {
    /// SASL/PLAIN authentication mechanism.
    ///
    /// See: <https://kafka.apache.org/documentation/#security_sasl_plain>
    ///
    /// Requires a username and password.
    Plain { username: String, password: String },
    /// SASL/SCRAM-SHA-256 authentication mechanism.
    ///
    /// See: <https://kafka.apache.org/documentation/#security_sasl_scram>
    ///
    /// Requires a username and password.
    ScramSha256 { username: String, password: String },
    /// SASL/SCRAM-SHA-512 authentication mechanism.
    ///
    /// See: <https://kafka.apache.org/documentation/#security_sasl_scram>
    ///
    /// Requires a username and password.
    ScramSha512 { username: String, password: String },
    /// SASL/GSSAPI (Kerberos) authentication mechanism.
    ///
    /// See: <https://kafka.apache.org/documentation/#security_sasl_gssapi>
    ///
    /// Requires Kerberos configuration and credentials.
    Gssapi {
        /// Kerberos service name (e.g., "kafka").
        service_name: String,
        /// Kerberos realm (optional, may be required by some setups).
        realm: String,
        /// Kerberos principal (username).
        username: String,
        /// Kerberos password (optional, if not using keytab).
        password: Option<String>,
        /// Path to the keytab file (optional).
        keytab: Option<String>,
        /// Path to the Kerberos configuration file (optional).
        kerberos_config: Option<String>,
        /// Authentication type (e.g., "kinit").
        auth_type: String,
    },
    /// SASL/OAUTHBEARER authentication mechanism.
    ///
    /// See: <https://kafka.apache.org/documentation/#security_sasl_oauthbearer>
    ///
    /// Requires OAuth2 client credentials and token endpoint.
    Oauth {
        /// OAuth2 client ID.
        client_id: String,
        /// OAuth2 client secret.
        client_secret: String,
        /// OAuth2 token endpoint URL.
        token_endpoint: String,
    },
}

#[derive(Debug, Clone, PartialEq)]
/// Configuration for TLS settings when connecting to Kafka brokers.
pub struct TlsConfig {
    /// Whether to skip server certificate verification.
    ///
    /// When set to `true`, the client will not verify the server's certificate.
    pub insecure_skip_verify: bool,

    /// CA certificate used to verify the server's certificate.
    /// Note: This will contain the actual certificate, not the path to the certificate.
    pub ca_cert: Option<String>,

    /// Client authentication certificates for mutual TLS (mTLS).
    ///
    /// If `Some`, client authentication will be enabled using the provided certificates.
    /// If `None`, client authentication will be disabled.
    pub client_auth: Option<TlsClientAuthCerts>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct TlsClientAuthCerts {
    pub client_cert: String,
    pub client_cert_private_key: String,
}

fn update_auth_config(
    client_config: &mut ClientConfig,
    tls_config: Option<TlsConfig>,
    auth_config: Option<KafkaSaslAuth>,
) {
    let tls_enabled = tls_config.is_some();
    if let Some(tls_config) = tls_config {
        client_config.set("security.protocol", "SSL");
        if tls_config.insecure_skip_verify {
            warn!(
                "'insecureSkipVerify' is set to true, certificate validation will not be performed when connecting to Kafka server"
            );
            client_config.set("enable.ssl.certificate.verification", "false");
        }
        if let Some(ca_cert) = tls_config.ca_cert {
            client_config.set("ssl.ca.pem", ca_cert);
        }
        if let Some(client_auth) = tls_config.client_auth {
            client_config
                .set("ssl.certificate.pem", client_auth.client_cert)
                .set("ssl.key.pem", client_auth.client_cert_private_key);
        }
    }

    if let Some(auth) = auth_config {
        client_config.set(
            "security.protocol",
            if tls_enabled {
                "SASL_SSL"
            } else {
                "SASL_PLAINTEXT"
            },
        );
        match auth {
            KafkaSaslAuth::Plain { username, password } => {
                client_config
                    .set("sasl.mechanisms", "PLAIN")
                    .set("sasl.username", username)
                    .set("sasl.password", password);
            }
            KafkaSaslAuth::ScramSha256 { username, password } => {
                client_config
                    .set("sasl.mechanisms", "SCRAM-SHA-256")
                    .set("sasl.username", username)
                    .set("sasl.password", password);
            }
            KafkaSaslAuth::ScramSha512 { username, password } => {
                client_config
                    .set("sasl.mechanisms", "SCRAM-SHA-512")
                    .set("sasl.username", username)
                    .set("sasl.password", password);
            }
            KafkaSaslAuth::Gssapi {
                service_name,
                realm: _,
                username,
                password: _,
                keytab,
                kerberos_config,
                auth_type: _,
            } => {
                client_config.set("sasl.mechanisms", "GSSAPI");
                client_config.set("sasl.kerberos.service.name", service_name);
                client_config.set("sasl.kerberos.principal", username);
                if let Some(keytab) = keytab {
                    client_config.set("sasl.kerberos.keytab", keytab);
                }
                if let Some(kerberos_config) = kerberos_config {
                    client_config.set("sasl.kerberos.kinit.cmd", kerberos_config);
                }
            }
            KafkaSaslAuth::Oauth {
                client_id,
                client_secret,
                token_endpoint,
            } => {
                client_config
                    .set("sasl.mechanism", "OAUTHBEARER")
                    .set("sasl.oauthbearer.method", "oidc")
                    .set("sasl.oauthbearer.client.id", client_id)
                    .set("sasl.oauthbearer.client.secret", client_secret)
                    .set("sasl.oauthbearer.token.endpoint.url", token_endpoint);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rdkafka::config::ClientConfig;
    use std::collections::HashMap;

    #[test]
    fn test_update_auth_config_none() {
        let mut config = ClientConfig::new();
        update_auth_config(&mut config, None, None);
        // Should not set any security or SASL keys
        assert!(config.get("security.protocol").is_none());
        assert!(config.get("sasl.mechanisms").is_none());
    }

    #[test]
    fn test_update_auth_config_tls_only() {
        let mut config = ClientConfig::new();
        let tls = TlsConfig {
            insecure_skip_verify: true,
            ca_cert: Some("CA_CERT_DATA".to_string()),
            client_auth: Some(TlsClientAuthCerts {
                client_cert: "CLIENT_CERT_DATA".to_string(),
                client_cert_private_key: "CLIENT_KEY_DATA".to_string(),
            }),
        };
        update_auth_config(&mut config, Some(tls), None);
        let expected_config = [
            ("security.protocol", "SSL"),
            ("enable.ssl.certificate.verification", "false"),
            ("ssl.ca.pem", "CA_CERT_DATA"),
            ("ssl.certificate.pem", "CLIENT_CERT_DATA"),
            ("ssl.key.pem", "CLIENT_KEY_DATA"),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect::<HashMap<String, String>>();
        assert_eq!(config.config_map().clone(), expected_config);
    }

    #[test]
    fn update_auth_config_with_plain_auth() {
        let mut config = ClientConfig::new();
        let auth = KafkaSaslAuth::Plain {
            username: "user".to_string(),
            password: "pass".to_string(),
        };
        update_auth_config(&mut config, None, Some(auth));
        let expected_config = [
            ("security.protocol", "SASL_PLAINTEXT"),
            ("sasl.mechanisms", "PLAIN"),
            ("sasl.username", "user"),
            ("sasl.password", "pass"),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect::<HashMap<String, String>>();
        assert_eq!(config.config_map().clone(), expected_config);
    }

    #[test]
    fn update_auth_config_with_scram_sha256() {
        let mut config = ClientConfig::new();
        let auth = KafkaSaslAuth::ScramSha256 {
            username: "user256".to_string(),
            password: "pass256".to_string(),
        };
        update_auth_config(&mut config, None, Some(auth));
        let expected_config = [
            ("security.protocol", "SASL_PLAINTEXT"),
            ("sasl.mechanisms", "SCRAM-SHA-256"),
            ("sasl.username", "user256"),
            ("sasl.password", "pass256"),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect::<HashMap<String, String>>();
        assert_eq!(config.config_map().clone(), expected_config);
    }

    #[test]
    fn update_auth_config_with_scram_sha512() {
        let mut config = ClientConfig::new();
        let auth = KafkaSaslAuth::ScramSha512 {
            username: "user512".to_string(),
            password: "pass512".to_string(),
        };
        update_auth_config(&mut config, None, Some(auth));
        let expected_config = [
            ("security.protocol", "SASL_PLAINTEXT"),
            ("sasl.mechanisms", "SCRAM-SHA-512"),
            ("sasl.username", "user512"),
            ("sasl.password", "pass512"),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect::<HashMap<String, String>>();
        assert_eq!(config.config_map().clone(), expected_config);
    }

    #[test]
    fn update_auth_config_with_gssapi() {
        let mut config = ClientConfig::new();
        let auth = KafkaSaslAuth::Gssapi {
            service_name: "kafka".to_string(),
            realm: "REALM".to_string(),
            username: "principal".to_string(),
            password: None,
            keytab: Some("/path/to/keytab".to_string()),
            kerberos_config: Some("/path/to/krb5.conf".to_string()),
            auth_type: "kinit".to_string(),
        };
        update_auth_config(&mut config, None, Some(auth));
        let expected_config = [
            ("security.protocol", "SASL_PLAINTEXT"),
            ("sasl.mechanisms", "GSSAPI"),
            ("sasl.kerberos.service.name", "kafka"),
            ("sasl.kerberos.principal", "principal"),
            ("sasl.kerberos.keytab", "/path/to/keytab"),
            ("sasl.kerberos.kinit.cmd", "/path/to/krb5.conf"),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect::<HashMap<String, String>>();
        assert_eq!(config.config_map().clone(), expected_config);
    }

    #[test]
    fn update_auth_config_with_oauth() {
        let mut config = ClientConfig::new();
        let auth = KafkaSaslAuth::Oauth {
            client_id: "cid".to_string(),
            client_secret: "csecret".to_string(),
            token_endpoint: "https://token".to_string(),
        };
        update_auth_config(&mut config, None, Some(auth));
        let expected_config = [
            ("security.protocol", "SASL_PLAINTEXT"),
            ("sasl.mechanisms", "OAUTHBEARER"),
            ("sasl.oauthbearer.client.id", "cid"),
            ("sasl.oauthbearer.client.secret", "csecret"),
            ("sasl.oauthbearer.token.endpoint.url", "https://token"),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect::<HashMap<String, String>>();
        assert_eq!(config.config_map().clone(), expected_config);
    }

    #[test]
    fn update_auth_config_with_tls_and_plain() {
        let mut config = ClientConfig::new();
        let tls = TlsConfig {
            insecure_skip_verify: false,
            ca_cert: None,
            client_auth: None,
        };
        let auth = KafkaSaslAuth::Plain {
            username: "user".to_string(),
            password: "pass".to_string(),
        };
        update_auth_config(&mut config, Some(tls), Some(auth));
        let expected_config = [
            ("security.protocol", "SASL_SSL"),
            ("sasl.mechanisms", "PLAIN"),
            ("sasl.username", "user"),
            ("sasl.password", "pass"),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect::<HashMap<String, String>>();
        assert_eq!(config.config_map().clone(), expected_config);
    }
}
