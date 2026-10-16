use numaflow_shared::redacted_url::RedactedUrl;
use rdkafka::config::ClientConfig;
use secrecy::{ExposeSecret, SecretString};
use tracing::warn;

pub use numaflow_shared::tls::{TlsClientAuthCerts, TlsConfigWithClientAuth};

pub mod sink;
pub mod source;

const KAFKA_TOPIC_HEADER_KEY: &str = "X-NF-Kafka-TopicName";

pub type Result<T> = core::result::Result<T, Error>;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Connecting to Kafka {server:?} - {error}")]
    Connection {
        server: Vec<RedactedUrl>,
        error: String,
    },

    #[error("Kafka - {0}")]
    Kafka(String),

    #[error("{0}")]
    NonRetryable(String),

    #[error("{0}")]
    Other(String),
}

/// Represents the SASL authentication mechanism for connecting to Kafka.
///
/// See the [Kafka Security documentation](https://kafka.apache.org/documentation/#security_sasl) for more details on each mechanism.
#[derive(Clone, Debug)]
pub enum KafkaSaslAuth {
    /// SASL/PLAIN authentication mechanism.
    ///
    /// See: <https://kafka.apache.org/documentation/#security_sasl_plain>
    ///
    /// Requires a username and password.
    Plain {
        username: String,
        password: SecretString,
    },
    /// SASL/SCRAM-SHA-256 authentication mechanism.
    ///
    /// See: <https://kafka.apache.org/documentation/#security_sasl_scram>
    ///
    /// Requires a username and password.
    ScramSha256 {
        username: String,
        password: SecretString,
    },
    /// SASL/SCRAM-SHA-512 authentication mechanism.
    ///
    /// See: <https://kafka.apache.org/documentation/#security_sasl_scram>
    ///
    /// Requires a username and password.
    ScramSha512 {
        username: String,
        password: SecretString,
    },
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
        password: Option<SecretString>,
        /// Keytab (optional).
        keytab: Option<SecretString>,
        /// Kerberos configuration (optional).
        kerberos_config: Option<SecretString>,
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
        client_secret: SecretString,
        /// OAuth2 token endpoint URL.
        token_endpoint: RedactedUrl,
    },
}

fn update_auth_config(
    client_config: &mut ClientConfig,
    tls_config: Option<TlsConfigWithClientAuth>,
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
            client_config.set("ssl.ca.pem", ca_cert.expose_secret());
        }
        if let Some(client_auth) = tls_config.client_auth {
            client_config
                .set(
                    "ssl.certificate.pem",
                    client_auth.client_cert.expose_secret(),
                )
                .set(
                    "ssl.key.pem",
                    client_auth.client_cert_private_key.expose_secret(),
                );
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
                    .set("sasl.password", password.expose_secret());
            }
            KafkaSaslAuth::ScramSha256 { username, password } => {
                client_config
                    .set("sasl.mechanisms", "SCRAM-SHA-256")
                    .set("sasl.username", username)
                    .set("sasl.password", password.expose_secret());
            }
            KafkaSaslAuth::ScramSha512 { username, password } => {
                client_config
                    .set("sasl.mechanisms", "SCRAM-SHA-512")
                    .set("sasl.username", username)
                    .set("sasl.password", password.expose_secret());
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
                client_config
                    .set("sasl.mechanisms", "GSSAPI")
                    .set("sasl.kerberos.service.name", service_name)
                    .set("sasl.kerberos.principal", username);
                if let Some(keytab) = keytab {
                    client_config.set("sasl.kerberos.keytab", keytab.expose_secret());
                }
                if let Some(kerberos_config) = kerberos_config {
                    client_config.set("sasl.kerberos.kinit.cmd", kerberos_config.expose_secret());
                }
            }
            KafkaSaslAuth::Oauth {
                client_id,
                client_secret,
                token_endpoint,
            } => {
                client_config
                    .set("sasl.mechanisms", "OAUTHBEARER")
                    .set("sasl.oauthbearer.method", "oidc")
                    .set("sasl.oauthbearer.client.id", client_id)
                    .set(
                        "sasl.oauthbearer.client.secret",
                        client_secret.expose_secret(),
                    )
                    .set(
                        "sasl.oauthbearer.token.endpoint.url",
                        token_endpoint.expose_url().as_str(),
                    );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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
    fn tls_debug_redacts_contents_and_client_receives_them() {
        let tls = TlsConfigWithClientAuth {
            insecure_skip_verify: true,
            ca_cert: Some("secret-ca".into()),
            client_auth: Some(TlsClientAuthCerts {
                client_cert: "secret-cert".into(),
                client_cert_private_key: "secret-key".into(),
            }),
        };
        assert!(!format!("{tls:?}").contains("secret-"));
        assert!(!format!("{tls:#?}").contains("secret-"));
        let mut client = ClientConfig::new();
        update_auth_config(&mut client, Some(tls), None);
        for (key, value) in [
            ("security.protocol", "SSL"),
            ("enable.ssl.certificate.verification", "false"),
            ("enable.ssl.certificate.verification", "false"),
            ("ssl.ca.pem", "secret-ca"),
            ("ssl.certificate.pem", "secret-cert"),
            ("ssl.key.pem", "secret-key"),
        ] {
            assert_eq!(client.get(key), Some(value));
        }
    }

    #[test]
    fn update_auth_config_with_plain_auth() {
        let mut config = ClientConfig::new();
        let auth = KafkaSaslAuth::Plain {
            username: "user".to_string(),
            password: "pass".into(),
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
            password: "pass256".into(),
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
            password: "pass512".into(),
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
            keytab: Some("/path/to/keytab".into()),
            kerberos_config: Some("/path/to/krb5.conf".into()),
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
            client_secret: "csecret".into(),
            token_endpoint: "https://token".parse().unwrap(),
        };
        update_auth_config(&mut config, None, Some(auth));
        let expected_config = [
            ("security.protocol", "SASL_PLAINTEXT"),
            ("sasl.mechanisms", "OAUTHBEARER"),
            ("sasl.oauthbearer.method", "oidc"),
            ("sasl.oauthbearer.client.id", "cid"),
            ("sasl.oauthbearer.client.secret", "csecret"),
            ("sasl.oauthbearer.token.endpoint.url", "https://token/"),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect::<HashMap<String, String>>();
        assert_eq!(config.config_map().clone(), expected_config);
    }

    #[test]
    fn update_auth_config_with_tls_and_plain() {
        let mut config = ClientConfig::new();
        let tls = TlsConfigWithClientAuth {
            insecure_skip_verify: false,
            ca_cert: None,
            client_auth: None,
        };
        let auth = KafkaSaslAuth::Plain {
            username: "user".to_string(),
            password: "pass".into(),
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

    #[test]
    fn test_update_auth_config_tls_only() {
        let mut config = ClientConfig::new();
        let tls = TlsConfigWithClientAuth {
            insecure_skip_verify: true,
            ca_cert: Some("CA_CERT_DATA".to_string().into()),
            client_auth: Some(TlsClientAuthCerts {
                client_cert: "CLIENT_CERT_DATA".into(),
                client_cert_private_key: "CLIENT_KEY_DATA".into(),
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
    fn test_sasl_auth_debug_redacts_plain_password() {
        let auth = KafkaSaslAuth::Plain {
            username: "user".to_string(),
            password: "hunter2-secret-password".into(),
        };
        let debug_output = format!("{:?}", auth);
        assert!(!debug_output.contains("hunter2-secret-password"));
        assert!(debug_output.contains("username"));
        assert!(debug_output.contains("user"));
        assert!(debug_output.contains("REDACTED"));
    }

    #[test]
    fn sasl_debug_redacts_all_variants_and_client_receives_credentials() {
        for (auth, mechanism) in [
            (
                KafkaSaslAuth::Plain {
                    username: "user".into(),
                    password: "secret-password".into(),
                },
                "PLAIN",
            ),
            (
                KafkaSaslAuth::ScramSha256 {
                    username: "user".into(),
                    password: "secret-password".into(),
                },
                "SCRAM-SHA-256",
            ),
            (
                KafkaSaslAuth::ScramSha512 {
                    username: "user".into(),
                    password: "secret-password".into(),
                },
                "SCRAM-SHA-512",
            ),
            (
                KafkaSaslAuth::Gssapi {
                    service_name: "kafka".into(),
                    realm: "EXAMPLE.COM".into(),
                    username: "user".into(),
                    password: Some("secret-password".into()),
                    keytab: Some("secret-keytab".into()),
                    kerberos_config: Some("secret-config".into()),
                    auth_type: "kinit".into(),
                },
                "GSSAPI",
            ),
            (
                KafkaSaslAuth::Oauth {
                    client_id: "client".into(),
                    client_secret: "secret-client".into(),
                    token_endpoint: "https://user:secret-url@token.example.com/"
                        .parse()
                        .unwrap(),
                },
                "OAUTHBEARER",
            ),
        ] {
            assert!(!format!("{auth:?}").contains("secret-"));
            assert!(!format!("{auth:#?}").contains("secret-"));
            let mut client = ClientConfig::new();
            update_auth_config(&mut client, None, Some(auth));
            assert_eq!(client.get("sasl.mechanisms"), Some(mechanism));
            assert_eq!(client.get("security.protocol"), Some("SASL_PLAINTEXT"));
            match mechanism {
                "GSSAPI" => {
                    assert_eq!(client.get("sasl.kerberos.keytab"), Some("secret-keytab"));
                    assert_eq!(client.get("sasl.kerberos.kinit.cmd"), Some("secret-config"));
                    assert_eq!(client.get("sasl.kerberos.principal"), Some("user"));
                    assert_eq!(client.get("sasl.kerberos.service.name"), Some("kafka"));
                }
                "OAUTHBEARER" => {
                    assert_eq!(
                        client.get("sasl.oauthbearer.client.secret"),
                        Some("secret-client")
                    );
                    assert_eq!(
                        client.get("sasl.oauthbearer.token.endpoint.url"),
                        Some("https://user:secret-url@token.example.com/")
                    );
                }
                _ => {
                    assert_eq!(client.get("sasl.username"), Some("user"));
                    assert_eq!(client.get("sasl.password"), Some("secret-password"));
                }
            }
        }
    }

    #[test]
    fn security_protocol_respects_tls_and_absent_auth() {
        let mut client = ClientConfig::new();
        update_auth_config(&mut client, None, None);
        assert!(client.get("security.protocol").is_none());
        assert!(client.get("sasl.mechanisms").is_none());
        update_auth_config(
            &mut client,
            Some(TlsConfigWithClientAuth {
                insecure_skip_verify: false,
                ca_cert: None,
                client_auth: None,
            }),
            Some(KafkaSaslAuth::Plain {
                username: "user".into(),
                password: "secret-password".into(),
            }),
        );
        assert_eq!(client.get("security.protocol"), Some("SASL_SSL"));
        assert_eq!(client.get("sasl.password"), Some("secret-password"));
    }
}
