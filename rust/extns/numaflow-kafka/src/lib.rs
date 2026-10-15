use numaflow_shared::redacted_url::RedactedUrl;
use rdkafka::config::ClientConfig;
use secrecy::{ExposeSecret, SecretString};
use tracing::warn;

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

/// Configuration for TLS settings when connecting to Kafka brokers.
#[derive(Debug, Clone)]
pub struct TlsConfig {
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

#[derive(Debug, Clone)]
pub struct TlsClientAuthCerts {
    pub client_cert: SecretString,
    pub client_cert_private_key: SecretString,
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
    fn test_tls_config_debug_implementation() {
        // Test with a typical config
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
                    .into(),
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
                    .into(),
            }),
        };
        let debug_output = format!("{:?}", tls_config);
        assert!(!debug_output.contains("CERTIFICATE"));
        assert!(!debug_output.contains("PRIVATE"));

        let ca_cert_snippet = "YW5kYm94MSQwIgYDVQQDDBtta2NlcnQgcm9vdEBidWlsZGtpdHNhbmRib3gwggGi";
        // Make sure the test snippet is actually in the config value
        assert!(
            tls_config
                .ca_cert
                .as_ref()
                .unwrap()
                .expose_secret()
                .contains(ca_cert_snippet)
        );
        // but not in the debug output
        assert!(!debug_output.contains(ca_cert_snippet));

        let client_cert_private_key_snippet =
            "SLEdFFS185a7oB2gfoxgvvjZN9XGuZsDUbazPvruK0064QMKQ5F7csq5+1v4rCRF";
        assert!(
            tls_config
                .client_auth
                .as_ref()
                .unwrap()
                .client_cert_private_key
                .expose_secret()
                .contains(client_cert_private_key_snippet)
        );
        assert!(!debug_output.contains(client_cert_private_key_snippet));

        let client_cert_snippet =
            "w1Zd4hiRUXiydoZnaNdis/7v6yL+sWB8mP0HYY4qpq0FSQxSDsXaVnfW9Ly4vUu0";
        assert!(
            tls_config
                .client_auth
                .as_ref()
                .unwrap()
                .client_cert
                .expose_secret()
                .contains(client_cert_snippet)
        );
        assert!(!debug_output.contains(client_cert_snippet));
    }

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
        let tls = TlsConfig {
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
        let tls = TlsConfig {
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
        let tls = TlsConfig {
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
            Some(TlsConfig {
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
