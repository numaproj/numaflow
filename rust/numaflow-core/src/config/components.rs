//! This module contains the configuration for all the components of the pipeline and monovertex.

use crate::Error;
use crate::shared::create_components::get_secret_from_volume;

pub(crate) mod source;

pub(crate) mod sink;

pub(crate) mod transformer;

pub(crate) mod metrics;

pub(crate) mod ratelimit;

pub(crate) mod reduce;

fn parse_kafka_auth_config(
    auth_config: Option<Box<numaflow_models::models::Sasl>>,
    tls_config: Option<Box<numaflow_models::models::Tls>>,
) -> crate::Result<(
    Option<numaflow_kafka::KafkaSaslAuth>,
    Option<numaflow_kafka::TlsConfig>,
)> {
    let auth: Option<numaflow_kafka::KafkaSaslAuth> = match auth_config {
        Some(sasl) => {
            let mechanism = sasl.mechanism.to_uppercase();
            match mechanism.as_str() {
                "PLAIN" => {
                    let Some(plain) = sasl.plain else {
                        return Err(Error::Config(
                            "PLAIN mechanism requires plain auth configuration".into(),
                        ));
                    };
                    let username =
                        get_secret_from_volume(&plain.user_secret.name, &plain.user_secret.key)
                            .map_err(|e| {
                                Error::Config(format!("Failed to get user secret: {e:?}"))
                            })?;
                    let password = if let Some(password_secret) = plain.password_secret {
                        get_secret_from_volume(&password_secret.name, &password_secret.key)
                            .map_err(|e| {
                                Error::Config(format!("Failed to get password secret: {e:?}"))
                            })?
                    } else {
                        return Err(Error::Config("PLAIN mechanism requires password".into()));
                    };
                    Some(numaflow_kafka::KafkaSaslAuth::Plain { username, password })
                }
                "SCRAM-SHA-256" => {
                    let Some(scram) = sasl.scramsha256 else {
                        return Err(Error::Config(
                            "SCRAM-SHA-256 mechanism requires scramsha256 auth configuration"
                                .into(),
                        ));
                    };
                    let username =
                        get_secret_from_volume(&scram.user_secret.name, &scram.user_secret.key)
                            .map_err(|e| {
                                Error::Config(format!("Failed to get user secret: {e:?}"))
                            })?;
                    let password = if let Some(password_secret) = scram.password_secret {
                        get_secret_from_volume(&password_secret.name, &password_secret.key)
                            .map_err(|e| {
                                Error::Config(format!("Failed to get password secret: {e:?}"))
                            })?
                    } else {
                        return Err(Error::Config(
                            "SCRAM-SHA-256 mechanism requires password".into(),
                        ));
                    };
                    Some(numaflow_kafka::KafkaSaslAuth::ScramSha256 { username, password })
                }
                "SCRAM-SHA-512" => {
                    let Some(scram) = sasl.scramsha512 else {
                        return Err(Error::Config(
                            "SCRAM-SHA-512 mechanism requires scramsha512 auth configuration"
                                .into(),
                        ));
                    };
                    let username =
                        get_secret_from_volume(&scram.user_secret.name, &scram.user_secret.key)
                            .map_err(|e| {
                                Error::Config(format!("Failed to get user secret: {e:?}"))
                            })?;
                    let password = if let Some(password_secret) = scram.password_secret {
                        get_secret_from_volume(&password_secret.name, &password_secret.key)
                            .map_err(|e| {
                                Error::Config(format!("Failed to get password secret: {e:?}"))
                            })?
                    } else {
                        return Err(Error::Config(
                            "SCRAM-SHA-512 mechanism requires password".into(),
                        ));
                    };
                    Some(numaflow_kafka::KafkaSaslAuth::ScramSha512 { username, password })
                }
                "GSSAPI" => {
                    let Some(gssapi) = sasl.gssapi else {
                        return Err(Error::Config(
                            "GSSAPI mechanism requires gssapi configuration".into(),
                        ));
                    };
                    let service_name = gssapi.service_name.clone();
                    let realm = gssapi.realm.clone();
                    let username = get_secret_from_volume(
                        &gssapi.username_secret.name,
                        &gssapi.username_secret.key,
                    )
                    .map_err(|e| {
                        Error::Config(format!("Failed to get gssapi username secret: {e:?}"))
                    })?;
                    let password = if let Some(password_secret) = gssapi.password_secret {
                        Some(
                            get_secret_from_volume(&password_secret.name, &password_secret.key)
                                .map_err(|e| {
                                    Error::Config(format!(
                                        "Failed to get gssapi password secret: {e:?}"
                                    ))
                                })?,
                        )
                    } else {
                        None
                    };
                    let keytab = if let Some(keytab_secret) = gssapi.keytab_secret {
                        Some(
                            get_secret_from_volume(&keytab_secret.name, &keytab_secret.key)
                                .map_err(|e| {
                                    Error::Config(format!(
                                        "Failed to get gssapi keytab secret: {e:?}"
                                    ))
                                })?,
                        )
                    } else {
                        None
                    };
                    let kerberos_config =
                        if let Some(kerberos_config_secret) = gssapi.kerberos_config_secret {
                            Some(
                                get_secret_from_volume(
                                    &kerberos_config_secret.name,
                                    &kerberos_config_secret.key,
                                )
                                .map_err(|e| {
                                    Error::Config(format!(
                                        "Failed to get gssapi kerberos config secret: {e:?}"
                                    ))
                                })?,
                            )
                        } else {
                            None
                        };
                    let auth_type = format!("{:?}", gssapi.auth_type);
                    Some(numaflow_kafka::KafkaSaslAuth::Gssapi {
                        service_name,
                        realm,
                        username,
                        password,
                        keytab,
                        kerberos_config,
                        auth_type,
                    })
                }
                "OAUTH" | "OAUTHBEARER" => {
                    let Some(oauth) = sasl.oauth else {
                        return Err(Error::Config(
                            "OAUTH mechanism requires oauth configuration".into(),
                        ));
                    };
                    let client_id =
                        get_secret_from_volume(&oauth.client_id.name, &oauth.client_id.key)
                            .map_err(|e| {
                                Error::Config(format!("Failed to get client id secret: {e:?}"))
                            })?;
                    let client_secret =
                        get_secret_from_volume(&oauth.client_secret.name, &oauth.client_secret.key)
                            .map_err(|e| {
                                Error::Config(format!("Failed to get client secret: {e:?}"))
                            })?;
                    let token_endpoint = oauth.token_endpoint.clone();
                    Some(numaflow_kafka::KafkaSaslAuth::Oauth {
                        client_id,
                        client_secret,
                        token_endpoint,
                    })
                }
                _ => {
                    return Err(Error::Config(format!(
                        "Unsupported SASL mechanism: {mechanism}"
                    )));
                }
            }
        }
        None => None,
    };

    let tls = if let Some(tls_config) = tls_config {
        let ca_cert = tls_config
            .ca_cert_secret
            .map(|ca_cert_secret| {
                match get_secret_from_volume(&ca_cert_secret.name, &ca_cert_secret.key) {
                    Ok(secret) => Ok(secret),
                    Err(e) => Err(Error::Config(format!(
                        "Failed to get CA cert secret: {e:?}"
                    ))),
                }
            })
            .transpose()?;

        let tls_client_auth_certs = match tls_config.cert_secret {
            Some(client_cert_secret) => {
                let client_cert =
                    get_secret_from_volume(&client_cert_secret.name, &client_cert_secret.key)
                        .map_err(|e| {
                            Error::Config(format!("Failed to get client cert secret: {e:?}"))
                        })?;

                let Some(private_key_secret) = tls_config.key_secret else {
                    return Err(Error::Config("Client cert is specified for TLS authentication, but private key is not specified".into()));
                };

                let client_cert_private_key =
                    get_secret_from_volume(&private_key_secret.name, &private_key_secret.key)
                        .map_err(|e| {
                            Error::Config(format!(
                                "Failed to get client cert private key secret: {e:?}"
                            ))
                        })?;
                Some(numaflow_kafka::TlsClientAuthCerts {
                    client_cert,
                    client_cert_private_key,
                })
            }
            None => None,
        };

        Some(numaflow_kafka::TlsConfig {
            insecure_skip_verify: tls_config.insecure_skip_verify.unwrap_or(false),
            ca_cert,
            client_auth: tls_client_auth_certs,
        })
    } else {
        None
    };

    Ok((auth, tls))
}

fn parse_pulsar_auth_config(
    auth: Option<Box<numaflow_models::models::PulsarAuth>>,
) -> crate::Result<Option<numaflow_pulsar::PulsarAuth>> {
    let Some(auth) = auth else {
        return Ok(None);
    };

    if let Some(token) = auth.token {
        let secret = get_secret_from_volume(&token.name, &token.key)
            .map_err(|e| Error::Config(format!("Failed to get token secret from volume: {e:?}")))?;
        return Ok(Some(numaflow_pulsar::PulsarAuth::JWT(secret)));
    }

    if let Some(basic_auth) = auth.basic_auth {
        let user_secret_selector = &basic_auth
            .username
            .ok_or_else(|| Error::Config("Username can not be empty for basic auth".into()))?;
        let username =
            get_secret_from_volume(&user_secret_selector.name, &user_secret_selector.key).map_err(
                |e| Error::Config(format!("Failed to get username secret from volume: {e:?}")),
            )?;
        let password_secret_selector = &basic_auth
            .password
            .ok_or_else(|| Error::Config("Password can not be empty for basic auth".into()))?;
        let password = get_secret_from_volume(
            &password_secret_selector.name,
            &password_secret_selector.key,
        )
        .map_err(|e| Error::Config(format!("Failed to get password secret from volume: {e:?}")))?;
        return Ok(Some(numaflow_pulsar::PulsarAuth::HTTPBasic {
            username,
            password,
        }));
    }
    Err(Error::Config("Authentication configuration is enabled, however credentials are not provided in the Pulsar sink configuration".to_string()))
}

#[cfg(test)]
mod kafka_tests {
    use super::sink::SinkType;
    use super::source::SourceType;
    use k8s_openapi::api::core::v1::SecretKeySelector;
    use numaflow_models::models::gssapi::AuthType;
    use numaflow_models::models::{Gssapi, KafkaSource, Sasl, SaslPlain, SasloAuth, Tls};
    use std::collections::HashMap;
    use std::fs;
    use std::path::Path;

    const SECRET_BASE_PATH: &str = "/tmp/numaflow";

    fn setup_secret(name: &str, key: &str, value: &str) {
        let path = format!("{SECRET_BASE_PATH}/{name}");
        fs::create_dir_all(&path).unwrap();
        fs::write(format!("{path}/{key}"), value).unwrap();
    }

    fn cleanup_secret(name: &str) {
        let path = format!("{SECRET_BASE_PATH}/{name}");
        if Path::new(&path).exists() {
            fs::remove_dir_all(&path).unwrap();
        }
    }

    #[test]
    fn test_try_from_kafka_source_with_kafka_raw_config() {
        let kafka_user_config: &str = r#"
            max.poll.interval.ms: 100
            socket.timeout.ms: 10000
            queue.buffering.max.ms: 10000
            "#;

        let kafka_source = KafkaSource {
            brokers: Some(vec!["localhost:9092".to_string()]),
            topic: "test-topic".to_string(),
            consumer_group: Some("test-group".to_string()),
            sasl: None,
            tls: None,
            config: Some(kafka_user_config.to_string()),
            kafka_version: None,
        };

        let source_type = SourceType::try_from(Box::new(kafka_source)).unwrap();
        let SourceType::Kafka(config) = source_type else {
            panic!("Expected SourceType::Kafka");
        };

        let expected_config = HashMap::from([
            ("max.poll.interval.ms".to_string(), "100".to_string()),
            ("socket.timeout.ms".to_string(), "10000".to_string()),
            ("queue.buffering.max.ms".to_string(), "10000".to_string()),
        ]);
        assert_eq!(config.kafka_raw_config, expected_config);
    }

    #[test]
    fn test_try_from_kafka_sink_with_kafka_raw_config() {
        let kafka_user_config: &str = r#"
            max.poll.interval.ms: 100
            socket.timeout.ms: 10000
            queue.buffering.max.ms: 10000
            "#;

        let kafka_sink = numaflow_models::models::KafkaSink {
            brokers: Some(vec!["localhost:9092".to_string()]),
            topic: "test-topic".to_string(),
            config: Some(kafka_user_config.to_string()),
            sasl: None,
            set_key: Some(true),
            tls: None,
        };

        let sink_type = SinkType::try_from(Box::new(kafka_sink)).unwrap();
        let SinkType::Kafka(config) = sink_type else {
            panic!("Expected SinkType::Kafka");
        };

        let expected_config = HashMap::from([
            ("max.poll.interval.ms".to_string(), "100".to_string()),
            ("socket.timeout.ms".to_string(), "10000".to_string()),
            ("queue.buffering.max.ms".to_string(), "10000".to_string()),
        ]);
        assert_eq!(config.kafka_raw_config, expected_config);
    }

    #[test]
    fn test_try_from_kafka_source_with_plain_auth() {
        let secret_name = "test_try_from_kafka_source_with_plain_auth_plain-auth-secret";
        let user_key = "username";
        let pass_key = "password";
        setup_secret(secret_name, user_key, "test-user");
        setup_secret(secret_name, pass_key, "test-pass");

        let kafka_source = KafkaSource {
            brokers: Some(vec!["localhost:9092".to_string()]),
            topic: "test-topic".to_string(),
            consumer_group: Some("test-group".to_string()),
            sasl: Some(Box::new(Sasl {
                mechanism: "PLAIN".to_string(),
                plain: Some(Box::new(SaslPlain {
                    handshake: true,
                    user_secret: SecretKeySelector {
                        name: secret_name.to_string(),
                        key: user_key.to_string(),
                        ..Default::default()
                    },
                    password_secret: Some(SecretKeySelector {
                        name: secret_name.to_string(),
                        key: pass_key.to_string(),
                        ..Default::default()
                    }),
                })),
                scramsha256: None,
                scramsha512: None,
                oauth: None,
                gssapi: None,
            })),
            tls: None,
            config: None,
            kafka_version: None,
        };

        let source_type = SourceType::try_from(Box::new(kafka_source)).unwrap();
        if let SourceType::Kafka(config) = source_type {
            let auth = config.auth.unwrap();
            match auth {
                numaflow_kafka::KafkaSaslAuth::Plain { username, password } => {
                    assert_eq!(username, "test-user");
                    assert_eq!(password, "test-pass");
                }
                _ => panic!("Unexpected KafkaAuth variant"),
            }
            assert_eq!(config.brokers, vec!["localhost:9092"]);
            assert_eq!(config.topics, vec!["test-topic"]);
            assert_eq!(config.consumer_group, "test-group");
        } else {
            panic!("Expected SourceType::Kafka");
        }

        cleanup_secret(secret_name);
    }

    #[test]
    fn test_try_from_kafka_source_with_scram_auth() {
        let secret_name = "test_try_from_kafka_source_with_scram_auth_scram-auth-secret";
        let user_key = "username";
        let pass_key = "password";
        setup_secret(secret_name, user_key, "test-user");
        setup_secret(secret_name, pass_key, "test-pass");

        let kafka_source = KafkaSource {
            brokers: Some(vec!["localhost:9092".to_string()]),
            topic: "test-topic".to_string(),
            consumer_group: Some("test-group".to_string()),
            sasl: Some(Box::new(Sasl {
                mechanism: "SCRAM-SHA-256".to_string(),
                plain: None,
                scramsha256: Some(Box::new(SaslPlain {
                    handshake: true,
                    user_secret: SecretKeySelector {
                        name: secret_name.to_string(),
                        key: user_key.to_string(),
                        ..Default::default()
                    },
                    password_secret: Some(SecretKeySelector {
                        name: secret_name.to_string(),
                        key: pass_key.to_string(),
                        ..Default::default()
                    }),
                })),
                scramsha512: None,
                oauth: None,
                gssapi: None,
            })),
            tls: None,
            config: None,
            kafka_version: None,
        };

        let source_type = SourceType::try_from(Box::new(kafka_source)).unwrap();
        if let SourceType::Kafka(config) = source_type {
            let auth = config.auth.unwrap();
            match auth {
                numaflow_kafka::KafkaSaslAuth::ScramSha256 { username, password } => {
                    assert_eq!(username, "test-user");
                    assert_eq!(password, "test-pass");
                }
                _ => panic!("Unexpected KafkaAuth variant"),
            }
        } else {
            panic!("Expected SourceType::Kafka");
        }

        cleanup_secret(secret_name);
    }

    #[test]
    fn test_try_from_kafka_source_with_oauth() {
        let secret_name = "test_try_from_kafka_source_with_oauth_oauth-auth-secret";
        let client_id_key = "client_id";
        let client_secret_key = "client_secret";
        setup_secret(secret_name, client_id_key, "test-client");
        setup_secret(secret_name, client_secret_key, "test-secret");

        let kafka_source = KafkaSource {
            brokers: Some(vec!["localhost:9092".to_string()]),
            topic: "test-topic".to_string(),
            consumer_group: Some("test-group".to_string()),
            sasl: Some(Box::new(Sasl {
                mechanism: "OAUTH".to_string(),
                plain: None,
                scramsha256: None,
                scramsha512: None,
                oauth: Some(Box::new(SasloAuth {
                    client_id: SecretKeySelector {
                        name: secret_name.to_string(),
                        key: client_id_key.to_string(),
                        ..Default::default()
                    },
                    client_secret: SecretKeySelector {
                        name: secret_name.to_string(),
                        key: client_secret_key.to_string(),
                        ..Default::default()
                    },
                    token_endpoint: "https://oauth.example.com/token".to_string(),
                })),
                gssapi: None,
            })),
            tls: None,
            config: None,
            kafka_version: None,
        };

        let source_type = SourceType::try_from(Box::new(kafka_source)).unwrap();
        if let SourceType::Kafka(config) = source_type {
            let auth = config.auth.unwrap();
            match auth {
                numaflow_kafka::KafkaSaslAuth::Oauth {
                    client_id,
                    client_secret,
                    token_endpoint,
                } => {
                    assert_eq!(client_id, "test-client");
                    assert_eq!(client_secret, "test-secret");
                    assert_eq!(token_endpoint, "https://oauth.example.com/token");
                }
                _ => panic!("Unexpected KafkaAuth variant"),
            }
        } else {
            panic!("Expected SourceType::Kafka");
        }

        cleanup_secret(secret_name);
    }

    #[test]
    fn test_try_from_kafka_source_with_tls() {
        let test_name = "test_try_from_kafka_source_with_tls";
        let ca_cert_name = format!("{test_name}-tls-ca-cert");
        let cert_name = format!("{test_name}-tls-cert");
        let key_name = format!("{test_name}-tls-key");
        setup_secret(&ca_cert_name, "ca", "test-ca-cert");
        setup_secret(&cert_name, "cert", "test-cert");
        setup_secret(&key_name, "key", "test-key");

        let kafka_source = KafkaSource {
            brokers: Some(vec!["localhost:9092".to_string()]),
            topic: "test-topic".to_string(),
            consumer_group: Some("test-group".to_string()),
            sasl: None,
            tls: Some(Box::new(Tls {
                ca_cert_secret: Some(SecretKeySelector {
                    name: ca_cert_name.clone(),
                    key: "ca".to_string(),
                    ..Default::default()
                }),
                cert_secret: Some(SecretKeySelector {
                    name: cert_name.clone(),
                    key: "cert".to_string(),
                    ..Default::default()
                }),
                key_secret: Some(SecretKeySelector {
                    name: key_name.clone(),
                    key: "key".to_string(),
                    ..Default::default()
                }),
                insecure_skip_verify: Some(false),
            })),
            config: None,
            kafka_version: None,
        };

        let source_type = SourceType::try_from(Box::new(kafka_source)).unwrap();
        if let SourceType::Kafka(config) = source_type {
            let tls_config = config.tls.unwrap();
            assert_eq!(tls_config.ca_cert.unwrap(), "test-ca-cert");
            let client_auth = tls_config.client_auth.as_ref().unwrap();
            assert_eq!(client_auth.client_cert, "test-cert");
            assert_eq!(client_auth.client_cert_private_key, "test-key");
            assert!(!tls_config.insecure_skip_verify);
        } else {
            panic!("Expected SourceType::Kafka");
        }

        cleanup_secret(&ca_cert_name);
        cleanup_secret(&cert_name);
        cleanup_secret(&key_name);
    }

    #[test]
    fn test_try_from_kafka_source_with_insecure_tls() {
        let kafka_source = KafkaSource {
            brokers: Some(vec!["localhost:9092".to_string()]),
            topic: "test-topic".to_string(),
            consumer_group: Some("test-group".to_string()),
            sasl: None,
            tls: Some(Box::new(Tls {
                ca_cert_secret: None,
                cert_secret: None,
                key_secret: None,
                insecure_skip_verify: Some(true),
            })),
            config: None,
            kafka_version: None,
        };

        let source_type = SourceType::try_from(Box::new(kafka_source)).unwrap();
        if let SourceType::Kafka(config) = source_type {
            let tls_config = config.tls.unwrap();
            assert!(tls_config.insecure_skip_verify);
            assert!(tls_config.ca_cert.is_none());
            assert!(tls_config.client_auth.is_none());
        } else {
            panic!("Expected SourceType::Kafka");
        }
    }

    #[test]
    fn test_try_from_kafka_source_with_invalid_auth() {
        let kafka_source = KafkaSource {
            brokers: Some(vec!["localhost:9092".to_string()]),
            topic: "test-topic".to_string(),
            consumer_group: Some("test-group".to_string()),
            sasl: Some(Box::new(Sasl {
                mechanism: "GSSAPI".to_string(),
                plain: None,
                scramsha256: None,
                scramsha512: None,
                oauth: None,
                gssapi: None,
            })),
            tls: None,
            config: None,
            kafka_version: None,
        };

        let result = SourceType::try_from(Box::new(kafka_source));
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(
            err.to_string(),
            "Config Error - GSSAPI mechanism requires gssapi configuration"
        );
    }

    #[test]
    fn test_try_from_kafka_source_with_missing_password() {
        let secret_name = "test_try_from_kafka_source_with_missing_password_plain-auth-secret";
        let user_key = "username";
        setup_secret(secret_name, user_key, "test-user");

        let kafka_source = KafkaSource {
            brokers: Some(vec!["localhost:9092".to_string()]),
            topic: "test-topic".to_string(),
            consumer_group: Some("test-group".to_string()),
            sasl: Some(Box::new(Sasl {
                mechanism: "PLAIN".to_string(),
                plain: Some(Box::new(SaslPlain {
                    handshake: true,
                    user_secret: SecretKeySelector {
                        name: secret_name.to_string(),
                        key: user_key.to_string(),
                        ..Default::default()
                    },
                    password_secret: None,
                })),
                scramsha256: None,
                scramsha512: None,
                oauth: None,
                gssapi: None,
            })),
            tls: None,
            config: None,
            kafka_version: None,
        };

        let result = SourceType::try_from(Box::new(kafka_source));
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(
            err.to_string(),
            "Config Error - PLAIN mechanism requires password"
        );

        cleanup_secret(secret_name);
    }

    #[test]
    fn test_try_from_kafka_source_with_missing_client_cert_key() {
        let cert_name = "test_try_from_kafka_source_with_missing_client_cert_key_tls-cert";
        setup_secret(cert_name, "cert", "test-cert");

        let kafka_source = KafkaSource {
            brokers: Some(vec!["localhost:9092".to_string()]),
            topic: "test-topic".to_string(),
            consumer_group: Some("test-group".to_string()),
            sasl: None,
            tls: Some(Box::new(Tls {
                ca_cert_secret: None,
                cert_secret: Some(SecretKeySelector {
                    name: cert_name.to_string(),
                    key: "cert".to_string(),
                    ..Default::default()
                }),
                key_secret: None,
                insecure_skip_verify: Some(false),
            })),
            config: None,
            kafka_version: None,
        };

        let result = SourceType::try_from(Box::new(kafka_source));
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(
            err.to_string(),
            "Config Error - Client cert is specified for TLS authentication, but private key is not specified"
        );

        cleanup_secret(cert_name);
    }

    #[test]
    fn test_try_from_kafka_source_with_scram_sha512_auth() {
        let secret_name = "test_try_from_kafka_source_with_scram_sha512_auth_scram-auth-secret";
        let user_key = "username";
        let pass_key = "password";
        setup_secret(secret_name, user_key, "test-user");
        setup_secret(secret_name, pass_key, "test-pass");

        let kafka_source = KafkaSource {
            brokers: Some(vec!["localhost:9092".to_string()]),
            topic: "test-topic".to_string(),
            consumer_group: Some("test-group".to_string()),
            sasl: Some(Box::new(Sasl {
                mechanism: "SCRAM-SHA-512".to_string(),
                plain: None,
                scramsha256: None,
                scramsha512: Some(Box::new(SaslPlain {
                    handshake: true,
                    user_secret: SecretKeySelector {
                        name: secret_name.to_string(),
                        key: user_key.to_string(),
                        ..Default::default()
                    },
                    password_secret: Some(SecretKeySelector {
                        name: secret_name.to_string(),
                        key: pass_key.to_string(),
                        ..Default::default()
                    }),
                })),
                oauth: None,
                gssapi: None,
            })),
            tls: None,
            config: None,
            kafka_version: None,
        };

        let source_type = SourceType::try_from(Box::new(kafka_source)).unwrap();
        if let SourceType::Kafka(config) = source_type {
            let auth = config.auth.unwrap();
            match auth {
                numaflow_kafka::KafkaSaslAuth::ScramSha512 { username, password } => {
                    assert_eq!(username, "test-user");
                    assert_eq!(password, "test-pass");
                }
                _ => panic!("Unexpected KafkaAuth variant"),
            }
        } else {
            panic!("Expected SourceType::Kafka");
        }

        cleanup_secret(secret_name);
    }

    #[test]
    fn test_try_from_kafka_source_with_gssapi_auth_password() {
        let secret_name = "test_try_from_kafka_source_with_gssapi_auth_password_gssapi-secret";
        let user_key = "username";
        let pass_key = "password";
        setup_secret(secret_name, user_key, "test-user");
        setup_secret(secret_name, pass_key, "test-pass");

        let gssapi = Gssapi {
            auth_type: AuthType::UserAuth,
            kerberos_config_secret: None,
            keytab_secret: None,
            password_secret: Some(SecretKeySelector {
                name: secret_name.to_string(),
                key: pass_key.to_string(),
                ..Default::default()
            }),
            realm: "EXAMPLE.COM".to_string(),
            service_name: "kafka".to_string(),
            username_secret: SecretKeySelector {
                name: secret_name.to_string(),
                key: user_key.to_string(),
                ..Default::default()
            },
        };

        let kafka_source = KafkaSource {
            brokers: Some(vec!["localhost:9092".to_string()]),
            topic: "test-topic".to_string(),
            consumer_group: Some("test-group".to_string()),
            sasl: Some(Box::new(Sasl {
                mechanism: "GSSAPI".to_string(),
                plain: None,
                scramsha256: None,
                scramsha512: None,
                oauth: None,
                gssapi: Some(Box::new(gssapi)),
            })),
            tls: None,
            config: None,
            kafka_version: None,
        };

        let source_type = SourceType::try_from(Box::new(kafka_source)).unwrap();
        if let SourceType::Kafka(config) = source_type {
            let auth = config.auth.unwrap();
            match auth {
                numaflow_kafka::KafkaSaslAuth::Gssapi {
                    service_name,
                    realm,
                    username,
                    password,
                    keytab,
                    kerberos_config,
                    auth_type,
                } => {
                    assert_eq!(service_name, "kafka");
                    assert_eq!(realm, "EXAMPLE.COM");
                    assert_eq!(username, "test-user");
                    assert_eq!(password, Some("test-pass".to_string()));
                    assert!(keytab.is_none());
                    assert!(kerberos_config.is_none());
                    assert_eq!(auth_type, "UserAuth");
                }
                _ => panic!("Unexpected KafkaAuth variant"),
            }
        } else {
            panic!("Expected SourceType::Kafka");
        }

        cleanup_secret(secret_name);
    }

    #[test]
    fn test_try_from_kafka_source_with_gssapi_auth_keytab() {
        let secret_name = "test_try_from_kafka_source_with_gssapi_auth_keytab_gssapi-secret";
        let user_key = "username";
        let keytab_key = "keytab";
        setup_secret(secret_name, user_key, "test-user");
        setup_secret(secret_name, keytab_key, "test-keytab");

        let gssapi = Gssapi {
            auth_type: AuthType::KeytabAuth,
            kerberos_config_secret: None,
            keytab_secret: Some(SecretKeySelector {
                name: secret_name.to_string(),
                key: keytab_key.to_string(),
                ..Default::default()
            }),
            password_secret: None,
            realm: "EXAMPLE.COM".to_string(),
            service_name: "kafka".to_string(),
            username_secret: SecretKeySelector {
                name: secret_name.to_string(),
                key: user_key.to_string(),
                ..Default::default()
            },
        };

        let kafka_source = KafkaSource {
            brokers: Some(vec!["localhost:9092".to_string()]),
            topic: "test-topic".to_string(),
            consumer_group: Some("test-group".to_string()),
            sasl: Some(Box::new(Sasl {
                mechanism: "GSSAPI".to_string(),
                plain: None,
                scramsha256: None,
                scramsha512: None,
                oauth: None,
                gssapi: Some(Box::new(gssapi)),
            })),
            tls: None,
            config: None,
            kafka_version: None,
        };

        let source_type = SourceType::try_from(Box::new(kafka_source)).unwrap();
        if let SourceType::Kafka(config) = source_type {
            let auth = config.auth.unwrap();
            match auth {
                numaflow_kafka::KafkaSaslAuth::Gssapi {
                    service_name,
                    realm,
                    username,
                    password,
                    keytab,
                    kerberos_config,
                    auth_type,
                } => {
                    assert_eq!(service_name, "kafka");
                    assert_eq!(realm, "EXAMPLE.COM");
                    assert_eq!(username, "test-user");
                    assert!(password.is_none());
                    assert_eq!(keytab, Some("test-keytab".to_string()));
                    assert!(kerberos_config.is_none());
                    assert_eq!(auth_type, "KeytabAuth");
                }
                _ => panic!("Unexpected KafkaAuth variant"),
            }
        } else {
            panic!("Expected SourceType::Kafka");
        }

        cleanup_secret(secret_name);
    }
}
