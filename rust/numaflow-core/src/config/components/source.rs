use std::collections::HashMap;
use std::{fmt::Debug, time::Duration};

use super::{get_secret_from_volume, parse_kafka_auth_config};
use crate::Result;
use crate::config::get_vertex_name;
use crate::error::Error;
use crate::shared::create_components::{parse_nats_auth, parse_tls_config};
use base64::Engine;
use base64::prelude::BASE64_STANDARD;
use bytes::Bytes;
use numaflow_kafka::source::KafkaSourceConfig;
use numaflow_models::models::{GeneratorSource, PulsarSource, SqsSource};
use numaflow_nats::NatsAuth;
use numaflow_nats::jetstream::{ConsumerDeliverPolicy, JetstreamSourceConfig};
use numaflow_nats::nats::NatsSourceConfig;
use numaflow_pulsar::{PulsarAuth, source::PulsarSourceConfig};
use numaflow_sqs::source::SqsSourceConfig;
use tracing::warn;

const DEFAULT_GRPC_MAX_MESSAGE_SIZE: usize = 64 * 1024 * 1024; // 64 MB
const DEFAULT_SOURCE_SOCKET: &str = "/var/run/numaflow/source.sock";
const DEFAULT_SOURCE_SERVER_INFO_FILE: &str = "/var/run/numaflow/sourcer-server-info";

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct SourceConfig {
    /// for high-throughput use-cases we read-ahead the next batch before the previous batch has
    /// been acked (or completed). For most cases it should be set to false.
    pub(crate) read_ahead: bool,
    pub(crate) source_type: SourceType,
}

impl Default for SourceConfig {
    fn default() -> Self {
        Self {
            read_ahead: false,
            source_type: SourceType::Generator(GeneratorConfig::default()),
        }
    }
}

pub(crate) struct JetstreamSourceSpec {
    pipeline_name: String,
    vertex_name: String,
    spec: Box<numaflow_models::models::JetStreamSource>,
}

impl JetstreamSourceSpec {
    pub(crate) fn new(
        pipeline_name: String,
        vertex_name: String,
        spec: Box<numaflow_models::models::JetStreamSource>,
    ) -> Self {
        Self {
            pipeline_name,
            vertex_name,
            spec,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct SourceSpec {
    pipeline_name: String,
    vertex_name: String,
    spec: Box<numaflow_models::models::Source>,
}

impl SourceSpec {
    pub(crate) fn new(
        pipeline_name: String,
        vertex_name: String,
        spec: Box<numaflow_models::models::Source>,
    ) -> Self {
        Self {
            pipeline_name,
            vertex_name,
            spec,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum SourceType {
    Generator(GeneratorConfig),
    UserDefined(UserDefinedConfig),
    Pulsar(PulsarSourceConfig),
    Jetstream(JetstreamSourceConfig),
    Sqs(SqsSourceConfig),
    Kafka(Box<KafkaSourceConfig>),
    Http(numaflow_http::HttpSourceConfig),
    Nats(NatsSourceConfig),
}

impl TryFrom<Box<GeneratorSource>> for SourceType {
    type Error = Error;

    fn try_from(generator: Box<GeneratorSource>) -> Result<Self> {
        let mut generator_config = GeneratorConfig::default();

        if let Some(value_blob) = &generator.value_blob {
            let value_blob = BASE64_STANDARD.decode(value_blob.as_bytes()).map_err(|e| {
                Error::Config(format!(
                    "Failed to base64 decode generator value blob: {e:?}"
                ))
            })?;
            generator_config.content = Bytes::from(value_blob);
        }

        if let Some(msg_size) = generator.msg_size {
            if msg_size >= 0 {
                generator_config.msg_size_bytes = msg_size as u32;
            } else {
                warn!("'msgSize' cannot be negative, using default value (8 bytes)");
            }
        }

        generator_config.value = generator.value;
        generator_config.rpu = generator.rpu.unwrap_or(1) as usize;
        generator_config.duration = generator.duration.map_or(Duration::from_millis(1000), |d| {
            std::time::Duration::from(d)
        });
        generator_config.key_count = generator
            .key_count
            .map_or(0, |kc| std::cmp::min(kc, u8::MAX as i32) as u8);
        generator_config.jitter = generator
            .jitter
            .map_or(Duration::from_secs(0), std::time::Duration::from);

        Ok(SourceType::Generator(generator_config))
    }
}

impl TryFrom<Box<PulsarSource>> for SourceType {
    type Error = Error;
    fn try_from(value: Box<PulsarSource>) -> Result<Self> {
        let auth: Option<PulsarAuth> = super::parse_pulsar_auth_config(value.auth)?;
        let pulsar_config = PulsarSourceConfig {
            pulsar_server_addr: value.server_addr,
            topic: value.topic,
            consumer_name: value.consumer_name,
            subscription: value.subscription_name,
            max_unack: value.max_unack.unwrap_or(1000) as usize,
            auth,
        };
        Ok(SourceType::Pulsar(pulsar_config))
    }
}

impl TryFrom<Box<SqsSource>> for SourceType {
    type Error = Error;

    fn try_from(value: Box<SqsSource>) -> Result<Self> {
        if let Some(timeout) = value.visibility_timeout
            && !(0..=43200).contains(&timeout)
        {
            return Err(Error::Config(format!(
                "visibility_timeout must be between 0 and 43200 for SQS source, got {timeout}"
            )));
        }

        if let Some(wait_time) = value.wait_time_seconds
            && !(0..=20).contains(&wait_time)
        {
            return Err(Error::Config(format!(
                "wait_time_seconds must be between 0 and 20 for SQS source, got {wait_time}"
            )));
        }

        if let Some(max_number_of_messages) = value.max_number_of_messages
            && !(1..=10).contains(&max_number_of_messages)
        {
            return Err(Error::Config(format!(
                "max_number_of_messages must be between 1 and 10 for SQS source, got {max_number_of_messages}"
            )));
        }

        // Convert assume role configuration if present
        let assume_role_config = value.assume_role.map(|ar| numaflow_sqs::AssumeRoleConfig {
            role_arn: ar.role_arn,
            session_name: ar.session_name,
            duration_seconds: ar.duration_seconds,
            external_id: ar.external_id,
            policy: ar.policy,
            policy_arns: ar.policy_arns,
        });

        let sqs_source_config = SqsSourceConfig {
            queue_name: Box::leak(value.queue_name.into_boxed_str()),
            region: Box::leak(value.aws_region.into_boxed_str()),
            queue_owner_aws_account_id: Box::leak(
                value.queue_owner_aws_account_id.into_boxed_str(),
            ),
            attribute_names: value.attribute_names.unwrap_or_default(),
            message_attribute_names: value.message_attribute_names.unwrap_or_default(),
            max_number_of_messages: Some(value.max_number_of_messages.unwrap_or(10)),
            wait_time_seconds: Some(value.wait_time_seconds.unwrap_or(0)),
            visibility_timeout: Some(value.visibility_timeout.unwrap_or(30)),
            endpoint_url: value.endpoint_url,
            assume_role_config,
        };

        Ok(SourceType::Sqs(sqs_source_config))
    }
}

impl TryFrom<JetstreamSourceSpec> for SourceType {
    type Error = Error;
    fn try_from(value: JetstreamSourceSpec) -> std::result::Result<Self, Self::Error> {
        let auth: Option<NatsAuth> = parse_nats_auth(value.spec.auth)?;
        let tls = parse_tls_config(value.spec.tls)?;
        let mut consumer = value.spec.consumer.unwrap_or_default();
        if consumer.trim().is_empty() {
            consumer = format!(
                "numaflow-{}-{}-{}",
                value.pipeline_name, value.vertex_name, value.spec.stream
            )
        }

        let mut deliver_policy = ConsumerDeliverPolicy::ALL;
        if let Some(policy) = value.spec.deliver_policy
            && !policy.is_empty()
        {
            deliver_policy = policy.as_str().try_into()?;
        }

        let js_config = JetstreamSourceConfig {
            addr: value.spec.url,
            consumer,
            stream: value.spec.stream,
            deliver_policy,
            filter_subjects: value.spec.filter_subjects.unwrap_or_default(),
            auth,
            tls,
        };
        Ok(SourceType::Jetstream(js_config))
    }
}

impl TryFrom<Box<numaflow_models::models::NatsSource>> for SourceType {
    type Error = Error;
    fn try_from(
        value: Box<numaflow_models::models::NatsSource>,
    ) -> std::result::Result<Self, Self::Error> {
        let auth = parse_nats_auth(value.auth)?;
        let tls = parse_tls_config(value.tls)?;
        let nats_config = NatsSourceConfig {
            addr: value.url,
            subject: value.subject,
            queue: value.queue,
            auth,
            tls,
        };
        Ok(SourceType::Nats(nats_config))
    }
}

impl TryFrom<Box<numaflow_models::models::KafkaSource>> for SourceType {
    type Error = Error;
    fn try_from(
        value: Box<numaflow_models::models::KafkaSource>,
    ) -> std::result::Result<Self, Self::Error> {
        let (auth, tls) = parse_kafka_auth_config(value.sasl.clone(), value.tls.clone())?;

        let kafka_config = numaflow_kafka::source::KafkaSourceConfig {
            brokers: value.brokers.unwrap_or_default(),
            topics: value
                .topic
                .split(',')
                .map(|s| s.trim().to_string())
                .collect(),
            consumer_group: value.consumer_group.unwrap_or_default(),
            auth,
            tls,
            // config is multiline string with key: value pairs.
            // Eg:
            //  max.poll.interval.ms: 100
            //  socket.timeout.ms: 10000
            //  queue.buffering.max.ms: 10000
            kafka_raw_config: value
                .config
                .unwrap_or_default()
                .trim()
                .split('\n')
                .map(|s| s.split(':').collect::<Vec<&str>>())
                .filter(|parts| parts.len() == 2)
                .map(|parts| {
                    (
                        parts.first().unwrap().trim().to_string(),
                        parts.get(1).unwrap().trim().to_string(),
                    )
                })
                .collect::<HashMap<String, String>>(),
        };
        Ok(SourceType::Kafka(Box::new(kafka_config)))
    }
}

impl TryFrom<SourceSpec> for SourceType {
    type Error = Error;

    fn try_from(mut source: SourceSpec) -> Result<Self> {
        if let Some(generator) = source.spec.generator.take() {
            return generator.try_into();
        }

        if source.spec.udsource.is_some() {
            return Ok(SourceType::UserDefined(UserDefinedConfig::default()));
        }

        if let Some(pulsar) = source.spec.pulsar.take() {
            return pulsar.try_into();
        }

        if let Some(sqs) = source.spec.sqs.take() {
            return sqs.try_into();
        }

        if let Some(_serving) = source.spec.serving.take() {
            panic!("Serving source is invalid");
        }

        if let Some(jetstream) = source.spec.jetstream.take() {
            return JetstreamSourceSpec::new(source.pipeline_name, source.vertex_name, jetstream)
                .try_into();
        }

        if let Some(nats) = source.spec.nats.take() {
            return nats.try_into();
        }

        if let Some(kafka) = source.spec.kafka.take() {
            return kafka.try_into();
        }

        if let Some(http) = source.spec.http.take() {
            return http.try_into();
        }

        Err(Error::Config(format!("Invalid source type: {source:?}")))
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct GeneratorConfig {
    pub rpu: usize,
    pub content: Bytes,
    pub duration: Duration,
    pub value: Option<i64>,
    pub key_count: u8,
    pub msg_size_bytes: u32,
    pub jitter: Duration,
}

impl Default for GeneratorConfig {
    fn default() -> Self {
        Self {
            rpu: 1,
            content: Bytes::new(),
            duration: Duration::from_millis(1000),
            value: None,
            key_count: 0,
            msg_size_bytes: 8,
            jitter: Duration::from_secs(0),
        }
    }
}

impl TryFrom<Box<numaflow_models::models::HttpSource>> for SourceType {
    type Error = Error;
    fn try_from(
        value: Box<numaflow_models::models::HttpSource>,
    ) -> std::result::Result<Self, Self::Error> {
        let mut http_config = numaflow_http::HttpSourceConfigBuilder::new(get_vertex_name());

        if let Some(auth) = value.auth {
            let auth = auth.token.unwrap();
            let token = get_secret_from_volume(&auth.name, &auth.key).map_err(|e| {
                Error::Config(format!("Failed to get token secret from volume: {e:?}"))
            })?;
            http_config = http_config.token(Box::leak(token.into_boxed_str()));
        }

        Ok(SourceType::Http(http_config.build()))
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct UserDefinedConfig {
    pub grpc_max_message_size: usize,
    pub socket_path: String,
    pub server_info_path: String,
}

impl Default for UserDefinedConfig {
    fn default() -> Self {
        Self {
            grpc_max_message_size: DEFAULT_GRPC_MAX_MESSAGE_SIZE,
            socket_path: DEFAULT_SOURCE_SOCKET.to_string(),
            server_info_path: DEFAULT_SOURCE_SERVER_INFO_FILE.to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use std::time::Duration;

    #[test]
    fn test_default_generator_config() {
        let default_config = GeneratorConfig::default();
        assert_eq!(default_config.rpu, 1);
        assert_eq!(default_config.content, Bytes::new());
        assert_eq!(default_config.duration.as_millis(), 1000);
        assert_eq!(default_config.value, None);
        assert_eq!(default_config.key_count, 0);
        assert_eq!(default_config.msg_size_bytes, 8);
        assert_eq!(default_config.jitter, Duration::from_secs(0));
    }

    #[test]
    fn test_generator_config_from_value_blob() {
        let source: SourceType =
            SourceType::try_from(Box::new(numaflow_models::models::GeneratorSource {
                value_blob: Some("aGVsbG8gd29ybGQK".to_string()),
                duration: Some(kube::core::Duration::from(Duration::from_secs(1))),
                jitter: Some(kube::core::Duration::from(Duration::from_secs(0))),
                key_count: Some(0),
                msg_size: Some(8),
                rpu: Some(1),
                value: None,
            }))
            .unwrap();
        assert_eq!(
            source,
            SourceType::Generator(GeneratorConfig {
                content: Bytes::from("hello world\n"),
                duration: Duration::from_secs(1),
                jitter: Duration::from_secs(0),
                key_count: 0,
                rpu: 1,
                value: None,
                msg_size_bytes: 8,
            })
        );
    }

    #[test]
    fn test_generator_config_from_invalid_value_blob() {
        let source = SourceType::try_from(Box::new(numaflow_models::models::GeneratorSource {
            value_blob: Some("abcdef".to_string()),
            duration: Some(kube::core::Duration::from(Duration::from_secs(1))),
            jitter: Some(kube::core::Duration::from(Duration::from_secs(0))),
            key_count: Some(0),
            msg_size: Some(8),
            rpu: Some(1),
            value: None,
        }));
        assert!(
            source
                .unwrap_err()
                .to_string()
                .contains("Failed to base64 decode generator value blob")
        );
    }

    #[test]
    fn test_default_user_defined_config() {
        let default_config = UserDefinedConfig::default();
        assert_eq!(default_config.grpc_max_message_size, 64 * 1024 * 1024);
        assert_eq!(default_config.socket_path, "/var/run/numaflow/source.sock");
        assert_eq!(
            default_config.server_info_path,
            "/var/run/numaflow/sourcer-server-info"
        );
    }

    #[test]
    fn test_source_config_generator() {
        let generator_config = GeneratorConfig::default();
        let source_config = SourceConfig {
            read_ahead: false,
            source_type: SourceType::Generator(generator_config.clone()),
        };
        if let SourceType::Generator(config) = source_config.source_type {
            assert_eq!(config, generator_config);
        } else {
            panic!("Expected SourceType::Generator");
        }
    }

    #[test]
    fn test_source_config_user_defined() {
        let user_defined_config = UserDefinedConfig::default();
        let source_config = SourceConfig {
            read_ahead: false,
            source_type: SourceType::UserDefined(user_defined_config.clone()),
        };
        if let SourceType::UserDefined(config) = source_config.source_type {
            assert_eq!(config, user_defined_config);
        } else {
            panic!("Expected SourceType::UserDefined");
        }
    }
}

#[cfg(test)]
mod jetstream_tests {
    use std::fs;
    use std::path::Path;

    use k8s_openapi::api::core::v1::SecretKeySelector;
    use numaflow_models::models::BasicAuth;
    use numaflow_models::models::{JetStreamSource, Tls};
    use numaflow_nats::{NatsAuth, jetstream::ConsumerDeliverPolicy};

    use crate::config::components::source::JetstreamSourceSpec;

    use super::*;

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
    fn test_try_from_jetstream_source_with_basic_auth() {
        let secret_name = "basic-auth-secret";
        let user_key = "username";
        let pass_key = "password";
        setup_secret(secret_name, user_key, "test-user");
        setup_secret(secret_name, pass_key, "test-pass");

        let jetstream_source = JetStreamSource {
            auth: Some(Box::new(numaflow_models::models::NatsAuth {
                basic: Some(Box::new(BasicAuth {
                    user: Some(SecretKeySelector {
                        name: secret_name.to_string(),
                        key: user_key.to_string(),
                        ..Default::default()
                    }),
                    password: Some(SecretKeySelector {
                        name: secret_name.to_string(),
                        key: pass_key.to_string(),
                        ..Default::default()
                    }),
                })),
                nkey: None,
                token: None,
            })),
            stream: "test-stream".to_string(),
            consumer: Some("numaflow-test-stream".to_string()),
            deliver_policy: Some("by_start_sequence 10".into()),
            filter_subjects: None,
            tls: None,
            url: "nats://localhost:4222".to_string(),
        };

        let source_type = SourceType::try_from(JetstreamSourceSpec::new(
            "test-pipeline".to_string(),
            "test-vertex".to_string(),
            Box::new(jetstream_source),
        ))
        .unwrap();
        if let SourceType::Jetstream(config) = source_type {
            let NatsAuth::Basic { username, password } = config.auth.unwrap() else {
                panic!("Basic auth creds must be set");
            };
            assert_eq!(username, "test-user");
            assert_eq!(password, "test-pass");
            assert_eq!(config.consumer, "numaflow-test-stream");
            assert_eq!(config.addr, "nats://localhost:4222");
            assert_eq!(
                config.deliver_policy,
                ConsumerDeliverPolicy::by_start_sequence(10)
            );
        } else {
            panic!("Expected SourceType::Jetstream");
        }

        cleanup_secret(secret_name);
    }

    #[test]
    fn test_try_from_jetstream_source_with_tls() {
        let test_name = "test_try_from_jetstream_source_with_tls";
        let ca_cert_name = format!("{test_name}-tls-ca-cert");
        let cert_name = format!("{test_name}-tls-cert");
        let key_name = format!("{test_name}-tls-key");
        setup_secret(&ca_cert_name, "ca", "test-ca-cert");
        setup_secret(&cert_name, "cert", "test-cert");
        setup_secret(&key_name, "key", "test-key");

        let jetstream_source = JetStreamSource {
            auth: None,
            stream: "test-stream".to_string(),
            consumer: None,
            deliver_policy: None,
            filter_subjects: None,
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
            url: "nats://localhost:4222".to_string(),
        };

        let source_type = SourceType::try_from(JetstreamSourceSpec::new(
            "test-pipeline".to_string(),
            "test-vertex".to_string(),
            Box::new(jetstream_source),
        ))
        .unwrap();
        if let SourceType::Jetstream(config) = source_type {
            let tls_config = config.tls.unwrap();
            assert_eq!(tls_config.ca_cert.unwrap(), "test-ca-cert");
            let client_auth = tls_config.client_auth.as_ref().unwrap();
            assert_eq!(client_auth.client_cert, "test-cert");
            assert_eq!(client_auth.client_cert_private_key, "test-key");
            assert!(!tls_config.insecure_skip_verify);
        } else {
            panic!("Expected SourceType::Jetstream");
        }

        cleanup_secret(&ca_cert_name);
        cleanup_secret(&cert_name);
        cleanup_secret(&key_name);
    }

    #[test]
    fn test_try_from_jetstream_source_with_invalid_auth() {
        let jetstream_source = JetStreamSource {
            auth: Some(Box::new(numaflow_models::models::NatsAuth {
                basic: None,
                nkey: None,
                token: None,
            })),
            stream: "test-stream".to_string(),
            consumer: None,
            tls: None,
            url: "nats://localhost:4222".to_string(),
            deliver_policy: None,
            filter_subjects: None,
        };

        let result = SourceType::try_from(JetstreamSourceSpec::new(
            "test-pipeline".to_string(),
            "test-vertex".to_string(),
            Box::new(jetstream_source),
        ));
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(
            err.to_string(),
            "Config Error - Authentication is specified, but auth setting is empty"
        );
    }
}

#[cfg(test)]
mod pulsar_source_tests {
    use k8s_openapi::api::core::v1::SecretKeySelector;
    use numaflow_models::models::{PulsarAuth, PulsarBasicAuth, PulsarSource};
    use numaflow_pulsar::PulsarAuth as PulsarAuthEnum;

    use super::*;

    const SECRET_BASE_PATH: &str = "/tmp/numaflow";

    fn setup_secret(name: &str, key: &str, value: &str) {
        let path = format!("{SECRET_BASE_PATH}/{name}");
        std::fs::create_dir_all(&path).unwrap();
        std::fs::write(format!("{path}/{key}"), value).unwrap();
    }

    fn cleanup_secret(name: &str) {
        let path = format!("{SECRET_BASE_PATH}/{name}");
        if std::path::Path::new(&path).exists() {
            std::fs::remove_dir_all(&path).unwrap();
        }
    }

    #[test]
    fn test_try_from_pulsar_source_without_auth() {
        // Test case: Valid configuration without authentication
        let valid_pulsar_source = Box::new(PulsarSource {
            auth: None,
            consumer_name: "test-consumer".to_string(),
            server_addr: "pulsar://localhost:6650".to_string(),
            subscription_name: "test-subscription".to_string(),
            topic: "persistent://public/default/test-topic".to_string(),
            max_unack: Some(1000),
        });

        let result = SourceType::try_from(valid_pulsar_source);
        assert!(result.is_ok());
        if let Ok(SourceType::Pulsar(config)) = result {
            assert_eq!(config.pulsar_server_addr, "pulsar://localhost:6650");
            assert_eq!(config.topic, "persistent://public/default/test-topic");
            assert_eq!(config.consumer_name, "test-consumer");
            assert_eq!(config.subscription, "test-subscription");
            assert_eq!(config.max_unack, 1000);
            assert!(config.auth.is_none());
        } else {
            panic!("Expected SourceType::Pulsar");
        }
    }

    #[test]
    fn test_try_from_pulsar_source_with_jwt_auth() {
        // Test case: Valid configuration with JWT authentication
        let secret_name = "test_try_from_pulsar_source_with_jwt_auth_jwt-secret";
        let token_key = "token";
        setup_secret(secret_name, token_key, "test-jwt-token");

        let valid_pulsar_source_with_auth = Box::new(PulsarSource {
            auth: Some(Box::new(PulsarAuth {
                token: Some(SecretKeySelector {
                    name: secret_name.to_string(),
                    key: token_key.to_string(),
                    ..Default::default()
                }),
                basic_auth: None,
            })),
            consumer_name: "test-consumer".to_string(),
            server_addr: "pulsar://localhost:6650".to_string(),
            subscription_name: "test-subscription".to_string(),
            topic: "persistent://public/default/test-topic".to_string(),
            max_unack: Some(1000),
        });

        let result = SourceType::try_from(valid_pulsar_source_with_auth);
        assert!(result.is_ok());
        if let Ok(SourceType::Pulsar(config)) = result {
            assert_eq!(config.pulsar_server_addr, "pulsar://localhost:6650");
            assert_eq!(config.topic, "persistent://public/default/test-topic");
            assert_eq!(config.consumer_name, "test-consumer");
            assert_eq!(config.subscription, "test-subscription");
            assert_eq!(config.max_unack, 1000);
            let auth = config.auth.unwrap();
            let PulsarAuthEnum::JWT(token) = auth else {
                panic!("Expected PulsarAuth::JWT");
            };
            assert_eq!(token, "test-jwt-token");
        } else {
            panic!("Expected SourceType::Pulsar");
        }

        cleanup_secret(secret_name);
    }

    #[test]
    fn test_try_from_pulsar_source_with_basic_auth() {
        // Test case: Valid configuration with basic authentication
        let secret_name = "test_try_from_pulsar_source_with_basic_auth_basic-secret";
        let user_key = "username";
        let pass_key = "password";
        setup_secret(secret_name, user_key, "test-user");
        setup_secret(secret_name, pass_key, "test-pass");

        let valid_pulsar_source_with_basic_auth = Box::new(PulsarSource {
            auth: Some(Box::new(PulsarAuth {
                token: None,
                basic_auth: Some(Box::new(PulsarBasicAuth {
                    username: Some(SecretKeySelector {
                        name: secret_name.to_string(),
                        key: user_key.to_string(),
                        ..Default::default()
                    }),
                    password: Some(SecretKeySelector {
                        name: secret_name.to_string(),
                        key: pass_key.to_string(),
                        ..Default::default()
                    }),
                })),
            })),
            consumer_name: "test-consumer".to_string(),
            server_addr: "pulsar://localhost:6650".to_string(),
            subscription_name: "test-subscription".to_string(),
            topic: "persistent://public/default/test-topic".to_string(),
            max_unack: Some(1000),
        });

        let result = SourceType::try_from(valid_pulsar_source_with_basic_auth);
        assert!(result.is_ok());
        if let Ok(SourceType::Pulsar(config)) = result {
            assert_eq!(config.pulsar_server_addr, "pulsar://localhost:6650");
            assert_eq!(config.topic, "persistent://public/default/test-topic");
            assert_eq!(config.consumer_name, "test-consumer");
            assert_eq!(config.subscription, "test-subscription");
            assert_eq!(config.max_unack, 1000);
            let auth = config.auth.unwrap();
            let PulsarAuthEnum::HTTPBasic { username, password } = auth else {
                panic!("Expected PulsarAuth::HTTPBasic");
            };
            assert_eq!(username, "test-user");
            assert_eq!(password, "test-pass");
        } else {
            panic!("Expected SourceType::Pulsar");
        }

        cleanup_secret(secret_name);
    }

    #[test]
    fn test_try_from_pulsar_source_with_missing_username() {
        // Test case: Basic auth is specified but username is missing
        let secret_name = "test_try_from_pulsar_source_with_missing_username_basic-secret";
        let pass_key = "password";
        setup_secret(secret_name, pass_key, "test-pass");

        let invalid_pulsar_source = Box::new(PulsarSource {
            auth: Some(Box::new(PulsarAuth {
                token: None,
                basic_auth: Some(Box::new(PulsarBasicAuth {
                    username: None,
                    password: Some(SecretKeySelector {
                        name: secret_name.to_string(),
                        key: pass_key.to_string(),
                        ..Default::default()
                    }),
                })),
            })),
            consumer_name: "test-consumer".to_string(),
            server_addr: "pulsar://localhost:6650".to_string(),
            subscription_name: "test-subscription".to_string(),
            topic: "persistent://public/default/test-topic".to_string(),
            max_unack: Some(1000),
        });

        let result = SourceType::try_from(invalid_pulsar_source);
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().to_string(),
            "Config Error - Username can not be empty for basic auth"
        );

        cleanup_secret(secret_name);
    }

    #[test]
    fn test_try_from_pulsar_source_with_missing_password() {
        // Test case: Basic auth is specified but password is missing
        let secret_name = "test_try_from_pulsar_source_with_missing_password_basic-secret";
        let user_key = "username";
        setup_secret(secret_name, user_key, "test-user");

        let invalid_pulsar_source = Box::new(PulsarSource {
            auth: Some(Box::new(PulsarAuth {
                token: None,
                basic_auth: Some(Box::new(PulsarBasicAuth {
                    username: Some(SecretKeySelector {
                        name: secret_name.to_string(),
                        key: user_key.to_string(),
                        ..Default::default()
                    }),
                    password: None,
                })),
            })),
            consumer_name: "test-consumer".to_string(),
            server_addr: "pulsar://localhost:6650".to_string(),
            subscription_name: "test-subscription".to_string(),
            topic: "persistent://public/default/test-topic".to_string(),
            max_unack: Some(1000),
        });

        let result = SourceType::try_from(invalid_pulsar_source);
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().to_string(),
            "Config Error - Password can not be empty for basic auth"
        );

        cleanup_secret(secret_name);
    }

    #[test]
    fn test_try_from_pulsar_source_with_invalid_jwt_secret() {
        // Test case: JWT auth is specified but secret file doesn't exist
        let invalid_pulsar_source = Box::new(PulsarSource {
            auth: Some(Box::new(PulsarAuth {
                token: Some(SecretKeySelector {
                    name: "non-existent-secret".to_string(),
                    key: "token".to_string(),
                    ..Default::default()
                }),
                basic_auth: None,
            })),
            consumer_name: "test-consumer".to_string(),
            server_addr: "pulsar://localhost:6650".to_string(),
            subscription_name: "test-subscription".to_string(),
            topic: "persistent://public/default/test-topic".to_string(),
            max_unack: Some(1000),
        });

        let result = SourceType::try_from(invalid_pulsar_source);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            err.to_string()
                .contains("Failed to get token secret from volume")
        );
    }

    #[test]
    fn test_try_from_pulsar_source_with_invalid_basic_auth_secret() {
        // Test case: Basic auth is specified but secret file doesn't exist
        let invalid_pulsar_source = Box::new(PulsarSource {
            auth: Some(Box::new(PulsarAuth {
                token: None,
                basic_auth: Some(Box::new(PulsarBasicAuth {
                    username: Some(SecretKeySelector {
                        name: "non-existent-secret".to_string(),
                        key: "username".to_string(),
                        ..Default::default()
                    }),
                    password: Some(SecretKeySelector {
                        name: "non-existent-secret".to_string(),
                        key: "password".to_string(),
                        ..Default::default()
                    }),
                })),
            })),
            consumer_name: "test-consumer".to_string(),
            server_addr: "pulsar://localhost:6650".to_string(),
            subscription_name: "test-subscription".to_string(),
            topic: "persistent://public/default/test-topic".to_string(),
            max_unack: Some(1000),
        });

        let result = SourceType::try_from(invalid_pulsar_source);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            err.to_string()
                .contains("Failed to get username secret from volume")
        );
    }
}

#[cfg(test)]
mod nats_source_tests {
    use super::*;
    use k8s_openapi::api::core::v1::SecretKeySelector;
    use numaflow_models::models::NatsSource;
    use numaflow_models::models::{BasicAuth, NatsAuth};

    const SECRET_BASE_PATH: &str = "/tmp/numaflow";

    #[test]
    fn test_try_from_nats_source_with_basic_auth() {
        // Setup fake secrets
        let secret_name = "nats-basic-auth-secret";
        let user_key = "username";
        let pass_key = "password";
        let path = format!("{SECRET_BASE_PATH}/{secret_name}");
        std::fs::create_dir_all(&path).unwrap();
        std::fs::write(format!("{path}/{user_key}"), "test-user").unwrap();
        std::fs::write(format!("{path}/{pass_key}"), "test-pass").unwrap();

        let nats_source = Box::new(NatsSource {
            url: "nats://localhost:4222".to_string(),
            subject: "test-subject".to_string(),
            queue: "test-queue".to_string(),
            auth: Some(Box::new(NatsAuth {
                basic: Some(Box::new(BasicAuth {
                    user: Some(SecretKeySelector {
                        name: secret_name.to_string(),
                        key: user_key.to_string(),
                        ..Default::default()
                    }),
                    password: Some(SecretKeySelector {
                        name: secret_name.to_string(),
                        key: pass_key.to_string(),
                        ..Default::default()
                    }),
                })),
                nkey: None,
                token: None,
            })),
            tls: None,
        });

        let source_type = SourceType::try_from(nats_source).unwrap();
        if let SourceType::Nats(config) = source_type {
            let numaflow_nats::NatsAuth::Basic { username, password } = config.auth.unwrap() else {
                panic!("Basic auth creds must be set");
            };
            assert_eq!(username, "test-user");
            assert_eq!(password, "test-pass");
            assert_eq!(config.addr, "nats://localhost:4222");
            assert_eq!(config.subject, "test-subject");
            assert_eq!(config.queue, "test-queue");
        } else {
            panic!("Expected SourceType::Nats");
        }

        // Cleanup
        if std::path::Path::new(&path).exists() {
            std::fs::remove_dir_all(&path).unwrap();
        }
    }

    #[test]
    fn test_try_from_nats_source_basic() {
        let nats_source = Box::new(NatsSource {
            url: "nats://localhost:4222".to_string(),
            subject: "test-subject".to_string(),
            queue: "test-queue".to_string(),
            auth: None,
            tls: None,
        });

        let source_type = SourceType::try_from(nats_source).unwrap();
        if let SourceType::Nats(config) = source_type {
            assert_eq!(config.addr, "nats://localhost:4222");
            assert_eq!(config.subject, "test-subject");
            assert_eq!(config.queue, "test-queue");
            assert!(config.auth.is_none());
            assert!(config.tls.is_none());
        } else {
            panic!("Expected SourceType::Nats");
        }
    }

    #[test]
    fn test_try_from_nats_source_with_tls() {
        use k8s_openapi::api::core::v1::SecretKeySelector;
        use numaflow_models::models::Tls;

        let test_name = "test_try_from_nats_source_with_tls";
        let ca_cert_name = format!("{test_name}-tls-ca-cert");
        let cert_name = format!("{test_name}-tls-cert");
        let key_name = format!("{test_name}-tls-key");

        // Setup fake secrets
        let setup_secret = |name: &str, key: &str, value: &str| {
            let path = format!("{SECRET_BASE_PATH}/{name}");
            std::fs::create_dir_all(&path).unwrap();
            std::fs::write(format!("{path}/{key}"), value).unwrap();
        };
        setup_secret(&ca_cert_name, "ca", "test-ca-cert");
        setup_secret(&cert_name, "cert", "test-cert");
        setup_secret(&key_name, "key", "test-key");

        let nats_source = Box::new(NatsSource {
            url: "nats://localhost:4222".to_string(),
            subject: "test-subject".to_string(),
            queue: "test-queue".to_string(),
            auth: None,
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
        });

        let source_type = SourceType::try_from(nats_source).unwrap();
        if let SourceType::Nats(config) = source_type {
            let tls_config = config.tls.unwrap();
            assert_eq!(tls_config.ca_cert.unwrap(), "test-ca-cert");
            let client_auth = tls_config.client_auth.as_ref().unwrap();
            assert_eq!(client_auth.client_cert, "test-cert");
            assert_eq!(client_auth.client_cert_private_key, "test-key");
            assert!(!tls_config.insecure_skip_verify);
        } else {
            panic!("Expected SourceType::Nats");
        }

        // Cleanup
        let cleanup_secret = |name: &str| {
            let path = format!("{SECRET_BASE_PATH}/{name}");
            if std::path::Path::new(&path).exists() {
                std::fs::remove_dir_all(&path).unwrap();
            }
        };
        cleanup_secret(&ca_cert_name);
        cleanup_secret(&cert_name);
        cleanup_secret(&key_name);
    }

    #[test]
    fn test_try_from_nats_source_with_invalid_auth() {
        use numaflow_models::models::NatsAuth;

        let nats_source = Box::new(NatsSource {
            url: "nats://localhost:4222".to_string(),
            subject: "test-subject".to_string(),
            queue: "test-queue".to_string(),
            auth: Some(Box::new(NatsAuth {
                basic: None,
                nkey: None,
                token: None,
            })),
            tls: None,
        });

        let result = SourceType::try_from(nats_source);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(
            err.to_string(),
            "Config Error - Authentication is specified, but auth setting is empty"
        );
    }
}
