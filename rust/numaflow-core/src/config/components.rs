use numaflow_kafka::TlsConfig;
use numaflow_models::models::{Sasl, Tls};

use crate::Error;

pub(crate) mod source {
    const DEFAULT_GRPC_MAX_MESSAGE_SIZE: usize = 64 * 1024 * 1024; // 64 MB
    const DEFAULT_SOURCE_SOCKET: &str = "/var/run/numaflow/source.sock";
    const DEFAULT_SOURCE_SERVER_INFO_FILE: &str = "/var/run/numaflow/sourcer-server-info";

    use std::collections::HashMap;
    use std::{fmt::Debug, time::Duration};

    use super::parse_kafka_auth_config;
    use crate::Result;
    use crate::config::get_vertex_name;
    use crate::error::Error;
    use bytes::Bytes;
    use numaflow_jetstream::{JetstreamSourceConfig, NatsAuth, TlsClientAuthCerts, TlsConfig};
    use numaflow_kafka::source::KafkaSourceConfig;
    use numaflow_models::models::{GeneratorSource, PulsarSource, Source, SqsSource};
    use numaflow_pulsar::{PulsarAuth, source::PulsarSourceConfig};
    use numaflow_sqs::source::SqsSourceConfig;
    use serde::{Deserialize, Serialize};
    use tracing::warn;

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

    #[derive(Debug, Clone, PartialEq)]
    pub(crate) enum SourceType {
        Generator(GeneratorConfig),
        UserDefined(UserDefinedConfig),
        Pulsar(PulsarSourceConfig),
        Jetstream(JetstreamSourceConfig),
        Sqs(SqsSourceConfig),
        Kafka(Box<KafkaSourceConfig>),
        Http(numaflow_http::HttpSourceConfig),
    }

    impl From<Box<GeneratorSource>> for SourceType {
        fn from(generator: Box<GeneratorSource>) -> Self {
            let mut generator_config = GeneratorConfig::default();

            if let Some(value_blob) = &generator.value_blob {
                generator_config.content = Bytes::from(value_blob.clone());
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
            generator_config.duration =
                generator.duration.map_or(Duration::from_millis(1000), |d| {
                    std::time::Duration::from(d)
                });
            generator_config.key_count = generator
                .key_count
                .map_or(0, |kc| std::cmp::min(kc, u8::MAX as i32) as u8);
            generator_config.jitter = generator
                .jitter
                .map_or(Duration::from_secs(0), std::time::Duration::from);

            SourceType::Generator(generator_config)
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
            if value.aws_region.is_empty() {
                return Err(Error::Config(
                    "aws_region is required for SQS source".to_string(),
                ));
            }

            if value.queue_name.is_empty() {
                return Err(Error::Config(
                    "queue_name is required for SQS source".to_string(),
                ));
            }

            if value.queue_owner_aws_account_id.is_empty() {
                return Err(Error::Config(
                    "queue_owner_aws_account_id is required for SQS source".to_string(),
                ));
            }

            if let Some(timeout) = value.visibility_timeout {
                if !(0..=43200).contains(&timeout) {
                    return Err(Error::Config(format!(
                        "visibility_timeout must be between 0 and 43200 for SQS source, got {}",
                        timeout
                    )));
                }
            }

            if let Some(wait_time) = value.wait_time_seconds {
                if !(0..=20).contains(&wait_time) {
                    return Err(Error::Config(format!(
                        "wait_time_seconds must be between 0 and 20 for SQS source, got {}",
                        wait_time
                    )));
                }
            }

            if let Some(max_number_of_messages) = value.max_number_of_messages {
                if !(1..=10).contains(&max_number_of_messages) {
                    return Err(Error::Config(format!(
                        "max_number_of_messages must be between 1 and 10 for SQS source, got {}",
                        max_number_of_messages
                    )));
                }
            }

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
            };

            Ok(SourceType::Sqs(sqs_source_config))
        }
    }

    impl TryFrom<Box<numaflow_models::models::JetStreamSource>> for SourceType {
        type Error = Error;
        fn try_from(
            value: Box<numaflow_models::models::JetStreamSource>,
        ) -> std::result::Result<Self, Self::Error> {
            let auth: Option<NatsAuth> = match value.auth {
                Some(auth) => {
                    if let Some(basic_auth) = auth.basic {
                        let user_secret_selector = &basic_auth.user.ok_or_else(|| {
                            Error::Config("Username can not be empty for basic auth".into())
                        })?;
                        let username = crate::shared::create_components::get_secret_from_volume(
                            &user_secret_selector.name,
                            &user_secret_selector.key,
                        )
                        .map_err(|e| {
                            Error::Config(format!("Failed to get username secret: {e:?}"))
                        })?;

                        let password_secret_selector = &basic_auth.password.ok_or_else(|| {
                            Error::Config("Password can not be empty for basic auth".into())
                        })?;
                        let password = crate::shared::create_components::get_secret_from_volume(
                            &password_secret_selector.name,
                            &password_secret_selector.key,
                        )
                        .map_err(|e| {
                            Error::Config(format!("Failed to get password secret: {e:?}"))
                        })?;
                        Some(NatsAuth::Basic { username, password })
                    } else if let Some(nkey_auth) = auth.nkey {
                        let nkey = crate::shared::create_components::get_secret_from_volume(
                            &nkey_auth.name,
                            &nkey_auth.key,
                        )
                        .map_err(|e| Error::Config(format!("Failed to get nkey secret: {e:?}")))?;
                        Some(NatsAuth::NKey(nkey))
                    } else if let Some(token_auth) = auth.token {
                        let token = crate::shared::create_components::get_secret_from_volume(
                            &token_auth.name,
                            &token_auth.key,
                        )
                        .map_err(|e| Error::Config(format!("Failed to get token secret: {e:?}")))?;
                        Some(NatsAuth::Token(token))
                    } else {
                        return Err(Error::Config(
                            "Authentication is specified, but auth setting is empty".into(),
                        ));
                    }
                }
                None => None,
            };

            let tls = if let Some(tls_config) = value.tls {
                let tls_skip_verify = tls_config.insecure_skip_verify.unwrap_or(false);
                if tls_skip_verify {
                    Some(TlsConfig {
                        insecure_skip_verify: true,
                        ca_cert: None,
                        client_auth: None,
                    })
                } else {
                    let ca_cert = tls_config
                        .ca_cert_secret
                        .map(|ca_cert_secret| {
                            match crate::shared::create_components::get_secret_from_volume(
                                &ca_cert_secret.name,
                                &ca_cert_secret.key,
                            ) {
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
                                crate::shared::create_components::get_secret_from_volume(
                                    &client_cert_secret.name,
                                    &client_cert_secret.key,
                                )
                                .map_err(|e| {
                                    Error::Config(format!(
                                        "Failed to get client cert secret: {e:?}"
                                    ))
                                })?;

                            let Some(private_key_secret) = tls_config.key_secret else {
                                return Err(Error::Config("Client cert is specified for TLS authentication, but private key is not specified".into()));
                            };

                            let client_cert_private_key =
                                crate::shared::create_components::get_secret_from_volume(
                                    &private_key_secret.name,
                                    &private_key_secret.key,
                                )
                                .map_err(|e| {
                                    Error::Config(format!(
                                        "Failed to get client cert private key secret: {e:?}"
                                    ))
                                })?;
                            Some(TlsClientAuthCerts {
                                client_cert,
                                client_cert_private_key,
                            })
                        }
                        None => None,
                    };

                    Some(TlsConfig {
                        insecure_skip_verify: tls_config.insecure_skip_verify.unwrap_or(false),
                        ca_cert,
                        client_auth: tls_client_auth_certs,
                    })
                }
            } else {
                None
            };

            let js_config = JetstreamSourceConfig {
                addr: value.url,
                consumer: value.stream.clone(),
                stream: value.stream,
                auth,
                tls,
            };
            Ok(SourceType::Jetstream(js_config))
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
                    .map(|parts| (parts[0].trim().to_string(), parts[1].trim().to_string()))
                    .collect::<HashMap<String, String>>(),
            };
            Ok(SourceType::Kafka(Box::new(kafka_config)))
        }
    }

    impl TryFrom<Box<Source>> for SourceType {
        type Error = Error;

        fn try_from(mut source: Box<Source>) -> Result<Self> {
            if let Some(generator) = source.generator.take() {
                return Ok(generator.into());
            }

            if source.udsource.is_some() {
                return Ok(SourceType::UserDefined(UserDefinedConfig::default()));
            }

            if let Some(pulsar) = source.pulsar.take() {
                return pulsar.try_into();
            }

            if let Some(sqs) = source.sqs.take() {
                return sqs.try_into();
            }

            if let Some(_serving) = source.serving.take() {
                panic!("Serving source is invalid");
            }

            if let Some(jetstream) = source.jetstream.take() {
                return jetstream.try_into();
            }

            if let Some(kafka) = source.kafka.take() {
                return kafka.try_into();
            }

            if let Some(http) = source.http.take() {
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

    // Retrieve value from mounted secret volume
    // "/var/numaflow/secrets/${secretRef.name}/${secretRef.key}" is expected to be the file path
    pub(crate) fn get_secret_from_volume(name: &str, key: &str) -> String {
        let path = format!("/var/numaflow/secrets/{name}/{key}");
        let val = std::fs::read_to_string(path.clone())
            .map_err(|e| format!("Reading secret from file {path}: {e:?}"))
            .expect("Failed to read secret");
        val.trim().into()
    }

    #[derive(Serialize, Deserialize, Debug, Clone)]
    struct AuthToken {
        /// Name of the configmap
        name: String,
        /// Key within the configmap
        key: String,
    }

    #[derive(Serialize, Deserialize, Debug, Clone)]
    struct Auth {
        token: AuthToken,
    }

    impl TryFrom<Box<numaflow_models::models::HttpSource>> for SourceType {
        type Error = Error;
        fn try_from(
            value: Box<numaflow_models::models::HttpSource>,
        ) -> std::result::Result<Self, Self::Error> {
            let mut http_config = numaflow_http::HttpSourceConfigBuilder::new(get_vertex_name());

            if let Some(auth) = value.auth {
                let auth = auth.token.unwrap();
                let token = get_secret_from_volume(&auth.name, &auth.key);
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
}

pub(crate) mod sink {
    const DEFAULT_GRPC_MAX_MESSAGE_SIZE: usize = 64 * 1024 * 1024; // 64 MB
    const DEFAULT_SINK_SOCKET: &str = "/var/run/numaflow/sink.sock";
    const DEFAULT_SINK_SERVER_INFO_FILE: &str = "/var/run/numaflow/sinker-server-info";
    const DEFAULT_FB_SINK_SOCKET: &str = "/var/run/numaflow/fb-sink.sock";
    const DEFAULT_FB_SINK_SERVER_INFO_FILE: &str = "/var/run/numaflow/fb-sinker-server-info";
    const DEFAULT_SINK_RETRY_ON_FAIL_STRATEGY: OnFailureStrategy = OnFailureStrategy::Retry;
    const DEFAULT_MAX_SINK_RETRY_ATTEMPTS: u16 = u16::MAX;
    const DEFAULT_SINK_INITIAL_RETRY_INTERVAL_IN_MS: u32 = 1;
    const DEFAULT_SINK_MAX_RETRY_INTERVAL_IN_MS: u32 = u32::MAX;
    const DEFAULT_SINK_RETRY_FACTOR: f64 = 1.0;
    const DEFAULT_SINK_RETRY_JITTER: f64 = 0.0;

    use std::collections::HashMap;
    use std::fmt::Display;

    use numaflow_kafka::sink::KafkaSinkConfig;
    use numaflow_models::models::{Backoff, KafkaSink, PulsarSink, RetryStrategy, Sink, SqsSink};
    use numaflow_pulsar::PulsarAuth;
    use numaflow_pulsar::sink::Config as PulsarSinkConfig;
    use numaflow_sqs::sink::SqsSinkConfig;

    use crate::Result;
    use crate::error::Error;

    use super::parse_kafka_auth_config;

    #[derive(Debug, Clone, PartialEq)]
    pub(crate) struct SinkConfig {
        pub(crate) sink_type: SinkType,
        pub(crate) retry_config: Option<RetryConfig>,
    }

    #[derive(Debug, Clone, PartialEq)]
    pub(crate) enum SinkType {
        Log(LogConfig),
        Blackhole(BlackholeConfig),
        Serve,
        UserDefined(UserDefinedConfig),
        Sqs(SqsSinkConfig),
        Kafka(Box<KafkaSinkConfig>),
        Pulsar(Box<PulsarSinkConfig>),
    }

    impl SinkType {
        // FIXME(cr): why is sink.fallback Box<AbstrackSink> vs. sink Box<Sink>. This is coming from
        //   numaflow-models. Problem is, golang has embedded structures and rust does not. We might
        //   have to AbstractSink for sink-configs while Sink for real sink types.
        //   NOTE: I do not see this problem with Source?
        pub(crate) fn primary_sinktype(sink: &Sink) -> Result<Self> {
            sink.udsink
                .as_ref()
                .map(|_| Ok(SinkType::UserDefined(UserDefinedConfig::default())))
                .or_else(|| {
                    sink.log
                        .as_ref()
                        .map(|_| Ok(SinkType::Log(LogConfig::default())))
                })
                .or_else(|| {
                    sink.blackhole
                        .as_ref()
                        .map(|_| Ok(SinkType::Blackhole(BlackholeConfig::default())))
                })
                .or_else(|| sink.serve.as_ref().map(|_| Ok(SinkType::Serve)))
                .or_else(|| sink.sqs.as_ref().map(|sqs| sqs.clone().try_into()))
                .or_else(|| sink.kafka.as_ref().map(|kafka| kafka.clone().try_into()))
                .or_else(|| sink.pulsar.as_ref().map(|pulsar| pulsar.clone().try_into()))
                .ok_or_else(|| Error::Config("Sink type not found".to_string()))?
        }

        pub(crate) fn fallback_sinktype(sink: &Sink) -> Result<Self> {
            if let Some(fallback) = sink.fallback.as_ref() {
                fallback
                    .udsink
                    .as_ref()
                    .map(|_| Ok(SinkType::UserDefined(UserDefinedConfig::fallback_default())))
                    .or_else(|| {
                        fallback
                            .log
                            .as_ref()
                            .map(|_| Ok(SinkType::Log(LogConfig::default())))
                    })
                    .or_else(|| {
                        fallback
                            .blackhole
                            .as_ref()
                            .map(|_| Ok(SinkType::Blackhole(BlackholeConfig::default())))
                    })
                    .or_else(|| fallback.serve.as_ref().map(|_| Ok(SinkType::Serve)))
                    .or_else(|| fallback.sqs.as_ref().map(|sqs| sqs.clone().try_into()))
                    .or_else(|| {
                        fallback
                            .kafka
                            .as_ref()
                            .map(|kafka| kafka.clone().try_into())
                    })
                    .or_else(|| {
                        fallback
                            .pulsar
                            .as_ref()
                            .map(|pulsar| pulsar.clone().try_into())
                    })
                    .ok_or_else(|| Error::Config("Sink type not found".to_string()))?
            } else {
                Err(Error::Config("Fallback sink not found".to_string()))
            }
        }
    }

    #[derive(Debug, Clone, PartialEq, Default)]
    pub(crate) struct LogConfig {}

    #[derive(Debug, Clone, PartialEq, Default)]
    pub(crate) struct BlackholeConfig {}

    #[derive(Debug, Clone, PartialEq)]
    pub(crate) struct UserDefinedConfig {
        pub grpc_max_message_size: usize,
        pub socket_path: String,
        pub server_info_path: String,
    }

    impl TryFrom<Box<SqsSink>> for SinkType {
        type Error = Error;

        fn try_from(value: Box<SqsSink>) -> Result<Self> {
            if value.aws_region.is_empty() {
                return Err(Error::Config(
                    "AWS region is required for SQS sink".to_string(),
                ));
            }

            if value.queue_name.is_empty() {
                return Err(Error::Config(
                    "Queue name is required for SQS sink".to_string(),
                ));
            }

            if value.queue_owner_aws_account_id.is_empty() {
                return Err(Error::Config(
                    "Queue owner AWS account ID is required for SQS sink".to_string(),
                ));
            }

            let sqs_sink_config = SqsSinkConfig {
                queue_name: Box::leak(value.queue_name.into_boxed_str()),
                region: Box::leak(value.aws_region.into_boxed_str()),
                queue_owner_aws_account_id: Box::leak(
                    value.queue_owner_aws_account_id.into_boxed_str(),
                ),
            };
            Ok(SinkType::Sqs(sqs_sink_config))
        }
    }

    impl TryFrom<Box<KafkaSink>> for SinkType {
        type Error = Error;

        fn try_from(kafka_config: Box<KafkaSink>) -> Result<Self> {
            let Some(brokers) = kafka_config.brokers else {
                return Err(Error::Config(
                    "Brokers must be specified in the Kafka sink config".to_string(),
                ));
            };
            if brokers.is_empty() {
                return Err(Error::Config(
                    "At-least 1 broker URL must be specified in Kafka sink config".to_string(),
                ));
            }

            let (auth, tls) =
                parse_kafka_auth_config(kafka_config.sasl.clone(), kafka_config.tls.clone())?;

            Ok(SinkType::Kafka(Box::new(KafkaSinkConfig {
                brokers,
                topic: kafka_config.topic,
                auth,
                tls,
                set_partition_key: kafka_config.set_key.unwrap_or(false),
                // config is multiline string with key: value pairs.
                // Eg:
                //  max.poll.interval.ms: 100
                //  socket.timeout.ms: 10000
                //  queue.buffering.max.ms: 10000
                kafka_raw_config: kafka_config
                    .config
                    .unwrap_or_default()
                    .trim()
                    .split('\n')
                    .map(|s| s.split(':').collect::<Vec<&str>>())
                    .filter(|parts| parts.len() == 2)
                    .map(|parts| (parts[0].trim().to_string(), parts[1].trim().to_string()))
                    .collect::<HashMap<String, String>>(),
            })))
        }
    }

    impl TryFrom<Box<PulsarSink>> for SinkType {
        type Error = Error;
        fn try_from(sink_config: Box<PulsarSink>) -> std::result::Result<Self, Self::Error> {
            let auth: Option<PulsarAuth> = super::parse_pulsar_auth_config(sink_config.auth)?;
            let pulsar_sink_config = numaflow_pulsar::sink::Config {
                addr: sink_config.server_addr,
                topic: sink_config.topic,
                producer_name: sink_config.producer_name,
                auth,
            };
            Ok(SinkType::Pulsar(Box::new(pulsar_sink_config)))
        }
    }

    #[derive(Debug, Clone, PartialEq)]
    pub(crate) enum OnFailureStrategy {
        Retry,
        Fallback,
        Drop,
    }

    impl OnFailureStrategy {
        /// Converts a string slice to an `OnFailureStrategy` enum variant.
        /// Case insensitivity is considered to enhance usability.
        ///
        /// # Arguments
        /// * `s` - A string slice representing the retry strategy.
        ///
        /// # Returns
        /// An option containing the corresponding enum variant if successful,
        /// or DefaultStrategy if the input does not match known variants.
        pub(crate) fn from_str(s: &str) -> Self {
            match s.to_lowercase().as_str() {
                "retry" => OnFailureStrategy::Retry,
                "fallback" => OnFailureStrategy::Fallback,
                "drop" => OnFailureStrategy::Drop,
                _ => DEFAULT_SINK_RETRY_ON_FAIL_STRATEGY,
            }
        }
    }

    impl Display for OnFailureStrategy {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            match *self {
                OnFailureStrategy::Retry => write!(f, "retry"),
                OnFailureStrategy::Fallback => write!(f, "fallback"),
                OnFailureStrategy::Drop => write!(f, "drop"),
            }
        }
    }

    #[derive(Debug, Clone, PartialEq)]
    pub(crate) struct RetryConfig {
        pub sink_max_retry_attempts: u16,
        pub sink_initial_retry_interval_in_ms: u32,
        pub sink_retry_factor: f64,
        pub sink_retry_jitter: f64,
        pub sink_max_retry_interval_in_ms: u32,
        pub sink_retry_on_fail_strategy: OnFailureStrategy,
        pub sink_default_retry_strategy: RetryStrategy,
    }

    impl Default for RetryConfig {
        fn default() -> Self {
            let default_retry_strategy = RetryStrategy {
                backoff: Option::from(Box::from(Backoff {
                    interval: Option::from(kube::core::Duration::from(
                        std::time::Duration::from_millis(
                            DEFAULT_SINK_INITIAL_RETRY_INTERVAL_IN_MS as u64,
                        ),
                    )),
                    steps: Option::from(DEFAULT_MAX_SINK_RETRY_ATTEMPTS as i64),
                    cap: Option::from(kube::core::Duration::from(
                        std::time::Duration::from_millis(
                            DEFAULT_SINK_MAX_RETRY_INTERVAL_IN_MS as u64,
                        ),
                    )),
                    factor: Option::from(DEFAULT_SINK_RETRY_FACTOR),
                    jitter: Option::from(DEFAULT_SINK_RETRY_JITTER),
                })),
                on_failure: Option::from(DEFAULT_SINK_RETRY_ON_FAIL_STRATEGY.to_string()),
            };
            Self {
                sink_max_retry_attempts: DEFAULT_MAX_SINK_RETRY_ATTEMPTS,
                sink_initial_retry_interval_in_ms: DEFAULT_SINK_INITIAL_RETRY_INTERVAL_IN_MS,
                sink_max_retry_interval_in_ms: DEFAULT_SINK_MAX_RETRY_INTERVAL_IN_MS,
                sink_retry_factor: DEFAULT_SINK_RETRY_FACTOR,
                sink_retry_jitter: DEFAULT_SINK_RETRY_JITTER,
                sink_retry_on_fail_strategy: DEFAULT_SINK_RETRY_ON_FAIL_STRATEGY,
                sink_default_retry_strategy: default_retry_strategy,
            }
        }
    }

    impl From<Box<RetryStrategy>> for RetryConfig {
        fn from(retry: Box<RetryStrategy>) -> Self {
            let mut retry_config = RetryConfig::default();
            if let Some(backoff) = &retry.backoff {
                if let Some(interval) = backoff.interval {
                    retry_config.sink_initial_retry_interval_in_ms =
                        std::time::Duration::from(interval).as_millis() as u32;
                }

                if let Some(steps) = backoff.steps {
                    retry_config.sink_max_retry_attempts = steps as u16;
                }

                if let Some(factor) = backoff.factor {
                    retry_config.sink_retry_factor = factor;
                }

                if let Some(jitter) = backoff.jitter {
                    retry_config.sink_retry_jitter = jitter;
                }

                if let Some(cap) = backoff.cap {
                    retry_config.sink_max_retry_interval_in_ms =
                        std::time::Duration::from(cap).as_millis() as u32;
                }
            }

            if let Some(strategy) = &retry.on_failure {
                retry_config.sink_retry_on_fail_strategy = OnFailureStrategy::from_str(strategy);
            }
            retry_config
        }
    }

    impl Default for UserDefinedConfig {
        fn default() -> Self {
            Self {
                grpc_max_message_size: DEFAULT_GRPC_MAX_MESSAGE_SIZE,
                socket_path: DEFAULT_SINK_SOCKET.to_string(),
                server_info_path: DEFAULT_SINK_SERVER_INFO_FILE.to_string(),
            }
        }
    }

    impl UserDefinedConfig {
        pub(crate) fn fallback_default() -> Self {
            Self {
                grpc_max_message_size: DEFAULT_GRPC_MAX_MESSAGE_SIZE,
                socket_path: DEFAULT_FB_SINK_SOCKET.to_string(),
                server_info_path: DEFAULT_FB_SINK_SERVER_INFO_FILE.to_string(),
            }
        }
    }
}

pub(crate) mod transformer {
    const DEFAULT_GRPC_MAX_MESSAGE_SIZE: usize = 64 * 1024 * 1024; // 64 MB
    const DEFAULT_TRANSFORMER_SOCKET: &str = "/var/run/numaflow/sourcetransform.sock";
    const DEFAULT_TRANSFORMER_SERVER_INFO_FILE: &str =
        "/var/run/numaflow/sourcetransformer-server-info";

    #[derive(Debug, Clone, PartialEq)]
    pub(crate) struct TransformerConfig {
        pub(crate) concurrency: usize,
        pub(crate) transformer_type: TransformerType,
    }

    #[derive(Debug, Clone, PartialEq)]
    pub(crate) enum TransformerType {
        #[allow(dead_code)]
        Noop(NoopConfig), // will add built-in transformers
        UserDefined(UserDefinedConfig),
    }

    #[derive(Debug, Clone, PartialEq)]
    pub(crate) struct NoopConfig {}

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
                socket_path: DEFAULT_TRANSFORMER_SOCKET.to_string(),
                server_info_path: DEFAULT_TRANSFORMER_SERVER_INFO_FILE.to_string(),
            }
        }
    }
}

pub(crate) mod metrics {
    const DEFAULT_METRICS_PORT: u16 = 2469;
    const DEFAULT_LAG_CHECK_INTERVAL_IN_SECS: u16 = 5;
    const DEFAULT_LAG_REFRESH_INTERVAL_IN_SECS: u16 = 3;
    const DEFAULT_LOOKBACK_WINDOW_IN_SECS: u16 = 120;

    #[derive(Debug, Clone, PartialEq)]
    pub(crate) struct MetricsConfig {
        // TODO(lookback) - using new implementation for monovertex right now,
        // remove extra fields from here once new corresponding pipeline changes
        // in the daemon are done.
        pub metrics_server_listen_port: u16,
        pub lag_check_interval_in_secs: u16,
        pub lag_refresh_interval_in_secs: u16,
        pub lookback_window_in_secs: u16,
    }

    impl Default for MetricsConfig {
        fn default() -> Self {
            Self {
                metrics_server_listen_port: DEFAULT_METRICS_PORT,
                lag_check_interval_in_secs: DEFAULT_LAG_CHECK_INTERVAL_IN_SECS,
                lag_refresh_interval_in_secs: DEFAULT_LAG_REFRESH_INTERVAL_IN_SECS,
                lookback_window_in_secs: DEFAULT_LOOKBACK_WINDOW_IN_SECS,
            }
        }
    }

    impl MetricsConfig {
        pub(crate) fn with_lookback_window_in_secs(lookback_window_in_secs: u16) -> Self {
            MetricsConfig {
                lookback_window_in_secs,
                ..Default::default()
            }
        }
    }
}

pub(crate) mod reduce {
    const DEFAULT_GRPC_MAX_MESSAGE_SIZE: usize = 64 * 1024 * 1024; // 64 MB
    const DEFAULT_REDUCER_SOCKET: &str = "/var/run/numaflow/reduce.sock";
    const DEFAULT_REDUCER_SERVER_INFO_FILE: &str = "/var/run/numaflow/reducer-server-info";
    const DEFAULT_ACCUMULATOR_REDUCER_SOCKET: &str = "/var/run/numaflow/accumulator.sock";
    const DEFAULT_ACCUMULATOR_REDUCER_SERVER_INFO_FILE: &str =
        "/var/run/numaflow/accumulator-server-info";
    const DEFAULT_SESSION_REDUCER_SOCKET: &str = "/var/run/numaflow/sessionreduce.sock";
    const DEFAULT_SESSION_REDUCER_SERVER_INFO_FILE: &str =
        "/var/run/numaflow/sessionreducer-server-info";

    use std::time::Duration;

    use numaflow_models::models::{
        AccumulatorWindow, FixedWindow, GroupBy, PbqStorage, SessionWindow, SlidingWindow,
    };

    use crate::Result;
    use crate::error::Error;

    #[derive(Debug, Clone, PartialEq)]
    pub(crate) enum ReducerConfig {
        Aligned(AlignedReducerConfig),
        Unaligned(UnalignedReducerConfig),
    }

    #[derive(Debug, Clone, PartialEq)]
    pub(crate) struct AlignedReducerConfig {
        pub(crate) user_defined_config: UserDefinedConfig,
        pub(crate) window_config: AlignedWindowConfig,
    }

    #[derive(Debug, Clone, PartialEq)]
    pub(crate) struct UnalignedReducerConfig {
        pub(crate) user_defined_config: UserDefinedConfig,
        pub(crate) window_config: UnalignedWindowConfig,
    }

    #[derive(Debug, Clone, PartialEq)]
    pub(crate) struct UserDefinedConfig {
        pub grpc_max_message_size: usize,
        pub socket_path: &'static str,
        pub server_info_path: &'static str,
    }

    impl Default for UserDefinedConfig {
        fn default() -> Self {
            Self {
                grpc_max_message_size: DEFAULT_GRPC_MAX_MESSAGE_SIZE,
                socket_path: DEFAULT_REDUCER_SOCKET,
                server_info_path: DEFAULT_REDUCER_SERVER_INFO_FILE,
            }
        }
    }

    impl UserDefinedConfig {
        pub(crate) fn session_config() -> Self {
            Self {
                grpc_max_message_size: DEFAULT_GRPC_MAX_MESSAGE_SIZE,
                socket_path: DEFAULT_SESSION_REDUCER_SOCKET,
                server_info_path: DEFAULT_SESSION_REDUCER_SERVER_INFO_FILE,
            }
        }

        pub(crate) fn accumulator_config() -> Self {
            Self {
                grpc_max_message_size: DEFAULT_GRPC_MAX_MESSAGE_SIZE,
                socket_path: DEFAULT_ACCUMULATOR_REDUCER_SOCKET,
                server_info_path: DEFAULT_ACCUMULATOR_REDUCER_SERVER_INFO_FILE,
            }
        }
    }

    #[derive(Debug, Clone, PartialEq)]
    pub(crate) struct AlignedWindowConfig {
        pub(crate) window_type: AlignedWindowType,
        pub(crate) allowed_lateness: Duration,
        pub(crate) is_keyed: bool,
    }

    #[derive(Debug, Clone, PartialEq)]
    pub(crate) struct UnalignedWindowConfig {
        pub(crate) window_type: UnalignedWindowType,
        pub(crate) allowed_lateness: Duration,
        pub(crate) is_keyed: bool,
    }

    #[derive(Debug, Clone, PartialEq)]
    pub(crate) enum UnalignedWindowType {
        Accumulator(AccumulatorWindowConfig),
        Session(SessionWindowConfig),
    }

    #[derive(Debug, Clone, PartialEq)]
    pub(crate) enum AlignedWindowType {
        Fixed(FixedWindowConfig),
        Sliding(SlidingWindowConfig),
    }

    #[derive(Debug, Clone, PartialEq)]
    pub(crate) struct FixedWindowConfig {
        pub(crate) length: Duration,
    }

    impl From<Box<FixedWindow>> for FixedWindowConfig {
        fn from(value: Box<FixedWindow>) -> Self {
            Self {
                length: value.length.map(Duration::from).unwrap_or_default(),
            }
        }
    }

    #[derive(Debug, Clone, PartialEq)]
    pub(crate) struct SlidingWindowConfig {
        pub(crate) length: Duration,
        pub(crate) slide: Duration,
    }

    impl From<Box<SlidingWindow>> for SlidingWindowConfig {
        fn from(value: Box<SlidingWindow>) -> Self {
            Self {
                length: value.length.map(Duration::from).unwrap_or_default(),
                slide: value.slide.map(Duration::from).unwrap_or_default(),
            }
        }
    }

    #[derive(Debug, Clone, PartialEq)]
    pub(crate) struct SessionWindowConfig {
        pub(crate) timeout: Duration,
    }

    impl From<Box<SessionWindow>> for SessionWindowConfig {
        fn from(value: Box<SessionWindow>) -> Self {
            Self {
                timeout: value.timeout.map(Duration::from).unwrap_or_default(),
            }
        }
    }

    #[derive(Debug, Clone, PartialEq)]
    pub(crate) struct AccumulatorWindowConfig {
        pub(crate) timeout: Duration,
    }

    impl From<Box<AccumulatorWindow>> for AccumulatorWindowConfig {
        fn from(value: Box<AccumulatorWindow>) -> Self {
            Self {
                timeout: value.timeout.map(Duration::from).unwrap_or_default(),
            }
        }
    }

    impl TryFrom<&Box<GroupBy>> for ReducerConfig {
        type Error = Error;
        fn try_from(group_by: &Box<GroupBy>) -> Result<Self> {
            let window = group_by.window.as_ref();
            let allowed_lateness = group_by
                .allowed_lateness
                .map_or(Duration::from_secs(0), Duration::from);
            let is_keyed = group_by.keyed.unwrap_or(false);

            if let Some(fixed) = &window.fixed {
                let window_config = AlignedWindowConfig {
                    window_type: AlignedWindowType::Fixed(fixed.clone().into()),
                    allowed_lateness,
                    is_keyed,
                };
                Ok(ReducerConfig::Aligned(AlignedReducerConfig {
                    user_defined_config: UserDefinedConfig::default(),
                    window_config,
                }))
            } else if let Some(sliding) = &window.sliding {
                let window_config = AlignedWindowConfig {
                    window_type: AlignedWindowType::Sliding(sliding.clone().into()),
                    allowed_lateness,
                    is_keyed,
                };
                Ok(ReducerConfig::Aligned(AlignedReducerConfig {
                    user_defined_config: UserDefinedConfig::default(),
                    window_config,
                }))
            } else if let Some(session) = &window.session {
                let window_config = UnalignedWindowConfig {
                    window_type: UnalignedWindowType::Session(session.clone().into()),
                    allowed_lateness,
                    is_keyed,
                };
                Ok(ReducerConfig::Unaligned(UnalignedReducerConfig {
                    user_defined_config: UserDefinedConfig::session_config(),
                    window_config,
                }))
            } else if let Some(accumulator) = &window.accumulator {
                let window_config = UnalignedWindowConfig {
                    window_type: UnalignedWindowType::Accumulator(accumulator.clone().into()),
                    allowed_lateness,
                    is_keyed,
                };
                Ok(ReducerConfig::Unaligned(UnalignedReducerConfig {
                    user_defined_config: UserDefinedConfig::accumulator_config(),
                    window_config,
                }))
            } else {
                Err(Error::Config("No window type specified".to_string()))
            }
        }
    }
    #[derive(Debug, Clone, PartialEq)]
    pub(crate) struct StorageConfig {
        pub(crate) path: std::path::PathBuf,
        pub(crate) max_file_size_mb: u64,
        pub(crate) flush_interval_ms: u64,
        pub(crate) channel_buffer_size: usize,
        pub(crate) max_segment_age_secs: u64,
    }

    impl Default for StorageConfig {
        fn default() -> Self {
            Self {
                path: std::path::PathBuf::from("/var/numaflow/pbq"),
                max_file_size_mb: 10,
                flush_interval_ms: 100,
                channel_buffer_size: 500,
                max_segment_age_secs: 120,
            }
        }
    }

    impl TryFrom<PbqStorage> for StorageConfig {
        type Error = crate::error::Error;

        fn try_from(storage: PbqStorage) -> Result<Self> {
            if storage.persistent_volume_claim.is_some() {
                Err(Error::Config(
                    "Persistent volume claim is not supported".to_string(),
                ))
            } else {
                Ok(StorageConfig::default())
            }
        }
    }
}

fn parse_kafka_auth_config(
    auth_config: Option<Box<Sasl>>,
    tls_config: Option<Box<Tls>>,
) -> crate::Result<(Option<numaflow_kafka::KafkaSaslAuth>, Option<TlsConfig>)> {
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
                    let username = crate::shared::create_components::get_secret_from_volume(
                        &plain.user_secret.name,
                        &plain.user_secret.key,
                    )
                    .map_err(|e| Error::Config(format!("Failed to get user secret: {e:?}")))?;
                    let password = if let Some(password_secret) = plain.password_secret {
                        crate::shared::create_components::get_secret_from_volume(
                            &password_secret.name,
                            &password_secret.key,
                        )
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
                    let username = crate::shared::create_components::get_secret_from_volume(
                        &scram.user_secret.name,
                        &scram.user_secret.key,
                    )
                    .map_err(|e| Error::Config(format!("Failed to get user secret: {e:?}")))?;
                    let password = if let Some(password_secret) = scram.password_secret {
                        crate::shared::create_components::get_secret_from_volume(
                            &password_secret.name,
                            &password_secret.key,
                        )
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
                    let username = crate::shared::create_components::get_secret_from_volume(
                        &scram.user_secret.name,
                        &scram.user_secret.key,
                    )
                    .map_err(|e| Error::Config(format!("Failed to get user secret: {e:?}")))?;
                    let password = if let Some(password_secret) = scram.password_secret {
                        crate::shared::create_components::get_secret_from_volume(
                            &password_secret.name,
                            &password_secret.key,
                        )
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
                    let username = crate::shared::create_components::get_secret_from_volume(
                        &gssapi.username_secret.name,
                        &gssapi.username_secret.key,
                    )
                    .map_err(|e| {
                        Error::Config(format!("Failed to get gssapi username secret: {e:?}"))
                    })?;
                    let password = if let Some(password_secret) = gssapi.password_secret {
                        Some(
                            crate::shared::create_components::get_secret_from_volume(
                                &password_secret.name,
                                &password_secret.key,
                            )
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
                            crate::shared::create_components::get_secret_from_volume(
                                &keytab_secret.name,
                                &keytab_secret.key,
                            )
                            .map_err(|e| {
                                Error::Config(format!("Failed to get gssapi keytab secret: {e:?}"))
                            })?,
                        )
                    } else {
                        None
                    };
                    let kerberos_config =
                        if let Some(kerberos_config_secret) = gssapi.kerberos_config_secret {
                            Some(
                                crate::shared::create_components::get_secret_from_volume(
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
                    let client_id = crate::shared::create_components::get_secret_from_volume(
                        &oauth.client_id.name,
                        &oauth.client_id.key,
                    )
                    .map_err(|e| Error::Config(format!("Failed to get client id secret: {e:?}")))?;
                    let client_secret = crate::shared::create_components::get_secret_from_volume(
                        &oauth.client_secret.name,
                        &oauth.client_secret.key,
                    )
                    .map_err(|e| Error::Config(format!("Failed to get client secret: {e:?}")))?;
                    let token_endpoint = oauth.token_endpoint.clone();
                    Some(numaflow_kafka::KafkaSaslAuth::Oauth {
                        client_id,
                        client_secret,
                        token_endpoint,
                    })
                }
                _ => {
                    return Err(Error::Config(format!(
                        "Unsupported SASL mechanism: {}",
                        mechanism
                    )));
                }
            }
        }
        None => None,
    };

    let tls = if let Some(tls_config) = tls_config {
        let tls_skip_verify = tls_config.insecure_skip_verify.unwrap_or(false);
        if tls_skip_verify {
            Some(numaflow_kafka::TlsConfig {
                insecure_skip_verify: true,
                ca_cert: None,
                client_auth: None,
            })
        } else {
            let ca_cert = tls_config
                .ca_cert_secret
                .map(
                    |ca_cert_secret| match crate::shared::create_components::get_secret_from_volume(
                        &ca_cert_secret.name,
                        &ca_cert_secret.key,
                    ) {
                        Ok(secret) => Ok(secret),
                        Err(e) => Err(Error::Config(format!(
                            "Failed to get CA cert secret: {e:?}"
                        ))),
                    },
                )
                .transpose()?;

            let tls_client_auth_certs = match tls_config.cert_secret {
                Some(client_cert_secret) => {
                    let client_cert = crate::shared::create_components::get_secret_from_volume(
                        &client_cert_secret.name,
                        &client_cert_secret.key,
                    )
                    .map_err(|e| {
                        Error::Config(format!("Failed to get client cert secret: {e:?}"))
                    })?;

                    let Some(private_key_secret) = tls_config.key_secret else {
                        return Err(Error::Config("Client cert is specified for TLS authentication, but private key is not specified".into()));
                    };

                    let client_cert_private_key =
                        crate::shared::create_components::get_secret_from_volume(
                            &private_key_secret.name,
                            &private_key_secret.key,
                        )
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
        }
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
        let secret =
            crate::shared::create_components::get_secret_from_volume(&token.name, &token.key)
                .map_err(|e| {
                    Error::Config(format!("Failed to get token secret from volume: {e:?}"))
                })?;
        return Ok(Some(numaflow_pulsar::PulsarAuth::JWT(secret)));
    }

    if let Some(basic_auth) = auth.basic_auth {
        let user_secret_selector = &basic_auth
            .username
            .ok_or_else(|| Error::Config("Username can not be empty for basic auth".into()))?;
        let username = crate::shared::create_components::get_secret_from_volume(
            &user_secret_selector.name,
            &user_secret_selector.key,
        )
        .map_err(|e| Error::Config(format!("Failed to get username secret from volume: {e:?}")))?;
        let password_secret_selector = &basic_auth
            .password
            .ok_or_else(|| Error::Config("Password can not be empty for basic auth".into()))?;
        let password = crate::shared::create_components::get_secret_from_volume(
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
mod source_tests {
    use std::time::Duration;

    use bytes::Bytes;

    use super::source::{GeneratorConfig, SourceConfig, SourceType, UserDefinedConfig};

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
mod sink_tests {
    use super::sink::{
        BlackholeConfig, LogConfig, OnFailureStrategy, RetryConfig, SinkConfig, SinkType,
        UserDefinedConfig,
    };
    use numaflow_models::models::{Backoff, RetryStrategy};
    use numaflow_sqs::sink::SqsSinkConfig;

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
    fn test_default_log_config() {
        let default_config = LogConfig::default();
        assert_eq!(default_config, LogConfig {});
    }

    #[test]
    fn test_default_blackhole_config() {
        let default_config = BlackholeConfig::default();
        assert_eq!(default_config, BlackholeConfig {});
    }

    #[test]
    fn test_default_user_defined_config() {
        let default_config = UserDefinedConfig::default();
        assert_eq!(default_config.grpc_max_message_size, 64 * 1024 * 1024);
        assert_eq!(default_config.socket_path, "/var/run/numaflow/sink.sock");
        assert_eq!(
            default_config.server_info_path,
            "/var/run/numaflow/sinker-server-info"
        );
    }

    #[test]
    fn test_default_retry_config() {
        let default_retry_strategy = RetryStrategy {
            backoff: Option::from(Box::from(Backoff {
                interval: Option::from(kube::core::Duration::from(
                    std::time::Duration::from_millis(1u64),
                )),
                steps: Option::from(u16::MAX as i64),
                cap: Option::from(kube::core::Duration::from(
                    std::time::Duration::from_millis(4294967295u64),
                )),
                factor: Option::from(1.0),
                jitter: Option::from(0.0),
            })),
            on_failure: Option::from(OnFailureStrategy::Retry.to_string()),
        };
        let default_config = RetryConfig::default();
        assert_eq!(default_config.sink_max_retry_attempts, u16::MAX);
        assert_eq!(default_config.sink_initial_retry_interval_in_ms, 1);
        assert_eq!(default_config.sink_max_retry_interval_in_ms, 4294967295);
        assert_eq!(default_config.sink_retry_factor, 1.0);
        assert_eq!(default_config.sink_retry_jitter, 0.0);
        assert_eq!(
            default_config.sink_retry_on_fail_strategy,
            OnFailureStrategy::Retry
        );
        assert_eq!(
            default_config.sink_default_retry_strategy,
            default_retry_strategy
        );
    }

    #[test]
    fn test_on_failure_strategy_from_str() {
        assert_eq!(
            OnFailureStrategy::from_str("retry"),
            OnFailureStrategy::Retry
        );
        assert_eq!(
            OnFailureStrategy::from_str("fallback"),
            OnFailureStrategy::Fallback
        );
        assert_eq!(OnFailureStrategy::from_str("drop"), OnFailureStrategy::Drop);
        assert_eq!(
            OnFailureStrategy::from_str("unknown"),
            OnFailureStrategy::Retry
        );
    }

    #[test]
    fn test_sink_config_log() {
        let log_config = LogConfig::default();
        let sink_config = SinkConfig {
            sink_type: SinkType::Log(log_config.clone()),
            retry_config: None,
        };
        if let SinkType::Log(config) = sink_config.sink_type {
            assert_eq!(config, log_config);
        } else {
            panic!("Expected SinkType::Log");
        }
    }

    #[test]
    fn test_sink_config_blackhole() {
        let blackhole_config = BlackholeConfig::default();
        let sink_config = SinkConfig {
            sink_type: SinkType::Blackhole(blackhole_config.clone()),
            retry_config: None,
        };
        if let SinkType::Blackhole(config) = sink_config.sink_type {
            assert_eq!(config, blackhole_config);
        } else {
            panic!("Expected SinkType::Blackhole");
        }
    }

    #[test]
    fn test_sink_config_user_defined() {
        let user_defined_config = UserDefinedConfig::default();
        let sink_config = SinkConfig {
            sink_type: SinkType::UserDefined(user_defined_config.clone()),
            retry_config: None,
        };
        if let SinkType::UserDefined(config) = sink_config.sink_type {
            assert_eq!(config, user_defined_config);
        } else {
            panic!("Expected SinkType::UserDefined");
        }
    }

    #[test]
    fn test_sink_config_sqs() {
        let sqs_config = SqsSinkConfig {
            queue_name: "test-queue",
            region: "us-west-2",
            queue_owner_aws_account_id: "123456789012",
        };
        let sink_config = SinkConfig {
            sink_type: SinkType::Sqs(sqs_config.clone()),
            retry_config: None,
        };
        if let SinkType::Sqs(config) = sink_config.sink_type {
            assert_eq!(config, sqs_config);
        } else {
            panic!("Expected SinkType::Sqs");
        }
    }

    #[test]
    fn test_sqs_sink_type_conversion() {
        use numaflow_models::models::SqsSink;

        // Test case 1: Valid configuration
        let valid_sqs_sink = Box::new(SqsSink {
            aws_region: "us-west-2".to_string(),
            queue_name: "test-queue".to_string(),
            queue_owner_aws_account_id: "123456789012".to_string(),
        });

        let result = SinkType::try_from(valid_sqs_sink);
        assert!(result.is_ok());
        if let Ok(SinkType::Sqs(config)) = result {
            assert_eq!(config.region, "us-west-2");
            assert_eq!(config.queue_name, "test-queue");
            assert_eq!(config.queue_owner_aws_account_id, "123456789012");
        } else {
            panic!("Expected SinkType::Sqs");
        }

        // Test case 2: Missing required fields
        let invalid_sqs_sink = Box::new(SqsSink {
            aws_region: "".to_string(),
            queue_name: "test-queue".to_string(),
            queue_owner_aws_account_id: "123456789012".to_string(),
        });

        let result = SinkType::try_from(invalid_sqs_sink);
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().to_string(),
            "Config Error - AWS region is required for SQS sink"
        );
    }

    #[test]
    fn test_sqs_fallback_sink_type() {
        use numaflow_models::models::{AbstractSink, Sink, SqsSink};

        // Test case 1: Valid SQS fallback configuration
        let sink = Sink {
            udsink: None,
            log: None,
            blackhole: None,
            serve: None,
            sqs: None,
            fallback: Some(Box::new(AbstractSink {
                udsink: None,
                log: None,
                blackhole: None,
                serve: None,
                sqs: Some(Box::new(SqsSink {
                    aws_region: "us-west-2".to_string(),
                    queue_name: "fallback-queue".to_string(),
                    queue_owner_aws_account_id: "123456789012".to_string(),
                })),
                kafka: None,
                pulsar: None,
            })),
            retry_strategy: None,
            kafka: None,
            pulsar: None,
        };

        let result = SinkType::fallback_sinktype(&sink);
        assert!(result.is_ok());
        match result {
            Ok(SinkType::Sqs(config)) => {
                assert_eq!(config.region, "us-west-2");
                assert_eq!(config.queue_name, "fallback-queue");
                assert_eq!(config.queue_owner_aws_account_id, "123456789012");
            }
            _ => panic!("Expected SinkType::Sqs for fallback sink"),
        }

        // Test case 2: Missing fallback configuration
        let sink_without_fallback = Sink {
            udsink: None,
            log: None,
            blackhole: None,
            serve: None,
            sqs: None,
            fallback: None,
            retry_strategy: None,
            kafka: None,
            pulsar: None,
        };
        let result = SinkType::fallback_sinktype(&sink_without_fallback);
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().to_string(),
            "Config Error - Fallback sink not found"
        );

        // Test case 3: Missing required AWS region
        let sink_missing_region = Sink {
            udsink: None,
            log: None,
            blackhole: None,
            serve: None,
            sqs: None,
            fallback: Some(Box::new(AbstractSink {
                udsink: None,
                log: None,
                blackhole: None,
                serve: None,
                sqs: Some(Box::new(SqsSink {
                    aws_region: "".to_string(),
                    queue_name: "fallback-queue".to_string(),
                    queue_owner_aws_account_id: "123456789012".to_string(),
                })),
                kafka: None,
                pulsar: None,
            })),
            retry_strategy: None,
            kafka: None,
            pulsar: None,
        };
        let result = SinkType::fallback_sinktype(&sink_missing_region);
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().to_string(),
            "Config Error - AWS region is required for SQS sink"
        );

        // Test case 4: Empty fallback sink configuration
        let sink_empty_fallback = Sink {
            udsink: None,
            log: None,
            blackhole: None,
            serve: None,
            sqs: None,
            fallback: Some(Box::new(AbstractSink {
                udsink: None,
                log: None,
                blackhole: None,
                serve: None,
                sqs: None,
                kafka: None,
                pulsar: None,
            })),
            retry_strategy: None,
            kafka: None,
            pulsar: None,
        };
        let result = SinkType::fallback_sinktype(&sink_empty_fallback);
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().to_string(),
            "Config Error - Sink type not found"
        );
    }

    #[test]
    fn test_pulsar_sink_type_conversion() {
        use k8s_openapi::api::core::v1::SecretKeySelector;
        use numaflow_models::models::PulsarSink;

        // Test case 1: Valid configuration without authentication
        let valid_pulsar_sink = Box::new(PulsarSink {
            auth: None,
            producer_name: "test-producer".to_string(),
            server_addr: "pulsar://localhost:6650".to_string(),
            topic: "persistent://public/default/test-topic".to_string(),
        });

        let result = SinkType::try_from(valid_pulsar_sink);
        assert!(result.is_ok());
        if let Ok(SinkType::Pulsar(config)) = result {
            assert_eq!(config.addr, "pulsar://localhost:6650");
            assert_eq!(config.topic, "persistent://public/default/test-topic");
            assert_eq!(config.producer_name, "test-producer");
            assert!(config.auth.is_none());
        } else {
            panic!("Expected SinkType::Pulsar");
        }

        // Test case 2: Valid configuration with JWT authentication
        let secret_name = "test_pulsar_sink_type_conversion_jwt-secret";
        let token_key = "token";
        setup_secret(secret_name, token_key, "test-jwt-token");

        let valid_pulsar_sink_with_auth = Box::new(PulsarSink {
            auth: Some(Box::new(numaflow_models::models::PulsarAuth {
                token: Some(SecretKeySelector {
                    name: secret_name.to_string(),
                    key: token_key.to_string(),
                    ..Default::default()
                }),
                basic_auth: None,
            })),
            producer_name: "test-producer".to_string(),
            server_addr: "pulsar://localhost:6650".to_string(),
            topic: "persistent://public/default/test-topic".to_string(),
        });

        let result = SinkType::try_from(valid_pulsar_sink_with_auth);
        assert!(result.is_ok());
        if let Ok(SinkType::Pulsar(config)) = result {
            assert_eq!(config.addr, "pulsar://localhost:6650");
            assert_eq!(config.topic, "persistent://public/default/test-topic");
            assert_eq!(config.producer_name, "test-producer");
            let auth = config.auth.unwrap();
            let numaflow_pulsar::PulsarAuth::JWT(token) = auth else {
                panic!("Expected PulsarAuth::JWT");
            };
            assert_eq!(token, "test-jwt-token");
        } else {
            panic!("Expected SinkType::Pulsar");
        }

        cleanup_secret(secret_name);
    }

    #[test]
    fn test_pulsar_sink_type_conversion_with_missing_token() {
        use numaflow_models::models::PulsarSink;

        // Test case: Authentication is specified but token is missing
        let invalid_pulsar_sink = Box::new(PulsarSink {
            auth: Some(Box::new(numaflow_models::models::PulsarAuth {
                token: None,
                basic_auth: None,
            })),
            producer_name: "test-producer".to_string(),
            server_addr: "pulsar://localhost:6650".to_string(),
            topic: "test-topic".to_string(),
        });

        let result = SinkType::try_from(invalid_pulsar_sink);
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().to_string(),
            "Config Error - Authentication configuration is enabled, however credentials are not provided in the Pulsar sink configuration"
        );
    }

    #[test]
    fn test_pulsar_sink_type_conversion_with_invalid_secret() {
        use k8s_openapi::api::core::v1::SecretKeySelector;
        use numaflow_models::models::PulsarSink;

        // Test case: Authentication is specified but secret file doesn't exist
        let invalid_pulsar_sink = Box::new(PulsarSink {
            auth: Some(Box::new(numaflow_models::models::PulsarAuth {
                token: Some(SecretKeySelector {
                    name: "non-existent-secret".to_string(),
                    key: "token".to_string(),
                    ..Default::default()
                }),
                basic_auth: None,
            })),
            producer_name: "test-producer".to_string(),
            server_addr: "pulsar://localhost:6650".to_string(),
            topic: "test-topic".to_string(),
        });

        let result = SinkType::try_from(invalid_pulsar_sink);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            err.to_string()
                .contains("Failed to get token secret from volume")
        );
    }

    #[test]
    fn test_pulsar_fallback_sink_type() {
        use k8s_openapi::api::core::v1::SecretKeySelector;
        use numaflow_models::models::{AbstractSink, PulsarSink, Sink};

        // Test case 1: Valid Pulsar fallback configuration without auth
        let sink = Sink {
            udsink: None,
            log: None,
            blackhole: None,
            serve: None,
            sqs: None,
            fallback: Some(Box::new(AbstractSink {
                udsink: None,
                log: None,
                blackhole: None,
                serve: None,
                sqs: None,
                kafka: None,
                pulsar: Some(Box::new(PulsarSink {
                    auth: None,
                    producer_name: "fallback-producer".to_string(),
                    server_addr: "pulsar://localhost:6650".to_string(),
                    topic: "fallback-topic".to_string(),
                })),
            })),
            retry_strategy: None,
            kafka: None,
            pulsar: None,
        };

        let result = SinkType::fallback_sinktype(&sink);
        assert!(result.is_ok());
        match result {
            Ok(SinkType::Pulsar(config)) => {
                assert_eq!(config.addr, "pulsar://localhost:6650");
                assert_eq!(config.topic, "fallback-topic");
                assert_eq!(config.producer_name, "fallback-producer");
                assert!(config.auth.is_none());
            }
            _ => panic!("Expected SinkType::Pulsar for fallback sink"),
        }

        // Test case 2: Valid Pulsar fallback configuration with JWT auth
        let secret_name = "test_pulsar_fallback_sink_type_jwt-secret";
        let token_key = "token";
        setup_secret(secret_name, token_key, "fallback-jwt-token");

        let sink_with_auth = Sink {
            udsink: None,
            log: None,
            blackhole: None,
            serve: None,
            sqs: None,
            fallback: Some(Box::new(AbstractSink {
                udsink: None,
                log: None,
                blackhole: None,
                serve: None,
                sqs: None,
                kafka: None,
                pulsar: Some(Box::new(PulsarSink {
                    auth: Some(Box::new(numaflow_models::models::PulsarAuth {
                        token: Some(SecretKeySelector {
                            name: secret_name.to_string(),
                            key: token_key.to_string(),
                            ..Default::default()
                        }),
                        basic_auth: None,
                    })),
                    producer_name: "fallback-producer".to_string(),
                    server_addr: "pulsar://localhost:6650".to_string(),
                    topic: "fallback-topic".to_string(),
                })),
            })),
            retry_strategy: None,
            kafka: None,
            pulsar: None,
        };

        let result = SinkType::fallback_sinktype(&sink_with_auth);
        assert!(result.is_ok());
        match result {
            Ok(SinkType::Pulsar(config)) => {
                assert_eq!(config.addr, "pulsar://localhost:6650");
                assert_eq!(config.topic, "fallback-topic");
                assert_eq!(config.producer_name, "fallback-producer");
                let auth = config.auth.unwrap();
                let numaflow_pulsar::PulsarAuth::JWT(token) = auth else {
                    panic!("Expected PulsarAuth::JWT");
                };
                assert_eq!(token, "fallback-jwt-token");
            }
            _ => panic!("Expected SinkType::Pulsar for fallback sink"),
        }

        cleanup_secret(secret_name);
    }

    #[test]
    fn test_pulsar_fallback_sink_type_with_missing_token() {
        use numaflow_models::models::{AbstractSink, PulsarSink, Sink};

        // Test case: Fallback Pulsar sink with authentication but missing token
        let sink = Sink {
            udsink: None,
            log: None,
            blackhole: None,
            serve: None,
            sqs: None,
            fallback: Some(Box::new(AbstractSink {
                udsink: None,
                log: None,
                blackhole: None,
                serve: None,
                sqs: None,
                kafka: None,
                pulsar: Some(Box::new(PulsarSink {
                    auth: Some(Box::new(numaflow_models::models::PulsarAuth {
                        token: None,
                        basic_auth: None,
                    })),
                    producer_name: "fallback-producer".to_string(),
                    server_addr: "pulsar://localhost:6650".to_string(),
                    topic: "fallback-topic".to_string(),
                })),
            })),
            retry_strategy: None,
            kafka: None,
            pulsar: None,
        };

        let result = SinkType::fallback_sinktype(&sink);
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().to_string(),
            "Config Error - Authentication configuration is enabled, however credentials are not provided in the Pulsar sink configuration"
        );
    }

    #[test]
    fn test_pulsar_fallback_sink_type_with_invalid_secret() {
        use k8s_openapi::api::core::v1::SecretKeySelector;
        use numaflow_models::models::{AbstractSink, PulsarSink, Sink};

        // Test case: Fallback Pulsar sink with authentication but invalid secret
        let sink = Sink {
            udsink: None,
            log: None,
            blackhole: None,
            serve: None,
            sqs: None,
            fallback: Some(Box::new(AbstractSink {
                udsink: None,
                log: None,
                blackhole: None,
                serve: None,
                sqs: None,
                kafka: None,
                pulsar: Some(Box::new(PulsarSink {
                    auth: Some(Box::new(numaflow_models::models::PulsarAuth {
                        token: Some(SecretKeySelector {
                            name: "non-existent-secret".to_string(),
                            key: "token".to_string(),
                            ..Default::default()
                        }),
                        basic_auth: None,
                    })),
                    producer_name: "fallback-producer".to_string(),
                    server_addr: "pulsar://localhost:6650".to_string(),
                    topic: "fallback-topic".to_string(),
                })),
            })),
            retry_strategy: None,
            kafka: None,
            pulsar: None,
        };

        let result = SinkType::fallback_sinktype(&sink);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            err.to_string()
                .contains("Failed to get token secret from volume")
        );
    }
}

#[cfg(test)]
mod transformer_tests {
    use super::transformer::{TransformerConfig, TransformerType, UserDefinedConfig};

    #[test]
    fn test_default_user_defined_config() {
        let default_config = UserDefinedConfig::default();
        assert_eq!(default_config.grpc_max_message_size, 64 * 1024 * 1024);
        assert_eq!(
            default_config.socket_path,
            "/var/run/numaflow/sourcetransform.sock"
        );
        assert_eq!(
            default_config.server_info_path,
            "/var/run/numaflow/sourcetransformer-server-info"
        );
    }

    #[test]
    fn test_transformer_config_user_defined() {
        let user_defined_config = UserDefinedConfig::default();
        let transformer_config = TransformerConfig {
            concurrency: 1,
            transformer_type: TransformerType::UserDefined(user_defined_config.clone()),
        };
        if let TransformerType::UserDefined(config) = transformer_config.transformer_type {
            assert_eq!(config, user_defined_config);
        } else {
            panic!("Expected TransformerType::UserDefined");
        }
    }
}

#[cfg(test)]
mod jetstream_tests {
    use std::fs;
    use std::path::Path;

    use k8s_openapi::api::core::v1::SecretKeySelector;
    use numaflow_jetstream::NatsAuth;
    use numaflow_models::models::BasicAuth;
    use numaflow_models::models::{JetStreamSource, Tls};

    use super::source::SourceType;

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
            tls: None,
            url: "nats://localhost:4222".to_string(),
        };

        let source_type = SourceType::try_from(Box::new(jetstream_source)).unwrap();
        if let SourceType::Jetstream(config) = source_type {
            let NatsAuth::Basic { username, password } = config.auth.unwrap() else {
                panic!("Basic auth creds must be set");
            };
            assert_eq!(username, "test-user");
            assert_eq!(password, "test-pass");
            assert_eq!(config.consumer, "test-stream");
            assert_eq!(config.addr, "nats://localhost:4222");
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

        let source_type = SourceType::try_from(Box::new(jetstream_source)).unwrap();
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
            tls: None,
            url: "nats://localhost:4222".to_string(),
        };

        let result = SourceType::try_from(Box::new(jetstream_source));
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(
            err.to_string(),
            "Config Error - Authentication is specified, but auth setting is empty"
        );
    }
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

#[cfg(test)]
mod reducer_tests {
    use std::time::Duration;

    use numaflow_models::models::{
        AccumulatorWindow, FixedWindow, GroupBy, SessionWindow, SlidingWindow, Window,
    };

    use super::reduce::{AlignedWindowType, ReducerConfig, UnalignedWindowType, UserDefinedConfig};

    #[test]
    fn test_default_user_defined_config() {
        let default_config = UserDefinedConfig::default();
        assert_eq!(default_config.grpc_max_message_size, 64 * 1024 * 1024);
        assert_eq!(default_config.socket_path, "/var/run/numaflow/reduce.sock");
        assert_eq!(
            default_config.server_info_path,
            "/var/run/numaflow/reducer-server-info"
        );
    }

    #[test]
    fn test_window_config_from_group_by_fixed() {
        let window = Window {
            fixed: Some(Box::new(FixedWindow {
                length: Some(kube::core::Duration::from(Duration::from_secs(60))),
                streaming: None,
            })),
            sliding: None,
            session: None,
            accumulator: None,
        };

        let group_by = Box::new(GroupBy {
            allowed_lateness: Some(kube::core::Duration::from(Duration::from_secs(10))),
            keyed: Some(true),
            storage: None,
            window: Box::new(window),
        });

        let reducer_config = ReducerConfig::try_from(&group_by).unwrap();
        match reducer_config {
            ReducerConfig::Aligned(aligned_config) => {
                assert_eq!(
                    aligned_config.window_config.allowed_lateness,
                    Duration::from_secs(10)
                );
                assert!(aligned_config.window_config.is_keyed);

                match aligned_config.window_config.window_type {
                    AlignedWindowType::Fixed(config) => {
                        assert_eq!(config.length, Duration::from_secs(60));
                    }
                    _ => panic!("Expected fixed window type"),
                }
            }
            _ => panic!("Expected aligned reducer config"),
        }
    }

    #[test]
    fn test_window_config_from_group_by_sliding() {
        let window = Window {
            fixed: None,
            sliding: Some(Box::new(SlidingWindow {
                length: Some(kube::core::Duration::from(Duration::from_secs(60))),
                slide: Some(kube::core::Duration::from(Duration::from_secs(30))),
                streaming: None,
            })),
            session: None,
            accumulator: None,
        };

        let group_by = Box::new(GroupBy {
            allowed_lateness: None,
            keyed: None,
            storage: None,
            window: Box::new(window),
        });

        let reducer_config = ReducerConfig::try_from(&group_by).unwrap();
        match reducer_config {
            ReducerConfig::Aligned(aligned_config) => {
                assert_eq!(
                    aligned_config.window_config.allowed_lateness,
                    Duration::from_secs(0)
                );
                assert!(!aligned_config.window_config.is_keyed);

                match aligned_config.window_config.window_type {
                    AlignedWindowType::Sliding(config) => {
                        assert_eq!(config.length, Duration::from_secs(60));
                        assert_eq!(config.slide, Duration::from_secs(30));
                    }
                    _ => panic!("Expected sliding window type"),
                }
            }
            _ => panic!("Expected aligned reducer config"),
        }
    }

    #[test]
    fn test_window_config_from_group_by_session() {
        let window = Window {
            fixed: None,
            sliding: None,
            session: Some(Box::new(SessionWindow {
                timeout: Some(kube::core::Duration::from(Duration::from_secs(300))),
            })),
            accumulator: None,
        };

        let group_by = Box::new(GroupBy {
            allowed_lateness: None,
            keyed: Some(true),
            storage: None,
            window: Box::new(window),
        });

        let reducer_config = ReducerConfig::try_from(&group_by).unwrap();
        match reducer_config {
            ReducerConfig::Unaligned(unaligned_config) => {
                assert_eq!(
                    unaligned_config.window_config.allowed_lateness,
                    Duration::from_secs(0)
                );
                assert!(unaligned_config.window_config.is_keyed);

                match unaligned_config.window_config.window_type {
                    UnalignedWindowType::Session(config) => {
                        assert_eq!(config.timeout, Duration::from_secs(300));
                    }
                    _ => panic!("Expected session window type"),
                }
            }
            _ => panic!("Expected unaligned reducer config"),
        }
    }

    #[test]
    fn test_window_config_from_group_by_accumulator() {
        let window = Window {
            fixed: None,
            sliding: None,
            session: None,
            accumulator: Some(Box::new(AccumulatorWindow {
                timeout: Some(kube::core::Duration::from(Duration::from_secs(600))),
            })),
        };

        let group_by = Box::new(GroupBy {
            allowed_lateness: None,
            keyed: Some(true),
            storage: None,
            window: Box::new(window),
        });

        let reducer_config = ReducerConfig::try_from(&group_by).unwrap();
        match reducer_config {
            ReducerConfig::Unaligned(unaligned_config) => {
                assert_eq!(
                    unaligned_config.window_config.allowed_lateness,
                    Duration::from_secs(0)
                );
                assert!(unaligned_config.window_config.is_keyed);

                match unaligned_config.window_config.window_type {
                    UnalignedWindowType::Accumulator(config) => {
                        assert_eq!(config.timeout, Duration::from_secs(600));
                    }
                    _ => panic!("Expected accumulator window type"),
                }
            }
            _ => panic!("Expected unaligned reducer config"),
        }
    }

    #[test]
    fn test_window_config_from_group_by_no_window_type() {
        let window = Window {
            fixed: None,
            sliding: None,
            session: None,
            accumulator: None,
        };

        let group_by = Box::new(GroupBy {
            allowed_lateness: None,
            keyed: None,
            storage: None,
            window: Box::new(window),
        });

        let result = ReducerConfig::try_from(&group_by);
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().to_string(),
            "Config Error - No window type specified"
        );
    }

    #[test]
    fn test_session_user_defined_config() {
        let session_config = UserDefinedConfig::session_config();
        assert_eq!(session_config.grpc_max_message_size, 64 * 1024 * 1024);
        assert_eq!(
            session_config.socket_path,
            "/var/run/numaflow/sessionreduce.sock"
        );
        assert_eq!(
            session_config.server_info_path,
            "/var/run/numaflow/sessionreducer-server-info"
        );
    }

    #[test]
    fn test_accumulator_user_defined_config() {
        let accumulator_config = UserDefinedConfig::accumulator_config();
        assert_eq!(accumulator_config.grpc_max_message_size, 64 * 1024 * 1024);
        assert_eq!(
            accumulator_config.socket_path,
            "/var/run/numaflow/accumulator.sock"
        );
        assert_eq!(
            accumulator_config.server_info_path,
            "/var/run/numaflow/accumulator-server-info"
        );
    }

    #[test]
    fn test_fixed_window_config_from_fixed_window() {
        use super::reduce::FixedWindowConfig;

        let fixed_window = Box::new(FixedWindow {
            length: Some(kube::core::Duration::from(Duration::from_secs(120))),
            streaming: None,
        });

        let config = FixedWindowConfig::from(fixed_window);
        assert_eq!(config.length, Duration::from_secs(120));
    }

    #[test]
    fn test_fixed_window_config_from_fixed_window_default() {
        use super::reduce::FixedWindowConfig;

        let fixed_window = Box::new(FixedWindow {
            length: None,
            streaming: None,
        });

        let config = FixedWindowConfig::from(fixed_window);
        assert_eq!(config.length, Duration::default());
    }

    #[test]
    fn test_sliding_window_config_from_sliding_window() {
        use super::reduce::SlidingWindowConfig;

        let sliding_window = Box::new(SlidingWindow {
            length: Some(kube::core::Duration::from(Duration::from_secs(180))),
            slide: Some(kube::core::Duration::from(Duration::from_secs(60))),
            streaming: None,
        });

        let config = SlidingWindowConfig::from(sliding_window);
        assert_eq!(config.length, Duration::from_secs(180));
        assert_eq!(config.slide, Duration::from_secs(60));
    }

    #[test]
    fn test_sliding_window_config_from_sliding_window_defaults() {
        use super::reduce::SlidingWindowConfig;

        let sliding_window = Box::new(SlidingWindow {
            length: None,
            slide: None,
            streaming: None,
        });

        let config = SlidingWindowConfig::from(sliding_window);
        assert_eq!(config.length, Duration::default());
        assert_eq!(config.slide, Duration::default());
    }

    #[test]
    fn test_session_window_config_from_session_window() {
        use super::reduce::SessionWindowConfig;

        let session_window = Box::new(SessionWindow {
            timeout: Some(kube::core::Duration::from(Duration::from_secs(900))),
        });

        let config = SessionWindowConfig::from(session_window);
        assert_eq!(config.timeout, Duration::from_secs(900));
    }

    #[test]
    fn test_session_window_config_from_session_window_default() {
        use super::reduce::SessionWindowConfig;

        let session_window = Box::new(SessionWindow { timeout: None });

        let config = SessionWindowConfig::from(session_window);
        assert_eq!(config.timeout, Duration::default());
    }

    #[test]
    fn test_accumulator_window_config_from_accumulator_window() {
        use super::reduce::AccumulatorWindowConfig;

        let accumulator_window = Box::new(AccumulatorWindow {
            timeout: Some(kube::core::Duration::from(Duration::from_secs(1200))),
        });

        let config = AccumulatorWindowConfig::from(accumulator_window);
        assert_eq!(config.timeout, Duration::from_secs(1200));
    }

    #[test]
    fn test_accumulator_window_config_from_accumulator_window_default() {
        use super::reduce::AccumulatorWindowConfig;

        let accumulator_window = Box::new(AccumulatorWindow { timeout: None });

        let config = AccumulatorWindowConfig::from(accumulator_window);
        assert_eq!(config.timeout, Duration::default());
    }

    #[test]
    fn test_storage_config_default() {
        use super::reduce::StorageConfig;

        let default_config = StorageConfig::default();
        assert_eq!(
            default_config.path,
            std::path::PathBuf::from("/var/numaflow/pbq")
        );
        assert_eq!(default_config.max_file_size_mb, 10);
        assert_eq!(default_config.flush_interval_ms, 100);
        assert_eq!(default_config.channel_buffer_size, 500);
        assert_eq!(default_config.max_segment_age_secs, 120);
    }

    #[test]
    fn test_storage_config_from_pbq_storage_success() {
        use super::reduce::StorageConfig;
        use numaflow_models::models::PbqStorage;

        let pbq_storage = PbqStorage {
            persistent_volume_claim: None,
            empty_dir: None,
            no_store: None,
        };

        let result = StorageConfig::try_from(pbq_storage);
        assert!(result.is_ok());
        let config = result.unwrap();
        assert_eq!(config.path, std::path::PathBuf::from("/var/numaflow/pbq"));
        assert_eq!(config.max_file_size_mb, 10);
        assert_eq!(config.flush_interval_ms, 100);
        assert_eq!(config.channel_buffer_size, 500);
        assert_eq!(config.max_segment_age_secs, 120);
    }

    #[test]
    fn test_storage_config_from_pbq_storage_with_pvc_error() {
        use super::reduce::StorageConfig;
        use numaflow_models::models::{PbqStorage, PersistenceStrategy};

        let pbq_storage = PbqStorage {
            persistent_volume_claim: Some(Box::new(PersistenceStrategy {
                access_mode: Some("ReadWriteOnce".to_string()),
                storage_class_name: Some("standard".to_string()),
                volume_size: None,
            })),
            empty_dir: None,
            no_store: None,
        };

        let result = StorageConfig::try_from(pbq_storage);
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().to_string(),
            "Config Error - Persistent volume claim is not supported"
        );
    }

    #[test]
    fn test_fixed_window_with_large_duration() {
        let window = Window {
            fixed: Some(Box::new(FixedWindow {
                length: Some(kube::core::Duration::from(Duration::from_secs(86400))), // 1 day
                streaming: None,
            })),
            sliding: None,
            session: None,
            accumulator: None,
        };

        let group_by = Box::new(GroupBy {
            allowed_lateness: Some(kube::core::Duration::from(Duration::from_secs(3600))), // 1 hour
            keyed: Some(false),
            storage: None,
            window: Box::new(window),
        });

        let reducer_config = ReducerConfig::try_from(&group_by).unwrap();
        match reducer_config {
            ReducerConfig::Aligned(aligned_config) => {
                assert_eq!(
                    aligned_config.window_config.allowed_lateness,
                    Duration::from_secs(3600)
                );
                assert!(!aligned_config.window_config.is_keyed);

                match aligned_config.window_config.window_type {
                    AlignedWindowType::Fixed(config) => {
                        assert_eq!(config.length, Duration::from_secs(86400));
                    }
                    _ => panic!("Expected fixed window type"),
                }
            }
            _ => panic!("Expected aligned reducer config"),
        }
    }

    #[test]
    fn test_sliding_window_with_zero_slide() {
        let window = Window {
            fixed: None,
            sliding: Some(Box::new(SlidingWindow {
                length: Some(kube::core::Duration::from(Duration::from_secs(60))),
                slide: Some(kube::core::Duration::from(Duration::from_secs(0))),
                streaming: None,
            })),
            session: None,
            accumulator: None,
        };

        let group_by = Box::new(GroupBy {
            allowed_lateness: None,
            keyed: None,
            storage: None,
            window: Box::new(window),
        });

        let reducer_config = ReducerConfig::try_from(&group_by).unwrap();
        match reducer_config {
            ReducerConfig::Aligned(aligned_config) => {
                match aligned_config.window_config.window_type {
                    AlignedWindowType::Sliding(config) => {
                        assert_eq!(config.length, Duration::from_secs(60));
                        assert_eq!(config.slide, Duration::from_secs(0));
                    }
                    _ => panic!("Expected sliding window type"),
                }
            }
            _ => panic!("Expected aligned reducer config"),
        }
    }

    #[test]
    fn test_session_window_with_very_long_timeout() {
        let window = Window {
            fixed: None,
            sliding: None,
            session: Some(Box::new(SessionWindow {
                timeout: Some(kube::core::Duration::from(Duration::from_secs(7200))), // 2 hours
            })),
            accumulator: None,
        };

        let group_by = Box::new(GroupBy {
            allowed_lateness: Some(kube::core::Duration::from(Duration::from_secs(600))), // 10 minutes
            keyed: Some(true),
            storage: None,
            window: Box::new(window),
        });

        let reducer_config = ReducerConfig::try_from(&group_by).unwrap();
        match reducer_config {
            ReducerConfig::Unaligned(unaligned_config) => {
                assert_eq!(
                    unaligned_config.window_config.allowed_lateness,
                    Duration::from_secs(600)
                );
                assert!(unaligned_config.window_config.is_keyed);

                match unaligned_config.window_config.window_type {
                    UnalignedWindowType::Session(config) => {
                        assert_eq!(config.timeout, Duration::from_secs(7200));
                    }
                    _ => panic!("Expected session window type"),
                }

                // Verify session-specific user defined config
                assert_eq!(
                    unaligned_config.user_defined_config.socket_path,
                    "/var/run/numaflow/sessionreduce.sock"
                );
            }
            _ => panic!("Expected unaligned reducer config"),
        }
    }

    #[test]
    fn test_accumulator_window_with_specific_config() {
        let window = Window {
            fixed: None,
            sliding: None,
            session: None,
            accumulator: Some(Box::new(AccumulatorWindow {
                timeout: Some(kube::core::Duration::from(Duration::from_secs(1800))), // 30 minutes
            })),
        };

        let group_by = Box::new(GroupBy {
            allowed_lateness: Some(kube::core::Duration::from(Duration::from_secs(300))), // 5 minutes
            keyed: Some(false),
            storage: None,
            window: Box::new(window),
        });

        let reducer_config = ReducerConfig::try_from(&group_by).unwrap();
        match reducer_config {
            ReducerConfig::Unaligned(unaligned_config) => {
                assert_eq!(
                    unaligned_config.window_config.allowed_lateness,
                    Duration::from_secs(300)
                );
                assert!(!unaligned_config.window_config.is_keyed);

                match unaligned_config.window_config.window_type {
                    UnalignedWindowType::Accumulator(config) => {
                        assert_eq!(config.timeout, Duration::from_secs(1800));
                    }
                    _ => panic!("Expected accumulator window type"),
                }

                // Verify accumulator-specific user defined config
                assert_eq!(
                    unaligned_config.user_defined_config.socket_path,
                    "/var/run/numaflow/accumulator.sock"
                );
                assert_eq!(
                    unaligned_config.user_defined_config.server_info_path,
                    "/var/run/numaflow/accumulator-server-info"
                );
            }
            _ => panic!("Expected unaligned reducer config"),
        }
    }
}

#[cfg(test)]
mod pulsar_source_tests {
    use k8s_openapi::api::core::v1::SecretKeySelector;
    use numaflow_models::models::{PulsarAuth, PulsarBasicAuth, PulsarSource};
    use numaflow_pulsar::PulsarAuth as PulsarAuthEnum;

    use super::source::SourceType;

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
