pub(crate) mod source {
    const DEFAULT_GRPC_MAX_MESSAGE_SIZE: usize = 64 * 1024 * 1024; // 64 MB
    const DEFAULT_SOURCE_SOCKET: &str = "/var/run/numaflow/source.sock";
    const DEFAULT_SOURCE_SERVER_INFO_FILE: &str = "/var/run/numaflow/sourcer-server-info";

    use std::{fmt::Debug, time::Duration};

    use bytes::Bytes;
    use numaflow_jetstream::{JetstreamSourceConfig, NatsAuth, TlsClientAuthCerts, TlsConfig};
    use numaflow_kafka::KafkaSourceConfig;
    use numaflow_models::models::{GeneratorSource, PulsarSource, Source, SqsSource};
    use numaflow_pulsar::source::{PulsarAuth, PulsarSourceConfig};
    use numaflow_sqs::source::SqsSourceConfig;
    use tracing::warn;

    use crate::Result;
    use crate::error::Error;

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
        Kafka(KafkaSourceConfig),
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
            let auth: Option<PulsarAuth> = match value.auth {
                Some(auth) => 'out: {
                    let Some(token) = auth.token else {
                        tracing::warn!("JWT Token authentication is specified, but token is empty");
                        break 'out None;
                    };
                    let secret = crate::shared::create_components::get_secret_from_volume(
                        &token.name,
                        &token.key,
                    )
                    .unwrap();
                    Some(PulsarAuth::JWT(secret))
                }
                None => None,
            };
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
            let auth: Option<numaflow_kafka::KafkaAuth> = match value.sasl {
                Some(sasl) => {
                    let mechanism = sasl.mechanism;
                    match mechanism.as_str() {
                        "PLAIN" => {
                            let Some(plain) = sasl.plain else {
                                return Err(Error::Config(
                                    "PLAIN mechanism requires plain auth configuration".into(),
                                ));
                            };
                            let username =
                                crate::shared::create_components::get_secret_from_volume(
                                    &plain.user_secret.name,
                                    &plain.user_secret.key,
                                )
                                .map_err(|e| {
                                    Error::Config(format!("Failed to get user secret: {e:?}"))
                                })?;
                            let password = if let Some(password_secret) = plain.password_secret {
                                crate::shared::create_components::get_secret_from_volume(
                                    &password_secret.name,
                                    &password_secret.key,
                                )
                                .map_err(|e| {
                                    Error::Config(format!("Failed to get password secret: {e:?}"))
                                })?
                            } else {
                                return Err(Error::Config(
                                    "PLAIN mechanism requires password".into(),
                                ));
                            };
                            Some(numaflow_kafka::KafkaAuth::Sasl {
                                mechanism,
                                username,
                                password,
                            })
                        }
                        "SCRAM-SHA-256" | "SCRAM-SHA-512" => {
                            let scram = if mechanism == "SCRAM-SHA-256" {
                                sasl.scramsha256
                            } else {
                                sasl.scramsha512
                            };
                            let Some(scram) = scram else {
                                return Err(Error::Config(format!(
                                    "{} mechanism requires scram auth configuration",
                                    mechanism
                                )));
                            };
                            let username =
                                crate::shared::create_components::get_secret_from_volume(
                                    &scram.user_secret.name,
                                    &scram.user_secret.key,
                                )
                                .map_err(|e| {
                                    Error::Config(format!("Failed to get user secret: {e:?}"))
                                })?;
                            let password = if let Some(password_secret) = scram.password_secret {
                                crate::shared::create_components::get_secret_from_volume(
                                    &password_secret.name,
                                    &password_secret.key,
                                )
                                .map_err(|e| {
                                    Error::Config(format!("Failed to get password secret: {e:?}"))
                                })?
                            } else {
                                return Err(Error::Config(format!(
                                    "{} mechanism requires password",
                                    mechanism
                                )));
                            };
                            Some(numaflow_kafka::KafkaAuth::Sasl {
                                mechanism,
                                username,
                                password,
                            })
                        }
                        "OAUTH" => {
                            let Some(oauth) = sasl.oauth else {
                                return Err(Error::Config(
                                    "OAUTH mechanism requires oauth configuration".into(),
                                ));
                            };
                            let username =
                                crate::shared::create_components::get_secret_from_volume(
                                    &oauth.client_id.name,
                                    &oauth.client_id.key,
                                )
                                .map_err(|e| {
                                    Error::Config(format!("Failed to get client id secret: {e:?}"))
                                })?;
                            let password =
                                crate::shared::create_components::get_secret_from_volume(
                                    &oauth.client_secret.name,
                                    &oauth.client_secret.key,
                                )
                                .map_err(|e| {
                                    Error::Config(format!("Failed to get client secret: {e:?}"))
                                })?;
                            Some(numaflow_kafka::KafkaAuth::Sasl {
                                mechanism,
                                username,
                                password,
                            })
                        }
                        "GSSAPI" => {
                            return Err(Error::Config(
                                "GSSAPI mechanism is not supported yet".into(),
                            ));
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

            let tls = if let Some(tls_config) = value.tls {
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

            let kafka_config = numaflow_kafka::KafkaSourceConfig {
                brokers: value.brokers.unwrap_or_default(),
                topic: value.topic,
                consumer_group: value.consumer_group.unwrap_or_default(),
                auth,
                tls,
            };
            Ok(SourceType::Kafka(kafka_config))
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
    const DEFAULT_SINK_RETRY_INTERVAL_IN_MS: u32 = 1;

    use std::fmt::Display;

    use numaflow_models::models::{Backoff, RetryStrategy, Sink, SqsSink};
    use numaflow_sqs::sink::SqsSinkConfig;

    use crate::Result;
    use crate::error::Error;

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
                    .or_else(|| sink.serve.as_ref().map(|_| Ok(SinkType::Serve)))
                    .or_else(|| fallback.sqs.as_ref().map(|sqs| sqs.clone().try_into()))
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
        pub sink_retry_interval_in_ms: u32,
        pub sink_retry_on_fail_strategy: OnFailureStrategy,
        pub sink_default_retry_strategy: RetryStrategy,
    }

    impl Default for RetryConfig {
        fn default() -> Self {
            let default_retry_strategy = RetryStrategy {
                backoff: Option::from(Box::from(Backoff {
                    interval: Option::from(kube::core::Duration::from(
                        std::time::Duration::from_millis(DEFAULT_SINK_RETRY_INTERVAL_IN_MS as u64),
                    )),
                    steps: Option::from(DEFAULT_MAX_SINK_RETRY_ATTEMPTS as i64),
                })),
                on_failure: Option::from(DEFAULT_SINK_RETRY_ON_FAIL_STRATEGY.to_string()),
            };
            Self {
                sink_max_retry_attempts: DEFAULT_MAX_SINK_RETRY_ATTEMPTS,
                sink_retry_interval_in_ms: DEFAULT_SINK_RETRY_INTERVAL_IN_MS,
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
                    retry_config.sink_retry_interval_in_ms =
                        std::time::Duration::from(interval).as_millis() as u32;
                }

                if let Some(steps) = backoff.steps {
                    retry_config.sink_max_retry_attempts = steps as u16;
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
            })),
            on_failure: Option::from(OnFailureStrategy::Retry.to_string()),
        };
        let default_config = RetryConfig::default();
        assert_eq!(default_config.sink_max_retry_attempts, u16::MAX);
        assert_eq!(default_config.sink_retry_interval_in_ms, 1);
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
            })),
            retry_strategy: None,
            kafka: None,
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
            })),
            retry_strategy: None,
            kafka: None,
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
            })),
            retry_strategy: None,
            kafka: None,
        };
        let result = SinkType::fallback_sinktype(&sink_empty_fallback);
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().to_string(),
            "Config Error - Sink type not found"
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
    use super::source::SourceType;
    use k8s_openapi::api::core::v1::SecretKeySelector;
    use numaflow_models::models::{KafkaSource, Sasl, SaslPlain, SasloAuth, Tls};
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
    fn test_try_from_kafka_source_with_plain_auth() {
        let secret_name = "plain-auth-secret";
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
                numaflow_kafka::KafkaAuth::Sasl {
                    mechanism,
                    username,
                    password,
                } => {
                    assert_eq!(mechanism, "PLAIN");
                    assert_eq!(username, "test-user");
                    assert_eq!(password, "test-pass");
                }
            }
            assert_eq!(config.brokers, vec!["localhost:9092"]);
            assert_eq!(config.topic, "test-topic");
            assert_eq!(config.consumer_group, "test-group");
        } else {
            panic!("Expected SourceType::Kafka");
        }

        cleanup_secret(secret_name);
    }

    #[test]
    fn test_try_from_kafka_source_with_scram_auth() {
        let secret_name = "scram-auth-secret";
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
                numaflow_kafka::KafkaAuth::Sasl {
                    mechanism,
                    username,
                    password,
                } => {
                    assert_eq!(mechanism, "SCRAM-SHA-256");
                    assert_eq!(username, "test-user");
                    assert_eq!(password, "test-pass");
                }
            }
        } else {
            panic!("Expected SourceType::Kafka");
        }

        cleanup_secret(secret_name);
    }

    #[test]
    fn test_try_from_kafka_source_with_oauth() {
        let secret_name = "oauth-auth-secret";
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
                numaflow_kafka::KafkaAuth::Sasl {
                    mechanism,
                    username,
                    password,
                } => {
                    assert_eq!(mechanism, "OAUTH");
                    assert_eq!(username, "test-client");
                    assert_eq!(password, "test-secret");
                }
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
            let client_auth = tls_config.client_auth.unwrap();
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
            "Config Error - GSSAPI mechanism is not supported yet"
        );
    }

    #[test]
    fn test_try_from_kafka_source_with_missing_password() {
        let secret_name = "plain-auth-secret";
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
        let cert_name = "tls-cert";
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
}
