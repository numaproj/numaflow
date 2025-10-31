const DEFAULT_GRPC_MAX_MESSAGE_SIZE: usize = 64 * 1024 * 1024; // 64 MB
const DEFAULT_SINK_SOCKET: &str = "/var/run/numaflow/sink.sock";
const DEFAULT_SINK_SERVER_INFO_FILE: &str = "/var/run/numaflow/sinker-server-info";
const DEFAULT_FB_SINK_SOCKET: &str = "/var/run/numaflow/fb-sink.sock";
const DEFAULT_FB_SINK_SERVER_INFO_FILE: &str = "/var/run/numaflow/fb-sinker-server-info";
const DEFAULT_ONS_SINK_SOCKET: &str = "/var/run/numaflow/ons-sink.sock";
const DEFAULT_ONS_SINK_SERVER_INFO_FILE: &str = "/var/run/numaflow/ons-sinker-server-info";
const DEFAULT_SINK_RETRY_ON_FAIL_STRATEGY: OnFailureStrategy = OnFailureStrategy::Retry;
const DEFAULT_MAX_SINK_RETRY_ATTEMPTS: u16 = u16::MAX;
const DEFAULT_SINK_INITIAL_RETRY_INTERVAL_IN_MS: u32 = 1;
const DEFAULT_SINK_MAX_RETRY_INTERVAL_IN_MS: u32 = u32::MAX;
const DEFAULT_SINK_RETRY_FACTOR: f64 = 1.0;
const DEFAULT_SINK_RETRY_JITTER: f64 = 0.0;

use std::collections::HashMap;
use std::fmt::Display;

use numaflow_kafka::sink::KafkaSinkConfig;
use numaflow_models::models::{KafkaSink, PulsarSink, RetryStrategy, Sink, SqsSink};
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

    pub(crate) fn on_success_sinktype(sink: &Sink) -> Result<Self> {
        if let Some(on_success) = sink.on_success.as_ref() {
            on_success
                .udsink
                .as_ref()
                .map(|_| {
                    Ok(SinkType::UserDefined(
                        UserDefinedConfig::on_success_default(),
                    ))
                })
                .or_else(|| {
                    on_success
                        .log
                        .as_ref()
                        .map(|_| Ok(SinkType::Log(LogConfig::default())))
                })
                .or_else(|| {
                    on_success
                        .blackhole
                        .as_ref()
                        .map(|_| Ok(SinkType::Blackhole(BlackholeConfig::default())))
                })
                .or_else(|| on_success.serve.as_ref().map(|_| Ok(SinkType::Serve)))
                .or_else(|| on_success.sqs.as_ref().map(|sqs| sqs.clone().try_into()))
                .or_else(|| {
                    on_success
                        .kafka
                        .as_ref()
                        .map(|kafka| kafka.clone().try_into())
                })
                .or_else(|| {
                    on_success
                        .pulsar
                        .as_ref()
                        .map(|pulsar| pulsar.clone().try_into())
                })
                .ok_or_else(|| Error::Config("Sink type not found".to_string()))?
        } else {
            Err(Error::Config("OnSuccess sink not found".to_string()))
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
        // Convert assume role configuration if present
        let assume_role_config = value.assume_role.map(|ar| numaflow_sqs::AssumeRoleConfig {
            role_arn: ar.role_arn,
            session_name: ar.session_name,
            duration_seconds: ar.duration_seconds,
            external_id: ar.external_id,
            policy: ar.policy,
            policy_arns: ar.policy_arns,
        });

        let sqs_sink_config = SqsSinkConfig {
            queue_name: Box::leak(value.queue_name.into_boxed_str()),
            region: Box::leak(value.aws_region.into_boxed_str()),
            queue_owner_aws_account_id: Box::leak(
                value.queue_owner_aws_account_id.into_boxed_str(),
            ),
            assume_role_config,
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
                .map(|parts| {
                    (
                        parts
                            .first()
                            .expect("should have first part")
                            .trim()
                            .to_string(),
                        parts
                            .get(1)
                            .expect("should have second part")
                            .trim()
                            .to_string(),
                    )
                })
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
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            sink_max_retry_attempts: DEFAULT_MAX_SINK_RETRY_ATTEMPTS,
            sink_initial_retry_interval_in_ms: DEFAULT_SINK_INITIAL_RETRY_INTERVAL_IN_MS,
            sink_max_retry_interval_in_ms: DEFAULT_SINK_MAX_RETRY_INTERVAL_IN_MS,
            sink_retry_factor: DEFAULT_SINK_RETRY_FACTOR,
            sink_retry_jitter: DEFAULT_SINK_RETRY_JITTER,
            sink_retry_on_fail_strategy: DEFAULT_SINK_RETRY_ON_FAIL_STRATEGY,
        }
    }
}

impl From<Box<RetryStrategy>> for RetryConfig {
    fn from(retry: Box<RetryStrategy>) -> Self {
        let mut retry_config = RetryConfig::default();
        if let Some(strategy) = &retry.on_failure {
            retry_config.sink_retry_on_fail_strategy = OnFailureStrategy::from_str(strategy);
        }

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

        if retry_config.sink_retry_on_fail_strategy == OnFailureStrategy::Retry {
            retry_config.sink_max_retry_attempts = DEFAULT_MAX_SINK_RETRY_ATTEMPTS;
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

    pub(crate) fn on_success_default() -> Self {
        Self {
            grpc_max_message_size: DEFAULT_GRPC_MAX_MESSAGE_SIZE,
            socket_path: DEFAULT_ONS_SINK_SOCKET.to_string(),
            server_info_path: DEFAULT_ONS_SINK_SERVER_INFO_FILE.to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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
            assume_role_config: None,
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

        // Test case: Valid configuration
        let valid_sqs_sink = Box::new(SqsSink::new(
            "us-west-2".to_string(),
            "test-queue".to_string(),
            "123456789012".to_string(),
        ));

        let result = SinkType::try_from(valid_sqs_sink);
        assert!(result.is_ok());
        if let Ok(SinkType::Sqs(config)) = result {
            assert_eq!(config.region, "us-west-2");
            assert_eq!(config.queue_name, "test-queue");
            assert_eq!(config.queue_owner_aws_account_id, "123456789012");
        } else {
            panic!("Expected SinkType::Sqs");
        }
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
                sqs: Some(Box::new(SqsSink::new(
                    "us-west-2".to_string(),
                    "fallback-queue".to_string(),
                    "123456789012".to_string(),
                ))),
                kafka: None,
                pulsar: None,
            })),
            on_success: Some(Box::new(AbstractSink {
                udsink: None,
                log: None,
                blackhole: None,
                serve: None,
                sqs: Some(Box::new(SqsSink::new(
                    "us-west-2".to_string(),
                    "fallback-queue".to_string(),
                    "123456789012".to_string(),
                ))),
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

        let result = SinkType::on_success_sinktype(&sink);
        assert!(result.is_ok());
        match result {
            Ok(SinkType::Sqs(config)) => {
                assert_eq!(config.region, "us-west-2");
                assert_eq!(config.queue_name, "fallback-queue");
                assert_eq!(config.queue_owner_aws_account_id, "123456789012");
            }
            _ => panic!("Expected SinkType::Sqs for on success sink"),
        }

        // Test case 2: Missing fallback configuration
        let sink_without_fallback = Sink {
            udsink: None,
            log: None,
            blackhole: None,
            serve: None,
            sqs: None,
            fallback: None,
            on_success: None,
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

        // Test case 3: Empty fallback sink configuration
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
            on_success: Some(Box::new(AbstractSink {
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
        let result = SinkType::on_success_sinktype(&sink_empty_fallback);
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
            on_success: Some(Box::new(AbstractSink {
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

        let result = SinkType::on_success_sinktype(&sink);
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
            on_success: Some(Box::new(AbstractSink {
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

        let result = SinkType::on_success_sinktype(&sink_with_auth);
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
            on_success: Some(Box::new(AbstractSink {
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

        let result = SinkType::on_success_sinktype(&sink);
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
            on_success: Some(Box::new(AbstractSink {
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

        let result = SinkType::on_success_sinktype(&sink);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            err.to_string()
                .contains("Failed to get token secret from volume")
        );
    }
}
