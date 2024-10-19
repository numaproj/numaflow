pub(crate) mod source {
    const DEFAULT_GRPC_MAX_MESSAGE_SIZE: usize = 64 * 1024 * 1024; // 64 MB
    const DEFAULT_SOURCE_SOCKET: &str = "/var/run/numaflow/source.sock";
    const DEFAULT_SOURCE_SERVER_INFO_FILE: &str = "/var/run/numaflow/sourcer-server-info";

    use bytes::Bytes;
    use std::time::Duration;

    #[derive(Debug, Clone, PartialEq)]
    pub(crate) struct SourceConfig {
        pub(crate) source_type: SourceType,
    }

    #[derive(Debug, Clone, PartialEq)]
    pub(crate) enum SourceType {
        Generator(GeneratorConfig),
        UserDefined(UserDefinedConfig),
    }

    #[derive(Debug, Clone, PartialEq)]
    pub(crate) struct GeneratorConfig {
        pub rpu: usize,
        pub content: Bytes,
        pub duration: usize,
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
                duration: 1000,
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

    use crate::error::Error;
    use crate::Result;
    use numaflow_models::models::{Backoff, RetryStrategy};
    use std::fmt::Display;

    #[derive(Debug, Clone, PartialEq)]
    pub(crate) struct SinkConfig {
        pub(crate) sink_type: SinkType,
        pub(crate) retry_config: Option<RetryConfig>,
    }

    #[derive(Debug, Clone, PartialEq)]
    pub(crate) enum SinkType {
        Log(LogConfig),
        Blackhole(BlackholeConfig),
        UserDefined(UserDefinedConfig),
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
        pub(crate) transformer_type: TransformerType,
    }

    #[derive(Debug, Clone, PartialEq)]
    pub(crate) enum TransformerType {
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

    #[derive(Debug, Clone, PartialEq)]
    pub(crate) struct MetricsConfig {
        pub metrics_server_listen_port: u16,
        pub lag_check_interval_in_secs: u16,
        pub lag_refresh_interval_in_secs: u16,
    }

    impl Default for MetricsConfig {
        fn default() -> Self {
            Self {
                metrics_server_listen_port: DEFAULT_METRICS_PORT,
                lag_check_interval_in_secs: DEFAULT_LAG_CHECK_INTERVAL_IN_SECS,
                lag_refresh_interval_in_secs: DEFAULT_LAG_REFRESH_INTERVAL_IN_SECS,
            }
        }
    }
}

#[cfg(test)]
mod source_tests {
    use super::source::{GeneratorConfig, SourceConfig, SourceType, UserDefinedConfig};
    use bytes::Bytes;
    use std::time::Duration;

    #[test]
    fn test_default_generator_config() {
        let default_config = GeneratorConfig::default();
        assert_eq!(default_config.rpu, 1);
        assert_eq!(default_config.content, Bytes::new());
        assert_eq!(default_config.duration, 1000);
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
            transformer_type: TransformerType::UserDefined(user_defined_config.clone()),
        };
        if let TransformerType::UserDefined(config) = transformer_config.transformer_type {
            assert_eq!(config, user_defined_config);
        } else {
            panic!("Expected TransformerType::UserDefined");
        }
    }
}
