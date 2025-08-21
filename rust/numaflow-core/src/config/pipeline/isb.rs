use crate::config::pipeline::DEFAULT_MAX_ACK_PENDING;
/// JetStream ISB related configurations.
use std::fmt;
use std::fmt::Display;
use std::time::Duration;

const DEFAULT_PARTITION_IDX: u16 = 0;
const DEFAULT_MAX_LENGTH: usize = 30000;
const DEFAULT_USAGE_LIMIT: f64 = 0.8;
const DEFAULT_BUFFER_FULL_STRATEGY: BufferFullStrategy = BufferFullStrategy::RetryUntilSuccess;
const DEFAULT_WIP_ACK_INTERVAL_MILLIS: u64 = 1000;

pub(crate) mod jetstream {
    const DEFAULT_URL: &str = "localhost:4222";
    #[derive(Debug, Clone, PartialEq)]
    pub(crate) struct ClientConfig {
        pub url: String,
        pub user: Option<String>,
        pub password: Option<String>,
    }

    impl Default for ClientConfig {
        fn default() -> Self {
            ClientConfig {
                url: DEFAULT_URL.to_string(),
                user: None,
                password: None,
            }
        }
    }
}

/// Stream is a one of the partition of the ISB between two vertices.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Stream {
    pub(crate) name: &'static str,
    pub(crate) vertex: &'static str,
    pub(crate) partition: u16,
}

impl Default for Stream {
    fn default() -> Self {
        Stream {
            name: "",
            vertex: "",
            partition: DEFAULT_PARTITION_IDX,
        }
    }
}

impl Stream {
    pub(crate) fn new(name: &'static str, vertex: &'static str, partition: u16) -> Self {
        Stream {
            name,
            vertex,
            partition,
        }
    }
}

impl Display for Stream {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.name)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct BufferWriterConfig {
    pub(crate) streams: Vec<Stream>,
    pub(crate) max_length: usize,
    pub(crate) usage_limit: f64,
    pub(crate) buffer_full_strategy: BufferFullStrategy,
}

impl Default for BufferWriterConfig {
    fn default() -> Self {
        BufferWriterConfig {
            streams: vec![],
            max_length: DEFAULT_MAX_LENGTH,
            usage_limit: DEFAULT_USAGE_LIMIT,
            buffer_full_strategy: DEFAULT_BUFFER_FULL_STRATEGY,
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Default)]
pub(crate) enum BufferFullStrategy {
    #[default]
    RetryUntilSuccess,
    DiscardLatest,
}

impl TryFrom<String> for BufferFullStrategy {
    type Error = &'static str;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        match value.as_str() {
            "retryUntilSuccess" => Ok(BufferFullStrategy::RetryUntilSuccess),
            "discardLatest" => Ok(BufferFullStrategy::DiscardLatest),
            _ => Err("Invalid BufferFullStrategy string"),
        }
    }
}

impl Display for BufferFullStrategy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BufferFullStrategy::RetryUntilSuccess => write!(f, "retryUntilSuccess"),
            BufferFullStrategy::DiscardLatest => write!(f, "discardLatest"),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct BufferReaderConfig {
    pub(crate) streams: Vec<Stream>,
    pub(crate) wip_ack_interval: Duration,
    pub(crate) max_ack_pending: usize,
}

impl Default for BufferReaderConfig {
    fn default() -> Self {
        BufferReaderConfig {
            streams: vec![Stream::new("default-0", "default", DEFAULT_PARTITION_IDX)],
            wip_ack_interval: Duration::from_millis(DEFAULT_WIP_ACK_INTERVAL_MILLIS),
            max_ack_pending: DEFAULT_MAX_ACK_PENDING,
        }
    }
}

#[cfg(test)]
mod jetstream_client_config {
    use super::jetstream::*;

    #[test]
    fn test_default_client_config() {
        let expected_config = ClientConfig {
            url: "localhost:4222".to_string(),
            user: None,
            password: None,
        };
        let config = ClientConfig::default();
        assert_eq!(config, expected_config);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_buffer_writer_config() {
        let expected = BufferWriterConfig {
            streams: vec![],
            max_length: DEFAULT_MAX_LENGTH,
            usage_limit: DEFAULT_USAGE_LIMIT,
            buffer_full_strategy: DEFAULT_BUFFER_FULL_STRATEGY,
        };
        let config = BufferWriterConfig::default();

        assert_eq!(config, expected);
    }

    #[test]
    fn test_buffer_full_strategy_display() {
        let val = BufferFullStrategy::RetryUntilSuccess;
        assert_eq!(val.to_string(), "retryUntilSuccess");

        let val = BufferFullStrategy::DiscardLatest;
        assert_eq!(val.to_string(), "discardLatest");
    }

    #[test]
    fn test_default_buffer_reader_config() {
        let expected = BufferReaderConfig {
            streams: vec![Stream::new("default-0", "default", DEFAULT_PARTITION_IDX)],
            wip_ack_interval: Duration::from_millis(DEFAULT_WIP_ACK_INTERVAL_MILLIS),
            ..Default::default()
        };
        let config = BufferReaderConfig::default();
        assert_eq!(config, expected);
    }

    #[test]
    fn test_try_from_string_to_buffer_full_strategy() {
        assert_eq!(
            BufferFullStrategy::try_from("retryUntilSuccess".to_string()).unwrap(),
            BufferFullStrategy::RetryUntilSuccess
        );
        assert_eq!(
            BufferFullStrategy::try_from("discardLatest".to_string()).unwrap(),
            BufferFullStrategy::DiscardLatest
        );
        assert!(BufferFullStrategy::try_from("invalidStrategy".to_string()).is_err());
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ISBConfig {
    pub(crate) compression: Compression,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Compression {
    pub(crate) compress_type: CompressionType,
}

#[derive(Debug, Copy, Clone, PartialEq)]
pub(crate) enum CompressionType {
    None,
    Gzip,
    Zstd,
    LZ4,
}

impl TryFrom<numaflow_models::models::Compression> for Compression {
    type Error = String;
    fn try_from(value: numaflow_models::models::Compression) -> Result<Self, Self::Error> {
        match value.r#type {
            None => Ok(Compression {
                compress_type: CompressionType::None,
            }),
            Some(t) => match t.as_str() {
                "gzip" => Ok(Compression {
                    compress_type: CompressionType::Gzip,
                }),
                "zstd" => Ok(Compression {
                    compress_type: CompressionType::Zstd,
                }),
                "lz4" => Ok(Compression {
                    compress_type: CompressionType::LZ4,
                }),
                "none" => Ok(Compression {
                    compress_type: CompressionType::None,
                }),
                _ => Err(format!("Invalid compression type: {t}")),
            },
        }
    }
}
