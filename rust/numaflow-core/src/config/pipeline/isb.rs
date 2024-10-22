/// Jetstream ISB related configurations.
pub mod jetstream {
    use std::fmt;
    use std::time::Duration;

    // jetstream related constants
    const DEFAULT_PARTITION_IDX: u16 = 0;
    const DEFAULT_MAX_LENGTH: usize = 30000;
    const DEFAULT_USAGE_LIMIT: f64 = 0.8;
    const DEFAULT_REFRESH_INTERVAL_SECS: u64 = 1;
    const DEFAULT_BUFFER_FULL_STRATEGY: BufferFullStrategy = BufferFullStrategy::RetryUntilSuccess;
    const DEFAULT_RETRY_INTERVAL_MILLIS: u64 = 10;

    #[derive(Debug, Clone)]
    pub(crate) struct StreamWriterConfig {
        pub name: String,
        pub partition_idx: u16,
        pub max_length: usize,
        pub refresh_interval: Duration,
        pub usage_limit: f64,
        pub buffer_full_strategy: BufferFullStrategy,
        pub retry_interval: Duration,
    }

    impl Default for StreamWriterConfig {
        fn default() -> Self {
            StreamWriterConfig {
                name: "default".to_string(),
                partition_idx: DEFAULT_PARTITION_IDX,
                max_length: DEFAULT_MAX_LENGTH,
                usage_limit: DEFAULT_USAGE_LIMIT,
                refresh_interval: Duration::from_secs(DEFAULT_REFRESH_INTERVAL_SECS),
                buffer_full_strategy: DEFAULT_BUFFER_FULL_STRATEGY,
                retry_interval: Duration::from_millis(DEFAULT_RETRY_INTERVAL_MILLIS),
            }
        }
    }

    #[derive(Debug, Clone)]
    pub(crate) struct StreamReaderConfig {
        pub name: String,
        pub batch_size: usize,
        pub wip_acks: Duration,
    }

    #[derive(Debug, Clone, Eq, PartialEq)]
    pub(crate) enum BufferFullStrategy {
        RetryUntilSuccess,
        DiscardLatest,
    }

    impl fmt::Display for BufferFullStrategy {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            match self {
                BufferFullStrategy::RetryUntilSuccess => write!(f, "retryUntilSuccess"),
                BufferFullStrategy::DiscardLatest => write!(f, "discardLatest"),
            }
        }
    }
}
