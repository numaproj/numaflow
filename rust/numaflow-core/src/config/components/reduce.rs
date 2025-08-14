const DEFAULT_GRPC_MAX_MESSAGE_SIZE: usize = 64 * 1024 * 1024; // 64 MB
const DEFAULT_REDUCER_SOCKET: &str = "/var/run/numaflow/reduce.sock";
const DEFAULT_REDUCER_SERVER_INFO_FILE: &str = "/var/run/numaflow/reducer-server-info";
const DEFAULT_REDUCE_STREAMER_SERVER_INFO_FILE: &str =
    "/var/run/numaflow/reducestreamer-server-info";
const DEFAULT_REDUCE_STREAMER_SOCKET: &str = "/var/run/numaflow/reducestream.sock";
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
    pub(crate) fn streamer_config() -> Self {
        Self {
            grpc_max_message_size: DEFAULT_GRPC_MAX_MESSAGE_SIZE,
            socket_path: DEFAULT_REDUCE_STREAMER_SOCKET,
            server_info_path: DEFAULT_REDUCE_STREAMER_SERVER_INFO_FILE,
        }
    }
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
    pub(crate) streaming: bool,
}

impl From<Box<FixedWindow>> for FixedWindowConfig {
    fn from(value: Box<FixedWindow>) -> Self {
        Self {
            length: value.length.map(Duration::from).unwrap_or_default(),
            streaming: value.streaming.unwrap_or_default(),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct SlidingWindowConfig {
    pub(crate) length: Duration,
    pub(crate) slide: Duration,
    pub(crate) streaming: bool,
}

impl From<Box<SlidingWindow>> for SlidingWindowConfig {
    fn from(value: Box<SlidingWindow>) -> Self {
        Self {
            length: value.length.map(Duration::from).unwrap_or_default(),
            slide: value.slide.map(Duration::from).unwrap_or_default(),
            streaming: value.streaming.unwrap_or_default(),
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
            let user_defined_config = if fixed.streaming.unwrap_or(false) {
                UserDefinedConfig::streamer_config()
            } else {
                UserDefinedConfig::default()
            };
            Ok(ReducerConfig::Aligned(AlignedReducerConfig {
                user_defined_config,
                window_config,
            }))
        } else if let Some(sliding) = &window.sliding {
            let window_config = AlignedWindowConfig {
                window_type: AlignedWindowType::Sliding(sliding.clone().into()),
                allowed_lateness,
                is_keyed,
            };
            let user_defined_config = if sliding.streaming.unwrap_or(false) {
                UserDefinedConfig::streamer_config()
            } else {
                UserDefinedConfig::default()
            };
            Ok(ReducerConfig::Aligned(AlignedReducerConfig {
                user_defined_config,
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    use numaflow_models::models::{
        AccumulatorWindow, FixedWindow, GroupBy, SessionWindow, SlidingWindow, Window,
    };

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
        let fixed_window = Box::new(FixedWindow {
            length: Some(kube::core::Duration::from(Duration::from_secs(120))),
            streaming: None,
        });

        let config = FixedWindowConfig::from(fixed_window);
        assert_eq!(config.length, Duration::from_secs(120));
    }

    #[test]
    fn test_fixed_window_streaming_on() {
        use std::time::Duration;

        let window = Window {
            fixed: Some(Box::new(FixedWindow {
                length: Some(kube::core::Duration::from(Duration::from_secs(42))),
                streaming: Some(true),
            })),
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

        let reducer_config = ReducerConfig::try_from(&group_by).unwrap();
        match reducer_config {
            ReducerConfig::Aligned(aligned_config) => {
                match aligned_config.window_config.window_type {
                    AlignedWindowType::Fixed(config) => {
                        assert_eq!(config.length, Duration::from_secs(42));
                        assert!(config.streaming);
                    }
                    _ => panic!("Expected fixed window type"),
                }
                // Should use streamer_config
                assert_eq!(
                    aligned_config.user_defined_config.socket_path,
                    "/var/run/numaflow/reducestream.sock"
                );
            }
            _ => panic!("Expected aligned reducer config"),
        }
    }

    #[test]
    fn test_fixed_window_streaming_off() {
        use std::time::Duration;

        let window = Window {
            fixed: Some(Box::new(FixedWindow {
                length: Some(kube::core::Duration::from(Duration::from_secs(42))),
                streaming: Some(false),
            })),
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

        let reducer_config = ReducerConfig::try_from(&group_by).unwrap();
        match reducer_config {
            ReducerConfig::Aligned(aligned_config) => {
                match aligned_config.window_config.window_type {
                    AlignedWindowType::Fixed(config) => {
                        assert_eq!(config.length, Duration::from_secs(42));
                        assert!(!config.streaming);
                    }
                    _ => panic!("Expected fixed window type"),
                }
                // Should use default config
                assert_eq!(
                    aligned_config.user_defined_config.socket_path,
                    "/var/run/numaflow/reduce.sock"
                );
            }
            _ => panic!("Expected aligned reducer config"),
        }
    }

    #[test]
    fn test_fixed_window_config_from_fixed_window_default() {
        let fixed_window = Box::new(FixedWindow {
            length: None,
            streaming: None,
        });

        let config = FixedWindowConfig::from(fixed_window);
        assert_eq!(config.length, Duration::default());
    }

    #[test]
    fn test_sliding_window_streaming_on() {
        use std::time::Duration;

        let window = Window {
            fixed: None,
            sliding: Some(Box::new(SlidingWindow {
                length: Some(kube::core::Duration::from(Duration::from_secs(100))),
                slide: Some(kube::core::Duration::from(Duration::from_secs(10))),
                streaming: Some(true),
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
                        assert_eq!(config.length, Duration::from_secs(100));
                        assert_eq!(config.slide, Duration::from_secs(10));
                        assert!(config.streaming);
                    }
                    _ => panic!("Expected sliding window type"),
                }
                // Should use streamer_config
                assert_eq!(
                    aligned_config.user_defined_config.socket_path,
                    "/var/run/numaflow/reducestream.sock"
                );
            }
            _ => panic!("Expected aligned reducer config"),
        }
    }

    #[test]
    fn test_sliding_window_streaming_off() {
        use std::time::Duration;

        let window = Window {
            fixed: None,
            sliding: Some(Box::new(SlidingWindow {
                length: Some(kube::core::Duration::from(Duration::from_secs(100))),
                slide: Some(kube::core::Duration::from(Duration::from_secs(10))),
                streaming: Some(false),
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
                        assert_eq!(config.length, Duration::from_secs(100));
                        assert_eq!(config.slide, Duration::from_secs(10));
                        assert!(!config.streaming);
                    }
                    _ => panic!("Expected sliding window type"),
                }
                // Should use default config
                assert_eq!(
                    aligned_config.user_defined_config.socket_path,
                    "/var/run/numaflow/reduce.sock"
                );
            }
            _ => panic!("Expected aligned reducer config"),
        }
    }

    #[test]
    fn test_sliding_window_config_from_sliding_window() {
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
        let session_window = Box::new(SessionWindow {
            timeout: Some(kube::core::Duration::from(Duration::from_secs(900))),
        });

        let config = SessionWindowConfig::from(session_window);
        assert_eq!(config.timeout, Duration::from_secs(900));
    }

    #[test]
    fn test_session_window_config_from_session_window_default() {
        let session_window = Box::new(SessionWindow { timeout: None });

        let config = SessionWindowConfig::from(session_window);
        assert_eq!(config.timeout, Duration::default());
    }

    #[test]
    fn test_accumulator_window_config_from_accumulator_window() {
        let accumulator_window = Box::new(AccumulatorWindow {
            timeout: Some(kube::core::Duration::from(Duration::from_secs(1200))),
        });

        let config = AccumulatorWindowConfig::from(accumulator_window);
        assert_eq!(config.timeout, Duration::from_secs(1200));
    }

    #[test]
    fn test_accumulator_window_config_from_accumulator_window_default() {
        let accumulator_window = Box::new(AccumulatorWindow { timeout: None });

        let config = AccumulatorWindowConfig::from(accumulator_window);
        assert_eq!(config.timeout, Duration::default());
    }

    #[test]
    fn test_storage_config_default() {
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
        use numaflow_models::models::{PbqStorage, PersistenceStrategy};

        let pbq_storage = PbqStorage {
            persistent_volume_claim: Some(Box::new(PersistenceStrategy {
                access_mode: Some("ReadWriteOncePod".to_string()),
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
