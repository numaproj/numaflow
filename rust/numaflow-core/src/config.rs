use std::env;
use std::sync::OnceLock;

use crate::config::mvtxcfg::monovertex::MonovertexConfig;
use crate::config::plcfg::pipeline::PipelineConfig;
use crate::Error;
use crate::Result;

const ENV_MONO_VERTEX_OBJ: &str = "NUMAFLOW_MONO_VERTEX_OBJECT";
const ENV_VERTEX_OBJ: &str = "NUMAFLOW_VERTEX_OBJECT";

pub(crate) mod common;
pub(crate) mod mvtxcfg;
pub(crate) mod plcfg;

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

pub fn config() -> &'static Settings {
    static CONF: OnceLock<Settings> = OnceLock::new();
    CONF.get_or_init(|| match Settings::load() {
        Ok(v) => v,
        Err(e) => {
            panic!("Failed to load configuration: {:?}", e);
        }
    })
}

#[derive(Debug, Clone)]
pub(crate) enum CustomResourceType {
    MonoVertex(MonovertexConfig),
    Pipeline(PipelineConfig),
}

#[derive(Debug, Clone)]
pub(crate) struct Settings {
    pub custom_resource_type: CustomResourceType,
}

impl Settings {
    fn load() -> Result<Self> {
        if let Ok(obj) = env::var(ENV_MONO_VERTEX_OBJ) {
            let cfg = MonovertexConfig::load(obj)?;
            return Ok(Settings {
                custom_resource_type: CustomResourceType::MonoVertex(cfg),
            });
        }

        if let Ok(obj) = env::var(ENV_VERTEX_OBJ) {
            let cfg = PipelineConfig::load(obj)?;
            return Ok(Settings {
                custom_resource_type: CustomResourceType::Pipeline(cfg),
            });
        }
        Err(Error::Config("No configuration found".to_string()))
    }
}

#[cfg(test)]
mod tests {
    use crate::config::common::sink::OnFailureStrategy;
    use crate::config::{CustomResourceType, Settings, ENV_MONO_VERTEX_OBJ};
    use base64::prelude::BASE64_STANDARD;
    use base64::Engine;
    use serde_json::json;
    use std::env;

    #[test]
    fn test_settings_load_combined() {
        // Define all JSON test configurations in separate scopes to use them distinctively
        {
            let json_data = json!({
                "metadata": {
                    "name": "simple-mono-vertex",
                    "namespace": "default",
                    "creationTimestamp": null
                },
                "spec": {
                    "replicas": 0,
                    "source": {
                        "transformer": {
                            "container": {
                                "image": "xxxxxxx",
                                "resources": {}
                            },
                            "builtin": null
                        },
                        "udsource": {
                            "container": {
                                "image": "xxxxxxx",
                                "resources": {}
                            }
                        }
                    },
                    "sink": {
                        "udsink": {
                            "container": {
                                "image": "xxxxxx",
                                "resources": {}
                            }
                        }
                    },
                    "limits": {
                        "readBatchSize": 500,
                        "readTimeout": "1s"
                    },
                    "scale": {},
                    "status": {
                        "replicas": 0,
                        "lastUpdated": null,
                        "lastScaledAt": null
                    }
                }
            });
            let json_str = json_data.to_string();
            let encoded_json = BASE64_STANDARD.encode(json_str);
            env::set_var(ENV_MONO_VERTEX_OBJ, encoded_json);

            // Execute and verify
            let settings = Settings::load().unwrap();
            assert!(matches!(
                settings.custom_resource_type,
                CustomResourceType::MonoVertex(_)
            ));
            env::remove_var(ENV_MONO_VERTEX_OBJ);
        }

        {
            // Test Retry Strategy Load
            let json_data = json!({
                "metadata": {
                    "name": "simple-mono-vertex",
                    "namespace": "default",
                    "creationTimestamp": null
                },
                "spec": {
                    "replicas": 0,
                    "source": {
                        "udsource": {
                            "container": {
                                "image": "xxxxxxx",
                                "resources": {}
                            }
                        }
                    },
                    "sink": {
                        "udsink": {
                            "container": {
                                "image": "xxxxxx",
                                "resources": {}
                            }
                        },
                        "retryStrategy": {
                            "backoff": {
                                "interval": "1s",
                                "steps": 5
                            },
                        },
                    },
                    "limits": {
                        "readBatchSize": 500,
                        "readTimeout": "1s"
                    },
                }
            });
            let json_str = json_data.to_string();
            let encoded_json = BASE64_STANDARD.encode(json_str);
            env::set_var(ENV_MONO_VERTEX_OBJ, encoded_json);

            // Execute and verify
            let settings = Settings::load().unwrap();
            let mvtx_cfg = match settings.custom_resource_type {
                CustomResourceType::MonoVertex(cfg) => cfg,
                _ => panic!("Invalid configuration type"),
            };

            assert_eq!(
                mvtx_cfg
                    .sink_config
                    .retry_config
                    .clone()
                    .unwrap()
                    .sink_max_retry_attempts,
                5
            );
            assert_eq!(
                mvtx_cfg
                    .sink_config
                    .retry_config
                    .unwrap()
                    .sink_retry_interval_in_ms,
                1000
            );
            env::remove_var(ENV_MONO_VERTEX_OBJ);
        }

        {
            // Test Non default Retry Strategy Load
            let json_data = json!({
                "metadata": {
                    "name": "simple-mono-vertex",
                    "namespace": "default",
                    "creationTimestamp": null
                },
                "spec": {
                    "replicas": 0,
                    "source": {
                        "udsource": {
                            "container": {
                                "image": "xxxxxxx",
                                "resources": {}
                            }
                        }
                    },
                    "sink": {
                        "udsink": {
                            "container": {
                                "image": "xxxxxx",
                                "resources": {}
                            }
                        },
                        "retryStrategy": {
                            "backoff": {
                                "interval": "1s",
                                "steps": 5
                            },
                            "onFailure": "drop"
                        },
                    },
                    "limits": {
                        "readBatchSize": 500,
                        "readTimeout": "1s"
                    },
                }
            });
            let json_str = json_data.to_string();
            let encoded_json = BASE64_STANDARD.encode(json_str);
            env::set_var(ENV_MONO_VERTEX_OBJ, encoded_json);

            // Execute and verify
            let settings = Settings::load().unwrap();
            let mvtx_cfg = match settings.custom_resource_type {
                CustomResourceType::MonoVertex(cfg) => cfg,
                _ => panic!("Invalid configuration type"),
            };

            assert_eq!(
                mvtx_cfg
                    .sink_config
                    .retry_config
                    .clone()
                    .unwrap()
                    .sink_retry_on_fail_strategy,
                OnFailureStrategy::Drop
            );
            assert_eq!(
                mvtx_cfg
                    .sink_config
                    .retry_config
                    .clone()
                    .unwrap()
                    .sink_max_retry_attempts,
                5
            );
            assert_eq!(
                mvtx_cfg
                    .sink_config
                    .retry_config
                    .clone()
                    .unwrap()
                    .sink_retry_interval_in_ms,
                1000
            );
            env::remove_var(ENV_MONO_VERTEX_OBJ);
        }
        {
            // Test Invalid on failure strategy to use default
            let json_data = json!({
                "metadata": {
                    "name": "simple-mono-vertex",
                    "namespace": "default",
                    "creationTimestamp": null
                },
                "spec": {
                    "replicas": 0,
                    "source": {
                        "udsource": {
                            "container": {
                                "image": "xxxxxxx",
                                "resources": {}
                            }
                        }
                    },
                    "sink": {
                        "udsink": {
                            "container": {
                                "image": "xxxxxx",
                                "resources": {}
                            }
                        },
                        "retryStrategy": {
                            "backoff": {
                                "interval": "1s",
                                "steps": 5
                            },
                            "onFailure": "xxxxx"
                        },
                    },
                    "limits": {
                        "readBatchSize": 500,
                        "readTimeout": "1s"
                    },
                }
            });
            let json_str = json_data.to_string();
            let encoded_json = BASE64_STANDARD.encode(json_str);
            env::set_var(ENV_MONO_VERTEX_OBJ, encoded_json);

            // Execute and verify
            let settings = Settings::load().unwrap();
            let mvtx_config = match settings.custom_resource_type {
                CustomResourceType::MonoVertex(cfg) => cfg,
                _ => panic!("Invalid configuration type"),
            };

            assert_eq!(
                mvtx_config
                    .sink_config
                    .retry_config
                    .clone()
                    .unwrap()
                    .sink_retry_on_fail_strategy,
                OnFailureStrategy::Retry
            );
            assert_eq!(
                mvtx_config
                    .sink_config
                    .retry_config
                    .clone()
                    .unwrap()
                    .sink_max_retry_attempts,
                5
            );
            assert_eq!(
                mvtx_config
                    .sink_config
                    .retry_config
                    .clone()
                    .unwrap()
                    .sink_retry_interval_in_ms,
                1000
            );
            env::remove_var(ENV_MONO_VERTEX_OBJ);
        }
    }
}
