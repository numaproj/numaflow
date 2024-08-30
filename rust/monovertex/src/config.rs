use std::env;
use std::sync::OnceLock;

use base64::prelude::BASE64_STANDARD;
use base64::Engine;

use numaflow_models::models::{Backoff, MonoVertex, RetryStrategy};

use crate::error::Error;

const ENV_MONO_VERTEX_OBJ: &str = "NUMAFLOW_MONO_VERTEX_OBJECT";
const ENV_GRPC_MAX_MESSAGE_SIZE: &str = "NUMAFLOW_GRPC_MAX_MESSAGE_SIZE";
const ENV_POD_REPLICA: &str = "NUMAFLOW_REPLICA";
const DEFAULT_GRPC_MAX_MESSAGE_SIZE: usize = 64 * 1024 * 1024; // 64 MB
const DEFAULT_METRICS_PORT: u16 = 2469;
const DEFAULT_LAG_CHECK_INTERVAL_IN_SECS: u16 = 5;
const DEFAULT_LAG_REFRESH_INTERVAL_IN_SECS: u16 = 3;
const DEFAULT_BATCH_SIZE: u64 = 500;
const DEFAULT_TIMEOUT_IN_MS: u32 = 1000;
const DEFAULT_MAX_SINK_RETRY_ATTEMPTS: u16 = u16::MAX;
const DEFAULT_SINK_RETRY_INTERVAL_IN_MS: u32 = 1;
const DEFAULT_SINK_RETRY_ON_FAIL_STRATEGY: &str = "retry";

pub fn config() -> &'static Settings {
    static CONF: OnceLock<Settings> = OnceLock::new();
    CONF.get_or_init(|| match Settings::load() {
        Ok(v) => v,
        Err(e) => {
            panic!("Failed to load configuration: {:?}", e);
        }
    })
}

pub struct Settings {
    pub mono_vertex_name: String,
    pub replica: u32,
    pub batch_size: u64,
    pub timeout_in_ms: u32,
    pub metrics_server_listen_port: u16,
    pub grpc_max_message_size: usize,
    pub is_transformer_enabled: bool,
    pub is_fallback_enabled: bool,
    pub lag_check_interval_in_secs: u16,
    pub lag_refresh_interval_in_secs: u16,
    pub sink_max_retry_attempts: u16,
    pub sink_retry_interval_in_ms: u32,
    pub sink_retry_on_fail_strategy: String,
    pub sink_default_retry_strategy: RetryStrategy,
}

impl Default for Settings {
    fn default() -> Self {
        // Create a default retry strategy from defined constants
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
            mono_vertex_name: "default".to_string(),
            replica: 0,
            batch_size: DEFAULT_BATCH_SIZE,
            timeout_in_ms: DEFAULT_TIMEOUT_IN_MS,
            metrics_server_listen_port: DEFAULT_METRICS_PORT,
            grpc_max_message_size: DEFAULT_GRPC_MAX_MESSAGE_SIZE,
            is_transformer_enabled: false,
            is_fallback_enabled: false,
            lag_check_interval_in_secs: DEFAULT_LAG_CHECK_INTERVAL_IN_SECS,
            lag_refresh_interval_in_secs: DEFAULT_LAG_REFRESH_INTERVAL_IN_SECS,
            sink_max_retry_attempts: DEFAULT_MAX_SINK_RETRY_ATTEMPTS,
            sink_retry_interval_in_ms: DEFAULT_SINK_RETRY_INTERVAL_IN_MS,
            sink_retry_on_fail_strategy: DEFAULT_SINK_RETRY_ON_FAIL_STRATEGY.to_string(),
            sink_default_retry_strategy: default_retry_strategy,
        }
    }
}

impl Settings {
    fn load() -> Result<Self, Error> {
        let mut settings = Settings::default();
        if let Ok(mono_vertex_spec) = env::var(ENV_MONO_VERTEX_OBJ) {
            // decode the spec it will be base64 encoded
            let mono_vertex_spec = BASE64_STANDARD
                .decode(mono_vertex_spec.as_bytes())
                .map_err(|e| {
                    Error::ConfigError(format!("Failed to decode mono vertex spec: {:?}", e))
                })?;

            let mono_vertex_obj: MonoVertex =
                serde_json::from_slice(&mono_vertex_spec).map_err(|e| {
                    Error::ConfigError(format!("Failed to parse mono vertex spec: {:?}", e))
                })?;

            settings.batch_size = mono_vertex_obj
                .spec
                .limits
                .clone()
                .unwrap()
                .read_batch_size
                .map(|x| x as u64)
                .unwrap_or(DEFAULT_BATCH_SIZE);

            settings.timeout_in_ms = mono_vertex_obj
                .spec
                .limits
                .clone()
                .unwrap()
                .read_timeout
                .map(|x| std::time::Duration::from(x).as_millis() as u32)
                .unwrap_or(DEFAULT_TIMEOUT_IN_MS);

            settings.mono_vertex_name = mono_vertex_obj
                .metadata
                .and_then(|metadata| metadata.name)
                .ok_or_else(|| Error::ConfigError("Mono vertex name not found".to_string()))?;

            settings.is_transformer_enabled = mono_vertex_obj
                .spec
                .source
                .ok_or(Error::ConfigError("Source not found".to_string()))?
                .transformer
                .is_some();

            settings.is_fallback_enabled = mono_vertex_obj
                .spec
                .sink
                .as_deref()
                .ok_or(Error::ConfigError("Sink not found".to_string()))?
                .fallback
                .is_some();

            if let Some(retry_strategy) = mono_vertex_obj
                .spec
                .sink
                .expect("sink should not be empty")
                .retry_strategy
            {
                if let Some(sink_backoff) = retry_strategy.clone().backoff {
                    // Set the max retry attempts and retry interval using direct reference
                    settings.sink_retry_interval_in_ms = sink_backoff
                        .clone()
                        .interval
                        .map(|x| std::time::Duration::from(x).as_millis() as u32)
                        .unwrap_or(DEFAULT_SINK_RETRY_INTERVAL_IN_MS);

                    settings.sink_max_retry_attempts = sink_backoff
                        .clone()
                        .steps
                        .map(|x| x as u16)
                        .unwrap_or(DEFAULT_MAX_SINK_RETRY_ATTEMPTS);

                    // We do not allow 0 attempts to write to sink
                    if settings.sink_max_retry_attempts == 0 {
                        return Err(Error::ConfigError(
                            "Retry Strategy given with 0 retry attempts".to_string(),
                        ));
                    }
                }

                // Set the retry strategy using a direct reference whenever possible
                settings.sink_retry_on_fail_strategy = retry_strategy
                    .on_failure
                    .clone()
                    .unwrap_or_else(|| DEFAULT_SINK_RETRY_ON_FAIL_STRATEGY.to_string());

                // check if the sink retry strategy is set to fallback and there is no fallback sink configured
                // then we should return an error
                if settings.sink_retry_on_fail_strategy == "fallback"
                    && !settings.is_fallback_enabled
                {
                    return Err(Error::ConfigError(
                        "Retry Strategy given as fallback but Fallback sink not configured"
                            .to_string(),
                    ));
                }
            }
        }

        settings.grpc_max_message_size = env::var(ENV_GRPC_MAX_MESSAGE_SIZE)
            .unwrap_or_else(|_| DEFAULT_GRPC_MAX_MESSAGE_SIZE.to_string())
            .parse()
            .map_err(|e| {
                Error::ConfigError(format!("Failed to parse grpc max message size: {:?}", e))
            })?;

        settings.replica = env::var(ENV_POD_REPLICA)
            .unwrap_or_else(|_| "0".to_string())
            .parse()
            .map_err(|e| Error::ConfigError(format!("Failed to parse pod replica: {:?}", e)))?;

        Ok(settings)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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
            assert_eq!(settings.mono_vertex_name, "simple-mono-vertex");
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
            assert_eq!(settings.sink_retry_on_fail_strategy, "retry");
            assert_eq!(settings.sink_max_retry_attempts, 5);
            assert_eq!(settings.sink_retry_interval_in_ms, 1000);
            env::remove_var(ENV_MONO_VERTEX_OBJ);
        }

        {
            // Test Error Case: Retry Strategy Fallback without Fallback Sink
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
                            "onFailure": "fallback"
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
            assert!(Settings::load().is_err());
            env::remove_var(ENV_MONO_VERTEX_OBJ);
        }

        {
            // Test Error Case: Retry Strategy with 0 Retry Attempts
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
                                "steps": 0
                            },
                            "onFailure": "retry"
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
            assert!(Settings::load().is_err());
            env::remove_var(ENV_MONO_VERTEX_OBJ);
        }
        // General cleanup
        env::remove_var(ENV_GRPC_MAX_MESSAGE_SIZE);
    }
}
