use std::collections::HashMap;
use std::env;
use std::sync::OnceLock;

use monovertex::MonovertexConfig;

use crate::Error;
use crate::Result;
use crate::config::pipeline::PipelineConfig;

const ENV_MONO_VERTEX_OBJ: &str = "NUMAFLOW_MONO_VERTEX_OBJECT";
const ENV_VERTEX_OBJ: &str = "NUMAFLOW_VERTEX_OBJECT";

const ENV_CALLBACK_ENABLED: &str = "NUMAFLOW_CALLBACK_ENABLED";
const ENV_CALLBACK_CONCURRENCY: &str = "NUMAFLOW_CALLBACK_CONCURRENCY";
const ENV_NUMAFLOW_SERVING_SPEC: &str = "NUMAFLOW_SERVING_SPEC";
const ENV_NUMAFLOW_SERVING_CALLBACK_STORE: &str = "NUMAFLOW_SERVING_CALLBACK_STORE";
const ENV_NUMAFLOW_SERVING_RESPONSE_STORE: &str = "NUMAFLOW_SERVING_RESPONSE_STORE";
const DEFAULT_CALLBACK_CONCURRENCY: usize = 100;

/// Building blocks (Source, Sink, Transformer, FallBack, Metrics, etc.) to build a Pipeline or a
/// MonoVertex.
pub(crate) mod components;
/// MonoVertex specific configs.
pub(crate) mod monovertex;
/// Pipeline specific configs.
pub(crate) mod pipeline;

pub(crate) const NUMAFLOW_MONO_VERTEX_NAME: &str = "NUMAFLOW_MONO_VERTEX_NAME";
const NUMAFLOW_VERTEX_NAME: &str = "NUMAFLOW_VERTEX_NAME";
const NUMAFLOW_REPLICA: &str = "NUMAFLOW_REPLICA";
const NUMAFLOW_PIPELINE_NAME: &str = "NUMAFLOW_PIPELINE_NAME";
const NUMAFLOW_NAMESPACE: &str = "NUMAFLOW_NAMESPACE";

static VERTEX_NAME: OnceLock<String> = OnceLock::new();

/// fetch the vertex name from the environment variable
pub(crate) fn get_vertex_name() -> &'static str {
    VERTEX_NAME.get_or_init(|| {
        env::var(NUMAFLOW_MONO_VERTEX_NAME)
            .or_else(|_| env::var(NUMAFLOW_VERTEX_NAME))
            .unwrap_or("default".to_string())
    })
}

static IS_MONO_VERTEX: OnceLock<bool> = OnceLock::new();

/// returns true if the vertex is a mono vertex
pub(crate) fn is_mono_vertex() -> bool {
    *IS_MONO_VERTEX.get_or_init(|| env::var(NUMAFLOW_MONO_VERTEX_NAME).is_ok())
}

static COMPONENT_TYPE: OnceLock<String> = OnceLock::new();

/// fetch the component type from the environment variable
pub(crate) fn get_component_type() -> &'static str {
    COMPONENT_TYPE.get_or_init(|| {
        if is_mono_vertex() {
            "mono-vertex".to_string()
        } else {
            "pipeline".to_string()
        }
    })
}

static PIPELINE_NAME: OnceLock<String> = OnceLock::new();

pub(crate) fn get_pipeline_name() -> &'static str {
    PIPELINE_NAME.get_or_init(|| env::var(NUMAFLOW_PIPELINE_NAME).unwrap_or("default".to_string()))
}

static VERTEX_REPLICA: OnceLock<u16> = OnceLock::new();

/// fetch the vertex replica information from the environment variable
pub(crate) fn get_vertex_replica() -> &'static u16 {
    VERTEX_REPLICA.get_or_init(|| {
        env::var(NUMAFLOW_REPLICA)
            .unwrap_or_default()
            .parse()
            .unwrap_or(0)
    })
}

static NAMESPACE: OnceLock<String> = OnceLock::new();

/// fetch the namespace from the environment variable
pub(crate) fn get_namespace() -> &'static str {
    NAMESPACE.get_or_init(|| env::var(NUMAFLOW_NAMESPACE).unwrap_or("default".to_string()))
}

/// CustomResources supported by Numaflow.
#[derive(Debug, Clone)]
pub(crate) enum CustomResourceType {
    MonoVertex(MonovertexConfig),
    Pipeline(PipelineConfig),
}

/// The CRD and other necessary setting to get the Numaflow pipeline/monovertex running.
#[derive(Debug, Clone)]
pub(crate) struct Settings {
    pub(crate) custom_resource_type: CustomResourceType,
}

impl Settings {
    /// load based on the CRD type, either a pipeline or a monovertex.
    /// Settings are populated through reading the env vars set via the controller. The main
    /// CRD is the base64 spec of the CR.
    pub(crate) fn load(env_vars: HashMap<String, String>) -> Result<Self> {
        if env_vars.contains_key(ENV_MONO_VERTEX_OBJ) {
            let cfg = MonovertexConfig::load(env_vars)?;
            return Ok(Settings {
                custom_resource_type: CustomResourceType::MonoVertex(cfg),
            });
        }

        if let Some(obj) = env_vars.get(ENV_VERTEX_OBJ) {
            let cfg = PipelineConfig::load(obj.clone(), env::vars())?;
            return Ok(Settings {
                custom_resource_type: CustomResourceType::Pipeline(cfg),
            });
        }
        Err(Error::Config("No configuration found - environment variable {ENV_MONO_VERTEX_OBJ} or {ENV_VERTEX_OBJ} is not set".to_string()))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use base64::Engine;
    use base64::prelude::BASE64_STANDARD;
    use serde_json::json;

    use crate::config::components::sink::OnFailureStrategy;
    use crate::config::{CustomResourceType, ENV_MONO_VERTEX_OBJ, Settings};

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
            let mut env_vars = HashMap::new();
            env_vars.insert(ENV_MONO_VERTEX_OBJ.to_string(), encoded_json);

            // Execute and verify
            let settings = Settings::load(env_vars).unwrap();
            assert!(matches!(
                settings.custom_resource_type,
                CustomResourceType::MonoVertex(_)
            ));
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
            let mut env_vars = HashMap::new();
            env_vars.insert(ENV_MONO_VERTEX_OBJ.to_string(), encoded_json);

            // Execute and verify
            let settings = Settings::load(env_vars).unwrap();

            let mvtx_cfg = match settings.custom_resource_type {
                CustomResourceType::MonoVertex(cfg) => cfg,
                CustomResourceType::Pipeline(_) => panic!("Invalid configuration type"),
            };

            assert_eq!(
                mvtx_cfg
                    .sink_config
                    .retry_config
                    .clone()
                    .unwrap()
                    .sink_max_retry_attempts,
                65535
            );
            assert_eq!(
                mvtx_cfg
                    .sink_config
                    .retry_config
                    .clone()
                    .unwrap()
                    .sink_initial_retry_interval_in_ms,
                1000
            );
            assert_eq!(
                mvtx_cfg
                    .sink_config
                    .retry_config
                    .clone()
                    .unwrap()
                    .sink_max_retry_interval_in_ms,
                4294967295
            );
            assert_eq!(
                mvtx_cfg
                    .sink_config
                    .retry_config
                    .clone()
                    .unwrap()
                    .sink_retry_factor,
                1.0
            );
            assert_eq!(
                mvtx_cfg.sink_config.retry_config.unwrap().sink_retry_jitter,
                0.0
            );
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
                                "steps": 5,
                                "cap": "2s",
                                "jitter": 0.1,
                                "factor": 1.5
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
            let mut env_vars = HashMap::new();
            env_vars.insert(ENV_MONO_VERTEX_OBJ.to_string(), encoded_json);

            // Execute and verify
            let settings = Settings::load(env_vars).unwrap();

            let mvtx_cfg = match settings.custom_resource_type {
                CustomResourceType::MonoVertex(cfg) => cfg,
                CustomResourceType::Pipeline(_) => panic!("Invalid configuration type"),
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
                    .sink_initial_retry_interval_in_ms,
                1000
            );
            assert_eq!(
                mvtx_cfg
                    .sink_config
                    .retry_config
                    .clone()
                    .unwrap()
                    .sink_max_retry_interval_in_ms,
                2000
            );
            assert_eq!(
                mvtx_cfg
                    .sink_config
                    .retry_config
                    .clone()
                    .unwrap()
                    .sink_retry_factor,
                1.5
            );
            assert_eq!(
                mvtx_cfg.sink_config.retry_config.unwrap().sink_retry_jitter,
                0.1
            );
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
                                "steps": 5,
                                "cap": "10s",
                                "jitter": 0.05,
                                "factor": 2.5
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
            let mut env_vars = HashMap::new();
            env_vars.insert(ENV_MONO_VERTEX_OBJ.to_string(), encoded_json);

            // Execute and verify
            let settings = Settings::load(env_vars).unwrap();
            let mvtx_config = match settings.custom_resource_type {
                CustomResourceType::MonoVertex(cfg) => cfg,
                CustomResourceType::Pipeline(_) => panic!("Invalid configuration type"),
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
                65535
            );
            assert_eq!(
                mvtx_config
                    .sink_config
                    .retry_config
                    .clone()
                    .unwrap()
                    .sink_initial_retry_interval_in_ms,
                1000
            );
            assert_eq!(
                mvtx_config
                    .sink_config
                    .retry_config
                    .clone()
                    .unwrap()
                    .sink_max_retry_interval_in_ms,
                10000
            );
            assert_eq!(
                mvtx_config
                    .sink_config
                    .retry_config
                    .clone()
                    .unwrap()
                    .sink_retry_factor,
                2.5
            );
            assert_eq!(
                mvtx_config
                    .sink_config
                    .retry_config
                    .unwrap()
                    .sink_retry_jitter,
                0.05
            );
        }
    }
}
