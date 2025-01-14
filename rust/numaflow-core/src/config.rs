use std::env;
use std::sync::OnceLock;

use monovertex::MonovertexConfig;

use crate::config::pipeline::PipelineConfig;
use crate::Error;
use crate::Result;

const ENV_MONO_VERTEX_OBJ: &str = "NUMAFLOW_MONO_VERTEX_OBJECT";
const ENV_VERTEX_OBJ: &str = "NUMAFLOW_VERTEX_OBJECT";

/// Building blocks (Source, Sink, Transformer, FallBack, Metrics, etc.) to build a Pipeline or a
/// MonoVertex.
pub(crate) mod components;
/// MonoVertex specific configs.
pub(crate) mod monovertex;
/// Pipeline specific configs.
pub(crate) mod pipeline;

pub const NUMAFLOW_MONO_VERTEX_NAME: &str = "NUMAFLOW_MONO_VERTEX_NAME";
const NUMAFLOW_VERTEX_NAME: &str = "NUMAFLOW_VERTEX_NAME";
const NUMAFLOW_REPLICA: &str = "NUMAFLOW_REPLICA";
static VERTEX_NAME: OnceLock<String> = OnceLock::new();

/// fetch the vertex name from the environment variable
pub(crate) fn get_vertex_name() -> &'static str {
    VERTEX_NAME.get_or_init(|| {
        env::var(NUMAFLOW_MONO_VERTEX_NAME)
            .or_else(|_| env::var(NUMAFLOW_VERTEX_NAME))
            .unwrap_or_default()
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
    PIPELINE_NAME.get_or_init(|| env::var("NUMAFLOW_PIPELINE_NAME").unwrap_or_default())
}

static VERTEX_REPLICA: OnceLock<u16> = OnceLock::new();

/// fetch the vertex replica information from the environment variable
pub(crate) fn get_vertex_replica() -> &'static u16 {
    VERTEX_REPLICA.get_or_init(|| {
        env::var(NUMAFLOW_REPLICA)
            .unwrap_or_default()
            .parse()
            .unwrap_or_default()
    })
}

/// Exposes the [Settings] via lazy loading.
pub fn config() -> &'static Settings {
    static CONF: OnceLock<Settings> = OnceLock::new();
    CONF.get_or_init(|| match Settings::load() {
        Ok(v) => v,
        Err(e) => {
            panic!("Failed to load configuration: {:?}", e);
        }
    })
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
    fn load() -> Result<Self> {
        if let Ok(obj) = env::var(ENV_MONO_VERTEX_OBJ) {
            let cfg = MonovertexConfig::load(obj)?;
            return Ok(Settings {
                custom_resource_type: CustomResourceType::MonoVertex(cfg),
            });
        }

        if let Ok(obj) = env::var(ENV_VERTEX_OBJ) {
            let cfg = PipelineConfig::load(obj, env::vars())?;
            return Ok(Settings {
                custom_resource_type: CustomResourceType::Pipeline(cfg),
            });
        }
        Err(Error::Config("No configuration found".to_string()))
    }
}

#[cfg(test)]
mod tests {
    use std::env;

    use base64::prelude::BASE64_STANDARD;
    use base64::Engine;
    use serde_json::json;

    use crate::config::components::sink::OnFailureStrategy;
    use crate::config::{CustomResourceType, Settings, ENV_MONO_VERTEX_OBJ};

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
