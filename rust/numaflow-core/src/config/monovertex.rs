use std::collections::HashMap;
use std::time::Duration;

use base64::Engine;
use base64::prelude::BASE64_STANDARD;
use numaflow_models::models::MonoVertex;
use serde_json::from_slice;

use super::pipeline::ServingCallbackConfig;
use super::{
    DEFAULT_CALLBACK_CONCURRENCY, ENV_CALLBACK_CONCURRENCY, ENV_CALLBACK_ENABLED,
    ENV_MONO_VERTEX_OBJ, get_namespace, get_pipeline_name,
};
use crate::Result;
use crate::config::components::metrics::MetricsConfig;
use crate::config::components::sink::SinkConfig;
use crate::config::components::source::{GeneratorConfig, SourceConfig};
use crate::config::components::transformer::{
    TransformerConfig, TransformerType, UserDefinedConfig,
};
use crate::config::components::{sink, source};
use crate::config::get_vertex_replica;
use crate::config::monovertex::sink::SinkType;
use crate::error::Error;

const DEFAULT_BATCH_SIZE: u64 = 500;
const DEFAULT_TIMEOUT_IN_MS: u32 = 1000;
const DEFAULT_LOOKBACK_WINDOW_IN_SECS: u16 = 120;

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct MonovertexConfig {
    pub(crate) name: String,
    pub(crate) batch_size: usize,
    pub(crate) read_timeout: Duration,
    pub(crate) replica: u16,
    pub(crate) source_config: SourceConfig,
    pub(crate) sink_config: SinkConfig,
    pub(crate) transformer_config: Option<TransformerConfig>,
    pub(crate) fb_sink_config: Option<SinkConfig>,
    pub(crate) metrics_config: MetricsConfig,
    pub(crate) callback_config: Option<ServingCallbackConfig>,
}

impl Default for MonovertexConfig {
    fn default() -> Self {
        MonovertexConfig {
            name: "".to_string(),
            batch_size: DEFAULT_BATCH_SIZE as usize,
            read_timeout: Duration::from_millis(DEFAULT_TIMEOUT_IN_MS as u64),
            replica: 0,
            source_config: SourceConfig {
                read_ahead: false,
                source_type: source::SourceType::Generator(GeneratorConfig::default()),
            },
            sink_config: SinkConfig {
                sink_type: SinkType::Log(sink::LogConfig::default()),
                retry_config: None,
            },
            transformer_config: None,
            fb_sink_config: None,
            metrics_config: MetricsConfig::default(),
            callback_config: None,
        }
    }
}

impl MonovertexConfig {
    /// Load the MonoVertex Settings.
    pub(crate) fn load(env_vars: HashMap<String, String>) -> Result<Self> {
        // controller sets this env var.
        let mono_vertex_spec = env_vars
            .get(ENV_MONO_VERTEX_OBJ)
            .ok_or_else(|| Error::Config(format!("{ENV_MONO_VERTEX_OBJ} is not set")))?;
        let decoded_spec = BASE64_STANDARD
            .decode(mono_vertex_spec.as_bytes())
            .map_err(|e| Error::Config(format!("Failed to decode mono vertex spec: {:?}", e)))?;

        let mono_vertex_obj: MonoVertex = from_slice(&decoded_spec)
            .map_err(|e| Error::Config(format!("Failed to parse mono vertex spec: {:?}", e)))?;

        let batch_size = mono_vertex_obj
            .spec
            .limits
            .as_ref()
            .and_then(|limits| limits.read_batch_size.map(|x| x as u64))
            .unwrap_or(DEFAULT_BATCH_SIZE);

        let timeout_in_ms = mono_vertex_obj
            .spec
            .limits
            .as_ref()
            .and_then(|limits| {
                limits
                    .read_timeout
                    .map(|x| Duration::from(x).as_millis() as u32)
            })
            .unwrap_or(DEFAULT_TIMEOUT_IN_MS);

        let mono_vertex_name = mono_vertex_obj
            .metadata
            .as_ref()
            .and_then(|metadata| metadata.name.clone())
            .ok_or_else(|| Error::Config("MonoVertex name not found".to_string()))?;

        let transformer_config = mono_vertex_obj
            .spec
            .source
            .as_ref()
            .and_then(|source| source.transformer.as_ref())
            .map(|_| TransformerConfig {
                concurrency: batch_size as usize, // FIXME: introduce a new config called udf concurrency in the spec
                transformer_type: TransformerType::UserDefined(UserDefinedConfig::default()),
            });

        let source = mono_vertex_obj
            .spec
            .source
            .clone()
            .ok_or_else(|| Error::Config("Source not found".to_string()))?;

        let source_config = SourceConfig {
            read_ahead: env_vars
                .get("READ_AHEAD")
                .map(|val| val.as_str())
                .unwrap_or("false")
                .parse()
                .unwrap(),
            source_type: source.try_into()?,
        };

        let sink = mono_vertex_obj
            .spec
            .sink
            .clone()
            .ok_or_else(|| Error::Config("Sink not found".to_string()))?;

        let sink_config = SinkConfig {
            sink_type: SinkType::primary_sinktype(&sink)?,
            retry_config: sink.retry_strategy.clone().map(|retry| retry.into()),
        };

        let fb_sink_config = if sink.fallback.is_some() {
            Some(SinkConfig {
                sink_type: SinkType::fallback_sinktype(&sink)?,
                retry_config: None,
            })
        } else {
            None
        };

        let look_back_window = mono_vertex_obj
            .spec
            .scale
            .as_ref()
            .and_then(|scale| scale.lookback_seconds.map(|x| x as u16))
            .unwrap_or(DEFAULT_LOOKBACK_WINDOW_IN_SECS);

        let mut callback_config = None;
        if env_vars.contains_key(ENV_CALLBACK_ENABLED) {
            let callback_concurrency: usize = env_vars
                .get(ENV_CALLBACK_CONCURRENCY)
                .cloned()
                .unwrap_or_else(|| DEFAULT_CALLBACK_CONCURRENCY.to_string())
                .parse()
                .map_err(|e| {
                    Error::Config(format!(
                        "Parsing value of {ENV_CALLBACK_CONCURRENCY}: {e:?}"
                    ))
                })?;
            callback_config = Some(ServingCallbackConfig {
                callback_store: Box::leak(Box::new(format!(
                    "{}-{}_SERVING_CALLBACK_STORE",
                    get_namespace(),
                    get_pipeline_name(),
                ))),
                callback_concurrency,
            });
        }

        Ok(MonovertexConfig {
            name: mono_vertex_name,
            replica: *get_vertex_replica(),
            batch_size: batch_size as usize,
            read_timeout: Duration::from_millis(timeout_in_ms as u64),
            metrics_config: MetricsConfig::with_lookback_window_in_secs(look_back_window),
            source_config,
            sink_config,
            transformer_config,
            fb_sink_config,
            callback_config,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use base64::Engine;
    use base64::prelude::BASE64_STANDARD;

    use crate::config::ENV_MONO_VERTEX_OBJ;
    use crate::config::components::sink::SinkType;
    use crate::config::components::source::SourceType;
    use crate::config::components::transformer::TransformerType;
    use crate::config::monovertex::MonovertexConfig;
    use crate::error::Error;

    #[test]
    fn test_load_valid_config() {
        let valid_config = r#"
        {
            "metadata": {
                "name": "test_vertex"
            },
            "spec": {
                "limits": {
                    "readBatchSize": 1000,
                    "readTimeout": "2s"
                },
                "source": {
                    "udsource": {
                        "container": {
                            "image": "xxxxxxx",
                            "resources": {}
                        }
                    }
                },
                "sink": {
                    "log": {}
                }
            }
        }
        "#;

        let encoded_valid_config = BASE64_STANDARD.encode(valid_config);

        let mut env_vars = HashMap::new();
        env_vars.insert(ENV_MONO_VERTEX_OBJ.to_string(), encoded_valid_config);

        let config = MonovertexConfig::load(env_vars).unwrap();

        assert_eq!(config.name, "test_vertex");
        assert_eq!(config.batch_size, 1000);
        assert_eq!(config.read_timeout.as_millis(), 2000);
        assert!(matches!(
            config.source_config.source_type,
            SourceType::UserDefined(_)
        ));
        assert!(matches!(config.sink_config.sink_type, SinkType::Log(_)));
    }

    #[test]
    fn test_load_missing_source() {
        let invalid_config = r#"
        {
            "metadata": {
                "name": "test_vertex"
            },
            "spec": {
                "limits": {
                    "readBatchSize": 1000,
                    "readTimeout": "2s"
                },
                "sink": {
                    "log": {}
                }
            }
        }
        "#;
        let encoded_invalid_config = BASE64_STANDARD.encode(invalid_config);

        let mut env_vars = HashMap::new();
        env_vars.insert(ENV_MONO_VERTEX_OBJ.to_string(), encoded_invalid_config);

        let result = MonovertexConfig::load(env_vars);
        assert!(matches!(result, Err(Error::Config(_))));
    }

    #[test]
    fn test_load_missing_sink() {
        let invalid_config = r#"
        {
            "metadata": {
                "name": "test_vertex"
            },
            "spec": {
                "limits": {
                    "readBatchSize": 1000,
                    "readTimeout": "2s"
                },
                "source": {
                    "udsource": {
                        "container": {
                            "image": "xxxxxxx",
                            "resources": {}
                        }
                    }
                }
            }
        }
        "#;
        let encoded_invalid_config = BASE64_STANDARD.encode(invalid_config);
        let mut env_vars = HashMap::new();
        env_vars.insert(ENV_MONO_VERTEX_OBJ.to_string(), encoded_invalid_config);

        let result = MonovertexConfig::load(env_vars);

        assert!(matches!(result, Err(Error::Config(_))));
    }

    #[test]
    fn test_load_with_transformer() {
        let valid_config = r#"
        {
            "metadata": {
                "name": "test_vertex"
            },
            "spec": {
                "limits": {
                    "readBatchSize": 1000,
                    "readTimeout": "2s"
                },
                "source": {
                    "udsource": {
                        "container": {
                            "image": "xxxxxxx",
                            "resources": {}
                        }
                    },
                    "transformer": {}
                },
                "sink": {
                    "log": {}
                }
            }
        }
        "#;

        let encoded_invalid_config = BASE64_STANDARD.encode(valid_config);
        let mut env_vars = HashMap::new();
        env_vars.insert(ENV_MONO_VERTEX_OBJ.to_string(), encoded_invalid_config);

        let config = MonovertexConfig::load(env_vars).unwrap();

        assert_eq!(config.name, "test_vertex");
        assert!(config.transformer_config.is_some());
        assert!(matches!(
            config.transformer_config.unwrap().transformer_type,
            TransformerType::UserDefined(_)
        ));
    }

    #[test]
    fn test_load_sink_and_fallback() {
        let valid_config = r#"
        {
            "metadata": {
                "name": "test_vertex"
            },
            "spec": {
                "limits": {
                    "readBatchSize": 1000,
                    "readTimeout": "2s"
                },
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
                            "image": "primary-sink",
                            "resources": {}
                        }
                    },
                    "fallback": {
                        "udsink": {
                            "container": {
                                "image": "fallback-sink",
                                "resources": {}
                            }
                        }
                    }
                }
            }
        }
        "#;
        let encoded_invalid_config = BASE64_STANDARD.encode(valid_config);
        let mut env_vars = HashMap::new();
        env_vars.insert(ENV_MONO_VERTEX_OBJ.to_string(), encoded_invalid_config);

        let config = MonovertexConfig::load(env_vars).unwrap();

        assert_eq!(config.name, "test_vertex");
        assert!(matches!(
            config.sink_config.sink_type,
            SinkType::UserDefined(_)
        ));
        assert!(config.fb_sink_config.is_some());
        assert!(matches!(
            config.fb_sink_config.clone().unwrap().sink_type,
            SinkType::UserDefined(_)
        ));

        if let SinkType::UserDefined(config) = config.sink_config.sink_type.clone() {
            assert_eq!(config.socket_path, "/var/run/numaflow/sink.sock");
            assert_eq!(
                config.server_info_path,
                "/var/run/numaflow/sinker-server-info"
            );
        }

        if let SinkType::UserDefined(config) = config.fb_sink_config.unwrap().sink_type {
            assert_eq!(config.socket_path, "/var/run/numaflow/fb-sink.sock");
            assert_eq!(
                config.server_info_path,
                "/var/run/numaflow/fb-sinker-server-info"
            );
        }
    }
}
