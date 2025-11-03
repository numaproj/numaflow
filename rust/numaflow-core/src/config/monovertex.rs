use std::collections::HashMap;
use std::time::Duration;

use super::pipeline::ServingCallbackConfig;
use super::{
    DEFAULT_CALLBACK_CONCURRENCY, ENV_CALLBACK_CONCURRENCY, ENV_CALLBACK_ENABLED,
    ENV_MONO_VERTEX_OBJ, get_namespace, get_pipeline_name,
};
use crate::Result;
use crate::config::components::metrics::MetricsConfig;
use crate::config::components::ratelimit::RateLimitConfig;
use crate::config::components::sink;
use crate::config::components::sink::SinkConfig;
use crate::config::components::source::{GeneratorConfig, SourceConfig, SourceSpec, SourceType};
use crate::config::components::transformer::{
    TransformerConfig, TransformerType, UserDefinedConfig,
};
use crate::config::get_vertex_replica;
use crate::config::monovertex::sink::SinkType;
use crate::config::pipeline::map::MapVtxConfig;
use crate::error::Error;
use base64::Engine;
use base64::prelude::BASE64_STANDARD;
use numaflow_models::models::MonoVertex;
use serde_json::from_slice;

const DEFAULT_BATCH_SIZE: u64 = 500;
const DEFAULT_TIMEOUT_IN_MS: u32 = 1000;
const DEFAULT_LOOKBACK_WINDOW_IN_SECS: u16 = 120;
const DEFAULT_GRACEFUL_SHUTDOWN_TIME_SECS: u64 = 20; // time we will wait for UDFs to finish before shutting down
const ENV_NUMAFLOW_GRACEFUL_TIMEOUT_SECS: &str = "NUMAFLOW_GRACEFUL_TIMEOUT_SECS";

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct MonovertexConfig {
    pub(crate) name: String,
    pub(crate) batch_size: usize,
    pub(crate) read_timeout: Duration,
    pub(crate) graceful_shutdown_time: Duration,
    pub(crate) replica: u16,
    pub(crate) source_config: SourceConfig,
    pub(crate) map_config: Option<MapVtxConfig>,
    pub(crate) sink_config: SinkConfig,
    pub(crate) transformer_config: Option<TransformerConfig>,
    pub(crate) fb_sink_config: Option<SinkConfig>,
    pub(crate) on_success_sink_config: Option<SinkConfig>,
    pub(crate) metrics_config: MetricsConfig,
    pub(crate) callback_config: Option<ServingCallbackConfig>,
    pub(crate) rate_limit: Option<RateLimitConfig>,
}

impl Default for MonovertexConfig {
    fn default() -> Self {
        MonovertexConfig {
            name: "".to_string(),
            batch_size: DEFAULT_BATCH_SIZE as usize,
            read_timeout: Duration::from_millis(DEFAULT_TIMEOUT_IN_MS as u64),
            graceful_shutdown_time: Duration::from_secs(DEFAULT_GRACEFUL_SHUTDOWN_TIME_SECS),
            replica: 0,
            source_config: SourceConfig {
                read_ahead: false,
                source_type: SourceType::Generator(GeneratorConfig::default()),
            },
            sink_config: SinkConfig {
                sink_type: SinkType::Log(sink::LogConfig::default()),
                retry_config: None,
            },
            map_config: None,
            transformer_config: None,
            fb_sink_config: None,
            on_success_sink_config: None,
            metrics_config: MetricsConfig::default(),
            callback_config: None,
            rate_limit: None,
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
            .map_err(|e| Error::Config(format!("Failed to decode mono vertex spec: {e:?}")))?;

        let mono_vertex_obj: MonoVertex = from_slice(&decoded_spec)
            .map_err(|e| Error::Config(format!("Failed to parse mono vertex spec: {e:?}")))?;

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

        let rate_limit: Option<RateLimitConfig> = mono_vertex_obj
            .spec
            .limits
            .as_ref()
            .and_then(|limits| limits.rate_limit.clone())
            .map(|rate_limit| RateLimitConfig::new(batch_size as usize, true, *rate_limit));

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

        let source = SourceSpec::new(mono_vertex_name.clone(), "mvtx".into(), source);
        let source_type: SourceType = source.try_into()?;

        let source_config = SourceConfig {
            read_ahead: env_vars
                .get("READ_AHEAD")
                .map(|val| val.as_str())
                .unwrap_or("false")
                .parse()
                .unwrap(),
            source_type,
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

        // Based on whether UDF config is present or not, obtain a Result<Box<Udf>>
        // Not checking whether this is Map or Reduce UDF, only considering Map UDF for now
        let udf = mono_vertex_obj
            .spec
            .udf
            .clone()
            .ok_or_else(|| Error::Config("Map UDF not found".to_string()));

        let map_config = match udf {
            Ok(udf) => Some(MapVtxConfig {
                concurrency: batch_size as usize,
                map_type: udf.try_into()?,
            }),
            Err(_) => None,
        };

        let fb_sink_config = if sink.fallback.is_some() {
            Some(SinkConfig {
                sink_type: SinkType::fallback_sinktype(&sink)?,
                retry_config: None,
            })
        } else {
            None
        };

        let on_success_sink_config = if sink.on_success.is_some() {
            Some(SinkConfig {
                sink_type: SinkType::on_success_sinktype(&sink)?,
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

        let graceful_shutdown_time_secs = env_vars
            .get(ENV_NUMAFLOW_GRACEFUL_TIMEOUT_SECS)
            .and_then(|s| s.parse().ok())
            .unwrap_or(DEFAULT_GRACEFUL_SHUTDOWN_TIME_SECS);

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
            graceful_shutdown_time: Duration::from_secs(graceful_shutdown_time_secs),
            metrics_config: MetricsConfig::with_lookback_window_in_secs(look_back_window),
            source_config,
            map_config,
            sink_config,
            transformer_config,
            fb_sink_config,
            on_success_sink_config,
            callback_config,
            rate_limit,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::time::{Duration, SystemTime};

    use base64::Engine;
    use base64::prelude::BASE64_STANDARD;

    use crate::config::ENV_MONO_VERTEX_OBJ;
    use crate::config::components::sink::SinkType;
    use crate::config::components::source::SourceType;
    use crate::config::components::transformer::TransformerType;
    use crate::config::monovertex::MonovertexConfig;
    use crate::error::Error;

    use numaflow_nats::jetstream::ConsumerDeliverPolicy;

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
    fn test_load_valid_sqs_sink() {
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
                    "sqs": {
                        "queueName": "https://sqs.us-east-1.amazonaws.com/123456789012/MyQueue",
                        "awsRegion": "us-east-1",
                        "queueOwnerAWSAccountID": "123456789012"
                    }
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
        assert!(matches!(config.sink_config.sink_type, SinkType::Sqs(_)));
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
                    },
                    "onSuccess": {
                        "udsink": {
                            "container": {
                                "image": "on-success-sink",
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
        assert!(config.on_success_sink_config.is_some());
        assert!(matches!(
            config.on_success_sink_config.clone().unwrap().sink_type,
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

        if let SinkType::UserDefined(config) = config.on_success_sink_config.unwrap().sink_type {
            assert_eq!(config.socket_path, "/var/run/numaflow/ons-sink.sock");
            assert_eq!(
                config.server_info_path,
                "/var/run/numaflow/ons-sinker-server-info"
            );
        }
    }

    #[test]
    fn test_load_jetstream_source() {
        let mvtx_config = r#"
        {
            "metadata": {
                "name": "simple-mono-vertex",
                "namespace": "default",
                "creationTimestamp": null
            },
            "spec": {
                "replicas": 0,
                "source": {
                "jetstream": {
                    "url": "jetstream-server.internal",
                    "stream": "mystream",
                    "consumer": "",
                    "deliver_policy": "by_start_time 1753428483000",
                    "filter_subjects": [ "abc.A.*", "abc.B.*"],
                    "tls": null
                }
                },
                "sink": {
                "log": {},
                "retryStrategy": {}
                },
                "limits": {
                "readBatchSize": 500,
                "readTimeout": "1s"
                },
                "scale": {
                "lookbackSeconds": 120
                },
                "updateStrategy": {},
                "lifecycle": {}
            },
            "status": {
                "replicas": 0,
                "desiredReplicas": 0,
                "lastUpdated": null,
                "lastScaledAt": null
            }
        }
        "#;

        let encoded_mvtx_config = BASE64_STANDARD.encode(mvtx_config);
        let env_vars = HashMap::from([
            (ENV_MONO_VERTEX_OBJ.to_string(), encoded_mvtx_config),
            (
                "NUMAFLOW_MONO_VERTEX_NAME".to_string(),
                "simple-mono-vertex".to_string(),
            ),
        ]);

        let config = MonovertexConfig::load(env_vars).unwrap();
        let expected_source_config = crate::config::components::source::SourceConfig {
            read_ahead: false,
            source_type: SourceType::Jetstream(numaflow_nats::jetstream::JetstreamSourceConfig {
                addr: "jetstream-server.internal".to_string(),
                stream: "mystream".to_string(),
                consumer: "numaflow-simple-mono-vertex-mvtx-mystream".to_string(),
                deliver_policy: ConsumerDeliverPolicy::by_start_time(
                    SystemTime::UNIX_EPOCH + Duration::from_millis(1753428483000),
                ),
                filter_subjects: vec!["abc.A.*".into(), "abc.B.*".into()],
                auth: None,
                tls: None,
            }),
        };

        assert_eq!(config.source_config, expected_source_config);
    }
}
