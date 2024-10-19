use crate::config::components::metrics::MetricsConfig;
use crate::config::components::sink::{OnFailureStrategy, RetryConfig, SinkConfig};
use crate::config::components::source::{GeneratorConfig, SourceConfig};
use crate::config::components::transformer::{
    TransformerConfig, TransformerType, UserDefinedConfig,
};
use crate::config::components::{sink, source};
use crate::error::Error;
use crate::message::get_vertex_replica;
use crate::Result;
use base64::prelude::BASE64_STANDARD;
use base64::Engine;
use bytes::Bytes;
use numaflow_models::models::MonoVertex;
use serde_json::from_slice;
use std::time::Duration;
use tracing::warn;

const DEFAULT_BATCH_SIZE: u64 = 500;
const DEFAULT_TIMEOUT_IN_MS: u32 = 1000;

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct MonovertexConfig {
    pub(crate) name: String,
    pub(crate) batch_size: usize,
    pub(crate) timeout_in_ms: u64,
    pub(crate) replica: u16,
    pub(crate) source_config: SourceConfig,
    pub(crate) sink_config: SinkConfig,
    pub(crate) transformer_config: Option<TransformerConfig>,
    pub(crate) fb_sink_config: Option<SinkConfig>,
    pub(crate) metrics_config: MetricsConfig,
}

impl Default for MonovertexConfig {
    fn default() -> Self {
        MonovertexConfig {
            name: "".to_string(),
            batch_size: DEFAULT_BATCH_SIZE as usize,
            timeout_in_ms: DEFAULT_TIMEOUT_IN_MS as u64,
            replica: 0,
            source_config: SourceConfig {
                source_type: source::SourceType::Generator(GeneratorConfig::default()),
            },
            sink_config: SinkConfig {
                sink_type: sink::SinkType::Log(sink::LogConfig::default()),
                retry_config: None,
            },
            transformer_config: None,
            fb_sink_config: None,
            metrics_config: MetricsConfig::default(),
        }
    }
}

impl MonovertexConfig {
    /// Load the MonoVertex Settings.
    pub(crate) fn load(mono_vertex_spec: String) -> Result<Self> {
        // controller sets this env var.
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
            .ok_or_else(|| Error::Config("Mono vertex name not found".to_string()))?;

        let transformer_config = mono_vertex_obj
            .spec
            .source
            .as_ref()
            .and_then(|source| source.transformer.as_ref())
            .map(|_| TransformerConfig {
                transformer_type: TransformerType::UserDefined(UserDefinedConfig::default()),
            });

        let source_config = mono_vertex_obj
                .spec
                .source
                .as_ref()
                .ok_or_else(|| Error::Config("Source not found".to_string()))
                .and_then(|source| {
                    source.udsource.as_ref().map(|_| SourceConfig {
                        source_type: source::SourceType::UserDefined(source::UserDefinedConfig::default()),
                    }).or_else(|| {
                        source.generator.as_ref().map(|generator| {
                            let mut generator_config = GeneratorConfig::default();

                            if let Some(value_blob) = &generator.value_blob {
                                generator_config.content = Bytes::from(value_blob.clone());
                            }

                            if let Some(msg_size) = generator.msg_size {
                                if msg_size >= 0 {
                                    generator_config.msg_size_bytes = msg_size as u32;
                                } else {
                                    warn!("'msgSize' cannot be negative, using default value (8 bytes)");
                                }
                            }

                            generator_config.value = generator.value;
                            generator_config.rpu = generator.rpu.unwrap_or(1) as usize;
                            generator_config.duration = generator.duration.map_or(1000, |d| std::time::Duration::from(d).as_millis() as usize);
                            generator_config.key_count = generator.key_count.map_or(0, |kc| std::cmp::min(kc, u8::MAX as i32) as u8);
                            generator_config.jitter = generator.jitter.map_or(Duration::from_secs(0), std::time::Duration::from);

                            SourceConfig {
                                source_type: source::SourceType::Generator(generator_config),
                            }
                        })
                    }).ok_or_else(|| Error::Config("Source type not found".to_string()))
                })?;

        let sink_config = mono_vertex_obj
            .spec
            .sink
            .as_ref()
            .ok_or_else(|| Error::Config("Sink not found".to_string()))
            .and_then(|sink| {
                let retry_config = sink.retry_strategy.as_ref().map(|retry| {
                    let mut retry_config = RetryConfig::default();

                    if let Some(backoff) = &retry.backoff {
                        if let Some(interval) = backoff.interval {
                            retry_config.sink_retry_interval_in_ms =
                                std::time::Duration::from(interval).as_millis() as u32;
                        }

                        if let Some(steps) = backoff.steps {
                            retry_config.sink_max_retry_attempts = steps as u16;
                        }
                    }

                    if let Some(strategy) = &retry.on_failure {
                        retry_config.sink_retry_on_fail_strategy =
                            OnFailureStrategy::from_str(strategy);
                    }

                    retry_config
                });

                sink.udsink
                    .as_ref()
                    .map(|_| SinkConfig {
                        sink_type: sink::SinkType::UserDefined(sink::UserDefinedConfig::default()),
                        retry_config: retry_config.clone(),
                    })
                    .or_else(|| {
                        sink.log.as_ref().map(|_| SinkConfig {
                            sink_type: sink::SinkType::Log(sink::LogConfig::default()),
                            retry_config: retry_config.clone(),
                        })
                    })
                    .or_else(|| {
                        sink.blackhole.as_ref().map(|_| SinkConfig {
                            sink_type: sink::SinkType::Blackhole(sink::BlackholeConfig::default()),
                            retry_config: retry_config.clone(),
                        })
                    })
                    .ok_or_else(|| Error::Config("Sink type not found".to_string()))
            })?;

        let fb_sink_config = mono_vertex_obj
            .spec
            .sink
            .as_ref()
            .and_then(|sink| sink.fallback.as_ref())
            .map(|fallback| {
                fallback
                    .udsink
                    .as_ref()
                    .map(|_| SinkConfig {
                        sink_type: sink::SinkType::UserDefined(
                            sink::UserDefinedConfig::fallback_default(),
                        ),
                        retry_config: None,
                    })
                    .or_else(|| {
                        fallback.log.as_ref().map(|_| SinkConfig {
                            sink_type: sink::SinkType::Log(sink::LogConfig::default()),
                            retry_config: None,
                        })
                    })
                    .or_else(|| {
                        fallback.blackhole.as_ref().map(|_| SinkConfig {
                            sink_type: sink::SinkType::Blackhole(sink::BlackholeConfig::default()),
                            retry_config: None,
                        })
                    })
                    .ok_or_else(|| Error::Config("Fallback sink type not found".to_string()))
            })
            .transpose()?;

        Ok(MonovertexConfig {
            name: mono_vertex_name,
            replica: *get_vertex_replica(),
            batch_size: batch_size as usize,
            timeout_in_ms: timeout_in_ms as u64,
            metrics_config: MetricsConfig::default(),
            source_config,
            sink_config,
            transformer_config,
            fb_sink_config,
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::config::components::sink::SinkType;
    use crate::config::components::source::SourceType;
    use crate::config::components::transformer::TransformerType;
    use crate::config::monovertex::MonovertexConfig;
    use crate::error::Error;
    use base64::prelude::BASE64_STANDARD;
    use base64::Engine;
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
        let spec = encoded_valid_config.as_str();

        let config = MonovertexConfig::load(spec.to_string()).unwrap();

        assert_eq!(config.name, "test_vertex");
        assert_eq!(config.batch_size, 1000);
        assert_eq!(config.timeout_in_ms, 2000);
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
        let spec = encoded_invalid_config.as_str();

        let result = MonovertexConfig::load(spec.to_string());
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
        let spec = encoded_invalid_config.as_str();

        let result = MonovertexConfig::load(spec.to_string());
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
        let spec = encoded_invalid_config.as_str();

        let config = MonovertexConfig::load(spec.to_string()).unwrap();

        assert_eq!(config.name, "test_vertex");
        assert!(config.transformer_config.is_some());
        assert!(matches!(
            config.transformer_config.unwrap().transformer_type,
            TransformerType::UserDefined(_)
        ));
    }
}
