use std::collections::HashMap;
use std::env;
use std::time::Duration;

use base64::prelude::BASE64_STANDARD;
use base64::Engine;
use numaflow_models::models::{ForwardConditions, Vertex};
use serde_json::from_slice;

use crate::config::components::metrics::MetricsConfig;
use crate::config::components::sink::{SinkConfig, SinkType};
use crate::config::components::source::SourceConfig;
use crate::config::components::transformer::{TransformerConfig, TransformerType};
use crate::config::pipeline::isb::{BufferReaderConfig, BufferWriterConfig};
use crate::error::Error;
use crate::message::get_vertex_replica;
use crate::Result;

const DEFAULT_BATCH_SIZE: u64 = 500;
const DEFAULT_TIMEOUT_IN_MS: u32 = 1000;
const DEFAULT_LOOKBACK_WINDOW_IN_SECS: u16 = 120;
const ENV_NUMAFLOW_SERVING_JETSTREAM_URL: &str = "NUMAFLOW_ISBSVC_JETSTREAM_URL";
const ENV_NUMAFLOW_SERVING_JETSTREAM_USER: &str = "NUMAFLOW_ISBSVC_JETSTREAM_USER";
const ENV_NUMAFLOW_SERVING_JETSTREAM_PASSWORD: &str = "NUMAFLOW_ISBSVC_JETSTREAM_PASSWORD";

pub(crate) mod isb;

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct PipelineConfig {
    pub(crate) pipeline_name: String,
    pub(crate) vertex_name: String,
    pub(crate) replica: u16,
    pub(crate) batch_size: usize,
    // FIXME(cr): we cannot leak this as a paf, we need to use a different terminology.
    pub(crate) paf_batch_size: usize,
    pub(crate) read_timeout: Duration,
    pub(crate) js_client_config: isb::jetstream::ClientConfig, // TODO: make it enum, since we can have different ISB implementations
    pub(crate) from_vertex_config: Vec<FromVertexConfig>,
    pub(crate) to_vertex_config: Vec<ToVertexConfig>,
    pub(crate) vertex_config: VertexType,
    pub(crate) metrics_config: MetricsConfig,
}

impl Default for PipelineConfig {
    fn default() -> Self {
        PipelineConfig {
            pipeline_name: "default-pl".to_string(),
            vertex_name: "default-vtx".to_string(),
            replica: 0,
            batch_size: DEFAULT_BATCH_SIZE as usize,
            paf_batch_size: (DEFAULT_BATCH_SIZE * 2) as usize,
            read_timeout: Duration::from_secs(DEFAULT_TIMEOUT_IN_MS as u64),
            js_client_config: isb::jetstream::ClientConfig::default(),
            from_vertex_config: vec![],
            to_vertex_config: vec![],
            vertex_config: VertexType::Source(SourceVtxConfig {
                source_config: Default::default(),
                transformer_config: None,
            }),
            metrics_config: Default::default(),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct SourceVtxConfig {
    pub(crate) source_config: SourceConfig,
    pub(crate) transformer_config: Option<TransformerConfig>,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct SinkVtxConfig {
    pub(crate) sink_config: SinkConfig,
    pub(crate) fb_sink_config: Option<SinkConfig>,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum VertexType {
    Source(SourceVtxConfig),
    Sink(SinkVtxConfig),
}

impl std::fmt::Display for VertexType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::result::Result<(), std::fmt::Error> {
        match self {
            VertexType::Source(_) => write!(f, "Source"),
            VertexType::Sink(_) => write!(f, "Sink"),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct FromVertexConfig {
    pub(crate) name: String,
    pub(crate) reader_config: BufferReaderConfig,
    pub(crate) partitions: u16,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ToVertexConfig {
    pub(crate) name: String,
    pub(crate) writer_config: BufferWriterConfig,
    pub(crate) partitions: u16,
    pub(crate) conditions: Option<ForwardConditions>,
}

impl PipelineConfig {
    pub(crate) fn load(
        pipeline_spec_obj: String,
        env_vars: impl IntoIterator<Item = (impl Into<String>, impl Into<String>)>,
    ) -> Result<Self> {
        // controller sets this env var.
        let decoded_spec = BASE64_STANDARD
            .decode(pipeline_spec_obj.as_bytes())
            .map_err(|e| Error::Config(format!("Failed to decode pipeline spec: {:?}", e)))?;

        let vertex_obj: Vertex = from_slice(&decoded_spec)
            .map_err(|e| Error::Config(format!("Failed to parse pipeline spec: {:?}", e)))?;

        let pipeline_name = vertex_obj.spec.pipeline_name;
        let vertex_name = vertex_obj.spec.name;
        let replica = get_vertex_replica();

        let namespace = vertex_obj
            .metadata
            .ok_or_else(|| Error::Config("Missing metadata in vertex spec".to_string()))?
            .namespace
            .ok_or_else(|| Error::Config("Missing namespace in vertex spec".to_string()))?;

        let batch_size = vertex_obj
            .spec
            .limits
            .as_ref()
            .and_then(|limits| limits.read_batch_size.map(|x| x as u64))
            .unwrap_or(DEFAULT_BATCH_SIZE);

        let timeout_in_ms = vertex_obj
            .spec
            .limits
            .as_ref()
            .and_then(|limits| {
                limits
                    .read_timeout
                    .map(|x| Duration::from(x).as_millis() as u32)
            })
            .unwrap_or(DEFAULT_TIMEOUT_IN_MS);

        let from_edges = vertex_obj.spec.from_edges.unwrap_or_default();

        let to_edges = vertex_obj.spec.to_edges.unwrap_or_default();

        let vertex: VertexType = if let Some(source) = vertex_obj.spec.source {
            let transformer_config = source.transformer.as_ref().map(|_| TransformerConfig {
                transformer_type: TransformerType::UserDefined(Default::default()),
            });

            VertexType::Source(SourceVtxConfig {
                source_config: SourceConfig {
                    source_type: source.try_into()?,
                },
                transformer_config,
            })
        } else if let Some(sink) = vertex_obj.spec.sink {
            let fb_sink_config = if sink.fallback.as_ref().is_some() {
                Some(SinkConfig {
                    sink_type: SinkType::fallback_sinktype(sink.clone())?,
                    retry_config: None,
                })
            } else {
                None
            };

            VertexType::Sink(SinkVtxConfig {
                sink_config: SinkConfig {
                    sink_type: SinkType::primary_sinktype(sink)?,
                    retry_config: None,
                },
                fb_sink_config,
            })
        } else {
            return Err(Error::Config(
                "Only source and sink are supported ATM".to_string(),
            ));
        };

        let env_vars: HashMap<String, String> = env_vars
            .into_iter()
            .map(|(key, val)| (key.into(), val.into()))
            .filter(|(key, _val)| {
                // FIXME(cr): this filter is non-exhaustive, should we invert?
                key == ENV_NUMAFLOW_SERVING_JETSTREAM_URL
                    || key == ENV_NUMAFLOW_SERVING_JETSTREAM_USER
                    || key == ENV_NUMAFLOW_SERVING_JETSTREAM_PASSWORD
            })
            .collect();

        let get_var = |var: &str| -> Result<String> {
            Ok(env_vars
                .get(var)
                .ok_or_else(|| Error::Config(format!("Environment variable {var} is not set")))?
                .to_string())
        };

        let js_client_config = isb::jetstream::ClientConfig {
            url: get_var(ENV_NUMAFLOW_SERVING_JETSTREAM_URL)?,
            user: get_var(ENV_NUMAFLOW_SERVING_JETSTREAM_USER).ok(),
            password: get_var(ENV_NUMAFLOW_SERVING_JETSTREAM_PASSWORD).ok(),
        };

        let mut from_vertex_config = vec![];
        for edge in from_edges {
            let partition_count = edge.to_vertex_partition_count.unwrap_or_default() as u16;
            let buffer_name = format!("{}-{}-{}", namespace, pipeline_name, edge.to);

            let streams: Vec<(String, u16)> = (0..partition_count)
                .map(|i| (format!("{}-{}", buffer_name, i), i))
                .collect();

            from_vertex_config.push(FromVertexConfig {
                name: edge.from,
                reader_config: BufferReaderConfig {
                    partitions: partition_count,
                    streams,
                    ..Default::default()
                },
                partitions: 0,
            });
        }

        let mut to_vertex_config = vec![];
        for edge in to_edges {
            let partition_count = edge.to_vertex_partition_count.unwrap_or_default() as u16;
            let buffer_name = format!("{}-{}-{}", namespace, pipeline_name, edge.to);

            let streams: Vec<(String, u16)> = (0..partition_count)
                .map(|i| (format!("{}-{}", buffer_name, i), i))
                .collect();

            let default_writer_config = BufferWriterConfig::default();
            to_vertex_config.push(ToVertexConfig {
                name: edge.to,
                writer_config: BufferWriterConfig {
                    streams,
                    partitions: partition_count,
                    max_length: vertex_obj
                        .spec
                        .limits
                        .as_ref()
                        .and_then(|l| l.buffer_max_length)
                        .unwrap_or(default_writer_config.max_length as i64)
                        as usize,
                    usage_limit: vertex_obj
                        .spec
                        .limits
                        .as_ref()
                        .and_then(|l| l.buffer_usage_limit)
                        .unwrap_or(default_writer_config.usage_limit as i64)
                        as f64
                        / 100.0,
                    ..default_writer_config
                },
                partitions: edge.to_vertex_partition_count.unwrap_or_default() as u16,
                conditions: None,
            });
        }

        let look_back_window = vertex_obj
            .spec
            .scale
            .as_ref()
            .and_then(|scale| scale.lookback_seconds.map(|x| x as u16))
            .unwrap_or(DEFAULT_LOOKBACK_WINDOW_IN_SECS);

        Ok(PipelineConfig {
            batch_size: batch_size as usize,
            paf_batch_size: env::var("PAF_BATCH_SIZE")
                .unwrap_or("30000".to_string())
                .parse()
                .unwrap(),
            read_timeout: Duration::from_millis(timeout_in_ms as u64),
            pipeline_name,
            vertex_name,
            replica: *replica,
            js_client_config,
            from_vertex_config,
            to_vertex_config,
            vertex_config: vertex,
            metrics_config: MetricsConfig::with_lookback_window_in_secs(look_back_window),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::components::sink::{BlackholeConfig, LogConfig, SinkType};
    use crate::config::components::source::{GeneratorConfig, SourceType};

    #[test]
    fn test_default_pipeline_config() {
        let expected = PipelineConfig {
            pipeline_name: "default-pl".to_string(),
            vertex_name: "default-vtx".to_string(),
            replica: 0,
            batch_size: DEFAULT_BATCH_SIZE as usize,
            paf_batch_size: (DEFAULT_BATCH_SIZE * 2) as usize,
            read_timeout: Duration::from_secs(DEFAULT_TIMEOUT_IN_MS as u64),
            js_client_config: isb::jetstream::ClientConfig::default(),
            from_vertex_config: vec![],
            to_vertex_config: vec![],
            vertex_config: VertexType::Source(SourceVtxConfig {
                source_config: Default::default(),
                transformer_config: None,
            }),
            metrics_config: Default::default(),
        };

        let config = PipelineConfig::default();
        assert_eq!(config, expected);
    }

    #[test]
    fn test_vertex_type_display() {
        let src_type = VertexType::Source(SourceVtxConfig {
            source_config: SourceConfig::default(),
            transformer_config: None,
        });
        assert_eq!(src_type.to_string(), "Source");

        let sink_type = VertexType::Sink(SinkVtxConfig {
            sink_config: SinkConfig {
                sink_type: SinkType::Log(LogConfig {}),
                retry_config: None,
            },
            fb_sink_config: None,
        });
        assert_eq!(sink_type.to_string(), "Sink");
    }

    #[test]
    fn test_pipeline_config_load_sink_vertex() {
        let pipeline_cfg_base64 = "eyJtZXRhZGF0YSI6eyJuYW1lIjoic2ltcGxlLXBpcGVsaW5lLW91dCIsIm5hbWVzcGFjZSI6ImRlZmF1bHQiLCJjcmVhdGlvblRpbWVzdGFtcCI6bnVsbH0sInNwZWMiOnsibmFtZSI6Im91dCIsInNpbmsiOnsiYmxhY2tob2xlIjp7fSwicmV0cnlTdHJhdGVneSI6eyJvbkZhaWx1cmUiOiJyZXRyeSJ9fSwibGltaXRzIjp7InJlYWRCYXRjaFNpemUiOjUwMCwicmVhZFRpbWVvdXQiOiIxcyIsImJ1ZmZlck1heExlbmd0aCI6MzAwMDAsImJ1ZmZlclVzYWdlTGltaXQiOjgwfSwic2NhbGUiOnsibWluIjoxfSwidXBkYXRlU3RyYXRlZ3kiOnsidHlwZSI6IlJvbGxpbmdVcGRhdGUiLCJyb2xsaW5nVXBkYXRlIjp7Im1heFVuYXZhaWxhYmxlIjoiMjUlIn19LCJwaXBlbGluZU5hbWUiOiJzaW1wbGUtcGlwZWxpbmUiLCJpbnRlclN0ZXBCdWZmZXJTZXJ2aWNlTmFtZSI6IiIsInJlcGxpY2FzIjowLCJmcm9tRWRnZXMiOlt7ImZyb20iOiJpbiIsInRvIjoib3V0IiwiY29uZGl0aW9ucyI6bnVsbCwiZnJvbVZlcnRleFR5cGUiOiJTb3VyY2UiLCJmcm9tVmVydGV4UGFydGl0aW9uQ291bnQiOjEsImZyb21WZXJ0ZXhMaW1pdHMiOnsicmVhZEJhdGNoU2l6ZSI6NTAwLCJyZWFkVGltZW91dCI6IjFzIiwiYnVmZmVyTWF4TGVuZ3RoIjozMDAwMCwiYnVmZmVyVXNhZ2VMaW1pdCI6ODB9LCJ0b1ZlcnRleFR5cGUiOiJTaW5rIiwidG9WZXJ0ZXhQYXJ0aXRpb25Db3VudCI6MSwidG9WZXJ0ZXhMaW1pdHMiOnsicmVhZEJhdGNoU2l6ZSI6NTAwLCJyZWFkVGltZW91dCI6IjFzIiwiYnVmZmVyTWF4TGVuZ3RoIjozMDAwMCwiYnVmZmVyVXNhZ2VMaW1pdCI6ODB9fV0sIndhdGVybWFyayI6eyJtYXhEZWxheSI6IjBzIn19LCJzdGF0dXMiOnsicGhhc2UiOiIiLCJyZXBsaWNhcyI6MCwiZGVzaXJlZFJlcGxpY2FzIjowLCJsYXN0U2NhbGVkQXQiOm51bGx9fQ==".to_string();

        let env_vars = [("NUMAFLOW_ISBSVC_JETSTREAM_URL", "localhost:4222")];
        let pipeline_config = PipelineConfig::load(pipeline_cfg_base64, env_vars).unwrap();

        let expected = PipelineConfig {
            pipeline_name: "simple-pipeline".to_string(),
            vertex_name: "out".to_string(),
            replica: 0,
            batch_size: 500,
            paf_batch_size: 30000,
            read_timeout: Duration::from_secs(1),
            js_client_config: isb::jetstream::ClientConfig {
                url: "localhost:4222".to_string(),
                user: None,
                password: None,
            },
            from_vertex_config: vec![FromVertexConfig {
                name: "in".to_string(),
                reader_config: BufferReaderConfig {
                    partitions: 1,
                    streams: vec![("default-simple-pipeline-out-0".into(), 0)],
                    wip_ack_interval: Duration::from_secs(1),
                },
                partitions: 0,
            }],
            to_vertex_config: vec![],
            vertex_config: VertexType::Sink(SinkVtxConfig {
                sink_config: SinkConfig {
                    sink_type: SinkType::Blackhole(BlackholeConfig {}),
                    retry_config: None,
                },
                fb_sink_config: None,
            }),
            metrics_config: MetricsConfig {
                metrics_server_listen_port: 2469,
                lag_check_interval_in_secs: 5,
                lag_refresh_interval_in_secs: 3,
                lookback_window_in_secs: 120,
            },
        };
        assert_eq!(pipeline_config, expected);
    }

    #[test]
    fn test_pipeline_config_load_all() {
        let pipeline_cfg_base64 = "eyJtZXRhZGF0YSI6eyJuYW1lIjoic2ltcGxlLXBpcGVsaW5lLWluIiwibmFtZXNwYWNlIjoiZGVmYXVsdCIsImNyZWF0aW9uVGltZXN0YW1wIjpudWxsfSwic3BlYyI6eyJuYW1lIjoiaW4iLCJzb3VyY2UiOnsiZ2VuZXJhdG9yIjp7InJwdSI6MTAwMDAwLCJkdXJhdGlvbiI6IjFzIiwibXNnU2l6ZSI6OCwiaml0dGVyIjoiMHMifX0sImNvbnRhaW5lclRlbXBsYXRlIjp7InJlc291cmNlcyI6e30sImVudiI6W3sibmFtZSI6IlBBRl9CQVRDSF9TSVpFIiwidmFsdWUiOiIxMDAwMDAifV19LCJsaW1pdHMiOnsicmVhZEJhdGNoU2l6ZSI6MTAwMCwicmVhZFRpbWVvdXQiOiIxcyIsImJ1ZmZlck1heExlbmd0aCI6MTUwMDAwLCJidWZmZXJVc2FnZUxpbWl0Ijo4NX0sInNjYWxlIjp7Im1pbiI6MX0sInVwZGF0ZVN0cmF0ZWd5Ijp7InR5cGUiOiJSb2xsaW5nVXBkYXRlIiwicm9sbGluZ1VwZGF0ZSI6eyJtYXhVbmF2YWlsYWJsZSI6IjI1JSJ9fSwicGlwZWxpbmVOYW1lIjoic2ltcGxlLXBpcGVsaW5lIiwiaW50ZXJTdGVwQnVmZmVyU2VydmljZU5hbWUiOiIiLCJyZXBsaWNhcyI6MCwidG9FZGdlcyI6W3siZnJvbSI6ImluIiwidG8iOiJvdXQiLCJjb25kaXRpb25zIjpudWxsLCJmcm9tVmVydGV4VHlwZSI6IlNvdXJjZSIsImZyb21WZXJ0ZXhQYXJ0aXRpb25Db3VudCI6MSwiZnJvbVZlcnRleExpbWl0cyI6eyJyZWFkQmF0Y2hTaXplIjoxMDAwLCJyZWFkVGltZW91dCI6IjFzIiwiYnVmZmVyTWF4TGVuZ3RoIjoxNTAwMDAsImJ1ZmZlclVzYWdlTGltaXQiOjg1fSwidG9WZXJ0ZXhUeXBlIjoiU2luayIsInRvVmVydGV4UGFydGl0aW9uQ291bnQiOjEsInRvVmVydGV4TGltaXRzIjp7InJlYWRCYXRjaFNpemUiOjEwMDAsInJlYWRUaW1lb3V0IjoiMXMiLCJidWZmZXJNYXhMZW5ndGgiOjE1MDAwMCwiYnVmZmVyVXNhZ2VMaW1pdCI6ODV9fV0sIndhdGVybWFyayI6eyJkaXNhYmxlZCI6dHJ1ZSwibWF4RGVsYXkiOiIwcyJ9fSwic3RhdHVzIjp7InBoYXNlIjoiIiwicmVwbGljYXMiOjAsImRlc2lyZWRSZXBsaWNhcyI6MCwibGFzdFNjYWxlZEF0IjpudWxsfX0=";

        let env_vars = [("NUMAFLOW_ISBSVC_JETSTREAM_URL", "localhost:4222")];
        let pipeline_config =
            PipelineConfig::load(pipeline_cfg_base64.to_string(), env_vars).unwrap();

        let expected = PipelineConfig {
            pipeline_name: "simple-pipeline".to_string(),
            vertex_name: "in".to_string(),
            replica: 0,
            batch_size: 1000,
            paf_batch_size: 30000,
            read_timeout: Duration::from_secs(1),
            js_client_config: isb::jetstream::ClientConfig {
                url: "localhost:4222".to_string(),
                user: None,
                password: None,
            },
            from_vertex_config: vec![],
            to_vertex_config: vec![ToVertexConfig {
                name: "out".to_string(),
                writer_config: BufferWriterConfig {
                    streams: vec![("default-simple-pipeline-out-0".to_string(), 0)],
                    partitions: 1,
                    max_length: 150000,
                    usage_limit: 0.85,
                    ..Default::default()
                },
                partitions: 1,
                conditions: None,
            }],
            vertex_config: VertexType::Source(SourceVtxConfig {
                source_config: SourceConfig {
                    source_type: SourceType::Generator(GeneratorConfig {
                        rpu: 100000,
                        content: Default::default(),
                        duration: Duration::from_millis(1000),
                        value: None,
                        key_count: 0,
                        msg_size_bytes: 8,
                        jitter: Duration::from_secs(0),
                    }),
                },
                transformer_config: None,
            }),
            metrics_config: Default::default(),
        };

        assert_eq!(pipeline_config, expected);
    }
}
