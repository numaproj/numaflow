use crate::config::components::metrics::MetricsConfig;
use crate::config::components::sink::SinkConfig;
use crate::config::components::source::SourceConfig;
use crate::config::components::transformer::{TransformerConfig, TransformerType};
use crate::config::pipeline::isb::{BufferReaderConfig, BufferWriterConfig};
use crate::error::Error;
use crate::message::get_vertex_replica;
use crate::Result;
use base64::prelude::BASE64_STANDARD;
use base64::Engine;
use numaflow_models::models::{ForwardConditions, Vertex};
use serde_json::from_slice;
use std::env;
use std::time::Duration;

const DEFAULT_BATCH_SIZE: u64 = 500;
const DEFAULT_TIMEOUT_IN_MS: u32 = 1000;
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
    pub(crate) timeout_in_ms: u64,
    pub(crate) js_client_config: isb::jetstream::ClientConfig, // TODO: make it enum, since we can have different ISB implementations
    pub(crate) from_vertex_config: Vec<FromVertexConfig>,
    pub(crate) to_vertex_config: Vec<ToVertexConfig>,
    pub(crate) vertex_config: VertexType,
    pub(crate) metrics_config: MetricsConfig,
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
    pub fn load(pipeline_spec_obj: String) -> Result<Self> {
        // controller sets this env var.
        let decoded_spec = BASE64_STANDARD
            .decode(pipeline_spec_obj.as_bytes())
            .map_err(|e| Error::Config(format!("Failed to decode mono vertex spec: {:?}", e)))?;

        let vertex_obj: Vertex = from_slice(&decoded_spec)
            .map_err(|e| Error::Config(format!("Failed to parse mono vertex spec: {:?}", e)))?;

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
            let fb_sink_config = if let Some(_) = sink.fallback.as_ref() {
                Some(SinkConfig {
                    sink_type: sink.clone().try_into()?,
                    retry_config: None,
                })
            } else {
                None
            };

            VertexType::Sink(SinkVtxConfig {
                sink_config: SinkConfig {
                    sink_type: sink.try_into()?,
                    retry_config: None,
                },
                fb_sink_config,
            })
        } else {
            return Err(Error::Config(
                "Only source and sink are supported ATM".to_string(),
            ));
        };

        let js_client_config = isb::jetstream::ClientConfig {
            url: env::var(ENV_NUMAFLOW_SERVING_JETSTREAM_URL).map_err(|e| {
                Error::Config(format!(
                    "Failed to get Jetstream URL from environment variable: {:?}",
                    e
                ))
            })?,
            user: env::var(ENV_NUMAFLOW_SERVING_JETSTREAM_USER).ok(),
            password: env::var(ENV_NUMAFLOW_SERVING_JETSTREAM_PASSWORD).ok(),
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
                    name: buffer_name,
                    partitions: partition_count,
                    streams,
                    batch_size: 100,
                    wip_ack_interval: Duration::from_millis(10),
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

            to_vertex_config.push(ToVertexConfig {
                name: edge.to,
                writer_config: BufferWriterConfig {
                    name: buffer_name,
                    streams,
                    partitions: partition_count,
                    ..Default::default()
                },
                partitions: edge.to_vertex_partition_count.unwrap_or_default() as u16,
                conditions: None,
            });
        }

        Ok(PipelineConfig {
            batch_size: batch_size as usize,
            timeout_in_ms: timeout_in_ms as u64,
            pipeline_name,
            vertex_name,
            replica: *replica,
            js_client_config,
            from_vertex_config,
            to_vertex_config,
            vertex_config: vertex,
            metrics_config: Default::default(),
        })
    }
}
