use std::collections::HashMap;
use std::env;
use std::time::Duration;

use base64::Engine;
use base64::prelude::BASE64_STANDARD;
use numaflow_models::models::{ForwardConditions, Vertex, Watermark};
use serde::Deserialize;
use serde_json::from_slice;
use tracing::info;

use super::{
    DEFAULT_CALLBACK_CONCURRENCY, ENV_CALLBACK_CONCURRENCY, ENV_CALLBACK_ENABLED,
    ENV_NUMAFLOW_SERVING_RESPONSE_STORE,
};
use crate::Result;
use crate::config::ENV_NUMAFLOW_SERVING_CALLBACK_STORE;
use crate::config::ENV_NUMAFLOW_SERVING_SPEC;
use crate::config::components::metrics::MetricsConfig;
use crate::config::components::reduce::{ReducerConfig, StorageConfig};
use crate::config::components::sink::SinkConfig;
use crate::config::components::sink::SinkType;
use crate::config::components::source::SourceConfig;
use crate::config::components::transformer::{TransformerConfig, TransformerType};
use crate::config::get_vertex_replica;
use crate::config::pipeline::isb::{BufferReaderConfig, BufferWriterConfig, Stream};
use crate::config::pipeline::map::MapMode;
use crate::config::pipeline::map::MapVtxConfig;
use crate::config::pipeline::watermark::WatermarkConfig;
use crate::config::pipeline::watermark::{BucketConfig, EdgeWatermarkConfig};
use crate::config::pipeline::watermark::{IdleConfig, SourceWatermarkConfig};
use crate::error::Error;

const DEFAULT_BATCH_SIZE: u64 = 500;
const DEFAULT_TIMEOUT_IN_MS: u32 = 1000;
const DEFAULT_LOOKBACK_WINDOW_IN_SECS: u16 = 120;
const DEFAULT_GRACEFUL_SHUTDOWN_TIME_SECS: u64 = 20; // time we will wait for UDFs to finish before shutting down
const ENV_NUMAFLOW_SERVING_JETSTREAM_URL: &str = "NUMAFLOW_ISBSVC_JETSTREAM_URL";
const ENV_NUMAFLOW_SERVING_JETSTREAM_USER: &str = "NUMAFLOW_ISBSVC_JETSTREAM_USER";
const ENV_NUMAFLOW_SERVING_JETSTREAM_PASSWORD: &str = "NUMAFLOW_ISBSVC_JETSTREAM_PASSWORD";
const ENV_PAF_BATCH_SIZE: &str = "PAF_BATCH_SIZE";
const ENV_NUMAFLOW_GRACEFUL_TIMEOUT_SECS: &str = "NUMAFLOW_GRACEFUL_TIMEOUT_SECS";
const DEFAULT_GRPC_MAX_MESSAGE_SIZE: usize = 64 * 1024 * 1024; // 64 MB
const DEFAULT_MAP_SOCKET: &str = "/var/run/numaflow/map.sock";
pub(crate) const DEFAULT_BATCH_MAP_SOCKET: &str = "/var/run/numaflow/batchmap.sock";
pub(crate) const DEFAULT_STREAM_MAP_SOCKET: &str = "/var/run/numaflow/mapstream.sock";
const DEFAULT_MAP_SERVER_INFO_FILE: &str = "/var/run/numaflow/mapper-server-info";
const DEFAULT_SERVING_STORE_SOCKET: &str = "/var/run/numaflow/serving.sock";
const DEFAULT_SERVING_STORE_SERVER_INFO_FILE: &str = "/var/run/numaflow/serving-server-info";
pub(crate) const VERTEX_TYPE_SOURCE: &str = "Source";
pub(crate) const VERTEX_TYPE_SINK: &str = "Sink";
pub(crate) const VERTEX_TYPE_MAP_UDF: &str = "MapUDF";
pub(crate) const VERTEX_TYPE_REDUCE_UDF: &str = "ReduceUDF";

pub(crate) mod isb;
pub(crate) mod watermark;

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct PipelineConfig {
    pub(crate) pipeline_name: &'static str,
    pub(crate) vertex_name: &'static str,
    pub(crate) replica: u16,
    pub(crate) batch_size: usize,
    pub(crate) paf_concurrency: usize,
    pub(crate) read_timeout: Duration,
    pub(crate) graceful_shutdown_time: Duration,
    pub(crate) js_client_config: isb::jetstream::ClientConfig, // TODO: make it enum, since we can have different ISB implementations
    pub(crate) from_vertex_config: Vec<FromVertexConfig>,
    pub(crate) to_vertex_config: Vec<ToVertexConfig>,
    pub(crate) vertex_config: VertexConfig,
    pub(crate) vertex_type: VertexType,
    pub(crate) metrics_config: MetricsConfig,
    pub(crate) watermark_config: Option<WatermarkConfig>,
    pub(crate) callback_config: Option<ServingCallbackConfig>,
    pub(crate) isb_config: Option<isb_config::ISBConfig>,
}

pub(crate) mod isb_config {
    #[derive(Debug, Clone, PartialEq)]
    pub(crate) struct ISBConfig {
        pub(crate) compression: Compression,
    }

    #[derive(Debug, Clone, PartialEq)]
    pub(crate) struct Compression {
        pub(crate) compress_type: CompressionType,
    }

    #[derive(Debug, Copy, Clone, PartialEq)]
    pub(crate) enum CompressionType {
        None,
        Gzip,
        Zstd,
        LZ4,
    }

    impl TryFrom<numaflow_models::models::Compression> for Compression {
        type Error = String;
        fn try_from(value: numaflow_models::models::Compression) -> Result<Self, Self::Error> {
            match value.r#type {
                None => Ok(Compression {
                    compress_type: CompressionType::None,
                }),
                Some(t) => match t.as_str() {
                    "gzip" => Ok(Compression {
                        compress_type: CompressionType::Gzip,
                    }),
                    "zstd" => Ok(Compression {
                        compress_type: CompressionType::Zstd,
                    }),
                    "lz4" => Ok(Compression {
                        compress_type: CompressionType::LZ4,
                    }),
                    "none" => Ok(Compression {
                        compress_type: CompressionType::None,
                    }),
                    _ => Err(format!("Invalid compression type: {t}")),
                },
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ServingCallbackConfig {
    pub(crate) callback_store: &'static str,
    pub(crate) callback_concurrency: usize,
}

impl Default for PipelineConfig {
    fn default() -> Self {
        PipelineConfig {
            pipeline_name: Default::default(),
            vertex_name: Default::default(),
            replica: 0,
            batch_size: DEFAULT_BATCH_SIZE as usize,
            paf_concurrency: DEFAULT_BATCH_SIZE as usize,
            read_timeout: Duration::from_secs(DEFAULT_TIMEOUT_IN_MS as u64),
            graceful_shutdown_time: Duration::from_secs(DEFAULT_GRACEFUL_SHUTDOWN_TIME_SECS),
            js_client_config: isb::jetstream::ClientConfig::default(),
            from_vertex_config: vec![],
            to_vertex_config: vec![],
            vertex_config: VertexConfig::Source(SourceVtxConfig {
                source_config: Default::default(),
                transformer_config: None,
            }),
            vertex_type: VertexType::Source,
            metrics_config: Default::default(),
            watermark_config: None,
            callback_config: None,
            isb_config: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct SourceVtxConfig {
    pub(crate) source_config: SourceConfig,
    pub(crate) transformer_config: Option<TransformerConfig>,
}

pub(crate) mod map {
    use numaflow_models::models::Udf;

    use crate::config::pipeline::{
        DEFAULT_GRPC_MAX_MESSAGE_SIZE, DEFAULT_MAP_SERVER_INFO_FILE, DEFAULT_MAP_SOCKET,
    };
    use crate::error::Error;

    /// re-export MapMode from shared.
    pub use numaflow_shared::server_info::MapMode;

    #[derive(Debug, Clone, PartialEq)]
    pub(crate) struct MapVtxConfig {
        pub(crate) concurrency: usize,
        pub(crate) map_type: MapType,
        pub(crate) map_mode: MapMode,
    }

    #[derive(Debug, Clone, PartialEq)]
    pub(crate) enum MapType {
        UserDefined(UserDefinedConfig),
    }

    impl TryFrom<Box<Udf>> for MapType {
        type Error = Error;
        fn try_from(udf: Box<Udf>) -> Result<Self, Self::Error> {
            if let Some(_container) = udf.container {
                Ok(MapType::UserDefined(UserDefinedConfig {
                    grpc_max_message_size: DEFAULT_GRPC_MAX_MESSAGE_SIZE,
                    socket_path: DEFAULT_MAP_SOCKET.to_string(),
                    server_info_path: DEFAULT_MAP_SERVER_INFO_FILE.to_string(),
                }))
            } else {
                Err(Error::Config("Invalid UDF".to_string()))
            }
        }
    }

    #[derive(Debug, Clone, PartialEq)]
    pub(crate) struct UserDefinedConfig {
        pub grpc_max_message_size: usize,
        pub socket_path: String,
        pub server_info_path: String,
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct SinkVtxConfig {
    pub(crate) sink_config: SinkConfig,
    pub(crate) fb_sink_config: Option<SinkConfig>,
    pub(crate) serving_store_config: Option<ServingStoreType>,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum VertexConfig {
    Source(SourceVtxConfig),
    Sink(SinkVtxConfig),
    Map(MapVtxConfig),
    Reduce(ReduceVtxConfig),
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ReduceVtxConfig {
    pub(crate) reducer_config: ReducerConfig,
    pub(crate) wal_storage_config: Option<StorageConfig>,
}

#[derive(Debug, Deserialize, Clone, PartialEq)]
pub(crate) enum ServingStoreType {
    UserDefined(UserDefinedStoreConfig),
    Nats(NatsStoreConfig),
}

#[derive(Debug, Deserialize, Clone, PartialEq)]
pub(crate) struct UserDefinedStoreConfig {
    pub(crate) grpc_max_message_size: usize,
    pub(crate) socket_path: String,
    pub(crate) server_info_path: String,
}

#[derive(Debug, Deserialize, Clone, PartialEq)]
pub(crate) struct NatsStoreConfig {
    /// Response (or result) Store Name.
    pub(crate) rs_store_name: String,
}

impl Default for UserDefinedStoreConfig {
    fn default() -> Self {
        Self {
            grpc_max_message_size: DEFAULT_GRPC_MAX_MESSAGE_SIZE,
            socket_path: DEFAULT_SERVING_STORE_SOCKET.to_string(),
            server_info_path: DEFAULT_SERVING_STORE_SERVER_INFO_FILE.to_string(),
        }
    }
}

impl std::fmt::Display for VertexConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::result::Result<(), std::fmt::Error> {
        match self {
            VertexConfig::Source(_) => write!(f, "{VERTEX_TYPE_SOURCE}"),
            VertexConfig::Sink(_) => write!(f, "{VERTEX_TYPE_SINK}"),
            VertexConfig::Map(_) => write!(f, "{VERTEX_TYPE_MAP_UDF}"),
            VertexConfig::Reduce(_) => write!(f, "{VERTEX_TYPE_REDUCE_UDF}"),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct FromVertexConfig {
    pub(crate) name: &'static str,
    pub(crate) reader_config: BufferReaderConfig,
    pub(crate) partitions: u16,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) enum VertexType {
    Source,
    Sink,
    MapUDF,
    ReduceUDF,
}

impl VertexType {
    pub(crate) fn from_str(s: &str) -> Result<Self> {
        match s {
            VERTEX_TYPE_SOURCE => Ok(VertexType::Source),
            VERTEX_TYPE_SINK => Ok(VertexType::Sink),
            VERTEX_TYPE_MAP_UDF => Ok(VertexType::MapUDF),
            VERTEX_TYPE_REDUCE_UDF => Ok(VertexType::ReduceUDF),
            _ => Err(Error::Config(format!("Unknown vertex type: {s}"))),
        }
    }

    pub(crate) fn as_str(&self) -> &'static str {
        match self {
            VertexType::Source => VERTEX_TYPE_SOURCE,
            VertexType::Sink => VERTEX_TYPE_SINK,
            VertexType::MapUDF => VERTEX_TYPE_MAP_UDF,
            VertexType::ReduceUDF => VERTEX_TYPE_REDUCE_UDF,
        }
    }
}

impl std::fmt::Display for VertexType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::result::Result<(), std::fmt::Error> {
        write!(f, "{}", self.as_str())
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ToVertexConfig {
    pub(crate) name: &'static str,
    pub(crate) partitions: u16,
    pub(crate) writer_config: BufferWriterConfig,
    pub(crate) conditions: Option<Box<ForwardConditions>>,
    pub(crate) to_vertex_type: VertexType,
}

impl PipelineConfig {
    pub(crate) fn load(
        pipeline_spec_obj: String,
        env_vars: impl IntoIterator<Item = (impl Into<String>, impl Into<String>)>,
    ) -> Result<Self> {
        let env_vars: HashMap<String, String> = env_vars
            .into_iter()
            .map(|(key, val)| (key.into(), val.into()))
            .filter(|(key, _val)| {
                [
                    ENV_NUMAFLOW_SERVING_JETSTREAM_URL,
                    ENV_NUMAFLOW_SERVING_JETSTREAM_USER,
                    ENV_NUMAFLOW_SERVING_JETSTREAM_PASSWORD,
                    ENV_PAF_BATCH_SIZE,
                    ENV_CALLBACK_ENABLED,
                    ENV_CALLBACK_CONCURRENCY,
                    ENV_NUMAFLOW_SERVING_SPEC,
                    ENV_NUMAFLOW_SERVING_CALLBACK_STORE,
                    ENV_NUMAFLOW_SERVING_RESPONSE_STORE,
                    ENV_NUMAFLOW_GRACEFUL_TIMEOUT_SECS,
                ]
                .contains(&key.as_str())
            })
            .collect();

        let get_var = |var: &str| -> Result<String> {
            Ok(env_vars
                .get(var)
                .ok_or_else(|| Error::Config(format!("Environment variable {var} is not set")))?
                .to_string())
        };

        // controller sets this env var.
        let decoded_spec = BASE64_STANDARD
            .decode(pipeline_spec_obj.as_bytes())
            .map_err(|e| Error::Config(format!("Failed to decode pipeline spec: {e:?}")))?;

        let vertex_obj: Vertex = from_slice(&decoded_spec)
            .map_err(|e| Error::Config(format!("Failed to parse pipeline spec: {e:?}")))?;

        info!("Loaded pipeline spec: {:?}", vertex_obj);

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

        let (vertex, vertex_type): (VertexConfig, VertexType) = if let Some(source) =
            vertex_obj.spec.source
        {
            let transformer_config = source.transformer.as_ref().map(|_| TransformerConfig {
                concurrency: batch_size as usize, // FIXME: introduce a separate field in the spec
                transformer_type: TransformerType::UserDefined(Default::default()),
            });

            (
                VertexConfig::Source(SourceVtxConfig {
                    source_config: SourceConfig {
                        read_ahead: env::var("READ_AHEAD")
                            .unwrap_or("false".to_string())
                            .parse()
                            .unwrap(),
                        source_type: source.try_into()?,
                    },
                    transformer_config,
                }),
                VertexType::Source,
            )
        } else if let Some(sink) = vertex_obj.spec.sink {
            let fb_sink_config = if sink.fallback.as_ref().is_some() {
                Some(SinkConfig {
                    sink_type: SinkType::fallback_sinktype(&sink)?,
                    retry_config: None,
                })
            } else {
                None
            };

            let serving_store_config = if let Some(serving_spec) =
                env_vars.get(ENV_NUMAFLOW_SERVING_SPEC)
            {
                let serving_spec_decoded = BASE64_STANDARD
                    .decode(serving_spec.as_bytes())
                    .map_err(|e| {
                        Error::Config(
                            format!(
                                "Failed to base64 decode value of environment variable '{ENV_NUMAFLOW_SERVING_SPEC}'. value='{serving_spec}'. Err={e:?}"
                            )
                        )
                    })?;
                let serving_spec: numaflow_models::models::ServingSpec =
                    from_slice(serving_spec_decoded.as_slice()).map_err(|e| {
                        Error::Config(
                            format!(
                                "Failed to base64 decode value of environment variable '{ENV_NUMAFLOW_SERVING_SPEC}'. value='{serving_spec}'. Err={e:?}"
                            )
                        )
                    })?;

                if serving_spec.store.is_none() {
                    let rs_kv_store = get_var(ENV_NUMAFLOW_SERVING_RESPONSE_STORE)?;
                    Some(ServingStoreType::Nats(NatsStoreConfig {
                        rs_store_name: rs_kv_store,
                    }))
                } else {
                    Some(ServingStoreType::UserDefined(
                        UserDefinedStoreConfig::default(),
                    ))
                }
            } else {
                None
            };

            (
                VertexConfig::Sink(SinkVtxConfig {
                    sink_config: SinkConfig {
                        sink_type: SinkType::primary_sinktype(&sink)?,
                        retry_config: sink.retry_strategy.clone().map(|retry| retry.into()),
                    },
                    fb_sink_config,
                    serving_store_config,
                }),
                VertexType::Sink,
            )
        } else if let Some(udf) = vertex_obj.spec.udf {
            if let Some(group_by) = &udf.group_by {
                // This is a reduce vertex
                let storage_config = group_by.storage.as_ref().and_then(|storage| {
                    if storage.no_store.is_some() {
                        None
                    } else {
                        Some(Default::default())
                    }
                });

                (
                    VertexConfig::Reduce(ReduceVtxConfig {
                        reducer_config: group_by.try_into()?,
                        wal_storage_config: storage_config,
                    }),
                    VertexType::ReduceUDF,
                )
            } else {
                // This is a map vertex
                (
                    VertexConfig::Map(MapVtxConfig {
                        concurrency: batch_size as usize,
                        map_type: udf.try_into()?,
                        map_mode: MapMode::Unary,
                    }),
                    VertexType::MapUDF,
                )
            }
        } else {
            return Err(Error::Config(
                "Only source, sink, map, and reduce are supported".to_string(),
            ));
        };

        let js_client_config = isb::jetstream::ClientConfig {
            url: get_var(ENV_NUMAFLOW_SERVING_JETSTREAM_URL)?,
            user: get_var(ENV_NUMAFLOW_SERVING_JETSTREAM_USER).ok(),
            password: get_var(ENV_NUMAFLOW_SERVING_JETSTREAM_PASSWORD).ok(),
        };

        let mut from_vertex_config = vec![];
        for edge in from_edges {
            let partition_count = edge.to_vertex_partition_count.unwrap_or_default() as u16;

            let streams: Vec<Stream> = (0..partition_count)
                .map(|i| {
                    let ns: &'static str = Box::leak(namespace.clone().into_boxed_str());
                    let pl: &'static str = Box::leak(pipeline_name.clone().into_boxed_str());
                    let to: &'static str = Box::leak(edge.to.clone().into_boxed_str());
                    let name: &'static str =
                        Box::leak(format!("{ns}-{pl}-{to}-{i}").into_boxed_str());
                    Stream::new(name, to, i)
                })
                .collect();

            from_vertex_config.push(FromVertexConfig {
                name: Box::leak(edge.from.clone().into_boxed_str()),
                reader_config: BufferReaderConfig {
                    streams,
                    ..Default::default()
                },
                partitions: partition_count,
            });
        }

        let mut to_vertex_config = vec![];
        for edge in to_edges {
            let partition_count = edge.to_vertex_partition_count.unwrap_or_default() as u16;

            let streams: Vec<Stream> = (0..partition_count)
                .map(|i| {
                    let ns: &'static str = Box::leak(namespace.clone().into_boxed_str());
                    let pl: &'static str = Box::leak(pipeline_name.clone().into_boxed_str());
                    let to: &'static str = Box::leak(edge.to.clone().into_boxed_str());
                    let name: &'static str =
                        Box::leak(format!("{ns}-{pl}-{to}-{i}").into_boxed_str());
                    Stream::new(name, to, i)
                })
                .collect();

            let default_writer_config = BufferWriterConfig::default();
            to_vertex_config.push(ToVertexConfig {
                name: Box::leak(edge.to.clone().into_boxed_str()),
                partitions: partition_count,
                writer_config: BufferWriterConfig {
                    streams,
                    max_length: edge
                        .to_vertex_limits
                        .as_ref()
                        .and_then(|l| l.buffer_max_length)
                        .unwrap_or(default_writer_config.max_length as i64)
                        as usize,
                    usage_limit: edge
                        .to_vertex_limits
                        .as_ref()
                        .and_then(|l| l.buffer_usage_limit)
                        .unwrap_or(default_writer_config.usage_limit as i64)
                        as f64
                        / 100.0,
                    buffer_full_strategy: edge
                        .on_full
                        .and_then(|s| s.clone().try_into().ok())
                        .unwrap_or(default_writer_config.buffer_full_strategy),
                },
                conditions: edge.conditions,
                to_vertex_type: VertexType::from_str(&edge.to_vertex_type)?,
            });
        }

        let watermark_config = if vertex_obj
            .spec
            .watermark
            .clone()
            .is_none_or(|w| !w.disabled.unwrap_or(false))
        {
            Self::create_watermark_config(
                vertex_obj.spec.watermark.clone(),
                &namespace,
                &pipeline_name,
                &vertex_name,
                &vertex,
                &from_vertex_config,
                &to_vertex_config,
            )
        } else {
            None
        };

        let look_back_window = vertex_obj
            .spec
            .scale
            .as_ref()
            .and_then(|scale| scale.lookback_seconds.map(|x| x as u16))
            .unwrap_or(DEFAULT_LOOKBACK_WINDOW_IN_SECS);

        let mut callback_config = None;
        if get_var(ENV_CALLBACK_ENABLED)
            .map(|v| v == "true")
            .unwrap_or(false)
        {
            let callback_concurrency: usize = get_var(ENV_CALLBACK_CONCURRENCY)
                .unwrap_or_else(|_| format!("{DEFAULT_CALLBACK_CONCURRENCY}"))
                .parse()
                .map_err(|e| {
                    Error::Config(format!(
                        "Parsing value of {ENV_CALLBACK_CONCURRENCY}: {e:?}"
                    ))
                })?;

            let kv_store = env::var(ENV_NUMAFLOW_SERVING_CALLBACK_STORE).map_err(|_| {
                Error::Config("Serving store is default, but environment variable NUMAFLOW_SERVING_CALLBACK_STORE is not set".into())
            })?;
            callback_config = Some(ServingCallbackConfig {
                callback_store: Box::leak(kv_store.into_boxed_str()),
                callback_concurrency,
            });
        }

        let isb_config: Option<isb_config::ISBConfig> =
            match vertex_obj.spec.inter_step_buffer.as_ref() {
                None => None,
                Some(isb_spec) => {
                    let compress_type = match isb_spec.compression.as_ref() {
                        None => isb_config::CompressionType::None,
                        Some(t) => match &t.r#type {
                            None => isb_config::CompressionType::None,
                            Some(t) => match t.as_str() {
                                "gzip" => isb_config::CompressionType::Gzip,
                                "zstd" => isb_config::CompressionType::Zstd,
                                "lz4" => isb_config::CompressionType::LZ4,
                                _ => {
                                    return Err(Error::Config(format!(
                                        "Invalid compression type setting: {t}"
                                    )));
                                }
                            },
                        },
                    };
                    Some(isb_config::ISBConfig {
                        compression: isb_config::Compression { compress_type },
                    })
                }
            };

        let graceful_shutdown_time_secs = env_vars
            .get(ENV_NUMAFLOW_GRACEFUL_TIMEOUT_SECS)
            .and_then(|s| s.parse().ok())
            .unwrap_or(DEFAULT_GRACEFUL_SHUTDOWN_TIME_SECS);

        Ok(PipelineConfig {
            batch_size: batch_size as usize,
            paf_concurrency: get_var(ENV_PAF_BATCH_SIZE)
                .and_then(|s| {
                    s.parse().map_err(|e| {
                        Error::Config(format!("Parsing value of {ENV_PAF_BATCH_SIZE}: {e:?}"))
                    })
                })
                .unwrap_or(batch_size as usize),
            read_timeout: Duration::from_millis(timeout_in_ms as u64),
            graceful_shutdown_time: Duration::from_secs(graceful_shutdown_time_secs),
            pipeline_name: Box::leak(pipeline_name.into_boxed_str()),
            vertex_name: Box::leak(vertex_name.into_boxed_str()),
            replica: *replica,
            js_client_config,
            from_vertex_config,
            to_vertex_config,
            vertex_type,
            vertex_config: vertex,
            metrics_config: MetricsConfig::with_lookback_window_in_secs(look_back_window),
            watermark_config,
            callback_config,
            isb_config,
        })
    }

    fn create_watermark_config(
        watermark_spec: Option<Box<Watermark>>,
        namespace: &str,
        pipeline_name: &str,
        vertex_name: &str,
        vertex: &VertexConfig,
        from_vertex_config: &[FromVertexConfig],
        to_vertex_config: &[ToVertexConfig],
    ) -> Option<WatermarkConfig> {
        let max_delay = watermark_spec
            .as_ref()
            .and_then(|w| w.max_delay.map(|x| Duration::from(x).as_millis() as u64))
            .unwrap_or(0);

        let idle_config = watermark_spec
            .as_ref()
            .and_then(|w| w.idle_source.as_ref())
            .map(|idle| IdleConfig {
                increment_by: idle.increment_by.map(Duration::from).unwrap_or_default(),
                step_interval: idle.step_interval.map(Duration::from).unwrap_or_default(),
                threshold: idle.threshold.map(Duration::from).unwrap_or_default(),
            });

        // Helper function to create bucket config for to_vertex
        let create_to_vertex_bucket_config = |to: &ToVertexConfig| BucketConfig {
            vertex: to.name,
            partitions: to.partitions,
            ot_bucket: Box::leak(
                format!(
                    "{}-{}-{}-{}_OT",
                    namespace, pipeline_name, vertex_name, &to.name
                )
                .into_boxed_str(),
            ),
            hb_bucket: Box::leak(
                format!(
                    "{}-{}-{}-{}_PROCESSORS",
                    namespace, pipeline_name, vertex_name, &to.name
                )
                .into_boxed_str(),
            ),
        };

        // Helper function to create bucket config for from_vertex
        let create_from_vertex_bucket_config =
            |from: &FromVertexConfig, partitions: u16| BucketConfig {
                vertex: from.name,
                partitions,
                ot_bucket: Box::leak(
                    format!(
                        "{}-{}-{}-{}_OT",
                        namespace, pipeline_name, &from.name, vertex_name
                    )
                    .into_boxed_str(),
                ),
                hb_bucket: Box::leak(
                    format!(
                        "{}-{}-{}-{}_PROCESSORS",
                        namespace, pipeline_name, &from.name, vertex_name
                    )
                    .into_boxed_str(),
                ),
            };

        match vertex {
            VertexConfig::Source(_) => Some(WatermarkConfig::Source(SourceWatermarkConfig {
                max_delay: Duration::from_millis(max_delay),
                source_bucket_config: BucketConfig {
                    vertex: Box::leak(vertex_name.to_string().into_boxed_str()),
                    partitions: 1, // source will have only one partition
                    ot_bucket: Box::leak(
                        format!("{namespace}-{pipeline_name}-{vertex_name}_SOURCE_OT")
                            .into_boxed_str(),
                    ),
                    hb_bucket: Box::leak(
                        format!("{namespace}-{pipeline_name}-{vertex_name}_SOURCE_PROCESSORS")
                            .into_boxed_str(),
                    ),
                },
                to_vertex_bucket_config: to_vertex_config
                    .iter()
                    .map(create_to_vertex_bucket_config)
                    .collect(),
                idle_config,
            })),
            VertexConfig::Sink(_) | VertexConfig::Map(_) => {
                Some(WatermarkConfig::Edge(EdgeWatermarkConfig {
                    from_vertex_config: from_vertex_config
                        .iter()
                        .map(|from| create_from_vertex_bucket_config(from, from.partitions))
                        .collect(),
                    to_vertex_config: to_vertex_config
                        .iter()
                        .map(create_to_vertex_bucket_config)
                        .collect(),
                }))
            }
            VertexConfig::Reduce(_) => {
                Some(WatermarkConfig::Edge(EdgeWatermarkConfig {
                    from_vertex_config: from_vertex_config
                        .iter()
                        .map(|from| create_from_vertex_bucket_config(from, 1)) // reduce will have only one partition
                        .collect(),
                    to_vertex_config: to_vertex_config
                        .iter()
                        .map(create_to_vertex_bucket_config)
                        .collect(),
                }))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use numaflow_models::models::{Container, Udf};
    use numaflow_pulsar::source::PulsarSourceConfig;

    use super::*;
    use crate::config::components::sink::{BlackholeConfig, LogConfig, RetryConfig, SinkType};
    use crate::config::components::source::{GeneratorConfig, SourceType};
    use crate::config::pipeline::map::{MapType, UserDefinedConfig};

    #[test]
    fn test_default_pipeline_config() {
        let expected = PipelineConfig {
            pipeline_name: Default::default(),
            vertex_name: Default::default(),
            replica: 0,
            batch_size: DEFAULT_BATCH_SIZE as usize,
            paf_concurrency: DEFAULT_BATCH_SIZE as usize,
            read_timeout: Duration::from_secs(DEFAULT_TIMEOUT_IN_MS as u64),
            graceful_shutdown_time: Duration::from_secs(DEFAULT_GRACEFUL_SHUTDOWN_TIME_SECS),
            js_client_config: isb::jetstream::ClientConfig::default(),
            from_vertex_config: vec![],
            to_vertex_config: vec![],
            vertex_type: VertexType::Source,
            vertex_config: VertexConfig::Source(SourceVtxConfig {
                source_config: Default::default(),
                transformer_config: None,
            }),
            metrics_config: Default::default(),
            watermark_config: None,
            callback_config: None,
            isb_config: None,
        };

        let config = PipelineConfig::default();
        assert_eq!(config, expected);
    }

    #[test]
    fn test_vertex_type_display() {
        let src_type = VertexConfig::Source(SourceVtxConfig {
            source_config: SourceConfig::default(),
            transformer_config: None,
        });
        assert_eq!(src_type.to_string(), "Source");

        let sink_type = VertexConfig::Sink(SinkVtxConfig {
            sink_config: SinkConfig {
                sink_type: SinkType::Log(LogConfig {}),
                retry_config: None,
            },
            fb_sink_config: None,
            serving_store_config: None,
        });
        assert_eq!(sink_type.to_string(), "Sink");
    }

    #[test]
    fn test_to_vertex_type_conversion() {
        // Test from_str
        assert_eq!(VertexType::from_str("Source").unwrap(), VertexType::Source);
        assert_eq!(VertexType::from_str("Sink").unwrap(), VertexType::Sink);
        assert_eq!(VertexType::from_str("MapUDF").unwrap(), VertexType::MapUDF);
        assert_eq!(
            VertexType::from_str("ReduceUDF").unwrap(),
            VertexType::ReduceUDF
        );

        // Test invalid string
        assert!(VertexType::from_str("Invalid").is_err());

        // Test as_str
        assert_eq!(VertexType::Source.as_str(), "Source");
        assert_eq!(VertexType::Sink.as_str(), "Sink");
        assert_eq!(VertexType::MapUDF.as_str(), "MapUDF");
        assert_eq!(VertexType::ReduceUDF.as_str(), "ReduceUDF");
    }

    #[test]
    fn test_pipeline_config_load_sink_vertex() {
        let pipeline_cfg_base64 = "eyJtZXRhZGF0YSI6eyJuYW1lIjoic2ltcGxlLXBpcGVsaW5lLW91dCIsIm5hbWVzcGFjZSI6ImRlZmF1bHQiLCJjcmVhdGlvblRpbWVzdGFtcCI6bnVsbH0sInNwZWMiOnsibmFtZSI6Im91dCIsInNpbmsiOnsiYmxhY2tob2xlIjp7fSwicmV0cnlTdHJhdGVneSI6eyJvbkZhaWx1cmUiOiJyZXRyeSJ9fSwibGltaXRzIjp7InJlYWRCYXRjaFNpemUiOjUwMCwicmVhZFRpbWVvdXQiOiIxcyIsImJ1ZmZlck1heExlbmd0aCI6MzAwMDAsImJ1ZmZlclVzYWdlTGltaXQiOjgwfSwic2NhbGUiOnsibWluIjoxfSwidXBkYXRlU3RyYXRlZ3kiOnsidHlwZSI6IlJvbGxpbmdVcGRhdGUiLCJyb2xsaW5nVXBkYXRlIjp7Im1heFVuYXZhaWxhYmxlIjoiMjUlIn19LCJwaXBlbGluZU5hbWUiOiJzaW1wbGUtcGlwZWxpbmUiLCJpbnRlclN0ZXBCdWZmZXJTZXJ2aWNlTmFtZSI6IiIsInJlcGxpY2FzIjowLCJmcm9tRWRnZXMiOlt7ImZyb20iOiJpbiIsInRvIjoib3V0IiwiY29uZGl0aW9ucyI6bnVsbCwiZnJvbVZlcnRleFR5cGUiOiJTb3VyY2UiLCJmcm9tVmVydGV4UGFydGl0aW9uQ291bnQiOjEsImZyb21WZXJ0ZXhMaW1pdHMiOnsicmVhZEJhdGNoU2l6ZSI6NTAwLCJyZWFkVGltZW91dCI6IjFzIiwiYnVmZmVyTWF4TGVuZ3RoIjozMDAwMCwiYnVmZmVyVXNhZ2VMaW1pdCI6ODB9LCJ0b1ZlcnRleFR5cGUiOiJTaW5rIiwidG9WZXJ0ZXhQYXJ0aXRpb25Db3VudCI6MSwidG9WZXJ0ZXhMaW1pdHMiOnsicmVhZEJhdGNoU2l6ZSI6NTAwLCJyZWFkVGltZW91dCI6IjFzIiwiYnVmZmVyTWF4TGVuZ3RoIjozMDAwMCwiYnVmZmVyVXNhZ2VMaW1pdCI6ODB9fV0sIndhdGVybWFyayI6eyJtYXhEZWxheSI6IjBzIn19LCJzdGF0dXMiOnsicGhhc2UiOiIiLCJyZXBsaWNhcyI6MCwiZGVzaXJlZFJlcGxpY2FzIjowLCJsYXN0U2NhbGVkQXQiOm51bGx9fQ==".to_string();

        let env_vars = [
            ("NUMAFLOW_ISBSVC_JETSTREAM_URL", "localhost:4222"),
            (
                "NUMAFLOW_SERVING_SPEC",
                "eyJhdXRoIjpudWxsLCJzZXJ2aWNlIjp0cnVlLCJtc2dJREhlYWRlcktleSI6IlgtTnVtYWZsb3ctSWQifQ==",
            ),
            ("NUMAFLOW_SERVING_CALLBACK_STORE", "test-kv-store"),
            ("NUMAFLOW_SERVING_RESPONSE_STORE", "test-kv-store"),
        ];
        let pipeline_config = PipelineConfig::load(pipeline_cfg_base64, env_vars).unwrap();

        let expected = PipelineConfig {
            pipeline_name: "simple-pipeline",
            vertex_name: "out",
            replica: 0,
            batch_size: 500,
            paf_concurrency: 500,
            read_timeout: Duration::from_secs(1),
            graceful_shutdown_time: Duration::from_secs(DEFAULT_GRACEFUL_SHUTDOWN_TIME_SECS),
            js_client_config: isb::jetstream::ClientConfig {
                url: "localhost:4222".to_string(),
                user: None,
                password: None,
            },
            from_vertex_config: vec![FromVertexConfig {
                name: "in",
                reader_config: BufferReaderConfig {
                    streams: vec![Stream::new("default-simple-pipeline-out-0", "out", 0)],
                    wip_ack_interval: Duration::from_secs(1),
                },
                partitions: 1,
            }],
            to_vertex_config: vec![],
            vertex_config: VertexConfig::Sink(SinkVtxConfig {
                sink_config: SinkConfig {
                    sink_type: SinkType::Blackhole(BlackholeConfig {}),
                    retry_config: Some(RetryConfig::default()),
                },
                fb_sink_config: None,
                serving_store_config: Some(ServingStoreType::Nats(NatsStoreConfig {
                    rs_store_name: "test-kv-store".into(),
                })),
            }),
            vertex_type: VertexType::Sink,
            metrics_config: MetricsConfig {
                metrics_server_listen_port: 2469,
                lag_check_interval_in_secs: 5,
                lag_refresh_interval_in_secs: 3,
                lookback_window_in_secs: 120,
            },
            watermark_config: Some(WatermarkConfig::Edge(EdgeWatermarkConfig {
                from_vertex_config: vec![BucketConfig {
                    vertex: "in",
                    partitions: 1,
                    ot_bucket: "default-simple-pipeline-in-out_OT",
                    hb_bucket: "default-simple-pipeline-in-out_PROCESSORS",
                }],
                to_vertex_config: vec![],
            })),
            ..Default::default()
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
            pipeline_name: "simple-pipeline",
            vertex_name: "in",
            replica: 0,
            batch_size: 1000,
            paf_concurrency: 1000,
            read_timeout: Duration::from_secs(1),
            graceful_shutdown_time: Duration::from_secs(DEFAULT_GRACEFUL_SHUTDOWN_TIME_SECS),
            js_client_config: isb::jetstream::ClientConfig {
                url: "localhost:4222".to_string(),
                user: None,
                password: None,
            },
            from_vertex_config: vec![],
            to_vertex_config: vec![ToVertexConfig {
                name: "out",
                partitions: 1,
                writer_config: BufferWriterConfig {
                    streams: vec![Stream::new("default-simple-pipeline-out-0", "out", 0)],
                    max_length: 150000,
                    usage_limit: 0.85,
                    ..Default::default()
                },
                conditions: None,
                to_vertex_type: VertexType::Sink,
            }],
            vertex_config: VertexConfig::Source(SourceVtxConfig {
                source_config: SourceConfig {
                    read_ahead: false,
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
            watermark_config: None,
            ..Default::default()
        };

        assert_eq!(pipeline_config, expected);
    }

    #[test]
    fn test_pipeline_config_pulsar_source() {
        let pipeline_cfg_base64 = "eyJtZXRhZGF0YSI6eyJuYW1lIjoic2ltcGxlLXBpcGVsaW5lLWluIiwibmFtZXNwYWNlIjoiZGVmYXVsdCIsImNyZWF0aW9uVGltZXN0YW1wIjpudWxsfSwic3BlYyI6eyJuYW1lIjoiaW4iLCJzb3VyY2UiOnsicHVsc2FyIjp7InNlcnZlckFkZHIiOiJwdWxzYXI6Ly9wdWxzYXItc2VydmljZTo2NjUwIiwidG9waWMiOiJ0ZXN0X3BlcnNpc3RlbnQiLCJjb25zdW1lck5hbWUiOiJteV9wZXJzaXN0ZW50X2NvbnN1bWVyIiwic3Vic2NyaXB0aW9uTmFtZSI6Im15X3BlcnNpc3RlbnRfc3Vic2NyaXB0aW9uIn19LCJsaW1pdHMiOnsicmVhZEJhdGNoU2l6ZSI6NTAsInJlYWRUaW1lb3V0IjoiMXMiLCJidWZmZXJNYXhMZW5ndGgiOjMwMDAwLCJidWZmZXJVc2FnZUxpbWl0Ijo4MH0sInNjYWxlIjp7Im1pbiI6MSwibWF4IjoxfSwidXBkYXRlU3RyYXRlZ3kiOnsidHlwZSI6IlJvbGxpbmdVcGRhdGUiLCJyb2xsaW5nVXBkYXRlIjp7Im1heFVuYXZhaWxhYmxlIjoiMjUlIn19LCJwaXBlbGluZU5hbWUiOiJzaW1wbGUtcGlwZWxpbmUiLCJpbnRlclN0ZXBCdWZmZXJTZXJ2aWNlTmFtZSI6IiIsInJlcGxpY2FzIjowLCJ0b0VkZ2VzIjpbeyJmcm9tIjoiaW4iLCJ0byI6Im91dCIsImNvbmRpdGlvbnMiOm51bGwsImZyb21WZXJ0ZXhUeXBlIjoiU291cmNlIiwiZnJvbVZlcnRleFBhcnRpdGlvbkNvdW50IjoxLCJmcm9tVmVydGV4TGltaXRzIjp7InJlYWRCYXRjaFNpemUiOjUwLCJyZWFkVGltZW91dCI6IjFzIiwiYnVmZmVyTWF4TGVuZ3RoIjozMDAwMCwiYnVmZmVyVXNhZ2VMaW1pdCI6ODB9LCJ0b1ZlcnRleFR5cGUiOiJTaW5rIiwidG9WZXJ0ZXhQYXJ0aXRpb25Db3VudCI6MSwidG9WZXJ0ZXhMaW1pdHMiOnsicmVhZEJhdGNoU2l6ZSI6NTAsInJlYWRUaW1lb3V0IjoiMXMiLCJidWZmZXJNYXhMZW5ndGgiOjMwMDAwLCJidWZmZXJVc2FnZUxpbWl0Ijo4MH19XSwid2F0ZXJtYXJrIjp7Im1heERlbGF5IjoiMHMifX0sInN0YXR1cyI6eyJwaGFzZSI6IiIsInJlcGxpY2FzIjowLCJkZXNpcmVkUmVwbGljYXMiOjAsImxhc3RTY2FsZWRBdCI6bnVsbH19";

        let env_vars = [("NUMAFLOW_ISBSVC_JETSTREAM_URL", "localhost:4222")];
        let pipeline_config =
            PipelineConfig::load(pipeline_cfg_base64.to_string(), env_vars).unwrap();

        let expected = PipelineConfig {
            pipeline_name: "simple-pipeline",
            vertex_name: "in",
            replica: 0,
            batch_size: 50,
            paf_concurrency: 50,
            read_timeout: Duration::from_secs(1),
            graceful_shutdown_time: Duration::from_secs(DEFAULT_GRACEFUL_SHUTDOWN_TIME_SECS),
            js_client_config: isb::jetstream::ClientConfig {
                url: "localhost:4222".to_string(),
                user: None,
                password: None,
            },
            from_vertex_config: vec![],
            to_vertex_config: vec![ToVertexConfig {
                name: "out",
                partitions: 1,
                writer_config: BufferWriterConfig {
                    streams: vec![Stream::new("default-simple-pipeline-out-0", "out", 0)],
                    max_length: 30000,
                    usage_limit: 0.8,
                    ..Default::default()
                },
                conditions: None,
                to_vertex_type: VertexType::Sink,
            }],
            vertex_config: VertexConfig::Source(SourceVtxConfig {
                source_config: SourceConfig {
                    read_ahead: false,
                    source_type: SourceType::Pulsar(PulsarSourceConfig {
                        pulsar_server_addr: "pulsar://pulsar-service:6650".to_string(),
                        topic: "test_persistent".to_string(),
                        consumer_name: "my_persistent_consumer".to_string(),
                        subscription: "my_persistent_subscription".to_string(),
                        max_unack: 1000,
                        auth: None,
                    }),
                },
                transformer_config: None,
            }),
            metrics_config: Default::default(),
            watermark_config: Some(WatermarkConfig::Source(SourceWatermarkConfig {
                max_delay: Default::default(),
                source_bucket_config: BucketConfig {
                    vertex: "in",
                    partitions: 1,
                    ot_bucket: "default-simple-pipeline-in_SOURCE_OT",
                    hb_bucket: "default-simple-pipeline-in_SOURCE_PROCESSORS",
                },
                to_vertex_bucket_config: vec![BucketConfig {
                    vertex: "out",
                    partitions: 1,
                    ot_bucket: "default-simple-pipeline-in-out_OT",
                    hb_bucket: "default-simple-pipeline-in-out_PROCESSORS",
                }],
                idle_config: None,
            })),
            ..Default::default()
        };

        assert_eq!(pipeline_config, expected);
    }

    #[test]
    fn test_map_vertex_config_user_defined() {
        let udf = Udf {
            container: Some(Box::from(Container {
                args: None,
                command: None,
                env: None,
                env_from: None,
                image: None,
                image_pull_policy: None,
                liveness_probe: None,
                ports: None,
                readiness_probe: None,
                resources: None,
                security_context: None,
                volume_mounts: None,
            })),
            group_by: None,
        };

        let map_type = MapType::try_from(Box::new(udf)).unwrap();
        assert!(matches!(map_type, MapType::UserDefined(_)));

        let map_vtx_config = MapVtxConfig {
            concurrency: 10,
            map_type,
            map_mode: MapMode::Unary,
        };

        assert_eq!(map_vtx_config.concurrency, 10);
        let MapType::UserDefined(config) = map_vtx_config.map_type;
        assert_eq!(config.grpc_max_message_size, DEFAULT_GRPC_MAX_MESSAGE_SIZE);
        assert_eq!(config.socket_path, DEFAULT_MAP_SOCKET);
        assert_eq!(config.server_info_path, DEFAULT_MAP_SERVER_INFO_FILE);
    }

    #[test]
    fn test_pipeline_config_load_map_vertex() {
        let pipeline_cfg_base64 = "eyJtZXRhZGF0YSI6eyJuYW1lIjoic2ltcGxlLXBpcGVsaW5lLW1hcCIsIm5hbWVzcGFjZSI6ImRlZmF1bHQiLCJjcmVhdGlvblRpbWVzdGFtcCI6bnVsbH0sInNwZWMiOnsibmFtZSI6Im1hcCIsInVkZiI6eyJjb250YWluZXIiOnsidGVtcGxhdGUiOiJkZWZhdWx0In19LCJsaW1pdHMiOnsicmVhZEJhdGNoU2l6ZSI6NTAwLCJyZWFkVGltZW91dCI6IjFzIiwiYnVmZmVyTWF4TGVuZ3RoIjozMDAwMCwiYnVmZmVyVXNhZ2VMaW1pdCI6ODB9LCJzY2FsZSI6eyJtaW4iOjF9LCJwaXBlbGluZU5hbWUiOiJzaW1wbGUtcGlwZWxpbmUiLCJpbnRlclN0ZXBCdWZmZXJTZXJ2aWNlTmFtZSI6IiIsInJlcGxpY2FzIjowLCJmcm9tRWRnZXMiOlt7ImZyb20iOiJpbiIsInRvIjoibWFwIiwiY29uZGl0aW9ucyI6bnVsbCwiZnJvbVZlcnRleFR5cGUiOiJTb3VyY2UiLCJmcm9tVmVydGV4UGFydGl0aW9uQ291bnQiOjEsImZyb21WZXJ0ZXhMaW1pdHMiOnsicmVhZEJhdGNoU2l6ZSI6NTAwLCJyZWFkVGltZW91dCI6IjFzIiwiYnVmZmVyTWF4TGVuZ3RoIjozMDAwMCwiYnVmZmVyVXNhZ2VMaW1pdCI6ODB9LCJ0b1ZlcnRleFR5cGUiOiJNYXAiLCJ0b1ZlcnRleFBhcnRpdGlvbkNvdW50IjoxLCJ0b1ZlcnRleExpbWl0cyI6eyJyZWFkQmF0Y2hTaXplIjo1MDAsInJlYWRUaW1lb3V0IjoiMXMiLCJidWZmZXJNYXhMZW5ndGgiOjMwMDAwLCJidWZmZXJVc2FnZUxpbWl0Ijo4MH19XSwid2F0ZXJtYXJrIjp7Im1heERlbGF5IjoiMHMifX0sInN0YXR1cyI6eyJwaGFzZSI6IiIsInJlcGxpY2FzIjowLCJkZXNpcmVkUmVwbGljYXMiOjAsImxhc3RTY2FsZWRBdCI6bnVsbH19";

        let env_vars = [("NUMAFLOW_ISBSVC_JETSTREAM_URL", "localhost:4222")];
        let pipeline_config =
            PipelineConfig::load(pipeline_cfg_base64.to_string(), env_vars).unwrap();

        let expected = PipelineConfig {
            pipeline_name: "simple-pipeline",
            vertex_name: "map",
            replica: 0,
            batch_size: 500,
            paf_concurrency: 500,
            read_timeout: Duration::from_secs(1),
            graceful_shutdown_time: Duration::from_secs(DEFAULT_GRACEFUL_SHUTDOWN_TIME_SECS),
            js_client_config: isb::jetstream::ClientConfig {
                url: "localhost:4222".to_string(),
                user: None,
                password: None,
            },
            from_vertex_config: vec![FromVertexConfig {
                name: "in",
                reader_config: BufferReaderConfig {
                    streams: vec![Stream::new("default-simple-pipeline-map-0", "map", 0)],
                    wip_ack_interval: Duration::from_secs(1),
                },
                partitions: 1,
            }],
            to_vertex_config: vec![],
            vertex_type: VertexType::MapUDF,
            vertex_config: VertexConfig::Map(MapVtxConfig {
                concurrency: 500,
                map_type: MapType::UserDefined(UserDefinedConfig {
                    grpc_max_message_size: DEFAULT_GRPC_MAX_MESSAGE_SIZE,
                    socket_path: DEFAULT_MAP_SOCKET.to_string(),
                    server_info_path: DEFAULT_MAP_SERVER_INFO_FILE.to_string(),
                }),
                map_mode: MapMode::Unary,
            }),
            metrics_config: MetricsConfig::default(),
            watermark_config: Some(WatermarkConfig::Edge(EdgeWatermarkConfig {
                from_vertex_config: vec![BucketConfig {
                    vertex: "in",
                    partitions: 1,
                    ot_bucket: "default-simple-pipeline-in-map_OT",
                    hb_bucket: "default-simple-pipeline-in-map_PROCESSORS",
                }],
                to_vertex_config: vec![],
            })),
            ..Default::default()
        };

        assert_eq!(pipeline_config, expected);
    }

    #[test]
    fn test_graceful_timeout_env_var() {
        let pipeline_cfg_base64 = "eyJtZXRhZGF0YSI6eyJuYW1lIjoic2ltcGxlLXBpcGVsaW5lLW1hcCIsIm5hbWVzcGFjZSI6ImRlZmF1bHQiLCJjcmVhdGlvblRpbWVzdGFtcCI6bnVsbH0sInNwZWMiOnsibmFtZSI6Im1hcCIsInVkZiI6eyJjb250YWluZXIiOnsidGVtcGxhdGUiOiJkZWZhdWx0In19LCJsaW1pdHMiOnsicmVhZEJhdGNoU2l6ZSI6NTAwLCJyZWFkVGltZW91dCI6IjFzIiwiYnVmZmVyTWF4TGVuZ3RoIjozMDAwMCwiYnVmZmVyVXNhZ2VMaW1pdCI6ODB9LCJzY2FsZSI6eyJtaW4iOjF9LCJwaXBlbGluZU5hbWUiOiJzaW1wbGUtcGlwZWxpbmUiLCJpbnRlclN0ZXBCdWZmZXJTZXJ2aWNlTmFtZSI6IiIsInJlcGxpY2FzIjowLCJmcm9tRWRnZXMiOlt7ImZyb20iOiJpbiIsInRvIjoibWFwIiwiY29uZGl0aW9ucyI6bnVsbCwiZnJvbVZlcnRleFR5cGUiOiJTb3VyY2UiLCJmcm9tVmVydGV4UGFydGl0aW9uQ291bnQiOjEsImZyb21WZXJ0ZXhMaW1pdHMiOnsicmVhZEJhdGNoU2l6ZSI6NTAwLCJyZWFkVGltZW91dCI6IjFzIiwiYnVmZmVyTWF4TGVuZ3RoIjozMDAwMCwiYnVmZmVyVXNhZ2VMaW1pdCI6ODB9LCJ0b1ZlcnRleFR5cGUiOiJNYXAiLCJ0b1ZlcnRleFBhcnRpdGlvbkNvdW50IjoxLCJ0b1ZlcnRleExpbWl0cyI6eyJyZWFkQmF0Y2hTaXplIjo1MDAsInJlYWRUaW1lb3V0IjoiMXMiLCJidWZmZXJNYXhMZW5ndGgiOjMwMDAwLCJidWZmZXJVc2FnZUxpbWl0Ijo4MH19XSwid2F0ZXJtYXJrIjp7Im1heERlbGF5IjoiMHMifX0sInN0YXR1cyI6eyJwaGFzZSI6IiIsInJlcGxpY2FzIjowLCJkZXNpcmVkUmVwbGljYXMiOjAsImxhc3RTY2FsZWRBdCI6bnVsbH19";

        // Test with custom graceful timeout
        let env_vars = [
            ("NUMAFLOW_ISBSVC_JETSTREAM_URL", "localhost:4222"),
            ("NUMAFLOW_GRACEFUL_TIMEOUT_SECS", "30"),
        ];
        let pipeline_config =
            PipelineConfig::load(pipeline_cfg_base64.to_string(), env_vars).unwrap();

        assert_eq!(
            pipeline_config.graceful_shutdown_time,
            Duration::from_secs(30)
        );

        // Test with default graceful timeout (no env var)
        let env_vars = [("NUMAFLOW_ISBSVC_JETSTREAM_URL", "localhost:4222")];
        let pipeline_config =
            PipelineConfig::load(pipeline_cfg_base64.to_string(), env_vars).unwrap();

        assert_eq!(
            pipeline_config.graceful_shutdown_time,
            Duration::from_secs(DEFAULT_GRACEFUL_SHUTDOWN_TIME_SECS)
        );
    }
}
