//! Construction of a [`PipelineConfig`] (and the per-kind vertex/edge/watermark sub-configs) from
//! a [`LocalUdf`]. This owns every config literal so the runner stays lifecycle-only.
//!
//! The topology is fixed for every run: a single input vertex `nfcli-in` feeding the UDF vertex
//! `nfcli-udf`, which (except for terminal sinks) writes to a single output vertex `nfcli-out`.
//! Identity names that must be `&'static str` are module consts.

use std::time::Duration;

use crate::config::components::metrics::MetricsConfig;
use crate::config::components::reduce::{
    AccumulatorWindowConfig, UserDefinedConfig as ReduceUdConfig,
};
use crate::config::components::reduce::{
    AlignedReducerConfig, AlignedWindowConfig, AlignedWindowType, FixedWindowConfig, ReducerConfig,
    SessionWindowConfig, SlidingWindowConfig, UnalignedReducerConfig, UnalignedWindowConfig,
    UnalignedWindowType,
};
use crate::config::components::sink::{SinkConfig, SinkType, UserDefinedConfig as SinkUdConfig};
use crate::config::components::source::{
    SourceConfig, SourceType, UserDefinedConfig as SourceUdConfig,
};
use crate::config::components::transformer::{
    TransformerConfig, TransformerType, UserDefinedConfig as TransformerUdConfig,
};
use crate::config::pipeline::isb::{
    BufferReaderConfig, BufferWriterConfig, ISBClientConfig, Stream,
};
use crate::config::pipeline::map::{MapType, MapVtxConfig, UserDefinedConfig as MapUdConfig};
use crate::config::pipeline::watermark::{BucketConfig, EdgeWatermarkConfig, WatermarkConfig};
use crate::config::pipeline::{
    FromVertexConfig, PipelineConfig, ReduceVtxConfig, SinkVtxConfig, SourceVtxConfig,
    ToVertexConfig, VertexConfig, VertexType,
};
use crate::local::{LocalUdf, LocalWindow};

// Fixed identity literals. `Stream`, `FromVertexConfig.name`, and `BucketConfig` fields all
// require `&'static str`, so these are consts rather than per-run formatted strings (G6).
pub(crate) const PIPELINE_NAME: &str = "nfcli";
pub(crate) const INPUT_VERTEX: &str = "nfcli-in";
pub(crate) const UDF_VERTEX: &str = "nfcli-udf";
pub(crate) const OUTPUT_VERTEX: &str = "nfcli-out";
pub(crate) const INPUT_STREAM: &str = "nfcli-in-0";
pub(crate) const OUTPUT_STREAM: &str = "nfcli-out-0";
pub(crate) const INPUT_OT_BUCKET: &str = "nfcli-in-ot";
pub(crate) const OUTPUT_OT_BUCKET: &str = "nfcli-out-ot";

/// Discriminates which per-type forwarder the runner should spawn. Derived from the `LocalUdf`
/// variant once during `start` so the runner does not have to keep the whole `LocalUdf` around.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RunKind {
    Map,
    Sink,
    Reduce,
    Transform,
    Source,
}

/// Knobs that influence config construction. A subset of `LocalRunOpts`, passed explicitly so this
/// module does not depend on the runner (avoids a module cycle).
#[derive(Debug, Clone, Copy)]
pub(crate) struct BuildOpts {
    pub batch_size: usize,
    pub buffer_capacity: usize,
    pub grpc_max_message_size: usize,
    pub graceful_shutdown: Duration,
}

/// The input reader edge (`nfcli-in`), shared by Map/Sink/Reduce (kinds that consume the input
/// buffer). Transform/Source have no from-vertex.
fn input_from_vertex() -> FromVertexConfig {
    FromVertexConfig {
        name: INPUT_VERTEX,
        reader_config: BufferReaderConfig {
            streams: vec![Stream::new(INPUT_STREAM, INPUT_VERTEX, 0)],
            ..Default::default()
        },
        partitions: 1,
    }
}

/// The output writer edge (`nfcli-out`), shared by every non-terminal kind. `to_vertex_type: Sink`
/// makes the output vertex a terminal collector, exactly as the template test does.
fn output_to_vertex(opts: &BuildOpts) -> ToVertexConfig {
    ToVertexConfig {
        name: OUTPUT_VERTEX,
        partitions: 1,
        writer_config: BufferWriterConfig {
            streams: vec![Stream::new(OUTPUT_STREAM, OUTPUT_VERTEX, 0)],
            max_length: opts.buffer_capacity,
            ..Default::default()
        },
        conditions: None,
        to_vertex_type: VertexType::Sink,
        ordered_processing_enabled: false,
    }
}

/// The common `PipelineConfig` skeleton; per-kind builders fill in the vertex/edge/watermark
/// pieces. Mirrors the reduce-forwarder template test's literal.
fn base_config(opts: &BuildOpts) -> PipelineConfig {
    PipelineConfig {
        pipeline_name: PIPELINE_NAME,
        vertex_name: UDF_VERTEX,
        replica: 0,
        batch_size: opts.batch_size,
        concurrency: opts.batch_size,
        // Small read timeout keeps drain detection snappy (the reader wakes often to notice the
        // buffer has drained).
        read_timeout: Duration::from_millis(100),
        graceful_shutdown_time: opts.graceful_shutdown,
        isb_client_config: ISBClientConfig::InMemory,
        from_vertex_config: vec![],
        to_vertex_config: vec![],
        // Overwritten per kind below.
        vertex_config: VertexConfig::Map(MapVtxConfig {
            concurrency: opts.batch_size,
            map_type: MapType::UserDefined(MapUdConfig {
                grpc_max_message_size: opts.grpc_max_message_size,
                socket_path: String::new(),
                server_info_path: String::new(),
            }),
        }),
        vertex_type: VertexType::MapUDF,
        // Port 0 → OS-assigned; metrics server is fire-and-forget and must not squat 2469 (G5).
        metrics_config: MetricsConfig {
            metrics_server_listen_port: 0,
            ..Default::default()
        },
        watermark_config: None,
        // ..Default::default() covers callback_config / isb_config / rate_limit = None,
        // writer_concurrency, ordered_processing_enabled = false.
        ..Default::default()
    }
}

/// Build the full `PipelineConfig` for the given UDF. Returns the config plus the derived
/// [`RunKind`] the runner uses to pick the forwarder entry point.
pub(crate) fn build(udf: &LocalUdf, opts: &BuildOpts) -> (PipelineConfig, RunKind) {
    match udf {
        LocalUdf::Map {
            socket_path,
            server_info_path,
        } => {
            let mut config = base_config(opts);
            config.vertex_type = VertexType::MapUDF;
            config.vertex_config = VertexConfig::Map(MapVtxConfig {
                concurrency: opts.batch_size,
                map_type: MapType::UserDefined(MapUdConfig {
                    grpc_max_message_size: opts.grpc_max_message_size,
                    socket_path: path_string(socket_path),
                    server_info_path: path_string(server_info_path),
                }),
            });
            config.from_vertex_config = vec![input_from_vertex()];
            config.to_vertex_config = vec![output_to_vertex(opts)];
            (config, RunKind::Map)
        }

        LocalUdf::Sink {
            socket_path,
            server_info_path,
            fallback,
            on_success,
        } => {
            let mut config = base_config(opts);
            config.vertex_type = VertexType::Sink;
            config.vertex_config = VertexConfig::Sink(Box::new(SinkVtxConfig {
                sink_config: user_defined_sink(socket_path, server_info_path, opts),
                fb_sink_config: fallback
                    .as_ref()
                    .map(|(s, i)| user_defined_sink(s, i, opts)),
                on_success_sink_config: on_success
                    .as_ref()
                    .map(|(s, i)| user_defined_sink(s, i, opts)),
                serving_store_config: None,
            }));
            config.from_vertex_config = vec![input_from_vertex()];
            // Sink is terminal: no output edge.
            config.to_vertex_config = vec![];
            (config, RunKind::Sink)
        }

        LocalUdf::Reduce {
            socket_path,
            server_info_path,
            window,
            keyed,
            allowed_lateness,
        } => {
            let mut config = base_config(opts);
            config.vertex_type = VertexType::ReduceUDF;
            config.vertex_config = VertexConfig::Reduce(reduce_vtx_config(
                socket_path,
                server_info_path,
                *window,
                *keyed,
                *allowed_lateness,
                opts,
            ));
            config.from_vertex_config = vec![input_from_vertex()];
            config.to_vertex_config = vec![output_to_vertex(opts)];
            // Reduce requires watermarks to close windows. The input/output OT buckets are served
            // by the factory's in-memory KV stores; the watermark driver publishes into the input
            // bucket (playing the upstream vertex).
            config.watermark_config = Some(WatermarkConfig::Edge(EdgeWatermarkConfig {
                from_vertex_config: vec![input_bucket_config()],
                to_vertex_config: vec![output_bucket_config()],
            }));
            (config, RunKind::Reduce)
        }

        LocalUdf::Transform {
            socket_path,
            server_info_path,
        } => {
            // Transformers only run inside the source forwarder, so the topology is a CLI-owned
            // replay source + the user's transformer → output buffer. The replay source's socket
            // is patched in by the runner once the replay server is up (its path is not known
            // here); we leave placeholders that the runner overwrites.
            let mut config = base_config(opts);
            config.vertex_type = VertexType::Source;
            config.vertex_config = VertexConfig::Source(SourceVtxConfig {
                source_config: SourceConfig {
                    read_ahead: false,
                    // Placeholder; the runner rewrites this to the replay source's real socket.
                    source_type: SourceType::UserDefined(SourceUdConfig::default()),
                },
                transformer_config: Some(TransformerConfig {
                    concurrency: opts.batch_size,
                    transformer_type: TransformerType::UserDefined(TransformerUdConfig {
                        grpc_max_message_size: opts.grpc_max_message_size,
                        socket_path: path_string(socket_path),
                        server_info_path: path_string(server_info_path),
                    }),
                }),
            });
            config.from_vertex_config = vec![];
            config.to_vertex_config = vec![output_to_vertex(opts)];
            (config, RunKind::Transform)
        }

        LocalUdf::Source {
            socket_path,
            server_info_path,
        } => {
            let mut config = base_config(opts);
            config.vertex_type = VertexType::Source;
            config.vertex_config = VertexConfig::Source(SourceVtxConfig {
                source_config: SourceConfig {
                    read_ahead: false,
                    source_type: SourceType::UserDefined(SourceUdConfig {
                        grpc_max_message_size: opts.grpc_max_message_size,
                        socket_path: path_string(socket_path),
                        server_info_path: path_string(server_info_path),
                    }),
                },
                transformer_config: None,
            });
            config.from_vertex_config = vec![];
            config.to_vertex_config = vec![output_to_vertex(opts)];
            (config, RunKind::Source)
        }
    }
}

/// A `SinkConfig` pointing at a user-defined sink over the given socket/server-info.
fn user_defined_sink(
    socket: &std::path::Path,
    server_info: &std::path::Path,
    opts: &BuildOpts,
) -> SinkConfig {
    SinkConfig {
        sink_type: SinkType::UserDefined(SinkUdConfig {
            grpc_max_message_size: opts.grpc_max_message_size,
            socket_path: path_string(socket),
            server_info_path: path_string(server_info),
        }),
        // None → the production RetryConfig default (RetryUntilSuccess) is used downstream.
        retry_config: None,
    }
}

/// Build the reduce vertex config, translating the public [`LocalWindow`] to the internal
/// aligned/unaligned window configs. Reduce's `UserDefinedConfig` fields are `&'static str`, so
/// the socket/server-info paths are `Box::leak`ed once per run (bounded — G4).
fn reduce_vtx_config(
    socket: &std::path::Path,
    server_info: &std::path::Path,
    window: LocalWindow,
    keyed: bool,
    allowed_lateness: Duration,
    opts: &BuildOpts,
) -> ReduceVtxConfig {
    let user_defined_config = ReduceUdConfig {
        grpc_max_message_size: opts.grpc_max_message_size,
        socket_path: leak_path(socket),
        server_info_path: leak_path(server_info),
    };

    let reducer_config = match window {
        LocalWindow::Fixed { length } => ReducerConfig::Aligned(AlignedReducerConfig {
            user_defined_config,
            window_config: AlignedWindowConfig {
                window_type: AlignedWindowType::Fixed(FixedWindowConfig {
                    length,
                    streaming: false,
                }),
                allowed_lateness,
                is_keyed: keyed,
            },
        }),
        LocalWindow::Sliding { length, slide } => ReducerConfig::Aligned(AlignedReducerConfig {
            user_defined_config,
            window_config: AlignedWindowConfig {
                window_type: AlignedWindowType::Sliding(SlidingWindowConfig {
                    length,
                    slide,
                    streaming: false,
                }),
                allowed_lateness,
                is_keyed: keyed,
            },
        }),
        LocalWindow::Session { gap } => ReducerConfig::Unaligned(UnalignedReducerConfig {
            user_defined_config,
            window_config: UnalignedWindowConfig {
                window_type: UnalignedWindowType::Session(SessionWindowConfig { timeout: gap }),
                allowed_lateness,
                is_keyed: keyed,
            },
        }),
        LocalWindow::Accumulator { timeout } => ReducerConfig::Unaligned(UnalignedReducerConfig {
            user_defined_config,
            window_config: UnalignedWindowConfig {
                window_type: UnalignedWindowType::Accumulator(AccumulatorWindowConfig { timeout }),
                allowed_lateness,
                is_keyed: keyed,
            },
        }),
    };

    ReduceVtxConfig {
        keyed,
        reducer_config,
        // No WAL/fencing — crash-replay is platform behavior, not UDF behavior (design §9.5).
        wal_storage_config: None,
    }
}

/// The input-edge OT bucket the watermark driver publishes into.
pub(crate) fn input_bucket_config() -> BucketConfig {
    BucketConfig {
        vertex: INPUT_VERTEX,
        partitions: vec![0],
        ot_bucket: INPUT_OT_BUCKET,
        delay: None,
    }
}

/// The output-edge OT bucket the reduce forwarder publishes into.
fn output_bucket_config() -> BucketConfig {
    BucketConfig {
        vertex: OUTPUT_VERTEX,
        partitions: vec![0],
        ot_bucket: OUTPUT_OT_BUCKET,
        delay: None,
    }
}

/// A `Path` → owned `String`, lossily (paths under test/`/var/run/numaflow` are always UTF-8).
fn path_string(p: &std::path::Path) -> String {
    p.to_string_lossy().into_owned()
}

/// A `Path` → `&'static str` via a one-time leak. Only used for the reduce config's
/// `&'static str` socket/server-info fields; bounded to once per run (G4).
fn leak_path(p: &std::path::Path) -> &'static str {
    Box::leak(path_string(p).into_boxed_str())
}
