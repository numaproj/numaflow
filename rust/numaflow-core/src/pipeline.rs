use std::time::Duration;

use async_nats::jetstream::Context;
use async_nats::{ConnectOptions, jetstream};
use futures::future::try_join_all;
use serving::callback::CallbackHandler;
use tokio_util::sync::CancellationToken;
use tracing::info;

use crate::config::components::reduce::WindowType;
use crate::config::is_mono_vertex;
use crate::config::pipeline;
use crate::config::pipeline::isb::Stream;
use crate::config::pipeline::map::MapVtxConfig;
use crate::config::pipeline::watermark::WatermarkConfig;
use crate::config::pipeline::{
    PipelineConfig, ReduceVtxConfig, ServingStoreType, SinkVtxConfig, SourceVtxConfig,
};
use crate::metrics::{ComponentHealthChecks, LagReader, PendingReaderTasks, PipelineComponents};
use crate::pipeline::forwarder::reduce_forwarder::ReduceForwarder;
use crate::pipeline::forwarder::source_forwarder;
use crate::pipeline::isb::jetstream::reader::JetStreamReader;
use crate::pipeline::isb::jetstream::writer::JetstreamWriter;
use crate::pipeline::pipeline::isb::BufferReaderConfig;
use crate::reduce::pbq::PBQBuilder;
use crate::reduce::reducer::aligned::reducer::AlignedReducer;
use crate::reduce::reducer::aligned::windower::WindowManager;
use crate::reduce::reducer::aligned::windower::fixed::FixedWindowManager;
use crate::reduce::reducer::aligned::windower::sliding::SlidingWindowManager;
use crate::reduce::wal::segment::WalType;
use crate::reduce::wal::segment::append::AppendOnlyWal;
use crate::reduce::wal::segment::compactor::{Compactor, WindowKind};
use crate::shared::create_components;
use crate::shared::metrics::start_metrics_server;
use crate::sink::serve::ServingStore;
use crate::sink::serve::nats::NatsServingStore;
use crate::sink::serve::user_defined::UserDefinedStore;
use crate::tracker::TrackerHandle;
use crate::watermark::WatermarkHandle;
use crate::watermark::isb::ISBWatermarkHandle;
use crate::watermark::source::SourceWatermarkHandle;
use crate::{Result, error, shared};

mod forwarder;
pub(crate) mod isb;

/// Starts the appropriate forwarder based on the pipeline configuration.
pub(crate) async fn start_forwarder(
    cln_token: CancellationToken,
    config: PipelineConfig,
) -> Result<()> {
    let js_context = create_js_context(config.js_client_config.clone()).await?;

    match &config.vertex_type_config {
        pipeline::VertexType::Source(source) => {
            info!("Starting source forwarder");

            // create watermark handle, if watermark is enabled
            let source_watermark_handle = match &config.watermark_config {
                Some(WatermarkConfig::Source(source_config)) => Some(
                    SourceWatermarkHandle::new(
                        config.read_timeout,
                        js_context.clone(),
                        &config.to_vertex_config,
                        source_config,
                        cln_token.clone(),
                    )
                    .await?,
                ),
                _ => None,
            };

            start_source_forwarder(
                cln_token,
                js_context,
                config.clone(),
                source.clone(),
                source_watermark_handle,
            )
            .await?;
        }
        pipeline::VertexType::Sink(sink) => {
            info!("Starting sink forwarder");
            start_sink_forwarder(cln_token, js_context, config.clone(), sink.clone()).await?;
        }
        pipeline::VertexType::Map(map) => {
            info!("Starting map forwarder");
            start_map_forwarder(cln_token, js_context, config.clone(), map.clone()).await?;
        }
        pipeline::VertexType::Reduce(reduce) => {
            info!("Starting reduce forwarder");
            start_reduce_forwarder(cln_token, js_context, config.clone(), reduce.clone()).await?;
        }
    }
    Ok(())
}

async fn start_source_forwarder(
    cln_token: CancellationToken,
    js_context: Context,
    config: PipelineConfig,
    source_config: SourceVtxConfig,
    source_watermark_handle: Option<SourceWatermarkHandle>,
) -> Result<()> {
    let serving_callback_handler = if let Some(cb_cfg) = &config.callback_config {
        Some(
            CallbackHandler::new(
                config.vertex_name.to_string(),
                js_context.clone(),
                cb_cfg.callback_store,
                cb_cfg.callback_concurrency,
            )
            .await,
        )
    } else {
        None
    };
    let tracker_handle = TrackerHandle::new(None, serving_callback_handler);

    let buffer_writer = create_buffer_writer(
        &config,
        js_context.clone(),
        tracker_handle.clone(),
        cln_token.clone(),
        source_watermark_handle.clone().map(WatermarkHandle::Source),
        config.vertex_type_config.to_string(),
    )
    .await;

    let transformer = create_components::create_transformer(
        config.batch_size,
        source_config.transformer_config.clone(),
        tracker_handle.clone(),
        cln_token.clone(),
    )
    .await?;

    let source = create_components::create_source(
        config.batch_size,
        config.read_timeout,
        &source_config.source_config,
        tracker_handle,
        transformer,
        source_watermark_handle,
        cln_token.clone(),
    )
    .await?;

    // only check the pending and lag for source for pod_id = 0
    let _pending_reader_handle: Option<PendingReaderTasks> = if config.replica == 0 {
        let pending_reader = shared::metrics::create_pending_reader(
            &config.metrics_config,
            LagReader::Source(source.clone()),
        )
        .await;
        Some(pending_reader.start(is_mono_vertex()).await)
    } else {
        None
    };

    start_metrics_server(
        config.metrics_config.clone(),
        ComponentHealthChecks::Pipeline(PipelineComponents::Source(source.clone())),
    )
    .await;

    let forwarder = source_forwarder::SourceForwarder::new(source, buffer_writer);

    forwarder.start(cln_token).await?;
    Ok(())
}

async fn start_map_forwarder(
    cln_token: CancellationToken,
    js_context: Context,
    config: PipelineConfig,
    map_vtx_config: MapVtxConfig,
) -> Result<()> {
    // create watermark handle, if watermark is enabled
    let watermark_handle =
        create_components::create_edge_watermark_handle(&config, &js_context, &cln_token, None)
            .await?;

    // Only the reader config of the first "from" vertex is needed, as all "from" vertices currently write
    // to a common buffer, in the case of a join.
    let reader_config = &config
        .from_vertex_config
        .first()
        .ok_or_else(|| error::Error::Config("No from vertex config found".to_string()))?
        .reader_config;

    // Create buffer writers and buffer readers
    let mut forwarder_components = vec![];
    let mut mapper_handle = None;
    let mut isb_lag_readers = vec![];

    let serving_callback_handler = if let Some(cb_cfg) = &config.callback_config {
        Some(
            CallbackHandler::new(
                config.vertex_name.to_string(),
                js_context.clone(),
                cb_cfg.callback_store,
                cb_cfg.callback_concurrency,
            )
            .await,
        )
    } else {
        None
    };

    // create tracker and buffer writer, they can be shared across all forwarders
    let tracker_handle =
        TrackerHandle::new(watermark_handle.clone(), serving_callback_handler.clone());

    let buffer_writer = create_buffer_writer(
        &config,
        js_context.clone(),
        tracker_handle.clone(),
        cln_token.clone(),
        watermark_handle.clone().map(WatermarkHandle::ISB),
        config.vertex_type_config.to_string(),
    )
    .await;

    for stream in reader_config.streams.clone() {
        info!("Creating buffer reader for stream {:?}", stream);
        let buffer_reader = create_buffer_reader(
            config.vertex_type_config.to_string(),
            stream,
            reader_config.clone(),
            js_context.clone(),
            tracker_handle.clone(),
            config.batch_size,
            watermark_handle.clone(),
        )
        .await?;

        isb_lag_readers.push(buffer_reader.clone());
        let mapper = create_components::create_mapper(
            config.batch_size,
            config.read_timeout,
            map_vtx_config.clone(),
            tracker_handle.clone(),
            cln_token.clone(),
        )
        .await?;

        if mapper_handle.is_none() {
            mapper_handle = Some(mapper.clone());
        }

        forwarder_components.push((buffer_reader, buffer_writer.clone(), mapper));
    }

    let pending_reader = shared::metrics::create_pending_reader(
        &config.metrics_config,
        LagReader::ISB(isb_lag_readers),
    )
    .await;
    let _pending_reader_handle = pending_reader.start(is_mono_vertex()).await;

    let metrics_server_handle = start_metrics_server(
        config.metrics_config.clone(),
        ComponentHealthChecks::Pipeline(PipelineComponents::Map(mapper_handle.unwrap().clone())),
    )
    .await;

    let mut forwarder_tasks = vec![];
    for (buffer_reader, buffer_writer, mapper) in forwarder_components {
        info!(%buffer_reader, "Starting forwarder for buffer reader");
        let forwarder =
            forwarder::map_forwarder::MapForwarder::new(buffer_reader, mapper, buffer_writer).await;
        let task = tokio::spawn({
            let cln_token = cln_token.clone();
            async move { forwarder.start(cln_token.clone()).await }
        });
        forwarder_tasks.push(task);
    }

    let results = try_join_all(forwarder_tasks)
        .await
        .map_err(|e| error::Error::Forwarder(e.to_string()))?;

    for result in results {
        error!(?result, "Forwarder task failed");
        result?;
    }

    // abort the metrics server
    metrics_server_handle.abort();

    info!("All forwarders have stopped successfully");
    Ok(())
}

async fn start_reduce_forwarder(
    cln_token: CancellationToken,
    js_context: Context,
    config: PipelineConfig,
    reduce_vtx_config: ReduceVtxConfig,
) -> Result<()> {
    let window_manager = match &reduce_vtx_config.reducer_config.window_config.window_type {
        WindowType::Fixed(fixed_config) => {
            WindowManager::Fixed(FixedWindowManager::new(fixed_config.length))
        }
        WindowType::Sliding(sliding_config) => {
            // sliding window needs to save state if WAL is configured to avoid duplicate processing
            // since a message can be part of multiple windows.
            let state_file_path =
                if let Some(storage_config) = &reduce_vtx_config.wal_storage_config {
                    let mut path = storage_config.path.clone();
                    path.push(format!("{}-window.state", config.vertex_name));
                    Some(path)
                } else {
                    None
                };

            WindowManager::Sliding(SlidingWindowManager::new(
                sliding_config.length,
                sliding_config.slide,
                state_file_path,
            ))
        }
        WindowType::Session(_) | WindowType::Accumulator(_) => {
            panic!("Session and Accumulator windows are not supported yet");
        }
    };

    // create watermark handle, if watermark is enabled
    let watermark_handle = create_components::create_edge_watermark_handle(
        &config,
        &js_context,
        &cln_token,
        Some(window_manager.clone()),
    )
    .await?;

    let reader_config = &config
        .from_vertex_config
        .first()
        .ok_or_else(|| error::Error::Config("No from vertex config found".to_string()))?
        .reader_config;

    // reduce pod always reads from a single stream (pod per partition)
    let stream = reader_config
        .streams
        .first()
        .cloned()
        .ok_or_else(|| error::Error::Config("No stream found for reduce vertex".to_string()))?;

    // we don't need to pass the watermark handle to the tracker because in reduce windower is
    // responsible for identifying the lowest watermark in the pod.
    let tracker_handle = TrackerHandle::new(None, None);
    // Create buffer reader
    let buffer_reader = create_buffer_reader(
        config.vertex_type_config.to_string(),
        stream,
        reader_config.clone(),
        js_context.clone(),
        tracker_handle.clone(),
        config.batch_size,
        watermark_handle.clone(),
    )
    .await?;

    // Create buffer writer
    let buffer_writer = create_buffer_writer(
        &config,
        js_context.clone(),
        tracker_handle.clone(),
        cln_token.clone(),
        watermark_handle.clone().map(WatermarkHandle::ISB),
        config.vertex_type_config.to_string(),
    )
    .await;

    // Create user-defined aligned reducer client
    let reducer_client =
        create_components::create_aligned_reducer(reduce_vtx_config.reducer_config.clone()).await?;

    // Create WAL if configured
    let (wal, gc_wal) = if let Some(storage_config) = &reduce_vtx_config.wal_storage_config {
        let wal_path = storage_config.path.clone();

        let append_only_wal = AppendOnlyWal::new(
            WalType::Data,
            wal_path.clone(),
            storage_config.max_file_size_mb,
            storage_config.flush_interval_ms,
            storage_config.channel_buffer_size,
            storage_config.max_segment_age_secs,
        )
        .await?;

        let compactor = Compactor::new(
            wal_path.clone(),
            WindowKind::Aligned,
            storage_config.max_file_size_mb,
            storage_config.flush_interval_ms,
            storage_config.channel_buffer_size,
            storage_config.max_segment_age_secs,
        )
        .await?;

        let gc_wal = AppendOnlyWal::new(
            WalType::Gc,
            wal_path,
            storage_config.max_file_size_mb,
            storage_config.flush_interval_ms,
            storage_config.channel_buffer_size,
            storage_config.max_segment_age_secs,
        )
        .await?;

        (
            Some(crate::reduce::pbq::WAL {
                append_only_wal,
                compactor,
            }),
            Some(gc_wal),
        )
    } else {
        (None, None)
    };

    // Create PBQ
    let pbq_builder = PBQBuilder::new(buffer_reader.clone(), tracker_handle.clone());
    let pbq = if let Some(wal) = wal {
        pbq_builder.wal(wal).build()
    } else {
        pbq_builder.build()
    };

    let pending_reader = shared::metrics::create_pending_reader(
        &config.metrics_config,
        LagReader::ISB(vec![buffer_reader]),
    )
    .await;
    let _pending_reader_handle = pending_reader.start(is_mono_vertex()).await;

    // Start the metrics server with one of the clients
    start_metrics_server(
        config.metrics_config.clone(),
        ComponentHealthChecks::Pipeline(PipelineComponents::Reduce(reducer_client.clone())),
    )
    .await;

    let reducer = AlignedReducer::new(reducer_client, window_manager, buffer_writer, gc_wal).await;
    let forwarder = ReduceForwarder::new(pbq, reducer);
    forwarder.start(cln_token).await?;

    info!("Reduce forwarder has stopped successfully");
    Ok(())
}

async fn start_sink_forwarder(
    cln_token: CancellationToken,
    js_context: Context,
    config: PipelineConfig,
    sink: SinkVtxConfig,
) -> Result<()> {
    // create watermark handle, if watermark is enabled
    let watermark_handle =
        create_components::create_edge_watermark_handle(&config, &js_context, &cln_token, None)
            .await?;

    // Only the reader config of the first "from" vertex is needed, as all "from" vertices currently write
    // to a common buffer, in the case of a join.
    let reader_config = &config
        .from_vertex_config
        .first()
        .ok_or_else(|| error::Error::Config("No from vertex config found".to_string()))?
        .reader_config;

    let serving_callback_handler = if let Some(cb_cfg) = &config.callback_config {
        Some(
            CallbackHandler::new(
                config.vertex_name.to_string(),
                js_context.clone(),
                cb_cfg.callback_store,
                cb_cfg.callback_concurrency,
            )
            .await,
        )
    } else {
        None
    };

    let serving_store = match &sink.serving_store_config {
        Some(serving_store_config) => match serving_store_config {
            ServingStoreType::UserDefined(config) => {
                let serving_store = UserDefinedStore::new(config.clone()).await?;
                Some(ServingStore::UserDefined(serving_store))
            }
            ServingStoreType::Nats(config) => {
                let serving_store =
                    NatsServingStore::new(js_context.clone(), config.clone()).await?;
                Some(ServingStore::Nats(serving_store))
            }
        },
        None => None,
    };

    // Create sink writers and buffer readers for each stream
    let mut sink_writers = vec![];
    let mut buffer_readers = vec![];
    for stream in reader_config.streams.clone() {
        let tracker_handle =
            TrackerHandle::new(watermark_handle.clone(), serving_callback_handler.clone());

        let buffer_reader = create_buffer_reader(
            config.vertex_type_config.to_string(),
            stream,
            reader_config.clone(),
            js_context.clone(),
            tracker_handle.clone(),
            config.batch_size,
            watermark_handle.clone(),
        )
        .await?;
        buffer_readers.push(buffer_reader);

        let sink_writer = create_components::create_sink_writer(
            config.batch_size,
            config.read_timeout,
            sink.sink_config.clone(),
            sink.fb_sink_config.clone(),
            tracker_handle,
            serving_store.clone(),
            &cln_token,
        )
        .await?;
        sink_writers.push(sink_writer);
    }

    let pending_reader = shared::metrics::create_pending_reader(
        &config.metrics_config,
        LagReader::ISB(buffer_readers.clone()),
    )
    .await;
    let _pending_reader_handle = pending_reader.start(is_mono_vertex()).await;

    // Start the metrics server with one of the clients
    if let Some(sink_handle) = sink_writers.first() {
        start_metrics_server(
            config.metrics_config.clone(),
            ComponentHealthChecks::Pipeline(PipelineComponents::Sink(sink_handle.clone())),
        )
        .await;
    }

    // Start a new forwarder for each buffer reader
    let mut forwarder_tasks = Vec::new();
    for (buffer_reader, sink_writer) in buffer_readers.into_iter().zip(sink_writers) {
        info!(%buffer_reader, "Starting forwarder for buffer reader");
        let forwarder =
            forwarder::sink_forwarder::SinkForwarder::new(buffer_reader, sink_writer).await;

        let task = tokio::spawn({
            let cln_token = cln_token.clone();
            async move { forwarder.start(cln_token.clone()).await }
        });
        forwarder_tasks.push(task);
    }

    let results = try_join_all(forwarder_tasks)
        .await
        .map_err(|e| error::Error::Forwarder(e.to_string()))?;

    for result in results {
        error!(?result, "Forwarder task failed");
        result?;
    }

    info!("All forwarders have stopped successfully");
    Ok(())
}

async fn create_buffer_writer(
    config: &PipelineConfig,
    js_context: Context,
    tracker_handle: TrackerHandle,
    cln_token: CancellationToken,
    watermark_handle: Option<WatermarkHandle>,
    vertex_type: String,
) -> JetstreamWriter {
    JetstreamWriter::new(
        config.to_vertex_config.clone(),
        js_context,
        config.paf_concurrency,
        tracker_handle,
        cln_token,
        watermark_handle,
        vertex_type,
    )
}

async fn create_buffer_reader(
    vertex_type: String,
    stream: Stream,
    reader_config: BufferReaderConfig,
    js_context: Context,
    tracker_handle: TrackerHandle,
    batch_size: usize,
    watermark_handle: Option<ISBWatermarkHandle>,
) -> Result<JetStreamReader> {
    JetStreamReader::new(
        vertex_type,
        stream,
        js_context,
        reader_config,
        tracker_handle,
        batch_size,
        watermark_handle,
    )
    .await
}

/// Creates a jetstream context based on the provided configuration
async fn create_js_context(config: pipeline::isb::jetstream::ClientConfig) -> Result<Context> {
    // TODO: make these configurable. today this is hardcoded on Golang code too.
    let mut opts = ConnectOptions::new()
        .max_reconnects(None) // unlimited reconnects
        .ping_interval(Duration::from_secs(3))
        .retry_on_initial_connect();

    if let (Some(user), Some(password)) = (config.user, config.password) {
        opts = opts.user_and_password(user, password);
    }

    let js_client = async_nats::connect_with_options(&config.url, opts)
        .await
        .map_err(|e| error::Error::Connection(e.to_string()))?;

    Ok(jetstream::new(js_client))
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::time::Duration;

    use async_nats::jetstream;
    use async_nats::jetstream::{consumer, stream};
    use numaflow::map;
    use tempfile::TempDir;
    use tokio_stream::StreamExt;

    use super::*;
    use crate::config::components::metrics::MetricsConfig;
    use crate::config::components::sink::{BlackholeConfig, SinkConfig, SinkType};
    use crate::config::components::source::GeneratorConfig;
    use crate::config::components::source::SourceConfig;
    use crate::config::components::source::SourceType;
    use crate::config::pipeline::PipelineConfig;
    use crate::config::pipeline::map::{MapType, UserDefinedConfig};
    use crate::pipeline::pipeline::VertexType;
    use crate::pipeline::pipeline::isb;
    use crate::pipeline::pipeline::isb::{BufferReaderConfig, BufferWriterConfig};
    use crate::pipeline::pipeline::map::MapMode;
    use crate::pipeline::pipeline::{FromVertexConfig, ToVertexConfig};
    use crate::pipeline::pipeline::{SinkVtxConfig, SourceVtxConfig};
    use crate::pipeline::tests::isb::BufferFullStrategy::RetryUntilSuccess;

    // e2e test for source forwarder, reads from generator and writes to
    // multi-partitioned buffer.
    #[cfg(feature = "nats-tests")]
    #[tokio::test]
    async fn test_forwarder_for_source_vertex() {
        // Unique names for the streams we use in this test
        let streams = vec![
            Stream::new("default-test-forwarder-for-source-vertex-out-0", "test", 0),
            Stream::new("default-test-forwarder-for-source-vertex-out-1", "test", 1),
            Stream::new("default-test-forwarder-for-source-vertex-out-2", "test", 2),
            Stream::new("default-test-forwarder-for-source-vertex-out-3", "test", 3),
            Stream::new("default-test-forwarder-for-source-vertex-out-4", "test", 4),
        ];

        let js_url = "localhost:4222";
        let client = async_nats::connect(js_url).await.unwrap();
        let context = jetstream::new(client);

        let mut consumers = vec![];
        // Create streams to which the generator source vertex we create later will forward
        // messages to. The consumers created for the corresponding streams will be used to ensure
        // that messages were actually written to the streams.
        for stream in &streams {
            // Delete stream if it exists
            let _ = context.delete_stream(stream.name).await;
            let _stream = context
                .get_or_create_stream(stream::Config {
                    name: stream.name.to_string(),
                    subjects: vec![stream.name.into()],
                    max_message_size: 64 * 1024,
                    max_messages: 10000,
                    ..Default::default()
                })
                .await
                .unwrap();

            let c: consumer::PullConsumer = context
                .create_consumer_on_stream(
                    consumer::pull::Config {
                        name: Some(stream.to_string()),
                        ack_policy: consumer::AckPolicy::Explicit,
                        ..Default::default()
                    },
                    stream.name,
                )
                .await
                .unwrap();
            consumers.push((stream.to_string(), c));
        }

        let pipeline_config = PipelineConfig {
            pipeline_name: "simple-pipeline",
            vertex_name: "in",
            replica: 0,
            batch_size: 1000,
            paf_concurrency: 30000,
            read_timeout: Duration::from_secs(1),
            js_client_config: isb::jetstream::ClientConfig {
                url: "localhost:4222".to_string(),
                user: None,
                password: None,
            },
            from_vertex_config: vec![],
            to_vertex_config: vec![ToVertexConfig {
                name: "out",
                partitions: 5,
                writer_config: BufferWriterConfig {
                    streams: streams.clone(),
                    max_length: 30000,
                    usage_limit: 0.8,
                    buffer_full_strategy: RetryUntilSuccess,
                },
                conditions: None,
            }],
            vertex_type_config: VertexType::Source(SourceVtxConfig {
                source_config: SourceConfig {
                    read_ahead: false,
                    source_type: SourceType::Generator(GeneratorConfig {
                        rpu: 10,
                        content: bytes::Bytes::new(),
                        duration: Duration::from_secs(1),
                        value: None,
                        key_count: 0,
                        msg_size_bytes: 300,
                        jitter: Duration::from_millis(0),
                    }),
                },
                transformer_config: None,
            }),
            metrics_config: MetricsConfig {
                metrics_server_listen_port: 2469,
                lag_check_interval_in_secs: 5,
                lag_refresh_interval_in_secs: 3,
                lookback_window_in_secs: 120,
            },
            watermark_config: None,
            callback_config: None,
        };

        let cancellation_token = CancellationToken::new();
        let forwarder_task = tokio::spawn({
            let cancellation_token = cancellation_token.clone();
            async move {
                start_forwarder(cancellation_token, pipeline_config)
                    .await
                    .unwrap();
            }
        });

        // Wait for a few messages to be forwarded
        tokio::time::sleep(Duration::from_secs(2)).await;
        cancellation_token.cancel();
        forwarder_task.await.unwrap();

        for (stream_name, stream_consumer) in consumers {
            let messages: Vec<jetstream::Message> = stream_consumer
                .batch()
                .max_messages(10)
                .expires(Duration::from_millis(50))
                .messages()
                .await
                .unwrap()
                .map(|msg| msg.unwrap())
                .collect()
                .await;
            assert!(
                !messages.is_empty(),
                "Stream {} is expected to have messages",
                stream_name
            );
        }

        // Delete all streams created in this test
        for stream in streams {
            context.delete_stream(stream.name).await.unwrap();
        }
    }

    // e2e test for sink forwarder, reads from multi-partitioned buffer and
    // writes to sink.
    #[cfg(feature = "nats-tests")]
    #[tokio::test]
    async fn test_forwarder_for_sink_vertex() {
        // Unique names for the streams we use in this test
        let streams = vec![
            Stream::new("default-test-forwarder-for-sink-vertex-out-0", "test", 0),
            Stream::new("default-test-forwarder-for-sink-vertex-out-1", "test", 1),
            Stream::new("default-test-forwarder-for-sink-vertex-out-2", "test", 2),
            Stream::new("default-test-forwarder-for-sink-vertex-out-3", "test", 3),
            Stream::new("default-test-forwarder-for-sink-vertex-out-4", "test", 4),
        ];

        let js_url = "localhost:4222";
        let client = async_nats::connect(js_url).await.unwrap();
        let context = jetstream::new(client);

        const MESSAGE_COUNT: usize = 10;
        let mut consumers = vec![];
        for stream in &streams {
            // Delete stream if it exists
            let _ = context.delete_stream(stream.name).await;
            let _stream = context
                .get_or_create_stream(stream::Config {
                    name: stream.name.into(),
                    subjects: vec![stream.name.into()],
                    max_message_size: 64 * 1024,
                    max_messages: 10000,
                    ..Default::default()
                })
                .await
                .unwrap();

            // Publish some messages into the stream
            use chrono::{TimeZone, Utc};

            use crate::message::{Message, MessageID, Offset, StringOffset};
            let message = Message {
                typ: Default::default(),
                keys: Arc::from(vec!["key1".to_string()]),
                tags: None,
                value: vec![1, 2, 3].into(),
                offset: Offset::String(StringOffset::new("123".to_string(), 0)),
                event_time: Utc.timestamp_opt(1627846261, 0).unwrap(),
                watermark: None,
                id: MessageID {
                    vertex_name: "vertex".to_string().into(),
                    offset: "123".to_string().into(),
                    index: 0,
                },
                headers: HashMap::new(),
                metadata: None,
            };
            let message: bytes::BytesMut = message.try_into().unwrap();

            for _ in 0..MESSAGE_COUNT {
                context
                    .publish(stream.name, message.clone().into())
                    .await
                    .unwrap()
                    .await
                    .unwrap();
            }

            let c: consumer::PullConsumer = context
                .create_consumer_on_stream(
                    consumer::pull::Config {
                        name: Some(stream.name.to_string()),
                        ack_policy: consumer::AckPolicy::Explicit,
                        ..Default::default()
                    },
                    stream.name,
                )
                .await
                .unwrap();
            consumers.push((stream.name.to_string(), c));
        }

        let pipeline_config = PipelineConfig {
            pipeline_name: "simple-pipeline",
            vertex_name: "in",
            replica: 0,
            batch_size: 1000,
            paf_concurrency: 1000,
            read_timeout: Duration::from_secs(1),
            js_client_config: isb::jetstream::ClientConfig {
                url: "localhost:4222".to_string(),
                user: None,
                password: None,
            },
            to_vertex_config: vec![],
            from_vertex_config: vec![FromVertexConfig {
                name: "in",
                reader_config: BufferReaderConfig {
                    streams: streams.clone(),
                    wip_ack_interval: Duration::from_secs(1),
                },
                partitions: 0,
            }],
            vertex_type_config: VertexType::Sink(SinkVtxConfig {
                sink_config: SinkConfig {
                    sink_type: SinkType::Blackhole(BlackholeConfig::default()),
                    retry_config: None,
                },
                fb_sink_config: None,
                serving_store_config: None,
            }),
            metrics_config: MetricsConfig {
                metrics_server_listen_port: 2469,
                lag_check_interval_in_secs: 5,
                lag_refresh_interval_in_secs: 3,
                lookback_window_in_secs: 120,
            },
            watermark_config: None,
            callback_config: None,
        };

        let cancellation_token = CancellationToken::new();
        let forwarder_task = tokio::spawn({
            let cancellation_token = cancellation_token.clone();
            async move {
                start_forwarder(cancellation_token, pipeline_config)
                    .await
                    .unwrap();
            }
        });

        // Wait for a few messages to be forwarded
        tokio::time::sleep(Duration::from_secs(3)).await;
        cancellation_token.cancel();
        // token cancellation is not aborting the forwarder since we fetch messages from jetstream
        // as a stream of messages (not using `consumer.batch()`).
        // See `JetstreamReader::start` method in src/pipeline/isb/jetstream/reader.rs
        //forwarder_task.await.unwrap();
        forwarder_task.abort();

        for (stream_name, mut stream_consumer) in consumers {
            let stream_info = stream_consumer.info().await.unwrap();
            assert_eq!(
                stream_info.delivered.stream_sequence, MESSAGE_COUNT as u64,
                "Stream={}, expected delivered stream sequence to be {}, current value is {}",
                stream_name, MESSAGE_COUNT, stream_info.delivered.stream_sequence
            );
            assert_eq!(
                stream_info.ack_floor.stream_sequence, MESSAGE_COUNT as u64,
                "Stream={}, expected ack'ed stream sequence to be {}, current value is {}",
                stream_name, MESSAGE_COUNT, stream_info.ack_floor.stream_sequence
            );
        }

        // Delete all streams created in this test
        for stream in streams {
            context.delete_stream(stream.name).await.unwrap();
        }
    }

    struct SimpleCat;

    #[tonic::async_trait]
    impl map::Mapper for SimpleCat {
        async fn map(&self, input: map::MapRequest) -> Vec<map::Message> {
            let message = map::Message::new(input.value)
                .with_keys(input.keys)
                .with_tags(vec!["test-forwarder".to_string()]);
            vec![message]
        }
    }

    // e2e test for map forwarder, reads from multi-partitioned buffer, invokes map
    // and writes to multi-partitioned buffer.
    #[cfg(feature = "nats-tests")]
    #[tokio::test]
    async fn test_forwarder_for_map_vertex() {
        let tmp_dir = TempDir::new().unwrap();
        let sock_file = tmp_dir.path().join("map.sock");
        let server_info_file = tmp_dir.path().join("mapper-server-info");

        let server_info = server_info_file.clone();
        let server_socket = sock_file.clone();
        let _handle = tokio::spawn(async move {
            map::Server::new(SimpleCat)
                .with_socket_file(server_socket)
                .with_server_info_file(server_info)
                .start()
                .await
                .expect("server failed");
        });

        // wait for the server to start
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Unique names for the streams we use in this test
        let input_streams = vec![
            Stream::new("default-test-forwarder-for-map-vertex-in-0", "test", 0),
            Stream::new("default-test-forwarder-for-map-vertex-in-1", "test", 1),
            Stream::new("default-test-forwarder-for-map-vertex-in-2", "test", 2),
            Stream::new("default-test-forwarder-for-map-vertex-in-3", "test", 3),
            Stream::new("default-test-forwarder-for-map-vertex-in-4", "test", 4),
        ];

        let output_streams = vec![
            Stream::new("default-test-forwarder-for-map-vertex-out-0", "test", 0),
            Stream::new("default-test-forwarder-for-map-vertex-out-1", "test", 1),
            Stream::new("default-test-forwarder-for-map-vertex-out-2", "test", 2),
            Stream::new("default-test-forwarder-for-map-vertex-out-3", "test", 3),
            Stream::new("default-test-forwarder-for-map-vertex-out-4", "test", 4),
        ];

        let js_url = "localhost:4222";
        let client = async_nats::connect(js_url).await.unwrap();
        let context = jetstream::new(client);

        const MESSAGE_COUNT: usize = 10;
        let mut input_consumers = vec![];
        let mut output_consumers = vec![];
        for stream in &input_streams {
            // Delete stream if it exists
            let _ = context.delete_stream(stream.name).await;
            let _stream = context
                .get_or_create_stream(stream::Config {
                    name: stream.name.to_string(),
                    subjects: vec![stream.name.to_string()],
                    max_message_size: 64 * 1024,
                    max_messages: 10000,
                    ..Default::default()
                })
                .await
                .unwrap();

            // Publish some messages into the stream
            use chrono::{TimeZone, Utc};

            use crate::message::{Message, MessageID, Offset, StringOffset};
            let message = Message {
                typ: Default::default(),
                keys: Arc::from(vec!["key1".to_string()]),
                tags: None,
                value: vec![1, 2, 3].into(),
                offset: Offset::String(StringOffset::new("123".to_string(), 0)),
                event_time: Utc.timestamp_opt(1627846261, 0).unwrap(),
                watermark: None,
                id: MessageID {
                    vertex_name: "vertex".to_string().into(),
                    offset: "123".to_string().into(),
                    index: 0,
                },
                headers: HashMap::new(),
                metadata: None,
            };
            let message: bytes::BytesMut = message.try_into().unwrap();

            for _ in 0..MESSAGE_COUNT {
                context
                    .publish(stream.name, message.clone().into())
                    .await
                    .unwrap()
                    .await
                    .unwrap();
            }

            let c: consumer::PullConsumer = context
                .create_consumer_on_stream(
                    consumer::pull::Config {
                        name: Some(stream.name.to_string()),
                        ack_policy: consumer::AckPolicy::Explicit,
                        ..Default::default()
                    },
                    stream.name,
                )
                .await
                .unwrap();

            input_consumers.push((stream.name.to_string(), c));
        }

        // Create output streams and consumers
        for stream in &output_streams {
            // Delete stream if it exists
            let _ = context.delete_stream(stream.name).await;
            let _stream = context
                .get_or_create_stream(stream::Config {
                    name: stream.name.to_string(),
                    subjects: vec![stream.name.into()],
                    max_message_size: 64 * 1024,
                    max_messages: 1000,
                    ..Default::default()
                })
                .await
                .unwrap();

            let c: consumer::PullConsumer = context
                .create_consumer_on_stream(
                    consumer::pull::Config {
                        name: Some(stream.name.to_string()),
                        ack_policy: consumer::AckPolicy::Explicit,
                        ..Default::default()
                    },
                    stream.name,
                )
                .await
                .unwrap();
            output_consumers.push((stream.name.to_string(), c));
        }

        let pipeline_config = PipelineConfig {
            pipeline_name: "simple-map-pipeline",
            vertex_name: "in",
            replica: 0,
            batch_size: 1000,
            paf_concurrency: 1000,
            read_timeout: Duration::from_secs(1),
            js_client_config: isb::jetstream::ClientConfig {
                url: "localhost:4222".to_string(),
                user: None,
                password: None,
            },
            to_vertex_config: vec![ToVertexConfig {
                name: "map-out",
                partitions: 5,
                writer_config: BufferWriterConfig {
                    streams: output_streams.clone(),
                    max_length: 30000,
                    usage_limit: 0.8,
                    buffer_full_strategy: RetryUntilSuccess,
                },
                conditions: None,
            }],
            from_vertex_config: vec![FromVertexConfig {
                name: "map-in",
                reader_config: BufferReaderConfig {
                    streams: input_streams.clone(),
                    wip_ack_interval: Duration::from_secs(1),
                },
                partitions: 0,
            }],
            vertex_type_config: VertexType::Map(MapVtxConfig {
                concurrency: 10,
                map_type: MapType::UserDefined(UserDefinedConfig {
                    grpc_max_message_size: 4 * 1024 * 1024,
                    socket_path: sock_file.to_str().unwrap().to_string(),
                    server_info_path: server_info_file.to_str().unwrap().to_string(),
                }),
                map_mode: MapMode::Unary,
            }),
            metrics_config: MetricsConfig {
                metrics_server_listen_port: 2469,
                lag_check_interval_in_secs: 5,
                lag_refresh_interval_in_secs: 3,
                lookback_window_in_secs: 120,
            },
            watermark_config: None,
            callback_config: None,
        };

        let cancellation_token = CancellationToken::new();
        let forwarder_task = tokio::spawn({
            let cancellation_token = cancellation_token.clone();
            async move {
                start_forwarder(cancellation_token, pipeline_config)
                    .await
                    .unwrap();
            }
        });

        // Wait for a few messages to be forwarded
        tokio::time::sleep(Duration::from_secs(3)).await;
        cancellation_token.cancel();
        // token cancellation is not aborting the forwarder since we fetch messages from jetstream
        // as a stream of messages (not using `consumer.batch()`).
        // See `JetstreamReader::start` method in src/pipeline/isb/jetstream/reader.rs
        //forwarder_task.await.unwrap();
        forwarder_task.abort();

        // make sure we have mapped and written all messages to downstream
        let mut written_count = 0;
        for (_, mut stream_consumer) in output_consumers {
            written_count += stream_consumer.info().await.unwrap().num_pending;
        }
        assert_eq!(written_count, (MESSAGE_COUNT * input_streams.len()) as u64);

        // make sure all the upstream messages are read and acked
        for (_, mut stream_consumer) in input_consumers {
            let con_info = stream_consumer.info().await.unwrap();
            assert_eq!(con_info.num_pending, 0);
            assert_eq!(con_info.num_ack_pending, 0);
        }

        // Delete all streams created in this test
        for stream in input_streams.iter().chain(output_streams.iter()) {
            context.delete_stream(stream.name).await.unwrap();
        }
    }
}
