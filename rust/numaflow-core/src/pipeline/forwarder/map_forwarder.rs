use crate::config::is_mono_vertex;
use crate::config::pipeline::PipelineConfig;
use crate::config::pipeline::isb::BufferReaderConfig;
use crate::config::pipeline::map::MapVtxConfig;
use crate::error::Error;
use crate::mapper::map::MapHandle;
use crate::metrics::{
    ComponentHealthChecks, LagReader, MetricsState, PendingReaderTasks, PipelineComponents,
    WatermarkFetcherState,
};
use crate::pipeline::PipelineContext;

use crate::pipeline::isb::jetstream::reader::{ISBReaderComponents, JetStreamReader};
use crate::pipeline::isb::jetstream::writer::{ISBWriterComponents, JetstreamWriter};
use crate::shared::create_components;
use crate::shared::metrics::start_metrics_server;
use crate::tracker::TrackerHandle;
use crate::typ::{
    NumaflowTypeConfig, WithInMemoryRateLimiter, WithRedisRateLimiter, WithoutRateLimiter,
    build_in_memory_rate_limiter_config, build_redis_rate_limiter_config,
    should_use_redis_rate_limiter,
};
use crate::watermark::WatermarkHandle;
use crate::{Result, shared};
use async_nats::jetstream::Context;
use futures::future::try_join_all;
use serving::callback::CallbackHandler;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

/// Map forwarder is a component which starts a streaming reader, a mapper, and a writer
/// and manages the lifecycle of these components.
pub(crate) struct MapForwarder<C: crate::typ::NumaflowTypeConfig> {
    jetstream_reader: JetStreamReader<C>,
    mapper: MapHandle,
    jetstream_writer: JetstreamWriter,
}

impl<C: crate::typ::NumaflowTypeConfig> MapForwarder<C> {
    pub(crate) async fn new(
        jetstream_reader: JetStreamReader<C>,
        mapper: MapHandle,
        jetstream_writer: JetstreamWriter,
    ) -> Self {
        Self {
            jetstream_reader,
            mapper,
            jetstream_writer,
        }
    }

    pub(crate) async fn start(self, cln_token: CancellationToken) -> Result<()> {
        let child_token = cln_token.child_token();
        // only the reader need to listen on the cancellation token, if the reader stops all
        // other components will stop gracefully because they are chained using tokio streams.
        let (read_messages_stream, reader_handle) = self
            .jetstream_reader
            .streaming_read(child_token.clone())
            .await?;

        let (mapped_messages_stream, mapper_handle) = self
            .mapper
            .streaming_map(read_messages_stream, child_token.clone())
            .await?;

        let writer_handle = self
            .jetstream_writer
            .streaming_write(mapped_messages_stream, child_token)
            .await?;

        // Join the reader, mapper, and writer
        let (reader_result, mapper_result, writer_result) =
            tokio::try_join!(reader_handle, mapper_handle, writer_handle).map_err(|e| {
                error!(?e, "Error while joining reader, mapper, and writer");
                Error::Forwarder(format!(
                    "Error while joining reader, mapper, and writer: {e}"
                ))
            })?;

        writer_result.inspect_err(|e| {
            error!(?e, "Error while writing messages");
            cln_token.cancel();
        })?;

        mapper_result.inspect_err(|e| {
            error!(?e, "Error while mapping messages");
            cln_token.cancel();
        })?;

        reader_result.inspect_err(|e| {
            error!(?e, "Error while reading messages");
            cln_token.cancel();
        })?;

        Ok(())
    }
}

pub async fn start_map_forwarder(
    cln_token: CancellationToken,
    js_context: Context,
    config: PipelineConfig,
    map_vtx_config: MapVtxConfig,
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

    let reader_config = &config
        .from_vertex_config
        .first()
        .ok_or_else(|| Error::Config("No from vertex config found".to_string()))?
        .reader_config;

    let from_partitions: Vec<u16> = (0..reader_config.streams.len() as u16).collect();

    let tracker_handle = TrackerHandle::new(serving_callback_handler.clone());
    let watermark_handle = create_components::create_edge_watermark_handle(
        &config,
        &js_context,
        &cln_token,
        None,
        tracker_handle.clone(),
        from_partitions.clone(),
    )
    .await?;

    let context = PipelineContext {
        cln_token: cln_token.clone(),
        js_context: &js_context,
        config: &config,
        tracker_handle,
    };

    let writer_components =
        ISBWriterComponents::new(watermark_handle.clone().map(WatermarkHandle::ISB), &context);

    let buffer_writer = JetstreamWriter::new(writer_components);
    let (forwarder_tasks, mapper_handle, _pending_reader_task) = if let Some(rate_limit_config) =
        &config.rate_limit
    {
        if should_use_redis_rate_limiter(rate_limit_config) {
            let redis_config =
                build_redis_rate_limiter_config(rate_limit_config, cln_token.clone()).await?;
            run_all_map_forwarders::<WithRedisRateLimiter>(
                &context,
                &map_vtx_config,
                reader_config,
                buffer_writer,
                watermark_handle.clone(),
                Some(redis_config.throttling_config),
            )
            .await?
        } else {
            let in_mem_config =
                build_in_memory_rate_limiter_config(rate_limit_config, cln_token.clone()).await?;
            run_all_map_forwarders::<WithInMemoryRateLimiter>(
                &context,
                &map_vtx_config,
                reader_config,
                buffer_writer,
                watermark_handle.clone(),
                Some(in_mem_config.throttling_config),
            )
            .await?
        }
    } else {
        run_all_map_forwarders::<WithoutRateLimiter>(
            &context,
            &map_vtx_config,
            reader_config,
            buffer_writer,
            watermark_handle.clone(),
            None,
        )
        .await?
    };

    let metrics_server_handle = start_metrics_server::<WithoutRateLimiter>(
        config.metrics_config.clone(),
        MetricsState {
            health_checks: ComponentHealthChecks::Pipeline(Box::new(PipelineComponents::Map(
                mapper_handle,
            ))),
            watermark_fetcher_state: watermark_handle.map(|handle| WatermarkFetcherState {
                watermark_handle: WatermarkHandle::ISB(handle),
                partitions: from_partitions,
            }),
        },
    )
    .await;

    let results = try_join_all(forwarder_tasks)
        .await
        .map_err(|e| Error::Forwarder(e.to_string()))?;

    for result in results {
        error!(?result, "Forwarder task failed");
        result?;
    }

    metrics_server_handle.abort();

    info!("All forwarders have stopped successfully");
    Ok(())
}

/// Starts map forwarder for all the streams.
async fn run_all_map_forwarders<C: NumaflowTypeConfig>(
    context: &PipelineContext<'_>,
    map_vtx_config: &MapVtxConfig,
    reader_config: &BufferReaderConfig,
    buffer_writer: JetstreamWriter,
    watermark_handle: Option<crate::watermark::isb::ISBWatermarkHandle>,
    rate_limiter: Option<C::RateLimiter>,
) -> Result<(
    Vec<tokio::task::JoinHandle<Result<()>>>,
    MapHandle,
    PendingReaderTasks,
)> {
    let mut forwarder_tasks = vec![];
    let mut isb_lag_readers: Vec<JetStreamReader<C>> = vec![];
    let mut mapper_handle = None;

    for stream in reader_config.streams.clone() {
        info!("Creating buffer reader for stream {:?}", stream);

        let mapper = create_components::create_mapper(
            context.config.batch_size,
            context.config.read_timeout,
            context.config.graceful_shutdown_time,
            map_vtx_config.clone(),
            context.tracker_handle.clone(),
            context.cln_token.clone(),
        )
        .await?;

        if mapper_handle.is_none() {
            mapper_handle = Some(mapper.clone());
        }

        let reader_components = ISBReaderComponents::new(
            stream,
            reader_config.clone(),
            watermark_handle.clone(),
            context,
        );

        let (task, reader) = run_map_forwarder_for_stream::<C>(
            reader_components,
            mapper,
            buffer_writer.clone(),
            rate_limiter.clone(),
        )
        .await?;

        forwarder_tasks.push(task);
        isb_lag_readers.push(reader);
    }

    let pending_reader = shared::metrics::create_pending_reader(
        &context.config.metrics_config,
        LagReader::ISB(isb_lag_readers),
    )
    .await;
    info!("Starting pending reader");
    let pending_reader_task = pending_reader.start(is_mono_vertex()).await;

    Ok((forwarder_tasks, mapper_handle.unwrap(), pending_reader_task))
}

/// Start a map forwarder for a single stream, returns the task handle and the reader,
/// it's returned so that we can create a pending reader for metrics.
async fn run_map_forwarder_for_stream<C: NumaflowTypeConfig>(
    reader_components: ISBReaderComponents,
    mapper: crate::mapper::map::MapHandle,
    buffer_writer: JetstreamWriter,
    rate_limiter: Option<C::RateLimiter>,
) -> Result<(tokio::task::JoinHandle<Result<()>>, JetStreamReader<C>)> {
    let cln_token = reader_components.cln_token.clone();
    let buffer_reader = JetStreamReader::<C>::new(reader_components, rate_limiter).await?;

    let forwarder = MapForwarder::<C>::new(buffer_reader.clone(), mapper, buffer_writer).await;

    let task = tokio::spawn(async move { forwarder.start(cln_token).await });
    Ok((task, buffer_reader))
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::config::components::metrics::MetricsConfig;
    use crate::config::pipeline::isb::BufferFullStrategy::RetryUntilSuccess;
    use crate::config::pipeline::isb::{BufferReaderConfig, BufferWriterConfig, Stream};
    use crate::config::pipeline::map::{MapType, UserDefinedConfig};
    use crate::config::pipeline::{
        FromVertexConfig, ToVertexConfig, VertexConfig, VertexType, isb,
    };
    use async_nats::jetstream;
    use async_nats::jetstream::{consumer, stream};
    use numaflow::map;
    use numaflow_shared::server_info::MapMode;
    use std::time::Duration;
    use tempfile::TempDir;

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

            use async_nats::jetstream::{consumer, stream};
            use chrono::{TimeZone, Utc};
            use std::collections::HashMap;
            use std::sync::Arc; // Publish some messages into the stream

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
                is_late: false,
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
            writer_concurrency: 1000,
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
                to_vertex_type: VertexType::Sink,
            }],
            from_vertex_config: vec![FromVertexConfig {
                name: "map-in",
                reader_config: BufferReaderConfig {
                    streams: input_streams.clone(),
                    wip_ack_interval: Duration::from_secs(1),
                    ..Default::default()
                },
                partitions: 0,
            }],
            vertex_config: VertexConfig::Map(MapVtxConfig {
                concurrency: 10,
                map_type: MapType::UserDefined(UserDefinedConfig {
                    grpc_max_message_size: 4 * 1024 * 1024,
                    socket_path: sock_file.to_str().unwrap().to_string(),
                    server_info_path: server_info_file.to_str().unwrap().to_string(),
                }),
                map_mode: MapMode::Unary,
            }),
            vertex_type: VertexType::MapUDF,
            metrics_config: MetricsConfig {
                metrics_server_listen_port: 2469,
                lag_check_interval_in_secs: 5,
                lag_refresh_interval_in_secs: 3,
                lookback_window_in_secs: 120,
            },
            ..Default::default()
        };

        // Extract the map config from the pipeline config
        let map_vtx_config =
            if let VertexConfig::Map(ref map_config) = pipeline_config.vertex_config {
                map_config.clone()
            } else {
                panic!("Expected map vertex config");
            };

        let cancellation_token = CancellationToken::new();
        let forwarder_task = tokio::spawn({
            let cancellation_token = cancellation_token.clone();
            let context = context.clone();
            async move {
                start_map_forwarder(cancellation_token, context, pipeline_config, map_vtx_config)
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
