use crate::config::is_mono_vertex;
use crate::config::pipeline::isb::BufferReaderConfig;
use crate::config::pipeline::{PipelineConfig, ServingStoreType, SinkVtxConfig};
use crate::error::Error;
use crate::metrics::{
    ComponentHealthChecks, LagReader, MetricsState, PendingReaderTasks, PipelineComponents,
    WatermarkFetcherState,
};
use crate::pipeline::PipelineContext;
use crate::pipeline::isb::jetstream::js_reader::JetStreamReader;
use crate::pipeline::isb::reader::{ISBReader, ISBReaderComponents};
use crate::shared::create_components;
use crate::shared::metrics::start_metrics_server;
use crate::sinker::sink::SinkWriter;
use crate::sinker::sink::serve::ServingStore;
use crate::sinker::sink::serve::nats::NatsServingStore;
use crate::sinker::sink::serve::user_defined::UserDefinedStore;
use crate::tracker::Tracker;
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

/// Sink forwarder is a component which starts a streaming reader and a sink writer
/// and manages the lifecycle of these components.
pub(crate) struct SinkForwarder<C: crate::typ::NumaflowTypeConfig> {
    jetstream_reader: ISBReader<C>,
    sink_writer: SinkWriter,
}

impl<C: crate::typ::NumaflowTypeConfig> SinkForwarder<C> {
    pub(crate) async fn new(jetstream_reader: ISBReader<C>, sink_writer: SinkWriter) -> Self {
        Self {
            jetstream_reader,
            sink_writer,
        }
    }

    pub(crate) async fn start(self, cln_token: CancellationToken) -> Result<()> {
        // only the reader need to listen on the cancellation token, if the reader stops all
        // other components will stop gracefully because they are chained using tokio streams.
        let (read_messages_stream, reader_handle) = self
            .jetstream_reader
            .streaming_read(cln_token.clone())
            .await?;

        let sink_writer_handle = self
            .sink_writer
            .streaming_write(read_messages_stream, cln_token.clone())
            .await?;

        // Join the reader and sink writer
        let (reader_result, sink_writer_result) =
            tokio::try_join!(reader_handle, sink_writer_handle).map_err(|e| {
                error!(?e, "Error while joining reader and sink writer");
                Error::Forwarder(format!("Error while joining reader and sink writer: {e}",))
            })?;

        sink_writer_result.inspect_err(|e| {
            error!(?e, "Error while writing messages");
        })?;

        reader_result.inspect_err(|e| {
            error!(?e, "Error while reading messages");
        })?;

        Ok(())
    }
}

pub async fn start_sink_forwarder(
    cln_token: CancellationToken,
    js_context: Context,
    config: PipelineConfig,
    sink: SinkVtxConfig,
) -> Result<()> {
    // 1. One-time setup
    let serving_callback_handler = if let Some(cb_cfg) = &config.callback_config {
        Some(
            CallbackHandler::new(
                config.vertex_name,
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

    let tracker = Tracker::new(serving_callback_handler.clone(), cln_token.clone());
    let watermark_handle = create_components::create_edge_watermark_handle(
        &config,
        &js_context,
        &cln_token,
        None,
        tracker.clone(),
        from_partitions.clone(),
    )
    .await?;

    let serving_store = match &sink.serving_store_config {
        Some(serving_store_config) => match serving_store_config {
            ServingStoreType::UserDefined(config) => {
                let serving_store = UserDefinedStore::new(config.clone()).await?;
                Some(ServingStore::UserDefined(serving_store))
            }
            ServingStoreType::Nats(config) => {
                let serving_store =
                    NatsServingStore::new(js_context.clone(), config.clone()).await?;
                Some(ServingStore::Nats(Box::new(serving_store)))
            }
        },
        None => None,
    };

    let context = PipelineContext {
        cln_token: cln_token.clone(),
        js_context: &js_context,
        config: &config,
        tracker,
    };

    // 2. Clean dispatch logic
    let (forwarder_tasks, first_sink_writer, _pending_reader_task) =
        if let Some(rate_limit_config) = &config.rate_limit {
            if should_use_redis_rate_limiter(rate_limit_config) {
                let redis_config =
                    build_redis_rate_limiter_config(rate_limit_config, cln_token.clone()).await?;

                run_all_sink_forwarders::<WithRedisRateLimiter>(
                    &context,
                    &sink,
                    reader_config,
                    watermark_handle.clone(),
                    serving_store,
                    Some(redis_config.throttling_config),
                )
                .await?
            } else {
                let in_mem_config =
                    build_in_memory_rate_limiter_config(rate_limit_config, cln_token.clone())
                        .await?;

                run_all_sink_forwarders::<WithInMemoryRateLimiter>(
                    &context,
                    &sink,
                    reader_config,
                    watermark_handle.clone(),
                    serving_store,
                    Some(in_mem_config.throttling_config),
                )
                .await?
            }
        } else {
            run_all_sink_forwarders::<WithoutRateLimiter>(
                &context,
                &sink,
                reader_config,
                watermark_handle.clone(),
                serving_store,
                None,
            )
            .await?
        };

    start_metrics_server::<WithoutRateLimiter>(
        config.metrics_config.clone(),
        MetricsState {
            health_checks: ComponentHealthChecks::Pipeline(Box::new(PipelineComponents::Sink(
                Box::new(first_sink_writer),
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
        info!(?result, "Forwarder task completed");
        result?;
    }

    info!("All forwarders have stopped successfully");
    Ok(())
}

/// Starts sink forwarder for all the streams.
async fn run_all_sink_forwarders<C: NumaflowTypeConfig>(
    context: &PipelineContext<'_>,
    sink: &SinkVtxConfig,
    reader_config: &BufferReaderConfig,
    watermark_handle: Option<crate::watermark::isb::ISBWatermarkHandle>,
    serving_store: Option<ServingStore>,
    rate_limiter: Option<C::RateLimiter>,
) -> Result<(
    Vec<tokio::task::JoinHandle<Result<()>>>,
    SinkWriter,
    PendingReaderTasks,
)> {
    let mut forwarder_tasks = vec![];
    let mut isb_lag_readers: Vec<ISBReader<C>> = vec![];
    let mut first_sink_writer = None;

    for stream in reader_config.streams.clone() {
        info!(
            "Creating sink writer and buffer reader for stream {:?}",
            stream
        );

        let sink_writer = create_components::create_sink_writer(
            context.config.batch_size,
            context.config.read_timeout,
            sink.sink_config.clone(),
            sink.fb_sink_config.clone(),
            sink.on_success_sink_config.clone(),
            serving_store.clone(),
            &context.cln_token,
        )
        .await?;

        if first_sink_writer.is_none() {
            first_sink_writer = Some(sink_writer.clone());
        }

        let reader_components = ISBReaderComponents::new(
            stream,
            reader_config.clone(),
            watermark_handle.clone(),
            context,
        );

        let (task, reader) = run_sink_forwarder_for_stream::<C>(
            reader_components,
            sink_writer,
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
    let pending_reader_task = pending_reader.start(is_mono_vertex()).await;

    Ok((
        forwarder_tasks,
        first_sink_writer.expect("one sink writer is expected"),
        pending_reader_task,
    ))
}

/// Starts sink forwarder for a single stream.
async fn run_sink_forwarder_for_stream<C: NumaflowTypeConfig>(
    reader_components: ISBReaderComponents,
    sink_writer: SinkWriter,
    rate_limiter: Option<C::RateLimiter>,
) -> Result<(tokio::task::JoinHandle<Result<()>>, ISBReader<C>)> {
    let cln_token = reader_components.cln_token.clone();

    let js_reader = JetStreamReader::new(
        reader_components.stream.clone(),
        reader_components.js_ctx.clone(),
        reader_components.isb_config.clone(),
    )
    .await?;

    let isb_reader = ISBReader::<C>::new(reader_components, js_reader, rate_limiter).await?;

    let forwarder = SinkForwarder::<C>::new(isb_reader.clone(), sink_writer).await;

    let task = tokio::spawn(async move { forwarder.start(cln_token).await });
    Ok((task, isb_reader))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use super::*;
    use crate::config::components::metrics::MetricsConfig;
    use crate::config::components::sink::{BlackholeConfig, SinkConfig, SinkType};
    use crate::config::pipeline::isb::Stream;
    use crate::config::pipeline::{PipelineConfig, VertexType};
    use crate::pipeline::pipeline::FromVertexConfig;
    use crate::pipeline::pipeline::SinkVtxConfig;
    use crate::pipeline::pipeline::VertexConfig;
    use crate::pipeline::pipeline::isb;
    use crate::pipeline::pipeline::isb::BufferReaderConfig;
    use async_nats::jetstream;
    use async_nats::jetstream::{consumer, stream};

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
                ..Default::default()
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
            writer_concurrency: 1000,
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
                    ..Default::default()
                },
                partitions: 0,
            }],
            vertex_type: VertexType::Sink,
            vertex_config: VertexConfig::Sink(SinkVtxConfig {
                sink_config: SinkConfig {
                    sink_type: SinkType::Blackhole(BlackholeConfig::default()),
                    retry_config: None,
                },
                fb_sink_config: None,
                on_success_sink_config: None,
                serving_store_config: None,
            }),
            metrics_config: MetricsConfig {
                metrics_server_listen_port: 2469,
                lag_check_interval_in_secs: 5,
                lag_refresh_interval_in_secs: 3,
                lookback_window_in_secs: 120,
            },
            ..Default::default()
        };

        // Extract the sink config from the pipeline config
        let sink_vtx_config =
            if let VertexConfig::Sink(ref sink_config) = pipeline_config.vertex_config {
                sink_config.clone()
            } else {
                panic!("Expected sink vertex config");
            };

        let cancellation_token = CancellationToken::new();
        let forwarder_task = tokio::spawn({
            let cancellation_token = cancellation_token.clone();
            let context = context.clone();
            async move {
                start_sink_forwarder(
                    cancellation_token,
                    context,
                    pipeline_config,
                    sink_vtx_config,
                )
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
}
