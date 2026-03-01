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
use crate::pipeline::isb::reader::{ISBReaderComponents, ISBReaderOrchestrator};
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
    jetstream_reader: ISBReaderOrchestrator<C>,
    sink_writer: SinkWriter,
}

impl<C: crate::typ::NumaflowTypeConfig> SinkForwarder<C> {
    pub(crate) async fn new(
        jetstream_reader: ISBReaderOrchestrator<C>,
        sink_writer: SinkWriter,
    ) -> Self {
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

    // Create the ISB factory from the JetStream context
    use crate::pipeline::isb::jetstream::JetStreamFactory;
    let isb_factory = JetStreamFactory::new(js_context);

    // 2. Clean dispatch logic
    let (forwarder_tasks, first_sink_writer, _pending_reader_task) =
        if let Some(rate_limit_config) = &config.rate_limit {
            if should_use_redis_rate_limiter(rate_limit_config) {
                let redis_config =
                    build_redis_rate_limiter_config(rate_limit_config, cln_token.clone()).await?;

                let context = PipelineContext::<WithRedisRateLimiter, _>::new(
                    cln_token.clone(),
                    &isb_factory,
                    &config,
                    tracker.clone(),
                );

                run_all_sink_forwarders::<WithRedisRateLimiter, _>(
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

                let context = PipelineContext::<WithInMemoryRateLimiter, _>::new(
                    cln_token.clone(),
                    &isb_factory,
                    &config,
                    tracker.clone(),
                );

                run_all_sink_forwarders::<WithInMemoryRateLimiter, _>(
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
            let context = PipelineContext::<WithoutRateLimiter, _>::new(
                cln_token.clone(),
                &isb_factory,
                &config,
                tracker.clone(),
            );

            run_all_sink_forwarders::<WithoutRateLimiter, _>(
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
async fn run_all_sink_forwarders<C, F>(
    context: &PipelineContext<'_, C, F>,
    sink: &SinkVtxConfig,
    reader_config: &BufferReaderConfig,
    watermark_handle: Option<crate::watermark::isb::ISBWatermarkHandle>,
    serving_store: Option<ServingStore>,
    rate_limiter: Option<C::RateLimiter>,
) -> Result<(
    Vec<tokio::task::JoinHandle<Result<()>>>,
    SinkWriter,
    PendingReaderTasks,
)>
where
    C: NumaflowTypeConfig<ISBReader = JetStreamReader>,
    F: crate::pipeline::isb::ISBFactory<Reader = C::ISBReader, Writer = C::ISBWriter>,
{
    let mut forwarder_tasks = vec![];
    let mut isb_lag_readers: Vec<ISBReaderOrchestrator<C>> = vec![];
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
            None,
        )
        .await?;

        if first_sink_writer.is_none() {
            first_sink_writer = Some(sink_writer.clone());
        }

        let reader_components = ISBReaderComponents::new::<C, F>(
            stream,
            reader_config.clone(),
            watermark_handle.clone(),
            context,
        );

        let (task, reader) = run_sink_forwarder_for_stream::<C, F>(
            reader_components,
            sink_writer,
            rate_limiter.clone(),
            context.factory(),
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
async fn run_sink_forwarder_for_stream<C, F>(
    reader_components: ISBReaderComponents,
    sink_writer: SinkWriter,
    rate_limiter: Option<C::RateLimiter>,
    isb_factory: &F,
) -> Result<(
    tokio::task::JoinHandle<Result<()>>,
    ISBReaderOrchestrator<C>,
)>
where
    C: NumaflowTypeConfig<ISBReader = JetStreamReader>,
    F: crate::pipeline::isb::ISBFactory<Reader = C::ISBReader, Writer = C::ISBWriter>,
{
    let cln_token = reader_components.cln_token.clone();

    let isb_reader_impl = isb_factory
        .create_reader(
            reader_components.stream.clone(),
            reader_components.isb_config.as_ref(),
        )
        .await?;

    let isb_reader =
        ISBReaderOrchestrator::<C>::new(reader_components, isb_reader_impl, rate_limiter).await?;

    let forwarder = SinkForwarder::<C>::new(isb_reader.clone(), sink_writer).await;

    let task = tokio::spawn(async move { forwarder.start(cln_token).await });
    Ok((task, isb_reader))
}

#[cfg(test)]
mod simple_buffer_tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use bytes::Bytes;
    use chrono::Utc;
    use numaflow::sink;
    use numaflow_testing::simplebuffer::SimpleBuffer;
    use tokio::sync::mpsc::Receiver;
    use tokio_util::sync::CancellationToken;

    use crate::config::pipeline::isb::{BufferReaderConfig, Stream};
    use crate::message::{IntOffset, Message, MessageID, Offset};
    use crate::pipeline::isb::reader::{ISBReaderComponents, ISBReaderOrchestrator};
    use crate::pipeline::isb::simplebuffer::{SimpleBufferAdapter, WithSimpleBuffer};
    use crate::sinker::test_utils::{SinkTestHandle, SinkType as TestSinkType};
    use crate::tracker::Tracker;

    /// A UD sink that writes received messages into a SimpleBuffer for verification.
    /// Uses a shared atomic counter to generate globally unique IDs for each write,
    /// avoiding dedup collisions when multiple partitions send messages with similar IDs.
    /// (Since we only have single simple buffer to write all the data to)
    struct SimpleBufferSink {
        output_buffer: SimpleBuffer,
        counter: Arc<AtomicUsize>,
    }

    #[tonic::async_trait]
    impl sink::Sinker for SimpleBufferSink {
        async fn sink(&self, mut input: Receiver<sink::SinkRequest>) -> Vec<sink::Response> {
            let mut responses = vec![];
            let writer = self.output_buffer.writer();
            while let Some(datum) = input.recv().await {
                // Generate a unique ID so that we can avoid id collisions
                // during multi partition tests. (Since our sink is backed by a single simple buffer)
                let unique_id = format!(
                    "{}-{}",
                    datum.id,
                    self.counter.fetch_add(1, Ordering::Relaxed)
                );
                let _ = writer
                    .write(unique_id, Bytes::from(datum.value), HashMap::new())
                    .await;
                responses.push(sink::Response::ok(datum.id));
            }
            responses
        }
    }

    /// Helper to create and write test messages into a SimpleBufferAdapter via its ISBWriter.
    async fn write_test_messages(adapter: &SimpleBufferAdapter, count: usize) {
        use crate::pipeline::isb::ISBWriter;
        let writer = adapter.writer();
        for i in 0..count {
            let msg = Message {
                typ: Default::default(),
                keys: Arc::from(vec![format!("key-{}", i)]),
                tags: None,
                value: Bytes::from(format!("payload-{}", i)),
                offset: Offset::Int(IntOffset::new(i as i64, 0)),
                event_time: Utc::now(),
                watermark: None,
                id: MessageID {
                    vertex_name: "test-in".into(),
                    index: i as i32,
                    offset: format!("{}", i).into(),
                },
                headers: Arc::new(HashMap::new()),
                metadata: None,
                is_late: false,
                ack_handle: None,
            };
            writer.write(msg).await.expect("write should succeed");
        }
    }

    // End-to-end test for sink forwarder using SimpleBuffer.
    // Reads from a SimpleBuffer-backed ISB, writes to a UD sink that stores
    // messages in a different SimpleBuffer for verification.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_sink_forwarder_with_single_stream() {
        const MESSAGE_COUNT: usize = 10;

        let cln_token = CancellationToken::new();
        let tracker = Tracker::new(None, cln_token.clone());
        let batch_size = 500;

        // Input buffer
        let input_adapter = SimpleBufferAdapter::new(SimpleBuffer::new(1000, 0, "sink-input"));

        // Write all the messages to the input buffer
        write_test_messages(&input_adapter, MESSAGE_COUNT).await;

        // Output buffer for verification (the UD sink writes here)
        let output_buffer = SimpleBuffer::new(1000, 0, "sink-output");

        // Create the UD sink backed by the output buffer
        let sink_svc = SimpleBufferSink {
            output_buffer: output_buffer.clone(),
            counter: Arc::new(AtomicUsize::new(0)),
        };

        let SinkTestHandle {
            sink_writer,
            ud_sink_server_handle: _server_handle,
            ..
        } = SinkTestHandle::create_sink(
            TestSinkType::UserDefined(sink_svc),
            None,
            None,
            batch_size,
        )
        .await;

        // ISB Reader Orchestrator
        let input_stream = Stream::new("sink-input-buffer", "test-in", 0);
        let buf_reader_config = BufferReaderConfig {
            streams: vec![input_stream.clone()],
            wip_ack_interval: Duration::from_millis(10),
            ..Default::default()
        };

        let reader_components = ISBReaderComponents {
            vertex_type: "Sink".to_string(),
            stream: input_stream,
            config: buf_reader_config,
            tracker: tracker.clone(),
            batch_size,
            read_timeout: Duration::from_millis(500),
            watermark_handle: None,
            isb_config: None,
            cln_token: cln_token.clone(),
        };

        let isb_reader: ISBReaderOrchestrator<WithSimpleBuffer> =
            ISBReaderOrchestrator::new(reader_components, input_adapter.reader(), None)
                .await
                .unwrap();

        // Create and start the SinkForwarder
        let forwarder = SinkForwarder::<WithSimpleBuffer>::new(isb_reader, sink_writer).await;

        let forwarder_cln = cln_token.clone();
        let forwarder_handle = tokio::spawn(async move { forwarder.start(forwarder_cln).await });

        // Wait until all messages appear in the output buffer
        let result = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if output_buffer.pending_count() >= MESSAGE_COUNT {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await;

        assert!(
            result.is_ok(),
            "Timed out waiting for messages in output buffer. Got {} of {}",
            output_buffer.pending_count(),
            MESSAGE_COUNT,
        );

        assert_eq!(
            output_buffer.pending_count(),
            MESSAGE_COUNT,
            "All messages should be forwarded to the sink output buffer"
        );

        // Shutdown
        cln_token.cancel();
        let forwarder_result = tokio::time::timeout(Duration::from_secs(2), forwarder_handle).await;
        assert!(
            forwarder_result.is_ok(),
            "Forwarder task should complete gracefully"
        );
    }

    // End-to-end test for sink forwarder with multiple input streams.
    // Reads from multiple SimpleBuffer-backed ISB partitions and writes to
    // separate UD sinks (one per stream) that all
    // store messages in the same SimpleBuffer for verification.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_sink_forwarder_with_multi_streams() {
        const MESSAGE_COUNT: usize = 100;
        const NUM_PARTITIONS: usize = 5;

        let cln_token = CancellationToken::new();
        let tracker = Tracker::new(None, cln_token.clone());
        let batch_size = 500;

        // Shared output buffer for verification (all UD sinks write here)
        let output_buffer = SimpleBuffer::new(10000, 0, "sink-multi-output");
        // Shared counter to ensure globally unique IDs across all sink instances
        let shared_counter = Arc::new(AtomicUsize::new(0));

        // Create input buffers and streams
        let input_adapters: Vec<SimpleBufferAdapter> = (0..NUM_PARTITIONS)
            .map(|i| {
                let name: &'static str =
                    Box::leak(format!("sink-multi-input-{}", i).into_boxed_str());
                SimpleBufferAdapter::new(SimpleBuffer::new(10000, i as u16, name))
            })
            .collect();

        let input_streams: Vec<Stream> = (0..NUM_PARTITIONS)
            .map(|i| {
                let name: &'static str =
                    Box::leak(format!("default-test-sink-forwarder-in-{}", i).into_boxed_str());
                Stream::new(name, "test", i as u16)
            })
            .collect();

        // Write messages to all input buffers
        for adapter in &input_adapters {
            write_test_messages(adapter, MESSAGE_COUNT).await;
        }

        let buf_reader_config = BufferReaderConfig {
            streams: input_streams.clone(),
            wip_ack_interval: Duration::from_millis(10),
            ..Default::default()
        };

        // Create one SinkWriter + SinkForwarder per stream (matching production pattern)
        let mut forwarder_handles = vec![];
        let mut _server_handles = vec![];
        for (i, input_stream) in input_streams.iter().enumerate() {
            // Each stream gets its own UD sink server writing to the shared output buffer
            let sink_svc = SimpleBufferSink {
                output_buffer: output_buffer.clone(),
                counter: Arc::clone(&shared_counter),
            };

            let SinkTestHandle {
                sink_writer,
                ud_sink_server_handle,
                ..
            } = SinkTestHandle::create_sink(
                TestSinkType::UserDefined(sink_svc),
                None,
                None,
                batch_size,
            )
            .await;
            _server_handles.push(ud_sink_server_handle);

            let reader_components = ISBReaderComponents {
                vertex_type: "Sink".to_string(),
                stream: input_stream.clone(),
                config: buf_reader_config.clone(),
                tracker: tracker.clone(),
                batch_size,
                read_timeout: Duration::from_millis(500),
                watermark_handle: None,
                isb_config: None,
                cln_token: cln_token.clone(),
            };

            let isb_reader: ISBReaderOrchestrator<WithSimpleBuffer> = ISBReaderOrchestrator::new(
                reader_components,
                input_adapters.get(i).unwrap().reader(),
                None,
            )
            .await
            .unwrap();

            let forwarder = SinkForwarder::<WithSimpleBuffer>::new(isb_reader, sink_writer).await;

            let forwarder_cln = cln_token.clone();
            let handle = tokio::spawn(async move { forwarder.start(forwarder_cln).await });
            forwarder_handles.push(handle);
        }

        let total_expected = MESSAGE_COUNT * NUM_PARTITIONS;

        // Wait until all messages exit the input buffers
        let input_result = tokio::time::timeout(Duration::from_secs(10), async {
            for adapter in &input_adapters {
                loop {
                    if adapter.pending_count() == 0 {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            }
        })
        .await;

        assert!(
            input_result.is_ok(),
            "Timed out waiting for messages to exit input buffers."
        );

        // Ensure all messages have left the input buffers
        for adapter in &input_adapters {
            assert_eq!(
                adapter.pending_count(),
                0,
                "All messages should be consumed from input buffer"
            );
        }

        // Wait until all messages appear in the output buffer
        let output_result = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                if output_buffer.pending_count() >= total_expected {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await;

        assert!(
            output_result.is_ok(),
            "Timed out waiting for messages in output buffer. Got {} of {}",
            output_buffer.pending_count(),
            total_expected,
        );

        assert_eq!(
            output_buffer.pending_count(),
            total_expected,
            "All messages from all partitions should be forwarded to the sink"
        );

        // Shutdown
        cln_token.cancel();
        for handle in forwarder_handles {
            let forwarder_result = tokio::time::timeout(Duration::from_secs(2), handle).await;
            assert!(
                forwarder_result.is_ok(),
                "Forwarder task should complete gracefully"
            );
        }
    }

    // Test sink forwarder with a blackhole (builtin) sink using SimpleBuffer input.
    // This verifies the forwarder completes without errors even when using a
    // builtin sink that doesn't produce output.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_sink_forwarder_with_blackhole_sink() {
        const MESSAGE_COUNT: usize = 10;

        let cln_token = CancellationToken::new();
        let tracker = Tracker::new(None, cln_token.clone());
        let batch_size = 500;

        // Input buffer
        let input_adapter = SimpleBufferAdapter::new(SimpleBuffer::new(1000, 0, "bh-sink-input"));

        // Write all the messages to the input buffer
        write_test_messages(&input_adapter, MESSAGE_COUNT).await;

        // Create a blackhole sink writer
        use crate::sinker::sink::SinkClientType;
        let SinkTestHandle {
            sink_writer,
            ud_sink_server_handle: _,
            ..
        } = SinkTestHandle::create_sink::<crate::sinker::test_utils::NoOpSink>(
            TestSinkType::BuiltIn(SinkClientType::Blackhole),
            None,
            None,
            batch_size,
        )
        .await;

        // ISB Reader Orchestrator
        let input_stream = Stream::new("bh-sink-input-buffer", "test-in", 0);
        let buf_reader_config = BufferReaderConfig {
            streams: vec![input_stream.clone()],
            wip_ack_interval: Duration::from_millis(10),
            ..Default::default()
        };

        let reader_components = ISBReaderComponents {
            vertex_type: "Sink".to_string(),
            stream: input_stream,
            config: buf_reader_config,
            tracker: tracker.clone(),
            batch_size,
            read_timeout: Duration::from_millis(500),
            watermark_handle: None,
            isb_config: None,
            cln_token: cln_token.clone(),
        };

        let isb_reader: ISBReaderOrchestrator<WithSimpleBuffer> =
            ISBReaderOrchestrator::new(reader_components, input_adapter.reader(), None)
                .await
                .unwrap();

        // Create and start the SinkForwarder
        let forwarder = SinkForwarder::<WithSimpleBuffer>::new(isb_reader, sink_writer).await;

        let forwarder_cln = cln_token.clone();
        let forwarder_handle = tokio::spawn(async move { forwarder.start(forwarder_cln).await });

        // Wait until all messages are consumed from the input buffer
        let result = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if input_adapter.pending_count() == 0 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await;

        assert!(
            result.is_ok(),
            "Timed out waiting for messages to be consumed. Remaining: {}",
            input_adapter.pending_count(),
        );

        assert_eq!(
            input_adapter.pending_count(),
            0,
            "All messages should be consumed from the input buffer"
        );

        // Shutdown
        cln_token.cancel();
        let forwarder_result = tokio::time::timeout(Duration::from_secs(2), forwarder_handle).await;
        assert!(
            forwarder_result.is_ok(),
            "Forwarder task should complete gracefully"
        );
    }

    struct PanickingSink;

    #[tonic::async_trait]
    impl sink::Sinker for PanickingSink {
        async fn sink(&self, _input: Receiver<sink::SinkRequest>) -> Vec<sink::Response> {
            panic!("Panic in sink");
        }
    }

    // Test sink forwarder with a panic in the sink writer.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_sink_forwarder_with_panic() {
        const MESSAGE_COUNT: usize = 10;

        let cln_token = CancellationToken::new();
        let tracker = Tracker::new(None, cln_token.clone());
        let batch_size = 500;

        // Input buffer
        let input_adapter = SimpleBufferAdapter::new(SimpleBuffer::new(1000, 0, "sink-input"));

        // Write all the messages to the input buffer
        write_test_messages(&input_adapter, MESSAGE_COUNT).await;

        let SinkTestHandle {
            sink_writer,
            ud_sink_server_handle: _server_handle,
            ..
        } = SinkTestHandle::create_sink(
            TestSinkType::UserDefined(PanickingSink),
            None,
            None,
            batch_size,
        )
        .await;

        // ISB Reader Orchestrator
        let input_stream = Stream::new("sink-input-buffer", "test-in", 0);
        let buf_reader_config = BufferReaderConfig {
            streams: vec![input_stream.clone()],
            wip_ack_interval: Duration::from_millis(10),
            ..Default::default()
        };

        let reader_components = ISBReaderComponents {
            vertex_type: "Sink".to_string(),
            stream: input_stream,
            config: buf_reader_config,
            tracker: tracker.clone(),
            batch_size,
            read_timeout: Duration::from_millis(500),
            watermark_handle: None,
            isb_config: None,
            cln_token: cln_token.clone(),
        };

        let isb_reader: ISBReaderOrchestrator<WithSimpleBuffer> =
            ISBReaderOrchestrator::new(reader_components, input_adapter.reader(), None)
                .await
                .unwrap();

        // Create and start the SinkForwarder
        let forwarder = SinkForwarder::<WithSimpleBuffer>::new(isb_reader, sink_writer).await;

        let forwarder_cln = cln_token.clone();
        let forwarder_handle = tokio::spawn(async move { forwarder.start(forwarder_cln).await });

        tokio::time::sleep(Duration::from_millis(500)).await;

        assert_eq!(
            input_adapter.pending_count(),
            MESSAGE_COUNT,
            "All messages should stay in the input buffer"
        );

        // Shutdown
        cln_token.cancel();
        let forwarder_result = tokio::time::timeout(Duration::from_secs(2), forwarder_handle).await;
        assert!(
            forwarder_result.is_ok(),
            "Forwarder task should complete gracefully"
        );
    }
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
            vertex_config: VertexConfig::Sink(Box::new(SinkVtxConfig {
                sink_config: SinkConfig {
                    sink_type: SinkType::Blackhole(BlackholeConfig::default()),
                    retry_config: None,
                },
                fb_sink_config: None,
                on_success_sink_config: None,
                serving_store_config: None,
            })),
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
                (**sink_config).clone()
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
