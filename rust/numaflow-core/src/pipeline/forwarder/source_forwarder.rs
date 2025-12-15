use crate::config::is_mono_vertex;
use crate::config::pipeline::{PipelineConfig, SourceVtxConfig};
use crate::error::Error;
use crate::metrics::{
    ComponentHealthChecks, LagReader, MetricsState, PendingReaderTasks, PipelineComponents,
    WatermarkFetcherState,
};
use crate::pipeline::PipelineContext;

use crate::pipeline::isb::writer::{ISBWriter, ISBWriterComponents};
use crate::shared::create_components;
use crate::shared::metrics::start_metrics_server;
use crate::source::Source;
use crate::tracker::Tracker;
use crate::transformer::Transformer;
use crate::typ::{
    NumaflowTypeConfig, WithInMemoryRateLimiter, WithRedisRateLimiter, WithoutRateLimiter,
    build_in_memory_rate_limiter_config, build_redis_rate_limiter_config,
    should_use_redis_rate_limiter,
};
use crate::watermark::WatermarkHandle;
use crate::watermark::source::SourceWatermarkHandle;
use crate::{error, shared};
use async_nats::jetstream::Context;
use serving::callback::CallbackHandler;
use tokio_util::sync::CancellationToken;
use tracing::info;

/// Source forwarder is the orchestrator which starts streaming source, a transformer, and an isb writer
/// and manages the lifecycle of these components.
pub(crate) struct SourceForwarder<C: crate::typ::NumaflowTypeConfig> {
    source: Source<C>,
    writer: ISBWriter,
}

impl<C: crate::typ::NumaflowTypeConfig> SourceForwarder<C> {
    pub(crate) fn new(source: Source<C>, writer: ISBWriter) -> Self {
        Self { source, writer }
    }

    /// Start the forwarder by starting the streaming source, transformer, and writer.
    pub(crate) async fn start(self, cln_token: CancellationToken) -> error::Result<()> {
        let (messages_stream, reader_handle) = self.source.streaming_read(cln_token.clone())?;

        let writer_handle = self
            .writer
            .streaming_write(messages_stream, cln_token.clone())
            .await?;

        let (reader_result, sink_writer_result) = tokio::try_join!(reader_handle, writer_handle)
            .map_err(|e| {
                error!(?e, "Error while joining reader and sink writer");
                Error::Forwarder(format!("Error while joining reader and sink writer: {e}"))
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

pub(crate) async fn start_source_forwarder(
    cln_token: CancellationToken,
    js_context: Context,
    config: PipelineConfig,
    source_config: SourceVtxConfig,
    source_watermark_handle: Option<SourceWatermarkHandle>,
) -> error::Result<()> {
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

    let tracker = Tracker::new(serving_callback_handler, cln_token.clone());

    let context = PipelineContext {
        cln_token: cln_token.clone(),
        js_context: &js_context,
        config: &config,
        tracker: tracker.clone(),
    };

    let writers = create_components::create_js_writers(
        &config.to_vertex_config,
        js_context.clone(),
        config.isb_config.as_ref(),
        cln_token.clone(),
    )
    .await?;

    let writer_components = ISBWriterComponents {
        config: config.to_vertex_config.clone(),
        writers,
        paf_concurrency: config.writer_concurrency,
        watermark_handle: source_watermark_handle.clone().map(WatermarkHandle::Source),
        vertex_type: config.vertex_type,
    };

    let buffer_writer = ISBWriter::new(writer_components);
    let transformer = create_components::create_transformer(
        config.batch_size,
        config.graceful_shutdown_time,
        source_config.transformer_config.clone(),
        tracker.clone(),
        cln_token.clone(),
    )
    .await?;

    // Apply rate limiting dispatch pattern similar to other forwarders
    if let Some(rate_limit_config) = &config.rate_limit {
        if should_use_redis_rate_limiter(rate_limit_config) {
            let redis_config =
                build_redis_rate_limiter_config(rate_limit_config, cln_token.clone()).await?;

            run_source_forwarder::<WithRedisRateLimiter>(
                &context,
                &source_config,
                transformer,
                source_watermark_handle,
                buffer_writer,
                Some(redis_config.throttling_config),
            )
            .await?
        } else {
            let in_mem_config =
                build_in_memory_rate_limiter_config(rate_limit_config, cln_token.clone()).await?;

            run_source_forwarder::<WithInMemoryRateLimiter>(
                &context,
                &source_config,
                transformer,
                source_watermark_handle,
                buffer_writer,
                Some(in_mem_config.throttling_config),
            )
            .await?
        }
    } else {
        run_source_forwarder::<WithoutRateLimiter>(
            &context,
            &source_config,
            transformer,
            source_watermark_handle,
            buffer_writer,
            None,
        )
        .await?
    };

    Ok(())
}

/// Starts source forwarder.
async fn run_source_forwarder<C: NumaflowTypeConfig>(
    context: &PipelineContext<'_>,
    source_config: &SourceVtxConfig,
    transformer: Option<Transformer>,
    source_watermark_handle: Option<SourceWatermarkHandle>,
    buffer_writer: ISBWriter,
    rate_limiter: Option<C::RateLimiter>,
) -> error::Result<()> {
    let source = create_components::create_source::<C>(
        context.config.batch_size,
        context.config.read_timeout,
        &source_config.source_config,
        context.tracker.clone(),
        transformer,
        source_watermark_handle.clone(),
        context.cln_token.clone(),
        rate_limiter,
    )
    .await?;

    // only check the pending and lag for source for pod_id = 0
    let _pending_reader_handle: Option<PendingReaderTasks> = if context.config.replica == 0 {
        let pending_reader = shared::metrics::create_pending_reader::<C>(
            &context.config.metrics_config,
            LagReader::Source(Box::new(source.clone())),
        )
        .await;
        info!("Started pending reader");
        Some(pending_reader.start(is_mono_vertex()).await)
    } else {
        None
    };

    start_metrics_server::<C>(
        context.config.metrics_config.clone(),
        MetricsState {
            health_checks: ComponentHealthChecks::Pipeline(Box::new(PipelineComponents::Source(
                Box::new(source.clone()),
            ))),
            watermark_fetcher_state: source_watermark_handle.map(|handle| WatermarkFetcherState {
                watermark_handle: WatermarkHandle::Source(handle),
                partitions: vec![0], // Source vertices always have single partition
            }),
        },
    )
    .await;

    let forwarder = SourceForwarder::<C>::new(source, buffer_writer);

    forwarder.start(context.cln_token.clone()).await
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::collections::HashSet;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use crate::Result;
    use crate::config::components::metrics::MetricsConfig;
    use crate::config::components::source::{GeneratorConfig, SourceConfig};
    use crate::config::pipeline::isb::BufferFullStrategy::RetryUntilSuccess;
    use crate::config::pipeline::isb::{BufferWriterConfig, Stream};
    use crate::config::pipeline::{ToVertexConfig, VertexConfig, VertexType, isb};
    use crate::pipeline::forwarder::source_forwarder::SourceForwarder;
    use crate::pipeline::isb::writer::{ISBWriter, ISBWriterComponents};
    use crate::shared::grpc::create_rpc_channel;
    use crate::source::user_defined::new_source;
    use crate::source::{Source, SourceType};
    use crate::tracker::Tracker;
    use crate::transformer::Transformer;
    use async_nats::jetstream;
    use async_nats::jetstream::{consumer, stream};
    use chrono::Utc;
    use numaflow::shared::ServerExtras;
    use numaflow::source::{Message, Offset, SourceReadRequest};
    use numaflow::{source, sourcetransform};
    use numaflow_pb::clients::source::source_client::SourceClient;
    use numaflow_pb::clients::sourcetransformer::source_transform_client::SourceTransformClient;
    use tempfile::TempDir;
    use tokio::sync::mpsc::Sender;
    use tokio::sync::oneshot;
    use tokio::task::JoinHandle;
    use tokio_stream::StreamExt;
    use tokio_util::sync::CancellationToken;

    struct SimpleSource {
        num: usize,
        sent_count: AtomicUsize,
        yet_to_ack: std::sync::RwLock<HashSet<String>>,
    }

    impl SimpleSource {
        fn new(num: usize) -> Self {
            Self {
                num,
                sent_count: AtomicUsize::new(0),
                yet_to_ack: std::sync::RwLock::new(HashSet::new()),
            }
        }
    }

    #[tonic::async_trait]
    impl source::Sourcer for SimpleSource {
        async fn read(&self, request: SourceReadRequest, transmitter: Sender<Message>) {
            let event_time = Utc::now();
            let mut message_offsets = Vec::with_capacity(request.count);

            for i in 0..request.count {
                if self.sent_count.load(Ordering::SeqCst) >= self.num {
                    return;
                }

                let offset = format!("{}-{}", event_time.timestamp_nanos_opt().unwrap(), i);
                transmitter
                    .send(Message {
                        value: b"hello".to_vec(),
                        event_time,
                        offset: Offset {
                            offset: offset.clone().into_bytes(),
                            partition_id: 0,
                        },
                        keys: vec![],
                        headers: Default::default(),
                        user_metadata: None,
                    })
                    .await
                    .unwrap();
                message_offsets.push(offset);
                self.sent_count.fetch_add(1, Ordering::SeqCst);
            }
            self.yet_to_ack.write().unwrap().extend(message_offsets);
        }

        async fn ack(&self, offsets: Vec<Offset>) {
            for offset in offsets {
                self.yet_to_ack
                    .write()
                    .unwrap()
                    .remove(&String::from_utf8(offset.offset).unwrap());
            }
        }

        async fn nack(&self, _offsets: Vec<Offset>) {}

        async fn pending(&self) -> Option<usize> {
            Some(
                self.num - self.sent_count.load(Ordering::SeqCst)
                    + self.yet_to_ack.read().unwrap().len(),
            )
        }

        async fn partitions(&self) -> Option<Vec<i32>> {
            Some(vec![1, 2])
        }
    }

    struct SimpleTransformer;

    #[tonic::async_trait]
    impl sourcetransform::SourceTransformer for SimpleTransformer {
        async fn transform(
            &self,
            input: sourcetransform::SourceTransformRequest,
        ) -> Vec<sourcetransform::Message> {
            let message =
                sourcetransform::Message::new(input.value, Utc::now()).with_keys(input.keys);
            vec![message]
        }
    }

    #[cfg(feature = "nats-tests")]
    #[tokio::test]
    async fn test_source_forwarder() {
        // create the source which produces x number of messages
        let cln_token = CancellationToken::new();
        let tracker = Tracker::new(None, cln_token.clone());

        // create a transformer
        let (st_shutdown_tx, st_shutdown_rx) = oneshot::channel();
        let tmp_dir = TempDir::new().unwrap();
        let sock_file = tmp_dir.path().join("sourcetransform.sock");
        let server_info_file = tmp_dir.path().join("sourcetransformer-server-info");

        let server_info = server_info_file.clone();
        let server_socket = sock_file.clone();
        let transformer_handle = tokio::spawn(async move {
            sourcetransform::Server::new(SimpleTransformer)
                .with_socket_file(server_socket)
                .with_server_info_file(server_info)
                .start_with_shutdown(st_shutdown_rx)
                .await
                .expect("server failed");
        });

        // wait for the server to start
        tokio::time::sleep(Duration::from_millis(100)).await;
        let client = SourceTransformClient::new(create_rpc_channel(sock_file).await.unwrap());
        let transformer =
            Transformer::new(10, 10, Duration::from_secs(10), client, tracker.clone())
                .await
                .unwrap();

        let (src_shutdown_tx, src_shutdown_rx) = oneshot::channel();
        let tmp_dir = TempDir::new().unwrap();
        let sock_file = tmp_dir.path().join("source.sock");
        let server_info_file = tmp_dir.path().join("source-server-info");

        let server_info = server_info_file.clone();
        let server_socket = sock_file.clone();
        let source_handle = tokio::spawn(async move {
            // a simple source which generates total of 100 messages
            source::Server::new(SimpleSource::new(100))
                .with_socket_file(server_socket)
                .with_server_info_file(server_info)
                .start_with_shutdown(src_shutdown_rx)
                .await
                .unwrap()
        });

        // wait for the server to start
        // TODO: flaky
        tokio::time::sleep(Duration::from_millis(100)).await;

        let client = SourceClient::new(create_rpc_channel(sock_file).await.unwrap());

        let (src_read, src_ack, lag_reader) = new_source(
            client,
            5,
            Duration::from_millis(1000),
            cln_token.clone(),
            true,
        )
        .await
        .map_err(|e| panic!("failed to create source reader: {:?}", e))
        .unwrap();

        let source: Source<crate::typ::WithoutRateLimiter> = Source::new(
            5,
            SourceType::UserDefinedSource(Box::new(src_read), Box::new(src_ack), lag_reader),
            tracker.clone(),
            true,
            Some(transformer),
            None,
            None,
        )
        .await;

        // create a js writer
        let js_url = "localhost:4222";
        // Create JetStream context
        let client = async_nats::connect(js_url).await.unwrap();
        let context = jetstream::new(client);

        let stream = Stream::new("test_source_forwarder", "test", 0);
        // Delete stream if it exists
        let _ = context.delete_stream(stream.name).await;
        let _stream = context
            .get_or_create_stream(stream::Config {
                name: stream.name.to_string(),
                subjects: vec![stream.name.into()],
                max_message_size: 1024,
                ..Default::default()
            })
            .await
            .unwrap();

        let _consumer = context
            .create_consumer_on_stream(
                consumer::Config {
                    name: Some(stream.name.to_string()),
                    ack_policy: consumer::AckPolicy::Explicit,
                    ..Default::default()
                },
                stream.name,
            )
            .await
            .unwrap();

        let writer_config = BufferWriterConfig {
            streams: vec![stream.clone()],
            ..Default::default()
        };

        let mut writers = std::collections::HashMap::new();
        writers.insert(
            stream.name,
            crate::pipeline::isb::jetstream::js_writer::JetStreamWriter::new(
                stream.clone(),
                context.clone(),
                writer_config.clone(),
                None,
                cln_token.clone(),
            )
            .await
            .unwrap(),
        );

        let writer_components = ISBWriterComponents {
            config: vec![ToVertexConfig {
                partitions: 1,
                writer_config,
                conditions: None,
                name: "test-vertex",
                to_vertex_type: VertexType::MapUDF,
            }],
            writers,
            paf_concurrency: 100,
            watermark_handle: None,
            vertex_type: VertexType::Source,
        };
        let writer = ISBWriter::new(writer_components);

        // create the forwarder with the source, transformer, and writer
        let forwarder = SourceForwarder::new(source.clone(), writer);

        let cancel_token = cln_token.clone();
        let forwarder_handle: JoinHandle<Result<()>> = tokio::spawn(async move {
            forwarder.start(cancel_token).await?;
            Ok(())
        });

        // wait for one sec to check if the pending becomes zero, because all the messages
        // should be read and acked; if it doesn't, then fail the test
        let tokio_result = tokio::time::timeout(Duration::from_secs(1), async move {
            loop {
                let pending = source.pending().await.unwrap();
                if pending == Some(0) {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await;

        assert!(
            tokio_result.is_ok(),
            "Timeout occurred before pending became zero"
        );
        cln_token.cancel();
        forwarder_handle.await.unwrap().unwrap();
        st_shutdown_tx.send(()).unwrap();
        src_shutdown_tx.send(()).unwrap();
        source_handle.await.unwrap();
        transformer_handle.await.unwrap();
    }

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
            writer_concurrency: 30000,
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
                to_vertex_type: VertexType::Sink,
            }],
            vertex_type: VertexType::Source,
            vertex_config: VertexConfig::Source(SourceVtxConfig {
                source_config: SourceConfig {
                    read_ahead: false,
                    source_type: crate::config::components::source::SourceType::Generator(
                        GeneratorConfig {
                            rpu: 10,
                            content: bytes::Bytes::new(),
                            duration: Duration::from_secs(1),
                            value: None,
                            key_count: 0,
                            msg_size_bytes: 300,
                            jitter: Duration::from_millis(0),
                        },
                    ),
                },
                transformer_config: None,
            }),
            metrics_config: MetricsConfig {
                metrics_server_listen_port: 2469,
                lag_check_interval_in_secs: 5,
                lag_refresh_interval_in_secs: 3,
                lookback_window_in_secs: 120,
            },
            ..Default::default()
        };

        // Extract the source config from the pipeline config
        let source_vtx_config =
            if let VertexConfig::Source(ref source_config) = pipeline_config.vertex_config {
                source_config.clone()
            } else {
                panic!("Expected source vertex config");
            };

        // For this test, we don't have watermark config, so watermark handle is None
        let source_watermark_handle = None;

        let cancellation_token = CancellationToken::new();
        let forwarder_task = tokio::spawn({
            let cancellation_token = cancellation_token.clone();
            let context = context.clone();
            async move {
                start_source_forwarder(
                    cancellation_token,
                    context,
                    pipeline_config,
                    source_vtx_config,
                    source_watermark_handle,
                )
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
}
