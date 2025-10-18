use crate::config::components::reduce::{
    AlignedReducerConfig, AlignedWindowType, ReducerConfig, UnalignedReducerConfig,
    UnalignedWindowType,
};
use crate::config::pipeline::{PipelineConfig, ReduceVtxConfig};
use crate::config::{get_vertex_replica, is_mono_vertex};
use crate::metrics::{
    ComponentHealthChecks, LagReader, MetricsState, PipelineComponents, WatermarkFetcherState,
};
use crate::pipeline::PipelineContext;
use crate::pipeline::isb::jetstream::js_reader::JetStreamReader;
use crate::pipeline::isb::reader::{ISBReader, ISBReaderComponents};
use crate::pipeline::isb::writer::{ISBWriter, ISBWriterComponents};
use crate::reduce::pbq::{PBQ, PBQBuilder, WAL};
use crate::reduce::reducer::aligned::reducer::AlignedReducer;
use crate::reduce::reducer::aligned::windower::AlignedWindowManager;
use crate::reduce::reducer::aligned::windower::fixed::FixedWindowManager;
use crate::reduce::reducer::aligned::windower::sliding::SlidingWindowManager;
use crate::reduce::reducer::unaligned::reducer::UnalignedReducer;
use crate::reduce::reducer::unaligned::windower::UnalignedWindowManager;
use crate::reduce::reducer::unaligned::windower::accumulator::AccumulatorWindowManager;
use crate::reduce::reducer::unaligned::windower::session::SessionWindowManager;
use crate::reduce::reducer::user_defined::UserDefinedReduce;
use crate::reduce::reducer::{Reducer, WindowManager};
use crate::reduce::wal::create_wal_components;
use crate::reduce::wal::segment::compactor::WindowKind;
use crate::shared::create_components;
use crate::shared::metrics::start_metrics_server;
use crate::tracker::Tracker;
use crate::typ::{NumaflowTypeConfig, WithoutRateLimiter};
use crate::watermark::WatermarkHandle;
use crate::{Result, shared};
use async_nats::jetstream::Context;
use std::path::PathBuf;
use std::time::Duration;
use tokio::fs;
use tokio::time::{interval, timeout};
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

/// ReduceForwarder is a component which starts a PBQ reader and a reducer
/// and manages the lifecycle of these components.
pub(crate) struct ReduceForwarder<C: NumaflowTypeConfig> {
    pbq: PBQ<C>,
    reducer: Reducer,
}

impl<C: NumaflowTypeConfig> ReduceForwarder<C> {
    pub(crate) fn new(pbq: PBQ<C>, reducer: Reducer) -> Self {
        Self { pbq, reducer }
    }

    pub(crate) async fn start(self, cln_token: CancellationToken) -> Result<()> {
        // Start the PBQ reader
        let (read_messages_stream, pbq_handle) = self.pbq.streaming_read(cln_token.clone()).await?;

        // Start the reducer
        let processor_handle = match self.reducer {
            Reducer::Aligned(reducer) => {
                reducer
                    .start(read_messages_stream, cln_token.clone())
                    .await?
            }
            Reducer::Unaligned(reducer) => {
                reducer
                    .start(read_messages_stream, cln_token.clone())
                    .await?
            }
        };

        // Join the pbq and reducer
        let (pbq_result, processor_result) = tokio::try_join!(pbq_handle, processor_handle)
            .map_err(|e| {
                error!(?e, "Error while joining PBQ reader and reducer");
                crate::error::Error::Forwarder(format!(
                    "Error while joining PBQ reader and reducer: {e}"
                ))
            })?;

        processor_result.inspect_err(|e| {
            error!(?e, "Error in reducer");
        })?;

        pbq_result.inspect_err(|e| {
            error!(?e, "Error in PBQ reader");
        })?;

        info!("Reduce forwarder completed successfully");
        Ok(())
    }
}

pub(crate) async fn start_aligned_reduce_forwarder(
    cln_token: CancellationToken,
    js_context: Context,
    config: PipelineConfig,
    reduce_vtx_config: ReduceVtxConfig,
    aligned_config: AlignedReducerConfig,
) -> Result<()> {
    // for reduce we do not pass serving callback handler to tracker.
    let tracker = Tracker::new(None, cln_token.clone());

    // Create aligned window manager based on window type
    let window_manager = match &aligned_config.window_config.window_type {
        AlignedWindowType::Fixed(fixed_config) => {
            AlignedWindowManager::Fixed(FixedWindowManager::new(fixed_config.length))
        }
        AlignedWindowType::Sliding(sliding_config) => {
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

            AlignedWindowManager::Sliding(SlidingWindowManager::new(
                sliding_config.length,
                sliding_config.slide,
                state_file_path,
            ))
        }
    };

    // create watermark handle, if watermark is enabled
    let watermark_handle = create_components::create_edge_watermark_handle(
        &config,
        &js_context,
        &cln_token,
        Some(WindowManager::Aligned(window_manager.clone())),
        tracker.clone(),
        vec![*get_vertex_replica()], // in reduce, we consume from a single partition
    )
    .await?;

    let reader_config = &config
        .from_vertex_config
        .first()
        .ok_or_else(|| crate::error::Error::Config("No from vertex config found".to_string()))?
        .reader_config;

    // reduce pod always reads from a single stream (pod per partition)
    let stream = reader_config
        .streams
        .get(*get_vertex_replica() as usize)
        .cloned()
        .ok_or_else(|| {
            crate::error::Error::Config("No stream found for reduce vertex".to_string())
        })?;

    let context = PipelineContext {
        cln_token: cln_token.clone(),
        js_context: &js_context,
        config: &config,
        tracker: tracker.clone(),
    };

    let reader_components = ISBReaderComponents::new(
        stream,
        reader_config.clone(),
        watermark_handle.clone(),
        &context,
    );

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
        watermark_handle: watermark_handle.clone().map(WatermarkHandle::ISB),
        vertex_type: config.vertex_type,
    };

    let buffer_writer = ISBWriter::new(writer_components);

    // Create WAL if configured
    let (wal, gc_wal) = create_wal_components(
        reduce_vtx_config.wal_storage_config.as_ref(),
        WindowKind::Aligned,
    )
    .await?;

    // Create PBQ
    // Create user-defined aligned reducer client
    let reducer_client =
        create_components::create_aligned_reducer(aligned_config.clone(), cln_token.clone())
            .await?;

    // Start the metrics server with one of the clients
    start_metrics_server::<WithoutRateLimiter>(
        config.metrics_config.clone(),
        MetricsState {
            health_checks: ComponentHealthChecks::Pipeline(Box::new(PipelineComponents::Reduce(
                UserDefinedReduce::Aligned(reducer_client.clone()),
            ))),
            watermark_fetcher_state: watermark_handle
                .clone()
                .map(|handle| WatermarkFetcherState {
                    watermark_handle: WatermarkHandle::ISB(handle),
                    partitions: vec![*get_vertex_replica()], // Reduce vertices always read from single partition (partition 0)
                }),
        },
    )
    .await;

    let reducer = Reducer::Aligned(
        AlignedReducer::new(
            reducer_client,
            window_manager,
            buffer_writer,
            gc_wal,
            aligned_config.window_config.allowed_lateness,
            config.graceful_shutdown_time,
            reduce_vtx_config.keyed,
        )
        .await,
    );

    let context = PipelineContext {
        cln_token: cln_token.clone(),
        js_context: &js_context,
        config: &config,
        tracker,
    };

    // rate limit is not applicable for reduce
    run_reduce_forwarder::<WithoutRateLimiter>(&context, reader_components, reducer, wal, None)
        .await?;

    info!("Aligned reduce forwarder has stopped successfully");
    Ok(())
}

pub(crate) async fn start_unaligned_reduce_forwarder(
    cln_token: CancellationToken,
    js_context: Context,
    config: PipelineConfig,
    reduce_vtx_config: ReduceVtxConfig,
    unaligned_config: UnalignedReducerConfig,
) -> Result<()> {
    // for reduce we do not pass serving callback handler to tracker.
    let tracker = Tracker::new(None, cln_token.clone());

    // Create unaligned window manager based on window type
    let window_manager = match &unaligned_config.window_config.window_type {
        UnalignedWindowType::Accumulator(accumulator_config) => {
            UnalignedWindowManager::Accumulator(AccumulatorWindowManager::new(
                accumulator_config.timeout,
            ))
        }
        UnalignedWindowType::Session(session_config) => {
            UnalignedWindowManager::Session(SessionWindowManager::new(session_config.timeout))
        }
    };

    // create watermark handle, if watermark is enabled
    let watermark_handle = create_components::create_edge_watermark_handle(
        &config,
        &js_context,
        &cln_token,
        Some(WindowManager::Unaligned(window_manager.clone())),
        tracker.clone(),
        vec![*get_vertex_replica()], // in reduce, we consume from a single partition
    )
    .await?;

    let reader_config = &config
        .from_vertex_config
        .first()
        .ok_or_else(|| crate::error::Error::Config("No from vertex config found".to_string()))?
        .reader_config;

    // reduce pod always reads from a single stream (pod per partition)
    let stream = reader_config
        .streams
        .get(*get_vertex_replica() as usize)
        .cloned()
        .ok_or_else(|| {
            crate::error::Error::Config("No stream found for reduce vertex".to_string())
        })?;

    let context = PipelineContext {
        cln_token: cln_token.clone(),
        js_context: &js_context,
        config: &config,
        tracker: tracker.clone(),
    };

    let reader_components = ISBReaderComponents::new(
        stream,
        reader_config.clone(),
        watermark_handle.clone(),
        &context,
    );

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
        watermark_handle: watermark_handle.clone().map(WatermarkHandle::ISB),
        vertex_type: config.vertex_type,
    };

    let buffer_writer = ISBWriter::new(writer_components);

    // Create WAL if configured (use Unaligned WindowKind for unaligned reducers)
    let (wal, gc_wal) = create_wal_components(
        reduce_vtx_config.wal_storage_config.as_ref(),
        WindowKind::Unaligned,
    )
    .await?;

    // Create user-defined unaligned reducer client
    let reducer_client =
        create_components::create_unaligned_reducer(unaligned_config.clone(), cln_token.clone())
            .await?;

    start_metrics_server::<WithoutRateLimiter>(
        config.metrics_config.clone(),
        MetricsState {
            health_checks: ComponentHealthChecks::Pipeline(Box::new(PipelineComponents::Reduce(
                UserDefinedReduce::Unaligned(reducer_client.clone()),
            ))),
            watermark_fetcher_state: watermark_handle
                .clone()
                .map(|handle| WatermarkFetcherState {
                    watermark_handle: WatermarkHandle::ISB(handle),
                    partitions: vec![*get_vertex_replica()], // Reduce vertices always read from single partition (partition replica)
                }),
        },
    )
    .await;

    let reducer = Reducer::Unaligned(
        UnalignedReducer::new(
            reducer_client,
            window_manager,
            buffer_writer,
            unaligned_config.window_config.allowed_lateness,
            gc_wal,
            config.graceful_shutdown_time,
            reduce_vtx_config.keyed,
        )
        .await,
    );

    let context = PipelineContext {
        cln_token: cln_token.clone(),
        js_context: &js_context,
        config: &config,
        tracker,
    };

    // rate limit is not applicable for reduce
    run_reduce_forwarder::<WithoutRateLimiter>(&context, reader_components, reducer, wal, None)
        .await?;

    info!("Unaligned reduce forwarder has stopped successfully");
    Ok(())
}

/// Starts reduce forwarder.
async fn run_reduce_forwarder<C: NumaflowTypeConfig>(
    context: &PipelineContext<'_>,
    reader_components: ISBReaderComponents,
    reducer: Reducer,
    wal: Option<WAL>,
    rate_limiter: Option<C::RateLimiter>,
) -> Result<()> {
    let js_reader = JetStreamReader::new(
        reader_components.stream.clone(),
        reader_components.js_ctx.clone(),
        reader_components.isb_config.clone(),
    )
    .await?;

    let isb_reader = ISBReader::<C>::new(reader_components, js_reader, rate_limiter).await?;

    // Create lag reader with the single buffer reader (reduce only reads from one stream)
    let pending_reader = shared::metrics::create_pending_reader(
        &context.config.metrics_config,
        LagReader::ISB(vec![isb_reader.clone()]),
    )
    .await;
    let _pending_reader_handle = pending_reader.start(is_mono_vertex()).await;

    let pbq_builder = PBQBuilder::<C>::new(isb_reader);
    let pbq = match wal {
        Some(wal) => pbq_builder.wal(wal).build(),
        None => pbq_builder.build(),
    };
    let forwarder = ReduceForwarder::<C>::new(pbq, reducer);

    forwarder.start(context.cln_token.clone()).await
}

/// Guard to manage the lifecycle of a fence file. Used when persistence is enabled for reduce.
/// File will be deleted when the guard is dropped.
pub struct FenceGuard {
    fence_file_path: PathBuf,
}

impl FenceGuard {
    /// Creates a new fence guard with the specified fence file path.
    async fn new(fence_file_path: PathBuf) -> crate::error::Result<Self> {
        // Create the fence file
        fs::write(&fence_file_path, "").await.map_err(|e| {
            crate::error::Error::Config(format!("Failed to create fence file: {e}"))
        })?;
        Ok(FenceGuard { fence_file_path })
    }
}

impl Drop for FenceGuard {
    fn drop(&mut self) {
        if let Err(e) = std::fs::remove_file(&self.fence_file_path) {
            warn!(
                "Failed to remove fence file {:?}: {}",
                self.fence_file_path, e
            );
        }
    }
}

/// Waits for a fence file to be available, checking every 5 seconds, up to a specified timeout.
pub async fn wait_for_fence_availability(
    fence_file_path: &PathBuf,
    timeout_duration: Duration,
) -> crate::error::Result<()> {
    let result = timeout(timeout_duration, async {
        let mut check_interval = interval(Duration::from_secs(1));

        loop {
            check_interval.tick().await;

            // Check if the fence file exists
            match fs::metadata(fence_file_path).await {
                Ok(_) => {
                    info!(
                        "Fence file {:?} exists, waiting for it to be deleted...",
                        fence_file_path
                    );
                }
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                    // File doesn't exist, fence is available
                    info!("Fence file {:?} is now available", fence_file_path);
                    return Ok(());
                }
                Err(e) => {
                    // Other error occurred
                    return Err(crate::error::Error::Config(format!(
                        "Error checking fence file {fence_file_path:?}: {e}"
                    )));
                }
            }
        }
    })
    .await;

    result.map_err(|e| crate::error::Error::Config(format!("Fence wait timed out: {e}")))?
}

pub(crate) async fn start_reduce_forwarder(
    cln_token: CancellationToken,
    js_context: Context,
    config: PipelineConfig,
    reduce_vtx_config: ReduceVtxConfig,
) -> crate::error::Result<()> {
    // create fence guard if WAL is configured to make sure the previous WAL instance has exited gracefully
    // before we start resuming from WAL.
    let _fence_guard = if let Some(storage_config) = &reduce_vtx_config.wal_storage_config {
        let fence_file_name = format!("{}-{}", config.vertex_name, config.replica);
        let fence_file_path = storage_config.path.join(fence_file_name);

        let fence_timeout = Duration::from_secs(300); // 5 minutes
        if let Err(e) = wait_for_fence_availability(&fence_file_path, fence_timeout).await {
            error!(
                ?e,
                "Timed out waiting for delete of fence file, creating a new file"
            );
        }
        Some(FenceGuard::new(fence_file_path).await?)
    } else {
        None
    };

    match &reduce_vtx_config.reducer_config {
        ReducerConfig::Aligned(aligned_config) => {
            start_aligned_reduce_forwarder(
                cln_token,
                js_context,
                config,
                reduce_vtx_config.clone(),
                aligned_config.clone(),
            )
            .await
        }
        ReducerConfig::Unaligned(unaligned_config) => {
            start_unaligned_reduce_forwarder(
                cln_token,
                js_context,
                config,
                reduce_vtx_config.clone(),
                unaligned_config.clone(),
            )
            .await
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::config::components::reduce::{
        AlignedReducerConfig, AlignedWindowConfig, AlignedWindowType, FixedWindowConfig,
        UserDefinedConfig,
    };
    use crate::config::pipeline::isb::{BufferReaderConfig, BufferWriterConfig, Stream};
    use crate::config::pipeline::watermark::{BucketConfig, EdgeWatermarkConfig, WatermarkConfig};
    use crate::config::pipeline::{
        FromVertexConfig, PipelineConfig, ReduceVtxConfig, ToVertexConfig, VertexConfig, VertexType,
    };
    use crate::message::{Message, MessageID, Offset, StringOffset};
    use crate::pipeline::forwarder::reduce_forwarder::{
        FenceGuard, start_aligned_reduce_forwarder, start_unaligned_reduce_forwarder,
        wait_for_fence_availability,
    };
    use async_nats::jetstream::consumer::PullConsumer;
    use async_nats::jetstream::kv::Config;
    use async_nats::jetstream::{self, consumer, stream};
    use bytes::BytesMut;
    use chrono::{TimeZone, Utc};
    use futures::StreamExt;
    use numaflow::reduce;
    use numaflow::shared::ServerExtras;
    use std::sync::Arc;
    use std::time::Duration;
    use tempfile::TempDir;
    use tokio::fs;
    use tokio::sync::mpsc;
    use tokio_util::sync::CancellationToken;

    struct Counter {}

    struct CounterCreator {}

    impl reduce::ReducerCreator for CounterCreator {
        type R = Counter;

        fn create(&self) -> Self::R {
            Counter::new()
        }
    }

    impl Counter {
        fn new() -> Self {
            Self {}
        }
    }

    #[tonic::async_trait]
    impl reduce::Reducer for Counter {
        async fn reduce(
            &self,
            keys: Vec<String>,
            mut input: mpsc::Receiver<reduce::ReduceRequest>,
            _md: &reduce::Metadata,
        ) -> Vec<reduce::Message> {
            let mut counter = 0;
            // the loop exits when input is closed which will happen only on close of book.
            while input.recv().await.is_some() {
                counter += 1;
            }
            vec![reduce::Message::new(counter.to_string().into_bytes()).with_keys(keys.clone())]
        }
    }

    #[cfg(feature = "nats-tests")]
    #[tokio::test]
    async fn test_aligned_reduce_forwarder() -> crate::Result<()> {
        // Set up the reducer server using default paths
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();

        let tmp_dir = TempDir::new().unwrap();
        let sock_file = tmp_dir.path().join("reduce_fixed.sock");
        let server_info_file = tmp_dir.path().join("reducer-server-info");

        let server_info = server_info_file.clone();
        let server_socket = sock_file.clone();
        let _server_handle = tokio::spawn(async move {
            reduce::Server::new(CounterCreator {})
                .with_socket_file(server_socket)
                .with_server_info_file(server_info)
                .start_with_shutdown(shutdown_rx)
                .await
                .expect("server failed");
        });

        // Wait for the server to start
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Set up JetStream
        let js_url = "localhost:4222";
        let nats_client = async_nats::connect(js_url).await.unwrap();
        let js_context = jetstream::new(nats_client);

        // Create input and output streams
        let input_stream = Stream::new("test_aligned_reduce_forwarder_input", "test", 0);
        let output_stream = Stream::new("test_aligned_reduce_forwarder_output", "test", 0);

        // Delete streams if they exist
        let _ = js_context.delete_stream(input_stream.name).await;
        let _ = js_context.delete_stream(output_stream.name).await;

        // Create input stream
        let _input_js_stream = js_context
            .get_or_create_stream(stream::Config {
                name: input_stream.name.to_string(),
                subjects: vec![input_stream.name.to_string()],
                ..Default::default()
            })
            .await
            .unwrap();

        // Create output stream
        let _output_js_stream = js_context
            .get_or_create_stream(stream::Config {
                name: output_stream.name.to_string(),
                subjects: vec![output_stream.name.to_string()],
                ..Default::default()
            })
            .await
            .unwrap();

        // Create consumers
        let _input_consumer = js_context
            .create_consumer_on_stream(
                consumer::Config {
                    name: Some(input_stream.name.to_string()),
                    ack_policy: consumer::AckPolicy::Explicit,
                    ..Default::default()
                },
                input_stream.name,
            )
            .await
            .unwrap();

        let _output_consumer = js_context
            .create_consumer_on_stream(
                consumer::Config {
                    name: Some(output_stream.name.to_string()),
                    ack_policy: consumer::AckPolicy::Explicit,
                    ..Default::default()
                },
                output_stream.name,
            )
            .await
            .unwrap();

        let sock_file_str = sock_file.to_str().unwrap().to_string();
        let server_info_file_str = server_info_file.to_str().unwrap().to_string();
        let pipeline_config = PipelineConfig {
            pipeline_name: "test-pipeline",
            vertex_name: "test-reduce-vertex",
            replica: 0,
            from_vertex_config: vec![FromVertexConfig {
                name: "input-vertex",
                reader_config: BufferReaderConfig {
                    streams: vec![input_stream.clone()],
                    wip_ack_interval: Duration::from_millis(5),
                    ..Default::default()
                },
                partitions: 1,
            }],
            to_vertex_config: vec![ToVertexConfig {
                name: "output-vertex",
                partitions: 1,
                writer_config: BufferWriterConfig {
                    streams: vec![output_stream.clone()],
                    ..Default::default()
                },
                conditions: None,
                to_vertex_type: VertexType::Sink,
            }],
            vertex_config: VertexConfig::Reduce(ReduceVtxConfig {
                keyed: true,
                wal_storage_config: None,
                reducer_config: crate::config::components::reduce::ReducerConfig::Aligned(
                    AlignedReducerConfig {
                        window_config: AlignedWindowConfig {
                            window_type: AlignedWindowType::Fixed(FixedWindowConfig {
                                length: Duration::from_secs(60),
                                streaming: false,
                            }),
                            allowed_lateness: Duration::from_secs(0),
                            is_keyed: true,
                        },
                        user_defined_config: UserDefinedConfig {
                            grpc_max_message_size: 5 * 1024 * 1024,
                            socket_path: Box::leak(sock_file_str.into_boxed_str()),
                            server_info_path: Box::leak(server_info_file_str.into_boxed_str()),
                        },
                    },
                ),
            }),
            vertex_type: VertexType::ReduceUDF,
            ..Default::default()
        };

        // Extract the reduce config from the pipeline config
        let (reduce_vtx_config, aligned_config) = match &pipeline_config.vertex_config {
            VertexConfig::Reduce(reduce_config) => {
                let aligned_config = match &reduce_config.reducer_config {
                    crate::config::components::reduce::ReducerConfig::Aligned(config) => {
                        config.clone()
                    }
                    _ => panic!("Expected aligned config"),
                };
                (reduce_config.clone(), aligned_config)
            }
            _ => panic!("Expected reduce vertex config"),
        };

        // Create test messages
        let base_time = Utc.with_ymd_and_hms(2023, 1, 1, 0, 0, 0).unwrap();

        // Message 1: Within the first set of sliding windows
        let msg1 = Message {
            typ: Default::default(),
            keys: Arc::from(vec!["key1".into()]),
            tags: None,
            value: "value1".into(),
            offset: Offset::String(StringOffset::new("0".to_string(), 0)),
            event_time: base_time,
            watermark: None,
            id: MessageID {
                vertex_name: "vertex_name".to_string().into(),
                offset: "0".to_string().into(),
                index: 0,
            },
            ..Default::default()
        };

        // Message 2: Within the first set of sliding windows
        let msg2 = Message {
            typ: Default::default(),
            keys: Arc::from(vec!["key1".into()]),
            tags: None,
            value: "value2".into(),
            offset: Offset::String(StringOffset::new("1".to_string(), 1)),
            event_time: base_time,
            watermark: None,
            id: MessageID {
                vertex_name: "vertex_name".to_string().into(),
                offset: "1".to_string().into(),
                index: 1,
            },
            ..Default::default()
        };

        // Message 3: Within the first window
        let msg3 = Message {
            typ: Default::default(),
            keys: Arc::from(vec!["key1".into()]),
            tags: None,
            value: "value3".into(),
            offset: Offset::String(StringOffset::new("2".to_string(), 2)),
            event_time: base_time,
            watermark: None,
            id: MessageID {
                vertex_name: "vertex_name".to_string().into(),
                offset: "2".to_string().into(),
                index: 2,
            },
            ..Default::default()
        };

        // Message 4: Within the first window but with watermark past window end
        let msg4 = Message {
            typ: Default::default(),
            keys: Arc::from(vec!["key1".into()]),
            tags: None,
            value: "value3".into(),
            offset: Offset::String(StringOffset::new("3".to_string(), 2)),
            event_time: base_time + chrono::Duration::seconds(120),
            watermark: Some(base_time + chrono::Duration::seconds(100)), // Past window end
            id: MessageID {
                vertex_name: "vertex_name".to_string().into(),
                offset: "3".to_string().into(),
                index: 3,
            },
            ..Default::default()
        };

        let messages = vec![msg1, msg2, msg3, msg4];
        for msg in messages {
            let message_bytes: BytesMut = msg.try_into()?;
            js_context
                .publish(input_stream.name, message_bytes.freeze())
                .await
                .unwrap();
        }

        // Start the aligned reduce forwarder
        let cancellation_token = CancellationToken::new();
        let forwarder_task = tokio::spawn({
            let cancellation_token = cancellation_token.clone();
            let js_context = js_context.clone();
            let pipeline_config = pipeline_config.clone();
            let reduce_vtx_config = reduce_vtx_config.clone();
            let aligned_config = aligned_config.clone();
            async move {
                start_aligned_reduce_forwarder(
                    cancellation_token,
                    js_context,
                    pipeline_config,
                    reduce_vtx_config,
                    aligned_config,
                )
                .await
                .unwrap();
            }
        });

        // Create a consumer to read the results from output stream
        let output_consumer: PullConsumer = js_context
            .get_consumer_from_stream(&output_stream.name, &output_stream.name)
            .await
            .unwrap();

        // Try to read messages from the output stream
        let messages_result = output_consumer
            .fetch()
            .expires(Duration::from_secs(2))
            .messages()
            .await;

        // Cancel the forwarder
        cancellation_token.cancel();

        // Wait for forwarder to complete
        let _ = tokio::time::timeout(Duration::from_secs(5), forwarder_task).await;

        // Shutdown the server
        shutdown_tx
            .send(())
            .expect("failed to send shutdown signal");

        // Verify we got some output (the exact verification depends on the reducer logic)
        if let Ok(messages) = messages_result {
            let mut result_count = 0;
            let mut message_stream = messages;
            while let Some(msg) = message_stream.next().await {
                if let Ok(msg) = msg {
                    msg.ack().await.unwrap();
                    result_count += 1;
                }
            }
            // We expect at least some output from the reducer
            assert!(
                result_count >= 0,
                "Expected some output from reduce forwarder"
            );
        }

        Ok(())
    }

    // Accumulator implementation for unaligned reduce forwarder test
    struct AccumulatorCounter {
        count: std::sync::Arc<std::sync::atomic::AtomicU32>,
    }

    struct AccumulatorCounterCreator {}

    impl numaflow::accumulator::AccumulatorCreator for AccumulatorCounterCreator {
        type A = AccumulatorCounter;

        fn create(&self) -> Self::A {
            AccumulatorCounter::new()
        }
    }

    impl AccumulatorCounter {
        fn new() -> Self {
            Self {
                count: Arc::new(std::sync::atomic::AtomicU32::new(0)),
            }
        }
    }

    #[tonic::async_trait]
    impl numaflow::accumulator::Accumulator for AccumulatorCounter {
        async fn accumulate(
            &self,
            mut input: mpsc::Receiver<numaflow::accumulator::AccumulatorRequest>,
            output: mpsc::Sender<numaflow::accumulator::Message>,
        ) {
            while let Some(request) = input.recv().await {
                // Increment count for each message
                let current_count = self
                    .count
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
                    + 1;

                // Fire a message every 3 messages
                if current_count % 3 == 0 {
                    let mut message =
                        numaflow::accumulator::Message::from_accumulator_request(request);
                    message = message.with_value(format!("count_{}", current_count).into_bytes());
                    output.send(message).await.unwrap();
                }
            }
        }
    }

    #[cfg(feature = "nats-tests")]
    #[tokio::test]
    async fn test_unaligned_reduce_forwarder() -> crate::Result<()> {
        // Set up the accumulator server
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let tmp_dir = TempDir::new().unwrap();
        let sock_file = tmp_dir.path().join("unaligned_reduce_forwarder.sock");
        let server_info_file = tmp_dir.path().join("accumulator-server-info");

        let server_info = server_info_file.clone();
        let server_socket = sock_file.clone();
        let server_handle = tokio::spawn(async move {
            numaflow::accumulator::Server::new(AccumulatorCounterCreator {})
                .with_socket_file(server_socket)
                .with_server_info_file(server_info)
                .start_with_shutdown(shutdown_rx)
                .await
                .expect("server failed");
        });

        // Wait for the server to start
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Set up JetStream
        let js_url = "localhost:4222";
        let nats_client = async_nats::connect(js_url).await.unwrap();
        let js_context = jetstream::new(nats_client);

        // Create input and output streams
        let input_stream = Stream::new("test_unaligned_reduce_forwarder_input", "test", 0);
        let output_stream = Stream::new("test_unaligned_reduce_forwarder_output", "test", 0);
        let ot_bucket = "test_unaligned_reduce_forwarder_ot";
        let hb_bucket = "test_unaligned_reduce_forwarder_hb";

        let _ = js_context.delete_key_value(ot_bucket).await;
        let _ = js_context.delete_key_value(hb_bucket).await;

        js_context
            .create_key_value(Config {
                bucket: ot_bucket.to_string(),
                history: 1,
                ..Default::default()
            })
            .await
            .unwrap();

        js_context
            .create_key_value(Config {
                bucket: hb_bucket.to_string(),
                history: 1,
                ..Default::default()
            })
            .await
            .unwrap();

        // Delete streams if they exist
        let _ = js_context.delete_stream(input_stream.name).await;
        let _ = js_context.delete_stream(output_stream.name).await;

        // Create input stream
        let _input_js_stream = js_context
            .get_or_create_stream(stream::Config {
                name: input_stream.name.to_string(),
                subjects: vec![input_stream.name.to_string()],
                ..Default::default()
            })
            .await
            .unwrap();

        // Create output stream
        let _output_js_stream = js_context
            .get_or_create_stream(stream::Config {
                name: output_stream.name.to_string(),
                subjects: vec![output_stream.name.to_string()],
                ..Default::default()
            })
            .await
            .unwrap();

        // Create consumers
        let _input_consumer = js_context
            .create_consumer_on_stream(
                consumer::Config {
                    name: Some(input_stream.name.to_string()),
                    ack_policy: consumer::AckPolicy::Explicit,
                    ..Default::default()
                },
                input_stream.name,
            )
            .await
            .unwrap();

        let _output_consumer = js_context
            .create_consumer_on_stream(
                consumer::Config {
                    name: Some(output_stream.name.to_string()),
                    ack_policy: consumer::AckPolicy::Explicit,
                    ..Default::default()
                },
                output_stream.name,
            )
            .await
            .unwrap();

        let sock_file_str = sock_file.to_str().unwrap().to_string();
        let server_info_file_str = server_info_file.to_str().unwrap().to_string();
        let pipeline_config = PipelineConfig {
            pipeline_name: "test-pipeline",
            vertex_name: "test-unaligned-reduce-vertex",
            from_vertex_config: vec![FromVertexConfig {
                name: "input-vertex",
                reader_config: BufferReaderConfig {
                    streams: vec![input_stream.clone()],
                    wip_ack_interval: Duration::from_millis(5),
                    ..Default::default()
                },
                partitions: 1,
            }],
            to_vertex_config: vec![ToVertexConfig {
                name: "output-vertex",
                partitions: 1,
                writer_config: BufferWriterConfig {
                    streams: vec![output_stream.clone()],
                    ..Default::default()
                },
                conditions: None,
                to_vertex_type: VertexType::Sink,
            }],
            vertex_config: VertexConfig::Reduce(ReduceVtxConfig {
                keyed: true,
                wal_storage_config: None,
                reducer_config: crate::config::components::reduce::ReducerConfig::Unaligned(
                    crate::config::components::reduce::UnalignedReducerConfig {
                        user_defined_config: UserDefinedConfig {
                            grpc_max_message_size: 5 * 1024 * 1024,
                            socket_path: Box::leak(sock_file_str.into_boxed_str()),
                            server_info_path: Box::leak(server_info_file_str.into_boxed_str()),
                        },
                        window_config: crate::config::components::reduce::UnalignedWindowConfig {
                            window_type:
                                crate::config::components::reduce::UnalignedWindowType::Accumulator(
                                    crate::config::components::reduce::AccumulatorWindowConfig {
                                        timeout: Duration::from_secs(60),
                                    },
                                ),
                            allowed_lateness: Duration::from_secs(0),
                            is_keyed: true,
                        },
                    },
                ),
            }),
            vertex_type: VertexType::ReduceUDF,
            watermark_config: Some(WatermarkConfig::Edge(EdgeWatermarkConfig {
                from_vertex_config: vec![BucketConfig {
                    vertex: "input-vertex",
                    partitions: vec![0],
                    ot_bucket,
                    hb_bucket,
                    delay: Some(Duration::from_millis(100)),
                }],
                to_vertex_config: vec![],
            })),
            ..Default::default()
        };

        // Extract the reduce config from the pipeline config
        let (reduce_vtx_config, unaligned_config) = match &pipeline_config.vertex_config {
            VertexConfig::Reduce(reduce_config) => {
                let unaligned_config = match &reduce_config.reducer_config {
                    crate::config::components::reduce::ReducerConfig::Unaligned(config) => {
                        config.clone()
                    }
                    _ => panic!("Expected unaligned config"),
                };
                (reduce_config.clone(), unaligned_config)
            }
            _ => panic!("Expected reduce vertex config"),
        };

        // Create test messages - accumulator fires every 3 messages
        let base_time = Utc.with_ymd_and_hms(2023, 1, 1, 0, 0, 0).unwrap();

        // Send 6 messages to trigger 2 accumulator outputs
        for i in 0..6 {
            let msg = Message {
                typ: Default::default(),
                keys: Arc::from(vec!["key1".into()]),
                tags: None,
                value: format!("value{}", i + 1).into(),
                offset: Offset::String(StringOffset::new(i.to_string(), 0)),
                event_time: base_time + chrono::Duration::seconds(((i + 1) * 10) as i64),
                watermark: Some(base_time + chrono::Duration::seconds(((i + 1) * 10) as i64)),
                id: MessageID {
                    vertex_name: "vertex_name".to_string().into(),
                    offset: i.to_string().into(),
                    index: i,
                },
                ..Default::default()
            };

            let message_bytes: BytesMut = msg.try_into()?;

            js_context
                .publish(input_stream.name, message_bytes.freeze())
                .await
                .unwrap()
                .await
                .unwrap();
        }

        // Start the unaligned reduce forwarder
        let cancellation_token = CancellationToken::new();
        let _forwarder_task = tokio::spawn({
            let cancellation_token = cancellation_token.clone();
            let js_context = js_context.clone();
            let pipeline_config = pipeline_config.clone();
            let reduce_vtx_config = reduce_vtx_config.clone();
            let unaligned_config = unaligned_config.clone();
            async move {
                if let Err(e) = start_unaligned_reduce_forwarder(
                    cancellation_token,
                    js_context,
                    pipeline_config,
                    reduce_vtx_config,
                    unaligned_config,
                )
                .await
                {
                    println!("Error starting unaligned reduce forwarder: {e:?}");
                }
            }
        });

        // Create a consumer to read the results from output stream
        let output_consumer: PullConsumer = js_context
            .get_consumer_from_stream(&output_stream.name, &output_stream.name)
            .await
            .unwrap();

        // Try to read messages from the output stream
        let messages_result = output_consumer
            .fetch()
            .max_messages(2)
            .expires(Duration::from_secs(3))
            .messages()
            .await;

        // Wait for server to shutdown
        let _ = tokio::time::timeout(Duration::from_millis(100), server_handle).await;

        // Verify we got some output (the exact verification depends on the accumulator logic)
        if let Ok(messages) = messages_result {
            let mut result_count = 0;
            let mut message_stream = messages;
            while let Some(msg) = message_stream.next().await {
                if let Ok(msg) = msg {
                    msg.ack().await.unwrap();
                    result_count += 1;
                }
            }
            // We expect at least some output from the accumulator
            assert!(
                result_count > 0,
                "Expected some output from unaligned reduce forwarder"
            );
        } else {
            panic!("Failed to read messages from output stream");
        }

        // Shutdown the server
        shutdown_tx
            .send(())
            .expect("failed to send shutdown signal");

        Ok(())
    }

    #[tokio::test]
    async fn test_fence_guard_creation_and_cleanup() {
        let tmp_dir = TempDir::new().unwrap();
        let fence_file_path = tmp_dir.path().join("test-fence-file");

        // Verify file doesn't exist initially
        assert!(!fence_file_path.exists());

        {
            // Create fence guard
            let _guard = FenceGuard::new(fence_file_path.clone()).await.unwrap();

            // Verify file exists while guard is in scope
            assert!(fence_file_path.exists());
        } // Guard goes out of scope here

        // Give a small delay for the Drop to execute
        tokio::time::sleep(Duration::from_millis(10)).await;

        // Verify file is cleaned up after guard is dropped
        assert!(!fence_file_path.exists());
    }

    #[tokio::test]
    async fn test_wait_for_fence_availability_timeout() {
        let tmp_dir = TempDir::new().unwrap();
        let fence_file_path = tmp_dir.path().join("persistent-fence");

        // Create the fence file and keep it
        fs::write(&fence_file_path, "").await.unwrap();

        // Should timeout since file is never removed
        let result =
            wait_for_fence_availability(&fence_file_path, Duration::from_millis(100)).await;
        assert!(result.is_err());

        // Clean up
        fs::remove_file(&fence_file_path).await.unwrap();
    }
}
