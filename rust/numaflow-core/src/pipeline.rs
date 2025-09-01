use std::path::PathBuf;
use std::time::Duration;

use async_nats::jetstream::Context;
use async_nats::{ConnectOptions, jetstream};
use futures::future::try_join_all;
use serving::callback::CallbackHandler;
use tokio::fs;
use tokio::time::{interval, timeout};
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::config::components::reduce::{
    AlignedReducerConfig, AlignedWindowType, ReducerConfig, UnalignedReducerConfig,
    UnalignedWindowType,
};
use crate::config::pipeline;
use crate::config::pipeline::isb::BufferReaderConfig;
use crate::config::pipeline::map::MapVtxConfig;
use crate::config::pipeline::watermark::WatermarkConfig;
use crate::config::pipeline::{
    PipelineConfig, ReduceVtxConfig, ServingStoreType, SinkVtxConfig, SourceVtxConfig,
};
use crate::config::{get_vertex_replica, is_mono_vertex};
use crate::metrics::{
    ComponentHealthChecks, LagReader, MetricsState, PendingReaderTasks, PipelineComponents,
    WatermarkFetcherState,
};
use crate::pipeline::forwarder::reduce_forwarder::ReduceForwarder;
use crate::pipeline::forwarder::source_forwarder;
use crate::pipeline::isb::jetstream::reader::{ISBReaderComponents, JetStreamReader};
use crate::pipeline::isb::jetstream::writer::{ISBWriterComponents, JetstreamWriter};
use crate::reduce::pbq::{PBQBuilder, WAL};
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
use crate::reduce::wal::segment::compactor::WindowKind;
use crate::shared::create_components;

use crate::mapper::map::MapHandle;
use crate::reduce::wal::create_wal_components;
use crate::shared::metrics::start_metrics_server;
use crate::sink::SinkWriter;
use crate::sink::serve::ServingStore;
use crate::sink::serve::nats::NatsServingStore;
use crate::sink::serve::user_defined::UserDefinedStore;
use crate::tracker::TrackerHandle;
use crate::transformer::Transformer;
use crate::typ::{
    NumaflowTypeConfig, WithInMemoryRateLimiter, WithRedisRateLimiter, WithoutRateLimiter,
    build_in_memory_rate_limiter_config, build_redis_rate_limiter_config,
    should_use_redis_rate_limiter,
};
use crate::watermark::WatermarkHandle;
use crate::watermark::source::SourceWatermarkHandle;
use crate::{Result, error, shared};

mod forwarder;
pub(crate) mod isb;

/// PipelineContext contains the common context for all the forwarders.
pub(crate) struct PipelineContext<'a> {
    cln_token: CancellationToken,
    js_context: &'a Context,
    config: &'a PipelineConfig,
    tracker_handle: TrackerHandle,
}

/// Starts the appropriate forwarder based on the pipeline configuration.
pub(crate) async fn start_forwarder(
    cln_token: CancellationToken,
    config: PipelineConfig,
) -> Result<()> {
    let js_context = create_js_context(config.js_client_config.clone()).await?;

    match &config.vertex_config {
        pipeline::VertexConfig::Source(source) => {
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
        pipeline::VertexConfig::Sink(sink) => {
            info!("Starting sink forwarder");
            start_sink_forwarder(cln_token, js_context, config.clone(), sink.clone()).await?;
        }
        pipeline::VertexConfig::Map(map) => {
            info!("Starting map forwarder");
            start_map_forwarder(cln_token, js_context, config.clone(), map.clone()).await?;
        }
        pipeline::VertexConfig::Reduce(reduce) => {
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

    let tracker_handle = TrackerHandle::new(serving_callback_handler);

    let context = PipelineContext {
        cln_token: cln_token.clone(),
        js_context: &js_context,
        config: &config,
        tracker_handle: tracker_handle.clone(),
    };

    let writer_components = ISBWriterComponents::new(
        source_watermark_handle.clone().map(WatermarkHandle::Source),
        &context,
    );

    let buffer_writer = JetstreamWriter::new(writer_components);
    let transformer = create_components::create_transformer(
        config.batch_size,
        config.graceful_shutdown_time,
        source_config.transformer_config.clone(),
        tracker_handle.clone(),
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

async fn start_map_forwarder(
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

    let tracker_handle = TrackerHandle::new(serving_callback_handler.clone());

    let watermark_handle = create_components::create_edge_watermark_handle(
        &config,
        &js_context,
        &cln_token,
        None,
        tracker_handle.clone(),
    )
    .await?;

    let reader_config = &config
        .from_vertex_config
        .first()
        .ok_or_else(|| error::Error::Config("No from vertex config found".to_string()))?
        .reader_config;

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
                partition_count: reader_config.streams.len() as u16, // Number of partitions = number of streams
            }),
        },
    )
    .await;

    let results = try_join_all(forwarder_tasks)
        .await
        .map_err(|e| error::Error::Forwarder(e.to_string()))?;

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

    let forwarder = forwarder::map_forwarder::MapForwarder::<C>::new(
        buffer_reader.clone(),
        mapper,
        buffer_writer,
    )
    .await;

    let task = tokio::spawn(async move { forwarder.start(cln_token).await });
    Ok((task, buffer_reader))
}

/// Guard to manage the lifecycle of a fence file. Used when persistence is enabled for reduce.
/// File will be deleted when the guard is dropped.
struct FenceGuard {
    fence_file_path: PathBuf,
}

impl FenceGuard {
    /// Creates a new fence guard with the specified fence file path.
    async fn new(fence_file_path: PathBuf) -> Result<Self> {
        // Create the fence file
        fs::write(&fence_file_path, "")
            .await
            .map_err(|e| error::Error::Config(format!("Failed to create fence file: {e}")))?;
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
async fn wait_for_fence_availability(
    fence_file_path: &PathBuf,
    timeout_duration: Duration,
) -> Result<()> {
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
                    return Err(error::Error::Config(format!(
                        "Error checking fence file {fence_file_path:?}: {e}"
                    )));
                }
            }
        }
    })
    .await;

    result.map_err(|e| error::Error::Config(format!("Fence wait timed out: {e}")))?
}

async fn start_reduce_forwarder(
    cln_token: CancellationToken,
    js_context: Context,
    config: PipelineConfig,
    reduce_vtx_config: ReduceVtxConfig,
) -> Result<()> {
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

async fn start_aligned_reduce_forwarder(
    cln_token: CancellationToken,
    js_context: Context,
    config: PipelineConfig,
    reduce_vtx_config: ReduceVtxConfig,
    aligned_config: AlignedReducerConfig,
) -> Result<()> {
    // for reduce we do not pass serving callback handler to tracker.
    let tracker_handle = TrackerHandle::new(None);

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
        tracker_handle.clone(),
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
        .get(*get_vertex_replica() as usize)
        .cloned()
        .ok_or_else(|| error::Error::Config("No stream found for reduce vertex".to_string()))?;

    let context = PipelineContext {
        cln_token: cln_token.clone(),
        js_context: &js_context,
        config: &config,
        tracker_handle: tracker_handle.clone(),
    };

    let reader_components = ISBReaderComponents::new(
        stream,
        reader_config.clone(),
        watermark_handle.clone(),
        &context,
    );

    let writer_components =
        ISBWriterComponents::new(watermark_handle.clone().map(WatermarkHandle::ISB), &context);

    let buffer_writer = JetstreamWriter::new(writer_components);

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
                    partition_count: 1, // Reduce vertices always read from single partition (partition 0)
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
            config.read_timeout,
            reduce_vtx_config.keyed,
            watermark_handle,
        )
        .await,
    );

    let context = PipelineContext {
        cln_token: cln_token.clone(),
        js_context: &js_context,
        config: &config,
        tracker_handle,
    };

    if let Some(rate_limit_config) = &config.rate_limit {
        if should_use_redis_rate_limiter(rate_limit_config) {
            let redis_config =
                build_redis_rate_limiter_config(rate_limit_config, cln_token.clone()).await?;

            run_reduce_forwarder::<WithRedisRateLimiter>(
                &context,
                reader_components.clone(),
                reducer,
                wal,
                Some(redis_config.throttling_config),
            )
            .await?
        } else {
            let in_mem_config =
                build_in_memory_rate_limiter_config(rate_limit_config, cln_token.clone()).await?;

            run_reduce_forwarder::<WithInMemoryRateLimiter>(
                &context,
                reader_components.clone(),
                reducer,
                wal,
                Some(in_mem_config.throttling_config),
            )
            .await?
        }
    } else {
        run_reduce_forwarder::<WithoutRateLimiter>(&context, reader_components, reducer, wal, None)
            .await?
    };

    info!("Aligned reduce forwarder has stopped successfully");
    Ok(())
}

async fn start_unaligned_reduce_forwarder(
    cln_token: CancellationToken,
    js_context: Context,
    config: PipelineConfig,
    reduce_vtx_config: ReduceVtxConfig,
    unaligned_config: UnalignedReducerConfig,
) -> Result<()> {
    // for reduce we do not pass serving callback handler to tracker.
    let tracker_handle = TrackerHandle::new(None);

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
        tracker_handle.clone(),
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
        .get(*get_vertex_replica() as usize)
        .cloned()
        .ok_or_else(|| error::Error::Config("No stream found for reduce vertex".to_string()))?;

    let context = PipelineContext {
        cln_token: cln_token.clone(),
        js_context: &js_context,
        config: &config,
        tracker_handle: tracker_handle.clone(),
    };

    let reader_components = ISBReaderComponents::new(
        stream,
        reader_config.clone(),
        watermark_handle.clone(),
        &context,
    );

    let writer_components =
        ISBWriterComponents::new(watermark_handle.clone().map(WatermarkHandle::ISB), &context);

    let buffer_writer = JetstreamWriter::new(writer_components);

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
                    partition_count: 1, // Reduce vertices always read from single partition (partition 0)
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
            config.read_timeout,
            reduce_vtx_config.keyed,
            watermark_handle,
        )
        .await,
    );

    let context = PipelineContext {
        cln_token: cln_token.clone(),
        js_context: &js_context,
        config: &config,
        tracker_handle,
    };

    if let Some(rate_limit_config) = &config.rate_limit {
        if should_use_redis_rate_limiter(rate_limit_config) {
            let redis_config =
                build_redis_rate_limiter_config(rate_limit_config, cln_token.clone()).await?;

            run_reduce_forwarder::<WithRedisRateLimiter>(
                &context,
                reader_components.clone(),
                reducer,
                wal,
                Some(redis_config.throttling_config),
            )
            .await?
        } else {
            let in_mem_config =
                build_in_memory_rate_limiter_config(rate_limit_config, cln_token.clone()).await?;

            run_reduce_forwarder::<WithInMemoryRateLimiter>(
                &context,
                reader_components.clone(),
                reducer,
                wal,
                Some(in_mem_config.throttling_config),
            )
            .await?
        }
    } else {
        run_reduce_forwarder::<WithoutRateLimiter>(&context, reader_components, reducer, wal, None)
            .await?
    };

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
    let buffer_reader = JetStreamReader::<C>::new(reader_components, rate_limiter).await?;

    // Create lag reader with the single buffer reader (reduce only reads from one stream)
    let pending_reader = shared::metrics::create_pending_reader(
        &context.config.metrics_config,
        LagReader::ISB(vec![buffer_reader.clone()]),
    )
    .await;
    let _pending_reader_handle = pending_reader.start(is_mono_vertex()).await;

    let pbq_builder = PBQBuilder::<C>::new(buffer_reader, context.tracker_handle.clone());
    let pbq = match wal {
        Some(wal) => pbq_builder.wal(wal).build(),
        None => pbq_builder.build(),
    };
    let forwarder = ReduceForwarder::<C>::new(pbq, reducer);

    forwarder.start(context.cln_token.clone()).await
}

/// Starts source forwarder.
async fn run_source_forwarder<C: NumaflowTypeConfig>(
    context: &PipelineContext<'_>,
    source_config: &SourceVtxConfig,
    transformer: Option<Transformer>,
    source_watermark_handle: Option<SourceWatermarkHandle>,
    buffer_writer: JetstreamWriter,
    rate_limiter: Option<C::RateLimiter>,
) -> Result<()> {
    let source = create_components::create_source::<C>(
        context.config.batch_size,
        context.config.read_timeout,
        &source_config.source_config,
        context.tracker_handle.clone(),
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
                partition_count: 1, // Source vertices always have partition count = 1
            }),
        },
    )
    .await;

    let forwarder = source_forwarder::SourceForwarder::<C>::new(source, buffer_writer);

    forwarder.start(context.cln_token.clone()).await
}

async fn start_sink_forwarder(
    cln_token: CancellationToken,
    js_context: Context,
    config: PipelineConfig,
    sink: SinkVtxConfig,
) -> Result<()> {
    // 1. One-time setup
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

    let tracker_handle = TrackerHandle::new(serving_callback_handler.clone());

    let watermark_handle = create_components::create_edge_watermark_handle(
        &config,
        &js_context,
        &cln_token,
        None,
        tracker_handle.clone(),
    )
    .await?;

    let reader_config = &config
        .from_vertex_config
        .first()
        .ok_or_else(|| error::Error::Config("No from vertex config found".to_string()))?
        .reader_config;

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
        tracker_handle,
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
                partition_count: reader_config.streams.len() as u16, // Number of partitions = number of streams
            }),
        },
    )
    .await;

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
    let mut isb_lag_readers: Vec<JetStreamReader<C>> = vec![];
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
            context.tracker_handle.clone(),
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
) -> Result<(tokio::task::JoinHandle<Result<()>>, JetStreamReader<C>)> {
    let cln_token = reader_components.cln_token.clone();
    let buffer_reader = JetStreamReader::<C>::new(reader_components, rate_limiter).await?;

    let forwarder =
        forwarder::sink_forwarder::SinkForwarder::<C>::new(buffer_reader.clone(), sink_writer)
            .await;

    let task = tokio::spawn(async move { forwarder.start(cln_token).await });
    Ok((task, buffer_reader))
}

/// Creates a jetstream context based on the provided configuration
pub(crate) async fn create_js_context(
    config: pipeline::isb::jetstream::ClientConfig,
) -> Result<Context> {
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
    use crate::config::pipeline::isb::Stream;
    use crate::config::pipeline::map::{MapType, UserDefinedConfig};
    use crate::config::pipeline::{PipelineConfig, VertexType};
    use crate::pipeline::pipeline::VertexConfig;
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
            ..Default::default()
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
