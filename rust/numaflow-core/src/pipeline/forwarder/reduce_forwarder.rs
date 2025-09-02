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
use crate::pipeline::isb::jetstream::reader::{ISBReaderComponents, JetStreamReader};
use crate::pipeline::isb::jetstream::writer::{ISBWriterComponents, JetstreamWriter};
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
use crate::tracker::TrackerHandle;
use crate::typ::{
    NumaflowTypeConfig, WithInMemoryRateLimiter, WithRedisRateLimiter, WithoutRateLimiter,
    build_in_memory_rate_limiter_config, build_redis_rate_limiter_config,
    should_use_redis_rate_limiter,
};
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
        let child_token = cln_token.child_token();

        // Start the PBQ reader
        let (read_messages_stream, pbq_handle) =
            self.pbq.streaming_read(child_token.clone()).await?;

        // Start the reducer
        let processor_handle = match self.reducer {
            Reducer::Aligned(reducer) => {
                reducer
                    .start(read_messages_stream, child_token.clone())
                    .await?
            }
            Reducer::Unaligned(reducer) => {
                reducer
                    .start(read_messages_stream, child_token.clone())
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
            cln_token.cancel();
        })?;

        pbq_result.inspect_err(|e| {
            error!(?e, "Error in PBQ reader");
            cln_token.cancel();
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
) -> crate::error::Result<()> {
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

pub(crate) async fn start_unaligned_reduce_forwarder(
    cln_token: CancellationToken,
    js_context: Context,
    config: PipelineConfig,
    reduce_vtx_config: ReduceVtxConfig,
    unaligned_config: UnalignedReducerConfig,
) -> crate::error::Result<()> {
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
) -> crate::error::Result<()> {
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
    use crate::pipeline::forwarder::reduce_forwarder::{FenceGuard, wait_for_fence_availability};
    use std::time::Duration;
    use tempfile::TempDir;
    use tokio::fs;

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
