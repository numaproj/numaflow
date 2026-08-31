//! Resilience wrapper for reconnectable built-in sources (Kafka, Pulsar, SQS, NATS, JetStream).
//!
//! When a source client fails, [`BuiltinSource`] recreates it with backoff instead of
//! crashing `numa`. In-flight ack state is preserved by each source adapter; this module
//! owns retry timing, health/readiness, and runtime-error reporting.
//!
//! Failure policy:
//! - `RetrySame + Benign` keeps the current generation ready (for example, Kafka rebalance).
//! - `RetrySame + Outage` keeps the generation but marks it degraded until an operation succeeds.
//! - `Recreate` retires the old generation before a background replacement is started.
//! - `StayDegraded` parks hot retries and probes recovery on a slow interval.
//!
//! Reads surface `SourceRedrive` while recovering. The forwarder backs off, reports a read error,
//! and pauses idle-watermark advancement while continuing non-idle heartbeat publication.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering};
use std::{future::Future, pin::Pin};

use async_trait::async_trait;
use tokio::sync::Mutex;
use tokio::time::{Duration, Instant, sleep_until, timeout};
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::error::{Error, Result, SourceFailureAction, SourceFailureImpact};
use crate::message::{Message, NackOffset, Offset};
use crate::reader::LagReader;
use crate::source::runtime_error::SourceRuntimeErrorTracker;
use crate::source::{SourceAcker, SourcePartitions, SourceReader};

const HEALTH_STARTING: u8 = 0;
const HEALTH_READY: u8 = 1;
const HEALTH_DEGRADED: u8 = 2;

/// One live connection to an external source (e.g. a Kafka consumer or Pulsar client).
///
/// The supervisor can discard and recreate this when operations fail. Anything that must
/// survive a reconnect—message IDs, receipt handles, WIP trackers—should live in the
/// factory or shared adapter state, not inside this backend.
#[async_trait]
pub(crate) trait BuiltinSourceBackend: Send {
    async fn read(&mut self) -> Option<Result<Vec<Message>>>;
    async fn ack(&mut self, offsets: Vec<Offset>) -> Result<()>;
    async fn nack(&mut self, offsets: Vec<NackOffset>) -> Result<()>;
    async fn pending(&mut self) -> Result<Option<usize>>;
    async fn partitions(&mut self) -> Result<SourcePartitions>;

    /// Release external resources before the supervisor drops this generation.
    async fn retire(&mut self) {}
}

/// Adapts an existing source type (Kafka, Pulsar, …) to [`BuiltinSourceBackend`].
///
/// This avoids duplicating read/ack/nack wiring in every built-in adapter.
type RetireFuture = Pin<Box<dyn Future<Output = ()> + Send>>;
type RetireHook = Box<dyn FnMut() -> RetireFuture + Send>;

pub(crate) struct SourceBackend<T> {
    source: T,
    retire_hook: Option<RetireHook>,
}

impl<T> SourceBackend<T> {
    pub(crate) fn new(source: T) -> Self {
        Self {
            source,
            retire_hook: None,
        }
    }

    pub(crate) fn with_retire<F, Fut>(source: T, mut retire_hook: F) -> Self
    where
        F: FnMut() -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        Self {
            source,
            retire_hook: Some(Box::new(move || Box::pin(retire_hook()))),
        }
    }
}

#[async_trait]
impl<T> BuiltinSourceBackend for SourceBackend<T>
where
    T: SourceReader + SourceAcker + LagReader + Send,
{
    async fn read(&mut self) -> Option<Result<Vec<Message>>> {
        SourceReader::read(&mut self.source).await
    }

    async fn ack(&mut self, offsets: Vec<Offset>) -> Result<()> {
        SourceAcker::ack(&mut self.source, offsets).await
    }

    async fn nack(&mut self, offsets: Vec<NackOffset>) -> Result<()> {
        SourceAcker::nack(&mut self.source, offsets).await
    }

    async fn pending(&mut self) -> Result<Option<usize>> {
        LagReader::pending(&mut self.source).await
    }

    async fn partitions(&mut self) -> Result<SourcePartitions> {
        SourceReader::partitions(&mut self.source).await
    }

    async fn retire(&mut self) {
        if let Some(retire_hook) = &mut self.retire_hook {
            retire_hook().await;
        }
    }
}

/// Creates a fresh [`BuiltinSourceBackend`] from saved configuration.
///
/// Parsing config, loading secrets, and opening the client all happen in [`build`].
/// If any step fails, the supervisor retries later with backoff—`numa` does not exit.
#[async_trait]
pub(crate) trait BuiltinSourceFactory: Send + Sync {
    fn name(&self) -> &'static str;
    async fn build(&self) -> Result<Box<dyn BuiltinSourceBackend>>;
}

type BuildFuture =
    Pin<Box<dyn Future<Output = Result<Box<dyn BuiltinSourceBackend>>> + Send + 'static>>;

/// Factory for sources whose generations are created by the same connect-style closure.
pub(crate) struct ConnectFactory {
    name: &'static str,
    connect: Box<dyn Fn() -> BuildFuture + Send + Sync>,
}

impl ConnectFactory {
    pub(crate) fn new<F, Fut>(name: &'static str, connect: F) -> Self
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Box<dyn BuiltinSourceBackend>>> + Send + 'static,
    {
        Self {
            name,
            connect: Box::new(move || Box::pin(connect())),
        }
    }
}

#[async_trait]
impl BuiltinSourceFactory for ConnectFactory {
    fn name(&self) -> &'static str {
        self.name
    }

    async fn build(&self) -> Result<Box<dyn BuiltinSourceBackend>> {
        (self.connect)().await
    }
}

/// Health of a built-in source from the supervisor's point of view.
#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BuiltinSourceHealth {
    /// Initial startup; backend not ready yet.
    Starting,
    /// Backend is connected and operations succeed.
    Ready,
    /// Backend failed or is being recreated; source is not ready but `numa` keeps running.
    Degraded,
}

#[cfg(test)]
impl BuiltinSourceHealth {
    fn from_atomic(value: u8) -> Self {
        match value {
            HEALTH_READY => Self::Ready,
            HEALTH_DEGRADED => Self::Degraded,
            _ => Self::Starting,
        }
    }
}

/// Controls how long the supervisor waits between rebuild attempts.
#[derive(Debug, Clone, Copy)]
pub(crate) struct BuiltinSourceRetryConfig {
    pub(crate) initial_backoff: Duration,
    pub(crate) max_backoff: Duration,
    pub(crate) build_timeout: Duration,
    /// Interval for [`SourceFailureAction::StayDegraded`] recovery probes.
    pub(crate) slow_recovery_interval: Duration,
}

impl Default for BuiltinSourceRetryConfig {
    fn default() -> Self {
        Self {
            initial_backoff: Duration::from_millis(100),
            max_backoff: Duration::from_secs(5),
            build_timeout: Duration::from_secs(10),
            slow_recovery_interval: Duration::from_secs(30),
        }
    }
}

type BackendHandle = Arc<Mutex<Box<dyn BuiltinSourceBackend>>>;

/// Mutable supervisor metadata. The lock must never be held across backend I/O or factory builds.
struct SupervisorMetadata {
    backend: Option<BackendHandle>,
    retry_backoff: Duration,
    retry_at: Instant,
    slow_recovery: bool,
    runtime_error_tracker: SourceRuntimeErrorTracker,
    last_pending: Option<usize>,
    last_partitions: SourcePartitions,
}

/// Resilient wrapper around built-in sources.
///
/// On failure, recreates the underlying client with exponential backoff and keeps
/// `numa` alive. Behavior by operation:
///
/// - **read** — returns [`Error::SourceRedrive`] while recovering
/// - **ack / nack** — returns [`Error::SourceRedrive`] so offsets are retried later
/// - **pending / partitions** — returns the last known good value while degraded
///
/// Readiness ([`is_ready`]) is false while degraded; liveness is unaffected.
#[derive(Clone)]
pub(crate) struct BuiltinSource {
    factory: Arc<dyn BuiltinSourceFactory>,
    metadata: Arc<Mutex<SupervisorMetadata>>,
    health: Arc<AtomicU8>,
    watermark_ready: Arc<AtomicBool>,
    generation: Arc<AtomicU64>,
    builder_active: Arc<AtomicBool>,
    cancel_token: CancellationToken,
    retry_config: BuiltinSourceRetryConfig,
}

impl BuiltinSource {
    pub(crate) fn new(
        factory: Arc<dyn BuiltinSourceFactory>,
        cancel_token: CancellationToken,
    ) -> Self {
        Self::with_retry_config(factory, cancel_token, BuiltinSourceRetryConfig::default())
    }

    pub(crate) fn with_retry_config(
        factory: Arc<dyn BuiltinSourceFactory>,
        cancel_token: CancellationToken,
        retry_config: BuiltinSourceRetryConfig,
    ) -> Self {
        Self::with_state(
            factory,
            cancel_token,
            retry_config,
            SourceRuntimeErrorTracker::default(),
        )
    }

    #[cfg(test)]
    fn with_runtime_error_path(
        factory: Arc<dyn BuiltinSourceFactory>,
        cancel_token: CancellationToken,
        retry_config: BuiltinSourceRetryConfig,
        app_error_path: String,
    ) -> Self {
        Self::with_state(
            factory,
            cancel_token,
            retry_config,
            SourceRuntimeErrorTracker::with_runtime_error_path(app_error_path),
        )
    }

    fn with_state(
        factory: Arc<dyn BuiltinSourceFactory>,
        cancel_token: CancellationToken,
        retry_config: BuiltinSourceRetryConfig,
        runtime_error_tracker: SourceRuntimeErrorTracker,
    ) -> Self {
        debug_assert!(
            !retry_config.initial_backoff.is_zero(),
            "initial source retry backoff must be greater than zero"
        );
        debug_assert!(
            retry_config.initial_backoff <= retry_config.max_backoff,
            "initial source retry backoff must not exceed max backoff"
        );
        debug_assert!(
            !retry_config.build_timeout.is_zero(),
            "source build timeout must be greater than zero"
        );
        debug_assert!(
            !retry_config.slow_recovery_interval.is_zero(),
            "slow recovery interval must be greater than zero"
        );

        let source = Self {
            factory,
            metadata: Arc::new(Mutex::new(SupervisorMetadata {
                backend: None,
                retry_backoff: retry_config.initial_backoff,
                retry_at: Instant::now(),
                slow_recovery: false,
                runtime_error_tracker,
                last_pending: None,
                last_partitions: SourcePartitions::default(),
            })),
            health: Arc::new(AtomicU8::new(HEALTH_STARTING)),
            watermark_ready: Arc::new(AtomicBool::new(false)),
            generation: Arc::new(AtomicU64::new(0)),
            builder_active: Arc::new(AtomicBool::new(false)),
            cancel_token,
            retry_config,
        };
        if let Ok(runtime) = tokio::runtime::Handle::try_current()
            && source.try_start_builder()
        {
            let source_to_build = source.clone();
            runtime.spawn(async move {
                source_to_build.run_background_builder(None).await;
            });
        }
        source
    }

    #[cfg(test)]
    pub(crate) async fn health(&self) -> BuiltinSourceHealth {
        BuiltinSourceHealth::from_atomic(self.health.load(Ordering::Acquire))
    }

    pub(crate) async fn is_ready(&self) -> bool {
        self.is_ready_now()
    }

    pub(crate) fn is_ready_now(&self) -> bool {
        self.health.load(Ordering::Acquire) == HEALTH_READY
    }

    pub(crate) fn is_watermark_ready_now(&self) -> bool {
        self.watermark_ready.load(Ordering::Acquire)
    }

    fn generation(&self) -> u64 {
        self.generation.load(Ordering::Acquire)
    }

    fn source_redrive(
        &self,
        operation: &'static str,
        action: SourceFailureAction,
        impact: SourceFailureImpact,
        code: &'static str,
        message: impl Into<String>,
    ) -> Error {
        Error::SourceRedrive {
            source_name: self.factory.name(),
            operation,
            action,
            impact,
            code,
            message: message.into(),
        }
    }

    async fn unavailable_redrive(&self, operation: &'static str) -> Error {
        let slow_recovery = self.metadata.lock().await.slow_recovery;
        self.source_redrive(
            operation,
            if slow_recovery {
                SourceFailureAction::StayDegraded
            } else {
                SourceFailureAction::Recreate
            },
            SourceFailureImpact::Outage,
            "backend_unavailable",
            "source backend is unavailable; recovery build in progress",
        )
    }

    fn classify_failure(
        error: &Error,
    ) -> (
        SourceFailureAction,
        SourceFailureImpact,
        &'static str,
        String,
    ) {
        if let Error::SourceRedrive {
            action,
            impact,
            code,
            message,
            ..
        } = error
        {
            return (*action, *impact, code, message.clone());
        }

        (
            SourceFailureAction::Recreate,
            SourceFailureImpact::Outage,
            stable_error_code(error),
            source_error_message(error),
        )
    }

    fn mark_degraded(&self, impact: SourceFailureImpact) {
        if impact == SourceFailureImpact::Outage {
            self.health.store(HEALTH_DEGRADED, Ordering::Release);
            self.watermark_ready.store(false, Ordering::Release);
        }
    }

    async fn mark_ready(&self) {
        self.watermark_ready.store(true, Ordering::Release);
        let previous = self.health.swap(HEALTH_READY, Ordering::AcqRel);
        self.metadata
            .lock()
            .await
            .runtime_error_tracker
            .record_recovery(self.factory.name());
        if previous == HEALTH_DEGRADED {
            info!(
                source = self.factory.name(),
                generation = self.generation.load(Ordering::Acquire),
                "Built-in source recovered successfully"
            );
        }
    }

    async fn report_outage(&self, operation: &'static str, error: &Error) {
        let mut metadata = self.metadata.lock().await;
        metadata
            .runtime_error_tracker
            .record_failure(self.factory.name(), operation, error);
    }

    async fn schedule_backoff(&self, slow_recovery: bool) {
        let mut metadata = self.metadata.lock().await;
        metadata.slow_recovery = slow_recovery;
        let interval = if slow_recovery {
            self.retry_config.slow_recovery_interval
        } else {
            metadata.retry_backoff
        };
        metadata.retry_at = Instant::now() + interval;
        if !slow_recovery {
            metadata.retry_backoff = metadata
                .retry_backoff
                .saturating_mul(2)
                .min(self.retry_config.max_backoff);
        }
    }

    fn try_start_builder(&self) -> bool {
        if self.cancel_token.is_cancelled() {
            return false;
        }
        !self.builder_active.swap(true, Ordering::AcqRel)
    }

    fn finish_builder(&self) {
        self.builder_active.store(false, Ordering::Release);
    }

    async fn schedule_build(&self, slow_recovery: bool, backend_to_retire: Option<BackendHandle>) {
        if !self.try_start_builder() {
            if let Some(backend_to_retire) = backend_to_retire {
                let source = self.clone();
                tokio::spawn(async move {
                    Self::retire_backend(backend_to_retire).await;
                    loop {
                        if source.cancel_token.is_cancelled() {
                            return;
                        }
                        if source.try_start_builder() {
                            source.schedule_backoff(slow_recovery).await;
                            let builder_source = source.clone();
                            tokio::spawn(async move {
                                builder_source.run_background_builder(None).await;
                            });
                            return;
                        }
                        tokio::select! {
                            _ = source.cancel_token.cancelled() => return,
                            _ = tokio::time::sleep(Duration::from_millis(10)) => {}
                        }
                    }
                });
            }
            return;
        }
        self.schedule_backoff(slow_recovery).await;
        let source = self.clone();
        tokio::spawn(async move {
            source.run_background_builder(backend_to_retire).await;
        });
    }

    async fn take_backend_for_retirement(&self) -> Option<BackendHandle> {
        let mut metadata = self.metadata.lock().await;
        metadata.backend.take()
    }

    async fn snapshot_backend(&self) -> Option<BackendHandle> {
        let metadata = self.metadata.lock().await;
        metadata.backend.clone()
    }

    async fn cached_pending(&self) -> Option<usize> {
        self.metadata.lock().await.last_pending
    }

    async fn cached_partitions(&self) -> SourcePartitions {
        self.metadata.lock().await.last_partitions.clone()
    }

    async fn update_cached_pending(&self, pending: Option<usize>) {
        self.metadata.lock().await.last_pending = pending;
    }

    async fn update_cached_partitions(&self, partitions: SourcePartitions) {
        self.metadata.lock().await.last_partitions = partitions;
    }

    async fn retire_backend(backend: BackendHandle) {
        backend.lock().await.retire().await;
    }

    async fn run_background_builder(self, backend_to_retire: Option<BackendHandle>) {
        struct BuilderGuard {
            source: BuiltinSource,
        }

        impl Drop for BuilderGuard {
            fn drop(&mut self) {
                self.source.finish_builder();
            }
        }

        let _guard = BuilderGuard {
            source: self.clone(),
        };

        let old_backend = match backend_to_retire {
            Some(backend) => Some(backend),
            None => self.take_backend_for_retirement().await,
        };
        if let Some(old_backend) = old_backend {
            Self::retire_backend(old_backend).await;
        }

        loop {
            if self.cancel_token.is_cancelled() {
                return;
            }

            let wait_until = {
                let metadata = self.metadata.lock().await;
                metadata.retry_at
            };

            tokio::select! {
                _ = self.cancel_token.cancelled() => return,
                _ = sleep_until(wait_until) => {}
            }

            if self.cancel_token.is_cancelled() {
                return;
            }

            let build = timeout(self.retry_config.build_timeout, self.factory.build());
            let result = tokio::select! {
                biased;
                _ = self.cancel_token.cancelled() => return,
                result = build => result,
            };

            match result {
                Ok(Ok(backend)) => {
                    let new_handle = Arc::new(Mutex::new(backend));
                    let next_generation = {
                        let mut metadata = self.metadata.lock().await;
                        metadata.backend = Some(new_handle);
                        metadata.retry_backoff = self.retry_config.initial_backoff;
                        metadata.retry_at = Instant::now();
                        metadata.slow_recovery = false;
                        let next_generation = self.generation.fetch_add(1, Ordering::AcqRel) + 1;
                        self.health.store(HEALTH_READY, Ordering::Release);
                        next_generation
                    };
                    info!(
                        source = self.factory.name(),
                        generation = next_generation,
                        "Built-in source client generation installed successfully"
                    );
                    return;
                }
                Ok(Err(error)) => {
                    self.health.store(HEALTH_DEGRADED, Ordering::Release);
                    self.watermark_ready.store(false, Ordering::Release);
                    let (action, _, _, _) = Self::classify_failure(&error);
                    let slow_recovery = action == SourceFailureAction::StayDegraded;
                    self.report_outage("build", &error).await;
                    warn!(
                        source = self.factory.name(),
                        ?error,
                        "Built-in source backend build failed; will retry after backoff"
                    );
                    self.schedule_backoff(slow_recovery).await;
                }
                Err(error) => {
                    let error = Error::Source(format!(
                        "timed out after {:?} building {} source backend: {error}",
                        self.retry_config.build_timeout,
                        self.factory.name()
                    ));
                    self.health.store(HEALTH_DEGRADED, Ordering::Release);
                    self.watermark_ready.store(false, Ordering::Release);
                    self.report_outage("build", &error).await;
                    warn!(
                        source = self.factory.name(),
                        ?error,
                        "Built-in source backend build timed out; will retry after backoff"
                    );
                    let slow_recovery = self.metadata.lock().await.slow_recovery;
                    self.schedule_backoff(slow_recovery).await;
                }
            }
        }
    }

    async fn handle_failure(&self, operation: &'static str, error: &Error) -> Error {
        let (action, impact, code, message) = Self::classify_failure(error);
        let redrive = self.source_redrive(operation, action, impact, code, message);

        match (action, impact) {
            (SourceFailureAction::RetrySame, SourceFailureImpact::Benign) => redrive,
            (SourceFailureAction::RetrySame, SourceFailureImpact::Outage) => {
                self.mark_degraded(impact);
                self.report_outage(operation, &redrive).await;
                warn!(
                    source = self.factory.name(),
                    operation,
                    generation = self.generation(),
                    ?error,
                    "Built-in source operation failed; keeping current client generation"
                );
                redrive
            }
            (SourceFailureAction::Recreate, _) => {
                self.mark_degraded(impact);
                self.report_outage(operation, &redrive).await;
                warn!(
                    source = self.factory.name(),
                    operation,
                    ?error,
                    "Built-in source operation failed; scheduling backend recreation"
                );
                let old_backend = self.take_backend_for_retirement().await;
                self.schedule_build(false, old_backend).await;
                redrive
            }
            (SourceFailureAction::StayDegraded, _) => {
                self.mark_degraded(impact);
                self.report_outage(operation, &redrive).await;
                warn!(
                    source = self.factory.name(),
                    operation,
                    ?error,
                    "Built-in source operation failed; staying degraded until slow recovery"
                );
                let old_backend = self.take_backend_for_retirement().await;
                self.schedule_build(true, old_backend).await;
                redrive
            }
        }
    }

    async fn read_messages(&mut self) -> Option<Result<Vec<Message>>> {
        if self.cancel_token.is_cancelled() {
            return None;
        }

        let backend = self.snapshot_backend().await;
        let Some(backend) = backend else {
            self.mark_degraded(SourceFailureImpact::Outage);
            self.schedule_build(false, None).await;
            return Some(Err(self.unavailable_redrive("read").await));
        };

        let result = backend.lock().await.read().await;
        match result {
            Some(Ok(messages)) => {
                self.mark_ready().await;
                Some(Ok(messages))
            }
            Some(Err(error)) => Some(Err(self.handle_failure("read", &error).await)),
            None if self.cancel_token.is_cancelled() => None,
            None => {
                let error = Error::Source(format!(
                    "{} source stream closed unexpectedly",
                    self.factory.name()
                ));
                Some(Err(self.handle_failure("read", &error).await))
            }
        }
    }

    async fn ack_offsets(&mut self, offsets: Vec<Offset>) -> Result<()> {
        if self.cancel_token.is_cancelled() {
            return Err(Error::Cancelled());
        }

        let backend = self.snapshot_backend().await;
        let Some(backend) = backend else {
            self.mark_degraded(SourceFailureImpact::Outage);
            self.schedule_build(false, None).await;
            return Err(self.unavailable_redrive("ack").await);
        };

        match backend.lock().await.ack(offsets).await {
            Ok(()) => {
                self.mark_ready().await;
                Ok(())
            }
            Err(error) => Err(self.handle_failure("ack", &error).await),
        }
    }

    async fn nack_offsets(&mut self, offsets: Vec<NackOffset>) -> Result<()> {
        if self.cancel_token.is_cancelled() {
            return Err(Error::Cancelled());
        }

        let backend = self.snapshot_backend().await;
        let Some(backend) = backend else {
            self.mark_degraded(SourceFailureImpact::Outage);
            self.schedule_build(false, None).await;
            return Err(self.unavailable_redrive("nack").await);
        };

        match backend.lock().await.nack(offsets).await {
            Ok(()) => {
                self.mark_ready().await;
                Ok(())
            }
            Err(error) => Err(self.handle_failure("nack", &error).await),
        }
    }

    async fn pending_messages(&mut self) -> Result<Option<usize>> {
        let backend = self.snapshot_backend().await;
        let Some(backend) = backend else {
            self.schedule_build(false, None).await;
            return Ok(self.cached_pending().await);
        };

        match backend.lock().await.pending().await {
            Ok(pending) => {
                self.update_cached_pending(pending).await;
                Ok(pending)
            }
            Err(error) => {
                let _ = self.handle_failure("pending", &error).await;
                Ok(self.cached_pending().await)
            }
        }
    }

    async fn source_partitions(&mut self) -> Result<SourcePartitions> {
        let backend = self.snapshot_backend().await;
        let Some(backend) = backend else {
            self.schedule_build(false, None).await;
            return Ok(self.cached_partitions().await);
        };

        match backend.lock().await.partitions().await {
            Ok(partitions) => {
                self.update_cached_partitions(partitions.clone()).await;
                Ok(partitions)
            }
            Err(error) => {
                let _ = self.handle_failure("partitions", &error).await;
                Ok(self.cached_partitions().await)
            }
        }
    }
}

fn stable_error_code(error: &Error) -> &'static str {
    match error {
        Error::Metrics(_) => "metrics",
        Error::Source(_) => "source",
        Error::Sink(_) => "sink",
        Error::FbSink(_) => "fb_sink",
        Error::OsSink(_) => "os_sink",
        Error::Transformer(_) => "transformer",
        Error::Mapper(_) => "mapper",
        Error::Forwarder(_) => "forwarder",
        Error::BypassRouter(_) => "bypass_router",
        Error::Connection(_) => "connection",
        Error::Grpc(_) => "grpc",
        Error::UdfRedrive(_) => "udf_redrive",
        Error::SourceRedrive { code, .. } => code,
        Error::Config(_) => "config",
        Error::Shared(_) => "shared",
        Error::Proto(_) => "proto",
        Error::ISB(_) => "isb",
        Error::ActorPatternRecv(_) => "actor_pattern_recv",
        Error::AckPendingExceeded(_) => "ack_pending_exceeded",
        Error::AckOffsetNotFound(_) => "ack_offset_not_found",
        Error::Lag(_) => "lag",
        Error::Tracker(_) => "tracker",
        Error::DuplicateInflight(_) => "duplicate_inflight",
        Error::Watermark(_) => "watermark",
        Error::SideInput(_) => "side_input",
        Error::Reduce(_) => "reduce",
        Error::Cancelled() => "cancelled",
        Error::WAL(_) => "wal",
        Error::NonRetryable(_) => "non_retryable",
    }
}

fn source_error_message(error: &Error) -> String {
    match error {
        Error::Source(message)
        | Error::Connection(message)
        | Error::Config(message)
        | Error::Lag(message)
        | Error::NonRetryable(message)
        | Error::ActorPatternRecv(message)
        | Error::Tracker(message) => message.clone(),
        Error::SourceRedrive { message, .. } => message.clone(),
        _ => error.to_string(),
    }
}

impl SourceReader for BuiltinSource {
    fn name(&self) -> &'static str {
        self.factory.name()
    }

    async fn read(&mut self) -> Option<Result<Vec<Message>>> {
        self.read_messages().await
    }

    async fn partitions(&mut self) -> Result<SourcePartitions> {
        self.source_partitions().await
    }
}

impl SourceAcker for BuiltinSource {
    async fn ack(&mut self, offsets: Vec<Offset>) -> Result<()> {
        self.ack_offsets(offsets).await
    }

    async fn nack(&mut self, offsets: Vec<NackOffset>) -> Result<()> {
        self.nack_offsets(offsets).await
    }
}

impl LagReader for BuiltinSource {
    async fn pending(&mut self) -> Result<Option<usize>> {
        self.pending_messages().await
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::sync::Mutex as StdMutex;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;

    struct FakeBackend {
        read_error: Option<Error>,
        read_returns_none: bool,
        ack_error: Option<Error>,
        nack_error: Option<Error>,
        pending: Option<usize>,
        pending_error: Option<Error>,
        partitions: SourcePartitions,
        partitions_error: Option<Error>,
    }

    impl FakeBackend {
        fn healthy() -> Self {
            Self {
                read_error: None,
                read_returns_none: false,
                ack_error: None,
                nack_error: None,
                pending: Some(12),
                pending_error: None,
                partitions: SourcePartitions::new(vec![1, 2], Some(2)),
                partitions_error: None,
            }
        }
    }

    #[async_trait]
    impl BuiltinSourceBackend for FakeBackend {
        async fn read(&mut self) -> Option<Result<Vec<Message>>> {
            if self.read_returns_none {
                return None;
            }
            Some(self.read_error.take().map_or_else(|| Ok(vec![]), Err))
        }

        async fn ack(&mut self, _offsets: Vec<Offset>) -> Result<()> {
            self.ack_error.take().map_or(Ok(()), Err)
        }

        async fn nack(&mut self, _offsets: Vec<NackOffset>) -> Result<()> {
            self.nack_error.take().map_or(Ok(()), Err)
        }

        async fn pending(&mut self) -> Result<Option<usize>> {
            if let Some(error) = self.pending_error.take() {
                return Err(error);
            }
            Ok(self.pending)
        }

        async fn partitions(&mut self) -> Result<SourcePartitions> {
            if let Some(error) = self.partitions_error.take() {
                return Err(error);
            }
            Ok(self.partitions.clone())
        }
    }

    struct FakeFactory {
        builds: StdMutex<VecDeque<Result<FakeBackend>>>,
        build_count: AtomicUsize,
    }

    impl FakeFactory {
        fn new(builds: Vec<Result<FakeBackend>>) -> Self {
            Self {
                builds: StdMutex::new(builds.into()),
                build_count: AtomicUsize::new(0),
            }
        }
    }

    #[async_trait]
    impl BuiltinSourceFactory for FakeFactory {
        fn name(&self) -> &'static str {
            "fake"
        }

        async fn build(&self) -> Result<Box<dyn BuiltinSourceBackend>> {
            self.build_count.fetch_add(1, Ordering::SeqCst);
            self.builds
                .lock()
                .expect("build queue lock poisoned")
                .pop_front()
                .expect("unexpected build attempt")
                .map(|backend| Box::new(backend) as Box<dyn BuiltinSourceBackend>)
        }
    }

    struct SlowFactory {
        build_count: AtomicUsize,
    }

    #[async_trait]
    impl BuiltinSourceFactory for SlowFactory {
        fn name(&self) -> &'static str {
            "slow"
        }

        async fn build(&self) -> Result<Box<dyn BuiltinSourceBackend>> {
            self.build_count.fetch_add(1, Ordering::SeqCst);
            tokio::time::sleep(Duration::from_millis(200)).await;
            Ok(Box::new(FakeBackend::healthy()))
        }
    }

    struct BlockingBackend {
        read_started: Arc<tokio::sync::Notify>,
        release_read: Arc<tokio::sync::Notify>,
    }

    #[async_trait]
    impl BuiltinSourceBackend for BlockingBackend {
        async fn read(&mut self) -> Option<Result<Vec<Message>>> {
            self.read_started.notify_one();
            self.release_read.notified().await;
            Some(Ok(vec![]))
        }

        async fn ack(&mut self, _offsets: Vec<Offset>) -> Result<()> {
            Ok(())
        }

        async fn nack(&mut self, _offsets: Vec<NackOffset>) -> Result<()> {
            Ok(())
        }

        async fn pending(&mut self) -> Result<Option<usize>> {
            Ok(None)
        }

        async fn partitions(&mut self) -> Result<SourcePartitions> {
            Ok(SourcePartitions::default())
        }
    }

    struct BlockingFactory {
        read_started: Arc<tokio::sync::Notify>,
        release_read: Arc<tokio::sync::Notify>,
    }

    #[async_trait]
    impl BuiltinSourceFactory for BlockingFactory {
        fn name(&self) -> &'static str {
            "blocking"
        }

        async fn build(&self) -> Result<Box<dyn BuiltinSourceBackend>> {
            Ok(Box::new(BlockingBackend {
                read_started: Arc::clone(&self.read_started),
                release_read: Arc::clone(&self.release_read),
            }))
        }
    }

    fn retry_config() -> BuiltinSourceRetryConfig {
        BuiltinSourceRetryConfig {
            initial_backoff: Duration::from_millis(1),
            max_backoff: Duration::from_millis(4),
            build_timeout: Duration::from_millis(50),
            slow_recovery_interval: Duration::from_millis(5),
        }
    }

    fn test_source(
        factory: Arc<FakeFactory>,
        cancel_token: CancellationToken,
    ) -> (BuiltinSource, tempfile::TempDir) {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let source = BuiltinSource::with_runtime_error_path(
            factory,
            cancel_token,
            retry_config(),
            temp_dir.path().to_str().expect("temp path").to_string(),
        );
        (source, temp_dir)
    }

    fn runtime_error_files(temp_dir: &tempfile::TempDir) -> Vec<std::path::PathBuf> {
        let dir = temp_dir.path().join("numa");
        if !dir.exists() {
            return vec![];
        }
        std::fs::read_dir(dir)
            .unwrap()
            .filter_map(|entry| entry.ok())
            .map(|entry| entry.path())
            .filter(|path| path.extension().is_some_and(|ext| ext == "json"))
            .collect()
    }

    fn failed_read_backend(message: &str) -> FakeBackend {
        let mut backend = FakeBackend::healthy();
        backend.read_error = Some(Error::Source(message.into()));
        backend
    }

    fn source_redrive(
        operation: &'static str,
        action: SourceFailureAction,
        impact: SourceFailureImpact,
        code: &'static str,
        message: &str,
    ) -> Error {
        Error::SourceRedrive {
            source_name: "fake",
            operation,
            action,
            impact,
            code,
            message: message.into(),
        }
    }

    async fn wait_for_ready(source: &BuiltinSource) {
        for _ in 0..50 {
            if source.is_ready().await {
                return;
            }
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
        panic!("source did not become ready");
    }

    #[tokio::test]
    async fn startup_failure_is_retried_and_returns_source_redrive() {
        let factory = Arc::new(FakeFactory::new(vec![
            Err(Error::Config("missing secret".into())),
            Ok(FakeBackend::healthy()),
        ]));
        let (mut source, _temp_dir) = test_source(Arc::clone(&factory), CancellationToken::new());

        let error = source.read().await.unwrap().unwrap_err();
        assert!(matches!(
            error,
            Error::SourceRedrive {
                operation: "read",
                action: SourceFailureAction::Recreate,
                impact: SourceFailureImpact::Outage,
                ..
            }
        ));
        assert_eq!(source.health().await, BuiltinSourceHealth::Degraded);

        wait_for_ready(&source).await;
        assert_eq!(source.read().await.unwrap().unwrap().len(), 0);
        assert!(source.is_ready().await);
        assert_eq!(factory.build_count.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn read_failure_recreates_backend_and_returns_source_redrive() {
        let mut failed_backend = FakeBackend::healthy();
        failed_backend.read_error = Some(Error::Source("broker disconnected".into()));
        let factory = Arc::new(FakeFactory::new(vec![
            Ok(failed_backend),
            Ok(FakeBackend::healthy()),
        ]));
        let (mut source, _temp_dir) = test_source(Arc::clone(&factory), CancellationToken::new());

        wait_for_ready(&source).await;
        let generation_before = source.generation();

        let error = source.read().await.unwrap().unwrap_err();
        assert!(matches!(
            error,
            Error::SourceRedrive {
                operation: "read",
                action: SourceFailureAction::Recreate,
                ..
            }
        ));
        assert_eq!(source.health().await, BuiltinSourceHealth::Degraded);

        wait_for_ready(&source).await;
        assert!(source.generation() > generation_before);
        assert_eq!(source.read().await.unwrap().unwrap().len(), 0);
        assert!(source.is_ready().await);
        assert_eq!(factory.build_count.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn recreate_waits_for_an_active_builder_to_release_its_slot() {
        let mut failed_backend = FakeBackend::healthy();
        failed_backend.read_error = Some(Error::Source("broker disconnected".into()));
        let factory = Arc::new(FakeFactory::new(vec![
            Ok(failed_backend),
            Ok(FakeBackend::healthy()),
        ]));
        let (mut source, _temp_dir) = test_source(Arc::clone(&factory), CancellationToken::new());

        wait_for_ready(&source).await;
        let generation_before = source.generation();
        source.builder_active.store(true, Ordering::Release);

        assert!(matches!(
            source.read().await.unwrap().unwrap_err(),
            Error::SourceRedrive {
                action: SourceFailureAction::Recreate,
                ..
            }
        ));
        assert_eq!(source.health().await, BuiltinSourceHealth::Degraded);

        source.builder_active.store(false, Ordering::Release);
        wait_for_ready(&source).await;
        assert!(source.generation() > generation_before);
        assert_eq!(factory.build_count.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn ack_failure_requests_redrive_then_succeeds_after_recreation() {
        let mut failed_backend = FakeBackend::healthy();
        failed_backend.ack_error = Some(Error::NonRetryable("authorization failed".into()));
        let factory = Arc::new(FakeFactory::new(vec![
            Ok(failed_backend),
            Ok(FakeBackend::healthy()),
        ]));
        let (mut source, _temp_dir) = test_source(Arc::clone(&factory), CancellationToken::new());

        wait_for_ready(&source).await;

        let error = source.ack(vec![]).await.unwrap_err();
        assert!(matches!(
            error,
            Error::SourceRedrive {
                operation: "ack",
                action: SourceFailureAction::Recreate,
                code: "non_retryable",
                ..
            }
        ));

        wait_for_ready(&source).await;
        source.ack(vec![]).await.unwrap();
        assert!(source.is_ready().await);
        assert_eq!(factory.build_count.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn cancellation_ends_read_instead_of_redriving() {
        let cancel_token = CancellationToken::new();
        cancel_token.cancel();
        let factory = Arc::new(FakeFactory::new(vec![]));
        let mut source = BuiltinSource::new(factory, cancel_token);

        assert!(source.read().await.is_none());
    }

    #[tokio::test]
    async fn health_transitions_through_degraded_and_recovers() {
        let factory = Arc::new(FakeFactory::new(vec![
            Ok(failed_read_backend("broker disconnected")),
            Ok(FakeBackend::healthy()),
        ]));
        let (mut source, _temp_dir) = test_source(Arc::clone(&factory), CancellationToken::new());

        assert_eq!(source.health().await, BuiltinSourceHealth::Starting);
        wait_for_ready(&source).await;
        assert_eq!(source.health().await, BuiltinSourceHealth::Ready);

        let error = source.read().await.unwrap().unwrap_err();
        assert!(matches!(error, Error::SourceRedrive { .. }));
        assert_eq!(source.health().await, BuiltinSourceHealth::Degraded);

        wait_for_ready(&source).await;
        assert_eq!(source.read().await.unwrap().unwrap().len(), 0);
        assert_eq!(source.health().await, BuiltinSourceHealth::Ready);
    }

    #[tokio::test]
    async fn nack_failure_requests_redrive_then_succeeds_after_recreation() {
        let mut failed_backend = FakeBackend::healthy();
        failed_backend.nack_error = Some(Error::Source("nack rejected".into()));
        let factory = Arc::new(FakeFactory::new(vec![
            Ok(failed_backend),
            Ok(FakeBackend::healthy()),
        ]));
        let (mut source, _temp_dir) = test_source(Arc::clone(&factory), CancellationToken::new());

        wait_for_ready(&source).await;

        let error = source.nack(vec![]).await.unwrap_err();
        assert!(matches!(
            error,
            Error::SourceRedrive {
                operation: "nack",
                ..
            }
        ));

        wait_for_ready(&source).await;
        source.nack(vec![]).await.unwrap();
        assert!(source.is_ready().await);
    }

    #[tokio::test]
    async fn repeated_same_failure_before_backoff_persists_one_runtime_error() {
        let factory = Arc::new(FakeFactory::new(vec![
            Ok(failed_read_backend("broker disconnected")),
            Ok(FakeBackend::healthy()),
        ]));
        let (mut source, temp_dir) = test_source(Arc::clone(&factory), CancellationToken::new());

        wait_for_ready(&source).await;
        assert_eq!(
            source.read().await.unwrap().unwrap_err().to_string(),
            source_redrive(
                "read",
                SourceFailureAction::Recreate,
                SourceFailureImpact::Outage,
                "source",
                "broker disconnected",
            )
            .to_string()
        );
        assert_eq!(runtime_error_files(&temp_dir).len(), 1);

        // Backoff has not elapsed yet, so the supervisor should not report again.
        assert!(matches!(
            source.read().await.unwrap().unwrap_err(),
            Error::SourceRedrive {
                operation: "read",
                ..
            }
        ));
        assert_eq!(runtime_error_files(&temp_dir).len(), 1);
    }

    #[tokio::test]
    async fn changed_message_with_same_code_remains_deduplicated_until_recovery() {
        let factory = Arc::new(FakeFactory::new(vec![
            Ok(failed_read_backend("broker disconnected")),
            Ok(failed_read_backend("authorization failed")),
            Ok(FakeBackend::healthy()),
        ]));
        let (mut source, temp_dir) = test_source(Arc::clone(&factory), CancellationToken::new());

        wait_for_ready(&source).await;
        assert_eq!(
            source.read().await.unwrap().unwrap_err().to_string(),
            source_redrive(
                "read",
                SourceFailureAction::Recreate,
                SourceFailureImpact::Outage,
                "source",
                "broker disconnected",
            )
            .to_string()
        );
        assert_eq!(runtime_error_files(&temp_dir).len(), 1);

        wait_for_ready(&source).await;
        assert_eq!(
            source.read().await.unwrap().unwrap_err().to_string(),
            source_redrive(
                "read",
                SourceFailureAction::Recreate,
                SourceFailureImpact::Outage,
                "source",
                "authorization failed",
            )
            .to_string()
        );
        assert_eq!(runtime_error_files(&temp_dir).len(), 1);
    }

    #[tokio::test]
    async fn build_timeout_marks_degraded_without_panicking() {
        let factory = Arc::new(SlowFactory {
            build_count: AtomicUsize::new(0),
        });
        let mut config = retry_config();
        config.build_timeout = Duration::from_millis(20);
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let mut source = BuiltinSource::with_runtime_error_path(
            factory,
            CancellationToken::new(),
            config,
            temp_dir.path().to_str().expect("temp path").to_string(),
        );

        let error = source.read().await.unwrap().unwrap_err();
        assert!(matches!(error, Error::SourceRedrive { .. }));
        assert_eq!(source.health().await, BuiltinSourceHealth::Degraded);
    }

    #[tokio::test]
    async fn readiness_does_not_wait_for_backend_io() {
        let read_started = Arc::new(tokio::sync::Notify::new());
        let release_read = Arc::new(tokio::sync::Notify::new());
        let factory = Arc::new(BlockingFactory {
            read_started: Arc::clone(&read_started),
            release_read: Arc::clone(&release_read),
        });
        let source =
            BuiltinSource::with_retry_config(factory, CancellationToken::new(), retry_config());
        wait_for_ready(&source).await;

        let mut reader = source.clone();
        let read_task = tokio::spawn(async move { reader.read().await });
        read_started.notified().await;

        assert!(
            tokio::time::timeout(Duration::from_millis(10), source.is_ready())
                .await
                .expect("readiness must not wait for backend I/O")
        );
        release_read.notify_one();
        assert!(read_task.await.unwrap().unwrap().is_ok());
    }

    #[tokio::test]
    async fn stream_closed_recreates_backend_and_returns_source_redrive() {
        let mut closed_backend = FakeBackend::healthy();
        closed_backend.read_returns_none = true;
        let factory = Arc::new(FakeFactory::new(vec![
            Ok(closed_backend),
            Ok(FakeBackend::healthy()),
        ]));
        let (mut source, _temp_dir) = test_source(Arc::clone(&factory), CancellationToken::new());

        wait_for_ready(&source).await;
        let error = source.read().await.unwrap().unwrap_err();
        assert!(matches!(error, Error::SourceRedrive { .. }));
        assert_eq!(source.health().await, BuiltinSourceHealth::Degraded);

        wait_for_ready(&source).await;
        assert_eq!(source.read().await.unwrap().unwrap().len(), 0);
        assert!(source.is_ready().await);
        assert_eq!(factory.build_count.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn pending_failure_marks_degraded() {
        let mut failing = FakeBackend::healthy();
        failing.pending_error = Some(Error::Source("lag query failed".into()));
        let factory = Arc::new(FakeFactory::new(vec![Ok(failing)]));
        let (mut source, _temp_dir) = test_source(factory, CancellationToken::new());

        wait_for_ready(&source).await;
        assert_eq!(source.pending().await.unwrap(), None);
        assert_eq!(source.health().await, BuiltinSourceHealth::Degraded);
    }

    #[tokio::test]
    async fn pending_after_success_returns_cached_value_when_later_query_fails() {
        let mut read_failed = FakeBackend::healthy();
        read_failed.read_error = Some(Error::Source("read failed".into()));
        let mut pending_failed = FakeBackend::healthy();
        pending_failed.pending_error = Some(Error::Source("lag query failed".into()));
        let factory = Arc::new(FakeFactory::new(vec![Ok(read_failed), Ok(pending_failed)]));
        let (mut source, _temp_dir) = test_source(Arc::clone(&factory), CancellationToken::new());

        wait_for_ready(&source).await;
        assert_eq!(source.pending().await.unwrap(), Some(12));
        assert_eq!(
            source.read().await.unwrap().unwrap_err().to_string(),
            source_redrive(
                "read",
                SourceFailureAction::Recreate,
                SourceFailureImpact::Outage,
                "source",
                "read failed",
            )
            .to_string()
        );
        assert_eq!(source.health().await, BuiltinSourceHealth::Degraded);

        wait_for_ready(&source).await;
        assert_eq!(source.read().await.unwrap().unwrap().len(), 0);
        assert_eq!(source.pending().await.unwrap(), Some(12));
        assert_eq!(source.health().await, BuiltinSourceHealth::Degraded);
    }

    #[tokio::test]
    async fn partitions_failure_marks_degraded_and_returns_cached_partitions() {
        let mut failing = FakeBackend::healthy();
        failing.partitions_error = Some(Error::Source("metadata unavailable".into()));
        let factory = Arc::new(FakeFactory::new(vec![Ok(failing)]));
        let (mut source, _temp_dir) = test_source(factory, CancellationToken::new());

        wait_for_ready(&source).await;
        let partitions = source.partitions().await.unwrap();
        assert!(partitions.active_partitions.is_empty());
        assert_eq!(source.health().await, BuiltinSourceHealth::Degraded);
    }

    #[tokio::test]
    async fn pending_while_backend_unavailable_returns_last_cached_value() {
        let factory = Arc::new(FakeFactory::new(vec![
            Ok(failed_read_backend("broker disconnected")),
            Err(Error::Config("still starting".into())),
            Ok(FakeBackend::healthy()),
        ]));
        let (mut source, _temp_dir) = test_source(Arc::clone(&factory), CancellationToken::new());

        wait_for_ready(&source).await;
        assert_eq!(source.pending().await.unwrap(), Some(12));

        let error = source.read().await.unwrap().unwrap_err();
        assert!(matches!(error, Error::SourceRedrive { .. }));

        assert_eq!(source.pending().await.unwrap(), Some(12));
    }

    #[tokio::test]
    async fn source_name_delegates_to_factory() {
        let factory = Arc::new(FakeFactory::new(vec![Ok(FakeBackend::healthy())]));
        let source =
            BuiltinSource::with_retry_config(factory, CancellationToken::new(), retry_config());
        assert_eq!(SourceReader::name(&source), "fake");
    }

    #[tokio::test]
    async fn retry_same_benign_keeps_ready_without_rebuild() {
        let mut backend = FakeBackend::healthy();
        backend.read_error = Some(source_redrive(
            "read",
            SourceFailureAction::RetrySame,
            SourceFailureImpact::Benign,
            "rebalance",
            "rebalance in progress",
        ));
        let factory = Arc::new(FakeFactory::new(vec![Ok(backend)]));
        let (mut source, temp_dir) = test_source(factory, CancellationToken::new());

        wait_for_ready(&source).await;
        source.ack(vec![]).await.unwrap();
        assert!(source.is_watermark_ready_now());
        let generation_before = source.generation();

        let error = source.read().await.unwrap().unwrap_err();
        assert!(matches!(
            error,
            Error::SourceRedrive {
                action: SourceFailureAction::RetrySame,
                impact: SourceFailureImpact::Benign,
                code: "rebalance",
                ..
            }
        ));
        assert!(source.is_ready().await);
        assert!(source.is_watermark_ready_now());
        assert_eq!(source.generation(), generation_before);
        assert!(runtime_error_files(&temp_dir).is_empty());
    }

    #[tokio::test]
    async fn retry_same_outage_marks_degraded_and_recovers_on_success() {
        let mut outage_backend = FakeBackend::healthy();
        outage_backend.read_error = Some(source_redrive(
            "read",
            SourceFailureAction::RetrySame,
            SourceFailureImpact::Outage,
            "broker_unavailable",
            "temporary broker outage",
        ));
        let factory = Arc::new(FakeFactory::new(vec![
            Ok(outage_backend),
            Ok(FakeBackend::healthy()),
        ]));
        let (mut source, temp_dir) = test_source(Arc::clone(&factory), CancellationToken::new());

        wait_for_ready(&source).await;
        source.ack(vec![]).await.unwrap();
        assert!(source.is_watermark_ready_now());
        let generation_before = source.generation();

        let error = source.read().await.unwrap().unwrap_err();
        assert!(matches!(
            error,
            Error::SourceRedrive {
                action: SourceFailureAction::RetrySame,
                impact: SourceFailureImpact::Outage,
                ..
            }
        ));
        assert_eq!(source.health().await, BuiltinSourceHealth::Degraded);
        assert!(!source.is_watermark_ready_now());
        assert_eq!(source.generation(), generation_before);
        assert_eq!(factory.build_count.load(Ordering::SeqCst), 1);
        assert_eq!(runtime_error_files(&temp_dir).len(), 1);

        assert_eq!(source.pending().await.unwrap(), Some(12));
        assert_eq!(
            source.partitions().await.unwrap().active_partitions,
            vec![1, 2]
        );
        assert_eq!(source.health().await, BuiltinSourceHealth::Degraded);
        assert!(!source.is_watermark_ready_now());

        assert_eq!(source.read().await.unwrap().unwrap().len(), 0);
        assert!(source.is_ready().await);
        assert!(source.is_watermark_ready_now());
        assert_eq!(factory.build_count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn stay_degraded_uses_slow_recovery_interval() {
        let mut backend = FakeBackend::healthy();
        backend.read_error = Some(source_redrive(
            "read",
            SourceFailureAction::StayDegraded,
            SourceFailureImpact::Outage,
            "auth_failed",
            "authorization failed permanently",
        ));
        let factory = Arc::new(FakeFactory::new(vec![
            Ok(backend),
            Ok(FakeBackend::healthy()),
        ]));
        let (mut source, _temp_dir) = test_source(Arc::clone(&factory), CancellationToken::new());

        wait_for_ready(&source).await;
        let error = source.read().await.unwrap().unwrap_err();
        assert!(matches!(
            error,
            Error::SourceRedrive {
                action: SourceFailureAction::StayDegraded,
                ..
            }
        ));
        assert_eq!(source.health().await, BuiltinSourceHealth::Degraded);
        assert_eq!(factory.build_count.load(Ordering::SeqCst), 1);

        wait_for_ready(&source).await;
        assert_eq!(source.read().await.unwrap().unwrap().len(), 0);
        assert_eq!(factory.build_count.load(Ordering::SeqCst), 2);
    }
}
