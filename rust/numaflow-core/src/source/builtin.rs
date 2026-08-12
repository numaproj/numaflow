//! Resilience wrapper for built-in sources (Kafka, Pulsar, SQS, NATS, HTTP).
//!
//! When a source client fails, [`BuiltinSource`] recreates it with backoff instead of
//! crashing `numa`. In-flight ack state is preserved by each source adapter; this module
//! owns retry timing, health/readiness, and runtime-error reporting.

use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::Mutex;
use tokio::time::{Duration, Instant, sleep_until, timeout};
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::error::{Error, Result};
use crate::message::{Message, NackOffset, Offset};
use crate::reader::LagReader;
use crate::source::runtime_error::SourceRuntimeErrorTracker;
use crate::source::{SourceAcker, SourcePartitions, SourceReader};

// Cap read-side wait so the forwarder is not blocked for the full backoff.
const READ_RETRY_WAIT_CAP: Duration = Duration::from_millis(100);

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
}

/// Adapts an existing source type (Kafka, Pulsar, …) to [`BuiltinSourceBackend`].
///
/// This avoids duplicating read/ack/nack wiring in every built-in adapter.
pub(crate) struct SourceBackend<T>(T);

impl<T> SourceBackend<T> {
    pub(crate) fn new(source: T) -> Self {
        Self(source)
    }
}

#[async_trait]
impl<T> BuiltinSourceBackend for SourceBackend<T>
where
    T: SourceReader + SourceAcker + LagReader + Send,
{
    async fn read(&mut self) -> Option<Result<Vec<Message>>> {
        SourceReader::read(&mut self.0).await
    }

    async fn ack(&mut self, offsets: Vec<Offset>) -> Result<()> {
        SourceAcker::ack(&mut self.0, offsets).await
    }

    async fn nack(&mut self, offsets: Vec<NackOffset>) -> Result<()> {
        SourceAcker::nack(&mut self.0, offsets).await
    }

    async fn pending(&mut self) -> Result<Option<usize>> {
        LagReader::pending(&mut self.0).await
    }

    async fn partitions(&mut self) -> Result<SourcePartitions> {
        SourceReader::partitions(&mut self.0).await
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

/// Health of a built-in source from the supervisor's point of view.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BuiltinSourceHealth {
    /// Initial startup; backend not ready yet.
    Starting,
    /// Backend is connected and operations succeed.
    Ready,
    /// Backend failed or is being recreated; source is not ready but `numa` keeps running.
    Degraded,
}

/// Controls how long the supervisor waits between rebuild attempts.
#[derive(Debug, Clone, Copy)]
pub(crate) struct BuiltinSourceRetryConfig {
    pub(crate) initial_backoff: Duration,
    pub(crate) max_backoff: Duration,
    pub(crate) build_timeout: Duration,
}

impl Default for BuiltinSourceRetryConfig {
    fn default() -> Self {
        Self {
            initial_backoff: Duration::from_millis(100),
            max_backoff: Duration::from_secs(5),
            build_timeout: Duration::from_secs(10),
        }
    }
}

/// Mutable supervisor state: active backend, retry schedule, cached lag/partitions,
/// and runtime-error deduplication.
struct SupervisorState {
    backend: Option<Box<dyn BuiltinSourceBackend>>,
    health: BuiltinSourceHealth,
    retry_backoff: Duration,
    retry_at: Instant,
    last_pending: Option<usize>,
    last_partitions: SourcePartitions,
    runtime_error_tracker: SourceRuntimeErrorTracker,
}

/// Resilient wrapper around built-in sources.
///
/// On failure, recreates the underlying client with exponential backoff and keeps
/// `numa` alive. Behavior by operation:
///
/// - **read** — returns an empty batch while recovering (forwarder keeps polling)
/// - **ack / nack** — returns [`Error::SourceRedrive`] so offsets are retried later
/// - **pending / partitions** — returns the last known good value while degraded
///
/// Readiness ([`is_ready`]) is false while degraded; liveness is unaffected.
#[derive(Clone)]
pub(crate) struct BuiltinSource {
    factory: Arc<dyn BuiltinSourceFactory>,
    state: Arc<Mutex<SupervisorState>>,
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
        assert!(
            !retry_config.initial_backoff.is_zero(),
            "initial source retry backoff must be greater than zero"
        );
        assert!(
            retry_config.initial_backoff <= retry_config.max_backoff,
            "initial source retry backoff must not exceed max backoff"
        );
        assert!(
            !retry_config.build_timeout.is_zero(),
            "source build timeout must be greater than zero"
        );

        Self {
            factory,
            state: Arc::new(Mutex::new(SupervisorState {
                backend: None,
                health: BuiltinSourceHealth::Starting,
                retry_backoff: retry_config.initial_backoff,
                retry_at: Instant::now(),
                last_pending: None,
                last_partitions: SourcePartitions::default(),
                runtime_error_tracker,
            })),
            cancel_token,
            retry_config,
        }
    }

    pub(crate) async fn health(&self) -> BuiltinSourceHealth {
        self.state.lock().await.health
    }

    pub(crate) async fn is_ready(&self) -> bool {
        self.health().await == BuiltinSourceHealth::Ready
    }

    fn redrive_error(&self, operation: &'static str, error: impl fmt::Display) -> Error {
        Error::SourceRedrive {
            source_name: self.factory.name().to_string(),
            operation,
            message: error.to_string(),
        }
    }

    fn mark_degraded(&self, state: &mut SupervisorState, operation: &'static str, error: &Error) {
        state
            .runtime_error_tracker
            .record_failure(self.factory.name(), operation, error);
        warn!(
            source = self.factory.name(),
            operation,
            ?error,
            retry_in_ms = state.retry_backoff.as_millis(),
            "Built-in source operation failed; will recreate the source client after backoff"
        );
        state.backend = None;
        state.health = BuiltinSourceHealth::Degraded;
        state.retry_at = Instant::now() + state.retry_backoff;
        state.retry_backoff = state
            .retry_backoff
            .saturating_mul(2)
            .min(self.retry_config.max_backoff);
    }

    async fn ensure_backend(&self, state: &mut SupervisorState) -> Result<()> {
        if state.backend.is_some() {
            return Ok(());
        }
        if self.cancel_token.is_cancelled() {
            return Err(Error::Cancelled());
        }
        if Instant::now() < state.retry_at {
            return Err(self.redrive_error("build", "backend recreation is waiting for backoff"));
        }

        let build = timeout(self.retry_config.build_timeout, self.factory.build());
        let result = tokio::select! {
            _ = self.cancel_token.cancelled() => return Err(Error::Cancelled()),
            result = build => result,
        };
        match result {
            Ok(Ok(backend)) => {
                let recovered = state.health == BuiltinSourceHealth::Degraded;
                state.backend = Some(backend);
                state.health = BuiltinSourceHealth::Ready;
                state.retry_backoff = self.retry_config.initial_backoff;
                state.retry_at = Instant::now();
                if recovered {
                    state
                        .runtime_error_tracker
                        .record_recovery(self.factory.name());
                    info!(
                        source = self.factory.name(),
                        "Source client reconnected successfully"
                    );
                }
                Ok(())
            }
            Ok(Err(error)) => {
                self.mark_degraded(state, "build", &error);
                Err(self.redrive_error("build", error))
            }
            Err(error) => {
                let error = Error::Source(format!(
                    "timed out after {:?} building {} source backend: {error}",
                    self.retry_config.build_timeout,
                    self.factory.name()
                ));
                self.mark_degraded(state, "build", &error);
                Err(self.redrive_error("build", error))
            }
        }
    }

    async fn wait_after_read_failure(&self, retry_at: Instant) {
        let wait_until = retry_at.min(Instant::now() + READ_RETRY_WAIT_CAP);
        tokio::select! {
            _ = self.cancel_token.cancelled() => {}
            _ = sleep_until(wait_until) => {}
        }
    }

    async fn read_messages(&mut self) -> Option<Result<Vec<Message>>> {
        if self.cancel_token.is_cancelled() {
            return None;
        }

        let mut state = self.state.lock().await;
        if self.ensure_backend(&mut state).await.is_err() {
            let retry_at = state.retry_at;
            drop(state);
            self.wait_after_read_failure(retry_at).await;
            return (!self.cancel_token.is_cancelled()).then_some(Ok(vec![]));
        }

        let result = state
            .backend
            .as_mut()
            .expect("backend must exist after ensure_backend")
            .read()
            .await;
        match result {
            Some(Ok(messages)) => Some(Ok(messages)),
            Some(Err(error)) => {
                self.mark_degraded(&mut state, "read", &error);
                let retry_at = state.retry_at;
                drop(state);
                self.wait_after_read_failure(retry_at).await;
                (!self.cancel_token.is_cancelled()).then_some(Ok(vec![]))
            }
            None if self.cancel_token.is_cancelled() => None,
            None => {
                let error = Error::Source(format!(
                    "{} source stream closed unexpectedly",
                    self.factory.name()
                ));
                self.mark_degraded(&mut state, "read", &error);
                let retry_at = state.retry_at;
                drop(state);
                self.wait_after_read_failure(retry_at).await;
                (!self.cancel_token.is_cancelled()).then_some(Ok(vec![]))
            }
        }
    }

    async fn ack_offsets(&mut self, offsets: Vec<Offset>) -> Result<()> {
        let mut state = self.state.lock().await;
        self.ensure_backend(&mut state).await?;
        let result = state
            .backend
            .as_mut()
            .expect("backend must exist after ensure_backend")
            .ack(offsets)
            .await;
        if let Err(error) = result {
            self.mark_degraded(&mut state, "ack", &error);
            return Err(self.redrive_error("ack", error));
        }
        Ok(())
    }

    async fn nack_offsets(&mut self, offsets: Vec<NackOffset>) -> Result<()> {
        let mut state = self.state.lock().await;
        self.ensure_backend(&mut state).await?;
        let result = state
            .backend
            .as_mut()
            .expect("backend must exist after ensure_backend")
            .nack(offsets)
            .await;
        if let Err(error) = result {
            self.mark_degraded(&mut state, "nack", &error);
            return Err(self.redrive_error("nack", error));
        }
        Ok(())
    }

    async fn pending_messages(&mut self) -> Result<Option<usize>> {
        let mut state = self.state.lock().await;
        if self.ensure_backend(&mut state).await.is_err() {
            return Ok(state.last_pending);
        }
        match state
            .backend
            .as_mut()
            .expect("backend must exist after ensure_backend")
            .pending()
            .await
        {
            Ok(pending) => {
                state.last_pending = pending;
                Ok(pending)
            }
            Err(error) => {
                self.mark_degraded(&mut state, "pending", &error);
                Ok(state.last_pending)
            }
        }
    }

    async fn source_partitions(&mut self) -> Result<SourcePartitions> {
        let mut state = self.state.lock().await;
        if self.ensure_backend(&mut state).await.is_err() {
            return Ok(state.last_partitions.clone());
        }
        match state
            .backend
            .as_mut()
            .expect("backend must exist after ensure_backend")
            .partitions()
            .await
        {
            Ok(partitions) => {
                state.last_partitions = partitions.clone();
                Ok(partitions)
            }
            Err(error) => {
                self.mark_degraded(&mut state, "partitions", &error);
                Ok(state.last_partitions.clone())
            }
        }
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
        ack_error: Option<Error>,
        pending: Option<usize>,
        partitions: SourcePartitions,
    }

    impl FakeBackend {
        fn healthy() -> Self {
            Self {
                read_error: None,
                ack_error: None,
                pending: Some(12),
                partitions: SourcePartitions::new(vec![1, 2], Some(2)),
            }
        }
    }

    #[async_trait]
    impl BuiltinSourceBackend for FakeBackend {
        async fn read(&mut self) -> Option<Result<Vec<Message>>> {
            Some(self.read_error.take().map_or_else(|| Ok(vec![]), Err))
        }

        async fn ack(&mut self, _offsets: Vec<Offset>) -> Result<()> {
            self.ack_error.take().map_or(Ok(()), Err)
        }

        async fn nack(&mut self, _offsets: Vec<NackOffset>) -> Result<()> {
            Ok(())
        }

        async fn pending(&mut self) -> Result<Option<usize>> {
            Ok(self.pending)
        }

        async fn partitions(&mut self) -> Result<SourcePartitions> {
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

    fn retry_config() -> BuiltinSourceRetryConfig {
        BuiltinSourceRetryConfig {
            initial_backoff: Duration::from_millis(1),
            max_backoff: Duration::from_millis(4),
            build_timeout: Duration::from_millis(50),
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

    #[tokio::test]
    async fn startup_failure_is_retried_without_returning_read_error() {
        let factory = Arc::new(FakeFactory::new(vec![
            Err(Error::Config("missing secret".into())),
            Ok(FakeBackend::healthy()),
        ]));
        let (mut source, _temp_dir) = test_source(factory.clone(), CancellationToken::new());

        assert_eq!(source.read().await.unwrap().unwrap().len(), 0);
        assert_eq!(source.health().await, BuiltinSourceHealth::Degraded);
        tokio::time::sleep(Duration::from_millis(2)).await;
        assert_eq!(source.read().await.unwrap().unwrap().len(), 0);
        assert!(source.is_ready().await);
        assert_eq!(factory.build_count.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn read_failure_recreates_backend_and_returns_empty_batch() {
        let mut failed_backend = FakeBackend::healthy();
        failed_backend.read_error = Some(Error::Source("broker disconnected".into()));
        let factory = Arc::new(FakeFactory::new(vec![
            Ok(failed_backend),
            Ok(FakeBackend::healthy()),
        ]));
        let (mut source, _temp_dir) = test_source(factory.clone(), CancellationToken::new());

        assert_eq!(source.read().await.unwrap().unwrap().len(), 0);
        assert_eq!(source.health().await, BuiltinSourceHealth::Degraded);
        tokio::time::sleep(Duration::from_millis(2)).await;
        assert_eq!(source.read().await.unwrap().unwrap().len(), 0);
        assert!(source.is_ready().await);
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
        let (mut source, _temp_dir) = test_source(factory.clone(), CancellationToken::new());

        let error = source.ack(vec![]).await.unwrap_err();
        assert!(matches!(
            error,
            Error::SourceRedrive {
                operation: "ack",
                ..
            }
        ));
        tokio::time::sleep(Duration::from_millis(2)).await;
        source.ack(vec![]).await.unwrap();
        assert!(source.is_ready().await);
        assert_eq!(factory.build_count.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn lag_and_partitions_use_last_successful_values_while_degraded() {
        let factory = Arc::new(FakeFactory::new(vec![Ok(FakeBackend::healthy())]));
        let mut source =
            BuiltinSource::with_retry_config(factory, CancellationToken::new(), retry_config());

        assert_eq!(source.pending().await.unwrap(), Some(12));
        let partitions = source.partitions().await.unwrap();
        assert_eq!(partitions.active_partitions, vec![1, 2]);
        assert_eq!(partitions.total_partitions, Some(2));
    }

    #[tokio::test]
    async fn cancellation_ends_read_instead_of_redriving() {
        let cancel_token = CancellationToken::new();
        cancel_token.cancel();
        let factory = Arc::new(FakeFactory::new(vec![]));
        let mut source = BuiltinSource::new(factory, cancel_token);

        assert!(source.read().await.is_none());
    }
}
