use metrics::MetricsState;
use shared::metrics::start_metrics_server;
use std::collections::HashMap;
use std::future::Future;
use std::time::Duration;

use crate::config::CustomResourceType;
use bytes::Bytes;
use config::Settings;
pub mod runtime_server;

use pipeline::forwarder;
use runtime_server::runtime;
use tokio::signal;
use tokio::task::JoinHandle;
use tokio::time;
use tokio_util::sync::CancellationToken;
use tonic::{Code, Status};
use tracing::{error, info};

/// Macro to emit critical error metrics for both MonoVertex and Pipeline.
///
/// This macro checks whether the current runtime is a MonoVertex or a Pipeline
/// and emits the appropriate critical error metric.
///
/// # Arguments
/// * `$vertex_type` - The vertex type (e.g., `VERTEX_TYPE_SINK`, `VERTEX_TYPE_SOURCE`).
///   This is only used for Pipeline metrics.
/// * `$reason` - The reason for the critical error (e.g., `"eot_received_from_sink"`).
///
/// # Example
/// ```ignore
/// use crate::critical_error;
/// use crate::config::pipeline::VERTEX_TYPE_SINK;
///
/// critical_error!(VERTEX_TYPE_SINK, "eot_received_from_sink");
/// ```
#[macro_export]
macro_rules! critical_error {
    ($vertex_type:expr, $reason:expr) => {
        if $crate::config::is_mono_vertex() {
            $crate::metrics::monovertex_metrics()
                .critical_error_total
                .get_or_create(&$crate::metrics::mvtx_critical_error_metric_labels($reason))
                .inc();
        } else {
            $crate::metrics::pipeline_metrics()
                .forwarder
                .critical_error_total
                .get_or_create(&$crate::metrics::pipeline_critical_error_metric_labels(
                    $vertex_type,
                    $reason,
                ))
                .inc();
        }
    };
}

/// Custom Error handling.
mod error;
pub(crate) use crate::error::{Error, Result};

/// [MonoVertex] is a simplified version of the [Pipeline] spec which is ideal for high TPS, low latency
/// use-cases which do not require [ISB].
///
/// [MonoVertex]: https://numaflow.numaproj.io/core-concepts/monovertex/
/// [Pipeline]: https://numaflow.numaproj.io/core-concepts/pipeline/
/// [ISB]: https://numaflow.numaproj.io/core-concepts/inter-step-buffer/
pub mod monovertex;

/// Parse configs, including Numaflow specifications.
mod config;

/// Internal message structure that is passed around.
mod message;
/// Metadata conversion utilities and types.
mod metadata;
/// Shared entities that can be used orthogonal to different modules.
mod shared;
/// [Sink] serves as the endpoint for processed data that has been outputted from the platform,
/// which is then sent to an external system or application.
///
/// [Sink]: https://numaflow.numaproj.io/user-guide/sinks/overview/
mod sinker;
/// [Source] is responsible for reliable reading data from an unbounded source into Numaflow.
///
/// [Source]: https://numaflow.numaproj.io/user-guide/sources/overview/
mod source;
/// Transformer is a feature that allows users to execute custom code to transform their data at
/// [source].
///
/// [Transformer]: https://numaflow.numaproj.io/user-guide/sources/transformer/overview/
mod transformer;

/// Reads from a stream.
mod reader;

pub(crate) mod metrics;
/// [Pipeline]
///
/// [Pipeline]: https://numaflow.numaproj.io/core-concepts/pipeline/
pub(crate) mod pipeline;

/// Tracker to track the completeness of message processing.
mod tracker;

/// [Map] is a feature that allows users to execute custom code to transform their data.
///
/// [Map]: https://numaflow.numaproj.io/user-guide/user-defined-functions/map/map/
mod mapper;

/// [Watermark] _is a monotonically increasing timestamp of the oldest work/event not yet completed_
///
///
/// [Watermark]: https://numaflow.numaproj.io/core-concepts/watermarks/
mod watermark;

/// Type configuration trait for Numaflow components.
pub(crate) mod typ;

/// [Reduce] is a function which "collects" a group of items and then perform some "reduction" operation
/// on all of them, thus reducing them to a single value.
///
/// [Reduce]:https://numaflow.numaproj.io/user-guide/user-defined-functions/reduce/reduce/
mod reduce;

/// Eagerly populate the cached OTel `BoxedTracer` after the binary has registered its tracer
/// provider. See [`shared::otel::init_tracer`] for the rationale.
pub use shared::otel::init_tracer;

const FORWARDER_RESTART_BACKOFF: Duration = Duration::from_secs(1);

pub async fn run() -> Result<()> {
    let root_token = CancellationToken::new();
    let shutdown_cln_token = root_token.clone();

    // wait for SIG{INT,TERM} and invoke cancellation token.
    let shutdown_handle: JoinHandle<Result<()>> = tokio::spawn(async move {
        shutdown_signal().await;
        shutdown_cln_token.cancel();
        Ok(())
    });

    let env_vars: HashMap<String, String> = std::env::vars().collect();
    let settings = Settings::load(env_vars)?;
    let crd_type = settings.custom_resource_type.clone();
    let metrics_config = match &crd_type {
        CustomResourceType::MonoVertex(config) => config.metrics_config.clone(),
        CustomResourceType::Pipeline(config) => config.metrics_config.clone(),
    };

    let _runtime_server_handle = runtime_server::spawn_runtime_errors_server(root_token.clone())
        .await
        .map_err(|e| Error::Mapper(format!("failed to start runtime errors server: {e}")))?;
    let metrics_state = MetricsState::new();
    let _metrics_server_handle =
        start_metrics_server(metrics_config, metrics_state.clone()).await?;

    match crd_type {
        CustomResourceType::MonoVertex(config) => {
            info!("Starting monovertex forwarder with config: {:#?}", config);
            let forwarder_config = (*config).clone();
            run_forwarder_with_recovery(
                root_token.clone(),
                move |attempt_token| {
                    monovertex::start_forwarder(
                        attempt_token,
                        forwarder_config.clone(),
                        metrics_state.clone(),
                    )
                },
                report_monovertex_forwarder_error,
                FORWARDER_RESTART_BACKOFF,
            )
            .await?;
        }
        CustomResourceType::Pipeline(config) => {
            info!("Starting pipeline forwarder with config: {:#?}", config);
            let forwarder_config = *config;
            let vertex_type = forwarder_config.vertex_type.as_str().to_string();
            run_forwarder_with_recovery(
                root_token.clone(),
                move |attempt_token| {
                    forwarder::start_forwarder(
                        attempt_token,
                        forwarder_config.clone(),
                        metrics_state.clone(),
                    )
                },
                move |error| report_pipeline_forwarder_error(&vertex_type, error),
                FORWARDER_RESTART_BACKOFF,
            )
            .await?;
        }
    }

    if !shutdown_handle.is_finished() {
        shutdown_handle.abort();
    }
    info!("Gracefully Exiting...");
    Ok(())
}

async fn run_forwarder_with_recovery<Forwarder, ForwarderFuture, Report>(
    root_token: CancellationToken,
    mut forwarder: Forwarder,
    mut report_error: Report,
    backoff: Duration,
) -> Result<()>
where
    Forwarder: FnMut(CancellationToken) -> ForwarderFuture,
    ForwarderFuture: Future<Output = Result<()>>,
    Report: FnMut(&Error),
{
    loop {
        if root_token.is_cancelled() {
            return Ok(());
        }

        let attempt_token = root_token.child_token();
        match forwarder(attempt_token.clone()).await {
            Ok(()) => return Ok(()),
            Err(error) => {
                attempt_token.cancel();

                if root_token.is_cancelled() {
                    return Ok(());
                }

                report_error(&error);
                error!(
                    ?error,
                    ?backoff,
                    "Forwarder attempt failed; rebuilding forwarder"
                );

                tokio::select! {
                    _ = root_token.cancelled() => return Ok(()),
                    _ = time::sleep(backoff) => {}
                }
            }
        }
    }
}

fn report_monovertex_forwarder_error(error: &Error) {
    critical_error!("", "mvtx_runtime_error");

    if let Error::Grpc(status) = error {
        error!(error=?status, "Monovertex failed because of UDF failure");
        runtime::persist_application_error(status.as_ref().clone());
    } else {
        error!(?error, "Error running monovertex");
        runtime::persist_application_error(Status::with_details(
            Code::Internal,
            "Error occurred while running MonoVertex".to_string(),
            Bytes::from(error.to_string()),
        ));
    }
}

fn report_pipeline_forwarder_error(vertex_type: &str, error: &Error) {
    critical_error!(vertex_type, "pipeline_runtime_error");

    if let Error::Grpc(status) = error {
        error!(error=?status, "Pipeline failed because of UDF failure");
        runtime::persist_application_error(status.as_ref().clone());
    } else {
        error!(?error, "Error running pipeline");
        runtime::persist_application_error(Status::with_details(
            Code::Internal,
            "Error occurred while running pipeline".to_string(),
            Bytes::from(error.to_string()),
        ));
    }
}

async fn shutdown_signal() {
    let ctrl_c = async {
        signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
        info!("Received Ctrl+C signal");
    };

    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("failed to install signal handler")
            .recv()
            .await;
        info!("Received terminate signal");
    };

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};

    use tokio::sync::oneshot;

    use super::*;

    #[tokio::test]
    async fn retries_with_a_fresh_attempt_token() {
        let root_token = CancellationToken::new();
        let attempts = Arc::new(AtomicUsize::new(0));
        let tokens = Arc::new(Mutex::new(Vec::new()));

        let result = run_forwarder_with_recovery(
            root_token,
            {
                let attempts = Arc::clone(&attempts);
                let tokens = Arc::clone(&tokens);
                move |attempt_token| {
                    tokens.lock().unwrap().push(attempt_token);
                    let attempt = attempts.fetch_add(1, Ordering::SeqCst);
                    async move {
                        if attempt == 0 {
                            Err(Error::Forwarder("first attempt failed".to_string()))
                        } else {
                            Ok(())
                        }
                    }
                }
            },
            |_| {},
            Duration::ZERO,
        )
        .await;

        assert!(result.is_ok());
        assert_eq!(attempts.load(Ordering::SeqCst), 2);
        let tokens = tokens.lock().unwrap();
        let [first_attempt, second_attempt] = tokens.as_slice() else {
            panic!("expected exactly two forwarder attempts");
        };
        assert!(first_attempt.is_cancelled());
        assert!(!second_attempt.is_cancelled());
    }

    #[tokio::test]
    async fn shutdown_cancels_backoff_without_starting_another_attempt() {
        let root_token = CancellationToken::new();
        let attempts = Arc::new(AtomicUsize::new(0));
        let (reported_tx, reported_rx) = oneshot::channel();

        let task = tokio::spawn(run_forwarder_with_recovery(
            root_token.clone(),
            {
                let attempts = Arc::clone(&attempts);
                move |_| {
                    attempts.fetch_add(1, Ordering::SeqCst);
                    async { Err(Error::Forwarder("attempt failed".to_string())) }
                }
            },
            {
                let mut reported_tx = Some(reported_tx);
                move |_| {
                    reported_tx.take().unwrap().send(()).unwrap();
                }
            },
            Duration::from_secs(60),
        ));

        reported_rx.await.unwrap();
        root_token.cancel();

        assert!(
            tokio::time::timeout(Duration::from_secs(1), task)
                .await
                .unwrap()
                .unwrap()
                .is_ok()
        );
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn shutdown_before_the_first_attempt_returns_without_starting_a_forwarder() {
        let root_token = CancellationToken::new();
        root_token.cancel();
        let attempts = Arc::new(AtomicUsize::new(0));

        let result = run_forwarder_with_recovery(
            root_token,
            {
                let attempts = Arc::clone(&attempts);
                move |_| {
                    attempts.fetch_add(1, Ordering::SeqCst);
                    async { Ok(()) }
                }
            },
            |_| {},
            Duration::ZERO,
        )
        .await;

        assert!(result.is_ok());
        assert_eq!(attempts.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn shutdown_does_not_report_an_attempt_error() {
        let root_token = CancellationToken::new();
        let (started_tx, started_rx) = oneshot::channel();
        let reports = Arc::new(AtomicUsize::new(0));

        let task = tokio::spawn(run_forwarder_with_recovery(
            root_token.clone(),
            {
                let mut started_tx = Some(started_tx);
                move |attempt_token| {
                    let started_tx = started_tx.take();
                    async move {
                        started_tx.unwrap().send(()).unwrap();
                        attempt_token.cancelled().await;
                        Err(Error::Cancelled())
                    }
                }
            },
            {
                let reports = Arc::clone(&reports);
                move |_| {
                    reports.fetch_add(1, Ordering::SeqCst);
                }
            },
            Duration::ZERO,
        ));

        started_rx.await.unwrap();
        root_token.cancel();

        assert!(
            tokio::time::timeout(Duration::from_secs(1), task)
                .await
                .unwrap()
                .unwrap()
                .is_ok()
        );
        assert_eq!(reports.load(Ordering::SeqCst), 0);
    }
}
