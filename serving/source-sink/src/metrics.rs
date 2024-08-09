use std::future::ready;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::{routing::get, Router};
use axum_server::tls_rustls::RustlsConfig;
use log::info;
use metrics::describe_counter;
use metrics_exporter_prometheus::{Matcher, PrometheusBuilder, PrometheusHandle};
use rcgen::{generate_simple_self_signed, CertifiedKey};
use tokio::net::{TcpListener, ToSocketAddrs};
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tokio::time;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error};

use crate::error::Error;
use crate::source::SourceClient;

// Define the labels for the metrics
pub const MONO_VERTEX_NAME: &str = "vertex";
pub const REPLICA_LABEL: &str = "replica";
pub const PARTITION_LABEL: &str = "partition_name";
pub const VERTEX_TYPE_LABEL: &str = "vertex_type";

// Define the metrics
pub const FORWARDER_READ_TOTAL: &str = "forwarder_read_total";
pub const FORWARDER_READ_BYTES_TOTAL: &str = "forwarder_read_bytes_total";

pub const FORWARDER_ACK_TOTAL: &str = "forwarder_ack_total";
pub const FORWARDER_WRITE_TOTAL: &str = "forwarder_write_total";

/// Collect and emit prometheus metrics.
/// Metrics router and server
pub async fn start_metrics_http_server<A>(addr: A) -> crate::Result<()>
where
    A: ToSocketAddrs + std::fmt::Debug,
{
    // setup_metrics_recorder should only be invoked once
    let recorder_handle = setup_metrics_recorder()?;

    let metrics_app = Router::new()
        .route("/metrics", get(move || ready(recorder_handle.render())))
        .route("/livez", get(livez))
        .route("/readyz", get(readyz))
        .route("/sidecar-livez", get(sidecar_livez));

    let listener = TcpListener::bind(&addr)
        .await
        .map_err(|e| Error::MetricsError(format!("Creating listener on {:?}: {}", addr, e)))?;

    debug!("metrics server started at addr: {:?}", addr);

    axum::serve(listener, metrics_app)
        .await
        .map_err(|e| Error::MetricsError(format!("Starting web server for metrics: {}", e)))?;
    Ok(())
}

pub async fn start_metrics_https_server(addr: SocketAddr) -> crate::Result<()>
where
{
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

    // Generate a self-signed certificate
    let CertifiedKey { cert, key_pair } = generate_simple_self_signed(vec!["localhost".into()])
        .map_err(|e| Error::MetricsError(format!("Generating self-signed certificate: {}", e)))?;

    let tls_config = RustlsConfig::from_pem(cert.pem().into(), key_pair.serialize_pem().into())
        .await
        .map_err(|e| Error::MetricsError(format!("Creating tlsConfig from pem: {}", e)))?;

    // setup_metrics_recorder should only be invoked once
    let recorder_handle = setup_metrics_recorder()?;

    let metrics_app = Router::new()
        .route("/metrics", get(move || ready(recorder_handle.render())))
        .route("/livez", get(livez))
        .route("/readyz", get(readyz))
        .route("/sidecar-livez", get(sidecar_livez));

    axum_server::bind_rustls(addr, tls_config)
        .serve(metrics_app.into_make_service())
        .await
        .map_err(|e| Error::MetricsError(format!("Starting web server for metrics: {}", e)))?;

    Ok(())
}

async fn livez() -> impl IntoResponse {
    StatusCode::NO_CONTENT
}

async fn readyz() -> impl IntoResponse {
    StatusCode::NO_CONTENT
}

async fn sidecar_livez() -> impl IntoResponse {
    StatusCode::NO_CONTENT
}

/// setup the Prometheus metrics recorder.
fn setup_metrics_recorder() -> crate::Result<PrometheusHandle> {
    // 1 micro-sec < t < 1000 seconds
    let log_to_power_of_sqrt2_bins: [f64; 62] = (0..62)
        .map(|i| 2_f64.sqrt().powf(i as f64))
        .collect::<Vec<_>>()
        .try_into()
        .unwrap();

    let prometheus_handle = PrometheusBuilder::new()
        .set_buckets_for_metric(
            Matcher::Full("fac_total_duration_micros".to_string()), // fac == forward-a-chunk
            &log_to_power_of_sqrt2_bins,
        )
        .map_err(|e| Error::MetricsError(format!("Prometheus install_recorder: {}", e)))?
        .install_recorder()
        .map_err(|e| Error::MetricsError(format!("Prometheus install_recorder: {}", e)))?;

    // Define forwarder metrics
    describe_counter!(
        FORWARDER_READ_TOTAL,
        "Total number of Data Messages Read in the forwarder"
    );
    describe_counter!(
        FORWARDER_READ_BYTES_TOTAL,
        "Total number of bytes read in the forwarder"
    );
    describe_counter!(
        FORWARDER_ACK_TOTAL,
        "Total number of acknowledgments by the forwarder"
    );
    describe_counter!(
        FORWARDER_WRITE_TOTAL,
        "Total number of Data Messages written by the forwarder"
    );
    Ok(prometheus_handle)
}

const MAX_PENDING_STATS: usize = 1800;

// Pending info with timestamp
struct TimestampedPending {
    pending: i64,
    timestamp: std::time::Instant,
}

/// `LagReader` is responsible for periodically checking the lag of the source client
/// and exposing the metrics. It maintains a list of pending stats and ensures that
/// only the most recent entries are kept.
pub(crate) struct LagReader {
    source_client: SourceClient,
    lag_checking_interval: Duration,
    refresh_interval: Duration,
    cancellation_token: CancellationToken,
    buildup_handle: Option<JoinHandle<()>>,
    expose_handle: Option<JoinHandle<()>>,
    pending_stats: Arc<Mutex<Vec<TimestampedPending>>>,
}

impl LagReader {
    /// Creates a new `LagReader` instance.
    pub(crate) fn new(
        source_client: SourceClient,
        lag_checking_interval: Option<Duration>,
        refresh_interval: Option<Duration>,
    ) -> Self {
        Self {
            source_client,
            lag_checking_interval: lag_checking_interval.unwrap_or_else(|| Duration::from_secs(3)),
            refresh_interval: refresh_interval.unwrap_or_else(|| Duration::from_secs(5)),
            cancellation_token: CancellationToken::new(),
            buildup_handle: None,
            expose_handle: None,
            pending_stats: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Starts the lag reader by spawning tasks to build up pending info and expose pending metrics.
    ///
    /// This method spawns two asynchronous tasks:
    /// - One to periodically check the lag and update the pending stats.
    /// - Another to periodically expose the pending metrics.
    pub async fn start(&mut self) {
        let token = self.cancellation_token.clone();
        let source_client = self.source_client.clone();
        let lag_checking_interval = self.lag_checking_interval;
        let refresh_interval = self.refresh_interval;
        let pending_stats = self.pending_stats.clone();

        self.buildup_handle = Some(tokio::spawn(async move {
            buildup_pending_info(source_client, token, lag_checking_interval, pending_stats).await;
        }));

        let token = self.cancellation_token.clone();
        let pending_stats = self.pending_stats.clone();
        self.expose_handle = Some(tokio::spawn(async move {
            expose_pending_metrics(token, refresh_interval, pending_stats).await;
        }));
    }

    /// Shuts down the lag reader by cancelling the tasks and waiting for them to complete.
    pub(crate) async fn shutdown(self) {
        self.cancellation_token.cancel();
        if let Some(handle) = self.buildup_handle {
            let _ = handle.await;
        }
        if let Some(handle) = self.expose_handle {
            let _ = handle.await;
        }
    }
}

// Periodically checks the pending messages from the source client and updates the pending stats.
async fn buildup_pending_info(
    mut source_client: SourceClient,
    cancellation_token: CancellationToken,
    lag_checking_interval: Duration,
    pending_stats: Arc<Mutex<Vec<TimestampedPending>>>,
) {
    let mut ticker = time::interval(lag_checking_interval);
    loop {
        tokio::select! {
            _ = cancellation_token.cancelled() => {
                return;
            }
            _ = ticker.tick() => {
                match source_client.pending_fn().await {
                    Ok(pending) => {
                        if pending != -1 {
                            let mut stats = pending_stats.lock().await;
                            stats.push(TimestampedPending {
                                pending,
                                timestamp: std::time::Instant::now(),
                            });
                            let n = stats.len();
                            // Ensure only the most recent MAX_PENDING_STATS entries are kept
                            if n > MAX_PENDING_STATS {
                                stats.drain(0..(n - MAX_PENDING_STATS));
                            }
                        }
                    }
                    Err(err) => {
                        error!("Failed to get pending messages: {:?}", err);
                    }
                }
            }
        }
    }
}

// Periodically exposes the pending metrics by calculating the average pending messages over different intervals.
async fn expose_pending_metrics(
    cancellation_token: CancellationToken,
    refresh_interval: Duration,
    pending_stats: Arc<Mutex<Vec<TimestampedPending>>>,
) {
    let mut ticker = time::interval(refresh_interval);
    let lookback_seconds_map = vec![("1m", 60), ("5m", 300), ("15m", 900)];
    loop {
        tokio::select! {
            _ = cancellation_token.cancelled() => {
                return;
            }
            _ = ticker.tick() => {
                for (label, seconds) in &lookback_seconds_map {
                    let pending = calculate_pending(*seconds, &pending_stats).await;
                    if pending != -1 {
                        // TODO: emit it as a metric
                        info!("Pending messages ({}): {}", label, pending);
                    }
                }
            }
        }
    }
}

// Calculate the average pending messages over the last `seconds` seconds.
async fn calculate_pending(
    seconds: i64,
    pending_stats: &Arc<Mutex<Vec<TimestampedPending>>>,
) -> i64 {
    let mut result = -1;
    let mut total = 0;
    let mut num = 0;
    let now = std::time::Instant::now();

    let stats = pending_stats.lock().await;
    for item in stats.iter().rev() {
        if now.duration_since(item.timestamp).as_secs() < seconds as u64 {
            total += item.pending;
            num += 1;
        } else {
            break;
        }
    }

    if num > 0 {
        result = total / num;
    }

    result
}
#[cfg(test)]
mod tests {
    use std::net::SocketAddr;
    use std::time::Duration;

    use tokio::time::sleep;

    use super::*;

    #[tokio::test]
    async fn test_start_metrics_server() {
        let addr = SocketAddr::from(([127, 0, 0, 1], 0));
        let server = tokio::spawn(async move {
            let result = start_metrics_http_server(addr).await;
            assert!(result.is_ok())
        });

        // Give the server a little bit of time to start
        sleep(Duration::from_millis(100)).await;

        // Stop the server
        server.abort();
    }
}
