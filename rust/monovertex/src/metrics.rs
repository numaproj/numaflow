use std::future::ready;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::{routing::get, Router};
use axum_server::tls_rustls::RustlsConfig;
use metrics::describe_counter;
use metrics_exporter_prometheus::{Matcher, PrometheusBuilder, PrometheusHandle};
use rcgen::{generate_simple_self_signed, CertifiedKey};
use tokio::net::{TcpListener, ToSocketAddrs};
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tokio::time;
use tracing::{debug, error, info};

use crate::error::Error;
use crate::sink::SinkClient;
use crate::source::SourceClient;
use crate::transformer::TransformerClient;

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

#[derive(Clone)]
pub(crate) struct MetricsState {
    pub source_client: SourceClient,
    pub sink_client: SinkClient,
    pub transformer_client: Option<TransformerClient>,
}

/// Collect and emit prometheus metrics.
/// Metrics router and server over HTTP endpoint.
// This is not used currently
#[allow(dead_code)]
pub(crate) async fn start_metrics_http_server<A>(
    addr: A,
    source_client: SourceClient,
    sink_client: SinkClient,
    transformer_client: Option<TransformerClient>,
) -> crate::Result<()>
where
    A: ToSocketAddrs + std::fmt::Debug,
{
    // setup_metrics_recorder should only be invoked once
    let recorder_handle = setup_metrics_recorder()?;

    let metrics_app = metrics_router(
        recorder_handle,
        MetricsState {
            source_client,
            sink_client,
            transformer_client,
        },
    );

    let listener = TcpListener::bind(&addr)
        .await
        .map_err(|e| Error::MetricsError(format!("Creating listener on {:?}: {}", addr, e)))?;

    debug!("metrics server started at addr: {:?}", addr);

    axum::serve(listener, metrics_app)
        .await
        .map_err(|e| Error::MetricsError(format!("Starting web server for metrics: {}", e)))?;
    Ok(())
}

pub(crate) async fn start_metrics_https_server(
    addr: SocketAddr,
    metrics_state: MetricsState,
) -> crate::Result<()> {
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

    // Generate a self-signed certificate
    let CertifiedKey { cert, key_pair } = generate_simple_self_signed(vec!["localhost".into()])
        .map_err(|e| Error::MetricsError(format!("Generating self-signed certificate: {}", e)))?;

    let tls_config = RustlsConfig::from_pem(cert.pem().into(), key_pair.serialize_pem().into())
        .await
        .map_err(|e| Error::MetricsError(format!("Creating tlsConfig from pem: {}", e)))?;

    // setup_metrics_recorder should only be invoked once
    let recorder_handle = setup_metrics_recorder()?;

    let metrics_app = metrics_router(recorder_handle, metrics_state);

    axum_server::bind_rustls(addr, tls_config)
        .serve(metrics_app.into_make_service())
        .await
        .map_err(|e| Error::MetricsError(format!("Starting web server for metrics: {}", e)))?;

    Ok(())
}

/// router for metrics and k8s health endpoints
fn metrics_router(recorder_handle: PrometheusHandle, metrics_state: MetricsState) -> Router {
     Router::new()
        .route("/metrics", get(move || ready(recorder_handle.render())))
        .route("/livez", get(livez))
        .route("/readyz", get(readyz))
        .route("/sidecar-livez", get(sidecar_livez))
        .with_state(metrics_state)
}

async fn livez() -> impl IntoResponse {
    StatusCode::NO_CONTENT
}

async fn readyz() -> impl IntoResponse {
    StatusCode::NO_CONTENT
}

async fn sidecar_livez(State(mut state): State<MetricsState>) -> impl IntoResponse {
    if !state.source_client.is_ready().await {
        return StatusCode::SERVICE_UNAVAILABLE;
    }
    if !state.sink_client.is_ready().await {
        return StatusCode::SERVICE_UNAVAILABLE;
    }
    if let Some(mut transformer_client) = state.transformer_client {
        if !transformer_client.is_ready().await {
            return StatusCode::SERVICE_UNAVAILABLE;
        }
    }
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
    buildup_handle: Option<JoinHandle<()>>,
    expose_handle: Option<JoinHandle<()>>,
    pending_stats: Arc<Mutex<Vec<TimestampedPending>>>,
}


/// LagReaderBuilder is used to build a `LagReader` instance.
pub(crate) struct LagReaderBuilder {
    source_client: SourceClient,
    lag_checking_interval: Option<Duration>,
    refresh_interval: Option<Duration>,
}

impl LagReaderBuilder {
    pub(crate) fn new(source_client: SourceClient) -> Self {
        Self {
            source_client,
            lag_checking_interval: None,
            refresh_interval: None,
        }
    }

    pub(crate) fn lag_checking_interval(mut self, interval: Duration) -> Self {
        self.lag_checking_interval = Some(interval);
        self
    }

    pub(crate) fn refresh_interval(mut self, interval: Duration) -> Self {
        self.refresh_interval = Some(interval);
        self
    }

    pub(crate) fn build(self) -> LagReader {
        LagReader {
            source_client: self.source_client,
            lag_checking_interval: self.lag_checking_interval.unwrap_or_else(|| Duration::from_secs(3)),
            refresh_interval: self.refresh_interval.unwrap_or_else(|| Duration::from_secs(5)),
            buildup_handle: None,
            expose_handle: None,
            pending_stats: Arc::new(Mutex::new(Vec::with_capacity(MAX_PENDING_STATS))),
        }
    }
}

impl LagReader {
    /// Starts the lag reader by spawning tasks to build up pending info and expose pending metrics.
    ///
    /// This method spawns two asynchronous tasks:
    /// - One to periodically check the lag and update the pending stats.
    /// - Another to periodically expose the pending metrics.
    pub async fn start(&mut self) {
        let source_client = self.source_client.clone();
        let lag_checking_interval = self.lag_checking_interval;
        let refresh_interval = self.refresh_interval;
        let pending_stats = self.pending_stats.clone();

        self.buildup_handle = Some(tokio::spawn(async move {
            build_pending_info(source_client, lag_checking_interval, pending_stats).await;
        }));

        let pending_stats = self.pending_stats.clone();
        self.expose_handle = Some(tokio::spawn(async move {
            expose_pending_metrics(refresh_interval, pending_stats).await;
        }));
    }
}

/// When lag-reader is dropped, we need to clean up the pending exposer and the pending builder tasks.
impl Drop for LagReader {
    fn drop(&mut self) {
        if let Some(handle) = self.expose_handle.take() {
            handle.abort();
        }
        if let Some(handle) = self.buildup_handle.take() {
            handle.abort();
        }

        info!("Stopped the Lag-Reader Expose and Builder tasks");
    }
}

/// Periodically checks the pending messages from the source client and build the pending stats.
async fn build_pending_info(
    mut source_client: SourceClient,
    lag_checking_interval: Duration,
    pending_stats: Arc<Mutex<Vec<TimestampedPending>>>,
) {
    let mut ticker = time::interval(lag_checking_interval);
    loop {
        ticker.tick().await;
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
                    if n >= MAX_PENDING_STATS {
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

// Periodically exposes the pending metrics by calculating the average pending messages over different intervals.
async fn expose_pending_metrics(
    refresh_interval: Duration,
    pending_stats: Arc<Mutex<Vec<TimestampedPending>>>,
) {
    let mut ticker = time::interval(refresh_interval);
    let lookback_seconds_map = vec![("1m", 60), ("default", 120), ("5m", 300), ("15m", 900)];
    loop {
        ticker.tick().await;
        for (label, seconds) in &lookback_seconds_map {
            let pending = calculate_pending(*seconds, &pending_stats).await;
            if pending != -1 {
                // TODO: emit it as a metric
                info!("Pending messages ({}): {}", label, pending);
            }
        }
    }
}

/// Calculate the average pending messages over the last `seconds` seconds.
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

// TODO add tests
