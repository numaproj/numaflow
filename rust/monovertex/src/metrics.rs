use std::net::SocketAddr;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use axum::body::Body;
use axum::extract::State;
use axum::http::{Response, StatusCode};
use axum::response::IntoResponse;
use axum::{routing::get, Router};
use axum_server::tls_rustls::RustlsConfig;
use rcgen::{generate_simple_self_signed, CertifiedKey};
use tokio::net::{TcpListener, ToSocketAddrs};
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tokio::time;
use tracing::{debug, error, info};

use crate::config::config;
use crate::error::Error;
use crate::sink::SinkClient;
use crate::source::SourceClient;
use crate::transformer::TransformerClient;
use prometheus_client::encoding::text::encode;
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::family::Family;
use prometheus_client::metrics::gauge::Gauge;
use prometheus_client::metrics::histogram::{exponential_buckets, Histogram};
use prometheus_client::registry::Registry;

// Define the labels for the metrics
// Note: Please keep consistent with the definitions in MonoVertex daemon
pub const MONO_VERTEX_NAME_LABEL: &str = "mvtx_name";
pub const REPLICA_LABEL: &str = "mvtx_replica";
const PENDING_PERIOD_LABEL: &str = "period";

// Define the metrics
// Note: We do not add a suffix to the metric name, as the suffix is inferred through the metric type
// by the prometheus client library
// refer: https://github.com/prometheus/client_rust/blob/master/src/registry.rs#L102

// Note: Please keep consistent with the definitions in MonoVertex daemon
const MONOVTX_READ_TOTAL: &str = "monovtx_read";
const MONOVTX_READ_BYTES_TOTAL: &str = "monovtx_read_bytes";
const MONOVTX_ACK_TOTAL: &str = "monovtx_ack";
const MONOVTX_SINK_WRITE_TOTAL: &str = "monovtx_sink_write";
const MONOVTX_PROCESSING_TIME: &str = "monovtx_processing_time";
const MONOVTX_PENDING: &str = "monovtx_pending";

#[derive(Clone)]
pub(crate) struct MetricsState {
    pub source_client: SourceClient,
    pub sink_client: SinkClient,
    pub transformer_client: Option<TransformerClient>,
    pub fb_sink_client: Option<SinkClient>,
}

/// The global register of all metrics.
#[derive(Default)]
pub struct GlobalRegistry {
    // It is okay to use std mutex because we register each metric only one time.
    pub registry: parking_lot::Mutex<Registry>,
}

impl GlobalRegistry {
    fn new() -> Self {
        GlobalRegistry {
            // Create a new registry for the metrics
            registry: parking_lot::Mutex::new(Registry::default()),
        }
    }
}

/// GLOBAL_REGISTER is the static global registry which is initialized
// only once.
static GLOBAL_REGISTER: OnceLock<GlobalRegistry> = OnceLock::new();

/// global_registry is a helper function to get the GLOBAL_REGISTER
fn global_registry() -> &'static GlobalRegistry {
    GLOBAL_REGISTER.get_or_init(GlobalRegistry::new)
}

// TODO: let's do sub-registry for forwarder so tomorrow we can add sink and source metrics.
/// MonoVtxMetrics is a struct which is used for storing the metrics related to MonoVertex
// These fields are exposed as pub to be used by other modules for
// changing the value of the metrics
// Each metric is defined as family of metrics, which means that they can be
// differentiated by their label values assigned.
// The labels are provided in the form of Vec<(String, String)
// The second argument is the metric kind.
pub struct MonoVtxMetrics {
    pub monovtx_read_total: Family<Vec<(String, String)>, Counter>,
    pub monovtx_read_bytes_total: Family<Vec<(String, String)>, Counter>,
    pub monovtx_ack_total: Family<Vec<(String, String)>, Counter>,
    pub monovtx_sink_write_total: Family<Vec<(String, String)>, Counter>,
    pub monovtx_processing_time: Family<Vec<(String, String)>, Histogram>,
    pub monovtx_pending: Family<Vec<(String, String)>, Gauge>,
}

/// impl the MonoVtxMetrics struct and create a new object
impl MonoVtxMetrics {
    fn new() -> Self {
        let monovtx_read_total = Family::<Vec<(String, String)>, Counter>::default();
        let monovtx_ack_total = Family::<Vec<(String, String)>, Counter>::default();
        let monovtx_read_bytes_total = Family::<Vec<(String, String)>, Counter>::default();
        let monovtx_sink_write_total = Family::<Vec<(String, String)>, Counter>::default();

        let monovtx_processing_time =
            Family::<Vec<(String, String)>, Histogram>::new_with_constructor(|| {
                Histogram::new(exponential_buckets(100.0, 60000000.0 * 15.0, 10))
            });
        let monovtx_pending = Family::<Vec<(String, String)>, Gauge>::default();

        let metrics = Self {
            monovtx_read_total,
            monovtx_read_bytes_total,
            monovtx_ack_total,
            monovtx_sink_write_total,
            monovtx_processing_time,
            monovtx_pending,
        };

        let mut registry = global_registry().registry.lock();
        // Register all the metrics to the global registry
        registry.register(
            MONOVTX_READ_TOTAL,
            "A Counter to keep track of the total number of messages read from the source",
            metrics.monovtx_read_total.clone(),
        );
        registry.register(
            MONOVTX_SINK_WRITE_TOTAL,
            "A Counter to keep track of the total number of messages written to the sink",
            metrics.monovtx_sink_write_total.clone(),
        );
        registry.register(
            MONOVTX_ACK_TOTAL,
            "A Counter to keep track of the total number of messages acknowledged by the sink",
            metrics.monovtx_ack_total.clone(),
        );
        registry.register(
            MONOVTX_PROCESSING_TIME,
            "A Histogram to keep track of the total time taken to forward a chunk, the time is in microseconds",
            metrics.monovtx_processing_time.clone(),
        );
        registry.register(
            MONOVTX_READ_BYTES_TOTAL,
            "A Counter to keep track of the total number of bytes read from the source",
            metrics.monovtx_read_bytes_total.clone(),
        );
        registry.register(
            MONOVTX_PENDING,
            "A Gauge to keep track of the total number of pending messages for the monovtx",
            metrics.monovtx_pending.clone(),
        );

        metrics
    }
}

/// MONOVTX_METRICS is the MonoVtxMetrics object which stores the metrics
static MONOVTX_METRICS: OnceLock<MonoVtxMetrics> = OnceLock::new();

// forward_metrics is a helper function used to fetch the
// MonoVtxMetrics object
pub(crate) fn forward_metrics() -> &'static MonoVtxMetrics {
    MONOVTX_METRICS.get_or_init(|| {
        let metrics = MonoVtxMetrics::new();
        metrics
    })
}

/// MONOVTX_METRICS_LABELS are used to store the common labels used in the metrics
static MONOVTX_METRICS_LABELS: OnceLock<Vec<(String, String)>> = OnceLock::new();

// forward_metrics_labels is a helper function used to fetch the
// MONOVTX_METRICS_LABELS object
pub(crate) fn forward_metrics_labels() -> &'static Vec<(String, String)> {
    crate::metrics::MONOVTX_METRICS_LABELS.get_or_init(|| {
        let common_labels = vec![
            (
                MONO_VERTEX_NAME_LABEL.to_string(),
                config().mono_vertex_name.clone(),
            ),
            (REPLICA_LABEL.to_string(), config().replica.to_string()),
        ];
        common_labels
    })
}

// metrics_handler is used to generate and return a snapshot of the
// current state of the metrics in the global registry
pub async fn metrics_handler() -> impl IntoResponse {
    let state = global_registry().registry.lock();
    let mut buffer = String::new();
    encode(&mut buffer, &state).unwrap();
    debug!("Exposing Metrics: {:?}", buffer);
    Response::builder()
        .status(StatusCode::OK)
        .body(Body::from(buffer))
        .unwrap()
}

/// Collect and emit prometheus metrics.
/// Metrics router and server over HTTP endpoint.
// This is not used currently
#[allow(dead_code)]
pub(crate) async fn start_metrics_http_server<A>(
    addr: A,
    metrics_state: MetricsState,
) -> crate::Result<()>
where
    A: ToSocketAddrs + std::fmt::Debug,
{
    let metrics_app = metrics_router(metrics_state);

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

    let metrics_app = metrics_router(metrics_state);

    axum_server::bind_rustls(addr, tls_config)
        .serve(metrics_app.into_make_service())
        .await
        .map_err(|e| Error::MetricsError(format!("Starting web server for metrics: {}", e)))?;

    Ok(())
}

/// router for metrics and k8s health endpoints
fn metrics_router(metrics_state: MetricsState) -> Router {
    Router::new()
        .route("/metrics", get(metrics_handler))
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
        error!("Source client is not available");
        return StatusCode::SERVICE_UNAVAILABLE;
    }
    if !state.sink_client.is_ready().await {
        error!("Sink client is not available");
        return StatusCode::SERVICE_UNAVAILABLE;
    }
    if let Some(mut transformer_client) = state.transformer_client {
        if !transformer_client.is_ready().await {
            error!("Transformer client is not available");
            return StatusCode::SERVICE_UNAVAILABLE;
        }
    }
    if let Some(mut fb_sink_client) = state.fb_sink_client {
        if !fb_sink_client.is_ready().await {
            error!("Fallback sink client is not available");
            return StatusCode::SERVICE_UNAVAILABLE;
        }
    }
    StatusCode::NO_CONTENT
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
            lag_checking_interval: self
                .lag_checking_interval
                .unwrap_or_else(|| Duration::from_secs(3)),
            refresh_interval: self
                .refresh_interval
                .unwrap_or_else(|| Duration::from_secs(5)),
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
                let mut metric_labels = forward_metrics_labels().clone();
                metric_labels.push((PENDING_PERIOD_LABEL.to_string(), label.to_string()));
                forward_metrics()
                    .monovtx_pending
                    .get_or_create(&metric_labels)
                    .set(pending);
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
