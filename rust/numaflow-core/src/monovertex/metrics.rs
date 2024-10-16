use std::collections::BTreeMap;
use std::iter;
use std::net::SocketAddr;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use axum::body::Body;
use axum::extract::State;
use axum::http::{Response, StatusCode};
use axum::response::IntoResponse;
use axum::{routing::get, Router};
use axum_server::tls_rustls::RustlsConfig;
use prometheus_client::encoding::text::encode;
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::family::Family;
use prometheus_client::metrics::gauge::Gauge;
use prometheus_client::metrics::histogram::Histogram;
use prometheus_client::registry::Registry;
use rcgen::{generate_simple_self_signed, CertifiedKey};
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tokio::time;
use tonic::transport::Channel;
use tonic::Request;
use tracing::{debug, error, info};

use numaflow_grpc::clients::sink::sink_client::SinkClient;
use numaflow_grpc::clients::source::source_client::SourceClient;
use numaflow_grpc::clients::sourcetransformer::source_transform_client::SourceTransformClient;

use crate::config::config;
use crate::source::SourceHandle;
use crate::Error;

// Define the labels for the metrics
// Note: Please keep consistent with the definitions in MonoVertex daemon
const MVTX_NAME_LABEL: &str = "mvtx_name";
const REPLICA_LABEL: &str = "mvtx_replica";
const PENDING_PERIOD_LABEL: &str = "period";

// Define the metrics
// Note: We do not add a suffix to the metric name, as the suffix is inferred through the metric type
// by the prometheus client library
// refer: https://github.com/prometheus/client_rust/blob/master/src/registry.rs#L102
// Note: Please keep consistent with the definitions in MonoVertex daemon

// counters (please note the prefix _total, and read above link)
const READ_TOTAL: &str = "monovtx_read";
const READ_BYTES_TOTAL: &str = "monovtx_read_bytes";
const ACK_TOTAL: &str = "monovtx_ack";
const SINK_WRITE_TOTAL: &str = "monovtx_sink_write";
const DROPPED_TOTAL: &str = "monovtx_dropped";
const FALLBACK_SINK_WRITE_TOTAL: &str = "monovtx_fallback_sink_write";

// pending as gauge
const SOURCE_PENDING: &str = "monovtx_pending";

// processing times as timers
const E2E_TIME: &str = "monovtx_processing_time";
const READ_TIME: &str = "monovtx_read_time";
const TRANSFORM_TIME: &str = "monovtx_transformer_time";
const ACK_TIME: &str = "monovtx_ack_time";
const SINK_TIME: &str = "monovtx_sink_time";

/// Only used defined functions will have containers since rest
/// are builtins. We save the gRPC clients to retrieve metrics and also
/// to do liveness checks. This means, these will be optionals since
/// we do not require these for builtins.
#[derive(Clone)]
pub(crate) struct UserDefinedContainerState {
    pub source_client: Option<SourceClient<Channel>>,
    pub sink_client: Option<SinkClient<Channel>>,
    pub transformer_client: Option<SourceTransformClient<Channel>>,
    pub fb_sink_client: Option<SinkClient<Channel>>,
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
    // counters
    pub read_total: Family<Vec<(String, String)>, Counter>,
    pub read_bytes_total: Family<Vec<(String, String)>, Counter>,
    pub ack_total: Family<Vec<(String, String)>, Counter>,
    pub sink_write_total: Family<Vec<(String, String)>, Counter>,
    pub dropped_total: Family<Vec<(String, String)>, Counter>,
    pub fbsink_write_total: Family<Vec<(String, String)>, Counter>,

    // gauge
    pub source_pending: Family<Vec<(String, String)>, Gauge>,

    // timers
    pub e2e_time: Family<Vec<(String, String)>, Histogram>,
    pub read_time: Family<Vec<(String, String)>, Histogram>,
    pub transform_time: Family<Vec<(String, String)>, Histogram>,
    pub ack_time: Family<Vec<(String, String)>, Histogram>,
    pub sink_time: Family<Vec<(String, String)>, Histogram>,
}

/// Exponential bucket distribution with range.
/// Creates `length` buckets, where the lowest bucket is `min` and the highest bucket is `max`.
/// The final +Inf bucket is not counted and not included in the returned iterator.
/// The function panics if `length` is 0 or negative, or if `min` is 0 or negative.
fn exponential_buckets_range(min: f64, max: f64, length: u16) -> impl Iterator<Item = f64> {
    if length < 1 {
        panic!("ExponentialBucketsRange length needs a positive length");
    }
    if min <= 0.0 {
        panic!("ExponentialBucketsRange min needs to be greater than 0");
    }

    // We know max/min and highest bucket. Solve for growth_factor.
    let growth_factor = (max / min).powf(1.0 / (length as f64 - 1.0));

    iter::repeat(())
        .enumerate()
        .map(move |(i, _)| min * growth_factor.powf(i as f64))
        .take(length.into())
}

/// impl the MonoVtxMetrics struct and create a new object
impl MonoVtxMetrics {
    fn new() -> Self {
        let metrics = Self {
            read_total: Family::<Vec<(String, String)>, Counter>::default(),
            read_bytes_total: Family::<Vec<(String, String)>, Counter>::default(),
            ack_total: Family::<Vec<(String, String)>, Counter>::default(),
            sink_write_total: Family::<Vec<(String, String)>, Counter>::default(),
            dropped_total: Family::<Vec<(String, String)>, Counter>::default(),
            fbsink_write_total: Family::<Vec<(String, String)>, Counter>::default(),
            // gauge
            source_pending: Family::<Vec<(String, String)>, Gauge>::default(),
            // timers
            // exponential buckets in the range 100 microseconds to 15 minutes
            e2e_time: Family::<Vec<(String, String)>, Histogram>::new_with_constructor(|| {
                Histogram::new(exponential_buckets_range(100.0, 60000000.0 * 15.0, 10))
            }),
            read_time: Family::<Vec<(String, String)>, Histogram>::new_with_constructor(|| {
                Histogram::new(exponential_buckets_range(100.0, 60000000.0 * 15.0, 10))
            }),
            transform_time: Family::<Vec<(String, String)>, Histogram>::new_with_constructor(
                || Histogram::new(exponential_buckets_range(100.0, 60000000.0 * 15.0, 10)),
            ),
            ack_time: Family::<Vec<(String, String)>, Histogram>::new_with_constructor(|| {
                Histogram::new(exponential_buckets_range(100.0, 60000000.0 * 15.0, 10))
            }),
            sink_time: Family::<Vec<(String, String)>, Histogram>::new_with_constructor(|| {
                Histogram::new(exponential_buckets_range(100.0, 60000000.0 * 15.0, 10))
            }),
        };

        let mut registry = global_registry().registry.lock();
        // Register all the metrics to the global registry
        registry.register(
            READ_TOTAL,
            "A Counter to keep track of the total number of messages read from the source",
            metrics.read_total.clone(),
        );
        registry.register(
            SINK_WRITE_TOTAL,
            "A Counter to keep track of the total number of messages written to the sink",
            metrics.sink_write_total.clone(),
        );
        registry.register(
            ACK_TOTAL,
            "A Counter to keep track of the total number of messages acknowledged by the sink",
            metrics.ack_total.clone(),
        );
        registry.register(
            READ_BYTES_TOTAL,
            "A Counter to keep track of the total number of bytes read from the source",
            metrics.read_bytes_total.clone(),
        );

        registry.register(
            DROPPED_TOTAL,
            "A Counter to keep track of the total number of messages dropped by the monovtx",
            metrics.dropped_total.clone(),
        );

        registry.register(
            FALLBACK_SINK_WRITE_TOTAL,
            "A Counter to keep track of the total number of messages written to the fallback sink",
            metrics.fbsink_write_total.clone(),
        );

        // gauges
        registry.register(
            SOURCE_PENDING,
            "A Gauge to keep track of the total number of pending messages for the monovtx",
            metrics.source_pending.clone(),
        );
        // timers
        registry.register(
            E2E_TIME,
            "A Histogram to keep track of the total time taken to forward a chunk, in microseconds",
            metrics.e2e_time.clone(),
        );
        registry.register(
            READ_TIME,
            "A Histogram to keep track of the total time taken to Read from the Source, in microseconds",
            metrics.read_time.clone(),
        );
        registry.register(
            TRANSFORM_TIME,
            "A Histogram to keep track of the total time taken to Transform, in microseconds",
            metrics.transform_time.clone(),
        );
        registry.register(
            ACK_TIME,
            "A Histogram to keep track of the total time taken to Ack to the Source, in microseconds",
            metrics.ack_time.clone(),
        );
        registry.register(
            SINK_TIME,
            "A Histogram to keep track of the total time taken to Write to the Sink, in microseconds",
            metrics.sink_time.clone(),
        );
        metrics
    }
}

/// MONOVTX_METRICS is the MonoVtxMetrics object which stores the metrics
static MONOVTX_METRICS: OnceLock<MonoVtxMetrics> = OnceLock::new();

// forward_metrics is a helper function used to fetch the
// MonoVtxMetrics object
pub(crate) fn forward_metrics() -> &'static MonoVtxMetrics {
    MONOVTX_METRICS.get_or_init(MonoVtxMetrics::new)
}

/// MONOVTX_METRICS_LABELS are used to store the common labels used in the metrics
static MONOVTX_METRICS_LABELS: OnceLock<Vec<(String, String)>> = OnceLock::new();

// forward_metrics_labels is a helper function used to fetch the
// MONOVTX_METRICS_LABELS object
pub(crate) fn forward_metrics_labels() -> &'static Vec<(String, String)> {
    MONOVTX_METRICS_LABELS.get_or_init(|| {
        let common_labels = vec![
            (
                MVTX_NAME_LABEL.to_string(),
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

pub(crate) async fn start_metrics_https_server(
    addr: SocketAddr,
    metrics_state: UserDefinedContainerState,
) -> crate::Result<()> {
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

    // Generate a self-signed certificate
    let CertifiedKey { cert, key_pair } = generate_simple_self_signed(vec!["localhost".into()])
        .map_err(|e| Error::Metrics(format!("Generating self-signed certificate: {}", e)))?;

    let tls_config = RustlsConfig::from_pem(cert.pem().into(), key_pair.serialize_pem().into())
        .await
        .map_err(|e| Error::Metrics(format!("Creating tlsConfig from pem: {}", e)))?;

    let metrics_app = metrics_router(metrics_state);

    axum_server::bind_rustls(addr, tls_config)
        .serve(metrics_app.into_make_service())
        .await
        .map_err(|e| Error::Metrics(format!("Starting web server for metrics: {}", e)))?;

    Ok(())
}

/// router for metrics and k8s health endpoints
fn metrics_router(metrics_state: UserDefinedContainerState) -> Router {
    Router::new()
        .route("/metrics", get(metrics_handler))
        .route("/livez", get(livez))
        .route("/readyz", get(sidecar_livez))
        .route("/sidecar-livez", get(sidecar_livez))
        .with_state(metrics_state)
}

async fn livez() -> impl IntoResponse {
    StatusCode::NO_CONTENT
}

async fn sidecar_livez(State(state): State<UserDefinedContainerState>) -> impl IntoResponse {
    if let Some(mut source_client) = state.source_client {
        if source_client.is_ready(Request::new(())).await.is_err() {
            error!("Source client is not available");
            return StatusCode::SERVICE_UNAVAILABLE;
        }
    }
    if let Some(mut sink_client) = state.sink_client {
        if sink_client.is_ready(Request::new(())).await.is_err() {
            error!("Sink client is not available");
            return StatusCode::SERVICE_UNAVAILABLE;
        }
    }
    if let Some(mut transformer_client) = state.transformer_client {
        if transformer_client.is_ready(Request::new(())).await.is_err() {
            error!("Transformer client is not available");
            return StatusCode::SERVICE_UNAVAILABLE;
        }
    }
    if let Some(mut fb_sink_client) = state.fb_sink_client {
        if fb_sink_client.is_ready(Request::new(())).await.is_err() {
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

/// PendingReader is responsible for periodically checking the lag of the reader
/// and exposing the metrics. It maintains a list of pending stats and ensures that
/// only the most recent entries are kept.
pub(crate) struct PendingReader {
    lag_reader: SourceHandle,
    lag_checking_interval: Duration,
    refresh_interval: Duration,
    pending_stats: Arc<Mutex<Vec<TimestampedPending>>>,
}

pub(crate) struct PendingReaderTasks {
    buildup_handle: JoinHandle<()>,
    expose_handle: JoinHandle<()>,
}

/// PendingReaderBuilder is used to build a [LagReader] instance.
pub(crate) struct PendingReaderBuilder {
    lag_reader: SourceHandle,
    lag_checking_interval: Option<Duration>,
    refresh_interval: Option<Duration>,
}

impl PendingReaderBuilder {
    pub(crate) fn new(lag_reader: SourceHandle) -> Self {
        Self {
            lag_reader,
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

    pub(crate) fn build(self) -> PendingReader {
        PendingReader {
            lag_reader: self.lag_reader,
            lag_checking_interval: self
                .lag_checking_interval
                .unwrap_or_else(|| Duration::from_secs(3)),
            refresh_interval: self
                .refresh_interval
                .unwrap_or_else(|| Duration::from_secs(5)),
            pending_stats: Arc::new(Mutex::new(Vec::with_capacity(MAX_PENDING_STATS))),
        }
    }
}

impl PendingReader {
    /// Starts the lag reader by spawning tasks to build up pending info and expose pending metrics.
    ///
    /// This method spawns two asynchronous tasks:
    /// - One to periodically check the lag and update the pending stats.
    /// - Another to periodically expose the pending metrics.
    ///
    /// Dropping the PendingReaderTasks will abort the background tasks.
    pub async fn start(&self) -> PendingReaderTasks {
        let pending_reader = self.lag_reader.clone();
        let lag_checking_interval = self.lag_checking_interval;
        let refresh_interval = self.refresh_interval;
        let pending_stats = self.pending_stats.clone();

        let buildup_handle = tokio::spawn(async move {
            build_pending_info(pending_reader, lag_checking_interval, pending_stats).await;
        });

        let pending_stats = self.pending_stats.clone();
        let expose_handle = tokio::spawn(async move {
            expose_pending_metrics(refresh_interval, pending_stats).await;
        });
        PendingReaderTasks {
            buildup_handle,
            expose_handle,
        }
    }
}

/// When the PendingReaderTasks is dropped, we need to clean up the pending exposer and the pending builder tasks.
impl Drop for PendingReaderTasks {
    fn drop(&mut self) {
        self.expose_handle.abort();
        self.buildup_handle.abort();
        info!("Stopped the Lag-Reader Expose and Builder tasks");
    }
}

/// Periodically checks the pending messages from the source client and build the pending stats.
async fn build_pending_info(
    source: SourceHandle,
    lag_checking_interval: Duration,
    pending_stats: Arc<Mutex<Vec<TimestampedPending>>>,
) {
    let mut ticker = time::interval(lag_checking_interval);
    loop {
        ticker.tick().await;
        match fetch_pending(&source).await {
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

async fn fetch_pending(lag_reader: &SourceHandle) -> crate::error::Result<i64> {
    let response: i64 = lag_reader.pending().await?.map_or(-1, |p| p as i64); // default to -1(unavailable)
    Ok(response)
}

const LOOKBACK_SECONDS_MAP: [(&str, i64); 4] =
    [("1m", 60), ("default", 120), ("5m", 300), ("15m", 900)];

// Periodically exposes the pending metrics by calculating the average pending messages over different intervals.
async fn expose_pending_metrics(
    refresh_interval: Duration,
    pending_stats: Arc<Mutex<Vec<TimestampedPending>>>,
) {
    let mut ticker = time::interval(refresh_interval);

    // store the pending info in a sorted way for deterministic display
    // string concat is more efficient?
    let mut pending_info: BTreeMap<&str, i64> = BTreeMap::new();

    loop {
        ticker.tick().await;
        for (label, seconds) in LOOKBACK_SECONDS_MAP {
            let pending = calculate_pending(seconds, &pending_stats).await;
            if pending != -1 {
                let mut metric_labels = forward_metrics_labels().clone();
                metric_labels.push((PENDING_PERIOD_LABEL.to_string(), label.to_string()));
                pending_info.insert(label, pending);
                forward_metrics()
                    .source_pending
                    .get_or_create(&metric_labels)
                    .set(pending);
            }
        }
        // skip for those the pending is not implemented
        if !pending_info.is_empty() {
            info!("Pending messages {:?}", pending_info);
            pending_info.clear();
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

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;
    use std::time::Instant;

    use numaflow::source::{Message, Offset, SourceReadRequest};
    use numaflow::{sink, source, sourcetransform};
    use tokio::sync::mpsc::Sender;

    use super::*;
    use crate::monovertex::metrics::UserDefinedContainerState;
    use crate::shared::utils::create_rpc_channel;

    struct SimpleSource;
    #[tonic::async_trait]
    impl source::Sourcer for SimpleSource {
        async fn read(&self, _: SourceReadRequest, _: Sender<Message>) {}

        async fn ack(&self, _: Offset) {}

        async fn pending(&self) -> usize {
            0
        }

        async fn partitions(&self) -> Option<Vec<i32>> {
            None
        }
    }

    struct SimpleSink;

    #[tonic::async_trait]
    impl sink::Sinker for SimpleSink {
        async fn sink(
            &self,
            _input: tokio::sync::mpsc::Receiver<sink::SinkRequest>,
        ) -> Vec<sink::Response> {
            vec![]
        }
    }

    struct NowCat;

    #[tonic::async_trait]
    impl sourcetransform::SourceTransformer for NowCat {
        async fn transform(
            &self,
            _input: sourcetransform::SourceTransformRequest,
        ) -> Vec<sourcetransform::Message> {
            vec![]
        }
    }

    #[tokio::test]
    async fn test_start_metrics_https_server() {
        let (src_shutdown_tx, src_shutdown_rx) = tokio::sync::oneshot::channel();
        let tmp_dir = tempfile::TempDir::new().unwrap();
        let src_sock_file = tmp_dir.path().join("source.sock");
        let src_info_file = tmp_dir.path().join("source-server-info");

        let server_info = src_info_file.clone();
        let server_socket = src_sock_file.clone();
        let src_server_handle = tokio::spawn(async move {
            source::Server::new(SimpleSource)
                .with_socket_file(server_socket)
                .with_server_info_file(server_info)
                .start_with_shutdown(src_shutdown_rx)
                .await
                .unwrap();
        });

        let (sink_shutdown_tx, sink_shutdown_rx) = tokio::sync::oneshot::channel();
        let (fb_sink_shutdown_tx, fb_sink_shutdown_rx) = tokio::sync::oneshot::channel();
        let tmp_dir = tempfile::TempDir::new().unwrap();
        let sink_sock_file = tmp_dir.path().join("sink.sock");
        let sink_server_info = tmp_dir.path().join("sink-server-info");
        let fb_sink_sock_file = tmp_dir.path().join("fallback-sink.sock");
        let fb_sink_server_info = tmp_dir.path().join("fallback-sink-server-info");

        let server_socket = sink_sock_file.clone();
        let server_info = sink_server_info.clone();
        let sink_server_handle = tokio::spawn(async move {
            sink::Server::new(SimpleSink)
                .with_socket_file(server_socket)
                .with_server_info_file(server_info)
                .start_with_shutdown(sink_shutdown_rx)
                .await
                .unwrap();
        });
        let fb_server_socket = fb_sink_sock_file.clone();
        let fb_server_info = fb_sink_server_info.clone();
        let fb_sink_server_handle = tokio::spawn(async move {
            sink::Server::new(SimpleSink)
                .with_socket_file(fb_server_socket)
                .with_server_info_file(fb_server_info)
                .start_with_shutdown(fb_sink_shutdown_rx)
                .await
                .unwrap();
        });

        // start the transformer server
        let (transformer_shutdown_tx, transformer_shutdown_rx) = tokio::sync::oneshot::channel();
        let sock_file = tmp_dir.path().join("sourcetransform.sock");
        let server_info_file = tmp_dir.path().join("sourcetransformer-server-info");

        let server_info = server_info_file.clone();
        let server_socket = sock_file.clone();
        let transformer_handle = tokio::spawn(async move {
            sourcetransform::Server::new(NowCat)
                .with_socket_file(server_socket)
                .with_server_info_file(server_info)
                .start_with_shutdown(transformer_shutdown_rx)
                .await
                .expect("server failed");
        });

        // wait for the servers to start
        // FIXME: we need to have a better way, this is flaky
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        let metrics_state = UserDefinedContainerState {
            source_client: Some(SourceClient::new(
                create_rpc_channel(src_sock_file).await.unwrap(),
            )),
            sink_client: Some(SinkClient::new(
                create_rpc_channel(sink_sock_file).await.unwrap(),
            )),
            transformer_client: Some(SourceTransformClient::new(
                create_rpc_channel(sock_file).await.unwrap(),
            )),
            fb_sink_client: Some(SinkClient::new(
                create_rpc_channel(fb_sink_sock_file).await.unwrap(),
            )),
        };

        let addr: SocketAddr = "127.0.0.1:9091".parse().unwrap();
        let metrics_state_clone = metrics_state.clone();
        let server_handle = tokio::spawn(async move {
            start_metrics_https_server(addr, metrics_state_clone)
                .await
                .unwrap();
        });

        // invoke the sidecar-livez endpoint
        let response = sidecar_livez(State(metrics_state)).await;
        assert_eq!(response.into_response().status(), StatusCode::NO_CONTENT);

        // invoke the livez endpoint
        let response = livez().await;
        assert_eq!(response.into_response().status(), StatusCode::NO_CONTENT);

        // invoke the metrics endpoint
        let response = metrics_handler().await;
        assert_eq!(response.into_response().status(), StatusCode::OK);

        // Stop the servers
        server_handle.abort();
        src_shutdown_tx.send(()).unwrap();
        sink_shutdown_tx.send(()).unwrap();
        fb_sink_shutdown_tx.send(()).unwrap();
        transformer_shutdown_tx.send(()).unwrap();
        src_server_handle.await.unwrap();
        sink_server_handle.await.unwrap();
        fb_sink_server_handle.await.unwrap();
        transformer_handle.await.unwrap();
    }

    #[tokio::test]
    async fn test_expose_pending_metrics() {
        let pending_stats = Arc::new(Mutex::new(Vec::with_capacity(MAX_PENDING_STATS)));
        let refresh_interval = Duration::from_secs(1);

        // Populate pending_stats with some values.
        // The array will be sorted by the timestamp with the most recent last.
        {
            let mut pending_stats = pending_stats.lock().await;
            pending_stats.push(TimestampedPending {
                pending: 15,
                timestamp: Instant::now() - Duration::from_secs(150),
            });
            pending_stats.push(TimestampedPending {
                pending: 30,
                timestamp: Instant::now() - Duration::from_secs(70),
            });
            pending_stats.push(TimestampedPending {
                pending: 20,
                timestamp: Instant::now() - Duration::from_secs(30),
            });
            pending_stats.push(TimestampedPending {
                pending: 10,
                timestamp: Instant::now(),
            });
        }

        tokio::spawn({
            let pending_stats = pending_stats.clone();
            async move {
                expose_pending_metrics(refresh_interval, pending_stats).await;
            }
        });
        // We use tokio::time::interval() as the ticker in the expose_pending_metrics() function.
        // The first tick happens immediately, so we don't need to wait for the refresh_interval for the first iteration to complete.
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Get the stored values for all time intervals
        // We will store the values corresponding to the labels (from LOOKBACK_SECONDS_MAP) "1m", "default", "5m", "15" in the same order in this array
        let mut stored_values: [i64; 4] = [0; 4];
        {
            for (i, (label, _)) in LOOKBACK_SECONDS_MAP.iter().enumerate() {
                let mut metric_labels = forward_metrics_labels().clone();
                metric_labels.push((PENDING_PERIOD_LABEL.to_string(), label.to_string()));
                let guage = forward_metrics()
                    .source_pending
                    .get_or_create(&metric_labels)
                    .get();
                stored_values[i] = guage;
            }
        }
        assert_eq!(stored_values, [15, 20, 18, 18]);
    }
    #[test]
    fn test_exponential_buckets_range_basic() {
        let min = 1.0;
        let max = 32.0;
        let length = 6;
        let buckets: Vec<f64> = exponential_buckets_range(min, max, length).collect();
        let expected = vec![1.0, 2.0, 4.0, 8.0, 16.0, 32.0];
        assert_eq!(buckets, expected);
    }

    #[test]
    fn test_exponential_buckets_range_mico_to_seconds_minutes() {
        let min = 100.0;
        let max = 60000000.0 * 15.0;
        let length = 10;
        let buckets: Vec<f64> = exponential_buckets_range(min, max, length).collect();
        let expected: Vec<f64> = vec![
            100.0,
            592.5071727239734,
            3510.6474972935644,
            20800.838230519028,
            123246.45850253566,
            730244.1067557991,
            4.32674871092222e+06,
            2.5636296457956206e+07,
            1.5189689533417246e+08,
            8.999999999999983e+08,
        ];
        for (i, bucket) in buckets.iter().enumerate() {
            assert!((bucket - expected[i]).abs() < 1e-2);
        }
    }
    #[test]
    #[should_panic(expected = "ExponentialBucketsRange length needs a positive length")]
    fn test_exponential_buckets_range_zero_length() {
        let _ = exponential_buckets_range(1.0, 100.0, 0).collect::<Vec<f64>>();
    }

    #[test]
    #[should_panic(expected = "ExponentialBucketsRange min needs to be greater than 0")]
    fn test_exponential_buckets_range_zero_min() {
        let _ = exponential_buckets_range(0.0, 100.0, 10).collect::<Vec<f64>>();
    }

    #[test]
    #[should_panic(expected = "ExponentialBucketsRange min needs to be greater than 0")]
    fn test_exponential_buckets_range_negative_min() {
        let _ = exponential_buckets_range(-1.0, 100.0, 10).collect::<Vec<f64>>();
    }
}
