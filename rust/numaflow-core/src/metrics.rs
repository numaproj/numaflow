use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::{Arc, OnceLock};
use std::time::Duration;
use std::{env, iter};

use crate::config::{config, get_pipeline_name, get_vertex_name, get_vertex_replica};
use crate::source::Source;
use crate::Error;
use axum::body::Body;
use axum::extract::State;
use axum::http::{Response, StatusCode};
use axum::response::IntoResponse;
use axum::{routing::get, Router};
use axum_server::tls_rustls::RustlsConfig;
use numaflow_models::models::tls;
use numaflow_pb::clients::mvtxdaemon::mono_vertex_daemon_service_client::MonoVertexDaemonServiceClient;
use numaflow_pb::clients::sink::sink_client::SinkClient;
use numaflow_pb::clients::source::source_client::SourceClient;
use numaflow_pb::clients::sourcetransformer::source_transform_client::SourceTransformClient;
use prometheus_client::encoding::text::encode;
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::family::Family;
use prometheus_client::metrics::gauge::Gauge;
use prometheus_client::metrics::histogram::Histogram;
use prometheus_client::registry::Registry;
use rcgen::{generate_simple_self_signed, CertifiedKey};
use rustls::ClientConfig;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tokio::time;
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Endpoint};
use tonic::Request;
use tracing::{debug, error, info};

// SDK information
const SDK_INFO: &str = "sdk_info";
const COMPONENT: &str = "component";
const COMPONENT_NAME: &str = "component_name";
const SDK_VERSION: &str = "version";
const SDK_LANGUAGE: &str = "language";
const SDK_TYPE: &str = "type";

// Define the labels for the metrics
// Note: Please keep consistent with the definitions in MonoVertex daemon
const MVTX_NAME_LABEL: &str = "mvtx_name";
const REPLICA_LABEL: &str = "mvtx_replica";
const PENDING_PERIOD_LABEL: &str = "period";

const PIPELINE_NAME_LABEL: &str = "pipeline";
const PIPELINE_REPLICA_LABEL: &str = "replica";
const PIPELINE_PARTITION_NAME_LABEL: &str = "partition_name";
const PIPELINE_VERTEX_LABEL: &str = "vertex";
const PIPELINE_VERTEX_TYPE_LABEL: &str = "vertex_type";

// The top-level metric registry is created with the GLOBAL_PREFIX
const MVTX_REGISTRY_GLOBAL_PREFIX: &str = "monovtx";
// Prefixes for the sub-registries
const SINK_REGISTRY_PREFIX: &str = "sink";
const FALLBACK_SINK_REGISTRY_PREFIX: &str = "fallback_sink";
const TRANSFORMER_REGISTRY_PREFIX: &str = "transformer";

// Define the metrics
// Note: We do not add a suffix to the metric name, as the suffix is inferred through the metric type
// by the prometheus client library
// refer: https://github.com/prometheus/client_rust/blob/master/src/registry.rs#L102
// Note: Please keep consistent with the definitions in MonoVertex daemon

// counters (please note the prefix _total, and read above link)
const READ_TOTAL: &str = "read";
const READ_BYTES_TOTAL: &str = "read_bytes";
const ACK_TOTAL: &str = "ack";
const SINK_WRITE_TOTAL: &str = "write";
const DROPPED_TOTAL: &str = "dropped";
const FALLBACK_SINK_WRITE_TOTAL: &str = "write";

// pending as gauge
const PENDING: &str = "pending";

// processing times as timers
const E2E_TIME: &str = "processing_time";
const READ_TIME: &str = "read_time";
const TRANSFORM_TIME: &str = "time";
const ACK_TIME: &str = "ack_time";
const SINK_TIME: &str = "time";

const PIPELINE_FORWARDER_READ_TOTAL: &str = "data_read";

/// Only user defined functions will have containers since rest
/// are builtins. We save the gRPC clients to retrieve metrics and also
/// to do liveness checks.
#[derive(Clone)]
pub(crate) enum UserDefinedContainerState {
    Monovertex(MonovertexContainerState),
    Pipeline(PipelineContainerState),
}

/// MonovertexContainerState is used to store the gRPC clients for the
/// monovtx. These will be optionals since
/// we do not require these for builtins.
#[derive(Clone)]
pub(crate) struct MonovertexContainerState {
    pub source_client: Option<SourceClient<Channel>>,
    pub sink_client: Option<SinkClient<Channel>>,
    pub transformer_client: Option<SourceTransformClient<Channel>>,
    pub fb_sink_client: Option<SinkClient<Channel>>,
}

/// PipelineContainerState is used to store the gRPC clients for the
/// pipeline.
#[derive(Clone)]
pub(crate) enum PipelineContainerState {
    Source(
        (
            Option<SourceClient<Channel>>,
            Option<SourceTransformClient<Channel>>,
        ),
    ),
    Sink((Option<SinkClient<Channel>>, Option<SinkClient<Channel>>)),
}

/// The global register of all metrics.
#[derive(Default)]
struct GlobalRegistry {
    // It is okay to use std mutex because we register each metric only one time.
    registry: parking_lot::Mutex<Registry>,
}

impl GlobalRegistry {
    fn new() -> Self {
        GlobalRegistry {
            // Create a new registry for the metrics
            registry: parking_lot::Mutex::new(Registry::default()),
        }
    }
}

/// GLOBAL_REGISTRY is the static global registry which is initialized only once.
static GLOBAL_REGISTRY: OnceLock<GlobalRegistry> = OnceLock::new();

/// global_registry is a helper function to get the GLOBAL_REGISTRY
fn global_registry() -> &'static GlobalRegistry {
    GLOBAL_REGISTRY.get_or_init(GlobalRegistry::new)
}

/// GlobalMetrics is a struct which is used for storing the global metrics
pub(crate) struct GlobalMetrics {
    pub(crate) sdk_info: Family<Vec<(String, String)>, Gauge>,
}

impl GlobalMetrics {
    fn new() -> Self {
        let metrics = Self {
            sdk_info: Family::<Vec<(String, String)>, Gauge>::default(),
        };
        let mut registry = global_registry().registry.lock();
        // Register all the metrics to the global registry
        registry.register(
            SDK_INFO,
            "A metric with a constant value '1', labeled by SDK information such as version, language, and type",
            metrics.sdk_info.clone(),
        );
        metrics
    }
}

/// GLOBAL_METRICS is the GlobalMetrics object which stores the metrics
static GLOBAL_METRICS: OnceLock<GlobalMetrics> = OnceLock::new();

pub(crate) fn global_metrics() -> &'static GlobalMetrics {
    GLOBAL_METRICS.get_or_init(GlobalMetrics::new)
}

/// MonoVtxMetrics is a struct which is used for storing the metrics related to MonoVertex
// These fields are exposed as pub to be used by other modules for
// changing the value of the metrics
// Each metric is defined as family of metrics, which means that they can be
// differentiated by their label values assigned.
// The labels are provided in the form of Vec<(String, String)>
// The second argument is the metric kind.
pub(crate) struct MonoVtxMetrics {
    // counters
    pub(crate) read_total: Family<Vec<(String, String)>, Counter>,
    pub(crate) read_bytes_total: Family<Vec<(String, String)>, Counter>,
    pub(crate) ack_total: Family<Vec<(String, String)>, Counter>,
    pub(crate) dropped_total: Family<Vec<(String, String)>, Counter>,

    // gauge
    pub(crate) pending: Family<Vec<(String, String)>, Gauge>,

    // timers
    pub(crate) e2e_time: Family<Vec<(String, String)>, Histogram>,
    pub(crate) read_time: Family<Vec<(String, String)>, Histogram>,
    pub(crate) ack_time: Family<Vec<(String, String)>, Histogram>,

    pub(crate) transformer: TransformerMetrics,
    pub(crate) sink: SinkMetrics,
    pub(crate) fb_sink: FallbackSinkMetrics,
}

/// PipelineMetrics is a struct which is used for storing the metrics related to the Pipeline
// TODO: Add the metrics for the pipeline
pub(crate) struct PipelineMetrics {
    pub(crate) forwarder: PipelineForwarderMetrics,
    pub(crate) isb: PipelineISBMetrics,
}

/// Family of metrics for the sink
pub(crate) struct SinkMetrics {
    pub(crate) write_total: Family<Vec<(String, String)>, Counter>,
    pub(crate) time: Family<Vec<(String, String)>, Histogram>,
}

/// Family of metrics for the Fallback Sink
pub(crate) struct FallbackSinkMetrics {
    pub(crate) write_total: Family<Vec<(String, String)>, Counter>,
}

/// Family of metrics for the Transformer
pub(crate) struct TransformerMetrics {
    /// Transformer latency
    pub(crate) time: Family<Vec<(String, String)>, Histogram>,
}

pub(crate) struct PipelineForwarderMetrics {
    pub(crate) read_total: Family<Vec<(String, String)>, Counter>,
    pub(crate) read_time: Family<Vec<(String, String)>, Histogram>,
    pub(crate) ack_total: Family<Vec<(String, String)>, Counter>,
    pub(crate) ack_time: Family<Vec<(String, String)>, Histogram>,
    pub(crate) write_total: Family<Vec<(String, String)>, Counter>,
    pub(crate) read_bytes_total: Family<Vec<(String, String)>, Counter>,
    pub(crate) processed_time: Family<Vec<(String, String)>, Histogram>,
    pub(crate) pending: Family<Vec<(String, String)>, Gauge>,
}

pub(crate) struct PipelineISBMetrics {
    pub(crate) paf_resolution_time: Family<Vec<(String, String)>, Histogram>,
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
            dropped_total: Family::<Vec<(String, String)>, Counter>::default(),
            // gauge
            pending: Family::<Vec<(String, String)>, Gauge>::default(),
            // timers
            // exponential buckets in the range 100 microseconds to 15 minutes
            e2e_time: Family::<Vec<(String, String)>, Histogram>::new_with_constructor(|| {
                Histogram::new(exponential_buckets_range(100.0, 60000000.0 * 15.0, 10))
            }),
            read_time: Family::<Vec<(String, String)>, Histogram>::new_with_constructor(|| {
                Histogram::new(exponential_buckets_range(100.0, 60000000.0 * 15.0, 10))
            }),
            ack_time: Family::<Vec<(String, String)>, Histogram>::new_with_constructor(|| {
                Histogram::new(exponential_buckets_range(100.0, 60000000.0 * 15.0, 10))
            }),

            transformer: TransformerMetrics {
                time: Family::<Vec<(String, String)>, Histogram>::new_with_constructor(|| {
                    Histogram::new(exponential_buckets_range(100.0, 60000000.0 * 15.0, 10))
                }),
            },

            sink: SinkMetrics {
                write_total: Family::<Vec<(String, String)>, Counter>::default(),
                time: Family::<Vec<(String, String)>, Histogram>::new_with_constructor(|| {
                    Histogram::new(exponential_buckets_range(100.0, 60000000.0 * 15.0, 10))
                }),
            },

            fb_sink: FallbackSinkMetrics {
                write_total: Family::<Vec<(String, String)>, Counter>::default(),
            },
        };

        let mut registry = global_registry().registry.lock();
        let registry = registry.sub_registry_with_prefix(MVTX_REGISTRY_GLOBAL_PREFIX);
        // Register all the metrics to the global registry
        registry.register(
            READ_TOTAL,
            "A Counter to keep track of the total number of messages read from the source",
            metrics.read_total.clone(),
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

        // gauges
        registry.register(
            PENDING,
            "A Gauge to keep track of the total number of pending messages for the monovtx",
            metrics.pending.clone(),
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
            ACK_TIME,
            "A Histogram to keep track of the total time taken to Ack to the Source, in microseconds",
            metrics.ack_time.clone(),
        );

        // Transformer metrics
        let transformer_registry = registry.sub_registry_with_prefix(TRANSFORMER_REGISTRY_PREFIX);
        transformer_registry.register(
            TRANSFORM_TIME,
            "A Histogram to keep track of the total time taken to Transform, in microseconds",
            metrics.transformer.time.clone(),
        );

        // Sink metrics
        let sink_registry = registry.sub_registry_with_prefix(SINK_REGISTRY_PREFIX);
        sink_registry.register(
            SINK_WRITE_TOTAL,
            "A Counter to keep track of the total number of messages written to the sink",
            metrics.sink.write_total.clone(),
        );
        sink_registry.register(
            SINK_TIME,
            "A Histogram to keep track of the total time taken to Write to the Sink, in microseconds",
            metrics.sink.time.clone(),
        );

        // Fallback Sink metrics
        let fb_sink_registry = registry.sub_registry_with_prefix(FALLBACK_SINK_REGISTRY_PREFIX);

        fb_sink_registry.register(
            FALLBACK_SINK_WRITE_TOTAL,
            "A Counter to keep track of the total number of messages written to the fallback sink",
            metrics.fb_sink.write_total.clone(),
        );
        metrics
    }
}

impl PipelineMetrics {
    fn new() -> Self {
        let metrics = Self {
            forwarder: PipelineForwarderMetrics {
                read_total: Family::<Vec<(String, String)>, Counter>::default(),
                processed_time: Family::<Vec<(String, String)>, Histogram>::new_with_constructor(
                    || Histogram::new(exponential_buckets_range(100.0, 60000000.0 * 15.0, 10)),
                ),
                read_time: Family::<Vec<(String, String)>, Histogram>::new_with_constructor(|| {
                    Histogram::new(exponential_buckets_range(100.0, 60000000.0 * 15.0, 10))
                }),
                read_bytes_total: Family::<Vec<(String, String)>, Counter>::default(),
                ack_total: Family::<Vec<(String, String)>, Counter>::default(),
                ack_time: Family::<Vec<(String, String)>, Histogram>::new_with_constructor(|| {
                    Histogram::new(exponential_buckets_range(100.0, 60000000.0 * 15.0, 10))
                }),
                pending: Family::<Vec<(String, String)>, Gauge>::default(),
                write_total: Family::<Vec<(String, String)>, Counter>::default(),
            },
            isb: PipelineISBMetrics {
                paf_resolution_time:
                    Family::<Vec<(String, String)>, Histogram>::new_with_constructor(|| {
                        Histogram::new(exponential_buckets_range(100.0, 60000000.0 * 15.0, 10))
                    }),
            },
        };
        let mut registry = global_registry().registry.lock();

        // Pipeline forwarder sub-registry
        let forwarder_registry = registry.sub_registry_with_prefix("forwarder");
        forwarder_registry.register(
            PIPELINE_FORWARDER_READ_TOTAL,
            "Total number of Data Messages Read",
            metrics.forwarder.read_total.clone(),
        );
        forwarder_registry.register(
            READ_TIME,
            "Time taken to read data",
            metrics.forwarder.read_time.clone(),
        );
        forwarder_registry.register(
            READ_BYTES_TOTAL,
            "Total number of bytes read",
            metrics.forwarder.read_bytes_total.clone(),
        );
        forwarder_registry.register(
            E2E_TIME,
            "Time taken to process data",
            metrics.forwarder.processed_time.clone(),
        );
        forwarder_registry.register(
            ACK_TOTAL,
            "Total number of Ack Messages",
            metrics.forwarder.ack_total.clone(),
        );
        forwarder_registry.register(
            ACK_TIME,
            "Time taken to ack data",
            metrics.forwarder.ack_time.clone(),
        );
        forwarder_registry.register(
            PENDING,
            "Number of pending messages",
            metrics.forwarder.pending.clone(),
        );
        metrics
    }
}

/// MONOVTX_METRICS is the MonoVtxMetrics object which stores the metrics
static MONOVTX_METRICS: OnceLock<MonoVtxMetrics> = OnceLock::new();

// forward_metrics is a helper function used to fetch the
// MonoVtxMetrics object
pub(crate) fn monovertex_metrics() -> &'static MonoVtxMetrics {
    MONOVTX_METRICS.get_or_init(MonoVtxMetrics::new)
}

/// PIPELINE_METRICS is the PipelineMetrics object which stores the metrics
static PIPELINE_METRICS: OnceLock<PipelineMetrics> = OnceLock::new();

// pipeline_metrics is a helper function used to fetch the
// PipelineMetrics object
pub(crate) fn pipeline_metrics() -> &'static PipelineMetrics {
    PIPELINE_METRICS.get_or_init(PipelineMetrics::new)
}

// sdk_info_labels is a helper function used to build the labels used in sdk_info
pub(crate) fn sdk_info_labels(
    component: String,
    component_name: String,
    language: String,
    version: String,
    container_type: String,
) -> Vec<(String, String)> {
    let labels = vec![
        (COMPONENT.to_string(), component),
        (COMPONENT_NAME.to_string(), component_name),
        (SDK_LANGUAGE.to_string(), language),
        (SDK_VERSION.to_string(), version),
        (SDK_TYPE.to_string(), container_type),
    ];
    labels
}

/// MONOVTX_METRICS_LABELS are used to store the common labels used in the metrics
static MONOVTX_METRICS_LABELS: OnceLock<Vec<(String, String)>> = OnceLock::new();

// forward_metrics_labels is a helper function used to fetch the
// MONOVTX_METRICS_LABELS object
pub(crate) fn mvtx_forward_metric_labels() -> &'static Vec<(String, String)> {
    MONOVTX_METRICS_LABELS.get_or_init(|| {
        let common_labels = vec![
            (MVTX_NAME_LABEL.to_string(), get_vertex_name().to_string()),
            (REPLICA_LABEL.to_string(), get_vertex_replica().to_string()),
        ];
        common_labels
    })
}

static PIPELINE_READ_METRICS_LABELS: OnceLock<Vec<(String, String)>> = OnceLock::new();

pub(crate) fn pipeline_forward_metric_labels(
    vertex_type: &str,
    partition_name: Option<&str>,
) -> &'static Vec<(String, String)> {
    PIPELINE_READ_METRICS_LABELS.get_or_init(|| {
        let mut labels = vec![
            (
                PIPELINE_NAME_LABEL.to_string(),
                get_pipeline_name().to_string(),
            ),
            (
                PIPELINE_REPLICA_LABEL.to_string(),
                get_vertex_replica().to_string(),
            ),
            (
                PIPELINE_VERTEX_TYPE_LABEL.to_string(),
                vertex_type.to_string(),
            ),
            (
                PIPELINE_VERTEX_LABEL.to_string(),
                get_vertex_name().to_string(),
            ),
        ];

        if let Some(partition) = partition_name {
            labels.push((
                PIPELINE_PARTITION_NAME_LABEL.to_string(),
                partition.to_string(),
            ));
        }

        labels
    })
}

static PIPELINE_ISB_METRICS_LABELS: OnceLock<Vec<(String, String)>> = OnceLock::new();

pub(crate) fn pipeline_isb_metric_labels() -> &'static Vec<(String, String)> {
    PIPELINE_ISB_METRICS_LABELS.get_or_init(|| {
        vec![
            (
                PIPELINE_NAME_LABEL.to_string(),
                get_pipeline_name().to_string(),
            ),
            (
                PIPELINE_REPLICA_LABEL.to_string(),
                get_vertex_replica().to_string(),
            ),
            (
                PIPELINE_VERTEX_LABEL.to_string(),
                get_vertex_name().to_string(),
            ),
        ]
    })
}

// metrics_handler is used to generate and return a snapshot of the
// current state of the metrics in the global registry
pub async fn metrics_handler() -> impl IntoResponse {
    let state = global_registry().registry.lock();
    let mut buffer = String::new();
    encode(&mut buffer, &state).unwrap();
    debug!("Exposing metrics: {:?}", buffer);
    Response::builder()
        .status(StatusCode::OK)
        .header(
            axum::http::header::CONTENT_TYPE,
            "application/openmetrics-text; version=1.0.0; charset=utf-8",
        )
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
    match state {
        UserDefinedContainerState::Monovertex(monovertex_state) => {
            if let Some(mut source_client) = monovertex_state.source_client {
                if source_client.is_ready(Request::new(())).await.is_err() {
                    error!("Monovertex source client is not ready");
                    return StatusCode::INTERNAL_SERVER_ERROR;
                }
            }
            if let Some(mut sink_client) = monovertex_state.sink_client {
                if sink_client.is_ready(Request::new(())).await.is_err() {
                    error!("Monovertex sink client is not ready");
                    return StatusCode::INTERNAL_SERVER_ERROR;
                }
            }
            if let Some(mut transformer_client) = monovertex_state.transformer_client {
                if transformer_client.is_ready(Request::new(())).await.is_err() {
                    error!("Monovertex transformer client is not ready");
                    return StatusCode::INTERNAL_SERVER_ERROR;
                }
            }
            if let Some(mut fb_sink_client) = monovertex_state.fb_sink_client {
                if fb_sink_client.is_ready(Request::new(())).await.is_err() {
                    error!("Monovertex fallback sink client is not ready");
                    return StatusCode::INTERNAL_SERVER_ERROR;
                }
            }
        }
        UserDefinedContainerState::Pipeline(pipeline_state) => match pipeline_state {
            PipelineContainerState::Source((source_client, transformer_client)) => {
                if let Some(mut source_client) = source_client {
                    if source_client.is_ready(Request::new(())).await.is_err() {
                        error!("Pipeline source client is not ready");
                        return StatusCode::INTERNAL_SERVER_ERROR;
                    }
                }
                if let Some(mut transformer_client) = transformer_client {
                    if transformer_client.is_ready(Request::new(())).await.is_err() {
                        error!("Pipeline transformer client is not ready");
                        return StatusCode::INTERNAL_SERVER_ERROR;
                    }
                }
            }
            PipelineContainerState::Sink((sink_client, fb_sink_client)) => {
                if let Some(mut sink_client) = sink_client {
                    if sink_client.is_ready(Request::new(())).await.is_err() {
                        error!("Pipeline sink client is not ready");
                        return StatusCode::INTERNAL_SERVER_ERROR;
                    }
                }
                if let Some(mut fb_sink_client) = fb_sink_client {
                    if fb_sink_client.is_ready(Request::new(())).await.is_err() {
                        error!("Pipeline fallback sink client is not ready");
                        return StatusCode::INTERNAL_SERVER_ERROR;
                    }
                }
            }
        },
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
    lag_reader: Source,
    lag_checking_interval: Duration,
    refresh_interval: Duration,
    pending_stats: Arc<Mutex<Vec<TimestampedPending>>>,
    lookback_seconds: u16,
}

pub(crate) struct PendingReaderTasks {
    buildup_handle: JoinHandle<()>,
    expose_handle: JoinHandle<()>,
}

/// PendingReaderBuilder is used to build a [LagReader] instance.
pub(crate) struct PendingReaderBuilder {
    lag_reader: Source,
    lag_checking_interval: Option<Duration>,
    refresh_interval: Option<Duration>,
    lookback_seconds: Option<u16>,
}

impl PendingReaderBuilder {
    pub(crate) fn new(lag_reader: Source) -> Self {
        Self {
            lag_reader,
            lag_checking_interval: None,
            refresh_interval: None,
            lookback_seconds: None,
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

    pub(crate) fn lookback_seconds(mut self, seconds: u16) -> Self {
        self.lookback_seconds = Some(seconds);
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
            lookback_seconds: self.lookback_seconds.unwrap_or(120),
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
    pub async fn start(&self, is_mono_vertex: bool) -> PendingReaderTasks {
        let pending_reader = self.lag_reader.clone();
        let lag_checking_interval = self.lag_checking_interval;
        let refresh_interval = self.refresh_interval;
        let pending_stats = Arc::clone(&self.pending_stats);
        let lookback_seconds = self.lookback_seconds;

        let buildup_handle = tokio::spawn(async move {
            build_pending_info(pending_reader, lag_checking_interval, pending_stats).await;
        });

        let pending_stats = Arc::clone(&self.pending_stats);
        let expose_handle = tokio::spawn(async move {
            expose_pending_metrics(
                is_mono_vertex,
                refresh_interval,
                pending_stats,
                lookback_seconds,
            )
            .await;
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
    source: Source,
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

async fn fetch_pending(lag_reader: &Source) -> crate::error::Result<i64> {
    let response: i64 = lag_reader.pending().await?.map_or(-1, |p| p as i64); // default to -1(unavailable)
    Ok(response)
}

// Periodically exposes the pending metrics by calculating the average pending messages over different intervals.
async fn expose_pending_metrics(
    is_mono_vertex: bool,
    refresh_interval: Duration,
    pending_stats: Arc<Mutex<Vec<TimestampedPending>>>,
    lookback_seconds: u16,
) {
    let mut ticker = time::interval(refresh_interval);

    // store the pending info in a sorted way for deterministic display
    // string concat is more efficient?
    let mut pending_info: BTreeMap<&str, i64> = BTreeMap::new();

    let mut lookback_seconds_map: [(&str, u16); 4] = [
        ("1m", 60),
        ("default", lookback_seconds),
        ("5m", 300),
        ("15m", 900),
    ];

    loop {
        let server_url = env::var("SERVER_URL").expect("NO SERVER");
        info!("Server URL: {}", server_url);

        let server_root_ca_cert = std::fs::read_to_string("").expect("CA should be present");
        let server_root_ca_cert = Certificate::from_pem(server_root_ca_cert);
        let _ = server_root_ca_cert;
        let tls = ClientTlsConfig::new().domain_name("localhost");

        // create an RPC channel to the daemon using an insecure verify true tls
        let channel = Endpoint::try_from(server_url.clone())
            .map_err(|e| Error::Connection(format!("Failed to create endpoint: {:?}", e)))
            .unwrap()
            .tls_config(tls)
            .expect("TLS config should not fail")
            .connect()
            .await
            .unwrap();

        let mut daemon_client = MonoVertexDaemonServiceClient::new(channel);

        let res = daemon_client
            .get_mono_vertex_status(Request::new(()))
            .await
            .expect("daemon should be up");

        let status = res.into_inner().status.expect("cannot be none").status;
        info!(?status, "mvtx daemon status");

        // let mut config = rustls::ClientConfig::
        // TODO(mvtx-adapt):  Create TLS client
        // if channel.is_err() {
        //     info!("MYDEBUG: Failed to connect to the daemon {}", channel.unwrap_err());
        // } else {
        //     let mut daemon_client = MonoVertexDaemonServiceClient::new(server_url.clone());
        //     let res = daemon_client.get_mono_vertex_status(Request::new(())).await.expect("TODO: panic message");
        //     // extract the status from the response
        //     let status = res.into_inner().status;
        //     info!("MYDEBUG: Status: {:?}", status.unwrap().status);
        // }

        // // let channel = Endpoint::try_from(server_url.clone())
        // //     .map_err(|e| Error::Connection(format!("Failed to create endpoint: {:?}", e))).unwrap()
        // //     .connect().await.unwrap();
        // let daemon_client = MonoVertexDaemonServiceClient::connect(server_url.clone()).await;
        // if daemon_client.is_err() {
        //     info!("MYDEBUG: Failed to connect to the daemon {} {}", server_url.clone(), daemon_client.unwrap_err());
        // } else {
        //     let res = daemon_client.unwrap().get_mono_vertex_status(Request::new(())).await.expect("TODO: panic message");
        //     // extract the status from the response
        //     let status = res.into_inner().status;
        //     info!("MYDEBUG: Status: {:?}", status.unwrap().status);
        // }
        // ticker.tick().await;
        // call the daemon -> new_value
        // err = default
        for (label, seconds) in lookback_seconds_map {
            let pending = calculate_pending(seconds as i64, &pending_stats).await;
            if pending != -1 {
                let mut metric_labels = mvtx_forward_metric_labels().clone();
                metric_labels.push((PENDING_PERIOD_LABEL.to_string(), label.to_string()));
                pending_info.insert(label, pending);
                if is_mono_vertex {
                    monovertex_metrics()
                        .pending
                        .get_or_create(&metric_labels)
                        .set(pending);
                } else {
                    pipeline_metrics()
                        .forwarder
                        .pending
                        .get_or_create(&metric_labels)
                        .set(pending);
                }
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
    use crate::shared::grpc::create_rpc_channel;

    struct SimpleSource;
    #[tonic::async_trait]
    impl source::Sourcer for SimpleSource {
        async fn read(&self, _: SourceReadRequest, _: Sender<Message>) {}

        async fn ack(&self, _: Vec<Offset>) {}

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
        tokio::time::sleep(Duration::from_millis(100)).await;
        let metrics_state = UserDefinedContainerState::Monovertex(MonovertexContainerState {
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
        });

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
        let lookback_seconds = 120;

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
                expose_pending_metrics(true, refresh_interval, pending_stats, lookback_seconds)
                    .await;
            }
        });
        // We use tokio::time::interval() as the ticker in the expose_pending_metrics() function.
        // The first tick happens immediately, so we don't need to wait for the refresh_interval for the first iteration to complete.
        tokio::time::sleep(Duration::from_millis(50)).await;

        let lookback_seconds_map: [(&str, u16); 4] =
            [("1m", 60), ("default", 120), ("5m", 300), ("15m", 900)];

        // Get the stored values for all time intervals
        // We will store the values corresponding to the labels (from lookback_seconds_map) "1m", "default", "5m", "15" in the same order in this array
        let mut stored_values: [i64; 4] = [0; 4];
        {
            for (i, (label, _)) in lookback_seconds_map.iter().enumerate() {
                let mut metric_labels = mvtx_forward_metric_labels().clone();
                metric_labels.push((PENDING_PERIOD_LABEL.to_string(), label.to_string()));
                let guage = monovertex_metrics()
                    .pending
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

    #[test]
    fn test_metric_names() {
        let global_metrics = global_metrics();
        let sdk_labels = sdk_info_labels(
            "component".to_string(),
            "component_name".to_string(),
            "language".to_string(),
            "version".to_string(),
            "container_type".to_string(),
        );
        global_metrics.sdk_info.get_or_create(&sdk_labels).set(1);

        let metrics = monovertex_metrics();
        // Use a fixed set of labels instead of the ones from mvtx_forward_metric_labels() since other test functions may also set it.
        let common_labels = vec![
            (
                MVTX_NAME_LABEL.to_string(),
                "test-monovertex-metric-names".to_string(),
            ),
            (REPLICA_LABEL.to_string(), "3".to_string()),
        ];
        // Populate all metrics
        metrics.read_total.get_or_create(&common_labels).inc();
        metrics.read_bytes_total.get_or_create(&common_labels).inc();
        metrics.ack_total.get_or_create(&common_labels).inc();
        metrics.dropped_total.get_or_create(&common_labels).inc();
        metrics.pending.get_or_create(&common_labels).set(10);
        metrics.e2e_time.get_or_create(&common_labels).observe(10.0);
        metrics.read_time.get_or_create(&common_labels).observe(3.0);
        metrics.ack_time.get_or_create(&common_labels).observe(2.0);

        metrics
            .transformer
            .time
            .get_or_create(&common_labels)
            .observe(5.0);

        metrics.sink.write_total.get_or_create(&common_labels).inc();
        metrics.sink.time.get_or_create(&common_labels).observe(4.0);

        metrics
            .fb_sink
            .write_total
            .get_or_create(&common_labels)
            .inc();

        // Validate the metric names
        let state = global_registry().registry.lock();
        let mut buffer = String::new();
        encode(&mut buffer, &state).unwrap();

        let expected = [
            r#"sdk_info{component="component",component_name="component_name",language="language",version="version",type="container_type"} 1"#,
            r#"monovtx_read_total{mvtx_name="test-monovertex-metric-names",mvtx_replica="3"} 1"#,
            r#"monovtx_ack_total{mvtx_name="test-monovertex-metric-names",mvtx_replica="3"} 1"#,
            r#"monovtx_read_bytes_total{mvtx_name="test-monovertex-metric-names",mvtx_replica="3"} 1"#,
            r#"monovtx_dropped_total{mvtx_name="test-monovertex-metric-names",mvtx_replica="3"} 1"#,
            r#"monovtx_pending{mvtx_name="test-monovertex-metric-names",mvtx_replica="3"} 10"#,
            r#"monovtx_processing_time_sum{mvtx_name="test-monovertex-metric-names",mvtx_replica="3"} 10.0"#,
            r#"monovtx_processing_time_count{mvtx_name="test-monovertex-metric-names",mvtx_replica="3"} 1"#,
            r#"monovtx_processing_time_bucket{le="100.0",mvtx_name="test-monovertex-metric-names",mvtx_replica="3"} 1"#,
            r#"monovtx_read_time_sum{mvtx_name="test-monovertex-metric-names",mvtx_replica="3"} 3.0"#,
            r#"monovtx_read_time_count{mvtx_name="test-monovertex-metric-names",mvtx_replica="3"} 1"#,
            r#"monovtx_read_time_bucket{le="100.0",mvtx_name="test-monovertex-metric-names",mvtx_replica="3"} 1"#,
            r#"monovtx_ack_time_sum{mvtx_name="test-monovertex-metric-names",mvtx_replica="3"} 2.0"#,
            r#"monovtx_ack_time_count{mvtx_name="test-monovertex-metric-names",mvtx_replica="3"} 1"#,
            r#"monovtx_ack_time_bucket{le="100.0",mvtx_name="test-monovertex-metric-names",mvtx_replica="3"} 1"#,
            r#"monovtx_transformer_time_sum{mvtx_name="test-monovertex-metric-names",mvtx_replica="3"} 5.0"#,
            r#"monovtx_transformer_time_count{mvtx_name="test-monovertex-metric-names",mvtx_replica="3"} 1"#,
            r#"monovtx_transformer_time_bucket{le="100.0",mvtx_name="test-monovertex-metric-names",mvtx_replica="3"} 1"#,
            r#"monovtx_sink_write_total{mvtx_name="test-monovertex-metric-names",mvtx_replica="3"} 1"#,
            r#"monovtx_sink_time_sum{mvtx_name="test-monovertex-metric-names",mvtx_replica="3"} 4.0"#,
            r#"monovtx_sink_time_count{mvtx_name="test-monovertex-metric-names",mvtx_replica="3"} 1"#,
            r#"monovtx_sink_time_bucket{le="100.0",mvtx_name="test-monovertex-metric-names",mvtx_replica="3"} 1"#,
            r#"monovtx_fallback_sink_write_total{mvtx_name="test-monovertex-metric-names",mvtx_replica="3"} 1"#,
        ];

        let got = buffer
            .trim()
            .lines()
            .filter(|line| !line.starts_with("#"))
            .collect::<Vec<&str>>()
            .join("\n");

        for t in expected {
            assert!(got.contains(t));
        }
    }
}
