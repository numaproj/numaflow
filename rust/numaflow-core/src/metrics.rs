use axum::body::Body;
use axum::extract::{Query, State};
use axum::http::{Response, StatusCode};
use axum::response::IntoResponse;
use axum::{Router, routing::get};
use axum_server::tls_rustls::RustlsConfig;
use prometheus_client::encoding::text::encode;
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::family::Family;
use prometheus_client::metrics::gauge::Gauge;
use prometheus_client::metrics::histogram::Histogram;
use prometheus_client::registry::Registry;
use rcgen::{CertifiedKey, generate_simple_self_signed};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::OnceLock;
use std::sync::atomic::AtomicU64;
use std::time::Duration;
use std::{env, iter};
use tokio::task::JoinHandle;
use tokio::time;
use tracing::{debug, error, info};

use crate::Error;
use crate::config::pipeline::VERTEX_TYPE_SOURCE;
use crate::config::{get_pipeline_name, get_vertex_name, get_vertex_replica};
use crate::mapper::map::MapHandle;
use crate::pipeline::isb::reader::{ISBReader as JetStreamReader, ISBReader};
use crate::reduce::reducer::unaligned::user_defined::UserDefinedUnalignedReduce;
use crate::reduce::reducer::user_defined::UserDefinedReduce;
use crate::sinker::sink::SinkWriter;
use crate::source::Source;
use crate::watermark::WatermarkHandle;

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
const PIPELINE_NAME_LABEL: &str = "pipeline";
const PIPELINE_REPLICA_LABEL: &str = "replica";
pub(crate) const PIPELINE_PARTITION_NAME_LABEL: &str = "partition_name";
const PIPELINE_VERTEX_LABEL: &str = "vertex";
const PIPELINE_VERTEX_TYPE_LABEL: &str = "vertex_type";
const PIPELINE_DROP_REASON_LABEL: &str = "reason";

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
const DATA_READ_TOTAL: &str = "data_read";
const READ_BYTES_TOTAL: &str = "read_bytes";
const DATA_READ_BYTES_TOTAL: &str = "data_read_bytes";
const READ_ERROR_TOTAL: &str = "read_error";
const UDF_READ_TOTAL: &str = "udf_read";

const WRITE_TOTAL: &str = "write";
const UDF_WRITE_TOTAL: &str = "udf_write";
const WRITE_BYTES_TOTAL: &str = "write_bytes";
const WRITE_ERROR_TOTAL: &str = "write_error";
const ACK_TOTAL: &str = "ack";
const UDF_ERROR_TOTAL: &str = "udf_error";

const SINK_WRITE_TOTAL: &str = "write";
const SINK_WRITE_ERRORS_TOTAL: &str = "write_errors";
const FALLBACK_SINK_WRITE_ERRORS_TOTAL: &str = "fbsink_write_errors";

const SINK_DROPPED_TOTAL: &str = "dropped";
const DROPPED_TOTAL: &str = "dropped";
const PIPELINE_FORWARDER_DROP_TOTAL: &str = "drop";
const PIPELINE_FORWARDER_DROP_BYTES_TOTAL: &str = "drop_bytes";
const UDF_DROP_TOTAL: &str = "ud_drop";

const FALLBACK_SINK_WRITE_TOTAL: &str = "write";
const PIPELINE_FALLBACK_SINK_WRITE_TOTAL: &str = "fbsink_write";
const PIPELINE_FALLBACK_SINK_WRITE_BYTES_TOTAL: &str = "fbsink_write_bytes";
const TRANSFORMER_DROPPED_TOTAL: &str = "dropped";
const TRANSFORMER_ERROR_TOTAL: &str = "transformer_error";
const TRANSFORMER_READ_TOTAL: &str = "transformer_read";
const TRANSFORMER_WRITE_TOTAL: &str = "transformer_write";
const PIPELINE_TRANSFORMER_DROP_TOTAL: &str = "transformer_drop";

// jetstream isb counters
const JETSTREAM_ISB_ISFULL_ERROR_TOTAL: &str = "isFull_error";
const JETSTREAM_ISB_ISFULL_TOTAL: &str = "isFull";
const JETSTREAM_ISB_WRITE_TIMEOUT_TOTAL: &str = "write_timeout";
const JETSTREAM_ISB_WRITE_ERROR_TOTAL: &str = "write_error";
const JETSTREAM_ISB_READ_ERROR_TOTAL: &str = "read_error";

// pending as gauge for mvtx (these metric names are hardcoded in the auto-scaler)
const PENDING_RAW: &str = "pending_raw";
// pending as gauge for pipeline
const VERTEX_PENDING_RAW: &str = "pending_messages_raw";

// read batch size as gauge
const READ_BATCH_SIZE: &str = "read_batch_size";

// jetstream isb gauges
const JETSTREAM_ISB_BUFFER_SOFT_USAGE: &str = "buffer_soft_usage";
const JETSTREAM_ISB_BUFFER_SOLID_USAGE: &str = "buffer_solid_usage";
const JETSTREAM_ISB_BUFFER_PENDING: &str = "buffer_pending";
const JETSTREAM_ISB_BUFFER_ACK_PENDING: &str = "buffer_ack_pending";

// processing times as timers
const E2E_TIME: &str = "processing_time";
const READ_TIME: &str = "read_time";
const READ_PROCESSING_TIME: &str = "read_processing_time";
const WRITE_PROCESSING_TIME: &str = "write_processing_time";
const ACK_PROCESSING_TIME: &str = "ack_processing_time";
const TRANSFORMER_PROCESSING_TIME: &str = "transformer_processing_time";
const UDF_PROCESSING_TIME: &str = "udf_processing_time";
const FALLBACK_SINK_WRITE_PROCESSING_TIME: &str = "fbsink_write_processing_time";
const TRANSFORM_TIME: &str = "time";
const ACK_TIME: &str = "ack_time";
const SINK_TIME: &str = "time";
const FALLBACK_SINK_TIME: &str = "time";

// jetstream isb processing times
const JETSTREAM_ISB_READ_TIME_TOTAL: &str = "read_time_total";
const JETSTREAM_ISB_WRITE_TIME_TOTAL: &str = "write_time_total";
const JETSTREAM_ISB_ACK_TIME_TOTAL: &str = "ack_time_total";

/// A deep healthcheck for components. Each component should implement IsReady for both builtins and
/// user-defined containers.
#[derive(Clone)]
pub(crate) enum ComponentHealthChecks<C: crate::typ::NumaflowTypeConfig> {
    Monovertex(Box<MonovertexComponents<C>>),
    Pipeline(Box<PipelineComponents<C>>),
}

/// MonovertexComponents is used to store all the components required for running mvtx. Transformer
/// and Fallback Sink is missing because they are internally referenced by Source and Sink.
#[derive(Clone)]
pub(crate) struct MonovertexComponents<C: crate::typ::NumaflowTypeConfig> {
    pub(crate) source: Source<C>,
    pub(crate) sink: SinkWriter,
}

/// PipelineComponents is used to store the all the components required for running pipeline. Transformer
/// and Fallback Sink is missing because they are internally referenced by Source and Sink.
#[derive(Clone)]
pub(crate) enum PipelineComponents<C: crate::typ::NumaflowTypeConfig> {
    Source(Box<Source<C>>),
    Sink(Box<SinkWriter>),
    Map(MapHandle),
    Reduce(UserDefinedReduce),
}

/// WatermarkFetcherState holds watermark handle and partition count for fetching watermarks
#[derive(Clone)]
pub(crate) struct WatermarkFetcherState {
    pub(crate) watermark_handle: WatermarkHandle,
    pub(crate) partitions: Vec<u16>,
}

/// MetricsState holds both component health checks and optional watermark fetcher state
/// for serving metrics and watermark endpoints
#[derive(Clone)]
pub(crate) struct MetricsState<C: crate::typ::NumaflowTypeConfig> {
    pub(crate) health_checks: ComponentHealthChecks<C>,
    pub(crate) watermark_fetcher_state: Option<WatermarkFetcherState>,
}

/// WatermarkQueryParams represents the query parameters for the /watermark endpoint
#[derive(Deserialize, Debug)]
pub(crate) struct WatermarkQueryParams {
    /// Optional from vertex name to filter watermarks by edge
    pub(crate) from: Option<String>,
}

/// WatermarkResponse represents the response structure for the /watermark endpoint
/// Simple map of partition -> watermark value
#[derive(Serialize, Deserialize, Debug)]
pub(crate) struct WatermarkResponse {
    /// The flatten attribute merges the HashMap's key-value pairs directly into the parent JSON object.
    /// Without flatten: {"partitions": {"0": 123, "1": 456}}
    /// With flatten: {"0": 123, "1": 456}
    #[serde(flatten)]
    pub(crate) partitions: HashMap<String, i64>,
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
    pub(crate) pending_raw: Family<Vec<(String, String)>, Gauge>,
    pub(crate) read_batch_size: Family<Vec<(String, String)>, Gauge>,

    // timers
    pub(crate) e2e_time: Family<Vec<(String, String)>, Histogram>,
    pub(crate) read_time: Family<Vec<(String, String)>, Histogram>,
    pub(crate) ack_time: Family<Vec<(String, String)>, Histogram>,

    pub(crate) transformer: TransformerMetrics,
    pub(crate) sink: SinkMetrics,
    pub(crate) fb_sink: FallbackSinkMetrics,
}

/// PipelineMetrics is a struct which is used for storing the metrics related to the Pipeline
pub(crate) struct PipelineMetrics {
    // generic forwarder metrics
    pub(crate) forwarder: PipelineForwarderMetrics,
    // source forwarder specific metrics
    pub(crate) source_forwarder: SourceForwarderMetrics,
    // sink forwarder specific metrics
    pub(crate) sink_forwarder: SinkForwarderMetrics,
    pub(crate) jetstream_isb: JetStreamISBMetrics,
    pub(crate) pending_raw: Family<Vec<(String, String)>, Gauge>,
}

/// Family of metrics for the sink
pub(crate) struct SinkMetrics {
    pub(crate) write_total: Family<Vec<(String, String)>, Counter>,
    pub(crate) time: Family<Vec<(String, String)>, Histogram>,
    pub(crate) dropped_total: Family<Vec<(String, String)>, Counter>,
    pub(crate) write_errors_total: Family<Vec<(String, String)>, Counter>,
}

/// Family of metrics for the Fallback Sink
pub(crate) struct FallbackSinkMetrics {
    pub(crate) write_total: Family<Vec<(String, String)>, Counter>,
    pub(crate) time: Family<Vec<(String, String)>, Histogram>,
}

/// Family of metrics for the Transformer
pub(crate) struct TransformerMetrics {
    /// Transformer latency
    pub(crate) time: Family<Vec<(String, String)>, Histogram>,
    pub(crate) dropped_total: Family<Vec<(String, String)>, Counter>,
}

/// Generic forwarder metrics
pub(crate) struct PipelineForwarderMetrics {
    // read counters
    pub(crate) read_total: Family<Vec<(String, String)>, Counter>,
    pub(crate) data_read_total: Family<Vec<(String, String)>, Counter>,
    pub(crate) read_bytes_total: Family<Vec<(String, String)>, Counter>,
    pub(crate) data_read_bytes_total: Family<Vec<(String, String)>, Counter>,
    pub(crate) read_error_total: Family<Vec<(String, String)>, Counter>,

    // write counters
    pub(crate) write_total: Family<Vec<(String, String)>, Counter>,
    pub(crate) write_bytes_total: Family<Vec<(String, String)>, Counter>,
    pub(crate) write_error_total: Family<Vec<(String, String)>, Counter>,

    // drop counters
    pub(crate) drop_total: Family<Vec<(String, String)>, Counter>,
    pub(crate) drop_bytes_total: Family<Vec<(String, String)>, Counter>,

    // ack counters
    pub(crate) ack_total: Family<Vec<(String, String)>, Counter>,

    // udf specific counters
    pub(crate) udf_read_total: Family<Vec<(String, String)>, Counter>,
    pub(crate) udf_write_total: Family<Vec<(String, String)>, Counter>,
    pub(crate) udf_drop_total: Family<Vec<(String, String)>, Counter>,
    pub(crate) udf_error_total: Family<Vec<(String, String)>, Counter>,

    // read histograms
    pub(crate) read_processing_time: Family<Vec<(String, String)>, Histogram>,

    // write histograms
    pub(crate) write_processing_time: Family<Vec<(String, String)>, Histogram>,

    // ack histograms
    pub(crate) ack_processing_time: Family<Vec<(String, String)>, Histogram>,

    // forwarder histograms
    pub(crate) e2e_time: Family<Vec<(String, String)>, Histogram>,

    // udf histograms
    pub(crate) udf_processing_time: Family<Vec<(String, String)>, Histogram>,

    // batch size as gauge
    pub(crate) read_batch_size: Family<Vec<(String, String)>, Gauge>,
}

impl PipelineForwarderMetrics {
    pub(crate) fn new() -> Self {
        Self {
            read_total: Family::<Vec<(String, String)>, Counter>::default(),
            data_read_total: Family::<Vec<(String, String)>, Counter>::default(),
            read_bytes_total: Family::<Vec<(String, String)>, Counter>::default(),
            data_read_bytes_total: Family::<Vec<(String, String)>, Counter>::default(),
            read_error_total: Family::<Vec<(String, String)>, Counter>::default(),
            write_total: Family::<Vec<(String, String)>, Counter>::default(),
            write_bytes_total: Family::<Vec<(String, String)>, Counter>::default(),
            write_error_total: Family::<Vec<(String, String)>, Counter>::default(),
            drop_total: Family::<Vec<(String, String)>, Counter>::default(),
            drop_bytes_total: Family::<Vec<(String, String)>, Counter>::default(),
            ack_total: Family::<Vec<(String, String)>, Counter>::default(),
            udf_read_total: Family::<Vec<(String, String)>, Counter>::default(),
            udf_drop_total: Family::<Vec<(String, String)>, Counter>::default(),
            udf_error_total: Family::<Vec<(String, String)>, Counter>::default(),
            udf_write_total: Family::<Vec<(String, String)>, Counter>::default(),
            read_batch_size: Family::<Vec<(String, String)>, Gauge>::default(),
            read_processing_time: Family::<Vec<(String, String)>, Histogram>::new_with_constructor(
                || Histogram::new(exponential_buckets_range(100.0, 60000000.0 * 10.0, 10)),
            ),
            write_processing_time: Family::<Vec<(String, String)>, Histogram>::new_with_constructor(
                || Histogram::new(exponential_buckets_range(100.0, 60000000.0 * 20.0, 10)),
            ),
            ack_processing_time: Family::<Vec<(String, String)>, Histogram>::new_with_constructor(
                || Histogram::new(exponential_buckets_range(100.0, 60000000.0 * 10.0, 10)),
            ),
            udf_processing_time: Family::<Vec<(String, String)>, Histogram>::new_with_constructor(
                || Histogram::new(exponential_buckets_range(100.0, 60000000.0 * 15.0, 10)),
            ),
            e2e_time: Family::<Vec<(String, String)>, Histogram>::new_with_constructor(|| {
                Histogram::new(exponential_buckets_range(100.0, 60000000.0 * 20.0, 10))
            }),
        }
    }
}

pub(crate) struct SourceForwarderMetrics {
    // source transformer counters
    pub(crate) transformer_read_total: Family<Vec<(String, String)>, Counter>,
    pub(crate) transformer_write_total: Family<Vec<(String, String)>, Counter>,
    pub(crate) transformer_error_total: Family<Vec<(String, String)>, Counter>,
    pub(crate) transformer_drop_total: Family<Vec<(String, String)>, Counter>,

    // source transformer histogram
    pub(crate) transformer_processing_time: Family<Vec<(String, String)>, Histogram>,
}

impl SourceForwarderMetrics {
    pub(crate) fn new() -> Self {
        Self {
            transformer_read_total: Family::<Vec<(String, String)>, Counter>::default(),
            transformer_write_total: Family::<Vec<(String, String)>, Counter>::default(),
            transformer_error_total: Family::<Vec<(String, String)>, Counter>::default(),
            transformer_drop_total: Family::<Vec<(String, String)>, Counter>::default(),
            transformer_processing_time:
                Family::<Vec<(String, String)>, Histogram>::new_with_constructor(|| {
                    Histogram::new(exponential_buckets_range(100.0, 60000000.0 * 15.0, 10))
                }),
        }
    }
}

pub(crate) struct SinkForwarderMetrics {
    // fallback sink counters
    pub(crate) fbsink_write_total: Family<Vec<(String, String)>, Counter>,
    pub(crate) fbsink_write_bytes_total: Family<Vec<(String, String)>, Counter>,
    pub(crate) fbsink_write_error_total: Family<Vec<(String, String)>, Counter>,

    // fallback sink histograms
    pub(crate) fbsink_write_processing_time: Family<Vec<(String, String)>, Histogram>,
}

impl SinkForwarderMetrics {
    pub(crate) fn new() -> Self {
        Self {
            fbsink_write_total: Family::<Vec<(String, String)>, Counter>::default(),
            fbsink_write_bytes_total: Family::<Vec<(String, String)>, Counter>::default(),
            fbsink_write_error_total: Family::<Vec<(String, String)>, Counter>::default(),
            fbsink_write_processing_time:
                Family::<Vec<(String, String)>, Histogram>::new_with_constructor(|| {
                    Histogram::new(exponential_buckets_range(100.0, 60000000.0 * 20.0, 10))
                }),
        }
    }
}

pub(crate) struct JetStreamISBMetrics {
    pub(crate) read_error_total: Family<Vec<(String, String)>, Counter>,
    pub(crate) isfull_error_total: Family<Vec<(String, String)>, Counter>,
    pub(crate) isfull_total: Family<Vec<(String, String)>, Counter>,
    pub(crate) write_error_total: Family<Vec<(String, String)>, Counter>,
    pub(crate) write_timeout_total: Family<Vec<(String, String)>, Counter>,

    pub(crate) buffer_soft_usage: Family<Vec<(String, String)>, Gauge<f64, AtomicU64>>,
    pub(crate) buffer_solid_usage: Family<Vec<(String, String)>, Gauge<f64, AtomicU64>>,
    pub(crate) buffer_pending: Family<Vec<(String, String)>, Gauge>,
    pub(crate) buffer_ack_pending: Family<Vec<(String, String)>, Gauge>,

    pub(crate) write_time_total: Family<Vec<(String, String)>, Histogram>,
    pub(crate) read_time_total: Family<Vec<(String, String)>, Histogram>,
    pub(crate) ack_time_total: Family<Vec<(String, String)>, Histogram>,
}

impl JetStreamISBMetrics {
    pub(crate) fn new() -> Self {
        Self {
            read_error_total: Family::<Vec<(String, String)>, Counter>::default(),
            isfull_error_total: Family::<Vec<(String, String)>, Counter>::default(),
            isfull_total: Family::<Vec<(String, String)>, Counter>::default(),
            write_error_total: Family::<Vec<(String, String)>, Counter>::default(),
            write_timeout_total: Family::<Vec<(String, String)>, Counter>::default(),

            buffer_soft_usage: Family::<Vec<(String, String)>, Gauge<f64, AtomicU64>>::default(),
            buffer_solid_usage: Family::<Vec<(String, String)>, Gauge<f64, AtomicU64>>::default(),
            buffer_pending: Family::<Vec<(String, String)>, Gauge>::default(),
            buffer_ack_pending: Family::<Vec<(String, String)>, Gauge>::default(),

            write_time_total: Family::<Vec<(String, String)>, Histogram>::new_with_constructor(
                || Histogram::new(exponential_buckets_range(100.0, 60000000.0 * 2.0, 10)),
            ),
            read_time_total: Family::<Vec<(String, String)>, Histogram>::new_with_constructor(
                || Histogram::new(exponential_buckets_range(100.0, 60000000.0 * 2.0, 10)),
            ),
            ack_time_total: Family::<Vec<(String, String)>, Histogram>::new_with_constructor(
                || Histogram::new(exponential_buckets_range(100.0, 60000000.0 * 2.0, 10)),
            ),
        }
    }
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
            pending_raw: Family::<Vec<(String, String)>, Gauge>::default(),
            read_batch_size: Family::<Vec<(String, String)>, Gauge>::default(),
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
                dropped_total: Family::<Vec<(String, String)>, Counter>::default(),
            },

            sink: SinkMetrics {
                write_total: Family::<Vec<(String, String)>, Counter>::default(),
                time: Family::<Vec<(String, String)>, Histogram>::new_with_constructor(|| {
                    Histogram::new(exponential_buckets_range(100.0, 60000000.0 * 15.0, 10))
                }),
                dropped_total: Family::<Vec<(String, String)>, Counter>::default(),
                write_errors_total: Family::<Vec<(String, String)>, Counter>::default(),
            },

            fb_sink: FallbackSinkMetrics {
                write_total: Family::<Vec<(String, String)>, Counter>::default(),
                time: Family::<Vec<(String, String)>, Histogram>::new_with_constructor(|| {
                    Histogram::new(exponential_buckets_range(100.0, 60000000.0 * 15.0, 10))
                }),
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
            PENDING_RAW,
            "A Gauge to keep track of the total number of source pending messages for the monovtx",
            metrics.pending_raw.clone(),
        );
        registry.register(
            READ_BATCH_SIZE,
            "A Gauge to keep track of the read batch size for monovtx",
            metrics.read_batch_size.clone(),
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
        transformer_registry.register(
            TRANSFORMER_DROPPED_TOTAL,
            "A Counter to keep track of the total number of messages dropped by the transformer",
            metrics.transformer.dropped_total.clone(),
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
        sink_registry.register(
            SINK_WRITE_ERRORS_TOTAL,
            "A counter to keep track of the total number of write errors for the sink",
            metrics.sink.write_errors_total.clone(),
        );
        sink_registry.register(
            SINK_DROPPED_TOTAL,
            "A counter to keep track of the total number of messages dropped by sink",
            metrics.sink.dropped_total.clone(),
        );

        // Fallback Sink metrics
        let fb_sink_registry = registry.sub_registry_with_prefix(FALLBACK_SINK_REGISTRY_PREFIX);

        fb_sink_registry.register(
            FALLBACK_SINK_WRITE_TOTAL,
            "A Counter to keep track of the total number of messages written to the fallback sink",
            metrics.fb_sink.write_total.clone(),
        );
        fb_sink_registry.register(FALLBACK_SINK_TIME,
            "A Histogram to keep track of the total time taken to Write to the fallback sink, in microseconds",
            metrics.fb_sink.time.clone());
        metrics
    }
}

impl PipelineMetrics {
    fn new() -> Self {
        let metrics = Self {
            forwarder: PipelineForwarderMetrics::new(),
            source_forwarder: SourceForwarderMetrics::new(),
            sink_forwarder: SinkForwarderMetrics::new(),
            jetstream_isb: JetStreamISBMetrics::new(),
            pending_raw: Family::<Vec<(String, String)>, Gauge>::default(),
        };
        let mut registry = global_registry().registry.lock();
        Self::register_forwarder_metrics(&metrics, &mut registry);
        Self::register_source_forwarder_metrics(&metrics, &mut registry);
        Self::register_sink_forwarder_metrics(&metrics, &mut registry);
        Self::register_jetstream_isb_metrics(&metrics, &mut registry);
        Self::register_vertex_metrics(&metrics, &mut registry);
        metrics
    }

    fn register_forwarder_metrics(metrics: &Self, registry: &mut Registry) {
        let forwarder_registry = registry.sub_registry_with_prefix("forwarder");
        forwarder_registry.register(
            READ_TOTAL,
            "Total number of Messages Read",
            metrics.forwarder.read_total.clone(),
        );
        forwarder_registry.register(
            READ_BYTES_TOTAL,
            "Total number of bytes read",
            metrics.forwarder.read_bytes_total.clone(),
        );
        forwarder_registry.register(
            DATA_READ_TOTAL,
            "Total number of Data Messages Read",
            metrics.forwarder.data_read_total.clone(),
        );
        forwarder_registry.register(
            DATA_READ_BYTES_TOTAL,
            "Total number of Data message bytes read",
            metrics.forwarder.data_read_bytes_total.clone(),
        );
        forwarder_registry.register(
            READ_ERROR_TOTAL,
            "Total number of Read Errors",
            metrics.forwarder.read_error_total.clone(),
        );
        forwarder_registry.register(
            READ_PROCESSING_TIME,
            "Processing times of read operations (100 microseconds to 10 minutes)",
            metrics.forwarder.read_processing_time.clone(),
        );
        forwarder_registry.register(
            WRITE_TOTAL,
            "Total number of Messages Written",
            metrics.forwarder.write_total.clone(),
        );
        forwarder_registry.register(
            WRITE_BYTES_TOTAL,
            "Total number of Messages Written",
            metrics.forwarder.write_bytes_total.clone(),
        );
        forwarder_registry.register(
            WRITE_ERROR_TOTAL,
            "Total number of Write Errors",
            metrics.forwarder.write_error_total.clone(),
        );
        forwarder_registry.register(
            WRITE_PROCESSING_TIME,
            "Processing times of write operations (100 microseconds to 20 minutes)",
            metrics.forwarder.write_processing_time.clone(),
        );
        forwarder_registry.register(
            PIPELINE_FORWARDER_DROP_TOTAL,
            "Total number of Messages Dropped",
            metrics.forwarder.drop_total.clone(),
        );
        forwarder_registry.register(
            PIPELINE_FORWARDER_DROP_BYTES_TOTAL,
            "Total number of Bytes Dropped",
            metrics.forwarder.drop_bytes_total.clone(),
        );
        forwarder_registry.register(
            ACK_TOTAL,
            "Total number of Messages Acknowledged",
            metrics.forwarder.ack_total.clone(),
        );
        forwarder_registry.register(
            ACK_PROCESSING_TIME,
            "Processing times of acknowledgment operations (100 microseconds to 10 minutes)",
            metrics.forwarder.ack_processing_time.clone(),
        );
        forwarder_registry.register(
            UDF_ERROR_TOTAL,
            "Total number of UDF Errors",
            metrics.forwarder.udf_error_total.clone(),
        );
        forwarder_registry.register(
            E2E_TIME,
            "Processing times (100 microseconds to 20 minutes)",
            metrics.forwarder.e2e_time.clone(),
        );
        forwarder_registry.register(
            UDF_PROCESSING_TIME,
            "Processing times of UDF (100 microseconds to 15 minutes)",
            metrics.forwarder.udf_processing_time.clone(),
        );
        forwarder_registry.register(
            UDF_READ_TOTAL,
            "Total number of Messages Read by UDF",
            metrics.forwarder.udf_read_total.clone(),
        );
        forwarder_registry.register(
            UDF_WRITE_TOTAL,
            "Total number of Messages Written by UDF",
            metrics.forwarder.udf_write_total.clone(),
        );
        forwarder_registry.register(
            UDF_DROP_TOTAL,
            "Total messages dropped by the user",
            metrics.forwarder.udf_drop_total.clone(),
        );
        forwarder_registry.register(
            READ_BATCH_SIZE,
            "A Gauge to keep track of the read batch size for source vtx",
            metrics.forwarder.read_batch_size.clone(),
        );
    }

    fn register_source_forwarder_metrics(metrics: &Self, registry: &mut Registry) {
        // Pipeline source forwarder sub-registry
        let source_forwarder_registry = registry.sub_registry_with_prefix("source_forwarder");

        source_forwarder_registry.register(
            TRANSFORMER_ERROR_TOTAL,
            "Total number of source transformer Errors",
            metrics.source_forwarder.transformer_error_total.clone(),
        );
        source_forwarder_registry.register(
            TRANSFORMER_READ_TOTAL,
            "Total number of Messages Read by source transformer",
            metrics.source_forwarder.transformer_read_total.clone(),
        );
        source_forwarder_registry.register(
            TRANSFORMER_WRITE_TOTAL,
            "Total number of Messages Written by source transformer",
            metrics.source_forwarder.transformer_write_total.clone(),
        );
        source_forwarder_registry.register(
            PIPELINE_TRANSFORMER_DROP_TOTAL,
            "Total number of Messages Dropped by source transformer",
            metrics.source_forwarder.transformer_drop_total.clone(),
        );
        source_forwarder_registry.register(
            TRANSFORMER_PROCESSING_TIME,
            "Processing times of source transformer (100 microseconds to 15 minutes)",
            metrics.source_forwarder.transformer_processing_time.clone(),
        );
    }

    fn register_sink_forwarder_metrics(metrics: &Self, registry: &mut Registry) {
        // Pipeline sink forwarder sub-registry
        let sink_forwarder_registry = registry.sub_registry_with_prefix("forwarder");

        sink_forwarder_registry.register(
            FALLBACK_SINK_WRITE_ERRORS_TOTAL,
            "Total number of Write Errors while writing to a fallback sink",
            metrics.sink_forwarder.fbsink_write_error_total.clone(),
        );
        sink_forwarder_registry.register(
            PIPELINE_FALLBACK_SINK_WRITE_TOTAL,
            "Total number of Messages written to a fallback sink",
            metrics.sink_forwarder.fbsink_write_total.clone(),
        );
        sink_forwarder_registry.register(
            PIPELINE_FALLBACK_SINK_WRITE_BYTES_TOTAL,
            "Total number of bytes written to a fallback sink",
            metrics.sink_forwarder.fbsink_write_bytes_total.clone(),
        );
        sink_forwarder_registry.register(
            FALLBACK_SINK_WRITE_PROCESSING_TIME,
            "Processing times of write operations to a fallback sink (100 microseconds to 20 minutes)",
            metrics.sink_forwarder.fbsink_write_processing_time.clone(),
        );
    }

    fn register_jetstream_isb_metrics(metrics: &Self, registry: &mut Registry) {
        // Pipeline JetStream ISB sub-registry
        let jetstream_isb_registry = registry.sub_registry_with_prefix("isb_jetstream");
        jetstream_isb_registry.register(
            JETSTREAM_ISB_READ_ERROR_TOTAL,
            "Total number of jetstream read errors",
            metrics.jetstream_isb.read_error_total.clone(),
        );
        jetstream_isb_registry.register(
            JETSTREAM_ISB_ISFULL_ERROR_TOTAL,
            "Total number of jetstream isFull errors",
            metrics.jetstream_isb.isfull_error_total.clone(),
        );
        jetstream_isb_registry.register(
            JETSTREAM_ISB_ISFULL_TOTAL,
            "Total number of IsFull",
            metrics.jetstream_isb.isfull_total.clone(),
        );
        jetstream_isb_registry.register(
            JETSTREAM_ISB_WRITE_ERROR_TOTAL,
            "Total number of jetstream write errors",
            metrics.jetstream_isb.write_error_total.clone(),
        );
        jetstream_isb_registry.register(
            JETSTREAM_ISB_WRITE_TIMEOUT_TOTAL,
            "Total number of jetstream write timeouts",
            metrics.jetstream_isb.write_timeout_total.clone(),
        );
        // isbSoftUsage is indicative of the buffer that is used up, it is calculated based on the messages in pending + ack pending
        jetstream_isb_registry.register(
            JETSTREAM_ISB_BUFFER_SOFT_USAGE,
            "Percentage of buffer soft usage",
            metrics.jetstream_isb.buffer_soft_usage.clone(),
        );
        // isbSolidUsage is indicative of buffer that is used up, it is calculated based on the messages remaining in the stream (if it's not Limits retention policy)
        jetstream_isb_registry.register(
            JETSTREAM_ISB_BUFFER_SOLID_USAGE,
            "Percentage of buffer solid usage",
            metrics.jetstream_isb.buffer_solid_usage.clone(),
        );
        jetstream_isb_registry.register(
            JETSTREAM_ISB_BUFFER_PENDING,
            "Number of pending messages",
            metrics.jetstream_isb.buffer_pending.clone(),
        );
        jetstream_isb_registry.register(
            JETSTREAM_ISB_BUFFER_ACK_PENDING,
            "Number of messages pending ack",
            metrics.jetstream_isb.buffer_ack_pending.clone(),
        );
        jetstream_isb_registry.register(
            JETSTREAM_ISB_WRITE_TIME_TOTAL,
            "Processing times of Writes for jetstream",
            metrics.jetstream_isb.write_time_total.clone(),
        );
        jetstream_isb_registry.register(
            JETSTREAM_ISB_READ_TIME_TOTAL,
            "Processing times of Reads for jetstream",
            metrics.jetstream_isb.read_time_total.clone(),
        );
        jetstream_isb_registry.register(
            JETSTREAM_ISB_ACK_TIME_TOTAL,
            "Processing times of Acks for jetstream",
            metrics.jetstream_isb.ack_time_total.clone(),
        );
    }

    fn register_vertex_metrics(metrics: &Self, registry: &mut Registry) {
        // Pipeline vertex sub-registry
        let vertex_registry = registry.sub_registry_with_prefix("vertex");
        vertex_registry.register(
            VERTEX_PENDING_RAW,
            "Total number of pending messages",
            metrics.pending_raw.clone(),
        );
    }
}

/// MONOVTX_METRICS is the MonoVtxMetrics object which stores the metrics
static MONOVTX_METRICS: OnceLock<MonoVtxMetrics> = OnceLock::new();

// monovertex_metrics is a helper function used to fetch the
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

static PIPELINE_METRIC_LABELS: OnceLock<Vec<(String, String)>> = OnceLock::new();

pub(crate) fn pipeline_metric_labels(vertex_type: &str) -> &'static Vec<(String, String)> {
    PIPELINE_METRIC_LABELS.get_or_init(|| {
        vec![
            (
                PIPELINE_VERTEX_LABEL.to_string(),
                get_vertex_name().to_string(),
            ),
            (
                PIPELINE_NAME_LABEL.to_string(),
                get_pipeline_name().to_string(),
            ),
            (
                PIPELINE_VERTEX_TYPE_LABEL.to_string(),
                vertex_type.to_string(),
            ),
            (
                PIPELINE_REPLICA_LABEL.to_string(),
                get_vertex_replica().to_string(),
            ),
        ]
    })
}

/// drop metric labels which can be due to buffer-full and retry strategy,
/// or due to conditional forwarding rules not being met, or user setting "to_drop"
pub(crate) fn pipeline_drop_metric_labels(
    vertex_type: &str,
    partition_name: &str,
    reason: &str,
) -> Vec<(String, String)> {
    vec![
        (
            PIPELINE_VERTEX_LABEL.to_string(),
            get_vertex_name().to_string(),
        ),
        (
            PIPELINE_NAME_LABEL.to_string(),
            get_pipeline_name().to_string(),
        ),
        (
            PIPELINE_VERTEX_TYPE_LABEL.to_string(),
            vertex_type.to_string(),
        ),
        (
            PIPELINE_REPLICA_LABEL.to_string(),
            get_vertex_replica().to_string(),
        ),
        (
            PIPELINE_PARTITION_NAME_LABEL.to_string(),
            partition_name.to_string(),
        ),
        (PIPELINE_DROP_REASON_LABEL.to_string(), reason.to_string()),
    ]
}

pub(crate) fn jetstream_isb_metrics_labels(buffer_name: &str) -> Vec<(String, String)> {
    vec![("buffer".to_string(), buffer_name.to_string())]
}
pub(crate) fn jetstream_isb_error_metrics_labels(
    buffer_name: &str,
    reason: String,
) -> Vec<(String, String)> {
    vec![
        ("buffer".to_string(), buffer_name.to_string()),
        ("reason".to_string(), reason),
    ]
}

/// metrics_handler is used to generate and return a snapshot of the
/// current state of the metrics in the global registry
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

// watermark_handler is used to fetch and return watermark information
// for all partitions based on available watermark handles.
// Optionally accepts a 'from' query parameter to filter watermarks by edge (from vertex).
pub async fn watermark_handler<C: crate::typ::NumaflowTypeConfig>(
    State(state): State<MetricsState<C>>,
    Query(params): Query<WatermarkQueryParams>,
) -> impl IntoResponse {
    let Some(watermark_fetcher_state) = &state.watermark_fetcher_state else {
        return Response::builder()
            .status(StatusCode::NOT_FOUND)
            .header(axum::http::header::CONTENT_TYPE, "application/json")
            .body(Body::from(
                r#"{"error": "Watermark not available for this vertex type"}"#,
            ))
            .unwrap();
    };

    let mut partitions = HashMap::new();
    match &watermark_fetcher_state.watermark_handle {
        // For source watermark handle, always fetch only partition 0
        // Source vertices don't have a 'from' vertex, so ignore the query parameter
        WatermarkHandle::Source(source_handle) => {
            let handle_clone = source_handle.clone();
            let watermark = handle_clone.fetch_head_watermark(0).await;
            partitions.insert("0".to_string(), watermark.timestamp_millis());
        }

        // For ISB vertices, fetch watermarks for all partitions
        // If 'from' query parameter is provided, fetch watermarks only for that specific edge
        WatermarkHandle::ISB(isb_handle) => {
            let handle_clone = isb_handle.clone();

            // For reduce vertices, only return partition 0 since they read from single partition
            // For other vertex types, fetch watermarks for all partitions
            // watermark_fetcher_state.partition_count will already be set to 1 for reduce vertex
            for partition_idx in watermark_fetcher_state.partitions.iter() {
                let watermark = handle_clone
                    .fetch_head_watermark(params.from.as_deref(), *partition_idx)
                    .await;
                partitions.insert(partition_idx.to_string(), watermark.timestamp_millis());
            }
        }
    }

    let response = WatermarkResponse { partitions };
    let json_response = serde_json::to_string(&response)
        .unwrap_or_else(|_| r#"{"error": "Failed to serialize watermark response"}"#.to_string());

    debug!(?json_response, ?params.from, "Watermark response");
    Response::builder()
        .status(StatusCode::OK)
        .header(axum::http::header::CONTENT_TYPE, "application/json")
        .body(Body::from(json_response))
        .unwrap()
}

pub(crate) async fn start_metrics_https_server<C: crate::typ::NumaflowTypeConfig>(
    addr: SocketAddr,
    metrics_state: MetricsState<C>,
) -> crate::Result<()> {
    // Setup the CryptoProvider (controls core cryptography used by rustls) for the process
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

    // Generate a self-signed certificate
    let CertifiedKey { cert, signing_key } = generate_simple_self_signed(vec!["localhost".into()])
        .map_err(|e| Error::Metrics(format!("Generating self-signed certificate: {e}")))?;

    let tls_config = RustlsConfig::from_pem(cert.pem().into(), signing_key.serialize_pem().into())
        .await
        .map_err(|e| Error::Metrics(format!("Creating tlsConfig from pem: {e}")))?;

    let metrics_app = metrics_router(metrics_state);

    axum_server::bind_rustls(addr, tls_config)
        .serve(metrics_app.into_make_service())
        .await
        .map_err(|e| Error::Metrics(format!("Starting web server for metrics: {e}")))?;

    Ok(())
}

/// router for metrics and k8s health endpoints
fn metrics_router<C: crate::typ::NumaflowTypeConfig>(metrics_state: MetricsState<C>) -> Router {
    Router::new()
        .route("/metrics", get(metrics_handler))
        .route("/runtime/watermark", get(watermark_handler))
        .route("/livez", get(livez))
        .route("/readyz", get(sidecar_livez))
        .route("/sidecar-livez", get(sidecar_livez))
        .with_state(metrics_state)
}

async fn livez() -> impl IntoResponse {
    StatusCode::NO_CONTENT
}

async fn sidecar_livez<C: crate::typ::NumaflowTypeConfig>(
    State(state): State<MetricsState<C>>,
) -> impl IntoResponse {
    // Check if health checks are disabled via the environment variable
    if env::var("NUMAFLOW_HEALTH_CHECK_DISABLED")
        .map(|v| v.eq_ignore_ascii_case("true"))
        .unwrap_or(false)
    {
        return StatusCode::NO_CONTENT;
    }
    match state.health_checks {
        ComponentHealthChecks::Monovertex(mut monovertex_state) => {
            // this call also check the health of transformer if it is configured in the Source.
            if !monovertex_state.source.ready().await {
                error!("Monovertex source component is not ready");
                return StatusCode::INTERNAL_SERVER_ERROR;
            }
            if !monovertex_state.sink.ready().await {
                error!("Monovertex sink client is not ready");
                return StatusCode::INTERNAL_SERVER_ERROR;
            }
        }
        ComponentHealthChecks::Pipeline(pipeline_state) => match *pipeline_state {
            PipelineComponents::Source(mut source) => {
                if !source.ready().await {
                    error!("Pipeline source component is not ready");
                    return StatusCode::INTERNAL_SERVER_ERROR;
                }
            }
            PipelineComponents::Sink(mut sink) => {
                // this call also check for fbsink if it is there
                if !sink.ready().await {
                    error!("Pipeline sink component is not ready");
                    return StatusCode::INTERNAL_SERVER_ERROR;
                }
            }
            PipelineComponents::Map(mut map) => {
                if !map.ready().await {
                    error!("Pipeline map component is not ready");
                    return StatusCode::INTERNAL_SERVER_ERROR;
                }
            }
            PipelineComponents::Reduce(reducer) => match reducer {
                UserDefinedReduce::Aligned(mut reducer) => {
                    if !reducer.ready().await {
                        error!("Pipeline aligned reduce is not ready");
                        return StatusCode::INTERNAL_SERVER_ERROR;
                    }
                }
                UserDefinedReduce::Unaligned(reducer) => match reducer {
                    UserDefinedUnalignedReduce::Accumulator(mut reducer) => {
                        if !reducer.ready().await {
                            error!("Pipeline accumulator reduce component is not ready");
                            return StatusCode::INTERNAL_SERVER_ERROR;
                        }
                    }
                    UserDefinedUnalignedReduce::Session(mut reducer) => {
                        if !reducer.ready().await {
                            error!("Pipeline session reduce component is not ready");
                            return StatusCode::INTERNAL_SERVER_ERROR;
                        }
                    }
                },
            },
        },
    }
    StatusCode::NO_CONTENT
}

#[derive(Clone)]
pub(crate) enum LagReader<C: crate::typ::NumaflowTypeConfig> {
    Source(Box<Source<C>>),
    #[allow(clippy::upper_case_acronyms)]
    ISB(Vec<ISBReader<C>>), // multiple partitions
}

/// PendingReader is responsible for periodically checking the lag of the reader
/// and exposing the metrics.
pub(crate) struct PendingReader<C: crate::typ::NumaflowTypeConfig> {
    lag_reader: LagReader<C>,
    lag_checking_interval: Duration,
}

pub(crate) struct PendingReaderTasks {
    expose_handle: JoinHandle<()>,
}

/// PendingReaderBuilder is used to build a [LagReader] instance.
pub(crate) struct PendingReaderBuilder<C: crate::typ::NumaflowTypeConfig> {
    lag_reader: LagReader<C>,
    lag_checking_interval: Option<Duration>,
}

impl<C: crate::typ::NumaflowTypeConfig> PendingReaderBuilder<C> {
    pub(crate) fn new(lag_reader: LagReader<C>) -> Self {
        Self {
            lag_reader,
            lag_checking_interval: None,
        }
    }

    pub(crate) fn lag_checking_interval(mut self, interval: Duration) -> Self {
        self.lag_checking_interval = Some(interval);
        self
    }
    pub(crate) fn build(self) -> PendingReader<C> {
        PendingReader {
            lag_reader: self.lag_reader,
            lag_checking_interval: self
                .lag_checking_interval
                .unwrap_or_else(|| Duration::from_secs(3)),
        }
    }
}

impl<C: crate::typ::NumaflowTypeConfig> PendingReader<C> {
    /// Starts the lag reader by spawning task to expose pending metrics for daemon server.
    /// Dropping the PendingReaderTasks will abort the background tasks.
    pub async fn start(&self, is_mono_vertex: bool) -> PendingReaderTasks {
        let lag_checking_interval = self.lag_checking_interval;

        let lag_reader = self.lag_reader.clone();
        let expose_handle = tokio::spawn(async move {
            expose_pending_metrics(lag_reader, lag_checking_interval, is_mono_vertex).await;
        });
        PendingReaderTasks { expose_handle }
    }
}

/// When the PendingReaderTasks is dropped, we need to clean up the pending exposer and the pending builder tasks.
impl Drop for PendingReaderTasks {
    fn drop(&mut self) {
        self.expose_handle.abort();
        info!("Stopped the Lag-Reader Expose tasks");
    }
}

// Periodically exposes the pending metrics by calculating the average pending messages over different intervals.
async fn expose_pending_metrics<C: crate::typ::NumaflowTypeConfig>(
    mut lag_reader: LagReader<C>,
    lag_checking_interval: Duration,
    is_mono_vertex: bool,
) {
    let mut ticker = time::interval(lag_checking_interval);
    // print pending messages every one minute
    let mut last_logged = std::time::Instant::now();

    loop {
        ticker.tick().await;

        match &mut lag_reader {
            LagReader::Source(source) => match fetch_source_pending::<C>(source).await {
                Ok(pending) => {
                    if last_logged.elapsed().as_secs() >= 60 {
                        info!(
                            "Pending messages {:?}",
                            if pending != -1 { Some(pending) } else { None }
                        );
                        last_logged = std::time::Instant::now();
                    } else {
                        debug!(
                            "Pending messages {:?}",
                            if pending != -1 { Some(pending) } else { None }
                        );
                    }
                    if is_mono_vertex {
                        let metric_labels = mvtx_forward_metric_labels().clone();
                        monovertex_metrics()
                            .pending_raw
                            .get_or_create(&metric_labels)
                            .set(pending);
                    } else {
                        let mut metric_labels = pipeline_metric_labels(VERTEX_TYPE_SOURCE).clone();
                        metric_labels.push((
                            PIPELINE_PARTITION_NAME_LABEL.to_string(),
                            "Source".to_string(),
                        ));
                        pipeline_metrics()
                            .pending_raw
                            .get_or_create(&metric_labels)
                            .set(pending);
                    }
                }
                Err(err) => {
                    error!("Failed to get pending messages: {:?}", err);
                }
            },
            LagReader::ISB(readers) => {
                for reader in readers {
                    match fetch_isb_pending(reader).await {
                        Ok(pending) => {
                            if last_logged.elapsed().as_secs() >= 60 {
                                info!(
                                    "Pending messages {:?}, partition: {}",
                                    if pending != -1 { Some(pending) } else { None },
                                    reader.name(),
                                );
                                last_logged = std::time::Instant::now();
                            } else {
                                debug!(
                                    "Pending messages {:?}, partition: {}",
                                    if pending != -1 { Some(pending) } else { None },
                                    reader.name(),
                                );
                            }
                            let mut metric_labels = pipeline_metric_labels(reader.name()).clone();
                            metric_labels.push((
                                PIPELINE_PARTITION_NAME_LABEL.to_string(),
                                reader.name().to_string(),
                            ));
                            pipeline_metrics()
                                .pending_raw
                                .get_or_create(&metric_labels)
                                .set(pending);
                        }
                        Err(err) => {
                            error!("Failed to get pending messages: {:?}", err);
                        }
                    }
                }
            }
        }
    }
}

async fn fetch_source_pending<C: crate::typ::NumaflowTypeConfig>(
    lag_reader: &Source<C>,
) -> crate::error::Result<i64> {
    let response: i64 = lag_reader.pending().await?.map_or(-1, |p| p as i64); // default to -1(unavailable)
    Ok(response)
}

async fn fetch_isb_pending<C: crate::typ::NumaflowTypeConfig>(
    reader: &mut JetStreamReader<C>,
) -> crate::error::Result<i64> {
    let response: i64 = reader.pending().await?.map_or(-1, |p| p as i64); // default to -1(unavailable)
    Ok(response)
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;

    use super::*;
    use crate::shared::grpc::create_rpc_channel;
    use crate::sinker::sink::{SinkClientType, SinkWriterBuilder};
    use crate::source::SourceType;
    use crate::source::user_defined::new_source;
    use crate::tracker::Tracker;
    use numaflow::shared::ServerExtras;
    use numaflow::source::{Message, Offset, SourceReadRequest};
    use numaflow::{sink, source, sourcetransform};
    use numaflow_pb::clients::sink::sink_client::SinkClient;
    use numaflow_pb::clients::source::source_client::SourceClient;
    use tokio::sync::mpsc::Sender;
    use tokio_util::sync::CancellationToken;

    struct SimpleSource;
    #[tonic::async_trait]
    impl source::Sourcer for SimpleSource {
        async fn read(&self, _: SourceReadRequest, _: Sender<Message>) {}

        async fn ack(&self, _: Vec<Offset>) {}

        async fn nack(&self, _offsets: Vec<Offset>) {}

        async fn pending(&self) -> Option<usize> {
            Some(0)
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
        let cln_token = CancellationToken::new();
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

        let src_client = SourceClient::new(create_rpc_channel(src_sock_file).await.unwrap());
        let (src_read, src_ack, lag_reader) = new_source(
            src_client,
            5,
            Duration::from_millis(1000),
            cln_token.clone(),
            true,
        )
        .await
        .expect("Failed to create source reader");

        let tracker = Tracker::new(None, CancellationToken::new());
        let source = Source::new(
            5,
            SourceType::UserDefinedSource(Box::new(src_read), Box::new(src_ack), lag_reader),
            tracker.clone(),
            true,
            None,
            None,
            None,
        )
        .await;

        let sink_writer = SinkWriterBuilder::new(
            10,
            Duration::from_millis(100),
            SinkClientType::UserDefined(SinkClient::new(
                create_rpc_channel(sink_sock_file).await.unwrap(),
            )),
        )
        .fb_sink_client(SinkClientType::UserDefined(SinkClient::new(
            create_rpc_channel(fb_sink_sock_file).await.unwrap(),
        )))
        .build()
        .await
        .expect("failed to create sink writer");

        let metrics_state: MetricsState<crate::typ::WithoutRateLimiter> = MetricsState {
            health_checks: ComponentHealthChecks::Monovertex(Box::new(MonovertexComponents {
                source,
                sink: sink_writer,
            })),
            watermark_fetcher_state: None,
        };

        let addr: SocketAddr = "127.0.0.1:9091".parse().unwrap();
        let metrics_state_clone = metrics_state.clone();
        let server_handle = tokio::spawn(async move {
            start_metrics_https_server::<crate::typ::WithoutRateLimiter>(addr, metrics_state_clone)
                .await
                .unwrap();
        });

        // invoke the sidecar-livez endpoint
        let response = sidecar_livez::<crate::typ::WithoutRateLimiter>(State(metrics_state)).await;
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
            2.563_629_645_795_620_6e7,
            1.518_968_953_341_724_6e8,
            8.999_999_999_999_983e8,
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
        let jetstream_isb_labels = jetstream_isb_metrics_labels("test_jetstream_isb");
        let jetstream_isb_error_labels =
            jetstream_isb_error_metrics_labels("test_jetstream_isb", "test_error".to_string());
        global_metrics.sdk_info.get_or_create(&sdk_labels).set(1);

        let metrics = monovertex_metrics();
        let pipeline_metrics = pipeline_metrics();

        let common_pipeline_labels = vec![
            (PIPELINE_NAME_LABEL.to_string(), "test-pipeline".to_string()),
            (PIPELINE_VERTEX_LABEL.to_string(), "test-vertex".to_string()),
            (
                PIPELINE_VERTEX_TYPE_LABEL.to_string(),
                "test-vertex-type".to_string(),
            ),
            (
                PIPELINE_REPLICA_LABEL.to_string(),
                "test-replica".to_string(),
            ),
        ];

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
        metrics.e2e_time.get_or_create(&common_labels).observe(10.0);
        metrics.read_time.get_or_create(&common_labels).observe(3.0);
        metrics.ack_time.get_or_create(&common_labels).observe(2.0);

        metrics
            .transformer
            .time
            .get_or_create(&common_labels)
            .observe(5.0);
        metrics
            .transformer
            .dropped_total
            .get_or_create(&common_labels)
            .inc_by(2);

        metrics.sink.write_total.get_or_create(&common_labels).inc();
        metrics
            .sink
            .write_errors_total
            .get_or_create(&common_labels)
            .inc_by(3);
        metrics
            .sink
            .dropped_total
            .get_or_create(&common_labels)
            .inc_by(2);
        metrics.sink.time.get_or_create(&common_labels).observe(4.0);

        metrics
            .fb_sink
            .write_total
            .get_or_create(&common_labels)
            .inc();
        metrics
            .fb_sink
            .time
            .get_or_create(&common_labels)
            .observe(5.0);

        // pipeline forwarder metrics
        pipeline_metrics
            .forwarder
            .read_total
            .get_or_create(&common_pipeline_labels)
            .inc_by(10);

        pipeline_metrics
            .forwarder
            .ack_processing_time
            .get_or_create(&common_pipeline_labels)
            .observe(5.0);

        // populate jetstream isb metrics
        pipeline_metrics
            .jetstream_isb
            .buffer_pending
            .get_or_create(&jetstream_isb_labels)
            .set(5);
        pipeline_metrics
            .jetstream_isb
            .buffer_soft_usage
            .get_or_create(&jetstream_isb_labels)
            .set(0.22);
        pipeline_metrics
            .jetstream_isb
            .write_error_total
            .get_or_create(&jetstream_isb_error_labels)
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
            r#"monovtx_transformer_dropped_total{mvtx_name="test-monovertex-metric-names",mvtx_replica="3"} 2"#,
            r#"monovtx_sink_write_total{mvtx_name="test-monovertex-metric-names",mvtx_replica="3"} 1"#,
            r#"monovtx_sink_time_sum{mvtx_name="test-monovertex-metric-names",mvtx_replica="3"} 4.0"#,
            r#"monovtx_sink_time_count{mvtx_name="test-monovertex-metric-names",mvtx_replica="3"} 1"#,
            r#"monovtx_sink_time_bucket{le="100.0",mvtx_name="test-monovertex-metric-names",mvtx_replica="3"} 1"#,
            r#"monovtx_sink_write_errors_total{mvtx_name="test-monovertex-metric-names",mvtx_replica="3"} 3"#,
            r#"monovtx_sink_dropped_total{mvtx_name="test-monovertex-metric-names",mvtx_replica="3"} 2"#,
            r#"monovtx_fallback_sink_write_total{mvtx_name="test-monovertex-metric-names",mvtx_replica="3"} 1"#,
            r#"monovtx_fallback_sink_time_sum{mvtx_name="test-monovertex-metric-names",mvtx_replica="3"} 5.0"#,
            r#"monovtx_fallback_sink_time_count{mvtx_name="test-monovertex-metric-names",mvtx_replica="3"} 1"#,
            r#"monovtx_fallback_sink_time_bucket{le="100.0",mvtx_name="test-monovertex-metric-names",mvtx_replica="3"} 1"#,
            r#"forwarder_read_total{pipeline="test-pipeline",vertex="test-vertex",vertex_type="test-vertex-type",replica="test-replica"} 10"#,
            r#"forwarder_ack_processing_time_sum{pipeline="test-pipeline",vertex="test-vertex",vertex_type="test-vertex-type",replica="test-replica"} 5.0"#,
            r#"forwarder_ack_processing_time_count{pipeline="test-pipeline",vertex="test-vertex",vertex_type="test-vertex-type",replica="test-replica"} 1"#,
            r#"forwarder_ack_processing_time_bucket{le="100.0",pipeline="test-pipeline",vertex="test-vertex",vertex_type="test-vertex-type",replica="test-replica"} 1"#,
            r#"isb_jetstream_write_error_total{buffer="test_jetstream_isb",reason="test_error"} 1"#,
            r#"isb_jetstream_buffer_soft_usage{buffer="test_jetstream_isb"} 0.22"#,
            r#"isb_jetstream_buffer_pending{buffer="test_jetstream_isb"} 5"#,
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
