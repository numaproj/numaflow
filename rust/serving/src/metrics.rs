use std::net::SocketAddr;
use std::sync::OnceLock;
use std::time::Instant;

use axum::body::Body;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::{
    extract::{MatchedPath, Request},
    middleware::Next,
    response::Response,
    routing::get,
    Router,
};
use axum_server::tls_rustls::RustlsConfig;
use prometheus_client::encoding::text::encode;
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::family::Family;
use prometheus_client::metrics::histogram::{exponential_buckets, Histogram};
use prometheus_client::registry::Registry;
use tracing::debug;

use crate::Error::MetricsServer;

// Define the labels for the metrics
pub const SERVING_METHOD_LABEL: &str = "method";
pub const SERVING_PATH_LABEL: &str = "path";
const SERVING_STATUS_LABEL: &str = "status";

// Define the metrics
const HTTP_REQUESTS_TOTAL: &str = "http_requests_count";
const HTTP_REQUESTS_DURATION: &str = "http_requests_duration";

#[derive(Default)]
pub struct GlobalRegistry {
    pub registry: parking_lot::Mutex<Registry>,
}

impl GlobalRegistry {
    fn new() -> Self {
        GlobalRegistry {
            registry: parking_lot::Mutex::new(Registry::default()),
        }
    }
}

static GLOBAL_REGISTER: OnceLock<GlobalRegistry> = OnceLock::new();

fn global_registry() -> &'static GlobalRegistry {
    GLOBAL_REGISTER.get_or_init(GlobalRegistry::new)
}

pub(crate) struct ServingMetrics {
    pub(crate) http_requests_count: Family<Vec<(String, String)>, Counter>,
    pub(crate) http_requests_duration: Family<Vec<(String, String)>, Histogram>,

    pub(crate) cb_store_register_count: Counter,
    pub(crate) cb_store_register_fail_count: Counter,
    pub(crate) cb_store_register_duplicate_count: Counter,
    pub(crate) cb_store_register_duration: Histogram,

    pub(crate) payload_save_duration: Histogram,
    pub(crate) datum_retrive_duration: Histogram,
    pub(crate) processing_time: Histogram,
}

impl ServingMetrics {
    fn new() -> Self {
        let http_requests_total = Family::<Vec<(String, String)>, Counter>::default();
        let http_requests_duration =
            Family::<Vec<(String, String)>, Histogram>::new_with_constructor(|| {
                Histogram::new(exponential_buckets(0.001, 2.0, 10))
            });

        let cb_store_register_count = Counter::default();
        let cb_store_register_fail_count = Counter::default();
        let cb_store_register_duplicate_count = Counter::default();

        let cb_store_register_duration = Histogram::new(exponential_buckets(0.001, 2.0, 10));

        let payload_save_duration = Histogram::new(exponential_buckets(0.001, 2.0, 10));
        let datum_retrive_duration = Histogram::new(exponential_buckets(0.001, 2.0, 10));

        let processing_time = Histogram::new(exponential_buckets(1.0, 2.0, 10));

        let metrics = Self {
            http_requests_count: http_requests_total,
            http_requests_duration,
            cb_store_register_count,
            cb_store_register_fail_count,
            cb_store_register_duplicate_count,
            cb_store_register_duration,
            payload_save_duration,
            datum_retrive_duration,
            processing_time,
        };

        let mut registry = global_registry().registry.lock();

        registry.register(
            HTTP_REQUESTS_TOTAL,
            "A Counter to keep track of the total number of HTTP requests",
            metrics.http_requests_count.clone(),
        );
        registry.register(
            HTTP_REQUESTS_DURATION,
            "A Histogram to keep track of the duration of HTTP requests",
            metrics.http_requests_duration.clone(),
        );

        registry.register(
            "CALLBACK_STORE_REGISTER",
            "A Counter to keep track of the number of callback store register requests",
            metrics.cb_store_register_count.clone(),
        );

        registry.register(
            "CALLBACK_STORE_REGISTER_FAIL",
            "A Counter to keep track of the number of failed callback store register requests",
            metrics.cb_store_register_fail_count.clone(),
        );

        registry.register(
            "CALLBACK_STORE_REGISTER_DUPLICATES",
            "A Counter to keep track of the number of failed callback store register requests due to duplicate request id",
            metrics.cb_store_register_fail_count.clone(),
        );

        registry.register(
            "CALLBACK_STORE_REGISTER_DURATION",
            "A Histogram to keep track of the duration of the successful callback store register requests",
            metrics.cb_store_register_duration.clone(),
        );

        registry.register(
            "PAYLOAD_JESTREAM_SAVE_DURATION",
            "A Histogram to keep track of the time it takes to save request payload to Jestream",
            metrics.payload_save_duration.clone(),
        );

        registry.register(
            "DATUM_RETRIEVE_DURATION",
            "A Histogram to keep track of the latency for retrieving pipeline result from datum store",
            metrics.datum_retrive_duration.clone(),
        );

        registry.register(
            "PROCESSING_TIME",
            "A Histogram to keep track of the pipeline processing time of a request payload",
            metrics.processing_time.clone(),
        );

        metrics
    }
}

static SERVING_METRICS: OnceLock<ServingMetrics> = OnceLock::new();

pub(crate) fn serving_metrics() -> &'static ServingMetrics {
    SERVING_METRICS.get_or_init(ServingMetrics::new)
}

pub(crate) async fn start_https_metrics_server(
    addr: SocketAddr,
    tls_config: RustlsConfig,
) -> crate::Result<()> {
    let metrics_app = Router::new().route("/metrics", get(metrics_handler));

    tracing::info!(?addr, "Starting metrics server");
    axum_server::bind_rustls(addr, tls_config)
        .serve(metrics_app.into_make_service())
        .await
        .map_err(|e| MetricsServer(format!("Starting web server for metrics: {}", e)))?;

    Ok(())
}

// metrics_handler is used to generate and return a snapshot of the
// current state of the metrics in the global registry
pub async fn metrics_handler() -> impl IntoResponse {
    let state = global_registry().registry.lock();
    let mut buffer = String::new();
    encode(&mut buffer, &state).unwrap();
    debug!("Exposing Metrics: {:?}", buffer);
    axum::http::Response::builder()
        .status(StatusCode::OK)
        .body(Body::from(buffer))
        .unwrap()
}

/// Emit request metrics and also latency metrics.
pub(crate) async fn capture_metrics(request: Request, next: Next) -> Response {
    let start = Instant::now();

    // get path (can be matched too)
    let path = if let Some(matched_path) = request.extensions().get::<MatchedPath>() {
        matched_path.as_str().to_owned()
    } else {
        request.uri().path().to_owned()
    };

    let method = request.method().to_string();

    let response = next.run(request).await;

    let latency = start.elapsed().as_micros() as f64;
    let status = response.status().as_u16().to_string();

    let labels = vec![
        (SERVING_METHOD_LABEL.to_string(), method),
        (SERVING_PATH_LABEL.to_string(), path),
        (SERVING_STATUS_LABEL.to_string(), status),
    ];

    serving_metrics()
        .http_requests_duration
        .get_or_create(&labels)
        .observe(latency);

    serving_metrics()
        .http_requests_count
        .get_or_create(&labels)
        .inc();

    response
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;
    use std::time::Duration;
    use std::usize;

    use axum::body::Body;
    use axum::http::{HeaderMap, StatusCode};
    use axum::middleware;
    use tokio::time::sleep;
    use tower::ServiceExt;

    use super::*;
    use crate::config::generate_certs;

    type Result<T> = core::result::Result<T, Error>;
    type Error = Box<dyn std::error::Error>;

    #[tokio::test]
    async fn test_start_metrics_server() -> Result<()> {
        // Setup the CryptoProvider (controls core cryptography used by rustls) for the process
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
        let (cert, key) = generate_certs()?;

        let tls_config = RustlsConfig::from_pem(cert.pem().into(), key.serialize_pem().into())
            .await
            .unwrap();

        let addr = SocketAddr::from(([127, 0, 0, 1], 0));
        let server = tokio::spawn(async move {
            let result = start_https_metrics_server(addr, tls_config).await;
            assert!(result.is_ok())
        });

        // Give the server a little bit of time to start
        sleep(Duration::from_millis(100)).await;

        // Stop the server
        server.abort();
        Ok(())
    }

    #[tokio::test]
    async fn test_capture_metrics() {
        async fn handle(_headers: HeaderMap) -> String {
            "Hello, World!".to_string()
        }

        let app = Router::new()
            .route("/", get(handle))
            .layer(middleware::from_fn(capture_metrics));

        let res = app
            .oneshot(Request::builder().uri("/").body(Body::empty()).unwrap())
            .await
            .unwrap();

        assert_eq!(res.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_metric_names() {
        let labels = vec![
            (SERVING_METHOD_LABEL.to_string(), "GET".to_string()),
            (
                SERVING_PATH_LABEL.to_string(),
                "/v1/process/fetch".to_string(),
            ),
            (SERVING_STATUS_LABEL.to_string(), "200".to_string()),
        ];

        serving_metrics()
            .http_requests_duration
            .get_or_create(&labels)
            .observe(200 as f64);

        serving_metrics()
            .http_requests_count
            .get_or_create(&labels)
            .inc();

        let metrics = metrics_handler().await.into_response().into_body();
        let metrics = axum::body::to_bytes(metrics, usize::MAX).await.unwrap();
        let metrics = String::from_utf8_lossy(&metrics);

        assert_eq!(
            metrics.trim(),
            r###"
# HELP http_requests_count A Counter to keep track of the total number of HTTP requests.
# TYPE http_requests_count counter
http_requests_count_total{method="GET",path="/v1/process/fetch",status="200"} 1
# HELP http_requests_duration A Histogram to keep track of the duration of HTTP requests.
# TYPE http_requests_duration histogram
http_requests_duration_sum{method="GET",path="/v1/process/fetch",status="200"} 200.0
http_requests_duration_count{method="GET",path="/v1/process/fetch",status="200"} 1
http_requests_duration_bucket{le="0.001",method="GET",path="/v1/process/fetch",status="200"} 0
http_requests_duration_bucket{le="0.002",method="GET",path="/v1/process/fetch",status="200"} 0
http_requests_duration_bucket{le="0.004",method="GET",path="/v1/process/fetch",status="200"} 0
http_requests_duration_bucket{le="0.008",method="GET",path="/v1/process/fetch",status="200"} 0
http_requests_duration_bucket{le="0.016",method="GET",path="/v1/process/fetch",status="200"} 0
http_requests_duration_bucket{le="0.032",method="GET",path="/v1/process/fetch",status="200"} 0
http_requests_duration_bucket{le="0.064",method="GET",path="/v1/process/fetch",status="200"} 0
http_requests_duration_bucket{le="0.128",method="GET",path="/v1/process/fetch",status="200"} 0
http_requests_duration_bucket{le="0.256",method="GET",path="/v1/process/fetch",status="200"} 0
http_requests_duration_bucket{le="0.512",method="GET",path="/v1/process/fetch",status="200"} 0
http_requests_duration_bucket{le="+Inf",method="GET",path="/v1/process/fetch",status="200"} 1
# HELP CALLBACK_STORE_REGISTER A Counter to keep track of the number of callback store register requests.
# TYPE CALLBACK_STORE_REGISTER counter
CALLBACK_STORE_REGISTER_total 0
# HELP CALLBACK_STORE_REGISTER_FAIL A Counter to keep track of the number of failed callback store register requests.
# TYPE CALLBACK_STORE_REGISTER_FAIL counter
CALLBACK_STORE_REGISTER_FAIL_total 0
# HELP CALLBACK_STORE_REGISTER_DUPLICATES A Counter to keep track of the number of failed callback store register requests due to duplicate request id.
# TYPE CALLBACK_STORE_REGISTER_DUPLICATES counter
CALLBACK_STORE_REGISTER_DUPLICATES_total 0
# HELP CALLBACK_STORE_REGISTER_DURATION A Histogram to keep track of the duration of the successful callback store register requests.
# TYPE CALLBACK_STORE_REGISTER_DURATION histogram
CALLBACK_STORE_REGISTER_DURATION_sum 0.0
CALLBACK_STORE_REGISTER_DURATION_count 0
CALLBACK_STORE_REGISTER_DURATION_bucket{le="0.001"} 0
CALLBACK_STORE_REGISTER_DURATION_bucket{le="0.002"} 0
CALLBACK_STORE_REGISTER_DURATION_bucket{le="0.004"} 0
CALLBACK_STORE_REGISTER_DURATION_bucket{le="0.008"} 0
CALLBACK_STORE_REGISTER_DURATION_bucket{le="0.016"} 0
CALLBACK_STORE_REGISTER_DURATION_bucket{le="0.032"} 0
CALLBACK_STORE_REGISTER_DURATION_bucket{le="0.064"} 0
CALLBACK_STORE_REGISTER_DURATION_bucket{le="0.128"} 0
CALLBACK_STORE_REGISTER_DURATION_bucket{le="0.256"} 0
CALLBACK_STORE_REGISTER_DURATION_bucket{le="0.512"} 0
CALLBACK_STORE_REGISTER_DURATION_bucket{le="+Inf"} 0
# HELP PAYLOAD_JESTREAM_SAVE_DURATION A Histogram to keep track of the time it takes to save request payload to Jestream.
# TYPE PAYLOAD_JESTREAM_SAVE_DURATION histogram
PAYLOAD_JESTREAM_SAVE_DURATION_sum 0.0
PAYLOAD_JESTREAM_SAVE_DURATION_count 0
PAYLOAD_JESTREAM_SAVE_DURATION_bucket{le="0.001"} 0
PAYLOAD_JESTREAM_SAVE_DURATION_bucket{le="0.002"} 0
PAYLOAD_JESTREAM_SAVE_DURATION_bucket{le="0.004"} 0
PAYLOAD_JESTREAM_SAVE_DURATION_bucket{le="0.008"} 0
PAYLOAD_JESTREAM_SAVE_DURATION_bucket{le="0.016"} 0
PAYLOAD_JESTREAM_SAVE_DURATION_bucket{le="0.032"} 0
PAYLOAD_JESTREAM_SAVE_DURATION_bucket{le="0.064"} 0
PAYLOAD_JESTREAM_SAVE_DURATION_bucket{le="0.128"} 0
PAYLOAD_JESTREAM_SAVE_DURATION_bucket{le="0.256"} 0
PAYLOAD_JESTREAM_SAVE_DURATION_bucket{le="0.512"} 0
PAYLOAD_JESTREAM_SAVE_DURATION_bucket{le="+Inf"} 0
# HELP DATUM_RETRIEVE_DURATION A Histogram to keep track of the latency for retrieving pipeline result from datum store.
# TYPE DATUM_RETRIEVE_DURATION histogram
DATUM_RETRIEVE_DURATION_sum 0.0
DATUM_RETRIEVE_DURATION_count 0
DATUM_RETRIEVE_DURATION_bucket{le="0.001"} 0
DATUM_RETRIEVE_DURATION_bucket{le="0.002"} 0
DATUM_RETRIEVE_DURATION_bucket{le="0.004"} 0
DATUM_RETRIEVE_DURATION_bucket{le="0.008"} 0
DATUM_RETRIEVE_DURATION_bucket{le="0.016"} 0
DATUM_RETRIEVE_DURATION_bucket{le="0.032"} 0
DATUM_RETRIEVE_DURATION_bucket{le="0.064"} 0
DATUM_RETRIEVE_DURATION_bucket{le="0.128"} 0
DATUM_RETRIEVE_DURATION_bucket{le="0.256"} 0
DATUM_RETRIEVE_DURATION_bucket{le="0.512"} 0
DATUM_RETRIEVE_DURATION_bucket{le="+Inf"} 0
# HELP PROCESSING_TIME A Histogram to keep track of the pipeline processing time of a request payload.
# TYPE PROCESSING_TIME histogram
PROCESSING_TIME_sum 0.0
PROCESSING_TIME_count 0
PROCESSING_TIME_bucket{le="1.0"} 0
PROCESSING_TIME_bucket{le="2.0"} 0
PROCESSING_TIME_bucket{le="4.0"} 0
PROCESSING_TIME_bucket{le="8.0"} 0
PROCESSING_TIME_bucket{le="16.0"} 0
PROCESSING_TIME_bucket{le="32.0"} 0
PROCESSING_TIME_bucket{le="64.0"} 0
PROCESSING_TIME_bucket{le="128.0"} 0
PROCESSING_TIME_bucket{le="256.0"} 0
PROCESSING_TIME_bucket{le="512.0"} 0
PROCESSING_TIME_bucket{le="+Inf"} 0
# EOF
        "###
            .trim()
        );
    }
}
