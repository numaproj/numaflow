use crate::Error::MetricsServer;
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
use std::net::SocketAddr;
use std::sync::OnceLock;
use std::time::Instant;
use tracing::debug;

// Define the labels for the metrics
pub const SERVING_METHOD_LABEL: &str = "method";
pub const SERVING_PATH_LABEL: &str = "path";
const SERVING_STATUS_LABEL: &str = "status";

// Define the metrics
const HTTP_REQUESTS_TOTAL: &str = "http_requests_total";
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

pub struct ServingMetrics {
    pub http_requests_total: Family<Vec<(String, String)>, Counter>,
    pub http_requests_duration: Family<Vec<(String, String)>, Histogram>,
}

impl ServingMetrics {
    fn new() -> Self {
        let http_requests_total = Family::<Vec<(String, String)>, Counter>::default();
        let http_requests_duration =
            Family::<Vec<(String, String)>, Histogram>::new_with_constructor(|| {
                Histogram::new(exponential_buckets(0.001, 2.0, 20))
            });

        let metrics = Self {
            http_requests_total,
            http_requests_duration,
        };

        let mut registry = global_registry().registry.lock();

        registry.register(
            HTTP_REQUESTS_TOTAL,
            "A Counter to keep track of the total number of HTTP requests",
            metrics.http_requests_total.clone(),
        );
        registry.register(
            HTTP_REQUESTS_DURATION,
            "A Histogram to keep track of the duration of HTTP requests",
            metrics.http_requests_duration.clone(),
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
        .http_requests_total
        .get_or_create(&labels)
        .inc();

    response
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;
    use std::time::Duration;

    use super::*;
    use crate::config::cert_key_pair;
    use axum::body::Body;
    use axum::http::{HeaderMap, StatusCode};
    use axum::middleware;
    use tokio::time::sleep;
    use tower::ServiceExt;

    #[tokio::test]
    async fn test_start_metrics_server() {
        let (cert, key) = cert_key_pair();

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
}
