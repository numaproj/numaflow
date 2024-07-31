use std::{future::ready, time::Instant};

use axum::{
    extract::{MatchedPath, Request},
    middleware::Next,
    response::Response,
    routing::get,
    Router,
};
use metrics::{counter, histogram};
use metrics_exporter_prometheus::{Matcher, PrometheusBuilder, PrometheusHandle};
use tokio::net::{TcpListener, ToSocketAddrs};
use tracing::debug;

/// Collect and emit prometheus metrics.

/// Metrics router and server
pub(crate) async fn start_metrics_server<A>(addr: A) -> crate::Result<()>
where
    A: ToSocketAddrs + std::fmt::Debug,
{
    // setup_metrics_recorder should only be invoked once
    let recorder_handle = setup_metrics_recorder()?;
    let metrics_app = Router::new().route("/metrics", get(move || ready(recorder_handle.render())));

    let listener = TcpListener::bind(&addr)
        .await
        .map_err(|e| format!("Creating listener on {:?}: {}", addr, e))?;

    debug!("metrics server started at addr: {:?}", addr);

    axum::serve(listener, metrics_app)
        .await
        .map_err(|e| format!("Starting web server for metrics: {}", e))?;
    Ok(())
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
            Matcher::Full("http_requests_duration_micros".to_string()),
            &log_to_power_of_sqrt2_bins,
        )
        .map_err(|e| format!("Prometheus set_buckets_for_metric: {}", e))?
        .install_recorder()
        .map_err(|e| format!("Prometheus install_recorder: {}", e))?;
    Ok(prometheus_handle)
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

    let labels = [("method", method), ("path", path), ("status", status)];

    histogram!("http_requests_duration_micros", &labels).record(latency);

    counter!("http_requests_total", &labels).increment(1);

    response
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;
    use std::time::Duration;

    use axum::body::Body;
    use axum::http::{HeaderMap, StatusCode};
    use axum::middleware;
    use tokio::time::sleep;
    use tower::ServiceExt;

    use super::*;

    #[tokio::test]
    async fn test_start_metrics_server() {
        let addr = SocketAddr::from(([127, 0, 0, 1], 0));
        let server = tokio::spawn(async move {
            let result = start_metrics_server(addr).await;
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
