use std::future::ready;

use axum::{routing::get, Router};
use metrics::describe_counter;
use metrics_exporter_prometheus::{Matcher, PrometheusBuilder, PrometheusHandle};
use tokio::net::{TcpListener, ToSocketAddrs};
use tracing::debug;

use crate::error::Error;
/// Collect and emit prometheus metrics.
/// Metrics router and server
pub async fn start_metrics_server<A>(addr: A) -> crate::Result<()>
where
    A: ToSocketAddrs + std::fmt::Debug,
{
    // setup_metrics_recorder should only be invoked once
    let recorder_handle = setup_metrics_recorder()?;
    let metrics_app = Router::new().route("/metrics", get(move || ready(recorder_handle.render())));

    let listener = TcpListener::bind(&addr)
        .await
        .map_err(|e| Error::MetricsError(format!("Creating listener on {:?}: {}", addr, e)))?;

    debug!("metrics server started at addr: {:?}", addr);

    axum::serve(listener, metrics_app)
        .await
        .map_err(|e| Error::MetricsError(format!("Starting web server for metrics: {}", e)))?;
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
            Matcher::Full("fac_total_duration_micros".to_string()), // fac == forward-a-chunk
            &log_to_power_of_sqrt2_bins,
        )
        .map_err(|e| Error::MetricsError(format!("Prometheus install_recorder: {}", e)))?
        .install_recorder()
        .map_err(|e| Error::MetricsError(format!("Prometheus install_recorder: {}", e)))?;

    // Define the metrics
    describe_counter!("data_read_total", "Total number of Data Messages Read");
    describe_counter!("read_bytes_total", "Total number of bytes read");

    Ok(prometheus_handle)
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
            let result = start_metrics_server(addr).await;
            assert!(result.is_ok())
        });

        // Give the server a little bit of time to start
        sleep(Duration::from_millis(100)).await;

        // Stop the server
        server.abort();
    }
}
