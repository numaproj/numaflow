use axum::{routing::get, Router};
use log::info;
use metrics::{describe_counter, describe_histogram};
use metrics_exporter_prometheus::{Matcher, PrometheusBuilder, PrometheusHandle};
use std::future::ready;
use std::time::Duration;
use tokio::net::{TcpListener, ToSocketAddrs};
use tokio::sync::mpsc;
use tokio::time;
use tracing::{debug, error};

use crate::error::Error;
use crate::source::SourceClient;

// Define the labels for the metrics
pub const VERTEX_LABEL: &str = "vertex";
pub const PIPELINE_LABEL: &str = "pipeline";
pub const REPLICA_LABEL: &str = "replica";
pub const PARTITION_LABEL: &str = "partition_name";
pub const VERTEX_TYPE_LABEL: &str = "vertex_type";

// Define the metrics
pub const FORWARDER_READ_TOTAL: &str = "forwarder_read_total";
pub const FORWARDER_READ_LATENCY: &str = "forwarder_read_time";
pub const FORWARDER_READ_BYTES_TOTAL: &str = "forwarder_read_bytes_total";
pub const FORWARDER_TRANSFORMER_LATENCY: &str = "forwarder_transformer_time";

pub const FORWARDER_LATENCY: &str = "forwarder_forward_chunk_processing_time";
pub const FORWARDER_ACK_TOTAL: &str = "forwarder_ack_total";
pub const FORWARDER_ACK_LATENCY: &str = "forwarder_ack_time";
pub const FORWARDER_WRITE_TOTAL: &str = "forwarder_write_total";
pub const FORWARDER_WRITE_LATENCY: &str = "forwarder_write_time";

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

    // Define forwarder metrics
    describe_counter!(
        FORWARDER_READ_TOTAL,
        "Total number of Data Messages Read in the forwarder"
    );
    describe_counter!(
        FORWARDER_READ_BYTES_TOTAL,
        "Total number of bytes read in the forwarder"
    );
    describe_histogram!(
        FORWARDER_READ_LATENCY,
        "Latency of one read iteration in the forwarder"
    );
    describe_histogram!(
        FORWARDER_TRANSFORMER_LATENCY,
        "Latency of one transform iteration in the forwarder"
    );
    describe_histogram!(
        FORWARDER_LATENCY,
        "Latency of one forward a chunk iteration in the forwarder"
    );
    describe_counter!(
        FORWARDER_ACK_TOTAL,
        "Total number of acknowledgments by the forwarder"
    );
    describe_histogram!(
        FORWARDER_ACK_LATENCY,
        "Latency of one ack iteration in the forwarder"
    );
    describe_counter!(
        FORWARDER_WRITE_TOTAL,
        "Total number of Data Messages written by the forwarder"
    );
    describe_histogram!(
        FORWARDER_WRITE_LATENCY,
        "Latency of one write iteration in the forwarder"
    );
    Ok(prometheus_handle)
}

// LagReader is responsible for reading pending information from
// the source client and exposing it as metrics.
#[allow(dead_code)]
pub(crate) struct LagReader {
    source_client: SourceClient,
    pending_stats: Vec<TimestampedPending>,
}

#[allow(dead_code)]
impl LagReader {
    pub(crate) fn new(source_client: SourceClient) -> Self {
        Self {
            source_client,
            pending_stats: Vec::new(),
        }
    }

    pub(crate) async fn buildup_pending_info(
        &mut self,
        mut shutdown_rx: mpsc::Receiver<()>,
        lag_checking_interval: Duration,
    ) {
        let mut ticker = time::interval(lag_checking_interval);
        loop {
            tokio::select! {
                _ = shutdown_rx.recv() => {
                    return;
                }
                _ = ticker.tick() => {
                    match self.source_client.pending_fn().await {
                        Ok(pending) => {
                            if pending != -1 {
                                let ts = TimestampedPending { pending, timestamp: std::time::Instant::now() };
                                self.pending_stats.push(ts);
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

    pub(crate) async fn expose_pending_metrics(
        &self,
        mut shutdown_rx: mpsc::Receiver<()>,
        refresh_interval: Duration,
    ) {
        let mut ticker = time::interval(refresh_interval);
        let lookback_seconds_map = vec![("1m", 60), ("5m", 300), ("15m", 900)];
        loop {
            tokio::select! {
                _ = shutdown_rx.recv() => {
                    return;
                }
                _ = ticker.tick() => {
                    for (label, seconds) in &lookback_seconds_map {
                        let pending = self.calculate_pending(*seconds).await;
                        if pending != -1 {
                            info!("Pending messages ({}): {}", label, pending);
                        }
                    }
                }
            }
        }
    }

    async fn calculate_pending(&self, seconds: i64) -> i64 {
        let mut result = -1;
        let mut total = 0;
        let mut num = 0;
        let now = std::time::Instant::now();

        for item in self.pending_stats.iter().rev() {
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
}

struct TimestampedPending {
    pending: i64,
    timestamp: std::time::Instant,
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
