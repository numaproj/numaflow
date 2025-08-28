use std::net::SocketAddr;
use std::time::Duration;

use tokio::task::JoinHandle;
use tracing::error;

use crate::config::components::metrics::MetricsConfig;
use crate::metrics::{
    LagReader, MetricsState, PendingReader, PendingReaderBuilder, start_metrics_https_server,
};

/// Starts the metrics server
pub(crate) async fn start_metrics_server<C: crate::typ::NumaflowTypeConfig>(
    metrics_config: MetricsConfig,
    metrics_state: MetricsState<C>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        // Start the metrics server, which server the prometheus metrics.
        let metrics_addr: SocketAddr =
            format!("0.0.0.0:{}", metrics_config.metrics_server_listen_port)
                .parse()
                .expect("Invalid address");

        if let Err(e) = start_metrics_https_server(metrics_addr, metrics_state).await {
            error!("metrics server error: {:?}", e);
        }
    })
}

/// Creates a pending reader
pub(crate) async fn create_pending_reader<C: crate::typ::NumaflowTypeConfig>(
    metrics_config: &MetricsConfig,
    lag_reader: LagReader<C>,
) -> PendingReader<C> {
    PendingReaderBuilder::new(lag_reader)
        .lag_checking_interval(Duration::from_secs(
            metrics_config.lag_check_interval_in_secs.into(),
        ))
        .build()
}
