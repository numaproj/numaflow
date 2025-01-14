use std::net::SocketAddr;
use std::time::Duration;

use tokio::task::JoinHandle;
use tracing::error;

use crate::config::components::metrics::MetricsConfig;
use crate::metrics::{
    start_metrics_https_server, PendingReader, PendingReaderBuilder, UserDefinedContainerState,
};
use crate::pipeline::isb::jetstream::reader::JetstreamReader;
use crate::source::Source;

/// Starts the metrics server
pub(crate) async fn start_metrics_server(
    metrics_config: MetricsConfig,
    metrics_state: UserDefinedContainerState,
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
pub(crate) async fn create_source_pending_reader(
    metrics_config: &MetricsConfig,
    lag_reader: Source,
) -> PendingReader {
    PendingReaderBuilder::new()
        .lag_checking_interval(Duration::from_secs(
            metrics_config.lag_check_interval_in_secs.into(),
        ))
        .refresh_interval(Duration::from_secs(
            metrics_config.lag_refresh_interval_in_secs.into(),
        ))
        .lookback_seconds(metrics_config.lookback_window_in_secs)
        .source_lag_reader(lag_reader)
        .build()
}

pub(crate) async fn create_isb_pending_reader(
    metrics_config: &MetricsConfig,
    lag_readers: Vec<JetstreamReader>,
) -> PendingReader {
    PendingReaderBuilder::new()
        .lag_checking_interval(Duration::from_secs(
            metrics_config.lag_check_interval_in_secs.into(),
        ))
        .refresh_interval(Duration::from_secs(
            metrics_config.lag_refresh_interval_in_secs.into(),
        ))
        .lookback_seconds(metrics_config.lookback_window_in_secs)
        .isb_lag_readers(lag_readers)
        .build()
}
