use std::net::SocketAddr;
use std::time::Duration;

use tokio_util::task::AbortOnDropHandle;
use tracing::error;

use crate::config::components::metrics::MetricsConfig;
use crate::metrics::{
    LagReader, MetricsState, PendingReader, PendingReaderBuilder, create_metrics_tls_config,
    start_metrics_https_server,
};
use crate::{Error, Result};

/// Starts the metrics server
pub(crate) async fn start_metrics_server(
    metrics_config: MetricsConfig,
    metrics_state: MetricsState,
) -> Result<AbortOnDropHandle<()>> {
    let metrics_addr: SocketAddr = format!("0.0.0.0:{}", metrics_config.metrics_server_listen_port)
        .parse()
        .expect("Invalid address");
    let listener = std::net::TcpListener::bind(metrics_addr)
        .map_err(|e| Error::Metrics(format!("Binding metrics server to {metrics_addr}: {e}")))?;
    let tls_config = create_metrics_tls_config().await?;

    Ok(AbortOnDropHandle::new(tokio::spawn(async move {
        // Serve Prometheus metrics and health endpoints.
        if let Err(e) = start_metrics_https_server(listener, tls_config, metrics_state).await {
            error!("metrics server error: {:?}", e);
        }
    })))
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

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn metrics_server_bind_failure_is_returned() {
        let listener = std::net::TcpListener::bind("0.0.0.0:0").unwrap();
        let config = MetricsConfig {
            metrics_server_listen_port: listener.local_addr().unwrap().port(),
            ..Default::default()
        };

        let result = start_metrics_server(config, MetricsState::new()).await;

        assert!(matches!(result, Err(Error::Metrics(_))));
    }
}
