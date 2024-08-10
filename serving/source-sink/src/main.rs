use std::net::SocketAddr;

use tokio::signal;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::level_filters::LevelFilter;
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

use sourcer_sinker::config::config;
use sourcer_sinker::metrics::start_metrics_https_server;
use sourcer_sinker::run_forwarder;
use sourcer_sinker::sink::SinkConfig;
use sourcer_sinker::source::SourceConfig;
use sourcer_sinker::transformer::TransformerConfig;

#[tokio::main]
async fn main() {
    // Initialize the logger
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::builder()
                .with_default_directive(LevelFilter::INFO.into())
                .parse_lossy(&config().log_level),
        )
        .with_target(false)
        .init();

    // Start the metrics server, which server the prometheus metrics.
    // TODO: make the port configurable.
    let metrics_addr: SocketAddr = "0.0.0.0:2469".parse().expect("Invalid address");

    // Start the metrics server in a separate background async spawn,
    // This should be running throughout the lifetime of the application, hence the handle is not
    // joined.
    let metrics_server_handle = tokio::spawn(async move {
        if let Err(e) = start_metrics_https_server(metrics_addr).await {
            error!("Metrics server error: {:?}", e);
        }
    });

    // Initialize the source, sink and transformer configurations
    // We are using the default configurations for now.
    let source_config = SourceConfig {
        max_message_size: config().grpc_max_message_size,
        ..Default::default()
    };

    let sink_config = SinkConfig {
        max_message_size: config().grpc_max_message_size,
        ..Default::default()
    };
    let transformer_config = if config().is_transformer_enabled {
        Some(TransformerConfig {
            max_message_size: config().grpc_max_message_size,
            ..Default::default()
        })
    } else {
        None
    };

    let cln_token = CancellationToken::new();
    let shutdown_cln_token = cln_token.clone();
    let shutdown_handle: JoinHandle<sourcer_sinker::error::Result<()>> = tokio::spawn(async move {
        shutdown_signal().await;
        shutdown_cln_token.cancel();
        Ok(())
    });

    // Run the forwarder
    if let Err(e) = run_forwarder(source_config, sink_config, transformer_config, cln_token).await {
        error!("Application error: {:?}", e);
    }

    if !shutdown_handle.is_finished() {
        shutdown_handle.abort();
    }

    if !metrics_server_handle.is_finished() {
        metrics_server_handle.abort();
    }
    info!("Gracefully Exiting...");
}

async fn shutdown_signal() {
    let ctrl_c = async {
        signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
        info!("Received Ctrl+C signal");
    };

    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("failed to install signal handler")
            .recv()
            .await;
        info!("Received terminate signal");
    };

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
}
