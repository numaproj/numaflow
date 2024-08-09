use std::env;
use std::net::SocketAddr;

use log::info;
use tracing::error;
use tracing::level_filters::LevelFilter;
use tracing_subscriber::EnvFilter;

use sourcer_sinker::sink::SinkConfig;
use sourcer_sinker::source::SourceConfig;
use sourcer_sinker::transformer::TransformerConfig;
use sourcer_sinker::{metrics::start_metrics_server, run_forwarder};

#[tokio::main]
async fn main() {
    let log_level = env::var("NUMAFLOW_DEBUG").unwrap_or_else(|_| "info".to_string());

    // Initialize the logger
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::builder()
                .with_default_directive(LevelFilter::INFO.into())
                .parse_lossy(log_level),
        )
        .with_target(false)
        .init();

    info!("Starting the forwarder");
    // Start the metrics server, which server the prometheus metrics.
    // TODO: make the port configurable.
    let metrics_addr: SocketAddr = "0.0.0.0:9090".parse().expect("Invalid address");

    // Start the metrics server in a separate background async spawn,
    // This should be running throughout the lifetime of the application, hence the handle is not
    // joined.
    tokio::spawn(async move {
        if let Err(e) = start_metrics_server(metrics_addr).await {
            error!("Metrics server error: {:?}", e);
        }
    });

    // Initialize the source, sink and transformer configurations
    // We are using the default configurations for now.
    // TODO: Make these configurations configurable or we see them not changing?
    let source_config = SourceConfig::default();
    let sink_config = SinkConfig::default();
    // TODO: We should decide transformer is enabled based on the mono vertex spec
    let transformer_config = if env::var("NUMAFLOW_TRANSFORMER").is_ok() {
        Some(TransformerConfig::default())
    } else {
        None
    };

    // Run the forwarder
    if let Err(e) = run_forwarder(source_config, sink_config, transformer_config, None).await {
        error!("Application error: {:?}", e);
    }
}
