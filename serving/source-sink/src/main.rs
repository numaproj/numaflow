use sourcer_sinker::config::config;
use sourcer_sinker::metrics::start_metrics_https_server;
use sourcer_sinker::run_forwarder;
use sourcer_sinker::sink::SinkConfig;
use sourcer_sinker::source::SourceConfig;
use sourcer_sinker::transformer::TransformerConfig;
use std::env;
use std::net::SocketAddr;
use tracing::level_filters::LevelFilter;
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() {
    let log_level = env::var("NUMAFLOW_DEBUG").unwrap_or_else(|_| LevelFilter::INFO.to_string());
    // Initialize the logger
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::builder()
                .with_default_directive(LevelFilter::INFO.into())
                .parse_lossy(log_level),
        )
        .with_target(false)
        .init();

    // Start the metrics server, which server the prometheus metrics.
    // TODO: make the port configurable.
    let metrics_addr: SocketAddr = "0.0.0.0:2469".parse().expect("Invalid address");

    // Start the metrics server in a separate background async spawn,
    // This should be running throughout the lifetime of the application, hence the handle is not
    // joined.
    tokio::spawn(async move {
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

    // Run the forwarder
    if let Err(e) = run_forwarder(source_config, sink_config, transformer_config, None).await {
        error!("Application error: {:?}", e);
    }

    info!("Gracefully Exiting...");
}
