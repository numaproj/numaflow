use std::env;
use std::net::SocketAddr;

use tracing::error;

use sourcer_sinker::sink::SinkConfig;
use sourcer_sinker::source::SourceConfig;
use sourcer_sinker::transformer::TransformerConfig;
use sourcer_sinker::{metrics::start_metrics_server, run_forwarder};

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    // Start the metrics server
    // TODO: make the port configurable.
    let metrics_addr: SocketAddr = "0.0.0.0:9090".parse().expect("Invalid address");
    tokio::spawn(async move {
        if let Err(e) = start_metrics_server(metrics_addr).await {
            error!("Metrics server error: {:?}", e);
        }
    });

    let source_config = SourceConfig::default();
    let sink_config = SinkConfig::default();
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
