use std::net::SocketAddr;

use tracing::error;

use sourcer_sinker::{metrics::start_metrics_server, run_forwarder};

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    // Start the metrics server
    let metrics_addr: SocketAddr = "0.0.0.0:9090".parse().expect("Invalid address");
    tokio::spawn(async move {
        if let Err(e) = start_metrics_server(metrics_addr).await {
            error!("Metrics server error: {:?}", e);
        }
    });

    // Run the forwarder
    if let Err(e) = run_forwarder(None).await {
        error!("Application error: {:?}", e);
    }
}
