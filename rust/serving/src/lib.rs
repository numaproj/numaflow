pub use self::error::{Error, Result};
use crate::app::start_main_server;
use crate::config::{cert_key_pair, config};
use crate::metrics::start_https_metrics_server;
use crate::pipeline::min_pipeline_spec;
use axum_server::tls_rustls::RustlsConfig;
use std::net::SocketAddr;
use tracing::info;

mod app;
mod config;
mod consts;
mod error;
mod metrics;
mod pipeline;

pub async fn serve() -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let (cert, key) = cert_key_pair();

    let tls_config = RustlsConfig::from_pem(cert.pem().into(), key.serialize_pem().into())
        .await
        .map_err(|e| format!("Failed to create tls config {:?}", e))?;

    info!(config = ?config(), pipeline_spec = ? min_pipeline_spec(), "Starting server with config and pipeline spec");

    // Start the metrics server, which serves the prometheus metrics.
    let metrics_addr: SocketAddr =
        format!("0.0.0.0:{}", &config().metrics_server_listen_port).parse()?;

    let metrics_server_handle =
        tokio::spawn(start_https_metrics_server(metrics_addr, tls_config.clone()));

    let app_addr: SocketAddr = format!("0.0.0.0:{}", &config().app_listen_port).parse()?;

    // Start the main server, which serves the application.
    let app_server_handle = tokio::spawn(start_main_server(app_addr, tls_config));

    // TODO: is try_join the best? we need to short-circuit at the first failure
    tokio::try_join!(flatten(app_server_handle), flatten(metrics_server_handle))?;

    Ok(())
}

async fn flatten<T>(handle: tokio::task::JoinHandle<Result<T>>) -> Result<T> {
    match handle.await {
        Ok(Ok(result)) => Ok(result),
        Ok(Err(err)) => Err(err),
        Err(err) => Err(Error::Other(format!("Spawning the server: {err:?}"))),
    }
}
