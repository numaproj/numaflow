mod app;
mod config;
mod error;
/// Runtime to persist the runtime information of the pod (e.g. application errors)
// FIXME: merge runtime and runtime errors, keep one orchestrator to put and get the errors from the filesystem
// pass that orchestrator using axum state to the handler
pub mod runtime;
mod runtime_errors;

use crate::app::start_main_server;
use crate::config::generate_certs;
use crate::config::server::MonitorServerConfig;
use crate::error::{Error, Result};
use axum_server::tls_rustls::RustlsConfig;
use std::net::SocketAddr;
use tracing::info;

pub async fn run() -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
    let (cert, key) = generate_certs()?;

    let tls_config = RustlsConfig::from_pem(cert.pem().into(), key.serialize_pem().into())
        .await
        .map_err(|e| format!("Failed to create tls config {:?}", e))?;

    let server_config = MonitorServerConfig::default();

    info!(?server_config, "Starting monitor server with config");

    // Start the monitor server which serves daemon server calls
    let app_addr: SocketAddr = format!("0.0.0.0:{}", server_config.server_listen_port)
        .parse()
        .map_err(|e| Error::Init(format!("{e:?}")))?;
    // Start the main server, which serves the application.
    start_main_server(app_addr, tls_config, server_config).await?;

    Ok(())
}
