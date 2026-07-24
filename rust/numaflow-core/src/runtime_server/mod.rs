//! Runtime error persistence and HTTPS endpoint.

mod app;
pub mod config;
mod error;
pub mod runtime;

use std::net::SocketAddr;

use axum_server::tls_rustls::RustlsConfig;
use config::{MonitorServerConfig, generate_certs};
use error::{Error, Result};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::info;

/// Spawns the HTTPS runtime-errors server on port 2470 in a background task.
pub async fn spawn_runtime_errors_server(cln_token: CancellationToken) -> Result<JoinHandle<()>> {
    let (cert, key) =
        generate_certs().map_err(|e| Error::Init(format!("Certificate generation failed: {e}")))?;
    let tls_config = RustlsConfig::from_pem(cert.pem().into(), key.serialize_pem().into())
        .await
        .map_err(|e| Error::Init(format!("Failed to create tls config {e:?}")))?;

    let server_config = MonitorServerConfig::default();
    info!(?server_config, "Starting runtime errors server");

    let app_addr: SocketAddr = format!("0.0.0.0:{}", server_config.server_listen_port)
        .parse()
        .map_err(|e| Error::Init(format!("{e:?}")))?;

    let handle = tokio::spawn(async move {
        if let Err(e) = app::start_main_server(app_addr, tls_config, server_config, cln_token).await
        {
            tracing::error!(?e, "runtime errors server exited with error");
        }
    });

    Ok(handle)
}
