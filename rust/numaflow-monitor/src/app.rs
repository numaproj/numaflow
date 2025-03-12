use axum::{routing::get, Router};
use axum_server::{tls_rustls::RustlsConfig, Handle};
use std::{net::SocketAddr, time::Duration};
use tokio::signal;
use tower_http::trace::TraceLayer;
use tracing::info;

use crate::{error::Error, runtime_errors::handle_runtime_app_errors, MonitorServerConfig};

/// Start the main application Router and the axum server.
pub(crate) async fn start_main_server(
    app_addr: SocketAddr,
    tls_config: RustlsConfig,
    server_config: MonitorServerConfig,
) -> crate::Result<()> {
    let handle = Handle::new();
    // Spawn a task to gracefully shutdown server.
    tokio::spawn(graceful_shutdown(handle.clone(), server_config));

    info!(?app_addr, "Starting monitor app server");

    let router = monitor_router();

    axum_server::bind_rustls(app_addr, tls_config)
        .handle(handle)
        .serve(router.into_make_service())
        .await
        .map_err(|e| Error::Router(format!("Monitor Server: {}", e)))?;

    Ok(())
}

fn monitor_router() -> Router {
    Router::new()
        .route("/runtime/errors", get(handle_runtime_app_errors))
        .route("/runtime/platform-errors", get(|| async { "--TODO--" }))
        .layer(TraceLayer::new_for_http())
}

async fn graceful_shutdown(handle: Handle, server_config: MonitorServerConfig) {
    let ctrl_c = async {
        signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("failed to install signal handler")
            .recv()
            .await;
    };

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }

    info!("sending graceful shutdown signal");

    // Signal the server to shutdown using Handle.
    handle.graceful_shutdown(Some(Duration::from_secs(
        server_config.graceful_shutdown_duration,
    )));
}
