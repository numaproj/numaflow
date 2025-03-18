use axum::{response::IntoResponse, routing::get, Extension, Json, Router};
use axum_server::{tls_rustls::RustlsConfig, Handle};
use http::StatusCode;
use std::{net::SocketAddr, sync::Arc, time::Duration};
use tokio::signal;
use tracing::{error, info};

use crate::{
    config::RuntimeInfoConfig,
    error::Error,
    runtime::{ApiResponse, Runtime},
    MonitorServerConfig,
};

pub(crate) struct AppState {
    pub(crate) runtime: Arc<Runtime>,
}
/// Start the main application Router and the axum server.
pub(crate) async fn start_main_server(
    app_addr: SocketAddr,
    tls_config: RustlsConfig,
    server_config: MonitorServerConfig,
) -> crate::Result<()> {
    let handle = Handle::new();
    // Spawn a task to gracefully shutdown server.
    tokio::spawn(graceful_shutdown(handle.clone(), server_config.clone()));

    info!(?app_addr, "Starting monitor app server..");

    let runtime = Runtime::new(Some(RuntimeInfoConfig::default()));

    // Initialize shared state
    let shared_state = Arc::new(AppState {
        runtime: Arc::new(runtime),
    });

    let router = monitor_router(shared_state.clone());

    axum_server::bind_rustls(app_addr, tls_config)
        .handle(handle)
        .serve(router.into_make_service())
        .await
        .map_err(|e| Error::Router(format!("Monitor Server: {}", e)))?;

    Ok(())
}

fn monitor_router(shared_state: Arc<AppState>) -> Router {
    Router::new()
        .route("/runtime/errors", get(handle_runtime_app_errors))
        .layer(axum::extract::Extension(shared_state))
}

/**
File Structure for application-errors

Root: /var/numaflow/runtime/
            └── application-errors
                └── udsource/
                        ├── ts1.json
                        └── ts2.json
                └── udsink/
                        ├── ts3.json
                        └── ts4.json

*/
async fn handle_runtime_app_errors(
    Extension(state): Extension<Arc<AppState>>,
) -> impl IntoResponse {
    let runtime = state.runtime.as_ref();

    // Call the get_application_errors method on the Runtime instance
    match runtime.get_application_errors() {
        Ok(errors) => (
            StatusCode::OK,
            Json(ApiResponse {
                error_message: None,
                data: errors,
            }),
        )
            .into_response(),
        Err(err) => {
            error!("{}", err);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ApiResponse {
                    error_message: Some(err.to_string()),
                    data: Vec::new(),
                }),
            )
                .into_response()
        }
    }
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

    // Signal the server to shut down using Handle.
    handle.graceful_shutdown(Some(Duration::from_secs(
        server_config.graceful_shutdown_duration,
    )));
}
