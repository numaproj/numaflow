//! This module contains the main application logic for the sidecar monitor server.
use axum::{extract::State, response::IntoResponse, routing::get, Json, Router};
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

/// AppState represents the shared application state that is accessible across all handlers.
/// It contains the Runtime instance which manages the application's runtime information
/// and error tracking.
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

    // Initialize shared app state
    let shared_state = Arc::new(AppState {
        runtime: Arc::new(runtime),
    });

    let router = monitor_router(Arc::clone(&shared_state));

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
        .with_state(shared_state)
}

/// Handler for the /runtime/errors route to get application errors.
async fn handle_runtime_app_errors(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let runtime = state.runtime.as_ref();

    // Get the application errors from the runtime.
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

/// Gracefully shutdown the server when a termination signal is received.
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

    // Signal the server to shutdown gracefully.
    handle.graceful_shutdown(Some(Duration::from_secs(
        server_config.graceful_shutdown_duration,
    )));
}

#[cfg(test)]
mod tests {
    use crate::{config::generate_certs, error::Result};

    use super::*;
    use axum::body::Body;
    use axum::http::Request;
    use rustls::crypto::ring::default_provider;
    use std::{
        net::{Ipv4Addr, SocketAddrV4},
        sync::Arc,
    };
    use tower::ServiceExt;

    #[tokio::test]
    async fn test_start_main_server() -> Result<()> {
        default_provider()
            .install_default()
            .expect("failed to initialize rustls crypto provider");
        let (cert, key) = generate_certs()
            .map_err(|e| Error::Init(format!("Certificate generation failed: {}", e)))?;

        let tls_config = RustlsConfig::from_pem(cert.pem().into(), key.serialize_pem().into())
            .await
            .map_err(|e| Error::Init(format!("TLS configuration failed: {}", e)))?;

        let server_config = MonitorServerConfig {
            graceful_shutdown_duration: 2,
            server_listen_port: 3000,
        };
        let app_addr = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0));

        let result = start_main_server(app_addr, tls_config, server_config).await;
        assert!(result.is_ok());
        Ok(())
    }

    #[tokio::test]
    async fn test_handle_runtime_app_errors_internal_error() {
        // Initialize runtime and app state
        let runtime = Arc::new(Runtime::new(Some(RuntimeInfoConfig::default())));
        let state = Arc::new(AppState { runtime });

        // Create a request to the /runtime/errors route
        let request = Request::builder()
            .uri("/runtime/errors")
            .body(Body::empty())
            .unwrap();

        // Create a router with the handler
        let router = Router::new()
            .route(
                "/runtime/errors",
                axum::routing::get(handle_runtime_app_errors),
            )
            .with_state(state);

        // Call the handler
        let response = router.oneshot(request).await.unwrap();

        // It should throw error since app-error directory doesn't exist
        // and we are trying to read from it
        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);

        let body = axum::body::to_bytes(response.into_body(), 1024 * 1024)
            .await
            .unwrap();
        let api_response: ApiResponse = serde_json::from_slice(&body).unwrap();
        assert!(api_response.data.is_empty());
        assert_eq!(
            api_response.error_message,
            Some("file Error - No application errors persisted yet".to_string())
        );
    }
}
