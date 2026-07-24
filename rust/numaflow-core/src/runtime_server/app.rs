//! HTTPS server exposing persisted runtime application errors to the daemon.
use axum::{Json, Router, extract::State, response::IntoResponse, routing::get};
use axum_server::{Handle, tls_rustls::RustlsConfig};
use http::StatusCode;
use std::{net::SocketAddr, sync::Arc, time::Duration};
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

use super::config::{MonitorServerConfig, RuntimeInfoConfig};
use super::error::Error;
use super::runtime::{ApiResponse, Runtime};

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
    cln_token: CancellationToken,
) -> super::error::Result<()> {
    let handle = Handle::new();
    tokio::spawn(graceful_shutdown(
        handle.clone(),
        server_config.clone(),
        cln_token,
    ));

    info!(?app_addr, "Starting runtime errors app server");

    let runtime = Runtime::new(Some(RuntimeInfoConfig::default()));

    // Initialize shared app state
    let shared_state = Arc::new(AppState {
        runtime: Arc::new(runtime),
    });

    let router = runtime_errors_router(Arc::clone(&shared_state));

    axum_server::bind_rustls(app_addr, tls_config)
        .handle(handle)
        .serve(router.into_make_service())
        .await
        .map_err(|e| Error::Router(format!("Runtime errors server: {e}")))?;

    Ok(())
}

fn runtime_errors_router(shared_state: Arc<AppState>) -> Router {
    Router::new()
        .route(
            "/runtime/errors",
            get(handle_runtime_app_errors).head(handle_runtime_app_errors_head),
        )
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

async fn handle_runtime_app_errors_head() -> StatusCode {
    StatusCode::OK
}

/// Gracefully shutdown the server when the process cancellation token fires.
async fn graceful_shutdown(
    handle: Handle,
    server_config: MonitorServerConfig,
    cln_token: CancellationToken,
) {
    cln_token.cancelled().await;
    info!("sending graceful shutdown signal to runtime errors server");

    // Signal the server to shutdown gracefully.
    handle.graceful_shutdown(Some(Duration::from_secs(
        server_config.graceful_shutdown_duration,
    )));
}

#[cfg(test)]
mod tests {
    use crate::runtime_server::config::generate_certs;
    use crate::runtime_server::error::Result;
    use crate::runtime_server::runtime::persist_application_error_to_file;

    use super::*;
    use axum::body::Body;
    use axum::http::Request;
    use rustls::crypto::ring::default_provider;
    use std::sync::Arc;
    use tempfile::tempdir;
    use tokio::time::sleep;
    use tonic::Status;
    use tower::ServiceExt;

    #[tokio::test]
    async fn test_start_main_server() -> Result<()> {
        // CryptoProvider may already be installed by another test running in parallel.
        let _ = default_provider().install_default();

        let (cert, key) = generate_certs()
            .map_err(|e| Error::Init(format!("Certificate generation failed: {}", e)))?;
        let tls_config = RustlsConfig::from_pem(cert.pem().into(), key.serialize_pem().into())
            .await
            .unwrap();

        let addr = SocketAddr::from(([127, 0, 0, 1], 0));
        let server_config = MonitorServerConfig {
            server_listen_port: 0,
            graceful_shutdown_duration: 2,
        };
        let cln_token = CancellationToken::new();
        let shutdown_token = cln_token.clone();
        let server = tokio::spawn(async move {
            start_main_server(addr, tls_config, server_config, shutdown_token).await
        });

        // Give the server a little bit of time to start
        sleep(Duration::from_millis(200)).await;
        assert!(
            !server.is_finished(),
            "runtime errors server exited before shutdown"
        );

        cln_token.cancel();
        let shutdown_result = tokio::time::timeout(Duration::from_secs(2), server).await;
        assert!(
            shutdown_result.is_ok(),
            "runtime errors server did not shut down in time"
        );
        assert!(shutdown_result.unwrap().is_ok());
        Ok(())
    }

    #[tokio::test]
    async fn test_handle_runtime_app_errors_ok() {
        // Create a temporary directory for testing
        let temp_dir = tempdir().unwrap();
        let application_error_path = temp_dir.path().to_str().unwrap().to_string();

        // Create a Runtime instance with the temporary directory path
        let runtime = Runtime::new(Some(RuntimeInfoConfig {
            app_error_path: application_error_path.clone(),
            max_error_files_per_container: 2,
        }));

        // Create a mock gRPC status
        let grpc_status = Status::internal("UDF_EXECUTION_ERROR(udsource): Test error message");

        // Call the function to persist error in temp app directory
        persist_application_error_to_file(application_error_path, 5, grpc_status.clone());

        let state = Arc::new(AppState {
            runtime: Arc::new(runtime),
        });

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

        // It should respond with Status Ok
        assert_eq!(response.status(), StatusCode::OK);
        // Extract and check the response body
        let body = axum::body::to_bytes(response.into_body(), 1024 * 1024)
            .await
            .unwrap();
        let api_response: ApiResponse = serde_json::from_slice(&body).unwrap();

        // Assert that the response does not have error message
        assert!(api_response.error_message.is_none());
        // Assert that length of data array is 1
        assert_eq!(api_response.data.len(), 1);
    }
    #[tokio::test]
    async fn test_handle_runtime_app_errors_ok_empty_data() {
        // Initialize runtime and app state
        let runtime = Arc::new(Runtime::new(Some(RuntimeInfoConfig {
            app_error_path: "test-path-error".to_string(),
            max_error_files_per_container: 2,
        })));
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

        assert_eq!(response.status(), StatusCode::OK);

        let body = axum::body::to_bytes(response.into_body(), 1024 * 1024)
            .await
            .unwrap();
        let api_response: ApiResponse = serde_json::from_slice(&body).unwrap();
        // It should return early since app-error directory doesn't exist
        // and we are trying to read from it
        assert!(api_response.data.is_empty());
    }

    #[tokio::test]
    async fn test_handle_runtime_app_errors_head() {
        let runtime = Arc::new(Runtime::new(Some(RuntimeInfoConfig::default())));
        let state = Arc::new(AppState { runtime });

        let request = Request::builder()
            .method("HEAD")
            .uri("/runtime/errors")
            .body(Body::empty())
            .unwrap();

        let router = Router::new()
            .route(
                "/runtime/errors",
                axum::routing::get(handle_runtime_app_errors).head(handle_runtime_app_errors_head),
            )
            .with_state(state);

        let response = router.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }
}
