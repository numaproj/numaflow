//! MonoVertex daemon server: one TLS port serving HTTP/1.1 (REST) and gRPC (h2) via ALPN.
//!
//! Uses Axum for the HTTP stack and nests the Tonic gRPC service so a single
//! `axum_server::bind_rustls` listen handles both protocols.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::http::StatusCode;
use axum::routing::get;
use axum_server::Handle;
use tokio_util::sync::CancellationToken;
use tracing::info;

use crate::api::{api_v1_errors, api_v1_metrics, api_v1_status};
use crate::grpc_adapter::GrpcAdapter;

mod api;
mod error;
mod grpc_adapter;
mod service;
mod tls;

pub(crate) use service::MvtxDaemonService;

use error::Result;
use tls::build_rustls_config;

/// Daemon service port; matches `pkg/apis/numaflow/v1alpha1/const.go`.
const DAEMON_SERVICE_PORT: u16 = 4327;

/// Runs the MonoVertex daemon: one port, TLS, ALPN (http/1.1 + h2).
/// REST routes (/readyz, /livez, etc.) and gRPC are served by the same Axum + Tonic stack.
pub async fn run_monovertex(mvtx_name: String, cln_token: CancellationToken) -> Result<()> {
    info!("Starting daemon server for MonoVertex {}", mvtx_name);

    let addr: SocketAddr = format!("[::]:{}", DAEMON_SERVICE_PORT)
        .parse()
        .map_err(|e: std::net::AddrParseError| error::Error::Address(e.to_string()))?;

    let tls_config = build_rustls_config().await?;

    let svc = Arc::new(MvtxDaemonService);
    let app = make_app(svc);

    let handle = Handle::new();
    let handle_clone = handle.clone();
    let shutdown_token = cln_token.clone();
    /// Max time to wait for in-flight requests to finish before forcing shutdown.
    /// Matches Kubernetes default termination grace period so the pod can exit before SIGKILL.
    const GRACEFUL_SHUTDOWN_TIMEOUT_SECS: u64 = 30;
    tokio::spawn(async move {
        shutdown_token.cancelled().await;
        info!("CancellationToken cancelled, graceful shutdown initiated");
        handle_clone.graceful_shutdown(Some(Duration::from_secs(GRACEFUL_SHUTDOWN_TIMEOUT_SECS)));
    });

    axum_server::bind_rustls(addr, tls_config)
        .handle(handle)
        .serve(app.into_make_service())
        .await
        .map_err(|e| error::Error::NotComplete(format!("Daemon server failed: {}", e)))?;

    Ok(())
}

/// Builds the daemon Axum app: readyz, livez, REST API routes, and gRPC fallback.
fn make_app(svc: Arc<MvtxDaemonService>) -> Router {
    let rest_router = Router::new()
        .route("/readyz", get(|| async { StatusCode::NO_CONTENT }))
        .route("/livez", get(|| async { StatusCode::NO_CONTENT }))
        .route("/api/v1/metrics", get(api_v1_metrics))
        .route("/api/v1/status", get(api_v1_status))
        .route(
            "/api/v1/mono-vertices/{mono_vertex}/errors",
            get(api_v1_errors),
        )
        .with_state(svc);
    // Unmatched paths aka. gRPC requests go to gRPC.
    rest_router.fallback_service(GrpcAdapter::new())
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use http::{Method, Request};
    use tower::ServiceExt;

    fn app() -> Router {
        make_app(Arc::new(MvtxDaemonService))
    }

    #[tokio::test]
    async fn readyz_returns_no_content() {
        let app = app();
        let request = Request::builder()
            .uri("/readyz")
            .body(Body::empty())
            .unwrap();
        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::NO_CONTENT);
    }

    #[tokio::test]
    async fn livez_returns_no_content() {
        let app = app();
        let request = Request::builder()
            .uri("/livez")
            .body(Body::empty())
            .unwrap();
        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::NO_CONTENT);
    }

    #[tokio::test]
    async fn grpc_fallback_returns_ok_and_grpc_content_type() {
        let app = app();
        let body = Body::from(vec![0u8, 0, 0, 0, 0]);
        let request = Request::builder()
            .method(Method::POST)
            .uri("/mvtxdaemon.MonoVertexDaemonService/GetMonoVertexMetrics")
            .header("content-type", "application/grpc")
            .body(body)
            .unwrap();
        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response
                .headers()
                .get("content-type")
                .and_then(|v| v.to_str().ok()),
            Some("application/grpc")
        );
    }
}
