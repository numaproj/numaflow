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
use base64::Engine;
use base64::prelude::BASE64_STANDARD;
use numaflow_models::models::MonoVertex;
use tokio_util::sync::CancellationToken;
use tracing::info;

use crate::api::{api_v1_errors, api_v1_metrics, api_v1_status};
use crate::grpc_adapter::GrpcAdapter;
use crate::runtime::RuntimeCache;

mod api;
mod error;
mod grpc_adapter;
mod runtime;
mod service;
mod tls;

pub(crate) use service::MvtxDaemonService;

use error::Result;
use tls::build_rustls_config;

/// Daemon service port; matches `pkg/apis/numaflow/v1alpha1/const.go`.
const DAEMON_SERVICE_PORT: u16 = 4327;

/// Environment variable carrying the base64-encoded MonoVertex CR JSON.
/// Matches Go's `EnvMonoVertexObject` constant.
const ENV_MONO_VERTEX_OBJECT: &str = "NUMAFLOW_MONO_VERTEX_OBJECT";

/// Default max replicas when spec.scale.max is not set. Matches Go's default.
const DEFAULT_MAX_REPLICAS: i32 = 50;

/// Runs the MonoVertex daemon: one port, TLS, ALPN (http/1.1 + h2).
/// REST routes (/readyz, /livez, etc.) and gRPC are served by the same Axum + Tonic stack.
pub async fn run_monovertex(cln_token: CancellationToken) -> Result<()> {
    let (name, namespace, max_replicas) = load_cr()?;
    info!(
        "Starting daemon server for MonoVertex {name} (namespace={namespace}, max_replicas={max_replicas})"
    );

    let addr: SocketAddr = format!("[::]:{}", DAEMON_SERVICE_PORT)
        .parse()
        .map_err(|e: std::net::AddrParseError| error::Error::Address(e.to_string()))?;

    let tls_config = build_rustls_config().await?;

    // Create the runtime error cache and start its background refresh task.
    let runtime = Arc::new(RuntimeCache::new(name.clone(), namespace, max_replicas));
    let runtime_bg = Arc::clone(&runtime);
    let runtime_token = cln_token.clone();
    tokio::spawn(async move {
        runtime_bg.run(runtime_token).await;
    });

    let svc = Arc::new(MvtxDaemonService::new(name, runtime));
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

/// Loads the MonoVertex CR from the `NUMAFLOW_MONO_VERTEX_OBJECT` environment variable
/// (base64-encoded JSON) and returns `(name, namespace, max_replicas)`.
fn load_cr() -> Result<(String, String, i32)> {
    let b64 = std::env::var(ENV_MONO_VERTEX_OBJECT)
        .map_err(|_| error::Error::Config(format!("{ENV_MONO_VERTEX_OBJECT} is not set")))?;

    let json = BASE64_STANDARD
        .decode(b64.as_bytes())
        .map_err(|e| error::Error::Config(format!("Failed to base64-decode CR: {e}")))?;

    let cr: MonoVertex = serde_json::from_slice(&json)
        .map_err(|e| error::Error::Config(format!("Failed to parse MonoVertex CR: {e}")))?;

    let name = cr
        .metadata
        .as_ref()
        .and_then(|m| m.name.clone())
        .ok_or_else(|| error::Error::Config("MonoVertex metadata.name is missing".to_string()))?;

    let namespace = cr
        .metadata
        .as_ref()
        .and_then(|m| m.namespace.clone())
        .unwrap_or_else(|| "default".to_string());

    let max_replicas = cr
        .spec
        .scale
        .as_ref()
        .and_then(|s| s.max)
        .unwrap_or(DEFAULT_MAX_REPLICAS);

    Ok((name, namespace, max_replicas))
}

/// Builds the daemon Axum app: readyz, livez, REST API routes, and gRPC fallback.
fn make_app(svc: Arc<MvtxDaemonService>) -> Router {
    // The gRPC adapter needs its own clone of the service (Tonic wraps it internally).
    // Cloning is cheap: MvtxDaemonService only holds a String and an Arc.
    let grpc_svc = (*svc).clone();
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
    rest_router.fallback_service(GrpcAdapter::new(grpc_svc))
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use http::{Method, Request};
    use tower::ServiceExt;

    fn make_test_svc() -> Arc<MvtxDaemonService> {
        let runtime = Arc::new(RuntimeCache::new(
            "test-mvtx".to_string(),
            "default".to_string(),
            2,
        ));
        Arc::new(MvtxDaemonService::new("test-mvtx".to_string(), runtime))
    }

    fn app() -> Router {
        make_app(make_test_svc())
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
