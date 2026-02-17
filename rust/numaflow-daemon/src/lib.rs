//! MonoVertex daemon server: one TLS port serving HTTP/1.1 (REST) and gRPC (h2) via ALPN.
//!
//! Uses Axum for the HTTP stack and nests the Tonic gRPC service so a single
//! `axum_server::bind_rustls` listen handles both protocols.

use std::net::SocketAddr;
use std::sync::Arc;

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
    info!("MonoVertex name is {}", mvtx_name);

    let addr: SocketAddr = format!("[::]:{}", DAEMON_SERVICE_PORT)
        .parse()
        .map_err(|e: std::net::AddrParseError| error::Error::Address(e.to_string()))?;

    let tls_config = build_rustls_config().await?;

    let svc = Arc::new(MvtxDaemonService);
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

    // Unmatched paths (e.g. /mvtxdaemon.MonoVertexDaemonService/GetMonoVertexMetrics) go to gRPC.
    let app = rest_router.fallback_service(GrpcAdapter::new());

    let handle = Handle::new();
    let handle_clone = handle.clone();
    let shutdown_token = cln_token.clone();
    tokio::spawn(async move {
        shutdown_token.cancelled().await;
        handle_clone.graceful_shutdown(None);
    });

    axum_server::bind_rustls(addr, tls_config)
        .handle(handle)
        .serve(app.into_make_service())
        .await
        .map_err(|e| error::Error::NotComplete(format!("Daemon server failed: {}", e)))?;

    Ok(())
}
