//! HTTP JSON API handlers for /api/v1/* (grpc-gateway style).

use axum::Json;
use axum::extract::Path;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use numaflow_pb::servers::mvtxdaemon::GetMonoVertexErrorsRequest;
use numaflow_pb::servers::mvtxdaemon::mono_vertex_daemon_service_server::MonoVertexDaemonService;
use serde::Deserialize;
use serde_json::Value;
use std::sync::Arc;
use tonic::Request;

use crate::MvtxDaemonService;

/// GET /api/v1/metrics — returns metrics as JSON (grpc-gateway style).
pub(crate) async fn api_v1_metrics(State(svc): State<Arc<MvtxDaemonService>>) -> impl IntoResponse {
    tracing::debug!("REST API: GET /api/v1/metrics called via HTTP/1.1");
    match svc.get_mono_vertex_metrics(Request::new(())).await {
        Ok(resp) => {
            let body = resp.into_inner();
            let json = match &body.metrics {
                Some(m) => serde_json::json!({
                    "metrics": {
                        "monoVertex": m.mono_vertex,
                        "processingRates": m.processing_rates,
                        "pendings": m.pendings,
                    }
                }),
                None => serde_json::json!({}),
            };
            (StatusCode::OK, Json(json))
        }
        Err(_) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({})),
        ),
    }
}

/// GET /api/v1/status — returns status as JSON (grpc-gateway style).
pub(crate) async fn api_v1_status(State(svc): State<Arc<MvtxDaemonService>>) -> impl IntoResponse {
    tracing::debug!("REST API: GET /api/v1/status called via HTTP/1.1");
    match svc.get_mono_vertex_status(Request::new(())).await {
        Ok(resp) => {
            let body = resp.into_inner();
            let json = match &body.status {
                Some(s) => serde_json::json!({
                    "status": {
                        "status": s.status,
                        "message": s.message,
                        "code": s.code,
                    }
                }),
                None => serde_json::json!({}),
            };
            (StatusCode::OK, Json(json))
        }
        Err(_) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({})),
        ),
    }
}

/// Path params for GET /api/v1/mono-vertices/:mono_vertex/errors
#[derive(Debug, Deserialize)]
pub(crate) struct ErrorsPathParams {
    pub(crate) mono_vertex: String,
}

/// GET /api/v1/mono-vertices/:mono_vertex/errors — returns errors as JSON (grpc-gateway style).
pub(crate) async fn api_v1_errors(
    State(svc): State<Arc<MvtxDaemonService>>,
    Path(params): Path<ErrorsPathParams>,
) -> impl IntoResponse {
    tracing::debug!(
        "REST API: GET /api/v1/mono-vertices/{}/errors called via HTTP/1.1",
        params.mono_vertex
    );
    let req = GetMonoVertexErrorsRequest {
        mono_vertex: params.mono_vertex,
    };
    match svc.get_mono_vertex_errors(Request::new(req)).await {
        Ok(resp) => {
            let body = resp.into_inner();
            let errors: Vec<Value> = body
                .errors
                .iter()
                .map(|re| {
                    let container_errors: Vec<Value> = re
                        .container_errors
                        .iter()
                        .map(|ce| {
                            serde_json::json!({
                                "container": ce.container,
                                "code": ce.code,
                                "message": ce.message,
                                "details": ce.details,
                            })
                        })
                        .collect();
                    serde_json::json!({
                        "replica": re.replica,
                        "containerErrors": container_errors,
                    })
                })
                .collect();
            let json = serde_json::json!({ "errors": errors });
            (StatusCode::OK, Json(json))
        }
        Err(_) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({})),
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::Router;
    use axum::body::Body;
    use axum::routing::get;
    use http::Request;
    use tower::ServiceExt;

    fn test_router() -> Router {
        let svc = Arc::new(MvtxDaemonService);
        Router::new()
            .route("/api/v1/metrics", get(api_v1_metrics))
            .route("/api/v1/status", get(api_v1_status))
            .route(
                "/api/v1/mono-vertices/{mono_vertex}/errors",
                get(api_v1_errors),
            )
            .with_state(svc)
    }

    #[tokio::test]
    async fn api_v1_metrics_returns_ok_and_json() {
        let app = test_router();
        let request = Request::builder()
            .uri("/api/v1/metrics")
            .body(Body::empty())
            .unwrap();
        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let metrics = json.get("metrics").and_then(|m| m.get("monoVertex"));
        assert_eq!(
            metrics,
            Some(&serde_json::Value::String("mock_mvtx_spec".into()))
        );
    }

    #[tokio::test]
    async fn api_v1_status_returns_ok_and_json() {
        let app = test_router();
        let request = Request::builder()
            .uri("/api/v1/status")
            .body(Body::empty())
            .unwrap();
        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let status = json.get("status").and_then(|s| s.get("status"));
        assert_eq!(
            status,
            Some(&serde_json::Value::String("mock_status".into()))
        );
    }

    #[tokio::test]
    async fn api_v1_errors_returns_ok_and_json() {
        let app = test_router();
        let request = Request::builder()
            .uri("/api/v1/mono-vertices/my-mono-vertex/errors")
            .body(Body::empty())
            .unwrap();
        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let errors = json.get("errors").and_then(|e| e.as_array()).unwrap();
        assert_eq!(errors.len(), 1);
        assert_eq!(
            errors.first().unwrap().get("replica"),
            Some(&serde_json::Value::String("mock_replica".into()))
        );
    }
}
