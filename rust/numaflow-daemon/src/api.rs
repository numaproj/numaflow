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
        let metrics = json.get("metrics").expect("metrics key");
        assert_eq!(
            metrics.get("monoVertex"),
            Some(&serde_json::Value::String("mock_mvtx_spec".into()))
        );
        let rates = metrics
            .get("processingRates")
            .and_then(|v| v.as_object())
            .expect("processingRates");
        assert_eq!(rates.get("default").and_then(|v| v.as_f64()), Some(67.0));
        assert_eq!(rates.get("1m").and_then(|v| v.as_f64()), Some(10.0));
        assert_eq!(rates.get("5m").and_then(|v| v.as_f64()), Some(50.5));
        assert_eq!(rates.get("15m").and_then(|v| v.as_f64()), Some(150.0));
        let pendings = metrics
            .get("pendings")
            .and_then(|v| v.as_object())
            .expect("pendings");
        assert_eq!(pendings.get("default").and_then(|v| v.as_i64()), Some(67));
        assert_eq!(pendings.get("1m").and_then(|v| v.as_i64()), Some(10));
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
        let status = json.get("status").expect("status key");
        assert_eq!(
            status.get("status"),
            Some(&serde_json::Value::String("mock_status".into()))
        );
        assert_eq!(
            status.get("message"),
            Some(&serde_json::Value::String("mock_status_message".into()))
        );
        assert_eq!(
            status.get("code"),
            Some(&serde_json::Value::String("mock_status_code".into()))
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
        let errors = json
            .get("errors")
            .and_then(|e| e.as_array())
            .expect("errors array");
        assert_eq!(errors.len(), 1);
        let first = errors.first().expect("first error");
        assert_eq!(
            first.get("replica"),
            Some(&serde_json::Value::String("mock_replica".into()))
        );
        let container_errors = first
            .get("containerErrors")
            .and_then(|c| c.as_array())
            .expect("containerErrors");
        assert!(
            container_errors.is_empty(),
            "mock returns no container errors"
        );
    }

    #[tokio::test]
    async fn api_v1_errors_path_param_extracted() {
        let app = test_router();
        let request = Request::builder()
            .uri("/api/v1/mono-vertices/any-vertex-name/errors")
            .body(Body::empty())
            .unwrap();
        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert!(
            json.get("errors").is_some(),
            "path param allows any mono_vertex"
        );
    }
}
