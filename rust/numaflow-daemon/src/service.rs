use numaflow_pb::servers::mvtxdaemon::mono_vertex_daemon_service_server::MonoVertexDaemonService;
use numaflow_pb::servers::mvtxdaemon::{
    ContainerError, GetMonoVertexErrorsRequest, GetMonoVertexErrorsResponse,
    GetMonoVertexMetricsResponse, GetMonoVertexStatusResponse, MonoVertexMetrics, MonoVertexStatus,
    ReplicaErrors,
};
use prost_types::Timestamp;
use std::collections::HashMap;
use std::env;
use std::result::Result;
use tonic::{Request, Response, Status};

/// Env var set by daemon deployment; matches pkg/apis/numaflow/v1alpha1/const.go EnvMonoVertexName.
const ENV_MONO_VERTEX_NAME: &str = "NUMAFLOW_MONO_VERTEX_NAME";

/// MvtxDaemonService is the Tonic gRPC service implementations for MonoVertex daemon server.
/// It's the single source of truth of how MonoVertex daemon server handles requests, regardless of HTTP or gRPC.
pub(crate) struct MvtxDaemonService;

#[tonic::async_trait]
impl MonoVertexDaemonService for MvtxDaemonService {
    async fn get_mono_vertex_metrics(
        &self,
        _: Request<()>,
    ) -> Result<Response<GetMonoVertexMetricsResponse>, Status> {
        let mock_processing_rates = HashMap::from([
            ("default".to_string(), 67.0),
            ("1m".to_string(), 10.0),
            ("5m".to_string(), 50.5),
            ("15m".to_string(), 150.0),
        ]);

        let mock_pendings = HashMap::from([
            ("default".to_string(), 67),
            ("1m".to_string(), 10),
            ("5m".to_string(), 50),
            ("15m".to_string(), 150),
        ]);

        let mono_vertex = env::var(ENV_MONO_VERTEX_NAME)
            .unwrap_or_else(|_| "simple-mono-vertex".to_string());
        let mock_resp = GetMonoVertexMetricsResponse {
            metrics: Some(MonoVertexMetrics {
                mono_vertex,
                processing_rates: mock_processing_rates,
                pendings: mock_pendings,
            }),
        };

        Ok(Response::new(mock_resp))
    }

    async fn get_mono_vertex_status(
        &self,
        _: Request<()>,
    ) -> Result<Response<GetMonoVertexStatusResponse>, Status> {
        let mock_resp = GetMonoVertexStatusResponse {
            status: Some(MonoVertexStatus {
                status: "healthy".to_string(),
                message: "MonoVertex data flow is healthy".to_string(),
                code: "D1".to_string(),
            }),
        };

        Ok(Response::new(mock_resp))
    }

    async fn get_mono_vertex_errors(
        &self,
        _: Request<GetMonoVertexErrorsRequest>,
    ) -> Result<Response<GetMonoVertexErrorsResponse>, Status> {
        let now = chrono::Utc::now();
        let timestamp = Some(Timestamp {
            seconds: now.timestamp(),
            nanos: now.timestamp_subsec_nanos() as i32,
        });
        let mock_resp = GetMonoVertexErrorsResponse {
            errors: vec![ReplicaErrors {
                replica: "mock_replica".to_string(),
                container_errors: vec![ContainerError {
                    container: "main".to_string(),
                    timestamp,
                    code: "mock_code".to_string(),
                    message: "mock_message".to_string(),
                    details: "mock_details".to_string(),
                }],
            }],
        };

        Ok(Response::new(mock_resp))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn get_mono_vertex_metrics_returns_mock_data() {
        let svc = MvtxDaemonService;
        let resp = svc
            .get_mono_vertex_metrics(Request::new(()))
            .await
            .expect("metrics response");
        let body = resp.into_inner();
        let metrics = body.metrics.expect("metrics payload");

        assert_eq!(metrics.mono_vertex, "simple-mono-vertex");
        assert_eq!(metrics.processing_rates.get("default"), Some(&67.0));
        assert_eq!(metrics.processing_rates.get("1m"), Some(&10.0));
        assert_eq!(metrics.processing_rates.get("5m"), Some(&50.5));
        assert_eq!(metrics.processing_rates.get("15m"), Some(&150.0));

        assert_eq!(metrics.pendings.get("default"), Some(&67));
        assert_eq!(metrics.pendings.get("1m"), Some(&10));
        assert_eq!(metrics.pendings.get("5m"), Some(&50));
        assert_eq!(metrics.pendings.get("15m"), Some(&150));
    }

    #[tokio::test]
    async fn get_mono_vertex_status_returns_mock_data() {
        let svc = MvtxDaemonService;
        let resp = svc
            .get_mono_vertex_status(Request::new(()))
            .await
            .expect("status response");
        let body = resp.into_inner();
        let status = body.status.expect("status payload");

        assert_eq!(status.status, "healthy");
        assert_eq!(status.message, "MonoVertex data flow is healthy");
        assert_eq!(status.code, "D1");
    }

    #[tokio::test]
    async fn get_mono_vertex_errors_returns_mock_data() {
        let svc = MvtxDaemonService;
        let resp = svc
            .get_mono_vertex_errors(Request::new(GetMonoVertexErrorsRequest {
                mono_vertex: "mock_mono_vertex".to_string(),
            }))
            .await
            .expect("errors response");
        let body = resp.into_inner();

        assert_eq!(body.errors.len(), 1);
        let first = body.errors.first().expect("first error");
        assert_eq!(first.replica, "mock_replica");
        assert_eq!(first.container_errors.len(), 1);
        let ce = first
            .container_errors
            .first()
            .expect("first container error");
        assert_eq!(ce.container, "main");
        assert_eq!(ce.code, "mock_code");
        assert_eq!(ce.message, "mock_message");
        assert_eq!(ce.details, "mock_details");
    }
}
