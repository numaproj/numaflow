//! The Tonic gRPC service implementations.

use numaflow_pb::servers::mvtxdaemon::mono_vertex_daemon_service_server::MonoVertexDaemonService;
use numaflow_pb::servers::mvtxdaemon::{
    GetMonoVertexErrorsRequest, GetMonoVertexErrorsResponse, GetMonoVertexMetricsResponse,
    GetMonoVertexStatusResponse, MonoVertexMetrics, MonoVertexStatus, ReplicaErrors,
};
use std::collections::HashMap;
use std::result::Result;
use tonic::{Request, Response, Status};

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

        let mock_resp = GetMonoVertexMetricsResponse {
            metrics: Some(MonoVertexMetrics {
                mono_vertex: "mock_mvtx_spec".to_string(),
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
                status: "mock_status".to_string(),
                message: "mock_status_message".to_string(),
                code: "mock_status_code".to_string(),
            }),
        };

        Ok(Response::new(mock_resp))
    }

    async fn get_mono_vertex_errors(
        &self,
        _: Request<GetMonoVertexErrorsRequest>,
    ) -> Result<Response<GetMonoVertexErrorsResponse>, Status> {
        let mock_resp = GetMonoVertexErrorsResponse {
            errors: vec![ReplicaErrors {
                replica: "mock_replica".to_string(),
                container_errors: vec![],
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

        assert_eq!(metrics.mono_vertex, "mock_mvtx_spec");
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

        assert_eq!(status.status, "mock_status");
        assert_eq!(status.message, "mock_status_message");
        assert_eq!(status.code, "mock_status_code");
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
        assert!(first.container_errors.is_empty());
    }
}
