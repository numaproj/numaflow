use std::collections::HashMap;
use std::sync::Arc;

use numaflow_pb::servers::mvtxdaemon::mono_vertex_daemon_service_server::MonoVertexDaemonService;
use numaflow_pb::servers::mvtxdaemon::{
    ContainerError, GetMonoVertexErrorsRequest, GetMonoVertexErrorsResponse,
    GetMonoVertexMetricsResponse, GetMonoVertexStatusResponse, MonoVertexMetrics, MonoVertexStatus,
    ReplicaErrors,
};
use prost_types::Timestamp;
use tonic::{Request, Response, Status};

use crate::runtime::RuntimeCache;

/// MvtxDaemonService is the Tonic gRPC service implementation for the MonoVertex daemon server.
/// It is the single source of truth for how the daemon handles requests, regardless of HTTP or gRPC.
#[derive(Clone)]
pub(crate) struct MvtxDaemonService {
    name: String,
    runtime: Arc<RuntimeCache>,
}

impl MvtxDaemonService {
    pub(crate) fn new(name: String, runtime: Arc<RuntimeCache>) -> Self {
        Self { name, runtime }
    }
}

#[tonic::async_trait]
impl MonoVertexDaemonService for MvtxDaemonService {
    async fn get_mono_vertex_metrics(
        &self,
        _: Request<()>,
    ) -> std::result::Result<Response<GetMonoVertexMetricsResponse>, Status> {
        // TODO(Phase 3): replace with real rater data.
        let mock_processing_rates = HashMap::from([
            ("default".to_string(), -1.0_f64),
            ("1m".to_string(), -1.0_f64),
            ("5m".to_string(), -1.0_f64),
            ("15m".to_string(), -1.0_f64),
        ]);

        let mock_pendings = HashMap::from([
            ("default".to_string(), -1_i64),
            ("1m".to_string(), -1_i64),
            ("5m".to_string(), -1_i64),
            ("15m".to_string(), -1_i64),
        ]);

        Ok(Response::new(GetMonoVertexMetricsResponse {
            metrics: Some(MonoVertexMetrics {
                mono_vertex: self.name.clone(),
                processing_rates: mock_processing_rates,
                pendings: mock_pendings,
            }),
        }))
    }

    async fn get_mono_vertex_status(
        &self,
        _: Request<()>,
    ) -> std::result::Result<Response<GetMonoVertexStatusResponse>, Status> {
        // TODO(Phase 5): replace with real health checker data.
        Ok(Response::new(GetMonoVertexStatusResponse {
            status: Some(MonoVertexStatus {
                status: "unknown".to_string(),
                message: "metrics not yet available".to_string(),
                code: "D4".to_string(),
            }),
        }))
    }

    async fn get_mono_vertex_errors(
        &self,
        _: Request<GetMonoVertexErrorsRequest>,
    ) -> std::result::Result<Response<GetMonoVertexErrorsResponse>, Status> {
        let errors = self.runtime.get_errors().await;

        let replica_errors: Vec<ReplicaErrors> = errors
            .into_iter()
            .map(|re| ReplicaErrors {
                replica: re.replica,
                container_errors: re
                    .container_errors
                    .into_iter()
                    .map(|ce| {
                        // The pod API returns Unix milliseconds; proto Timestamp uses seconds + nanos.
                        let timestamp = Some(Timestamp {
                            seconds: ce.timestamp / 1000,
                            nanos: ((ce.timestamp % 1000) * 1_000_000) as i32,
                        });
                        ContainerError {
                            container: ce.container,
                            timestamp,
                            code: ce.code,
                            message: ce.message,
                            details: ce.details,
                        }
                    })
                    .collect(),
            })
            .collect();

        Ok(Response::new(GetMonoVertexErrorsResponse {
            errors: replica_errors,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::RuntimeCache;

    fn make_svc() -> MvtxDaemonService {
        let runtime = Arc::new(RuntimeCache::new(
            "simple-mono-vertex".to_string(),
            "default".to_string(),
            2,
        ));
        MvtxDaemonService::new("simple-mono-vertex".to_string(), runtime)
    }

    #[tokio::test]
    async fn get_mono_vertex_metrics_returns_name_and_no_data_sentinels() {
        let svc = make_svc();
        let resp = svc
            .get_mono_vertex_metrics(Request::new(()))
            .await
            .expect("metrics response");
        let metrics = resp.into_inner().metrics.expect("metrics payload");

        assert_eq!(metrics.mono_vertex, "simple-mono-vertex");
        // -1 signals "no data yet" for all windows.
        assert_eq!(metrics.processing_rates.get("default"), Some(&-1.0));
        assert_eq!(metrics.pendings.get("1m"), Some(&-1));
    }

    #[tokio::test]
    async fn get_mono_vertex_status_returns_unknown_before_rater_ready() {
        let svc = make_svc();
        let resp = svc
            .get_mono_vertex_status(Request::new(()))
            .await
            .expect("status response");
        let status = resp.into_inner().status.expect("status payload");

        assert_eq!(status.code, "D4");
    }

    #[tokio::test]
    async fn get_mono_vertex_errors_returns_empty_before_first_fetch() {
        let svc = make_svc();
        let resp = svc
            .get_mono_vertex_errors(Request::new(GetMonoVertexErrorsRequest {
                mono_vertex: "simple-mono-vertex".to_string(),
            }))
            .await
            .expect("errors response");

        // Cache is empty until the background task runs, so we expect an empty list.
        assert!(resp.into_inner().errors.is_empty());
    }
}
