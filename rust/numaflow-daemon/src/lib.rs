use numaflow_pb::servers::mvtxdaemon::mono_vertex_daemon_service_server::{
    MonoVertexDaemonService, MonoVertexDaemonServiceServer,
};
use numaflow_pb::servers::mvtxdaemon::{
    GetMonoVertexErrorsRequest, GetMonoVertexErrorsResponse, GetMonoVertexMetricsResponse,
    GetMonoVertexStatusResponse,
};
use std::error::Error;
use std::result::Result;
use tonic::transport::Server;
use tonic::{Request, Response, Status};
use tracing::info;

#[derive(Debug, Default)]
pub struct MvtxDaemonService;

#[tonic::async_trait]
impl MonoVertexDaemonService for MvtxDaemonService {
    async fn get_mono_vertex_metrics(
        &self,
        _: Request<()>,
    ) -> Result<Response<GetMonoVertexMetricsResponse>, Status> {
        println!("Keran: I received the call!");

        let reply = GetMonoVertexMetricsResponse { metrics: None };

        Ok(Response::new(reply))
    }

    async fn get_mono_vertex_status(
        &self,
        _: Request<()>,
    ) -> Result<Response<GetMonoVertexStatusResponse>, Status> {
        println!("Keran: I received the call!");

        let reply = GetMonoVertexStatusResponse { status: None };

        Ok(Response::new(reply))
    }

    async fn get_mono_vertex_errors(
        &self,
        _: Request<GetMonoVertexErrorsRequest>,
    ) -> Result<Response<GetMonoVertexErrorsResponse>, Status> {
        println!("Keran: I received the call!");

        let reply = GetMonoVertexErrorsResponse { errors: vec![] };

        Ok(Response::new(reply))
    }
}

/// Matches the DaemonServicePort in pkg/apis/numaflow/v1alpha1/const.go
const DAEMON_SERVICE_PORT: u16 = 4327;

pub async fn run_monovertex(mvtx_name: String) -> Result<(), Box<dyn Error>> {
    info!("MonoVertex name is {}", mvtx_name);

    let addr = format!("[::]:{}", DAEMON_SERVICE_PORT).parse()?;

    let service = MvtxDaemonService::default();

    Server::builder()
        .add_service(MonoVertexDaemonServiceServer::new(service))
        .serve(addr)
        .await?;

    Ok(())
}

pub async fn run_pipeline(pipeline_name: String) -> Result<(), Box<dyn Error>> {
    info!("Pipeline name is {}", pipeline_name);

    Ok(())
}
