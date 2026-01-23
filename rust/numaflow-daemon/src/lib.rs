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
        todo!();
    }

    async fn get_mono_vertex_status(
        &self,
        _: Request<()>,
    ) -> Result<Response<GetMonoVertexStatusResponse>, Status> {
        println!("Keran: I received the call!");
        todo!();
    }

    async fn get_mono_vertex_errors(
        &self,
        _: Request<GetMonoVertexErrorsRequest>,
    ) -> Result<Response<GetMonoVertexErrorsResponse>, Status> {
        todo!();
    }
}

/// 4327

pub async fn run_monovertex(mvtx_name: String) -> Result<(), Box<dyn Error>> {
    info!("MonoVertex name is {}", mvtx_name);

    let addr = "[::]:4327".parse()?;

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
