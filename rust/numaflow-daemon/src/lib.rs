use numaflow_pb::servers::mvtxdaemon::mono_vertex_daemon_service_server::{
    MonoVertexDaemonService, MonoVertexDaemonServiceServer,
};
use numaflow_pb::servers::mvtxdaemon::{
    GetMonoVertexErrorsRequest, GetMonoVertexErrorsResponse, GetMonoVertexMetricsResponse,
    GetMonoVertexStatusResponse, MonoVertexMetrics, MonoVertexStatus, ReplicaErrors,
};
use rcgen::{
    CertificateParams, DistinguishedName, DnType, ExtendedKeyUsagePurpose, KeyPair, KeyUsagePurpose,
};
use std::collections::HashMap;
use std::error::Error;
use std::result::Result;
use time::{Duration, OffsetDateTime};
use tonic::transport::{Identity, Server, ServerTlsConfig};
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
        info!("Received GetMonoVertexMetrics");

        let mock_processing_rates = HashMap::from([
            ("default".to_string(), 67.0),
            ("1m".to_string(), 10.0),
            ("5m".to_string(), 50.0),
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
        info!("Received GetMonoVertexStatus");

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
        info!("Received GetMonoVertexErrors");

        let mock_resp = GetMonoVertexErrorsResponse {
            errors: vec![ReplicaErrors {
                replica: "mock_replica".to_string(),
                container_errors: vec![],
            }],
        };

        Ok(Response::new(mock_resp))
    }
}

/// Matches the DaemonServicePort in pkg/apis/numaflow/v1alpha1/const.go
const DAEMON_SERVICE_PORT: u16 = 4327;

pub async fn run_monovertex(mvtx_name: String) -> Result<(), Box<dyn Error>> {
    info!("MonoVertex name is {}", mvtx_name);

    let addr = format!("[::]:{}", DAEMON_SERVICE_PORT).parse()?;

    let service = MvtxDaemonService;
    let identity = generate_self_signed_identity()?;
    let tls = ServerTlsConfig::new().identity(identity);

    Server::builder()
        .tls_config(tls)?
        .add_service(MonoVertexDaemonServiceServer::new(service))
        .serve(addr)
        .await?;

    Ok(())
}

pub async fn run_pipeline(pipeline_name: String) -> Result<(), Box<dyn Error>> {
    info!("Pipeline name is {}", pipeline_name);

    Ok(())
}

fn generate_self_signed_identity() -> Result<Identity, Box<dyn Error>> {
    let mut params = CertificateParams::new(vec!["localhost".to_string()])?;

    let mut dn = DistinguishedName::new();
    dn.push(DnType::OrganizationName, "Numaproj");
    params.distinguished_name = dn;

    let not_before = OffsetDateTime::now_utc();
    params.not_before = not_before;
    params.not_after = not_before + Duration::days(365);

    params.key_usages = vec![
        KeyUsagePurpose::KeyEncipherment,
        KeyUsagePurpose::DigitalSignature,
    ];

    params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];

    let signing_key = KeyPair::generate()?;
    let cert = params.self_signed(&signing_key)?;

    Ok(Identity::from_pem(cert.pem(), signing_key.serialize_pem()))
}
