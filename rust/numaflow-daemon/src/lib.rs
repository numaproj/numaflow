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
use std::net::SocketAddr;
use std::result::Result;
use std::sync::Arc;
use std::sync::mpsc;
use time::{Duration, OffsetDateTime};
use tokio::net::TcpListener;
use tonic::{Request, Response, Status};
use tracing::{info, warn};

use rustls::ServerConfig;
use rustls::pki_types::{PrivateKeyDer, PrivatePkcs8KeyDer};
use tokio_rustls::TlsAcceptor;

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

    // 0. Create a TCP listener that can listen to both h2 and http 1.1.
    let addr: SocketAddr = format!("[::]:{}", DAEMON_SERVICE_PORT).parse()?;
    let tcp_listener = TcpListener::bind(addr).await?;
    let tls_config = generate_self_signed_tls_config()?;
    let tls_acceptor = TlsAcceptor::from(tls_config);

    // 1. Create the gRPC service.
    // The service is shared by both gRPC and HTTP server.
    let daemon_service = MonoVertexDaemonServiceServer::new(MvtxDaemonService);

    // 2. Create two channels, one serving gRPC requests, the other HTTP.
    let (g_sender, g_receiver) = mpsc::channel();
    let (h_sender, h_receiver) = mpsc::channel();

    // 3. Start a thread to accept requests.
    // Create a TLS acceptor.
    // Loop:
    //  receive a request.
    //  if HTTP, send it to HTTP channel.
    //  if gRPC, send it to gRPC channel.
    //  others, TODO
    let _ = tokio::spawn(async move {
        loop {
            let (tcp, peer_addr) = match tcp_listener.accept().await {
                Ok(v) => v,
                Err(e) => {
                    // TODO - what's numaflow rust's standard way of printing a warn?
                    warn!("ERROR: Failed to accept a TCP connection");
                    continue;
                }
            };
            // Handle the new connection.
            // Start a new thread so that we don't block on receiving other connections.
            tokio::spawn(async move {
                let stream = match tls_acceptor.accept(tcp).await {
                    Ok(s) => s,
                    Err(e) => {
                        warn!(peer_addr = %peer_addr, error = %e, "TLS handshake failed.");
                        // TLS handshake failed, skip handling this connection.
                        return;
                    }
                };

                let alpn = stream
                    .get_ref()
                    .1
                    .alpn_protocol()
                    .map(|p| String::from_utf8_lossy(p).into_owned());

                match alpn.as_deref() {
                    Some("http/1.1") => {
                        // Send to the HTTP channel.
                        todo!()
                    }
                    Some("h2") => {
                        // Send to the gRPC channel.
                        todo!()
                    }
                    _ => {
                        // Send to the gRPC channel by default
                    }
                }
            });
        }
    });

    // 4. Start a thread to serve gRPC requests.

    // 5. Start a thread to serve HTTP requests.

    // 6. Gracefully shutdown.
    Ok(())
}

pub async fn run_pipeline(pipeline_name: String) -> Result<(), Box<dyn Error>> {
    info!("Pipeline name is {}", pipeline_name);

    Ok(())
}

fn generate_self_signed_tls_config() -> Result<Arc<ServerConfig>, Box<dyn Error>> {
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

    let cert_der = cert.der().clone();
    let key_der = PrivatePkcs8KeyDer::from(signing_key.serialize_der());
    let key_der = PrivateKeyDer::from(key_der);

    let mut cfg = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(vec![cert_der], key_der)?;

    // Serve both gRPC and HTTP/1.1
    cfg.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];

    Ok(Arc::new(cfg))
}
