use numaflow_pb::servers::mvtxdaemon::mono_vertex_daemon_service_server::MonoVertexDaemonService;
use numaflow_pb::servers::mvtxdaemon::{
    GetMonoVertexErrorsRequest, GetMonoVertexErrorsResponse, GetMonoVertexMetricsResponse,
    GetMonoVertexStatusResponse, MonoVertexMetrics, MonoVertexStatus, ReplicaErrors,
};
use rcgen::{
    CertificateParams, DistinguishedName, DnType, ExtendedKeyUsagePurpose, KeyPair, KeyUsagePurpose,
};
use rustls::ServerConfig;
use rustls::pki_types::{PrivateKeyDer, PrivatePkcs8KeyDer};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::result::Result;
use std::sync::Arc;
use time::{Duration, OffsetDateTime};
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio::task::JoinSet;
use tokio_rustls::TlsAcceptor;
use tokio_util::sync::CancellationToken;
use tonic::{Request, Response, Status};
use tracing::{info, warn};

type TlsStreamSender = mpsc::Sender<tokio_rustls::server::TlsStream<tokio::net::TcpStream>>;
type TlsStreamReceiver = mpsc::Receiver<tokio_rustls::server::TlsStream<tokio::net::TcpStream>>;

mod connection_acceptor;
mod error;
mod grpc_server;
mod http_server;

use connection_acceptor::ConnectionAcceptor;
use error::Error;
use grpc_server::run_grpc_server;
use http_server::run_http_server;

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

/// Matches the DaemonServicePort in pkg/apis/numaflow/v1alpha1/const.go
const DAEMON_SERVICE_PORT: u16 = 4327;

pub async fn run_monovertex(
    mvtx_name: String,
    cln_token: CancellationToken,
) -> Result<(), Box<dyn std::error::Error>> {
    info!("MonoVertex name is {}", mvtx_name);

    // Create a TCP listener that can listen to both h2 and http 1.1.
    let addr: SocketAddr = format!("[::]:{}", DAEMON_SERVICE_PORT).parse()?;
    let tcp_listener = TcpListener::bind(addr).await?;
    let tls_config = generate_self_signed_tls_config()?;
    let tls_acceptor = TlsAcceptor::from(tls_config);

    // Create two channels, one serving gRPC requests, the other HTTP.
    // Given the request rate a daemon server expect to receive, a buffer size of 1000 should be sufficent.
    // Buffer size 1000 is sufficient for the expected request rate.
    let (grpc_tx, grpc_rx): (TlsStreamSender, TlsStreamReceiver) = mpsc::channel(1000);
    let (http_tx, http_rx): (TlsStreamSender, TlsStreamReceiver) = mpsc::channel(1000);

    // Use a join set to manage spawned tasks.
    let mut join_set = JoinSet::new();

    // Start a tokio task to accept tcp connections.
    let cln_token_copy_1 = cln_token.clone();
    join_set.spawn(async move {
        let acceptor = ConnectionAcceptor::new(
            tcp_listener,
            tls_acceptor,
            grpc_tx,
            http_tx,
            cln_token_copy_1,
        );
        if let Err(error) = acceptor.run().await {
            warn!(error = %error, "Connection acceptor failed");
        }
    });

    // Start a tokio task to serve gRPC requests.
    let cln_token_copy_2 = cln_token.clone();
    join_set.spawn(async move {
        if let Err(error) = run_grpc_server(grpc_rx, cln_token_copy_2).await {
            warn!(error = %error, "gRPC server failed");
        }
    });

    // Start a tokio task to serve HTTP requests.
    join_set.spawn(async move {
        if let Err(error) = run_http_server(http_rx).await {
            warn!(error = %error, "HTTP server failed");
        }
    });

    while let Some(res) = join_set.join_next().await {
        if let Err(join_err) = res {
            warn!(error = %join_err, "Daemon task failed waiting for completion");
        }
    }

    Ok(())
}

fn generate_self_signed_tls_config() -> Result<Arc<ServerConfig>, Error> {
    let mut params = CertificateParams::new(vec!["localhost".to_string()]).map_err(|e| {
        Error::ConnConfig(format!("Failed to create certificate parameters: {}", e))
    })?;

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

    let signing_key = KeyPair::generate()
        .map_err(|e| Error::ConnConfig(format!("Failed to generate signing key: {}", e)))?;

    let cert = params.self_signed(&signing_key).map_err(|e| {
        Error::ConnConfig(format!("Failed to generate self-signed certificate: {}", e))
    })?;

    let cert_der = cert.der().clone();
    let key_der = PrivatePkcs8KeyDer::from(signing_key.serialize_der());
    let key_der = PrivateKeyDer::from(key_der);

    let mut cfg = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(vec![cert_der], key_der)
        .map_err(|e| Error::ConnConfig(format!("Failed to build server config: {}", e)))?;

    // Serve both http and gRPC.
    // Note: order matters, most preferred first.
    // We choose http/1.1 first because it's more widely supported.
    cfg.alpn_protocols = vec![b"http/1.1".to_vec(), b"h2".to_vec()];

    Ok(Arc::new(cfg))
}
