use bytes::Bytes;
use http::{Request as HttpRequest, Response as HttpResponse, StatusCode};
use http_body_util::Full;
use hyper::body::Incoming;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper_util::rt::TokioIo;
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
use rustls::ServerConfig;
use rustls::pki_types::{PrivateKeyDer, PrivatePkcs8KeyDer};
use std::collections::HashMap;
use std::convert::Infallible;
use std::error::Error;
use std::net::SocketAddr;
use std::result::Result;
use std::sync::Arc;
use time::{Duration, OffsetDateTime};
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio::task::JoinSet;
use tokio_rustls::TlsAcceptor;
use tokio_stream::StreamExt;
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;
use tonic::transport::Server;
use tonic::{Request, Response, Status};
use tracing::{info, warn};

pub struct MvtxDaemonService;

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
) -> Result<(), Box<dyn Error>> {
    info!("MonoVertex name is {}", mvtx_name);

    // Create a TCP listener that can listen to both h2 and http 1.1.
    let addr: SocketAddr = format!("[::]:{}", DAEMON_SERVICE_PORT).parse()?;
    let tcp_listener = TcpListener::bind(addr).await?;
    let tls_config = generate_self_signed_tls_config()?;
    let tls_acceptor = TlsAcceptor::from(tls_config);

    // Create two channels, one serving gRPC requests, the other HTTP.
    // Given the request rate a daemon server expect to receive, a buffer size of 1000 should be sufficent.
    let (grpc_tx, grpc_rx) = mpsc::channel(1000);
    let (http_tx, mut http_rx) = mpsc::channel(1000);

    // Use a join set to manage spawned tasks.
    let mut join_set = JoinSet::new();

    // Start a tokio task to accept tcp connections.
    let cln_token_copy_1 = cln_token.clone();
    join_set.spawn(async move {
        let mut conn_set = JoinSet::new();
        loop {
            tokio::select! {
                _ = cln_token_copy_1.cancelled() => {
                    info!("Cancellation token triggered. Stopping accepting new connections.");
                    // Close both gRPC and HTTP channels.
                    drop(grpc_tx);
                    drop(http_tx);
                    break;
                }
                accept_res = tcp_listener.accept() => {
                    // Accept a connection.
                    let (tcp, peer_addr) = match accept_res {
                        Ok(v) => v,
                        Err(e) => {
                            warn!(error = %e, "Failed to accept a TCP connection");
                            continue;
                        }
                    };

                    // Handle the new connection.
                    // Start a new tokio task so that we don't block accepting other connections.
                    let grpc_sender = grpc_tx.clone();
                    let http_sender = http_tx.clone();
                    let acceptor = tls_acceptor.clone();

                    conn_set.spawn(async move {
                        let stream = match acceptor.accept(tcp).await {
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
                                let _ = http_sender.send(stream).await;
                            }
                            Some("h2") => {
                                // Send to the gRPC channel.
                                let _ = grpc_sender.send(stream).await;
                            }
                            _ => {
                                // Send to the HTTP channel by default.
                                // This is because most of the time, HTTP is used for communication.
                                // On Numaflow, if a client is sending a gRPC request, the h2 protocol is explicitly used.
                                let _ = http_sender.send(stream).await;
                            }
                        }
                    });
                }
            }
        }

        while let Some(res) = conn_set.join_next().await {
            if let Err(join_err) = res {
                warn!(error = %join_err, "TCP connection task failed");
            }
        }
    });

    // Start a tokio task to serve gRPC requests.
    let cln_token_copy_2 = cln_token.clone();
    join_set.spawn(async move {
        let grpc_service = MonoVertexDaemonServiceServer::new(MvtxDaemonService);
        let incoming_stream = ReceiverStream::new(grpc_rx).map(Ok::<_, std::io::Error>);
        let _ = Server::builder()
            .add_service(grpc_service)
            .serve_with_incoming_shutdown(incoming_stream, cln_token_copy_2.cancelled())
            .await;
    });

    // Start a tokio task to serve HTTP requests.
    join_set.spawn(async move {
        // TODO - _svc_for_http will be used for serving HTTP requests.
        let _svc_for_http = MvtxDaemonService;
        let mut conn_set = JoinSet::new();
        while let Some(stream) = http_rx.recv().await {
            conn_set.spawn(async move {
                let svc = service_fn(|req: HttpRequest<Incoming>| async move {
                    let method = req.method().clone();
                    let path = req.uri().path().to_string();

                    let resp = match (method.as_str(), path.as_str()) {
                        ("GET", "/readyz" | "/livez") => HttpResponse::builder()
                            .status(StatusCode::NO_CONTENT)
                            .body(Full::new(Bytes::new()))
                            .unwrap(),
                        // TODO - add remaining endpoints.
                        _ => HttpResponse::builder()
                            .status(StatusCode::NOT_FOUND)
                            .body(Full::new(Bytes::new()))
                            .unwrap(),
                    };

                    // Every error case is translated to a corresponding Response, hence infallible.
                    Ok::<HttpResponse<Full<Bytes>>, Infallible>(resp)
                });
                let io = TokioIo::new(stream);
                let _ = http1::Builder::new().serve_connection(io, svc).await;
            });
        }

        while let Some(res) = conn_set.join_next().await {
            if let Err(join_err) = res {
                warn!(error = %join_err, "HTTP connection task failed");
            }
        }
    });

    while let Some(res) = join_set.join_next().await {
        if let Err(join_err) = res {
            warn!(error = %join_err, "Daemon task failed waiting for completion");
        }
    }

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

    // Serve both http and gRPC.
    // Note: order matters, most preferred first.
    // We choose http/1.1 first because it's more widely supported.
    cfg.alpn_protocols = vec![b"http/1.1".to_vec(), b"h2".to_vec()];

    Ok(Arc::new(cfg))
}
