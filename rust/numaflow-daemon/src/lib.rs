use bytes::Bytes;
use http::{HeaderValue, Request as HttpRequest, Response as HttpResponse, StatusCode, header};
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
use prometheus_client::{encoding::text::encode, registry::Registry};
use rcgen::{
    CertificateParams, DistinguishedName, DnType, ExtendedKeyUsagePurpose, KeyPair, KeyUsagePurpose,
};
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::{ServerConfig, pki_types::PrivatePkcs8KeyDer};
use std::collections::HashMap;
use std::convert::Infallible;
use std::error::Error;
use std::net::SocketAddr;
use std::result::Result;
use std::sync::Arc;
use time::{Duration, OffsetDateTime};
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio_rustls::TlsAcceptor;
use tokio_stream::{StreamExt, wrappers::ReceiverStream};
use tonic::transport::Server;
use tonic::{Request, Response, Status};
use tracing::{info, warn};

#[derive(Debug)]
pub struct MvtxDaemonService {
    mvtx_name: String,
}

impl MvtxDaemonService {
    pub fn new(mvtx_name: String) -> Self {
        Self { mvtx_name }
    }
}

#[tonic::async_trait]
impl MonoVertexDaemonService for MvtxDaemonService {
    async fn get_mono_vertex_metrics(
        &self,
        _: Request<()>,
    ) -> Result<Response<GetMonoVertexMetricsResponse>, Status> {
        info!("gRPC: Received GetMonoVertexMetrics");

        // Mock response (useful for UI/dev testing). Real implementation will populate these
        // values from rater/runtime cache.
        let resp = GetMonoVertexMetricsResponse {
            metrics: Some(MonoVertexMetrics {
                mono_vertex: self.mvtx_name.clone(),
                processing_rates: mock_processing_rates(),
                pendings: mock_pendings(),
            }),
        };

        Ok(Response::new(resp))
    }

    async fn get_mono_vertex_status(
        &self,
        _: Request<()>,
    ) -> Result<Response<GetMonoVertexStatusResponse>, Status> {
        info!("gRPC: Received GetMonoVertexStatus");

        // Minimal response that won't crash callers (server handler expects Status != nil).
        let resp = GetMonoVertexStatusResponse {
            status: Some(MonoVertexStatus {
                status: "unknown".to_string(),
                message: "".to_string(),
                code: "".to_string(),
            }),
        };

        Ok(Response::new(resp))
    }

    async fn get_mono_vertex_errors(
        &self,
        _: Request<GetMonoVertexErrorsRequest>,
    ) -> Result<Response<GetMonoVertexErrorsResponse>, Status> {
        info!("gRPC: Received GetMonoVertexErrors");

        let resp = GetMonoVertexErrorsResponse {
            errors: Vec::<ReplicaErrors>::new(),
        };

        Ok(Response::new(resp))
    }
}

fn mock_processing_rates() -> HashMap<String, f64> {
    // Keys align with the Go daemon convention: 1m, 5m, 15m, default.
    HashMap::from([
        ("1m".to_string(), 100.0),
        ("5m".to_string(), 90.0),
        ("15m".to_string(), 80.0),
        ("default".to_string(), 90.0),
    ])
}

fn mock_pendings() -> HashMap<String, i64> {
    HashMap::from([
        ("1m".to_string(), 0),
        ("5m".to_string(), 0),
        ("15m".to_string(), 0),
        ("default".to_string(), 0),
    ])
}

/// Matches the DaemonServicePort in pkg/apis/numaflow/v1alpha1/const.go
const DAEMON_SERVICE_PORT: u16 = 4327;

pub async fn run_monovertex(mvtx_name: String) -> Result<(), Box<dyn Error>> {
    info!("MonoVertex name is {}", mvtx_name);

    let addr: SocketAddr = format!("[::]:{}", DAEMON_SERVICE_PORT).parse()?;

    // One TCP listener; TLS + ALPN will route to either HTTP/1.1 or gRPC(h2).
    let tcp_listener = TcpListener::bind(addr).await?;

    let tls_config = generate_self_signed_tls_config()?;
    let acceptor = TlsAcceptor::from(tls_config);

    // Shared business logic behind both gRPC and HTTP.
    let service = Arc::new(MvtxDaemonService::new(mvtx_name));
    let grpc_service = MonoVertexDaemonServiceServer::from_arc(service.clone());

    // Route connections based on ALPN: h2 => gRPC, http/1.1 => HTTP.
    let (grpc_tx, grpc_rx) = mpsc::channel(1024);
    let (http_tx, http_rx) = mpsc::channel(1024);

    let _accept_task = tokio::spawn(async move {
        loop {
            let (tcp, peer_addr) = match tcp_listener.accept().await {
                Ok(v) => v,
                Err(e) => {
                    warn!(error = %e, "Failed to accept TCP connection");
                    continue;
                }
            };

            let acceptor = acceptor.clone();
            let grpc_tx = grpc_tx.clone();
            let http_tx = http_tx.clone();

            tokio::spawn(async move {
                let tls_stream = match acceptor.accept(tcp).await {
                    Ok(s) => s,
                    Err(e) => {
                        warn!(peer_addr = %peer_addr, error = %e, "TLS handshake failed");
                        return;
                    }
                };

                let alpn = tls_stream
                    .get_ref()
                    .1
                    .alpn_protocol()
                    .map(|p| String::from_utf8_lossy(p).into_owned());

                match alpn.as_deref() {
                    Some("h2") => {
                        info!(
                            peer_addr = %peer_addr,
                            alpn = "h2",
                            "Routing connection to gRPC server (HTTP/2)"
                        );
                        let _ = grpc_tx.send(tls_stream).await;
                    }
                    Some("http/1.1") => {
                        info!(
                            peer_addr = %peer_addr,
                            alpn = "http/1.1",
                            "Routing connection to HTTP/1.1 server"
                        );
                        let _ = http_tx.send(tls_stream).await;
                    }
                    Some(other) => {
                        info!(
                            peer_addr = %peer_addr,
                            alpn = %other,
                            "Unknown ALPN; defaulting to HTTP/1.1 server"
                        );
                        let _ = http_tx.send(tls_stream).await;
                    }
                    None => {
                        info!(
                            peer_addr = %peer_addr,
                            "No ALPN negotiated; defaulting to HTTP/1.1 server"
                        );
                        let _ = http_tx.send(tls_stream).await;
                    }
                }
            });
        }
    });

    let grpc_task = tokio::spawn(async move {
        let incoming = ReceiverStream::new(grpc_rx).map(Ok::<_, std::io::Error>);
        Server::builder()
            .add_service(grpc_service)
            .serve_with_incoming(incoming)
            .await
    });

    let _http_task = tokio::spawn(async move {
        run_http1_server(http_rx, service).await;
    });

    // Wait for the gRPC server to exit (accept loop runs forever).
    // If gRPC server exits with error, bubble it up.
    let grpc_res = grpc_task.await?;
    grpc_res?;

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

    let cert_der: CertificateDer<'static> = cert.der().clone();
    let key_der = PrivatePkcs8KeyDer::from(signing_key.serialize_der());
    let key_der: PrivateKeyDer<'static> = PrivateKeyDer::from(key_der);

    let mut cfg = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(vec![cert_der], key_der)?;

    // Parity with Go daemon: advertise both so we can serve gRPC and HTTP/1.1 on the same port.
    cfg.alpn_protocols = vec![b"http/1.1".to_vec(), b"h2".to_vec()];
    Ok(Arc::new(cfg))
}

async fn run_http1_server(
    mut rx: mpsc::Receiver<tokio_rustls::server::TlsStream<tokio::net::TcpStream>>,
    service: Arc<MvtxDaemonService>,
) {
    while let Some(stream) = rx.recv().await {
        let service = service.clone();
        tokio::spawn(async move {
            let io = TokioIo::new(stream);
            let svc = service_fn(move |req| http_handler(req, service.clone()));
            let _ = http1::Builder::new().serve_connection(io, svc).await;
        });
    }
}

async fn http_handler(
    req: HttpRequest<Incoming>,
    service: Arc<MvtxDaemonService>,
) -> Result<HttpResponse<Full<Bytes>>, Infallible> {
    let method = req.method().clone();
    let path = req.uri().path().to_string();

    info!(transport = "http1", method = %method, path = %path, "HTTP request received");

    let resp = match (method.as_str(), path.as_str()) {
        ("GET", "/readyz") | ("GET", "/livez") => HttpResponse::builder()
            .status(StatusCode::NO_CONTENT)
            .body(Full::new(Bytes::new()))
            .unwrap(),

        ("GET", "/metrics") => {
            // Provide a valid Prometheus text format endpoint (even if empty for now).
            let registry = Registry::default();
            let mut buf = String::new();
            let _ = encode(&mut buf, &registry);
            HttpResponse::builder()
                .status(StatusCode::OK)
                .header(
                    header::CONTENT_TYPE,
                    HeaderValue::from_static("text/plain; version=0.0.4; charset=utf-8"),
                )
                .body(Full::new(Bytes::from(buf)))
                .unwrap()
        }

        ("GET", "/api/v1/metrics") => match service.get_mono_vertex_metrics(Request::new(())).await
        {
            Ok(rsp) => json_ok(mvtx_metrics_resp_json(&rsp.into_inner())),
            Err(st) => grpc_error_to_http(st),
        },

        ("GET", "/api/v1/status") => match service.get_mono_vertex_status(Request::new(())).await {
            Ok(rsp) => json_ok(mvtx_status_resp_json(&rsp.into_inner())),
            Err(st) => grpc_error_to_http(st),
        },

        // /api/v1/mono-vertices/{monoVertex}/errors
        ("GET", p) if p.starts_with("/api/v1/mono-vertices/") && p.ends_with("/errors") => {
            let mono_vertex = p
                .trim_start_matches("/api/v1/mono-vertices/")
                .trim_end_matches("/errors")
                .trim_matches('/');

            let req = GetMonoVertexErrorsRequest {
                mono_vertex: mono_vertex.to_string(),
            };
            match service.get_mono_vertex_errors(Request::new(req)).await {
                Ok(rsp) => json_ok(mvtx_errors_resp_json(&rsp.into_inner())),
                Err(st) => grpc_error_to_http(st),
            }
        }

        _ => HttpResponse::builder()
            .status(StatusCode::NOT_FOUND)
            .body(Full::new(Bytes::new()))
            .unwrap(),
    };

    Ok(resp)
}

fn json_ok(val: serde_json::Value) -> HttpResponse<Full<Bytes>> {
    let body = serde_json::to_vec(&val).unwrap_or_default();
    HttpResponse::builder()
        .status(StatusCode::OK)
        .header(
            header::CONTENT_TYPE,
            HeaderValue::from_static("application/json"),
        )
        .body(Full::new(Bytes::from(body)))
        .unwrap()
}

fn mvtx_metrics_resp_json(resp: &GetMonoVertexMetricsResponse) -> serde_json::Value {
    match resp.metrics.as_ref() {
        Some(m) => serde_json::json!({
            "metrics": {
                "monoVertex": m.mono_vertex,
                "processingRates": m.processing_rates,
                "pendings": m.pendings,
            }
        }),
        None => serde_json::json!({}),
    }
}

fn mvtx_status_resp_json(resp: &GetMonoVertexStatusResponse) -> serde_json::Value {
    match resp.status.as_ref() {
        Some(s) => serde_json::json!({
            "status": {
                "status": s.status,
                "message": s.message,
                "code": s.code,
            }
        }),
        None => serde_json::json!({}),
    }
}

fn mvtx_errors_resp_json(resp: &GetMonoVertexErrorsResponse) -> serde_json::Value {
    // Note: protobuf JSON mapping would omit empty repeated fields; returning "errors": []
    // is still accepted by the existing Go JSONPb unmarshaller.
    let errors = resp
        .errors
        .iter()
        .map(|re| {
            let container_errors = re
                .container_errors
                .iter()
                .map(|ce| {
                    let mut obj = serde_json::Map::new();
                    obj.insert(
                        "container".to_string(),
                        serde_json::Value::String(ce.container.clone()),
                    );
                    if let Some(ts) = ce
                        .timestamp
                        .as_ref()
                        .and_then(format_proto_timestamp_rfc3339)
                    {
                        obj.insert("timestamp".to_string(), serde_json::Value::String(ts));
                    }
                    obj.insert(
                        "code".to_string(),
                        serde_json::Value::String(ce.code.clone()),
                    );
                    obj.insert(
                        "message".to_string(),
                        serde_json::Value::String(ce.message.clone()),
                    );
                    obj.insert(
                        "details".to_string(),
                        serde_json::Value::String(ce.details.clone()),
                    );
                    serde_json::Value::Object(obj)
                })
                .collect::<Vec<_>>();

            serde_json::json!({
                "replica": re.replica,
                "containerErrors": container_errors,
            })
        })
        .collect::<Vec<_>>();

    serde_json::json!({ "errors": errors })
}

fn format_proto_timestamp_rfc3339(ts: &prost_types::Timestamp) -> Option<String> {
    use time::format_description::well_known::Rfc3339;

    let base = OffsetDateTime::from_unix_timestamp(ts.seconds).ok()?;
    let dt = base.checked_add(Duration::nanoseconds(ts.nanos as i64))?;
    dt.format(&Rfc3339).ok()
}

fn grpc_error_to_http(status: Status) -> HttpResponse<Full<Bytes>> {
    let http_status = match status.code() {
        tonic::Code::InvalidArgument => StatusCode::BAD_REQUEST,
        tonic::Code::NotFound => StatusCode::NOT_FOUND,
        tonic::Code::AlreadyExists => StatusCode::CONFLICT,
        tonic::Code::PermissionDenied => StatusCode::FORBIDDEN,
        tonic::Code::Unauthenticated => StatusCode::UNAUTHORIZED,
        tonic::Code::Unavailable => StatusCode::SERVICE_UNAVAILABLE,
        tonic::Code::Unimplemented => StatusCode::NOT_IMPLEMENTED,
        _ => StatusCode::INTERNAL_SERVER_ERROR,
    };

    // Minimal error shape; grpc-gateway uses a richer structure but callers generally key off HTTP status.
    let body = serde_json::json!({
        "code": status.code() as i32,
        "message": status.message(),
    });

    HttpResponse::builder()
        .status(http_status)
        .header(
            header::CONTENT_TYPE,
            HeaderValue::from_static("application/json"),
        )
        .body(Full::new(Bytes::from(body.to_string())))
        .unwrap()
}
