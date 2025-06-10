//! HTTP source for Numaflow.
//! There are two endpoints, one for health (`/health`) and another for data (`/data`).
//! The `/vertices/` endpoint is a `POST` endpoint that accepts all content-types of data. The
//! headers are propagated as is to the next vertex in the pipeline.
//! `X-Numaflow-Id` header is added to the message to track the message across the pipeline.
//! `X-Numaflow-Event-Time` is added to the message to track the event time of the message.

use axum::body::Body;
use axum::http::{HeaderValue, Request};
use axum::middleware::Next;
use axum::{
    Router,
    extract::State,
    http::{HeaderMap, StatusCode},
    middleware,
    response::IntoResponse,
    routing::{get, post},
};
use axum_server::tls_rustls::RustlsConfig;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use rcgen::{Certificate, CertifiedKey, generate_simple_self_signed};
use serde::Serialize;
use std::collections::HashMap;
use std::fmt::Debug;
use std::net::SocketAddr;

use std::time::Duration;
use tokio::sync::{mpsc, oneshot};
use tracing::{debug, error, info, trace, warn};
use uuid::Uuid;

/// HTTP source that manages incoming HTTP requests and forwards them to a processing channel
struct HttpSourceActor {
    server_rx: mpsc::Receiver<HttpMessage>,
    _server_handle: tokio::task::JoinHandle<Result<()>>,
    timeout: Duration,
}

/// Error types for the HTTP source
#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Channel send error: {0}")]
    ChannelSend(String),
    #[error("Channel recv error: {0}")]
    ChannelRecv(String),
    #[error("Channel Full: 429")]
    ChannelFull(),
    #[error("Server error: {0}")]
    Server(String),
}

type Result<T> = std::result::Result<T, Error>;

/// HTTP message containing the request body and headers
#[derive(Debug)]
pub struct HttpMessage {
    pub body: Bytes,
    pub headers: HashMap<String, String>,
    pub event_time: DateTime<Utc>,
    pub id: String,
}

/// Response for successful data ingestion
#[derive(Serialize)]
struct DataResponse {
    message: String,
    id: String,
}

/// HTTPSource Builder with custom buffer size
#[derive(Clone, PartialEq)]
pub struct HttpSourceConfig {
    pub vertex_name: &'static str,
    /// Default buffer size is 500
    pub buffer_size: usize,
    pub addr: SocketAddr,
    pub timeout: Duration,
    pub token: Option<&'static str>,
}

impl Debug for HttpSourceConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HttpSourceConfig")
            .field("batch_size", &self.buffer_size)
            .field("read_timeout", &self.timeout)
            .field("addr", &self.addr)
            .field("token", &self.token.as_ref().map(|_| "*****"))
            .finish()
    }
}

impl Default for HttpSourceConfig {
    fn default() -> Self {
        Self {
            vertex_name: "in",
            buffer_size: 500,
            addr: "0.0.0.0:8443".parse().expect("Invalid address"),
            timeout: Duration::from_secs(1),
            token: None,
        }
    }
}

pub struct HttpSourceConfigBuilder {
    vertex_name: &'static str,
    buffer_size: Option<usize>,
    addr: Option<SocketAddr>,
    timeout: Option<Duration>,
    token: Option<&'static str>,
}

impl HttpSourceConfigBuilder {
    pub fn new(vertex_name: &'static str) -> Self {
        Self {
            vertex_name,
            buffer_size: None,
            addr: None,
            timeout: None,
            token: None,
        }
    }

    pub fn buffer_size(mut self, buffer_size: usize) -> Self {
        self.buffer_size = Some(buffer_size);
        self
    }

    pub fn addr(mut self, addr: SocketAddr) -> Self {
        self.addr = Some(addr);
        self
    }

    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    pub fn token(mut self, token: &'static str) -> Self {
        self.token = Some(token);
        self
    }

    pub fn build(self) -> HttpSourceConfig {
        HttpSourceConfig {
            vertex_name: self.vertex_name,
            buffer_size: self.buffer_size.unwrap_or(500),
            addr: self
                .addr
                .unwrap_or_else(|| "0.0.0.0:8443".parse().expect("Invalid address")),
            timeout: self.timeout.unwrap_or(Duration::from_secs(1)),
            token: self.token,
        }
    }
}

#[derive(Debug)]
pub enum HttpActorMessage {
    Read {
        size: usize,
        response_tx: oneshot::Sender<Result<Vec<HttpMessage>>>,
    },
    Ack {
        offsets: Vec<String>,
        response_tx: oneshot::Sender<Result<()>>,
    },
    Pending(oneshot::Sender<Option<usize>>),
}

impl HttpSourceActor {
    async fn new(http_source_config: HttpSourceConfig) -> Self {
        let (tx, rx) = mpsc::channel(http_source_config.buffer_size); // Increased buffer size for better throughput

        let server_handle = tokio::spawn(start_server(
            http_source_config.vertex_name,
            tx,
            http_source_config.addr,
            http_source_config.token,
        ));

        Self {
            server_rx: rx,
            timeout: http_source_config.timeout,
            _server_handle: server_handle,
        }
    }

    /// Background task that processes messages from the channel
    async fn run(mut self, mut actor_rx: mpsc::Receiver<HttpActorMessage>) -> Result<()> {
        info!("HttpSource processor started");

        while let Some(msg) = actor_rx.recv().await {
            match msg {
                HttpActorMessage::Read { size, response_tx } => {
                    let messages = self.read(size).await;
                    debug!("Reading messages from HttpSource");
                    response_tx.send(messages).expect("rx should be open");
                }
                HttpActorMessage::Ack {
                    offsets,
                    response_tx,
                } => {
                    debug!(count = offsets.len(), "Ack'ing messages from HttpSource");
                    response_tx
                        .send(self.ack(offsets).await)
                        .expect("rx should be open");
                }
                HttpActorMessage::Pending(response_tx) => {
                    let pending = self.pending().await;
                    debug!(?pending, "Pending messages from HttpSource");
                    response_tx.send(pending).expect("rx should be open");
                }
            }
        }

        info!("HttpSource processor stopped");
        Ok(())
    }

    async fn pending(&self) -> Option<usize> {
        Some(self.server_rx.len())
    }

    async fn read(&mut self, count: usize) -> Result<Vec<HttpMessage>> {
        // return all the messages in self.server_rx as long as timeout is not reached and not
        // exceeding the count.

        let mut messages = vec![];

        let timeout = tokio::time::timeout(self.timeout, std::future::pending::<()>());
        tokio::pin!(timeout);

        loop {
            tokio::select! {
                biased;

                _ =  &mut timeout => {
                    return Ok(messages);
                }

                message = self.server_rx.recv() => {
                    // stream ended
                    let Some(message) = message else {
                        return Ok(messages);
                    };
                    messages.push(message);
                }
            }

            if messages.len() >= count {
                return Ok(messages);
            }
        }
    }

    /// http source cannot implement ack, we have already returned 202 or 429 accordingly
    async fn ack(&self, _offsets: Vec<String>) -> Result<()> {
        Ok(())
    }
}

/// Handle to interact with the HttpSource actor
#[derive(Clone)]
pub struct HttpSourceHandle {
    actor_tx: mpsc::Sender<HttpActorMessage>,
}

impl HttpSourceHandle {
    /// Create a new HttpSourceHandle
    pub async fn new(http_source_config: HttpSourceConfig) -> Self {
        let (actor_tx, actor_rx) =
            mpsc::channel::<HttpActorMessage>(http_source_config.buffer_size);

        let http_source = HttpSourceActor::new(http_source_config).await;

        // the tokio task will stop when tx is dropped
        tokio::spawn(async move { http_source.run(actor_rx).await });

        Self { actor_tx }
    }

    /// Read messages from the HttpSource.
    pub async fn read(&self, size: usize) -> Result<Vec<HttpMessage>> {
        let (tx, rx) = oneshot::channel();
        self.actor_tx
            .send(HttpActorMessage::Read {
                size,
                response_tx: tx,
            })
            .await
            .expect("actor should be running");
        rx.await.map_err(|e| Error::ChannelRecv(e.to_string()))?
    }

    /// Ack messages to the HttpSource.
    pub async fn ack(&self, offsets: Vec<String>) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.actor_tx
            .send(HttpActorMessage::Ack {
                offsets,
                response_tx: tx,
            })
            .await
            .expect("actor should be running");
        rx.await.map_err(|e| Error::ChannelRecv(e.to_string()))?
    }

    /// Get the number of pending messages from the HttpSource.
    pub async fn pending(&self) -> Option<usize> {
        let (tx, rx) = oneshot::channel();
        self.actor_tx
            .send(HttpActorMessage::Pending(tx))
            .await
            .expect("actor should be running");
        rx.await.ok().flatten()
    }
}

/// Send a message to the processing channel
pub async fn send_message(tx: mpsc::Sender<HttpMessage>, message: HttpMessage) -> Result<()> {
    match tx.try_send(message) {
        Ok(_) => Ok(()),
        Err(e) => match e {
            mpsc::error::TrySendError::Full(_) => Err(Error::ChannelFull()),
            mpsc::error::TrySendError::Closed(_) => {
                Err(Error::ChannelSend("Channel is closed".to_string()))
            }
        },
    }
}

#[derive(Clone)]
struct HttpState {
    tx: mpsc::Sender<HttpMessage>,
}

/// Create an Axum router with the HTTP source endpoints
pub fn create_router(
    vertex_name: &'static str,
    token: Option<&'static str>,
    tx: mpsc::Sender<HttpMessage>,
) -> Router {
    Router::new()
        .route("/health", get(health_handler))
        .route(
            format!("/vertices/{}", vertex_name).as_str(),
            post(data_handler),
        )
        .route_layer(middleware::from_fn(
            move |request: Request<Body>, next: Next| async move {
                // if no token is provided, skip the auth check
                let Some(token) = token else {
                    return next.run(request).await;
                };

                match request.headers().get("Authorization") {
                    Some(t) => {
                        let t = t.to_str().expect("token should be a string");
                        if t == format!("Bearer {}", token) {
                            return next.run(request).await;
                        }
                    }
                    None => {
                        return (StatusCode::UNAUTHORIZED, "Unauthorized").into_response();
                    }
                }

                (StatusCode::UNAUTHORIZED, "Unauthorized").into_response()
            },
        ))
        .with_state(HttpState { tx })
}

/// Generate self-signed TLS certificate
fn generate_certs() -> Result<(Certificate, rcgen::KeyPair)> {
    let CertifiedKey { cert, key_pair } = generate_simple_self_signed(vec!["localhost".into()])
        .map_err(|e| Error::Server(format!("Generating self-signed certificate: {}", e)))?;

    Ok((cert, key_pair))
}

/// Start the HTTPS server on the specified address
pub async fn start_server(
    vertex_name: &'static str,
    tx: mpsc::Sender<HttpMessage>,
    addr: SocketAddr,
    token: Option<&'static str>,
) -> Result<()> {
    let router = create_router(vertex_name, token, tx);

    info!(?addr, "Starting HTTPS source server");

    // Generate a self-signed certificate
    let (cert, key_pair) = generate_certs()?;

    let tls_config = RustlsConfig::from_pem(cert.pem().into(), key_pair.serialize_pem().into())
        .await
        .map_err(|e| Error::Server(format!("Creating TLS config from PEM: {}", e)))?;

    axum_server::bind_rustls(addr, tls_config)
        .serve(router.into_make_service())
        .await
        .map_err(|e| Error::Server(format!("HTTPS server error: {}", e)))?;

    Ok(())
}

/// Health check endpoint handler
async fn health_handler() -> impl IntoResponse {
    StatusCode::OK
}

/// Data ingestion endpoint handler
async fn data_handler(
    State(http_source): State<HttpState>,
    mut headers: HeaderMap,
    body: Bytes,
) -> impl IntoResponse {
    // Generate or extract X-Numaflow-Id
    let id = match headers.get("x-numaflow-id") {
        Some(val) => match parse_message_id_from_header(val) {
            Ok(id) => id,
            Err(e) => return e.into_response(),
        },
        None => Uuid::now_v7().to_string(),
    };

    // Generate or extract X-Numaflow-Event-Time
    let event_time = headers.get("x-numaflow-event-time");
    let event_time = match event_time {
        Some(etime) => match parse_event_time_from_header(etime) {
            Ok(time) => time,
            Err(resp) => return resp.into_response(),
        },
        None => Utc::now(),
    };

    // Remove all entries of "x-numaflow-event-time" header
    // https://github.com/numaproj/numaflow/blob/2cab60c2a1ddde0f0272b6570144071a49c4e94b/pkg/sources/http/http.go#L146
    if let axum::http::header::Entry::Occupied(hm) = headers.entry("x-numaflow-event-time") {
        hm.remove_entry_mult();
    }

    // Do not forward authorization header
    if let axum::http::header::Entry::Occupied(hm) =
        headers.entry(axum::http::header::AUTHORIZATION)
    {
        hm.remove_entry_mult();
    }

    // Convert headers to HashMap and ensure required headers are present
    let mut header_map = HashMap::new();
    // Ensure X-Numaflow-Id is in the headers
    header_map.insert("X-Numaflow-Id".to_string(), id.clone());

    for (key, value) in headers.iter() {
        if let Ok(value_str) = value.to_str() {
            header_map.insert(key.to_string(), value_str.to_string());
        } else {
            warn!(
                header_name=?key,
                header_value=?value,
                "Skipping header with invalid ASCII characters"
            );
        }
    }

    // Create the HTTP message
    let message = HttpMessage {
        body,
        headers: header_map,
        event_time,
        id: id.clone(),
    };

    // Send the message to the processing channel
    match send_message(http_source.tx, message).await {
        Ok(()) => {
            trace!(?id, "Successfully queued message");
            (
                StatusCode::OK,
                axum::Json(DataResponse {
                    message: "Data received successfully".to_string(),
                    id,
                }),
            )
                .into_response()
        }
        Err(e) => match e {
            Error::ChannelFull() => {
                warn!(?e, "Buffer is full");
                (
                    StatusCode::TOO_MANY_REQUESTS,
                    axum::Json(serde_json::json!({
                        "error": "Pipeline has stalled, buffer is full",
                        "details": e.to_string()
                    })),
                )
                    .into_response()
            }
            e => {
                error!(?e, "Failed to queue message");
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    axum::Json(serde_json::json!({
                        "error": "Failed to process request",
                        "details": e.to_string()
                    })),
                )
                    .into_response()
            }
        },
    }
}

fn parse_message_id_from_header(
    id_header_value: &HeaderValue,
) -> std::result::Result<String, (StatusCode, axum::Json<serde_json::Value>)> {
    let id = id_header_value.to_str().inspect_err(|e| {
        error!(?e, "The value of 'x-numaflow-id' header sent by the user contains non-printable ASCII characters");
    }).map_err(|_|{
        (
            StatusCode::BAD_REQUEST,
            axum::Json(serde_json::json!({
                "error": "Value of 'x-numaflow-id' header contains non-printable ASCII characters",
            })),
        )
    })?;
    Ok(id.to_string())
}

fn parse_event_time_from_header(
    event_time_header_value: &HeaderValue,
) -> std::result::Result<DateTime<Utc>, (StatusCode, axum::Json<serde_json::Value>)> {
    let epoch_millis_str = event_time_header_value
        .to_str()
        .inspect_err(|e| {
            error!(
                ?e,
                ?event_time_header_value,
                "Converting value of header 'x-numaflow-event-time' to string"
            )
        })
        .map_err(|_|  {
            (
                StatusCode::BAD_REQUEST,
                axum::Json(serde_json::json!({
                    "error": "Event time specified in header 'x-numaflow-event-time' is not an ASCII string",
                })),
            )
    })?;
    let epoch_millis = epoch_millis_str.parse::<i64>().inspect_err(|e| {
        error!(?e, epoch_millis_str, "Event time specified in header 'x-numaflow-event-time' is not a valid integer or within signed 64-bit integer range");
    }).map_err(|_| {
        (
            StatusCode::BAD_REQUEST,
            axum::Json(serde_json::json!({
                "error": "Event time specified in header 'x-numaflow-event-time' is not a valid integer",
                "details": format!("Specified value is {}", epoch_millis_str)
            })),
        )
    })?;

    let event_time = DateTime::from_timestamp_millis(epoch_millis).ok_or_else(|| {
        (
            StatusCode::BAD_REQUEST,
            axum::Json(serde_json::json!({
                "error": "Event time specified in header 'x-numaflow-event-time' is out of range",
                "details": format!("Specified value is {}", epoch_millis_str)
            })),
        )
    }).inspect_err(|_| {
        error!("Event time specified in header 'x-numaflow-event-time' is out of range for Epoch milliseconds");
    })?;

    Ok(event_time)
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use hyper::{Method, Request, StatusCode};
    use hyper_util::client::legacy::Client;
    use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
    use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
    use rustls::{DigitallySignedStruct, SignatureScheme};
    use std::net::TcpListener;
    use std::time::Duration;
    use tokio::sync::{mpsc, oneshot};
    use tower::ServiceExt;

    // Custom certificate verifier that accepts any certificate (for testing)
    #[derive(Debug)]
    struct AcceptAnyCertVerifier;

    impl ServerCertVerifier for AcceptAnyCertVerifier {
        fn verify_server_cert(
            &self,
            _end_entity: &CertificateDer<'_>,
            _intermediates: &[CertificateDer<'_>],
            _server_name: &ServerName<'_>,
            _ocsp_response: &[u8],
            _now: UnixTime,
        ) -> std::result::Result<ServerCertVerified, rustls::Error> {
            Ok(ServerCertVerified::assertion())
        }

        fn verify_tls12_signature(
            &self,
            _message: &[u8],
            _cert: &CertificateDer<'_>,
            _dss: &DigitallySignedStruct,
        ) -> std::result::Result<HandshakeSignatureValid, rustls::Error> {
            Ok(HandshakeSignatureValid::assertion())
        }

        fn verify_tls13_signature(
            &self,
            _message: &[u8],
            _cert: &CertificateDer<'_>,
            _dss: &DigitallySignedStruct,
        ) -> std::result::Result<HandshakeSignatureValid, rustls::Error> {
            Ok(HandshakeSignatureValid::assertion())
        }

        fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
            vec![
                SignatureScheme::RSA_PKCS1_SHA1,
                SignatureScheme::ECDSA_SHA1_Legacy,
                SignatureScheme::RSA_PKCS1_SHA256,
                SignatureScheme::ECDSA_NISTP256_SHA256,
                SignatureScheme::RSA_PKCS1_SHA384,
                SignatureScheme::ECDSA_NISTP384_SHA384,
                SignatureScheme::RSA_PKCS1_SHA512,
                SignatureScheme::ECDSA_NISTP521_SHA512,
                SignatureScheme::RSA_PSS_SHA256,
                SignatureScheme::RSA_PSS_SHA384,
                SignatureScheme::RSA_PSS_SHA512,
                SignatureScheme::ED25519,
                SignatureScheme::ED448,
            ]
        }
    }

    #[tokio::test]
    async fn test_health_endpoint() {
        let (tx, _rx) = mpsc::channel(500);

        let app = create_router("test", None, tx);

        let request = Request::builder()
            .method(Method::GET)
            .uri("/health")
            .body(Body::empty())
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_data_endpoint() {
        let (tx, rx) = mpsc::channel(10);

        let app = create_router("test", None, tx);

        let request = Request::builder()
            .method(Method::POST)
            .uri("/vertices/test")
            .header("Content-Type", "application/json")
            .body(Body::from(r#"{"test": "data"}"#))
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        assert_eq!(rx.len(), 1);
    }

    #[tokio::test]
    async fn test_data_endpoint_with_custom_id() {
        let (tx, rx) = mpsc::channel(10);

        let app = create_router("test", None, tx);

        let custom_id = "custom-test-id";
        let request = Request::builder()
            .method(Method::POST)
            .uri("/vertices/test")
            .header("Content-Type", "text/plain")
            .header("X-Numaflow-Id", custom_id)
            .body(Body::from("test data"))
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        assert_eq!(rx.len(), 1);
    }
    #[tokio::test]
    async fn test_http_source_read_with_real_server() {
        // Setup the CryptoProvider for rustls
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

        // Bind to a random available port
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener); // Release the listener so HttpSource can bind to it

        // Create HttpSource with the address
        let http_source_config = HttpSourceConfigBuilder::new("test").addr(addr).build();

        let http_source = HttpSourceActor::new(http_source_config).await;

        // Create actor channel for communicating with HttpSource
        let (actor_tx, actor_rx) = mpsc::channel::<HttpActorMessage>(10);

        // Spawn the HttpSource actor
        let source_handle = tokio::spawn(async move { http_source.run(actor_rx).await });

        // Wait a bit for the server to start
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Configure TLS client to accept any certificate (for testing with self-signed certs)
        let tls_config = rustls::ClientConfig::builder()
            .dangerous()
            .with_custom_certificate_verifier(std::sync::Arc::new(AcceptAnyCertVerifier))
            .with_no_client_auth();

        let https_connector = hyper_rustls::HttpsConnectorBuilder::new()
            .with_tls_config(tls_config)
            .https_or_http()
            .enable_http1()
            .build();
        let client = Client::builder(hyper_util::rt::TokioExecutor::new()).build(https_connector);

        // Send multiple HTTPS requests to the server
        let test_data = vec![
            (r#"{"message": "test1"}"#, "application/json"),
            (r#"{"message": "test2"}"#, "application/json"),
            ("plain text data", "text/plain"),
        ];

        for (body_data, content_type) in &test_data {
            let request = Request::builder()
                .method(Method::POST)
                .uri(format!("https://{}/vertices/test", addr))
                .header("Content-Type", *content_type)
                .header("X-Numaflow-Id", format!("test-id-{}", uuid::Uuid::now_v7()))
                .body((*body_data).to_string())
                .unwrap();

            let response = client.request(request).await.unwrap();
            assert_eq!(response.status(), StatusCode::OK);
        }

        // Use the actor to read messages from HttpSource
        let (read_tx, read_rx) = oneshot::channel();
        actor_tx
            .send(HttpActorMessage::Read {
                size: test_data.len(),
                response_tx: read_tx,
            })
            .await
            .unwrap();

        // Wait for the read response
        let messages = read_rx.await.unwrap().unwrap();

        // Verify we got the expected number of messages
        assert_eq!(messages.len(), test_data.len());

        let current_time = Utc::now();

        // Verify message contents
        for message in messages.iter() {
            assert!(!message.id.is_empty());
            assert!(message.headers.contains_key("X-Numaflow-Id"));
            assert!(message.headers.contains_key("content-type"));

            // Ensure current time is set when x-numaflow-event-time header is not specified
            assert!(
                current_time
                    .signed_duration_since(message.event_time)
                    .num_seconds()
                    .abs()
                    < 1
            );

            // Check that the body matches what we sent
            let body_str = String::from_utf8(message.body.to_vec()).unwrap();
            assert!(test_data.iter().any(|(data, _)| *data == body_str));
        }

        // Test pending count
        let (pending_tx, pending_rx) = oneshot::channel();
        actor_tx
            .send(HttpActorMessage::Pending(pending_tx))
            .await
            .unwrap();

        let pending_count = pending_rx.await.unwrap();
        assert_eq!(pending_count, Some(0)); // Should be 0 since we read all messages

        // Test ack (should always succeed for HTTP source)
        let offsets: Vec<String> = messages.iter().map(|m| m.id.clone()).collect();
        let (ack_tx, ack_rx) = oneshot::channel();
        actor_tx
            .send(HttpActorMessage::Ack {
                offsets,
                response_tx: ack_tx,
            })
            .await
            .unwrap();

        let ack_result = ack_rx.await.unwrap();
        assert!(ack_result.is_ok());

        // Clean up
        drop(actor_tx);
        let _ = source_handle.await;
    }

    #[tokio::test]
    async fn test_http_source_timeout_behavior() {
        // Test that read method respects timeout when no messages are available
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);

        let http_source_config = HttpSourceConfigBuilder::new("test").addr(addr).build();

        let http_source = HttpSourceActor::new(http_source_config).await;

        let (actor_tx, actor_rx) = mpsc::channel::<HttpActorMessage>(10);

        let source_handle = tokio::spawn(async move { http_source.run(actor_rx).await });

        // Try to read messages when none are available - should timeout and return empty vec
        let (read_tx, read_rx) = oneshot::channel();
        actor_tx
            .send(HttpActorMessage::Read {
                size: 5,
                response_tx: read_tx,
            })
            .await
            .unwrap();

        let messages = read_rx.await.unwrap().unwrap();
        assert_eq!(messages.len(), 0); // Should be empty due to timeout

        // Clean up
        drop(actor_tx);
        let _ = source_handle.await;
    }

    #[tokio::test]
    async fn test_http_source_handle() {
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

        // Bind to a random available port
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener); // Release the listener so HttpSource can bind to it

        // Create HttpSource config
        let http_source_config = HttpSourceConfigBuilder::new("test")
            .addr(addr)
            .buffer_size(10)
            .timeout(Duration::from_millis(100))
            .build();

        // Create HttpSourceHandle
        let handle = HttpSourceHandle::new(http_source_config.clone()).await;

        // Wait a bit for the server to start
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Configure TLS client to accept any certificate (for testing with self-signed certs)
        let tls_config = rustls::ClientConfig::builder()
            .dangerous()
            .with_custom_certificate_verifier(std::sync::Arc::new(AcceptAnyCertVerifier))
            .with_no_client_auth();

        let https_connector = hyper_rustls::HttpsConnectorBuilder::new()
            .with_tls_config(tls_config)
            .https_or_http()
            .enable_http1()
            .build();
        let client = Client::builder(hyper_util::rt::TokioExecutor::new()).build(https_connector);

        // Send test requests
        for i in 0..5 {
            let request = Request::builder()
                .method(Method::POST)
                .uri(format!("https://{}/vertices/test", addr))
                .header("Content-Type", "application/json")
                .header("X-Numaflow-Id", format!("test-id-{}", i))
                .header("x-numaflow-event-time", 1431628200000i64.to_string())
                .body(format!(r#"{{"message": "test{}"}}"#, i))
                .unwrap();

            let response = client.request(request).await.unwrap();
            assert_eq!(response.status(), StatusCode::OK);
        }

        // Test pending count
        let pending = handle.pending().await;
        assert_eq!(pending, Some(5), "Should have 5 pending messages");

        // Test read method
        let messages = handle.read(3).await.unwrap();
        assert_eq!(messages.len(), 3, "Should read 3 messages");

        let expected_event_time = DateTime::from_timestamp_millis(1431628200000).unwrap(); // May 15, 2015

        // Verify message contents
        for (i, message) in messages.iter().enumerate() {
            assert!(message.headers.contains_key("X-Numaflow-Id"));
            assert!(message.headers.contains_key("content-type"));

            assert_eq!(message.event_time, expected_event_time);

            let body_str = String::from_utf8(message.body.to_vec()).unwrap();
            assert!(body_str.contains(&format!("test{}", i)));
        }

        // Test pending count after reading
        let pending = handle.pending().await;
        assert_eq!(
            pending,
            Some(2),
            "Should have 2 pending messages after reading 3"
        );

        // Test ack method (should always succeed for HTTP source)
        let offsets: Vec<String> = messages.iter().map(|m| m.id.clone()).collect();
        let ack_result = handle.ack(offsets).await;
        assert!(ack_result.is_ok(), "Ack should succeed");

        // Read remaining messages
        let messages = handle.read(5).await.unwrap();
        assert_eq!(messages.len(), 2, "Should read remaining 2 messages");

        // Verify no more pending messages
        let pending = handle.pending().await;
        assert_eq!(pending, Some(0), "Should have 0 pending messages");
    }

    #[tokio::test]
    async fn test_auth_token_validation() {
        // Create a channel for the HTTP source
        let (tx, _rx) = mpsc::channel(10);

        // Set up router with auth token
        let test_token = "test-token";
        let app = create_router("test", Some(test_token), tx);

        // Test request with correct token
        let request = Request::builder()
            .method(Method::POST)
            .uri("/vertices/test")
            .header("Content-Type", "application/json")
            .header("Authorization", format!("Bearer {}", test_token))
            .body(Body::from(r#"{"test": "data"}"#))
            .unwrap();

        let response = app.clone().oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        // Test request with incorrect token
        let request = Request::builder()
            .method(Method::POST)
            .uri("/vertices/test")
            .header("Content-Type", "application/json")
            .header("Authorization", "Bearer wrong-token")
            .body(Body::from(r#"{"test": "data"}"#))
            .unwrap();

        let response = app.clone().oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);

        // Test request with missing token
        let request = Request::builder()
            .method(Method::POST)
            .uri("/vertices/test")
            .header("Content-Type", "application/json")
            .body(Body::from(r#"{"test": "data"}"#))
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[test]
    fn test_parse_message_id_with_invalid_header() {
        let eventtime_header_value = HeaderValue::from_str("héllo").unwrap(); // 'é' is not ASCII
        let result = parse_message_id_from_header(&eventtime_header_value).unwrap_err();
        assert_eq!(result.0, StatusCode::BAD_REQUEST);
    }

    #[test]
    fn test_parse_header_with_invalid_event_time() {
        let eventtime_header_value = HeaderValue::from_static("abcd");
        let result = parse_event_time_from_header(&eventtime_header_value).unwrap_err();
        assert_eq!(result.0, StatusCode::BAD_REQUEST);

        let eventtime_header_value = HeaderValue::from_str("héllo").unwrap(); // 'é' is not ASCII
        let result = parse_event_time_from_header(&eventtime_header_value).unwrap_err();
        assert_eq!(result.0, StatusCode::BAD_REQUEST);

        let eventtime_header_value = HeaderValue::from_str(&i64::MIN.to_string()).unwrap();
        let result = parse_event_time_from_header(&eventtime_header_value).unwrap_err();
        assert_eq!(result.0, StatusCode::BAD_REQUEST);
    }
}
