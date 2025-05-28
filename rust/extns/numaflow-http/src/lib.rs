//! HTTP source for Numaflow.
//! There are two endpoints, one for health (`/health`) and another for data (`/data`).
//! The `/vertices/` endpoint is a `POST` endpoint that accepts all content-types of data. The
//! headers are propagated as is to the next vertex in the pipeline.
//! `X-Numaflow-Id` header is added to the message to track the message across the pipeline.
//! `X-Numaflow-Event-Time` is added to the message to track the event time of the message.

use axum::{
    Router,
    extract::State,
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    routing::{get, post},
};
use bytes::Bytes;
use chrono::{DateTime, Utc};
use serde::Serialize;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};
use tracing::{debug, error, info, trace, warn};
use uuid::Uuid;

/// HTTP source that manages incoming HTTP requests and forwards them to a processing channel
pub struct HttpSource {
    server_rx: mpsc::Receiver<HttpMessage>,
    server_handle: tokio::task::JoinHandle<Result<()>>,
    timeout: Duration,
}

/// Error types for the HTTP source
#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Channel send error: {0}")]
    ChannelSend(String),
    #[error("Channel Full: 429")]
    ChannelFull(),
    #[error("Server error: {0}")]
    Server(String),
}

type Result<T> = std::result::Result<T, Error>;

/// HTTP message containing the request body and headers
#[derive(Debug, Clone)]
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
pub struct HttpSourceBuilder {
    /// Default buffer size is 2000
    buffer_size: usize,
    addr: SocketAddr,
    timeout: Duration,
}

impl HttpSourceBuilder {
    /// Create a new HttpSourceBuilder
    pub fn new() -> Self {
        Self {
            buffer_size: 500,
            addr: "0.0.0.0:8080".parse().expect("Invalid address"),
            timeout: Duration::from_secs(1),
        }
    }

    /// Set the buffer size for the channel
    pub fn with_buffer_size(mut self, buffer_size: usize) -> Self {
        self.buffer_size = buffer_size;
        self
    }

    /// Set the buffer size for the channel
    pub fn with_addr(mut self, addr: SocketAddr) -> Self {
        self.addr = addr;
        self
    }

    /// Build the HttpSource instance
    pub fn build(self) -> HttpSource {
        let (tx, rx) = mpsc::channel(self.buffer_size); // Increased buffer size for better throughput

        let server_handle = tokio::spawn(start_server(tx, self.addr));

        HttpSource {
            server_rx: rx,
            timeout: self.timeout,
            server_handle,
        }
    }
}

impl Default for HttpSourceBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug)]
pub enum HttpSourceMessage {
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

impl HttpSource {
    /// Background task that processes messages from the channel
    async fn run(mut self, mut actor_rx: mpsc::Receiver<HttpSourceMessage>) -> Result<()> {
        info!("HttpSource processor started");

        while let Some(msg) = actor_rx.recv().await {
            match msg {
                HttpSourceMessage::Read { size, response_tx } => {
                    let messages = self.read(size).await;
                    debug!("Reading messages from HttpSource");
                    let _ = response_tx.send(messages);
                }
                HttpSourceMessage::Ack {
                    offsets,
                    response_tx,
                } => {
                    debug!(count = offsets.len(), "Acking messages from HttpSource");
                    let _ = response_tx.send(self.ack(offsets).await);
                }
                HttpSourceMessage::Pending(response_tx) => {
                    let pending = self.pending().await;
                    debug!(?pending, "Pending messages from HttpSource");
                    let _ = response_tx.send(pending);
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

/// Create an Axum router with the HTTP source endpoints
pub fn create_router(tx: mpsc::Sender<HttpMessage>) -> Router {
    Router::new()
        .route("/health", get(health_handler))
        .route("/vertices", post(data_handler))
        .with_state(tx)
}

/// Start the HTTP server on the specified address
pub async fn start_server(tx: mpsc::Sender<HttpMessage>, addr: SocketAddr) -> Result<()> {
    let router = create_router(tx);

    info!(?addr, "Starting HTTP source server");

    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .map_err(|e| Error::Server(format!("Failed to bind to {}: {}", addr, e)))?;

    axum::serve(listener, router)
        .await
        .map_err(|e| Error::Server(format!("Server error: {}", e)))?;

    Ok(())
}

/// Health check endpoint handler
async fn health_handler() -> impl IntoResponse {
    StatusCode::OK
}

/// Data ingestion endpoint handler
async fn data_handler(
    State(http_source): State<mpsc::Sender<HttpMessage>>,
    headers: HeaderMap,
    body: Bytes,
) -> impl IntoResponse {
    // Convert headers to HashMap and ensure required headers are present
    let mut header_map = HashMap::new();

    for (key, value) in headers.iter() {
        if let Ok(value_str) = value.to_str() {
            header_map.insert(key.to_string(), value_str.to_string());
        } else {
            warn!(?key, "Skipping header with invalid UTF-8");
        }
    }

    // Generate or extract X-Numaflow-Id
    let id = header_map
        .get("x-numaflow-id")
        .or_else(|| header_map.get("X-Numaflow-Id"))
        .cloned()
        .unwrap_or_else(|| Uuid::now_v7().to_string());

    // Ensure X-Numaflow-Id is in the headers
    header_map.insert("X-Numaflow-Id".to_string(), id.clone());

    // Generate or extract X-Numaflow-Event-Time
    let event_time = header_map
        .get("x-numaflow-event-time")
        .or_else(|| header_map.get("X-Numaflow-Event-Time"))
        .and_then(|time_str| DateTime::parse_from_rfc3339(time_str).ok())
        .map(|dt| dt.with_timezone(&Utc))
        .unwrap_or_else(Utc::now);

    // Ensure X-Numaflow-Event-Time is in the headers
    header_map.insert("X-Numaflow-Event-Time".to_string(), event_time.to_rfc2822());

    // Create the HTTP message
    let message = HttpMessage {
        body,
        headers: header_map,
        event_time,
        id: id.clone(),
    };

    // Send the message to the processing channel
    match send_message(http_source, message).await {
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

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::{Method, Request};
    use tower::ServiceExt;

    #[tokio::test]
    async fn test_health_endpoint() {
        let (tx, _rx) = mpsc::channel(500);

        let app = create_router(tx);

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

        let app = create_router(tx);

        let request = Request::builder()
            .method(Method::POST)
            .uri("/vertices")
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

        let app = create_router(tx);

        let custom_id = "custom-test-id";
        let request = Request::builder()
            .method(Method::POST)
            .uri("/vertices")
            .header("Content-Type", "text/plain")
            .header("X-Numaflow-Id", custom_id)
            .body(Body::from("test data"))
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        assert_eq!(rx.len(), 1);
    }
}
