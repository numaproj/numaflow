use std::sync::Arc;

use crate::runtime_server::runtime;
use bytes::Bytes;
use numaflow_pb::clients::sink::sink_client::SinkClient;
use numaflow_pb::clients::sink::{Handshake, SinkRequest, SinkResponse, TransmissionStatus};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::Channel;
use tonic::{Code, Request, Status, Streaming};
use tracing::{error, warn};

use crate::Error;
use crate::Result;
use crate::config::pipeline::VERTEX_TYPE_SINK;
use crate::message::Message;
use crate::metrics::critical_error_reasons;
use crate::shared::grpc::{UdfReconnectConfig, create_sink_client, prost_timestamp_from_utc};
use crate::sinker::sink::{ResponseFromSink, Sink};

const DEFAULT_CHANNEL_SIZE: usize = 1000;

/// User-Defined Sink code writes messages to a custom [SinkWriter].
pub struct UserDefinedSink {
    sink_tx: mpsc::Sender<SinkRequest>,
    resp_stream: Streaming<SinkResponse>,
    reconnect_config: Option<ReconnectConfig>,
}

pub(crate) type ReconnectConfig = UdfReconnectConfig;

/// Convert [`Message`] to [`proto::SinkRequest`]
impl From<Message> for SinkRequest {
    fn from(message: Message) -> Self {
        Self {
            request: Some(numaflow_pb::clients::sink::sink_request::Request {
                keys: message.keys.to_vec(),
                value: message.value.to_vec(),
                event_time: Some(prost_timestamp_from_utc(message.event_time)),
                watermark: message.watermark.map(prost_timestamp_from_utc),
                id: message.id.to_string(),
                headers: Arc::unwrap_or_clone(message.headers),
                metadata: message.metadata.map(|m| Arc::unwrap_or_clone(m).into()),
            }),
            status: None,
            handshake: None,
        }
    }
}

impl UserDefinedSink {
    pub(crate) async fn new(
        mut client: SinkClient<Channel>,
        reconnect_config: Option<ReconnectConfig>,
    ) -> Result<Self> {
        let (sink_tx, resp_stream) = Self::create_sink_stream(&mut client).await?;
        Ok(Self {
            sink_tx,
            resp_stream,
            reconnect_config,
        })
    }

    async fn create_sink_stream(
        client: &mut SinkClient<Channel>,
    ) -> Result<(mpsc::Sender<SinkRequest>, Streaming<SinkResponse>)> {
        let (sink_tx, sink_rx) = mpsc::channel(DEFAULT_CHANNEL_SIZE);
        let sink_stream = ReceiverStream::new(sink_rx);

        // Perform handshake with the server before sending any requests
        let handshake_request = SinkRequest {
            request: None,
            status: None,
            handshake: Some(Handshake { sot: true }),
        };
        sink_tx
            .send(handshake_request)
            .await
            .map_err(|e| Error::Sink(format!("failed to send handshake request: {e}")))?;

        let mut resp_stream = client
            .sink_fn(Request::new(sink_stream))
            .await
            .map_err(|e| Error::Grpc(Box::new(e)))?
            .into_inner();

        // First response from the server will be the handshake response. We need to check if the
        // server has accepted the handshake.
        let handshake_response = resp_stream
            .message()
            .await
            .map_err(|e| Error::Grpc(Box::new(e)))?
            .ok_or(Error::Sink(
                "failed to receive handshake response".to_string(),
            ))?;

        // Handshake cannot be None during the initial phase, and it has to set `sot` to true.
        if handshake_response.handshake.is_none_or(|h| !h.sot) {
            return Err(Error::Sink("invalid handshake response".to_string()));
        }

        Ok((sink_tx, resp_stream))
    }

    async fn reconnect(&mut self) -> Result<()> {
        let Some(reconnect_config) = &self.reconnect_config else {
            return Err(Error::UdfRedrive(Box::new(Status::unavailable(
                "sink stream closed",
            ))));
        };
        let (mut client, _) = create_sink_client(
            reconnect_config.socket_path(),
            reconnect_config.server_info_path(),
            reconnect_config.cln_token(),
            reconnect_config.grpc_max_message_size(),
            reconnect_config.retry_interval(),
        )
        .await?;
        let (sink_tx, resp_stream) = match Self::create_sink_stream(&mut client).await {
            Ok(stream) => stream,
            Err(Error::Grpc(status)) => return Err(Self::redrive_error(*status)),
            Err(e) => return Err(e),
        };
        self.sink_tx = sink_tx;
        self.resp_stream = resp_stream;
        Ok(())
    }

    fn redrive_error(status: Status) -> Error {
        warn!(?status, "sink UDF error, redriving after reconnect");
        critical_error!(VERTEX_TYPE_SINK, critical_error_reasons::SINK_RUNTIME_ERROR);
        runtime::persist_application_error_with_container(status.clone(), "udsink");
        Error::UdfRedrive(Box::new(status))
    }

    async fn sink_once(&mut self, requests: &[SinkRequest]) -> Result<Vec<ResponseFromSink>> {
        let num_requests = requests.len();

        for request in requests.iter().cloned() {
            self.sink_tx
                .send(request)
                .await
                .map_err(|e| Self::redrive_error(Status::unavailable(e.to_string())))?;
        }

        let eot_request = SinkRequest {
            request: None,
            status: Some(TransmissionStatus { eot: true }),
            handshake: None,
        };
        self.sink_tx
            .send(eot_request)
            .await
            .map_err(|e| Self::redrive_error(Status::unavailable(e.to_string())))?;

        let mut responses = Vec::new();
        loop {
            let response = match self.resp_stream.message().await {
                Ok(Some(response)) => response,
                Ok(None) => {
                    return Err(Self::redrive_error(Status::unavailable(
                        "sink response stream closed",
                    )));
                }
                Err(e) => {
                    return Err(Self::redrive_error(e));
                }
            };

            if response.status.is_some_and(|s| s.eot) {
                if responses.len() != num_requests {
                    error!(
                        "received EOT message before all responses are received, gracefully exiting"
                    );
                    critical_error!(VERTEX_TYPE_SINK, "eot_received_from_sink");
                    return Err(Error::Grpc(Box::new(Status::with_details(
                        Code::Internal,
                        "UDF_PARTIAL_RESPONSE(udsink)",
                        Bytes::from_static(
                            b"received End-Of-Transmission (EOT) before all responses are received from the ud-sink. \
                            This indicates that there is a bug in the user-code. Please check whether you are accidentally \
                            skipping the messages.",
                        ),
                    ))));
                } else {
                    break;
                }
            }
            responses.extend(
                response
                    .results
                    .into_iter()
                    .map(Into::into)
                    .collect::<Vec<ResponseFromSink>>(),
            );
        }
        Ok(responses)
    }
}

impl Sink for UserDefinedSink {
    /// writes a set of messages to the sink.
    async fn sink(&mut self, messages: Vec<Message>) -> Result<Vec<ResponseFromSink>> {
        let requests: Vec<SinkRequest> =
            messages.into_iter().map(|message| message.into()).collect();
        loop {
            match self.sink_once(&requests).await {
                Ok(responses) => return Ok(responses),
                Err(Error::UdfRedrive(e)) => {
                    error!(?e, "redriving sink batch after reconnect");
                    self.reconnect().await?;
                }
                Err(e) => return Err(e),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use chrono::offset::Utc;
    use numaflow::shared::ServerExtras;
    use numaflow::sink;
    use tokio::sync::mpsc;
    use tracing::info;

    use super::*;
    use crate::error::Result;
    use crate::message::{IntOffset, Message, MessageID, Offset};
    use crate::metadata::{KeyValueGroup, Metadata};
    use crate::shared::grpc::create_rpc_channel;
    use crate::sinker::sink::user_defined::UserDefinedSink;

    struct Logger;
    #[tonic::async_trait]
    impl sink::Sinker for Logger {
        async fn sink(&self, mut input: mpsc::Receiver<sink::SinkRequest>) -> Vec<sink::Response> {
            let mut responses: Vec<sink::Response> = Vec::new();
            while let Some(datum) = input.recv().await {
                let response = match std::str::from_utf8(&datum.value) {
                    Ok(v) => {
                        info!("{}", v);
                        sink::Response::ok(datum.id)
                    }
                    Err(e) => {
                        sink::Response::failure(datum.id, format!("Invalid UTF-8 sequence: {}", e))
                    }
                };
                responses.push(response);
            }
            responses
        }
    }
    #[tokio::test]
    async fn sink_operations() -> Result<()> {
        // start the server
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let tmp_dir = tempfile::TempDir::new().unwrap();
        let sock_file = tmp_dir.path().join("sink.sock");
        let server_info_file = tmp_dir.path().join("sink-server-info");

        let server_info = server_info_file.clone();
        let server_socket = sock_file.clone();

        let server_handle = tokio::spawn(async move {
            sink::Server::new(Logger)
                .with_socket_file(server_socket)
                .with_server_info_file(server_info)
                .start_with_shutdown(shutdown_rx)
                .await
                .expect("failed to start sink server");
        });

        // wait for the server to start
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        let mut sink_client =
            UserDefinedSink::new(SinkClient::new(create_rpc_channel(sock_file).await?), None)
                .await
                .expect("failed to connect to sink server");

        let messages = vec![
            Message {
                typ: Default::default(),
                keys: Arc::from(vec![]),
                tags: None,
                value: b"Hello, World!".to_vec().into(),
                offset: Offset::Int(IntOffset::new(0, 0)),
                event_time: Utc::now(),
                headers: Default::default(),
                id: MessageID {
                    vertex_name: "vertex".to_string().into(),
                    offset: "1".to_string().into(),
                    index: 0,
                },
                ..Default::default()
            },
            Message {
                typ: Default::default(),
                keys: Arc::from(vec![]),
                tags: None,
                value: b"Hello, World!".to_vec().into(),
                offset: Offset::Int(IntOffset::new(0, 0)),
                event_time: Utc::now(),
                headers: Default::default(),
                id: MessageID {
                    vertex_name: "vertex".to_string().into(),
                    offset: "2".to_string().into(),
                    index: 1,
                },
                ..Default::default()
            },
        ];

        let response = sink_client.sink(messages.clone()).await?;
        assert_eq!(response.len(), 2);

        let response = sink_client.sink(messages.clone()).await?;
        assert_eq!(response.len(), 2);

        let reconnect = sink_client.reconnect().await;
        assert!(matches!(reconnect, Err(Error::UdfRedrive(_))));

        drop(sink_client);
        shutdown_tx
            .send(())
            .expect("failed to send shutdown signal");

        server_handle.await.expect("failed to join server task");
        Ok(())
    }

    #[test]
    fn test_message_to_sink_request_includes_headers_and_metadata() {
        let metadata = Metadata {
            previous_vertex: "map".to_string(),
            sys_metadata: HashMap::new(),
            user_metadata: HashMap::from([(
                "user".to_string(),
                KeyValueGroup {
                    key_value: HashMap::from([("key".to_string(), Bytes::from_static(b"value"))]),
                },
            )]),
        };
        let message = Message {
            typ: Default::default(),
            keys: Arc::from(vec!["key-1".to_string()]),
            tags: None,
            value: b"payload".to_vec().into(),
            offset: Offset::Int(IntOffset::new(0, 0)),
            event_time: Utc::now(),
            headers: Arc::new(HashMap::from([(
                "header-key".to_string(),
                "header-value".to_string(),
            )])),
            id: MessageID {
                vertex_name: "vertex".to_string().into(),
                offset: "1".to_string().into(),
                index: 0,
            },
            metadata: Some(Arc::new(metadata)),
            ..Default::default()
        };

        let request: SinkRequest = message.into();
        let request = request.request.expect("sink request should be present");

        assert_eq!(request.keys, vec!["key-1".to_string()]);
        assert_eq!(request.value, b"payload".to_vec());
        assert_eq!(
            request.headers.get("header-key").map(String::as_str),
            Some("header-value")
        );
        assert_eq!(
            request
                .metadata
                .expect("metadata should be present")
                .previous_vertex,
            "map"
        );
    }
}
