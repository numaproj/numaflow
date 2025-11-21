use std::sync::Arc;

use bytes::Bytes;
use numaflow_monitor::runtime;
use numaflow_pb::clients::sink::sink_client::SinkClient;
use numaflow_pb::clients::sink::{Handshake, SinkRequest, SinkResponse, TransmissionStatus};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::Channel;
use tonic::{Code, Request, Status, Streaming};
use tracing::error;

use crate::Error;
use crate::Result;
use crate::message::Message;
use crate::shared::grpc::prost_timestamp_from_utc;
use crate::sinker::sink::{ResponseFromSink, Sink};

const DEFAULT_CHANNEL_SIZE: usize = 1000;

/// User-Defined Sink code writes messages to a custom [SinkWriter].
pub struct UserDefinedSink {
    sink_tx: mpsc::Sender<SinkRequest>,
    resp_stream: Streaming<SinkResponse>,
}

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
    pub(crate) async fn new(mut client: SinkClient<Channel>) -> Result<Self> {
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

        Ok(Self {
            sink_tx,
            resp_stream,
        })
    }
}

impl Sink for UserDefinedSink {
    /// writes a set of messages to the sink.
    async fn sink(&mut self, messages: Vec<Message>) -> Result<Vec<ResponseFromSink>> {
        let requests: Vec<SinkRequest> =
            messages.into_iter().map(|message| message.into()).collect();
        let num_requests = requests.len();

        // write requests to the server
        for request in requests {
            let _ = self.sink_tx.send(request).await;
        }

        // send eot request to indicate the end of the stream
        let eot_request = SinkRequest {
            request: None,
            status: Some(TransmissionStatus { eot: true }),
            handshake: None,
        };
        self.sink_tx
            .send(eot_request)
            .await
            .map_err(|e| Error::Sink(format!("failed to send eot request: {e}")))?;

        // Now that we have sent, we wait for responses!
        // NOTE: This works now because the results are not streamed. As of today, it will give the
        // response only once it has read all the requests.
        // We wait for num_requests + 1 responses because the last response will be the EOT response.
        let mut responses = Vec::new();
        loop {
            let response = self
                .resp_stream
                .message()
                .await
                .map_err(|e| Error::Grpc(Box::new(e)))?
                .ok_or(Error::Sink("failed to receive response".to_string()))?;

            if response.status.is_some_and(|s| s.eot) {
                if responses.len() != num_requests {
                    error!(
                        "received EOT message before all responses are received, we will wait indefinitely for the remaining responses"
                    );
                    // persist the error for debugging
                    runtime::persist_application_error(Status::with_details(
                        Code::Internal,
                        "UDF_PARTIAL_RESPONSE(udsink)",
                        Bytes::from_static(
                            b"received End-Of-Transmission (EOT) before all responses are received from the ud-sink,\
                            we will wait indefinitely for the remaining responses. This indicates that there is a bug \
                            in the user-code. Please check whether you are accidentally skipping the messages.",
                        ),
                    ));
                } else {
                    break;
                }
                continue;
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use chrono::offset::Utc;
    use numaflow::shared::ServerExtras;
    use numaflow::sink;
    use tokio::sync::mpsc;
    use tracing::info;

    use super::*;
    use crate::error::Result;
    use crate::message::{IntOffset, Message, MessageID, Offset};
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
            UserDefinedSink::new(SinkClient::new(create_rpc_channel(sock_file).await?))
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

        drop(sink_client);
        shutdown_tx
            .send(())
            .expect("failed to send shutdown signal");

        server_handle.await.expect("failed to join server task");
        Ok(())
    }
}
