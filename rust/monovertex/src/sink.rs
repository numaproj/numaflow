use crate::error::{Error, Result};
use crate::message::Message;
use crate::sink_pb::sink_client::SinkClient;
use crate::sink_pb::sink_request::Status;
use crate::sink_pb::{Handshake, SinkRequest, SinkResponse};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::Channel;
use tonic::{Request, Streaming};

const DEFAULT_CHANNEL_SIZE: usize = 1000;

/// SinkWriter writes messages to a sink.
pub struct SinkWriter {
    sink_tx: mpsc::Sender<SinkRequest>,
    resp_stream: Streaming<SinkResponse>,
}

impl SinkWriter {
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
            .map_err(|e| Error::SinkError(format!("failed to send handshake request: {}", e)))?;

        let mut resp_stream = client
            .sink_fn(Request::new(sink_stream))
            .await?
            .into_inner();

        // First response from the server will be the handshake response. We need to check if the
        // server has accepted the handshake.
        let handshake_response = resp_stream.message().await?.ok_or(Error::SinkError(
            "failed to receive handshake response".to_string(),
        ))?;

        // Handshake cannot be None during the initial phase and it has to set `sot` to true.
        if handshake_response.handshake.map_or(true, |h| !h.sot) {
            return Err(Error::SinkError("invalid handshake response".to_string()));
        }

        Ok(Self {
            sink_tx,
            resp_stream,
        })
    }

    pub(crate) async fn sink_fn(&mut self, messages: Vec<Message>) -> Result<Vec<SinkResponse>> {
        let requests: Vec<SinkRequest> =
            messages.into_iter().map(|message| message.into()).collect();
        let num_requests = requests.len();

        for request in requests {
            self.sink_tx
                .send(request)
                .await
                .map_err(|e| Error::SinkError(format!("failed to send request: {}", e)))?;
        }

        // send eot request to indicate the end of the stream
        let eot_request = SinkRequest {
            request: None,
            status: Some(Status { eot: true }),
            handshake: None,
        };
        self.sink_tx
            .send(eot_request)
            .await
            .map_err(|e| Error::SinkError(format!("failed to send eot request: {}", e)))?;

        let mut responses = Vec::new();
        for _ in 0..num_requests {
            let response = self
                .resp_stream
                .message()
                .await?
                .ok_or(Error::SinkError("failed to receive response".to_string()))?;
            responses.push(response);
        }
        Ok(responses)
    }
}

#[cfg(test)]
mod tests {
    use chrono::offset::Utc;
    use numaflow::sink;
    use tracing::info;

    use super::*;
    use crate::message::Offset;
    use crate::shared::create_rpc_channel;

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
            SinkWriter::new(SinkClient::new(create_rpc_channel(sock_file).await?))
                .await
                .expect("failed to connect to sink server");

        let messages = vec![
            Message {
                keys: vec![],
                value: b"Hello, World!".to_vec(),
                offset: Offset {
                    offset: "1".to_string(),
                    partition_id: 0,
                },
                event_time: Utc::now(),
                headers: Default::default(),
                id: "one".to_string(),
            },
            Message {
                keys: vec![],
                value: b"Hello, World!".to_vec(),
                offset: Offset {
                    offset: "2".to_string(),
                    partition_id: 0,
                },
                event_time: Utc::now(),
                headers: Default::default(),
                id: "two".to_string(),
            },
        ];

        let response = sink_client.sink_fn(messages.clone()).await?;
        assert_eq!(response.len(), 2);

        let response = sink_client.sink_fn(messages.clone()).await?;
        assert_eq!(response.len(), 2);

        drop(sink_client);
        shutdown_tx
            .send(())
            .expect("failed to send shutdown signal");

        server_handle.await.expect("failed to join server task");
        Ok(())
    }
}
