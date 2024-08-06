use tonic::transport::Channel;
use tonic::Request;

use crate::error::Result;
use crate::message::Message;
use crate::shared::{connect_with_uds, prost_timestamp_from_utc};

pub mod proto {
    tonic::include_proto!("sink.v1");
}

const SINK_SOCKET: &str = "/var/run/numaflow/sink.sock";
const SINK_SERVER_INFO_FILE: &str = "/var/run/numaflow/sink-server-info";

/// SinkConfig is the configuration for the sink server.
#[derive(Debug, Clone)]
pub struct SinkConfig {
    pub socket_path: String,
    pub server_info_file: String,
    pub max_message_size: usize,
}

impl Default for SinkConfig {
    fn default() -> Self {
        SinkConfig {
            socket_path: SINK_SOCKET.to_string(),
            server_info_file: SINK_SERVER_INFO_FILE.to_string(),
            max_message_size: 64 * 1024 * 1024, // 64 MB
        }
    }
}

/// SinkClient is a client to interact with the sink server.
pub struct SinkClient {
    client: proto::sink_client::SinkClient<Channel>,
}

impl SinkClient {
    pub(crate) async fn connect(config: SinkConfig) -> Result<Self> {
        let channel = connect_with_uds(config.socket_path.into()).await?;
        let client = proto::sink_client::SinkClient::new(channel)
            .max_decoding_message_size(config.max_message_size)
            .max_encoding_message_size(config.max_message_size);
        Ok(Self { client })
    }

    pub(crate) async fn sink_fn(&mut self, messages: Vec<Message>) -> Result<proto::SinkResponse> {
        let requests: Vec<proto::SinkRequest> = messages
            .into_iter()
            .map(|message| proto::SinkRequest {
                keys: message.keys,
                value: message.value,
                event_time: prost_timestamp_from_utc(message.event_time),
                watermark: None,
                id: format!("{}-{}", message.offset.partition_id, message.offset.offset),
                headers: message.headers,
            })
            .collect();

        let (tx, rx) = tokio::sync::mpsc::channel(1);

        tokio::spawn(async move {
            for request in requests {
                if tx.send(request).await.is_err() {
                    break;
                }
            }
        });

        // TODO: retry for response with failure status
        let response = self
            .client
            .sink_fn(tokio_stream::wrappers::ReceiverStream::new(rx))
            .await?
            .into_inner();
        Ok(response)
    }

    pub(crate) async fn is_ready(&mut self) -> Result<proto::ReadyResponse> {
        let request = Request::new(());
        let response = self.client.is_ready(request).await?.into_inner();
        Ok(response)
    }
}

#[cfg(test)]
mod tests {
    use chrono::offset::Utc;
    use log::info;
    use numaflow::sink;

    use crate::message::Offset;

    use super::*;

    struct Logger;
    #[tonic::async_trait]
    impl sink::Sinker for Logger {
        async fn sink(
            &self,
            mut input: tokio::sync::mpsc::Receiver<sink::SinkRequest>,
        ) -> Vec<sink::Response> {
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
    async fn sink_operations() {
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
                .unwrap();
        });

        // wait for the server to start
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        let mut sink_client = SinkClient::connect(SinkConfig {
            socket_path: sock_file.to_str().unwrap().to_string(),
            server_info_file: server_info_file.to_str().unwrap().to_string(),
            max_message_size: 4 * 1024 * 1024,
        })
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
            },
        ];

        let ready_response = sink_client.is_ready().await.unwrap();
        assert_eq!(ready_response.ready, true);

        let response = sink_client.sink_fn(messages).await.unwrap();
        assert_eq!(response.results.len(), 2);

        shutdown_tx
            .send(())
            .expect("failed to send shutdown signal");
        server_handle.await.expect("failed to join server task");
    }
}
