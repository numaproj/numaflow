use crate::error::Result;
use crate::message::Message;
use crate::sink_pb::sink_client::SinkClient;
use crate::sink_pb::{SinkRequest, SinkResponse};
use tonic::transport::Channel;

pub(crate) const SINK_SOCKET: &str = "/var/run/numaflow/sink.sock";
pub(crate) const FB_SINK_SOCKET: &str = "/var/run/numaflow/fb-sink.sock";

pub(crate) const SINK_SERVER_INFO_FILE: &str = "/var/run/numaflow/sinker-server-info";
pub(crate) const FB_SINK_SERVER_INFO_FILE: &str = "/var/run/numaflow/fb-sinker-server-info";

/// SinkWriter writes messages to a sink.
#[derive(Clone)]
pub struct SinkWriter {
    client: SinkClient<Channel>,
}

impl SinkWriter {
    pub(crate) async fn new(client: SinkClient<Channel>) -> Result<Self> {
        Ok(Self { client })
    }

    pub(crate) async fn sink_fn(&mut self, messages: Vec<Message>) -> Result<SinkResponse> {
        // create a channel with at least size
        let (tx, rx) = tokio::sync::mpsc::channel(if messages.is_empty() {
            1
        } else {
            messages.len()
        });

        let requests: Vec<SinkRequest> =
            messages.into_iter().map(|message| message.into()).collect();

        tokio::spawn(async move {
            for request in requests {
                if tx.send(request).await.is_err() {
                    break;
                }
            }
        });

        let response = self
            .client
            .sink_fn(tokio_stream::wrappers::ReceiverStream::new(rx))
            .await?
            .into_inner();

        Ok(response)
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
                .unwrap();
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

        let response = sink_client.sink_fn(messages).await?;
        assert_eq!(response.results.len(), 2);

        shutdown_tx
            .send(())
            .expect("failed to send shutdown signal");
        server_handle.await.expect("failed to join server task");
        Ok(())
    }
}
