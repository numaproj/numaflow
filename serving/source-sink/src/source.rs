use base64::prelude::BASE64_STANDARD;
use base64::Engine;
use tokio_stream::StreamExt;
use tonic::transport::Channel;
use tonic::Request;

use crate::error::{Error, Result};
use crate::message::{Message, Offset};
use crate::shared::connect_with_uds;
pub mod proto {
    tonic::include_proto!("source.v1");
}

const SOURCE_SOCKET: &str = "/var/run/numaflow/source.sock";
const SOURCE_SERVER_INFO_FILE: &str = "/var/run/numaflow/sourcer-server-info";

/// SourceConfig is the configuration for the source server.
#[derive(Debug, Clone)]
pub struct SourceConfig {
    pub socket_path: String,
    pub server_info_file: String,
    pub max_message_size: usize,
}

impl Default for SourceConfig {
    fn default() -> Self {
        SourceConfig {
            socket_path: SOURCE_SOCKET.to_string(),
            server_info_file: SOURCE_SERVER_INFO_FILE.to_string(),
            max_message_size: 64 * 1024 * 1024, // 64 MB
        }
    }
}

/// SourceClient is a client to interact with the source server.
#[derive(Debug, Clone)]
pub(crate) struct SourceClient {
    client: proto::source_client::SourceClient<Channel>,
}

impl SourceClient {
    pub(crate) async fn connect(config: SourceConfig) -> Result<Self> {
        let channel = connect_with_uds(config.socket_path.into()).await?;
        let client = proto::source_client::SourceClient::new(channel)
            .max_encoding_message_size(config.max_message_size)
            .max_decoding_message_size(config.max_message_size);
        Ok(Self { client })
    }

    pub(crate) async fn read_fn(
        &mut self,
        num_records: u64,
        timeout_in_ms: u32,
    ) -> Result<Vec<Message>> {
        let request = Request::new(proto::ReadRequest {
            request: Some(proto::read_request::Request {
                num_records,
                timeout_in_ms,
            }),
        });

        let mut stream = self.client.read_fn(request).await?.into_inner();
        let mut messages = Vec::with_capacity(num_records as usize);

        while let Some(response) = stream.next().await {
            let result = response?
                .result
                .ok_or_else(|| Error::SourceError("Empty message".to_string()))?;

            messages.push(result.try_into()?);
        }

        Ok(messages)
    }

    pub(crate) async fn ack_fn(&mut self, offsets: Vec<Offset>) -> Result<proto::AckResponse> {
        let offsets = offsets
            .into_iter()
            .map(|offset| proto::Offset {
                offset: BASE64_STANDARD
                    .decode(offset.offset)
                    .expect("we control the encoding, so this should never fail"),
                partition_id: offset.partition_id,
            })
            .collect();

        let request = Request::new(proto::AckRequest {
            request: Some(proto::ack_request::Request { offsets }),
        });

        Ok(self.client.ack_fn(request).await?.into_inner())
    }

    #[allow(dead_code)]
    // TODO: remove dead_code
    pub(crate) async fn pending_fn(&mut self) -> Result<proto::PendingResponse> {
        let request = Request::new(());
        let response = self.client.pending_fn(request).await?.into_inner();
        Ok(response)
    }

    #[allow(dead_code)]
    // TODO: remove dead_code
    pub(crate) async fn partitions_fn(&mut self) -> Result<Vec<i32>> {
        let request = Request::new(());
        let response = self.client.partitions_fn(request).await?.into_inner();
        Ok(response.result.map_or(vec![], |r| r.partitions))
    }

    pub(crate) async fn is_ready(&mut self) -> Result<proto::ReadyResponse> {
        let request = Request::new(());
        let response = self.client.is_ready(request).await?.into_inner();
        Ok(response)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::error::Error;

    use chrono::Utc;
    use numaflow::source;
    use numaflow::source::{Message, Offset, SourceReadRequest};
    use tokio::sync::mpsc::Sender;

    use crate::source::{SourceClient, SourceConfig};

    struct SimpleSource {
        num: usize,
        yet_to_ack: std::sync::RwLock<HashSet<String>>,
    }

    impl SimpleSource {
        fn new(num: usize) -> Self {
            Self {
                num,
                yet_to_ack: std::sync::RwLock::new(HashSet::new()),
            }
        }
    }

    #[tonic::async_trait]
    impl source::Sourcer for SimpleSource {
        async fn read(&self, request: SourceReadRequest, transmitter: Sender<Message>) {
            let event_time = Utc::now();
            let mut message_offsets = Vec::with_capacity(request.count);
            for i in 0..request.count {
                let offset = format!("{}-{}", event_time.timestamp_nanos_opt().unwrap(), i);
                transmitter
                    .send(Message {
                        value: self.num.to_le_bytes().to_vec(),
                        event_time,
                        offset: Offset {
                            offset: offset.clone().into_bytes(),
                            partition_id: 0,
                        },
                        keys: vec![],
                        headers: Default::default(),
                    })
                    .await
                    .unwrap();
                message_offsets.push(offset)
            }
            self.yet_to_ack.write().unwrap().extend(message_offsets)
        }

        async fn ack(&self, offsets: Vec<Offset>) {
            for offset in offsets {
                self.yet_to_ack
                    .write()
                    .unwrap()
                    .remove(&String::from_utf8(offset.offset).unwrap());
            }
        }

        async fn pending(&self) -> usize {
            self.yet_to_ack.read().unwrap().len()
        }

        async fn partitions(&self) -> Option<Vec<i32>> {
            Some(vec![2])
        }
    }

    #[tokio::test]
    async fn source_operations() -> Result<(), Box<dyn Error>> {
        // start the server
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let tmp_dir = tempfile::TempDir::new().unwrap();
        let sock_file = tmp_dir.path().join("source.sock");
        let server_info_file = tmp_dir.path().join("source-server-info");

        let server_info = server_info_file.clone();
        let server_socket = sock_file.clone();
        let server_handle = tokio::spawn(async move {
            source::Server::new(SimpleSource::new(10))
                .with_socket_file(server_socket)
                .with_server_info_file(server_info)
                .start_with_shutdown(shutdown_rx)
                .await
                .unwrap();
        });

        // wait for the server to start
        // TODO: flaky
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        let mut source_client = SourceClient::connect(SourceConfig {
            socket_path: sock_file.to_str().unwrap().to_string(),
            server_info_file: server_info_file.to_str().unwrap().to_string(),
            max_message_size: 4 * 1024 * 1024,
        })
        .await
        .expect("failed to connect to source server");

        let response = source_client.is_ready().await.unwrap();
        assert_eq!(response.ready, true);

        let messages = source_client.read_fn(5, 1000).await.unwrap();
        assert_eq!(messages.len(), 5);

        let response = source_client
            .ack_fn(messages.iter().map(|m| m.offset.clone()).collect())
            .await
            .unwrap();
        assert!(response.result.unwrap().success.is_some());

        let pending = source_client.pending_fn().await.unwrap();
        assert_eq!(pending.result.unwrap().count, 0);

        let partitions = source_client.partitions_fn().await.unwrap();
        assert_eq!(partitions, vec![2]);

        shutdown_tx
            .send(())
            .expect("failed to send shutdown signal");
        server_handle.await.expect("failed to join server task");
        Ok(())
    }
}
