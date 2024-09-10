use crate::error::Error::SourceError;
use crate::error::Result;
use crate::message::{Message, Offset};
use crate::sourcepb;
use crate::sourcepb::source_client::SourceClient;
use crate::sourcepb::{
    ack_request, read_request, AckRequest, AckResponse, ReadRequest, ReadResponse,
};
use base64::prelude::BASE64_STANDARD;
use base64::Engine;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::Channel;
use tonic::{Request, Streaming};

pub(crate) const SOURCE_SOCKET: &str = "/var/run/numaflow/source.sock";
pub(crate) const SOURCE_SERVER_INFO_FILE: &str = "/var/run/numaflow/sourcer-server-info";

/// SourceClient is a client to interact with the source server.
#[derive(Debug)]
pub(crate) struct SourceReader {
    read_tx: mpsc::Sender<ReadRequest>,
    resp_stream: Streaming<ReadResponse>,
    ack_tx: mpsc::Sender<AckRequest>,
}

impl SourceReader {
    pub(crate) async fn new(mut client: SourceClient<Channel>) -> Result<Self> {
        let (read_tx, read_rx) = mpsc::channel(500);

        let resp_stream = client
            .read_fn(Request::new(ReceiverStream::new(read_rx)))
            .await?
            .into_inner();

        let (ack_tx, ack_rx) = mpsc::channel(500);
        let _ = client
            .ack_fn(Request::new(ReceiverStream::new(ack_rx)))
            .await?;

        Ok(Self {
            read_tx,
            resp_stream,
            ack_tx,
        })
    }

    pub(crate) async fn read_fn(
        &mut self,
        num_records: u64,
        timeout_in_ms: u32,
    ) -> Result<Vec<Message>> {
        let request = ReadRequest {
            request: Some(read_request::Request {
                num_records,
                timeout_in_ms,
            }),
        };

        self.read_tx
            .send(request)
            .await
            .map_err(|e| SourceError(e.to_string()))?;

        let mut messages = Vec::with_capacity(num_records as usize);

        while let Some(response) = self.resp_stream.message().await? {
            if response.status.as_ref().map_or(false, |status| status.eot) {
                break;
            }

            let result = response
                .result
                .ok_or_else(|| SourceError("Empty message".to_string()))?;

            messages.push(result.try_into()?);
        }

        Ok(messages)
    }

    pub(crate) async fn ack_fn(&mut self, offsets: Vec<Offset>) -> Result<AckResponse> {
        for offset in offsets {
            let request = AckRequest {
                request: Some(ack_request::Request {
                    offset: Some(sourcepb::Offset {
                        offset: BASE64_STANDARD
                            .decode(offset.offset)
                            .expect("we control the encoding, so this should never fail"),
                        partition_id: offset.partition_id,
                    }),
                }),
            };
            self.ack_tx
                .send(request)
                .await
                .map_err(|e| SourceError(e.to_string()))?;
        }
        Ok(AckResponse::default())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use crate::shared::create_rpc_channel;
    use crate::source::SourceReader;
    use crate::sourcepb::source_client::SourceClient;
    use chrono::Utc;
    use numaflow::source;
    use numaflow::source::{Message, Offset, SourceReadRequest};
    use tokio::sync::mpsc::Sender;

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
    async fn source_operations() {
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
                .unwrap()
        });

        // wait for the server to start
        // TODO: flaky
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        let mut source_client = SourceReader::new(SourceClient::new(
            create_rpc_channel(sock_file).await.unwrap(),
        ))
        .await
        .unwrap();

        let messages = source_client.read_fn(5, 1000).await.unwrap();
        assert_eq!(messages.len(), 5);

        let response = source_client
            .ack_fn(messages.iter().map(|m| m.offset.clone()).collect())
            .await
            .unwrap();
        assert!(response.result.unwrap().success.is_some());

        shutdown_tx
            .send(())
            .expect("failed to send shutdown signal");
        server_handle.await.expect("failed to join server task");
    }
}
