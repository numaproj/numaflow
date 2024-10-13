use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::Channel;
use tonic::{Request, Streaming};

use numaflow_grpc::clients::source;
use numaflow_grpc::clients::source::source_client::SourceClient;
use numaflow_grpc::clients::source::{
    read_request, AckRequest, AckResponse, ReadRequest, ReadResponse,
};

use crate::config::config;
use crate::error;
use crate::error::Error::SourceError;
use crate::message::{Message, Offset};
use crate::reader::LagReader;
use crate::source::{SourceAcker, SourceReader};

/// User-Defined Source to operative on custom sources.
#[derive(Debug)]
pub(crate) struct UserDefinedSourceRead {
    read_tx: mpsc::Sender<ReadRequest>,
    resp_stream: Streaming<ReadResponse>,
    num_records: usize,
    timeout_in_ms: u16,
}

/// User-Defined Source to operative on custom sources.
#[derive(Debug)]
pub(crate) struct UserDefinedSourceAck {
    ack_tx: mpsc::Sender<AckRequest>,
    ack_resp_stream: Streaming<AckResponse>,
}

/// Creates a new User-Defined Source and its corresponding Lag Reader.
pub(crate) async fn new_source(
    client: SourceClient<Channel>,
    num_records: usize,
    timeout_in_ms: u16,
) -> error::Result<(
    UserDefinedSourceRead,
    UserDefinedSourceAck,
    UserDefinedSourceLagReader,
)> {
    let src_read = UserDefinedSourceRead::new(client.clone(), num_records, timeout_in_ms).await?;
    let src_ack = UserDefinedSourceAck::new(client.clone()).await?;
    let lag_reader = UserDefinedSourceLagReader::new(client);

    Ok((src_read, src_ack, lag_reader))
}

impl UserDefinedSourceRead {
    async fn new(
        mut client: SourceClient<Channel>,
        num_records: usize,
        timeout_in_ms: u16,
    ) -> error::Result<Self> {
        let (read_tx, resp_stream) = Self::create_reader(&mut client).await?;

        Ok(Self {
            read_tx,
            resp_stream,
            num_records,
            timeout_in_ms,
        })
    }

    async fn create_reader(
        client: &mut SourceClient<Channel>,
    ) -> error::Result<(mpsc::Sender<ReadRequest>, Streaming<ReadResponse>)> {
        let (read_tx, read_rx) = mpsc::channel(config().batch_size as usize);
        let read_stream = ReceiverStream::new(read_rx);

        // do a handshake for read with the server before we start sending read requests
        let handshake_request = ReadRequest {
            request: None,
            handshake: Some(source::Handshake { sot: true }),
        };
        read_tx
            .send(handshake_request)
            .await
            .map_err(|e| SourceError(format!("failed to send handshake request: {}", e)))?;

        let mut resp_stream = client
            .read_fn(Request::new(read_stream))
            .await?
            .into_inner();

        // first response from the server will be the handshake response. We need to check if the
        // server has accepted the handshake.
        let handshake_response = resp_stream.message().await?.ok_or(SourceError(
            "failed to receive handshake response".to_string(),
        ))?;
        // handshake cannot to None during the initial phase and it has to set `sot` to true.
        if handshake_response.handshake.map_or(true, |h| !h.sot) {
            return Err(SourceError("invalid handshake response".to_string()));
        }

        Ok((read_tx, resp_stream))
    }
}

impl SourceReader for UserDefinedSourceRead {
    fn name(&self) -> &'static str {
        "user-defined-source"
    }

    async fn read(&mut self) -> error::Result<Vec<Message>> {
        let request = ReadRequest {
            request: Some(read_request::Request {
                num_records: self.num_records as u64,
                timeout_in_ms: self.timeout_in_ms as u32,
            }),
            handshake: None,
        };

        self.read_tx
            .send(request)
            .await
            .map_err(|e| SourceError(e.to_string()))?;

        let mut messages = Vec::with_capacity(self.num_records);

        while let Some(response) = self.resp_stream.message().await? {
            if response.status.map_or(false, |status| status.eot) {
                break;
            }

            let result = response
                .result
                .ok_or_else(|| SourceError("Empty message".to_string()))?;

            messages.push(result.try_into()?);
        }
        Ok(messages)
    }

    fn partitions(&self) -> Vec<u16> {
        todo!()
    }
}

impl UserDefinedSourceAck {
    async fn new(mut client: SourceClient<Channel>) -> error::Result<Self> {
        let (ack_tx, ack_resp_stream) = Self::create_acker(&mut client).await?;

        Ok(Self {
            ack_tx,
            ack_resp_stream,
        })
    }

    async fn create_acker(
        client: &mut SourceClient<Channel>,
    ) -> error::Result<(mpsc::Sender<AckRequest>, Streaming<AckResponse>)> {
        let (ack_tx, ack_rx) = mpsc::channel(config().batch_size as usize);
        let ack_stream = ReceiverStream::new(ack_rx);

        // do a handshake for ack with the server before we start sending ack requests
        let ack_handshake_request = AckRequest {
            request: None,
            handshake: Some(source::Handshake { sot: true }),
        };
        ack_tx
            .send(ack_handshake_request)
            .await
            .map_err(|e| SourceError(format!("failed to send ack handshake request: {}", e)))?;

        let mut ack_resp_stream = client.ack_fn(Request::new(ack_stream)).await?.into_inner();

        // first response from the server will be the handshake response. We need to check if the
        // server has accepted the handshake.
        let ack_handshake_response = ack_resp_stream.message().await?.ok_or(SourceError(
            "failed to receive ack handshake response".to_string(),
        ))?;
        // handshake cannot to None during the initial phase and it has to set `sot` to true.
        if ack_handshake_response.handshake.map_or(true, |h| !h.sot) {
            return Err(SourceError("invalid ack handshake response".to_string()));
        }

        Ok((ack_tx, ack_resp_stream))
    }
}

impl SourceAcker for UserDefinedSourceAck {
    async fn ack(&mut self, offsets: Vec<Offset>) -> error::Result<()> {
        let n = offsets.len();

        // send n ack requests
        for offset in offsets {
            let request = offset.into();
            self.ack_tx
                .send(request)
                .await
                .map_err(|e| SourceError(e.to_string()))?;
        }

        // make sure we get n responses for the n requests.
        for _ in 0..n {
            let _ = self
                .ack_resp_stream
                .message()
                .await?
                .ok_or(SourceError("failed to receive ack response".to_string()))?;
        }

        Ok(())
    }
}

#[derive(Clone)]
pub(crate) struct UserDefinedSourceLagReader {
    source_client: SourceClient<Channel>,
}

impl UserDefinedSourceLagReader {
    fn new(source_client: SourceClient<Channel>) -> Self {
        Self { source_client }
    }
}

impl LagReader for UserDefinedSourceLagReader {
    async fn pending(&mut self) -> error::Result<Option<usize>> {
        Ok(self
            .source_client
            .pending_fn(Request::new(()))
            .await?
            .into_inner()
            .result
            .map(|r| r.count as usize))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::collections::HashSet;

    use crate::shared::utils::create_rpc_channel;
    use numaflow_grpc::clients::source::source_client::SourceClient;

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

        async fn ack(&self, offset: Offset) {
            self.yet_to_ack
                .write()
                .unwrap()
                .remove(&String::from_utf8(offset.offset).unwrap());
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

        let client = SourceClient::new(create_rpc_channel(sock_file).await.unwrap());

        let (mut src_read, mut src_ack, mut lag_reader) = new_source(client, 5, 1000)
            .await
            .map_err(|e| panic!("failed to create source reader: {:?}", e))
            .unwrap();

        let messages = src_read.read().await.unwrap();
        assert_eq!(messages.len(), 5);

        let response = src_ack
            .ack(messages.iter().map(|m| m.offset.clone()).collect())
            .await;
        assert!(response.is_ok());

        let pending = lag_reader.pending().await.unwrap();
        assert_eq!(pending, Some(0));

        // we need to drop the client, because if there are any in-flight requests
        // server fails to shut down. https://github.com/numaproj/numaflow-rs/issues/85
        drop(src_read);
        drop(src_ack);

        shutdown_tx
            .send(())
            .expect("failed to send shutdown signal");
        server_handle.await.expect("failed to join server task");
    }
}
