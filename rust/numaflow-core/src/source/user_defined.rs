use crate::config::config;
use crate::error;
use crate::error::Error::SourceError;
use crate::message::{Message, Offset};
use crate::monovertex::source_pb;
use crate::monovertex::source_pb::source_client::SourceClient;
use crate::monovertex::source_pb::{
    read_request, AckRequest, AckResponse, ReadRequest, ReadResponse,
};
use crate::reader::LagReader;
use crate::source::Source;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::Channel;
use tonic::{Request, Streaming};

/// User-Defined Source to operative on custom sources.
#[derive(Debug)]
pub(crate) struct UserDefinedSource {
    read_tx: mpsc::Sender<ReadRequest>,
    resp_stream: Streaming<ReadResponse>,
    ack_tx: mpsc::Sender<AckRequest>,
    ack_resp_stream: Streaming<AckResponse>,
    num_records: usize,
    timeout_in_ms: u16,
}

impl UserDefinedSource {
    pub(crate) async fn new(
        mut client: SourceClient<Channel>,
        num_records: usize,
        timeout_in_ms: u16,
    ) -> error::Result<Self> {
        let (read_tx, resp_stream) = Self::create_reader(&mut client).await?;
        let (ack_tx, ack_resp_stream) = Self::create_acker(&mut client).await?;

        Ok(Self {
            read_tx,
            resp_stream,
            ack_tx,
            ack_resp_stream,
            num_records,
            timeout_in_ms,
        })
    }

    pub(crate) async fn create_reader(
        client: &mut SourceClient<Channel>,
    ) -> error::Result<(mpsc::Sender<ReadRequest>, Streaming<ReadResponse>)> {
        let (read_tx, read_rx) = mpsc::channel(config().batch_size as usize);
        let read_stream = ReceiverStream::new(read_rx);

        // do a handshake for read with the server before we start sending read requests
        let handshake_request = ReadRequest {
            request: None,
            handshake: Some(source_pb::Handshake { sot: true }),
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

    pub(crate) async fn create_acker(
        client: &mut SourceClient<Channel>,
    ) -> error::Result<(mpsc::Sender<AckRequest>, Streaming<AckResponse>)> {
        let (ack_tx, ack_rx) = mpsc::channel(config().batch_size as usize);
        let ack_stream = ReceiverStream::new(ack_rx);

        // do a handshake for ack with the server before we start sending ack requests
        let ack_handshake_request = AckRequest {
            request: None,
            handshake: Some(source_pb::Handshake { sot: true }),
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

impl Source for UserDefinedSource {
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

    fn partitions(&self) -> Vec<u16> {
        todo!()
    }
}

#[derive(Clone)]
pub(crate) struct UserDefinedSourceLagReader {
    source_client: SourceClient<Channel>,
}

impl UserDefinedSourceLagReader {
    pub(crate) fn new(source_client: SourceClient<Channel>) -> Self {
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

    use crate::monovertex::source_pb::source_client::SourceClient;
    use crate::shared::utils::create_rpc_channel;

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

        let mut source = UserDefinedSource::new(client.clone(), 5, 1000)
            .await
            .unwrap();

        let mut lag_reader = UserDefinedSourceLagReader::new(client.clone());

        let messages = source.read().await.unwrap();
        assert_eq!(messages.len(), 5);

        let response = source
            .ack(messages.iter().map(|m| m.offset.clone()).collect())
            .await
            .unwrap();
        assert_eq!(response, ());

        let pending = lag_reader.pending().await.unwrap();
        assert_eq!(pending, Some(0));

        // we need to drop the client, because if there are any in-flight requests
        // server fails to shut down. https://github.com/numaproj/numaflow-rs/issues/85
        drop(source);

        shutdown_tx
            .send(())
            .expect("failed to send shutdown signal");
        server_handle.await.expect("failed to join server task");
    }
}
