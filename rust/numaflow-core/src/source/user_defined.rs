use std::sync::Arc;
use std::time::Duration;

use base64::Engine;
use base64::prelude::BASE64_STANDARD;
use numaflow_pb::clients::source;
use numaflow_pb::clients::source::source_client::SourceClient;
use numaflow_pb::clients::source::{
    AckRequest, AckResponse, ReadRequest, ReadResponse, read_request, read_response,
};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::Channel;
use tonic::{Request, Streaming};

use crate::message::{Message, MessageID, Offset, StringOffset};
use crate::reader::LagReader;
use crate::shared::grpc::utc_from_timestamp;
use crate::source::{SourceAcker, SourceReader};
use crate::{Error, Result, config};

/// User-Defined Source to operative on custom sources.
#[derive(Debug)]
pub(crate) struct UserDefinedSourceRead {
    read_tx: mpsc::Sender<ReadRequest>,
    resp_stream: Streaming<ReadResponse>,
    num_records: usize,
    timeout: Duration,
    source_client: SourceClient<Channel>,
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
    read_timeout: Duration,
) -> Result<(
    UserDefinedSourceRead,
    UserDefinedSourceAck,
    UserDefinedSourceLagReader,
)> {
    let src_read = UserDefinedSourceRead::new(client.clone(), num_records, read_timeout).await?;
    let src_ack = UserDefinedSourceAck::new(client.clone(), num_records).await?;
    let lag_reader = UserDefinedSourceLagReader::new(client);

    Ok((src_read, src_ack, lag_reader))
}

impl UserDefinedSourceRead {
    async fn new(
        client: SourceClient<Channel>,
        batch_size: usize,
        timeout: Duration,
    ) -> Result<Self> {
        let (read_tx, resp_stream) = Self::create_reader(batch_size, &mut client.clone()).await?;

        Ok(Self {
            read_tx,
            resp_stream,
            num_records: batch_size,
            timeout,
            source_client: client,
        })
    }

    async fn create_reader(
        batch_size: usize,
        client: &mut SourceClient<Channel>,
    ) -> Result<(mpsc::Sender<ReadRequest>, Streaming<ReadResponse>)> {
        let (read_tx, read_rx) = mpsc::channel(batch_size);
        let read_stream = ReceiverStream::new(read_rx);

        // do a handshake for read with the server before we start sending read requests
        let handshake_request = ReadRequest {
            request: None,
            handshake: Some(source::Handshake { sot: true }),
        };
        read_tx
            .send(handshake_request)
            .await
            .map_err(|e| Error::Source(format!("failed to send handshake request: {e}")))?;

        let mut resp_stream = client
            .read_fn(Request::new(read_stream))
            .await
            .map_err(|e| Error::Grpc(Box::new(e)))?
            .into_inner();

        // first response from the server will be the handshake response. We need to check if the
        // server has accepted the handshake.
        let handshake_response = resp_stream
            .message()
            .await
            .map_err(|e| Error::Grpc(Box::new(e)))?
            .ok_or(Error::Source(
                "failed to receive handshake response".to_string(),
            ))?;
        // handshake cannot to None during the initial phase, and it has to set `sot` to true.
        if handshake_response.handshake.is_none_or(|h| !h.sot) {
            return Err(Error::Source("invalid handshake response".to_string()));
        }

        Ok((read_tx, resp_stream))
    }

    pub(crate) fn get_source_client(&self) -> SourceClient<Channel> {
        self.source_client.clone()
    }
}

/// Convert [`read_response::Result`] to [`Message`]
impl TryFrom<read_response::Result> for Message {
    type Error = Error;

    fn try_from(result: read_response::Result) -> Result<Self> {
        let source_offset = match result.offset {
            Some(o) if !o.offset.is_empty() => Offset::String(StringOffset {
                offset: BASE64_STANDARD.encode(o.offset).into(),
                partition_idx: o.partition_id as u16,
            }),

            Some(_) => {
                return Err(Error::Source(
                    "Invalid offset found in response. \
                    This is user code error. Please make sure that offset is not empty in response."
                        .to_string(),
                ));
            }

            None => {
                return Err(Error::Source(
                    "Offset not found. This is user code error. \
                    Please make sure that offset is present in response."
                        .to_string(),
                ));
            }
        };

        Ok(Message {
            typ: Default::default(),
            keys: Arc::from(result.keys),
            tags: None,
            value: result.payload.into(),
            offset: source_offset.clone(),
            event_time: result
                .event_time
                .map(utc_from_timestamp)
                .expect("event time should be present"),
            id: MessageID {
                vertex_name: config::get_vertex_name().to_string().into(),
                offset: source_offset.to_string().into(),
                index: 0,
            },
            headers: result.headers,
            watermark: None,
            metadata: None,
            is_late: false,
        })
    }
}

impl TryFrom<Offset> for source::Offset {
    type Error = Error;

    fn try_from(offset: Offset) -> std::result::Result<Self, Self::Error> {
        match offset {
            Offset::String(StringOffset {
                offset,
                partition_idx,
            }) => Ok(source::Offset {
                offset: BASE64_STANDARD
                    .decode(offset)
                    .expect("we control the encoding, so this should never fail"),
                partition_id: partition_idx as i32,
            }),
            Offset::Int(_) => Err(Error::Source("IntOffset not supported".to_string())),
        }
    }
}

impl SourceReader for UserDefinedSourceRead {
    fn name(&self) -> &'static str {
        "user-defined-source"
    }

    async fn read(&mut self) -> Result<Vec<Message>> {
        let request = ReadRequest {
            request: Some(read_request::Request {
                num_records: self.num_records as u64,
                timeout_in_ms: self.timeout.as_millis() as u32,
            }),
            handshake: None,
        };

        self.read_tx
            .send(request)
            .await
            .map_err(|e| Error::Source(e.to_string()))?;

        let mut messages = Vec::with_capacity(self.num_records);

        while let Some(response) = self
            .resp_stream
            .message()
            .await
            .map_err(|e| Error::Grpc(Box::new(e)))?
        {
            if response.status.is_some_and(|status| status.eot) {
                break;
            }

            let result = response
                .result
                .ok_or_else(|| Error::Source("Empty message in response".to_string()))?;

            messages.push(result.try_into()?);
        }
        Ok(messages)
    }

    async fn partitions(&mut self) -> Result<Vec<u16>> {
        let partitions = self
            .source_client
            .partitions_fn(Request::new(()))
            .await
            .map_err(|e| Error::Source(e.to_string()))?
            .into_inner()
            .result
            .expect("partitions not found")
            .partitions;

        Ok(partitions.iter().map(|p| *p as u16).collect())
    }
}

impl UserDefinedSourceAck {
    async fn new(mut client: SourceClient<Channel>, batch_size: usize) -> Result<Self> {
        let (ack_tx, ack_resp_stream) = Self::create_acker(batch_size, &mut client).await?;

        Ok(Self {
            ack_tx,
            ack_resp_stream,
        })
    }

    async fn create_acker(
        batch_size: usize,
        client: &mut SourceClient<Channel>,
    ) -> Result<(mpsc::Sender<AckRequest>, Streaming<AckResponse>)> {
        let (ack_tx, ack_rx) = mpsc::channel(batch_size);
        let ack_stream = ReceiverStream::new(ack_rx);

        // do a handshake for ack with the server before we start sending ack requests
        let ack_handshake_request = AckRequest {
            request: None,
            handshake: Some(source::Handshake { sot: true }),
        };
        ack_tx
            .send(ack_handshake_request)
            .await
            .map_err(|e| Error::Source(format!("failed to send ack handshake request: {e}")))?;

        let mut ack_resp_stream = client
            .ack_fn(Request::new(ack_stream))
            .await
            .map_err(|e| Error::Grpc(Box::new(e)))?
            .into_inner();

        // first response from the server will be the handshake response. We need to check if the
        // server has accepted the handshake.
        let ack_handshake_response = ack_resp_stream
            .message()
            .await
            .map_err(|e| Error::Grpc(Box::new(e)))?
            .ok_or(Error::Source(
                "failed to receive ack handshake response".to_string(),
            ))?;
        // handshake cannot to None during the initial phase, and it has to set `sot` to true.
        if ack_handshake_response.handshake.is_none_or(|h| !h.sot) {
            return Err(Error::Source("invalid ack handshake response".to_string()));
        }

        Ok((ack_tx, ack_resp_stream))
    }
}

impl SourceAcker for UserDefinedSourceAck {
    async fn ack(&mut self, offsets: Vec<Offset>) -> Result<()> {
        let ack_offsets: Result<Vec<source::Offset>> =
            offsets.into_iter().map(TryInto::try_into).collect();

        self.ack_tx
            .send(AckRequest {
                request: Some(source::ack_request::Request {
                    offsets: ack_offsets?,
                }),
                handshake: None,
            })
            .await
            .map_err(|e| Error::Source(e.to_string()))?;

        let _ = self
            .ack_resp_stream
            .message()
            .await
            .map_err(|e| Error::Grpc(Box::new(e)))?
            .ok_or(Error::Source("failed to receive ack response".to_string()))?;

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
    async fn pending(&mut self) -> Result<Option<usize>> {
        Ok(self
            .source_client
            .pending_fn(Request::new(()))
            .await
            .map_err(|e| Error::Grpc(Box::new(e)))?
            .into_inner()
            .result
            .map(|r| r.count as usize))
    }
}

#[cfg(test)]
mod tests {
    use chrono::{TimeZone, Utc};
    use numaflow::source;
    use numaflow::source::{Message, Offset, SourceReadRequest};
    use numaflow_pb::clients::source::source_client::SourceClient;
    use std::collections::{HashMap, HashSet};
    use tokio::sync::mpsc::Sender;

    use super::*;
    use crate::message::IntOffset;
    use crate::shared::grpc::{create_rpc_channel, prost_timestamp_from_utc};

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

        async fn pending(&self) -> Option<usize> {
            Some(self.yet_to_ack.read().unwrap().len())
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
        tokio::time::sleep(Duration::from_millis(100)).await;

        let client = SourceClient::new(create_rpc_channel(sock_file).await.unwrap());

        let (mut src_read, mut src_ack, mut lag_reader) =
            new_source(client, 5, Duration::from_millis(1000))
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

        let partitions = src_read.partitions().await.unwrap();
        assert_eq!(partitions, vec![2]);

        // we need to drop the client, because if there are any in-flight requests
        // server fails to shut down. https://github.com/numaproj/numaflow-rs/issues/85
        drop(src_read);
        drop(src_ack);

        shutdown_tx
            .send(())
            .expect("failed to send shutdown signal");
        server_handle.await.expect("failed to join server task");
    }

    #[test]
    fn test_read_response_result_to_message() {
        let result = read_response::Result {
            payload: vec![1, 2, 3],
            offset: Some(numaflow_pb::clients::source::Offset {
                offset: BASE64_STANDARD.encode("123").into_bytes(),
                partition_id: 0,
            }),
            event_time: Some(prost_timestamp_from_utc(
                Utc.timestamp_opt(1627846261, 0).unwrap(),
            )),
            keys: vec!["key1".to_string()],
            headers: HashMap::new(),
        };

        let message: Result<crate::message::Message> = result.try_into();
        assert!(message.is_ok());

        let message = message.unwrap();
        assert_eq!(message.keys.to_vec(), vec!["key1".to_string()]);
        assert_eq!(message.value, vec![1, 2, 3]);
        assert_eq!(
            message.event_time,
            Utc.timestamp_opt(1627846261, 0).unwrap()
        );
    }

    #[test]
    fn test_offset_conversion() {
        // Test conversion from Offset to AckRequest for StringOffset
        let offset =
            crate::message::Offset::String(StringOffset::new(BASE64_STANDARD.encode("42"), 1));
        let offset: Result<numaflow_pb::clients::source::Offset> = offset.try_into();
        assert_eq!(offset.unwrap().partition_id, 1);

        // Test conversion from Offset to AckRequest for IntOffset (should fail)
        let offset = crate::message::Offset::Int(IntOffset::new(42, 1));
        let result: Result<numaflow_pb::clients::source::Offset> = offset.try_into();
        assert!(result.is_err());
    }
}
