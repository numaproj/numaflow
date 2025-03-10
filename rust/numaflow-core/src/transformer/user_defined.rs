use std::collections::HashMap;
use std::sync::Arc;

use numaflow_pb::clients::sourcetransformer::{
    self, source_transform_client::SourceTransformClient, SourceTransformRequest,
    SourceTransformResponse,
};
use tokio::sync::{mpsc, oneshot, Mutex};
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::Channel;
use tonic::{Request, Streaming};

use crate::config::get_vertex_name;
use crate::error::{Error, Result};
use crate::message::{Message, MessageID, Offset};
use crate::shared::grpc::{prost_timestamp_from_utc, utc_from_timestamp};

type ResponseSenderMap =
    Arc<Mutex<HashMap<String, (ParentMessageInfo, oneshot::Sender<Result<Vec<Message>>>)>>>;

// fields which will not be changed
struct ParentMessageInfo {
    offset: Offset,
    headers: HashMap<String, String>,
}

/// UserDefinedTransformer exposes methods to do user-defined transformations.
pub(super) struct UserDefinedTransformer {
    rpc_client: SourceTransformClient<Channel>,
    read_tx: mpsc::Sender<SourceTransformRequest>,
    senders: ResponseSenderMap,
    task_handle: tokio::task::JoinHandle<()>,
}

/// Aborts the background task when the UserDefinedTransformer is dropped.
impl Drop for UserDefinedTransformer {
    fn drop(&mut self) {
        self.task_handle.abort();
    }
}

/// Convert the [`Message`] to [`SourceTransformRequest`]
impl From<Message> for SourceTransformRequest {
    fn from(message: Message) -> Self {
        Self {
            request: Some(sourcetransformer::source_transform_request::Request {
                id: message.offset.to_string(),
                keys: message.keys.to_vec(),
                value: message.value.to_vec(),
                event_time: Some(prost_timestamp_from_utc(message.event_time)),
                watermark: message.watermark.map(prost_timestamp_from_utc),
                headers: message.headers,
            }),
            handshake: None,
        }
    }
}

impl UserDefinedTransformer {
    /// Performs handshake with the server and creates a new UserDefinedTransformer.
    pub(super) async fn new(
        batch_size: usize,
        mut client: SourceTransformClient<Channel>,
    ) -> Result<Self> {
        let (read_tx, read_rx) = mpsc::channel(batch_size);
        let read_stream = ReceiverStream::new(read_rx);

        // perform handshake
        let handshake_request = SourceTransformRequest {
            request: None,
            handshake: Some(sourcetransformer::Handshake { sot: true }),
        };
        read_tx
            .send(handshake_request)
            .await
            .map_err(|e| Error::Transformer(format!("failed to send handshake request: {}", e)))?;

        let mut resp_stream = client
            .source_transform_fn(Request::new(read_stream))
            .await
            .map_err(Error::Grpc)?
            .into_inner();

        let handshake_response =
            resp_stream
                .message()
                .await
                .map_err(Error::Grpc)?
                .ok_or(Error::Transformer(
                    "failed to receive handshake response".to_string(),
                ))?;

        if handshake_response.handshake.map_or(true, |h| !h.sot) {
            return Err(Error::Transformer("invalid handshake response".to_string()));
        }

        // map to track the oneshot sender for each request along with the message info
        let sender_map = Arc::new(Mutex::new(HashMap::new()));

        // background task to receive responses from the server and send them to the appropriate
        // oneshot sender based on the message id
        let task_handle = tokio::spawn(Self::receive_responses(
            Arc::clone(&sender_map),
            resp_stream,
        ));

        let transformer = Self {
            rpc_client: client,
            read_tx,
            senders: sender_map,
            task_handle,
        };

        Ok(transformer)
    }

    // receive responses from the server and gets the corresponding oneshot sender from the map
    // and sends the response.
    async fn receive_responses(
        sender_map: ResponseSenderMap,
        mut resp_stream: Streaming<SourceTransformResponse>,
    ) {
        while let Some(resp) = match resp_stream.message().await {
            Ok(message) => message,
            Err(e) => {
                let mut senders = sender_map.lock().await;
                for (_, (_, sender)) in senders.drain() {
                    let _ = sender.send(Err(Error::Grpc(e.clone())));
                }
                None
            }
        } {
            let msg_id = resp.id;
            if let Some((msg_info, sender)) = sender_map.lock().await.remove(&msg_id) {
                let mut response_messages = vec![];
                for (i, result) in resp.results.into_iter().enumerate() {
                    let message = Message {
                        typ: Default::default(),
                        id: MessageID {
                            vertex_name: get_vertex_name().to_string().into(),
                            index: i as i32,
                            offset: msg_info.offset.clone().to_string().into(),
                        },
                        keys: Arc::from(result.keys),
                        tags: Some(Arc::from(result.tags)),
                        value: result.value.into(),
                        offset: msg_info.offset.clone(),
                        event_time: utc_from_timestamp(result.event_time),
                        headers: msg_info.headers.clone(),
                        watermark: None,
                        metadata: None,
                    };
                    response_messages.push(message);
                }
                sender
                    .send(Ok(response_messages))
                    .expect("failed to send response");
            }
        }
    }

    /// Handles the incoming message and sends it to the server for transformation.
    pub(super) async fn transform(
        &mut self,
        message: Message,
        respond_to: oneshot::Sender<Result<Vec<Message>>>,
    ) {
        let key = message.offset.clone().to_string();

        let msg_info = ParentMessageInfo {
            offset: message.offset.clone(),
            headers: message.headers.clone(),
        };

        self.senders
            .lock()
            .await
            .insert(key, (msg_info, respond_to));

        let _ = self.read_tx.send(message.into()).await;
    }

    pub(super) async fn ready(&mut self) -> bool {
        match self.rpc_client.is_ready(Request::new(())).await {
            Ok(response) => response.into_inner().ready,
            Err(_) => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::result::Result;
    use std::time::{Duration, SystemTime};

    use chrono::{TimeZone, Utc};
    use numaflow::sourcetransform;
    use tempfile::TempDir;

    use super::*;
    use crate::message::StringOffset;
    use crate::shared::grpc::create_rpc_channel;

    struct NowCat;

    #[tonic::async_trait]
    impl sourcetransform::SourceTransformer for NowCat {
        async fn transform(
            &self,
            input: sourcetransform::SourceTransformRequest,
        ) -> Vec<sourcetransform::Message> {
            let message = sourcetransform::Message::new(input.value, SystemTime::now())
                .with_keys(input.keys)
                .with_tags(vec![]);
            vec![message]
        }
    }

    #[tokio::test]
    async fn transformer_operations() -> Result<(), Box<dyn Error>> {
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let tmp_dir = TempDir::new()?;
        let sock_file = tmp_dir.path().join("sourcetransform.sock");
        let server_info_file = tmp_dir.path().join("sourcetransformer-server-info");

        let server_info = server_info_file.clone();
        let server_socket = sock_file.clone();
        let handle = tokio::spawn(async move {
            sourcetransform::Server::new(NowCat)
                .with_socket_file(server_socket)
                .with_server_info_file(server_info)
                .start_with_shutdown(shutdown_rx)
                .await
                .expect("server failed");
        });

        // wait for the server to start
        tokio::time::sleep(Duration::from_millis(100)).await;

        let mut client = UserDefinedTransformer::new(
            500,
            SourceTransformClient::new(create_rpc_channel(sock_file).await?),
        )
        .await?;

        let message = Message {
            typ: Default::default(),
            keys: Arc::from(vec!["first".into()]),
            tags: None,
            value: "hello".into(),
            offset: Offset::String(StringOffset::new("0".to_string(), 0)),
            event_time: Utc::now(),
            watermark: None,
            id: MessageID {
                vertex_name: "vertex_name".to_string().into(),
                offset: "0".to_string().into(),
                index: 0,
            },
            headers: Default::default(),
            metadata: None,
        };

        let (tx, rx) = oneshot::channel();

        tokio::time::timeout(Duration::from_secs(2), client.transform(message, tx))
            .await
            .unwrap();

        let messages = rx.await.unwrap();
        assert!(messages.is_ok());
        assert_eq!(messages?.len(), 1);

        // we need to drop the client, because if there are any in-flight requests
        // server fails to shut down. https://github.com/numaproj/numaflow-rs/issues/85
        drop(client);

        shutdown_tx
            .send(())
            .expect("failed to send shutdown signal");
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(
            handle.is_finished(),
            "Expected gRPC server to have shut down"
        );
        Ok(())
    }

    #[test]
    fn test_message_to_source_transform_request() {
        let message = Message {
            typ: Default::default(),
            keys: Arc::from(vec!["key1".to_string()]),
            tags: None,
            value: vec![1, 2, 3].into(),
            offset: Offset::String(StringOffset {
                offset: "123".to_string().into(),
                partition_idx: 0,
            }),
            event_time: Utc.timestamp_opt(1627846261, 0).unwrap(),
            watermark: None,
            id: MessageID {
                vertex_name: "vertex".to_string().into(),
                offset: "123".to_string().into(),
                index: 0,
            },
            headers: HashMap::new(),
            metadata: None,
        };

        let request: SourceTransformRequest = message.into();
        assert!(request.request.is_some());
    }
}
