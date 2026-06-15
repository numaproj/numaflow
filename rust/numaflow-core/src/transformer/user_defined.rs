use std::collections::HashMap;
use std::sync::Arc;

use numaflow_pb::clients::sourcetransformer::{
    self, SourceTransformRequest, SourceTransformResponse,
    source_transform_client::SourceTransformClient, source_transform_response,
};
use tokio::sync::{Mutex, mpsc, oneshot};
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::Channel;
use tonic::{Request, Status, Streaming};

use crate::config::get_vertex_name;
use crate::error::{Error, Result, is_udf_transport_failure};
use crate::message::{Message, MessageID, Offset};
use crate::metadata::Metadata;
use crate::shared::grpc::{
    UdfReconnectConfig, create_transformer_client, prost_timestamp_from_utc, utc_from_timestamp,
};

type ResponseSenderMap =
    Arc<Mutex<HashMap<String, (ParentMessageInfo, oneshot::Sender<Result<Vec<Message>>>)>>>;

pub(crate) type ReconnectConfig = UdfReconnectConfig;

// fields which will not be changed
struct ParentMessageInfo {
    offset: Offset,
    is_late: bool,
    headers: Arc<HashMap<String, String>>,
    metadata: Option<Arc<Metadata>>,
}

// we are passing the reference for msg info because we can have more than 1 response for a single request and
// each response will use the same parent message info.
struct UserDefinedTransformerMessage<'a>(
    source_transform_response::Result,
    &'a ParentMessageInfo,
    i32,
);

impl From<UserDefinedTransformerMessage<'_>> for Message {
    fn from(value: UserDefinedTransformerMessage<'_>) -> Self {
        Message {
            typ: Default::default(),
            id: MessageID {
                vertex_name: get_vertex_name().to_string().into(),
                index: value.2,
                offset: value.1.offset.clone().to_string().into(),
            },
            keys: Arc::from(value.0.keys),
            tags: Some(Arc::from(value.0.tags)),
            value: value.0.value.into(),
            offset: value.1.offset.clone(),
            event_time: value
                .0
                .event_time
                .map(utc_from_timestamp)
                .expect("event time should be present"),
            headers: Arc::clone(&value.1.headers),
            watermark: None,
            metadata: {
                let mut metadata = Metadata::default();
                // Get SystemMetadata from parent message info
                if let Some(parent_metadata) = &value.1.metadata {
                    metadata.sys_metadata = parent_metadata.sys_metadata.clone();
                }
                // Get UserMetadata from the response if present
                if let Some(response_metadata) = &value.0.metadata {
                    let response_meta: Metadata = response_metadata.clone().into();
                    metadata.user_metadata = response_meta.user_metadata;
                }
                Some(Arc::new(metadata))
            },
            is_late: value.1.is_late,
        }
    }
}

/// UserDefinedTransformer exposes methods to do user-defined transformations.
pub(super) struct UserDefinedTransformer {
    read_tx: mpsc::Sender<SourceTransformRequest>,
    senders: ResponseSenderMap,
    task_handle: tokio::task::JoinHandle<()>,
    batch_size: usize,
    reconnect_config: ReconnectConfig,
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
                headers: Arc::unwrap_or_clone(message.headers),
                metadata: message.metadata.map(|m| Arc::unwrap_or_clone(m).into()),
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
        reconnect_config: ReconnectConfig,
    ) -> Result<Self> {
        let (read_tx, resp_stream) = Self::create_stream(batch_size, &mut client).await?;
        let sender_map = Arc::new(Mutex::new(HashMap::new()));
        let task_handle = tokio::spawn(Self::receive_responses(
            Arc::clone(&sender_map),
            resp_stream,
        ));

        Ok(Self {
            read_tx,
            senders: sender_map,
            task_handle,
            batch_size,
            reconnect_config,
        })
    }

    async fn create_stream(
        batch_size: usize,
        client: &mut SourceTransformClient<Channel>,
    ) -> Result<(
        mpsc::Sender<SourceTransformRequest>,
        Streaming<SourceTransformResponse>,
    )> {
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
            .map_err(|e| Error::Transformer(format!("failed to send handshake request: {e}")))?;

        let mut resp_stream = client
            .source_transform_fn(Request::new(read_stream))
            .await
            .map_err(|e| Error::Grpc(Box::new(e)))?
            .into_inner();

        let handshake_response = resp_stream
            .message()
            .await
            .map_err(|e| Error::Grpc(Box::new(e)))?
            .ok_or(Error::Transformer(
                "failed to receive handshake response".to_string(),
            ))?;

        if handshake_response.handshake.is_none_or(|h| !h.sot) {
            return Err(Error::Transformer("invalid handshake response".to_string()));
        }

        Ok((read_tx, resp_stream))
    }

    async fn drain_senders(sender_map: &ResponseSenderMap, error: Error) {
        let mut senders = sender_map.lock().await;
        for (_, (_, sender)) in senders.drain() {
            let _ = sender.send(Err(error.clone()));
        }
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
                let error = if is_udf_transport_failure(&e) {
                    Error::UdfRedrive(Box::new(e.clone()))
                } else {
                    Error::Grpc(Box::new(e.clone()))
                };
                Self::drain_senders(&sender_map, error).await;
                None
            }
        } {
            let msg_id = resp.id;
            if let Some((msg_info, sender)) = sender_map.lock().await.remove(&msg_id) {
                let mut response_messages = vec![];
                for (i, result) in resp.results.into_iter().enumerate() {
                    let message = UserDefinedTransformerMessage(result, &msg_info, i as i32).into();
                    response_messages.push(message);
                }
                let _ = sender.send(Ok(response_messages));
            }
        }

        Self::drain_senders(
            &sender_map,
            Error::UdfRedrive(Box::new(Status::unavailable(
                "source transformer stream closed",
            ))),
        )
        .await;
    }

    async fn reconnect(&mut self) -> Result<()> {
        let (mut client, _) = create_transformer_client(
            self.reconnect_config.socket_path(),
            self.reconnect_config.server_info_path(),
            self.reconnect_config.cln_token(),
            self.reconnect_config.grpc_max_message_size(),
            self.reconnect_config.retry_interval(),
        )
        .await?;
        let (read_tx, resp_stream) = Self::create_stream(self.batch_size, &mut client).await?;
        Self::drain_senders(
            &self.senders,
            Error::UdfRedrive(Box::new(Status::unavailable(
                "source transformer reconnecting",
            ))),
        )
        .await;
        self.task_handle.abort();
        self.read_tx = read_tx;
        self.task_handle = tokio::spawn(Self::receive_responses(
            Arc::clone(&self.senders),
            resp_stream,
        ));
        Ok(())
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
            headers: Arc::clone(&message.headers),
            is_late: message.is_late,
            metadata: message.metadata.clone(),
        };

        let request: SourceTransformRequest = message.into();

        self.senders
            .lock()
            .await
            .insert(key.clone(), (msg_info, respond_to));

        if self.read_tx.send(request.clone()).await.is_ok() {
            return;
        }

        let Some((msg_info, respond_to)) = self.senders.lock().await.remove(&key) else {
            return;
        };

        if let Err(e) = self.reconnect().await {
            let _ = respond_to.send(Err(e));
            return;
        }

        self.senders
            .lock()
            .await
            .insert(key.clone(), (msg_info, respond_to));

        if self.read_tx.send(request).await.is_err() {
            let Some((_, respond_to)) = self.senders.lock().await.remove(&key) else {
                return;
            };
            let _ = respond_to.send(Err(Error::UdfRedrive(Box::new(Status::unavailable(
                "source transformer stream closed after reconnect",
            )))));
        }
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::result::Result;
    use std::time::Duration;

    use chrono::{TimeZone, Utc};
    use numaflow::shared::ServerExtras;
    use numaflow::sourcetransform;
    use tempfile::TempDir;
    use tokio_util::sync::CancellationToken;

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
            let message = sourcetransform::Message::new(input.value, Utc::now())
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
            SourceTransformClient::new(create_rpc_channel(sock_file.clone()).await?),
            ReconnectConfig::new(
                sock_file.clone(),
                server_info_file.clone(),
                CancellationToken::new(),
                crate::config::components::transformer::DEFAULT_GRPC_MAX_MESSAGE_SIZE,
            ),
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
            ..Default::default()
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
            ..Default::default()
        };

        let request: SourceTransformRequest = message.into();
        assert!(request.request.is_some());
    }

    // TODO(ajain60): add unit test for metadata once rust sdk supports it
}
