use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use numaflow_monitor::runtime;
use numaflow_pb::clients::sourcetransformer::{
    self, SourceTransformRequest, SourceTransformResponse,
    source_transform_client::SourceTransformClient, source_transform_response,
};
use tokio::sync::{mpsc, oneshot};
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::Channel;
use tonic::{Request, Status, Streaming};

use crate::config::get_vertex_name;
use crate::config::pipeline::VERTEX_TYPE_SOURCE;
use crate::error::{Error, Result};
use crate::message::{Message, MessageID, Offset};
use crate::metadata::Metadata;
use crate::metrics::critical_error_reasons;
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

    fn drain_senders(sender_map: &ResponseSenderMap, error: Error) {
        let senders = {
            let mut senders = sender_map.lock().expect("failed to acquire poisoned lock");
            std::mem::take(&mut *senders)
        };
        for (_, (_, sender)) in senders {
            let _ = sender.send(Err(error.clone()));
        }
    }

    fn record_udf_error(status: &Status) {
        critical_error!(
            VERTEX_TYPE_SOURCE,
            critical_error_reasons::SOURCE_TRANSFORMER_RUNTIME_ERROR
        );
        runtime::persist_application_error(status.clone());
    }

    // receive responses from the server and gets the corresponding oneshot sender from the map
    // and sends the response.
    async fn receive_responses(
        sender_map: ResponseSenderMap,
        mut resp_stream: Streaming<SourceTransformResponse>,
    ) {
        loop {
            let resp = match resp_stream.message().await {
                Ok(Some(resp)) => resp,
                Ok(None) => {
                    let status = Status::unavailable("source transformer stream closed");
                    Self::record_udf_error(&status);
                    Self::drain_senders(&sender_map, Error::UdfRedrive(Box::new(status)));
                    break;
                }
                Err(e) => {
                    Self::record_udf_error(&e);
                    Self::drain_senders(&sender_map, Error::UdfRedrive(Box::new(e.clone())));
                    break;
                }
            };
            let msg_id = resp.id;
            let sender_entry = {
                sender_map
                    .lock()
                    .expect("failed to acquire poisoned lock")
                    .remove(&msg_id)
            };
            if let Some((msg_info, sender)) = sender_entry {
                let mut response_messages = vec![];
                for (i, result) in resp.results.into_iter().enumerate() {
                    let message = UserDefinedTransformerMessage(result, &msg_info, i as i32).into();
                    response_messages.push(message);
                }
                let _ = sender.send(Ok(response_messages));
            }
        }
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
        );
        self.task_handle.abort();
        self.read_tx = read_tx;
        self.task_handle = tokio::spawn(Self::receive_responses(
            Arc::clone(&self.senders),
            resp_stream,
        ));
        Ok(())
    }

    async fn queue_request(
        &mut self,
        key: &str,
        msg_info: ParentMessageInfo,
        respond_to: oneshot::Sender<Result<Vec<Message>>>,
        request: SourceTransformRequest,
    ) -> Option<(ParentMessageInfo, oneshot::Sender<Result<Vec<Message>>>)> {
        {
            self.senders
                .lock()
                .expect("failed to acquire poisoned lock")
                .insert(key.to_string(), (msg_info, respond_to));
        }

        if self.read_tx.send(request).await.is_ok() {
            return None;
        }

        self.senders
            .lock()
            .expect("failed to acquire poisoned lock")
            .remove(key)
    }

    /// Handles the incoming message and sends it to the server for transformation.
    pub(super) async fn transform(
        &mut self,
        message: Message,
        respond_to: oneshot::Sender<Result<Vec<Message>>>,
    ) {
        if self.task_handle.is_finished()
            && let Err(e) = self.reconnect().await
        {
            let _ = respond_to.send(Err(e));
            return;
        }

        let key = message.offset.clone().to_string();

        let msg_info = ParentMessageInfo {
            offset: message.offset.clone(),
            headers: Arc::clone(&message.headers),
            is_late: message.is_late,
            metadata: message.metadata.clone(),
        };

        let request: SourceTransformRequest = message.into();

        let Some((msg_info, respond_to)) = self
            .queue_request(&key, msg_info, respond_to, request.clone())
            .await
        else {
            return;
        };

        if let Err(e) = self.reconnect().await {
            let _ = respond_to.send(Err(e));
            return;
        }

        if let Some((_, respond_to)) = self
            .queue_request(&key, msg_info, respond_to, request)
            .await
        {
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
    use numaflow_pb::common::metadata;
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
                crate::shared::grpc::GrpcClientConfig::new(
                    sock_file.clone(),
                    server_info_file.clone(),
                    crate::config::components::transformer::DEFAULT_GRPC_MAX_MESSAGE_SIZE,
                ),
                CancellationToken::new(),
                crate::shared::grpc::DEFAULT_RECONNECT_INTERVAL,
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

    #[tokio::test]
    async fn transformer_drains_pending_senders_with_udf_redrive() {
        let sender_map: ResponseSenderMap = Arc::new(Mutex::new(HashMap::new()));
        let (first_tx, first_rx) = oneshot::channel();
        let (second_tx, second_rx) = oneshot::channel();

        let first_msg_info = ParentMessageInfo {
            offset: Offset::String(StringOffset::new("first".to_string(), 0)),
            is_late: false,
            headers: Arc::new(HashMap::new()),
            metadata: None,
        };
        let second_msg_info = ParentMessageInfo {
            offset: Offset::String(StringOffset::new("second".to_string(), 0)),
            is_late: false,
            headers: Arc::new(HashMap::new()),
            metadata: None,
        };

        {
            let mut senders = sender_map.lock().expect("failed to acquire poisoned lock");
            senders.insert("first".to_string(), (first_msg_info, first_tx));
            senders.insert("second".to_string(), (second_msg_info, second_tx));
        }

        UserDefinedTransformer::drain_senders(
            &sender_map,
            crate::error::Error::UdfRedrive(Box::new(Status::unavailable(
                "source transformer stream closed",
            ))),
        );

        assert!(
            sender_map
                .lock()
                .expect("failed to acquire poisoned lock")
                .is_empty()
        );
        assert!(matches!(
            first_rx.await.unwrap(),
            Err(crate::error::Error::UdfRedrive(_))
        ));
        assert!(matches!(
            second_rx.await.unwrap(),
            Err(crate::error::Error::UdfRedrive(_))
        ));
    }

    #[test]
    fn transformer_message_conversion_preserves_parent_and_response_metadata() {
        let parent_metadata = Metadata {
            previous_vertex: "source".to_string(),
            sys_metadata: HashMap::from([(
                "system".to_string(),
                crate::metadata::KeyValueGroup {
                    key_value: HashMap::from([("trace".to_string(), "parent".into())]),
                },
            )]),
            user_metadata: HashMap::new(),
        };
        let msg_info = ParentMessageInfo {
            offset: Offset::String(StringOffset::new("parent-offset".to_string(), 0)),
            is_late: true,
            headers: Arc::new(HashMap::from([(
                "header-key".to_string(),
                "header-value".to_string(),
            )])),
            metadata: Some(Arc::new(parent_metadata)),
        };
        let response_metadata = metadata::Metadata {
            previous_vertex: "transformer".to_string(),
            sys_metadata: HashMap::new(),
            user_metadata: HashMap::from([(
                "user".to_string(),
                metadata::KeyValueGroup {
                    key_value: HashMap::from([("k".to_string(), b"v".to_vec())]),
                },
            )]),
        };
        let response = source_transform_response::Result {
            keys: vec!["key".to_string()],
            value: b"value".to_vec(),
            event_time: Some(prost_timestamp_from_utc(
                Utc.timestamp_opt(1627846261, 0).unwrap(),
            )),
            tags: vec!["tag".to_string()],
            metadata: Some(response_metadata),
        };

        let message: Message = UserDefinedTransformerMessage(response, &msg_info, 3).into();

        assert_eq!(message.offset, msg_info.offset);
        assert!(message.is_late);
        assert_eq!(
            message.headers.get("header-key").map(String::as_str),
            Some("header-value")
        );
        assert_eq!(message.keys.to_vec(), vec!["key".to_string()]);
        assert_eq!(message.tags.as_deref(), Some(&["tag".to_string()][..]));
        let metadata = message.metadata.expect("metadata should be present");
        assert!(metadata.sys_metadata.contains_key("system"));
        assert!(metadata.user_metadata.contains_key("user"));
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
