use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use numaflow_pb::clients::sourcetransformer::{
    self, source_transform_client::SourceTransformClient, SourceTransformRequest,
    SourceTransformResponse,
};
use tokio::sync::{mpsc, oneshot};
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::Channel;
use tonic::{Request, Streaming};

use crate::error::{Error, Result};
use crate::message::{Message, MessageID, Offset};
use crate::shared::utils::{get_vertex_name, utc_from_timestamp};

type ResponseSenderMap =
    Arc<Mutex<HashMap<String, (ParentMessageInfo, oneshot::Sender<Result<Vec<Message>>>)>>>;

// fields which will not be changed
struct ParentMessageInfo {
    offset: Offset,
    headers: HashMap<String, String>,
}

pub enum ActorMessage {
    Transform {
        message: Message,
        respond_to: oneshot::Sender<Result<Vec<Message>>>,
    },
}

/// UserDefinedTransformer exposes methods to do user-defined transformations.
pub(super) struct UserDefinedTransformer {
    read_tx: mpsc::Sender<SourceTransformRequest>,
    senders: ResponseSenderMap,
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
            .await?
            .into_inner();

        let handshake_response = resp_stream.message().await?.ok_or(Error::Transformer(
            "failed to receive handshake response".to_string(),
        ))?;

        if handshake_response.handshake.map_or(true, |h| !h.sot) {
            return Err(Error::Transformer("invalid handshake response".to_string()));
        }

        // map to track the oneshot sender for each request along with the message info
        let sender_map = Arc::new(Mutex::new(HashMap::new()));

        let transformer = Self {
            read_tx,
            senders: sender_map.clone(),
        };

        // background task to receive responses from the server and send them to the appropriate
        // oneshot sender based on the message id
        tokio::spawn(Self::receive_responses(sender_map, resp_stream));

        Ok(transformer)
    }

    // receive responses from the server and gets the corresponding oneshot sender from the map
    // and sends the response.
    async fn receive_responses(
        sender_map: ResponseSenderMap,
        mut resp_stream: Streaming<SourceTransformResponse>,
    ) {
        while let Some(resp) = resp_stream.message().await.unwrap() {
            let msg_id = resp.id;
            for (i, result) in resp.results.into_iter().enumerate() {
                if let Some((msg_info, sender)) = sender_map
                    .lock()
                    .expect("map entry should always be present")
                    .remove(&msg_id)
                {
                    let message = Message {
                        id: MessageID {
                            vertex_name: get_vertex_name().to_string(),
                            index: i as i32,
                            offset: msg_info.offset.to_string(),
                        },
                        keys: result.keys,
                        value: result.value.into(),
                        offset: None,
                        event_time: utc_from_timestamp(result.event_time),
                        headers: msg_info.headers.clone(),
                    };
                    let _ = sender.send(Ok(vec![message]));
                }
            }
        }
    }

    /// Handles the incoming message and sends it to the server for transformation.
    pub(super) async fn handle_message(&mut self, message: ActorMessage) {
        match message {
            ActorMessage::Transform {
                message,
                respond_to,
            } => {
                let msg_id = message.id.to_string();
                let msg_info = ParentMessageInfo {
                    offset: message.offset.clone().unwrap(),
                    headers: message.headers.clone(),
                };

                self.senders
                    .lock()
                    .unwrap()
                    .insert(msg_id, (msg_info, respond_to));

                self.read_tx.send(message.into()).await.unwrap();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::time::Duration;

    use numaflow::sourcetransform;
    use numaflow_pb::clients::sourcetransformer::source_transform_client::SourceTransformClient;
    use tempfile::TempDir;

    use crate::message::{MessageID, StringOffset};
    use crate::shared::utils::create_rpc_channel;
    use crate::transformer::user_defined::{ActorMessage, UserDefinedTransformer};
    struct NowCat;

    #[tonic::async_trait]
    impl sourcetransform::SourceTransformer for NowCat {
        async fn transform(
            &self,
            input: sourcetransform::SourceTransformRequest,
        ) -> Vec<sourcetransform::Message> {
            let message = sourcetransform::Message::new(input.value, chrono::offset::Utc::now())
                .keys(input.keys)
                .tags(vec![]);
            vec![message]
        }
    }

    #[tokio::test]
    async fn transformer_operations() -> Result<(), Box<dyn Error>> {
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
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

        let message = crate::message::Message {
            keys: vec!["first".into()],
            value: "hello".into(),
            offset: Some(crate::message::Offset::String(StringOffset::new(
                "0".to_string(),
                0,
            ))),
            event_time: chrono::Utc::now(),
            id: MessageID {
                vertex_name: "vertex_name".to_string(),
                offset: "0".to_string(),
                index: 0,
            },
            headers: Default::default(),
        };

        let (tx, rx) = tokio::sync::oneshot::channel();

        let _ = tokio::time::timeout(
            Duration::from_secs(2),
            client.handle_message(ActorMessage::Transform {
                message,
                respond_to: tx,
            }),
        )
        .await?;

        let messages = rx.await?;
        assert!(messages.is_ok());
        assert_eq!(messages.unwrap().len(), 1);

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
}
