use std::collections::HashMap;

use numaflow_pb::clients::sourcetransformer::{
    self, source_transform_client::SourceTransformClient, SourceTransformRequest,
    SourceTransformResponse,
};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;
use tonic::transport::Channel;
use tonic::{Request, Streaming};
use tracing::warn;

use crate::error::{Error, Result};
use crate::message::{get_vertex_name, Message, MessageID, Offset};
use crate::shared::utils::utc_from_timestamp;

const DROP: &str = "U+005C__DROP__";

/// TransformerClient is a client to interact with the transformer server.
struct SourceTransformer {
    actor_messages: mpsc::Receiver<ActorMessage>,
    read_tx: mpsc::Sender<SourceTransformRequest>,
    resp_stream: Streaming<SourceTransformResponse>,
}

impl SourceTransformer {
    async fn new(
        batch_size: usize,
        mut client: SourceTransformClient<Channel>,
        actor_messages: mpsc::Receiver<ActorMessage>,
    ) -> Result<Self> {
        let (read_tx, read_rx) = mpsc::channel(batch_size);
        let read_stream = ReceiverStream::new(read_rx);

        // do a handshake for read with the server before we start sending read requests
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

        // first response from the server will be the handshake response. We need to check if the
        // server has accepted the handshake.
        let handshake_response = resp_stream.message().await?.ok_or(Error::Transformer(
            "failed to receive handshake response".to_string(),
        ))?;

        // handshake cannot to None during the initial phase and it has to set `sot` to true.
        if handshake_response.handshake.map_or(true, |h| !h.sot) {
            return Err(Error::Transformer("invalid handshake response".to_string()));
        }

        Ok(Self {
            actor_messages,
            read_tx,
            resp_stream,
        })
    }

    async fn handle_message(&mut self, message: ActorMessage) {
        match message {
            ActorMessage::Transform {
                messages,
                respond_to,
            } => {
                let result = self.transform_fn(messages).await;
                let _ = respond_to.send(result);
            }
        }
    }

    async fn transform_fn(&mut self, messages: Vec<Message>) -> Result<Vec<Message>> {
        // fields which will not be changed
        struct MessageInfo {
            offset: Offset,
            headers: HashMap<String, String>,
        }

        let mut tracker: HashMap<String, MessageInfo> = HashMap::with_capacity(messages.len());
        for message in &messages {
            tracker.insert(
                message.id.to_string(),
                MessageInfo {
                    offset: message
                        .offset
                        .clone()
                        .ok_or(Error::Transformer("Message offset is missing".to_string()))?,
                    headers: message.headers.clone(),
                },
            );
        }

        // Cancellation token is used to cancel either sending task (if an error occurs while receiving) or receiving messages (if an error occurs on sending task)
        let token = CancellationToken::new();

        // Send transform requests to the source transformer server
        let sender_task: JoinHandle<Result<()>> = tokio::spawn({
            let read_tx = self.read_tx.clone();
            let token = token.clone();
            async move {
                for msg in messages {
                    let result = tokio::select! {
                        result = read_tx.send(msg.into()) => result,
                        _ = token.cancelled() => {
                            warn!("Cancellation token was cancelled while sending source transform requests");
                            return Ok(());
                        },
                    };

                    match result {
                        Ok(()) => continue,
                        Err(e) => {
                            token.cancel();
                            return Err(Error::Transformer(e.to_string()));
                        }
                    };
                }
                Ok(())
            }
        });

        // Receive transformer results
        let mut messages = Vec::new();
        while !tracker.is_empty() {
            let resp = tokio::select! {
                _ = token.cancelled() => {
                    break;
                },
                resp = self.resp_stream.message() => {resp}
            };

            let resp = match resp {
                Ok(Some(val)) => val,
                Ok(None) => {
                    // Logging at warning level since we don't expect this to happen
                    warn!("Source transformer server closed its sending end of the stream. No more messages to receive");
                    token.cancel();
                    break;
                }
                Err(e) => {
                    token.cancel();
                    return Err(Error::Transformer(format!(
                        "gRPC error while receiving messages from source transformer server: {e:?}"
                    )));
                }
            };

            let Some((_, msg_info)) = tracker.remove_entry(&resp.id) else {
                token.cancel();
                return Err(Error::Transformer(format!(
                    "Received message with unknown ID {}",
                    resp.id
                )));
            };

            for (i, result) in resp.results.into_iter().enumerate() {
                // TODO: Expose metrics
                if result.tags.iter().any(|x| x == DROP) {
                    continue;
                }
                let message = Message {
                    id: MessageID {
                        vertex_name: get_vertex_name().to_string(),
                        index: i as i32,
                        offset: msg_info.offset.to_string(),
                    },
                    keys: result.keys,
                    value: result.value,
                    offset: None,
                    event_time: utc_from_timestamp(result.event_time),
                    headers: msg_info.headers.clone(),
                };
                messages.push(message);
            }
        }

        sender_task.await.unwrap().map_err(|e| {
            Error::Transformer(format!(
                "Sending messages to gRPC transformer failed: {e:?}",
            ))
        })?;

        Ok(messages)
    }
}

enum ActorMessage {
    Transform {
        messages: Vec<Message>,
        respond_to: oneshot::Sender<Result<Vec<Message>>>,
    },
}

#[derive(Clone)]
pub(crate) struct SourceTransformHandle {
    sender: mpsc::Sender<ActorMessage>,
}

impl SourceTransformHandle {
    pub(crate) async fn new(client: SourceTransformClient<Channel>) -> Result<Self> {
        let batch_size = 500;
        let (sender, receiver) = mpsc::channel(batch_size);
        let mut client = SourceTransformer::new(batch_size, client, receiver).await?;
        tokio::spawn(async move {
            while let Some(msg) = client.actor_messages.recv().await {
                client.handle_message(msg).await;
            }
        });
        Ok(Self { sender })
    }

    pub(crate) async fn transform(&self, messages: Vec<Message>) -> Result<Vec<Message>> {
        let (sender, receiver) = oneshot::channel();
        let msg = ActorMessage::Transform {
            messages,
            respond_to: sender,
        };
        let _ = self.sender.send(msg).await;
        receiver.await.unwrap()
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
    use crate::transformer::user_defined::SourceTransformHandle;

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

        let client = SourceTransformHandle::new(SourceTransformClient::new(
            create_rpc_channel(sock_file).await?,
        ))
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

        let resp =
            tokio::time::timeout(Duration::from_secs(2), client.transform(vec![message])).await??;
        assert_eq!(resp.len(), 1);

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

    struct FilterCat;

    #[tonic::async_trait]
    impl sourcetransform::SourceTransformer for FilterCat {
        async fn transform(
            &self,
            input: sourcetransform::SourceTransformRequest,
        ) -> Vec<sourcetransform::Message> {
            let message = sourcetransform::Message::new(input.value, chrono::offset::Utc::now())
                .keys(input.keys)
                .tags(vec![crate::transformer::user_defined::DROP.to_string()]);
            vec![message]
        }
    }

    #[tokio::test]
    async fn transformer_operations_with_drop() -> Result<(), Box<dyn Error>> {
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let tmp_dir = TempDir::new()?;
        let sock_file = tmp_dir.path().join("sourcetransform.sock");
        let server_info_file = tmp_dir.path().join("sourcetransformer-server-info");

        let server_info = server_info_file.clone();
        let server_socket = sock_file.clone();
        let handle = tokio::spawn(async move {
            sourcetransform::Server::new(FilterCat)
                .with_socket_file(server_socket)
                .with_server_info_file(server_info)
                .start_with_shutdown(shutdown_rx)
                .await
                .expect("server failed");
        });

        // wait for the server to start
        tokio::time::sleep(Duration::from_millis(100)).await;

        let client = SourceTransformHandle::new(SourceTransformClient::new(
            create_rpc_channel(sock_file).await?,
        ))
        .await?;

        let message = crate::message::Message {
            keys: vec!["second".into()],
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

        let resp = client.transform(vec![message]).await?;
        assert!(resp.is_empty());

        // we need to drop the client, because if there are any in-flight requests
        // server fails to shut down. https://github.com/numaproj/numaflow-rs/issues/85
        drop(client);

        shutdown_tx
            .send(())
            .expect("failed to send shutdown signal");
        handle.await.expect("failed to join server task");
        Ok(())
    }
}
