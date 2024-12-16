use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use chrono::{DateTime, Utc};
use numaflow_pb::clients::map::{self, map_client::MapClient, MapRequest, MapResponse};
use tokio::sync::{mpsc, oneshot};
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::Channel;
use tonic::{Request, Streaming};

use crate::config::get_vertex_name;
use crate::error::{Error, Result};
use crate::message::{Message, MessageID, Offset};

type ResponseSenderMap =
    Arc<Mutex<HashMap<String, (ParentMessageInfo, oneshot::Sender<Result<Vec<Message>>>)>>>;

struct ParentMessageInfo {
    offset: Offset,
    event_time: DateTime<Utc>,
    headers: HashMap<String, String>,
}

/// UserDefinedMap exposes methods to do user-defined mappings.
pub(super) struct UserDefinedMap {
    read_tx: mpsc::Sender<MapRequest>,
    senders: ResponseSenderMap,
}

impl UserDefinedMap {
    /// Performs handshake with the server and creates a new UserDefinedMap.
    pub(super) async fn new(batch_size: usize, mut client: MapClient<Channel>) -> Result<Self> {
        let (read_tx, read_rx) = mpsc::channel(batch_size);
        let read_stream = ReceiverStream::new(read_rx);

        // perform handshake
        let handshake_request = MapRequest {
            request: None,
            id: "".to_string(),
            handshake: Some(map::Handshake { sot: true }),
            status: None,
        };
        read_tx
            .send(handshake_request)
            .await
            .map_err(|e| Error::Mapper(format!("failed to send handshake request: {}", e)))?;

        let mut resp_stream = client.map_fn(Request::new(read_stream)).await?.into_inner();

        let handshake_response = resp_stream.message().await?.ok_or(Error::Mapper(
            "failed to receive handshake response".to_string(),
        ))?;

        if handshake_response.handshake.map_or(true, |h| !h.sot) {
            return Err(Error::Mapper("invalid handshake response".to_string()));
        }

        // mapper to track the oneshot sender for each request along with the message info
        let sender_map = Arc::new(Mutex::new(HashMap::new()));

        let mapper = Self {
            read_tx,
            senders: Arc::clone(&sender_map),
        };

        // background task to receive responses from the server and send them to the appropriate
        // oneshot sender based on the message id
        tokio::spawn(Self::receive_responses(sender_map, resp_stream));

        Ok(mapper)
    }

    // receive responses from the server and gets the corresponding oneshot sender from the map
    // and sends the response.
    async fn receive_responses(
        sender_map: ResponseSenderMap,
        mut resp_stream: Streaming<MapResponse>,
    ) {
        while let Some(resp) = resp_stream
            .message()
            .await
            .expect("failed to receive response")
        {
            let msg_id = resp.id;
            if let Some((msg_info, sender)) = sender_map
                .lock()
                .expect("map entry should always be present")
                .remove(&msg_id)
            {
                let mut response_messages = vec![];
                for (i, result) in resp.results.into_iter().enumerate() {
                    let message = Message {
                        id: MessageID {
                            vertex_name: get_vertex_name().to_string().into(),
                            index: i as i32,
                            offset: msg_info.offset.to_string().into(),
                        },
                        keys: Arc::from(result.keys),
                        tags: Some(Arc::from(result.tags)),
                        value: result.value.into(),
                        offset: None,
                        event_time: msg_info.event_time,
                        headers: msg_info.headers.clone(),
                    };
                    response_messages.push(message);
                }
                sender
                    .send(Ok(response_messages))
                    .expect("failed to send response");
            }
        }
    }

    /// Handles the incoming message and sends it to the server for mapping.
    pub(super) async fn map(
        &mut self,
        message: Message,
        respond_to: oneshot::Sender<Result<Vec<Message>>>,
    ) {
        let msg_id = message.id.to_string();
        let msg_info = ParentMessageInfo {
            offset: message.offset.clone().expect("offset can never be none"),
            event_time: message.event_time,
            headers: message.headers.clone(),
        };

        self.senders
            .lock()
            .unwrap()
            .insert(msg_id, (msg_info, respond_to));

        self.read_tx.send(message.into()).await.unwrap();
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::sync::Arc;
    use std::time::Duration;

    use numaflow::map;
    use numaflow_pb::clients::map::map_client::MapClient;
    use tempfile::TempDir;

    use crate::mapper::user_defined::UserDefinedMap;
    use crate::message::{MessageID, StringOffset};
    use crate::shared::grpc::create_rpc_channel;

    struct Cat;

    #[tonic::async_trait]
    impl map::Mapper for Cat {
        async fn map(&self, input: map::MapRequest) -> Vec<map::Message> {
            let message = map::Message::new(input.value).keys(input.keys).tags(vec![]);
            vec![message]
        }
    }

    #[tokio::test]
    async fn map_operations() -> Result<(), Box<dyn Error>> {
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let tmp_dir = TempDir::new()?;
        let sock_file = tmp_dir.path().join("map.sock");
        let server_info_file = tmp_dir.path().join("map-server-info");

        let server_info = server_info_file.clone();
        let server_socket = sock_file.clone();
        let handle = tokio::spawn(async move {
            map::Server::new(Cat)
                .with_socket_file(server_socket)
                .with_server_info_file(server_info)
                .start_with_shutdown(shutdown_rx)
                .await
                .expect("server failed");
        });

        // wait for the server to start
        tokio::time::sleep(Duration::from_millis(100)).await;

        let mut client =
            UserDefinedMap::new(500, MapClient::new(create_rpc_channel(sock_file).await?)).await?;

        let message = crate::message::Message {
            keys: Arc::from(vec!["first".into()]),
            tags: None,
            value: "hello".into(),
            offset: Some(crate::message::Offset::String(StringOffset::new(
                "0".to_string(),
                0,
            ))),
            event_time: chrono::Utc::now(),
            id: MessageID {
                vertex_name: "vertex_name".to_string().into(),
                offset: "0".to_string().into(),
                index: 0,
            },
            headers: Default::default(),
        };

        let (tx, rx) = tokio::sync::oneshot::channel();

        tokio::time::timeout(Duration::from_secs(2), client.map(message, tx))
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
}
