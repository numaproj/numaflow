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

    // receive responses from the server and gets the corresponding oneshot sender from the mapper
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
                .expect("mapper entry should always be present")
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
