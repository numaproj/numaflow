use std::collections::HashMap;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use numaflow_pb::clients::map::{self, map_client::MapClient, MapRequest, MapResponse};
use tokio::sync::Mutex;
use tokio::sync::{mpsc, oneshot};
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::Channel;
use tonic::{Request, Streaming};
use tracing::error;

use crate::config::get_vertex_name;
use crate::error::{Error, Result};
use crate::message::{Message, MessageID, Offset};
use crate::shared::grpc::prost_timestamp_from_utc;

type ResponseSenderMap =
    Arc<Mutex<HashMap<String, (ParentMessageInfo, oneshot::Sender<Result<Vec<Message>>>)>>>;

type StreamResponseSenderMap =
    Arc<Mutex<HashMap<String, (ParentMessageInfo, mpsc::Sender<Result<Message>>)>>>;

struct ParentMessageInfo {
    offset: Offset,
    event_time: DateTime<Utc>,
    headers: HashMap<String, String>,
}

impl From<Message> for MapRequest {
    fn from(message: Message) -> Self {
        Self {
            request: Some(map::map_request::Request {
                keys: message.keys.to_vec(),
                value: message.value.to_vec(),
                event_time: prost_timestamp_from_utc(message.event_time),
                watermark: None,
                headers: message.headers,
            }),
            id: message.offset.unwrap().to_string(),
            handshake: None,
            status: None,
        }
    }
}

/// UserDefinedUnaryMap is a grpc client that sends unary requests to the map server
/// and forwards the responses.
pub(in crate::mapper) struct UserDefinedUnaryMap {
    read_tx: mpsc::Sender<MapRequest>,
    senders: ResponseSenderMap,
    task_handle: tokio::task::JoinHandle<()>,
}

/// Abort the background task that receives responses when the UserDefinedBatchMap is dropped.
impl Drop for UserDefinedUnaryMap {
    fn drop(&mut self) {
        self.task_handle.abort();
    }
}

impl UserDefinedUnaryMap {
    /// Performs handshake with the server and creates a new UserDefinedMap.
    pub(in crate::mapper) async fn new(
        batch_size: usize,
        mut client: MapClient<Channel>,
    ) -> Result<Self> {
        let (read_tx, read_rx) = mpsc::channel(batch_size);
        let resp_stream = create_response_stream(read_tx.clone(), read_rx, &mut client).await?;

        // map to track the oneshot sender for each request along with the message info
        let sender_map = Arc::new(Mutex::new(HashMap::new()));

        // background task to receive responses from the server and send them to the appropriate
        // oneshot sender based on the message id
        let task_handle = tokio::spawn(Self::receive_unary_responses(
            Arc::clone(&sender_map),
            resp_stream,
        ));

        let mapper = Self {
            read_tx,
            senders: sender_map,
            task_handle,
        };

        Ok(mapper)
    }

    /// receive responses from the server and gets the corresponding oneshot response sender from the map
    /// and sends the response.
    async fn receive_unary_responses(
        sender_map: ResponseSenderMap,
        mut resp_stream: Streaming<MapResponse>,
    ) {
        while let Some(resp) = match resp_stream.message().await {
            Ok(message) => message,
            Err(e) => {
                let error = Error::Mapper(format!("failed to receive map response: {}", e));
                let mut senders = sender_map.lock().await;
                for (_, (_, sender)) in senders.drain() {
                    let _ = sender.send(Err(error.clone()));
                }
                None
            }
        } {
            process_response(&sender_map, resp).await
        }
    }

    /// Handles the incoming message and sends it to the server for mapping.
    pub(in crate::mapper) async fn unary_map(
        &mut self,
        message: Message,
        respond_to: oneshot::Sender<Result<Vec<Message>>>,
    ) {
        let key = message.offset.clone().unwrap().to_string();
        let msg_info = ParentMessageInfo {
            offset: message.offset.clone().expect("offset can never be none"),
            event_time: message.event_time,
            headers: message.headers.clone(),
        };

        self.senders
            .lock()
            .await
            .insert(key, (msg_info, respond_to));

        self.read_tx
            .send(message.into())
            .await
            .expect("failed to send message");
    }
}

/// UserDefinedBatchMap is a grpc client that sends batch requests to the map server
/// and forwards the responses.
pub(in crate::mapper) struct UserDefinedBatchMap {
    read_tx: mpsc::Sender<MapRequest>,
    senders: ResponseSenderMap,
    task_handle: tokio::task::JoinHandle<()>,
}

/// Abort the background task that receives responses when the UserDefinedBatchMap is dropped.
impl Drop for UserDefinedBatchMap {
    fn drop(&mut self) {
        self.task_handle.abort();
    }
}

impl UserDefinedBatchMap {
    /// Performs handshake with the server and creates a new UserDefinedMap.
    pub(in crate::mapper) async fn new(
        batch_size: usize,
        mut client: MapClient<Channel>,
    ) -> Result<Self> {
        let (read_tx, read_rx) = mpsc::channel(batch_size);
        let resp_stream = create_response_stream(read_tx.clone(), read_rx, &mut client).await?;

        // map to track the oneshot response sender for each request along with the message info
        let sender_map = Arc::new(Mutex::new(HashMap::new()));

        // background task to receive responses from the server and send them to the appropriate
        // oneshot response sender based on the id
        let task_handle = tokio::spawn(Self::receive_batch_responses(
            Arc::clone(&sender_map),
            resp_stream,
        ));

        let mapper = Self {
            read_tx,
            senders: sender_map,
            task_handle,
        };
        Ok(mapper)
    }

    /// receive responses from the server and gets the corresponding oneshot response sender from the map
    /// and sends the response.
    async fn receive_batch_responses(
        sender_map: ResponseSenderMap,
        mut resp_stream: Streaming<MapResponse>,
    ) {
        while let Some(resp) = match resp_stream.message().await {
            Ok(message) => message,
            Err(e) => {
                let error = Error::Mapper(format!("failed to receive map response: {}", e));
                let mut senders = sender_map.lock().await;
                for (_, (_, sender)) in senders.drain() {
                    sender
                        .send(Err(error.clone()))
                        .expect("failed to send error response");
                }
                None
            }
        } {
            if let Some(map::TransmissionStatus { eot: true }) = resp.status {
                if !sender_map.lock().await.is_empty() {
                    error!("received EOT but not all responses have been received");
                }
                continue;
            }

            process_response(&sender_map, resp).await
        }
    }

    /// Handles the incoming message and sends it to the server for mapping.
    pub(in crate::mapper) async fn batch_map(
        &mut self,
        messages: Vec<Message>,
        respond_to: Vec<oneshot::Sender<Result<Vec<Message>>>>,
    ) {
        for (message, respond_to) in messages.into_iter().zip(respond_to) {
            let key = message.offset.clone().unwrap().to_string();
            let msg_info = ParentMessageInfo {
                offset: message.offset.clone().expect("offset can never be none"),
                event_time: message.event_time,
                headers: message.headers.clone(),
            };

            self.senders
                .lock()
                .await
                .insert(key, (msg_info, respond_to));
            self.read_tx
                .send(message.into())
                .await
                .expect("failed to send message");
        }

        // send eot request
        self.read_tx
            .send(MapRequest {
                request: None,
                id: "".to_string(),
                handshake: None,
                status: Some(map::TransmissionStatus { eot: true }),
            })
            .await
            .expect("failed to send eot request");
    }
}

/// Processes the response from the server and sends it to the appropriate oneshot sender
/// based on the message id entry in the map.
async fn process_response(sender_map: &ResponseSenderMap, resp: MapResponse) {
    let msg_id = resp.id;
    if let Some((msg_info, sender)) = sender_map.lock().await.remove(&msg_id) {
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
                offset: Some(msg_info.offset.clone()),
                event_time: msg_info.event_time,
                headers: msg_info.headers.clone(),
                metadata: None,
            };
            response_messages.push(message);
        }
        sender
            .send(Ok(response_messages))
            .expect("failed to send response");
    }
}

/// Performs handshake with the server and returns the response stream to receive responses.
async fn create_response_stream(
    read_tx: mpsc::Sender<MapRequest>,
    read_rx: mpsc::Receiver<MapRequest>,
    client: &mut MapClient<Channel>,
) -> Result<Streaming<MapResponse>> {
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

    let mut resp_stream = client
        .map_fn(Request::new(ReceiverStream::new(read_rx)))
        .await?
        .into_inner();

    let handshake_response = resp_stream.message().await?.ok_or(Error::Mapper(
        "failed to receive handshake response".to_string(),
    ))?;

    if handshake_response.handshake.map_or(true, |h| !h.sot) {
        return Err(Error::Mapper("invalid handshake response".to_string()));
    }

    Ok(resp_stream)
}

/// UserDefinedStreamMap is a grpc client that sends stream requests to the map server
pub(in crate::mapper) struct UserDefinedStreamMap {
    read_tx: mpsc::Sender<MapRequest>,
    senders: StreamResponseSenderMap,
    task_handle: tokio::task::JoinHandle<()>,
}

/// Abort the background task that receives responses when the UserDefinedBatchMap is dropped.
impl Drop for UserDefinedStreamMap {
    fn drop(&mut self) {
        self.task_handle.abort();
    }
}

impl UserDefinedStreamMap {
    /// Performs handshake with the server and creates a new UserDefinedMap.
    pub(in crate::mapper) async fn new(
        batch_size: usize,
        mut client: MapClient<Channel>,
    ) -> Result<Self> {
        let (read_tx, read_rx) = mpsc::channel(batch_size);
        let resp_stream = create_response_stream(read_tx.clone(), read_rx, &mut client).await?;

        // map to track the oneshot response sender for each request along with the message info
        let sender_map = Arc::new(Mutex::new(HashMap::new()));

        // background task to receive responses from the server and send them to the appropriate
        // mpsc sender based on the id
        let task_handle = tokio::spawn(Self::receive_stream_responses(
            Arc::clone(&sender_map),
            resp_stream,
        ));

        let mapper = Self {
            read_tx,
            senders: sender_map,
            task_handle,
        };
        Ok(mapper)
    }

    /// receive responses from the server and gets the corresponding oneshot sender from the map
    /// and sends the response.
    async fn receive_stream_responses(
        sender_map: StreamResponseSenderMap,
        mut resp_stream: Streaming<MapResponse>,
    ) {
        while let Some(resp) = match resp_stream.message().await {
            Ok(message) => message,
            Err(e) => {
                let error = Error::Mapper(format!("failed to receive map response: {}", e));
                let mut senders = sender_map.lock().await;
                for (_, (_, sender)) in senders.drain() {
                    let _ = sender.send(Err(error.clone())).await;
                }
                None
            }
        } {
            let (message_info, response_sender) = sender_map
                .lock()
                .await
                .remove(&resp.id)
                .expect("map entry should always be present");

            // once we get eot, we can drop the sender to let the callee
            // know that we are done sending responses
            if let Some(map::TransmissionStatus { eot: true }) = resp.status {
                continue;
            }

            for (i, result) in resp.results.into_iter().enumerate() {
                let message = Message {
                    id: MessageID {
                        vertex_name: get_vertex_name().to_string().into(),
                        index: i as i32,
                        offset: message_info.offset.to_string().into(),
                    },
                    keys: Arc::from(result.keys),
                    tags: Some(Arc::from(result.tags)),
                    value: result.value.into(),
                    offset: None,
                    event_time: message_info.event_time,
                    headers: message_info.headers.clone(),
                    metadata: None,
                };
                response_sender
                    .send(Ok(message))
                    .await
                    .expect("failed to send response");
            }

            // Write the sender back to the map, because we need to send
            // more responses for the same request
            sender_map
                .lock()
                .await
                .insert(resp.id, (message_info, response_sender));
        }
    }

    /// Handles the incoming message and sends it to the server for mapping.
    pub(in crate::mapper) async fn stream_map(
        &mut self,
        message: Message,
        respond_to: mpsc::Sender<Result<Message>>,
    ) {
        let key = message.offset.clone().unwrap().to_string();
        let msg_info = ParentMessageInfo {
            offset: message.offset.clone().expect("offset can never be none"),
            event_time: message.event_time,
            headers: message.headers.clone(),
        };

        self.senders
            .lock()
            .await
            .insert(key, (msg_info, respond_to));

        self.read_tx
            .send(message.into())
            .await
            .expect("failed to send message");
    }
}

#[cfg(test)]
mod tests {
    use numaflow::mapstream;
    use std::error::Error;
    use std::sync::Arc;
    use std::time::Duration;

    use numaflow::batchmap::Server;
    use numaflow::{batchmap, map};
    use numaflow_pb::clients::map::map_client::MapClient;
    use tempfile::TempDir;

    use crate::mapper::map::user_defined::{
        UserDefinedBatchMap, UserDefinedStreamMap, UserDefinedUnaryMap,
    };
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
            UserDefinedUnaryMap::new(500, MapClient::new(create_rpc_channel(sock_file).await?))
                .await?;

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
            metadata: None,
        };

        let (tx, rx) = tokio::sync::oneshot::channel();

        tokio::time::timeout(Duration::from_secs(2), client.unary_map(message, tx))
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

    struct SimpleBatchMap;

    #[tonic::async_trait]
    impl batchmap::BatchMapper for SimpleBatchMap {
        async fn batchmap(
            &self,
            mut input: tokio::sync::mpsc::Receiver<batchmap::Datum>,
        ) -> Vec<batchmap::BatchResponse> {
            let mut responses: Vec<batchmap::BatchResponse> = Vec::new();
            while let Some(datum) = input.recv().await {
                let mut response = batchmap::BatchResponse::from_id(datum.id);
                response.append(batchmap::Message {
                    keys: Option::from(datum.keys),
                    value: datum.value,
                    tags: None,
                });
                responses.push(response);
            }
            responses
        }
    }

    #[tokio::test]
    async fn batch_map_operations() -> Result<(), Box<dyn Error>> {
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let tmp_dir = TempDir::new()?;
        let sock_file = tmp_dir.path().join("batch_map.sock");
        let server_info_file = tmp_dir.path().join("batch_map-server-info");

        let server_info = server_info_file.clone();
        let server_socket = sock_file.clone();
        let handle = tokio::spawn(async move {
            Server::new(SimpleBatchMap)
                .with_socket_file(server_socket)
                .with_server_info_file(server_info)
                .start_with_shutdown(shutdown_rx)
                .await
                .expect("server failed");
        });

        // wait for the server to start
        tokio::time::sleep(Duration::from_millis(100)).await;

        let mut client =
            UserDefinedBatchMap::new(500, MapClient::new(create_rpc_channel(sock_file).await?))
                .await?;

        let messages = vec![
            crate::message::Message {
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
                metadata: None,
            },
            crate::message::Message {
                keys: Arc::from(vec!["second".into()]),
                tags: None,
                value: "world".into(),
                offset: Some(crate::message::Offset::String(StringOffset::new(
                    "1".to_string(),
                    1,
                ))),
                event_time: chrono::Utc::now(),
                id: MessageID {
                    vertex_name: "vertex_name".to_string().into(),
                    offset: "1".to_string().into(),
                    index: 1,
                },
                headers: Default::default(),
                metadata: None,
            },
        ];

        let (tx1, rx1) = tokio::sync::oneshot::channel();
        let (tx2, rx2) = tokio::sync::oneshot::channel();

        tokio::time::timeout(
            Duration::from_secs(2),
            client.batch_map(messages, vec![tx1, tx2]),
        )
        .await
        .unwrap();

        let messages1 = rx1.await.unwrap();
        let messages2 = rx2.await.unwrap();

        assert!(messages1.is_ok());
        assert!(messages2.is_ok());
        assert_eq!(messages1?.len(), 1);
        assert_eq!(messages2?.len(), 1);

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

    struct FlatmapStream;

    #[tonic::async_trait]
    impl mapstream::MapStreamer for FlatmapStream {
        async fn map_stream(
            &self,
            input: mapstream::MapStreamRequest,
            tx: tokio::sync::mpsc::Sender<mapstream::Message>,
        ) {
            let payload_str = String::from_utf8(input.value).unwrap_or_default();
            let splits: Vec<&str> = payload_str.split(',').collect();

            for split in splits {
                let message = mapstream::Message::new(split.as_bytes().to_vec())
                    .keys(input.keys.clone())
                    .tags(vec![]);
                if tx.send(message).await.is_err() {
                    break;
                }
            }
        }
    }

    #[tokio::test]
    async fn map_stream_operations() -> Result<(), Box<dyn Error>> {
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let tmp_dir = TempDir::new()?;
        let sock_file = tmp_dir.path().join("map_stream.sock");
        let server_info_file = tmp_dir.path().join("map_stream-server-info");

        let server_info = server_info_file.clone();
        let server_socket = sock_file.clone();
        let handle = tokio::spawn(async move {
            mapstream::Server::new(FlatmapStream)
                .with_socket_file(server_socket)
                .with_server_info_file(server_info)
                .start_with_shutdown(shutdown_rx)
                .await
                .expect("server failed");
        });

        // wait for the server to start
        tokio::time::sleep(Duration::from_millis(100)).await;

        let mut client =
            UserDefinedStreamMap::new(500, MapClient::new(create_rpc_channel(sock_file).await?))
                .await?;

        let message = crate::message::Message {
            keys: Arc::from(vec!["first".into()]),
            tags: None,
            value: "test,map,stream".into(),
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
            metadata: None,
        };

        let (tx, mut rx) = tokio::sync::mpsc::channel(3);

        tokio::time::timeout(Duration::from_secs(2), client.stream_map(message, tx))
            .await
            .unwrap();

        let mut responses = vec![];
        while let Some(response) = rx.recv().await {
            responses.push(response.unwrap());
        }

        assert_eq!(responses.len(), 3);
        // convert the bytes value to string and compare
        let values: Vec<String> = responses
            .iter()
            .map(|r| String::from_utf8(Vec::from(r.value.clone())).unwrap())
            .collect();
        assert_eq!(values, vec!["test", "map", "stream"]);

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
