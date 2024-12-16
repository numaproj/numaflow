use std::sync::Arc;

use numaflow_pb::clients::map::map_client::MapClient;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::sync::OwnedSemaphorePermit;
use tokio::sync::Semaphore;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt;
use tonic::transport::Channel;
use tracing::error;

use crate::mapper::user_defined::UserDefinedMap;
use crate::message::Message;
use crate::tracker::TrackerHandle;
use crate::Error;
use crate::Result;

pub(crate) mod user_defined;

/// Actor message to be sent to the mapper actor.
pub enum ActorMessage {
    Map {
        message: Message,
        respond_to: oneshot::Sender<Result<Vec<Message>>>,
    },
}

/// Mapper actor that handles mapping messages. It receives messages from the sender and
/// invokes the user-defined map function.
struct MapperActor {
    receiver: mpsc::Receiver<ActorMessage>,
    mapper: UserDefinedMap,
}

impl MapperActor {
    fn new(receiver: mpsc::Receiver<ActorMessage>, mapper: UserDefinedMap) -> Self {
        Self { receiver, mapper }
    }

    async fn handle_message(&mut self, msg: ActorMessage) {
        match msg {
            ActorMessage::Map {
                message,
                respond_to,
            } => self.mapper.map(message, respond_to).await,
        }
    }

    async fn run(mut self) {
        while let Some(msg) = self.receiver.recv().await {
            self.handle_message(msg).await;
        }
    }
}

/// Mapper is responsible for reading messages from the stream and invoke the map operation
/// on those messages and send the mapped messages to the output stream.
pub(crate) struct Mapper {
    batch_size: usize,
    sender: mpsc::Sender<ActorMessage>,
    concurrency: usize,
    tracker_handle: TrackerHandle,
}

impl Mapper {
    /// Creates a new mapper with the given batch size, concurrency, client, and tracker handle.
    /// The mapper actor is spawned in the background to handle the messages.
    pub(crate) async fn new(
        batch_size: usize,
        concurrency: usize,
        client: MapClient<Channel>,
        tracker_handle: TrackerHandle,
    ) -> Result<Self> {
        let (sender, receiver) = mpsc::channel(batch_size);
        let mapper_actor =
            MapperActor::new(receiver, UserDefinedMap::new(batch_size, client).await?);

        tokio::spawn(async move {
            mapper_actor.run().await;
        });

        Ok(Self {
            batch_size,
            concurrency,
            sender,
            tracker_handle,
        })
    }

    async fn map(
        map_handle: mpsc::Sender<ActorMessage>,
        permit: OwnedSemaphorePermit,
        read_msg: Message,
        output_tx: mpsc::Sender<Message>,
        tracker_handle: TrackerHandle,
    ) -> Result<()> {
        let output_tx = output_tx.clone();

        tokio::spawn(async move {
            let _permit = permit;

            let (sender, receiver) = oneshot::channel();
            let msg = ActorMessage::Map {
                message: read_msg.clone(),
                respond_to: sender,
            };

            map_handle.send(msg).await.expect("failed to send message");

            match receiver.await {
                Ok(Ok(mut mapped_messages)) => {
                    tracker_handle
                        .update(
                            read_msg.id.offset.clone(),
                            mapped_messages.len() as u32,
                            false,
                        )
                        .await
                        .expect("failed to update tracker");
                    for mapped_message in mapped_messages.drain(..) {
                        let _ = output_tx.send(mapped_message).await;
                    }
                }
                Err(_) | Ok(Err(_)) => {
                    error!("Failed to map message");
                    tracker_handle
                        .discard(read_msg.id.offset.clone())
                        .await
                        .expect("failed to discard tracker");
                }
            }
        });

        Ok(())
    }

    /// Maps the input stream of messages and returns the output stream and the handle to the
    /// background task.
    pub(crate) async fn map_stream(
        &self,
        input_stream: ReceiverStream<Message>,
    ) -> Result<(ReceiverStream<Message>, JoinHandle<Result<()>>)> {
        let (output_tx, output_rx) = mpsc::channel(self.batch_size);

        let map_handle = self.sender.clone();
        let tracker_handle = self.tracker_handle.clone();
        let semaphore = Arc::new(Semaphore::new(self.concurrency));

        let handle = tokio::spawn(async move {
            let mut input_stream = input_stream;

            while let Some(read_msg) = input_stream.next().await {
                let permit = Arc::clone(&semaphore)
                    .acquire_owned()
                    .await
                    .map_err(|e| Error::Mapper(format!("failed to acquire semaphore: {}", e)))?;

                Self::map(
                    map_handle.clone(),
                    permit,
                    read_msg,
                    output_tx.clone(),
                    tracker_handle.clone(),
                )
                .await?;
            }
            Ok(())
        });

        Ok((ReceiverStream::new(output_rx), handle))
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use numaflow::map;
    use numaflow_pb::clients::map::map_client::MapClient;
    use tempfile::TempDir;
    use tokio::sync::oneshot;

    use super::*;
    use crate::message::{Message, MessageID, Offset};
    use crate::shared::grpc::create_rpc_channel;

    struct SimpleMapper;

    #[tonic::async_trait]
    impl map::Mapper for SimpleMapper {
        async fn map(&self, input: map::MapRequest) -> Vec<map::Message> {
            let message = map::Message::new(input.value)
                .keys(input.keys)
                .tags(vec!["test".to_string()]);
            vec![message]
        }
    }

    #[tokio::test]
    async fn mapper_operations() -> Result<()> {
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let tmp_dir = TempDir::new().unwrap();
        let sock_file = tmp_dir.path().join("map.sock");
        let server_info_file = tmp_dir.path().join("map-server-info");

        let server_info = server_info_file.clone();
        let server_socket = sock_file.clone();
        let handle = tokio::spawn(async move {
            map::Server::new(SimpleMapper)
                .with_socket_file(server_socket)
                .with_server_info_file(server_info)
                .start_with_shutdown(shutdown_rx)
                .await
                .expect("server failed");
        });

        // wait for the server to start
        tokio::time::sleep(Duration::from_millis(100)).await;
        let tracker_handle = TrackerHandle::new();

        let client = MapClient::new(create_rpc_channel(sock_file).await?);
        let mapper = Mapper::new(500, 10, client, tracker_handle.clone()).await?;

        let message = Message {
            keys: Arc::from(vec!["first".into()]),
            tags: None,
            value: "hello".into(),
            offset: Some(Offset::String(crate::message::StringOffset::new(
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

        let (output_tx, mut output_rx) = mpsc::channel(10);

        let semaphore = Arc::new(Semaphore::new(10));
        let permit = semaphore.acquire_owned().await.unwrap();
        Mapper::map(
            mapper.sender.clone(),
            permit,
            message,
            output_tx,
            tracker_handle,
        )
        .await?;

        let mapped_message = output_rx.recv().await.unwrap();
        assert_eq!(mapped_message.value, "hello");

        // we need to drop the mapper, because if there are any in-flight requests
        // server fails to shut down. https://github.com/numaproj/numaflow-rs/issues/85
        drop(mapper);

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
    async fn test_map_stream() -> Result<()> {
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let tmp_dir = TempDir::new().unwrap();
        let sock_file = tmp_dir.path().join("map.sock");
        let server_info_file = tmp_dir.path().join("map-server-info");

        let server_info = server_info_file.clone();
        let server_socket = sock_file.clone();
        let handle = tokio::spawn(async move {
            map::Server::new(SimpleMapper)
                .with_socket_file(server_socket)
                .with_server_info_file(server_info)
                .start_with_shutdown(shutdown_rx)
                .await
                .expect("server failed");
        });

        // wait for the server to start
        tokio::time::sleep(Duration::from_millis(100)).await;

        let tracker_handle = TrackerHandle::new();
        let client = MapClient::new(create_rpc_channel(sock_file).await?);
        let mapper = Mapper::new(500, 10, client, tracker_handle.clone()).await?;

        let (input_tx, input_rx) = mpsc::channel(10);
        let input_stream = ReceiverStream::new(input_rx);

        for i in 0..5 {
            let message = Message {
                keys: Arc::from(vec![format!("key_{}", i)]),
                tags: None,
                value: format!("value_{}", i).into(),
                offset: Some(Offset::String(crate::message::StringOffset::new(
                    i.to_string(),
                    0,
                ))),
                event_time: chrono::Utc::now(),
                id: MessageID {
                    vertex_name: "vertex_name".to_string().into(),
                    offset: i.to_string().into(),
                    index: i,
                },
                headers: Default::default(),
            };
            input_tx.send(message).await.unwrap();
        }
        drop(input_tx);

        let (output_stream, map_handle) = mapper.map_stream(input_stream).await?;

        let mut output_rx = output_stream.into_inner();

        for i in 0..5 {
            let mapped_message = output_rx.recv().await.unwrap();
            assert_eq!(mapped_message.value, format!("value_{}", i));
        }

        // we need to drop the mapper, because if there are any in-flight requests
        // server fails to shut down. https://github.com/numaproj/numaflow-rs/issues/85
        drop(mapper);

        shutdown_tx
            .send(())
            .expect("failed to send shutdown signal");
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(
            handle.is_finished(),
            "Expected gRPC server to have shut down"
        );
        assert!(
            map_handle.is_finished(),
            "Expected mapper to have shut down"
        );
        Ok(())
    }
}
