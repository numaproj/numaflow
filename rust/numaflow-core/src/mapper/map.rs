use std::sync::Arc;
use std::time::Duration;

use numaflow_pb::clients::map::map_client::MapClient;
use tokio::sync::{mpsc, oneshot, OwnedSemaphorePermit, Semaphore};
use tokio::task::JoinHandle;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt;
use tonic::transport::Channel;

use crate::config::pipeline::map::MapMode;
use crate::error;
use crate::error::Error;
use crate::mapper::map::user_defined::{
    UserDefinedBatchMap, UserDefinedStreamMap, UserDefinedUnaryMap,
};
use crate::message::Message;
use crate::tracker::TrackerHandle;
pub(super) mod user_defined;

/// UnaryActorMessage is a message that is sent to the UnaryMapperActor.
struct UnaryActorMessage {
    message: Message,
    respond_to: oneshot::Sender<error::Result<Vec<Message>>>,
}

/// BatchActorMessage is a message that is sent to the BatchMapperActor.
struct BatchActorMessage {
    messages: Vec<Message>,
    respond_to: Vec<oneshot::Sender<error::Result<Vec<Message>>>>,
}

/// StreamActorMessage is a message that is sent to the StreamMapperActor.
struct StreamActorMessage {
    message: Message,
    respond_to: mpsc::Sender<error::Result<Message>>,
}

/// UnaryMapperActor is responsible for handling the unary map operation.
struct UnaryMapperActor {
    receiver: mpsc::Receiver<UnaryActorMessage>,
    mapper: UserDefinedUnaryMap,
}

impl UnaryMapperActor {
    fn new(receiver: mpsc::Receiver<UnaryActorMessage>, mapper: UserDefinedUnaryMap) -> Self {
        Self { receiver, mapper }
    }

    async fn handle_message(&mut self, msg: UnaryActorMessage) {
        self.mapper.unary_map(msg.message, msg.respond_to).await;
    }

    async fn run(mut self) {
        while let Some(msg) = self.receiver.recv().await {
            self.handle_message(msg).await;
        }
    }
}

/// BatchMapActor is responsible for handling the batch map operation.
struct BatchMapActor {
    receiver: mpsc::Receiver<BatchActorMessage>,
    mapper: UserDefinedBatchMap,
}

impl BatchMapActor {
    fn new(receiver: mpsc::Receiver<BatchActorMessage>, mapper: UserDefinedBatchMap) -> Self {
        Self { receiver, mapper }
    }

    async fn handle_message(&mut self, msg: BatchActorMessage) {
        self.mapper.batch_map(msg.messages, msg.respond_to).await;
    }

    async fn run(mut self) {
        while let Some(msg) = self.receiver.recv().await {
            self.handle_message(msg).await;
        }
    }
}

/// StreamMapActor is responsible for handling the stream map operation.
struct StreamMapActor {
    receiver: mpsc::Receiver<StreamActorMessage>,
    mapper: UserDefinedStreamMap,
}

impl StreamMapActor {
    fn new(receiver: mpsc::Receiver<StreamActorMessage>, mapper: UserDefinedStreamMap) -> Self {
        Self { receiver, mapper }
    }

    async fn handle_message(&mut self, msg: StreamActorMessage) {
        self.mapper.stream_map(msg.message, msg.respond_to).await;
    }

    async fn run(mut self) {
        while let Some(msg) = self.receiver.recv().await {
            self.handle_message(msg).await;
        }
    }
}

/// ActorSender is an enum to store the handles to different types of actors.
#[derive(Clone)]
enum ActorSender {
    Unary(mpsc::Sender<UnaryActorMessage>),
    Batch(mpsc::Sender<BatchActorMessage>),
    Stream(mpsc::Sender<StreamActorMessage>),
}

/// MapHandle is responsible for reading messages from the stream and invoke the map operation
/// on those messages and send the mapped messages to the output stream.
pub(crate) struct MapHandle {
    batch_size: usize,
    read_timeout: Duration,
    concurrency: usize,
    tracker: TrackerHandle,
    actor_sender: ActorSender,
    task_handle: JoinHandle<()>,
}

/// Abort all the background tasks when the mapper is dropped.
impl Drop for MapHandle {
    fn drop(&mut self) {
        self.task_handle.abort();
    }
}

/// Response channel size for streaming map.
const STREAMING_MAP_RESP_CHANNEL_SIZE: usize = 10;

impl MapHandle {
    /// Creates a new mapper with the given batch size, concurrency, client, and tracker handle.
    /// It spawns the appropriate actor based on the map mode.
    pub(crate) async fn new(
        map_mode: MapMode,
        batch_size: usize,
        read_timeout: Duration,
        concurrency: usize,
        client: MapClient<Channel>,
        tracker_handle: TrackerHandle,
    ) -> error::Result<Self> {
        let task_handle;

        // Based on the map mode, spawn the appropriate map actor
        // and store the sender handle in the actor_sender.
        let actor_sender = match map_mode {
            MapMode::Unary => {
                let (sender, receiver) = mpsc::channel(batch_size);
                let mapper_actor = UnaryMapperActor::new(
                    receiver,
                    UserDefinedUnaryMap::new(batch_size, client).await?,
                );

                let handle = tokio::spawn(async move {
                    mapper_actor.run().await;
                });
                task_handle = handle;
                ActorSender::Unary(sender)
            }
            MapMode::Batch => {
                let (batch_sender, batch_receiver) = mpsc::channel(batch_size);
                let batch_mapper_actor = BatchMapActor::new(
                    batch_receiver,
                    UserDefinedBatchMap::new(batch_size, client).await?,
                );

                let handle = tokio::spawn(async move {
                    batch_mapper_actor.run().await;
                });
                task_handle = handle;
                ActorSender::Batch(batch_sender)
            }
            MapMode::Stream => {
                let (stream_sender, stream_receiver) = mpsc::channel(batch_size);
                let stream_mapper_actor = StreamMapActor::new(
                    stream_receiver,
                    UserDefinedStreamMap::new(batch_size, client).await?,
                );

                let handle = tokio::spawn(async move {
                    stream_mapper_actor.run().await;
                });
                task_handle = handle;
                ActorSender::Stream(stream_sender)
            }
        };

        Ok(Self {
            actor_sender,
            batch_size,
            read_timeout,
            concurrency,
            tracker: tracker_handle,
            task_handle,
        })
    }

    /// Maps the input stream of messages and returns the output stream and the handle to the
    /// background task. In case of critical errors it stops reading from the input stream and
    /// returns the error using the join handle.
    pub(crate) async fn streaming_map(
        &self,
        input_stream: ReceiverStream<Message>,
    ) -> error::Result<(ReceiverStream<Message>, JoinHandle<error::Result<()>>)> {
        let (output_tx, output_rx) = mpsc::channel(self.batch_size);
        let (error_tx, mut error_rx) = mpsc::channel(1);

        let actor_handle = self.actor_sender.clone();
        let tracker = self.tracker.clone();
        let semaphore = Arc::new(Semaphore::new(self.concurrency));
        let batch_size = self.batch_size;
        let read_timeout = self.read_timeout;

        let handle = tokio::spawn(async move {
            let mut input_stream = input_stream;

            // based on the map mode, send the message to the appropriate actor handle.
            match actor_handle {
                ActorSender::Unary(map_handle) => loop {
                    // we need tokio select here because we have to listen to both the input stream
                    // and the error channel. If there is an error, we need to discard all the messages
                    // in the tracker and stop processing the input stream.
                    tokio::select! {
                        read_msg = input_stream.next() => {
                            if let Some(read_msg) = read_msg {
                                let permit = Arc::clone(&semaphore).acquire_owned().await.map_err(|e| Error::Mapper(format!("failed to acquire semaphore: {}", e)))?;
                                let error_tx = error_tx.clone();
                                Self::unary(
                                    map_handle.clone(),
                                    permit,
                                    read_msg,
                                    output_tx.clone(),
                                    tracker.clone(),
                                    error_tx,
                                ).await;
                            } else {
                                break;
                            }
                        },
                        Some(error) = error_rx.recv() => {
                            // if there is an error, discard all the messages in the tracker and return the error.
                            tracker.discard_all().await?;
                            return Err(error);
                        },
                    }
                },

                ActorSender::Batch(map_handle) => {
                    let timeout_duration = read_timeout;
                    let chunked_stream = input_stream.chunks_timeout(batch_size, timeout_duration);
                    tokio::pin!(chunked_stream);
                    // we don't need to tokio spawn here because, unlike unary and stream, batch is a blocking operation,
                    // and we process one batch at a time.
                    while let Some(batch) = chunked_stream.next().await {
                        if !batch.is_empty() {
                            if let Err(e) = Self::batch(
                                map_handle.clone(),
                                batch,
                                output_tx.clone(),
                                tracker.clone(),
                            )
                            .await
                            {
                                // if there is an error, discard all the messages in the tracker and return the error.
                                tracker.discard_all().await?;
                                return Err(e);
                            }
                        }
                    }
                }

                ActorSender::Stream(map_handle) => loop {
                    // we need tokio select here because we have to listen to both the input stream
                    // and the error channel. If there is an error, we need to discard all the messages
                    // in the tracker and stop processing the input stream.
                    tokio::select! {
                        read_msg = input_stream.next() => {
                            if let Some(read_msg) = read_msg {
                                let permit = Arc::clone(&semaphore).acquire_owned().await.map_err(|e| Error::Mapper(format!("failed to acquire semaphore: {}", e)))?;
                                let error_tx = error_tx.clone();
                                Self::stream(
                                    map_handle.clone(),
                                    permit,
                                    read_msg,
                                    output_tx.clone(),
                                    tracker.clone(),
                                    error_tx,
                                ).await;
                            } else {
                                break;
                            }
                        },
                        Some(error) = error_rx.recv() => {
                            // if there is an error, discard all the messages in the tracker and return the error.
                            tracker.discard_all().await?;
                            return Err(error);
                        },
                    }
                },
            }
            Ok(())
        });

        tracing::info!("Returning output_rx stream");
        Ok((ReceiverStream::new(output_rx), handle))
    }

    /// performs unary map operation on the given message and sends the mapped messages to the output
    /// stream. It updates the tracker with the number of messages sent. If there are any errors, it
    /// sends the error to the error channel.
    ///
    /// We use permit to limit the number of concurrent map unary operations, so that at any point in time
    /// we don't have more than `concurrency` number of map operations running.
    async fn unary(
        map_handle: mpsc::Sender<UnaryActorMessage>,
        permit: OwnedSemaphorePermit,
        read_msg: Message,
        output_tx: mpsc::Sender<Message>,
        tracker_handle: TrackerHandle,
        error_tx: mpsc::Sender<Error>,
    ) {
        let output_tx = output_tx.clone();

        // short-lived tokio spawns we don't need structured concurrency here
        tokio::spawn(async move {
            let _permit = permit;

            let (sender, receiver) = oneshot::channel();
            let msg = UnaryActorMessage {
                message: read_msg.clone(),
                respond_to: sender,
            };

            if let Err(e) = map_handle.send(msg).await {
                let _ = error_tx
                    .send(Error::Mapper(format!("failed to send message: {}", e)))
                    .await;
                return;
            }

            match receiver.await {
                Ok(Ok(mut mapped_messages)) => {
                    // update the tracker with the number of messages sent and send the mapped messages
                    if let Err(e) = tracker_handle.update_many(&mapped_messages, true).await {
                        error_tx.send(e).await.expect("failed to send error");
                        return;
                    }
                    for mapped_message in mapped_messages.drain(..) {
                        output_tx
                            .send(mapped_message)
                            .await
                            .expect("failed to send response");
                    }
                }
                Ok(Err(e)) => {
                    error_tx.send(e).await.expect("failed to send error");
                }
                Err(e) => {
                    error_tx
                        .send(Error::Mapper(format!("failed to receive message: {}", e)))
                        .await
                        .expect("failed to send error");
                }
            }
        });
    }

    /// performs batch map operation on the given batch of messages and sends the mapped messages to
    /// the output stream. It updates the tracker with the number of messages sent.
    async fn batch(
        map_handle: mpsc::Sender<BatchActorMessage>,
        batch: Vec<Message>,
        output_tx: mpsc::Sender<Message>,
        tracker_handle: TrackerHandle,
    ) -> error::Result<()> {
        let (senders, receivers): (Vec<_>, Vec<_>) =
            batch.iter().map(|_| oneshot::channel()).unzip();
        let msg = BatchActorMessage {
            messages: batch,
            respond_to: senders,
        };

        map_handle
            .send(msg)
            .await
            .map_err(|e| Error::Mapper(format!("failed to send message: {}", e)))?;

        for receiver in receivers {
            match receiver.await {
                Ok(Ok(mut mapped_messages)) => {
                    tracker_handle.update_many(&mapped_messages, true).await?;
                    for mapped_message in mapped_messages.drain(..) {
                        output_tx
                            .send(mapped_message)
                            .await
                            .expect("failed to send response");
                    }
                }
                Ok(Err(e)) => {
                    return Err(e);
                }
                Err(e) => {
                    return Err(Error::Mapper(format!("failed to receive message: {}", e)));
                }
            }
        }
        Ok(())
    }

    /// performs stream map operation on the given message and sends the mapped messages to the output
    /// stream. It updates the tracker with the number of messages sent. If there are any errors,
    /// it sends the error to the error channel.
    ///
    /// We use permit to limit the number of concurrent map unary operations, so that at any point in time
    /// we don't have more than `concurrency` number of map operations running.
    async fn stream(
        map_handle: mpsc::Sender<StreamActorMessage>,
        permit: OwnedSemaphorePermit,
        read_msg: Message,
        output_tx: mpsc::Sender<Message>,
        tracker_handle: TrackerHandle,
        error_tx: mpsc::Sender<Error>,
    ) {
        let output_tx = output_tx.clone();

        tokio::spawn(async move {
            let _permit = permit;

            let (sender, mut receiver) = mpsc::channel(STREAMING_MAP_RESP_CHANNEL_SIZE);
            let msg = StreamActorMessage {
                message: read_msg.clone(),
                respond_to: sender,
            };

            if let Err(e) = map_handle.send(msg).await {
                let _ = error_tx
                    .send(Error::Mapper(format!("failed to send message: {}", e)))
                    .await;
                return;
            }

            while let Some(result) = receiver.recv().await {
                match result {
                    Ok(mapped_message) => {
                        if let Err(e) = tracker_handle.update(&mapped_message).await {
                            error_tx.send(e).await.expect("failed to send error");
                            return;
                        }
                        if let Err(e) = output_tx.send(mapped_message).await {
                            error_tx
                                .send(Error::Mapper(format!("failed to send message: {}", e)))
                                .await
                                .expect("failed to send error");
                            return;
                        }
                    }
                    Err(e) => {
                        error_tx.send(e).await.expect("failed to send error");
                        return;
                    }
                }
            }

            if let Err(e) = tracker_handle.update_eof(read_msg.id.offset).await {
                error_tx.send(e).await.expect("failed to send error");
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use numaflow::mapstream;
    use numaflow::{batchmap, map};
    use numaflow_pb::clients::map::map_client::MapClient;
    use tempfile::TempDir;
    use tokio::sync::mpsc::Sender;
    use tokio::sync::oneshot;

    use super::*;
    use crate::message::{MessageID, Offset, StringOffset};
    use crate::shared::grpc::create_rpc_channel;
    use crate::Result;

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
        let tracker_handle = TrackerHandle::new(None);

        let client = MapClient::new(create_rpc_channel(sock_file).await?);
        let mapper = MapHandle::new(
            MapMode::Unary,
            500,
            Duration::from_millis(1000),
            10,
            client,
            tracker_handle.clone(),
        )
        .await?;

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
            metadata: None,
        };

        let (output_tx, mut output_rx) = mpsc::channel(10);

        let semaphore = Arc::new(Semaphore::new(10));
        let permit = semaphore.acquire_owned().await.unwrap();
        let (error_tx, mut error_rx) = mpsc::channel(1);

        let ActorSender::Unary(input_tx) = mapper.actor_sender.clone() else {
            panic!("Expected Unary actor sender");
        };

        MapHandle::unary(
            input_tx,
            permit,
            message,
            output_tx,
            tracker_handle,
            error_tx,
        )
        .await;

        // check for errors
        assert!(error_rx.recv().await.is_none());

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

        let tracker_handle = TrackerHandle::new(None);
        let client = MapClient::new(create_rpc_channel(sock_file).await?);
        let mapper = MapHandle::new(
            MapMode::Unary,
            10,
            Duration::from_millis(10),
            10,
            client,
            tracker_handle.clone(),
        )
        .await?;

        let (input_tx, input_rx) = mpsc::channel(10);
        let input_stream = ReceiverStream::new(input_rx);

        for i in 0..5 {
            let message = Message {
                keys: Arc::from(vec![format!("key_{}", i)]),
                tags: None,
                value: format!("value_{}", i).into(),
                offset: Some(Offset::String(StringOffset::new(i.to_string(), 0))),
                event_time: chrono::Utc::now(),
                id: MessageID {
                    vertex_name: "vertex_name".to_string().into(),
                    offset: i.to_string().into(),
                    index: i,
                },
                headers: Default::default(),
                metadata: None,
            };
            input_tx.send(message).await.unwrap();
        }
        drop(input_tx);

        let (output_stream, map_handle) = mapper.streaming_map(input_stream).await?;

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

    struct PanicCat;

    #[tonic::async_trait]
    impl map::Mapper for PanicCat {
        async fn map(&self, _input: map::MapRequest) -> Vec<map::Message> {
            panic!("PanicCat panicked!");
        }
    }

    #[tokio::test]
    async fn test_map_stream_with_panic() -> Result<()> {
        let tmp_dir = TempDir::new().unwrap();
        let sock_file = tmp_dir.path().join("map.sock");
        let server_info_file = tmp_dir.path().join("map-server-info");

        let server_info = server_info_file.clone();
        let server_socket = sock_file.clone();
        let handle = tokio::spawn(async move {
            map::Server::new(PanicCat)
                .with_socket_file(server_socket)
                .with_server_info_file(server_info)
                .start()
                .await
                .expect("server failed");
        });

        // wait for the server to start
        tokio::time::sleep(Duration::from_millis(100)).await;

        let tracker_handle = TrackerHandle::new(None);
        let client = MapClient::new(create_rpc_channel(sock_file).await?);
        let mapper = MapHandle::new(
            MapMode::Unary,
            500,
            Duration::from_millis(1000),
            10,
            client,
            tracker_handle.clone(),
        )
        .await?;

        let (input_tx, input_rx) = mpsc::channel(10);
        let input_stream = ReceiverStream::new(input_rx);

        let message = Message {
            keys: Arc::from(vec!["first".into()]),
            tags: None,
            value: "hello".into(),
            offset: Some(Offset::String(StringOffset::new("0".to_string(), 0))),
            event_time: chrono::Utc::now(),
            id: MessageID {
                vertex_name: "vertex_name".to_string().into(),
                offset: "0".to_string().into(),
                index: 0,
            },
            headers: Default::default(),
            metadata: None,
        };

        input_tx.send(message).await.unwrap();

        let (_output_stream, map_handle) = mapper.streaming_map(input_stream).await?;

        // Await the join handle and expect an error due to the panic
        let result = map_handle.await.unwrap();
        assert!(result.is_err(), "Expected an error due to panic");
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("PanicCat panicked!"));

        // we need to drop the mapper, because if there are any in-flight requests
        // server fails to shut down. https://github.com/numaproj/numaflow-rs/issues/85
        drop(mapper);

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
    async fn batch_mapper_operations() -> Result<()> {
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let tmp_dir = TempDir::new().unwrap();
        let sock_file = tmp_dir.path().join("batch_map.sock");
        let server_info_file = tmp_dir.path().join("batch_map-server-info");

        let server_info = server_info_file.clone();
        let server_socket = sock_file.clone();
        let handle = tokio::spawn(async move {
            batchmap::Server::new(SimpleBatchMap)
                .with_socket_file(server_socket)
                .with_server_info_file(server_info)
                .start_with_shutdown(shutdown_rx)
                .await
                .expect("server failed");
        });

        // wait for the server to start
        tokio::time::sleep(Duration::from_millis(100)).await;
        let tracker_handle = TrackerHandle::new(None);

        let client = MapClient::new(create_rpc_channel(sock_file).await?);
        let mapper = MapHandle::new(
            MapMode::Batch,
            500,
            Duration::from_millis(1000),
            10,
            client,
            tracker_handle.clone(),
        )
        .await?;

        let messages = vec![
            Message {
                keys: Arc::from(vec!["first".into()]),
                tags: None,
                value: "hello".into(),
                offset: Some(Offset::String(StringOffset::new("0".to_string(), 0))),
                event_time: chrono::Utc::now(),
                id: MessageID {
                    vertex_name: "vertex_name".to_string().into(),
                    offset: "0".to_string().into(),
                    index: 0,
                },
                headers: Default::default(),
                metadata: None,
            },
            Message {
                keys: Arc::from(vec!["second".into()]),
                tags: None,
                value: "world".into(),
                offset: Some(Offset::String(StringOffset::new("1".to_string(), 1))),
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

        let (input_tx, input_rx) = mpsc::channel(10);
        let input_stream = ReceiverStream::new(input_rx);

        for message in messages {
            input_tx.send(message).await.unwrap();
        }
        drop(input_tx);

        let (output_stream, map_handle) = mapper.streaming_map(input_stream).await?;
        let mut output_rx = output_stream.into_inner();

        let mapped_message1 = output_rx.recv().await.unwrap();
        assert_eq!(mapped_message1.value, "hello");

        let mapped_message2 = output_rx.recv().await.unwrap();
        assert_eq!(mapped_message2.value, "world");

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

    struct PanicBatchMap;

    #[tonic::async_trait]
    impl batchmap::BatchMapper for PanicBatchMap {
        async fn batchmap(
            &self,
            _input: mpsc::Receiver<batchmap::Datum>,
        ) -> Vec<batchmap::BatchResponse> {
            panic!("PanicBatchMap panicked!");
        }
    }

    #[tokio::test]
    async fn test_batch_map_with_panic() -> Result<()> {
        let (_shutdown_tx, shutdown_rx) = oneshot::channel();
        let tmp_dir = TempDir::new().unwrap();
        let sock_file = tmp_dir.path().join("batch_map_panic.sock");
        let server_info_file = tmp_dir.path().join("batch_map_panic-server-info");

        let server_info = server_info_file.clone();
        let server_socket = sock_file.clone();
        let handle = tokio::spawn(async move {
            batchmap::Server::new(PanicBatchMap)
                .with_socket_file(server_socket)
                .with_server_info_file(server_info)
                .start_with_shutdown(shutdown_rx)
                .await
                .expect("server failed");
        });

        // wait for the server to start
        tokio::time::sleep(Duration::from_millis(100)).await;

        let tracker_handle = TrackerHandle::new(None);
        let client = MapClient::new(create_rpc_channel(sock_file).await?);
        let mapper = MapHandle::new(
            MapMode::Batch,
            500,
            Duration::from_millis(1000),
            10,
            client,
            tracker_handle.clone(),
        )
        .await?;

        let messages = vec![
            Message {
                keys: Arc::from(vec!["first".into()]),
                tags: None,
                value: "hello".into(),
                offset: Some(Offset::String(StringOffset::new("0".to_string(), 0))),
                event_time: chrono::Utc::now(),
                id: MessageID {
                    vertex_name: "vertex_name".to_string().into(),
                    offset: "0".to_string().into(),
                    index: 0,
                },
                headers: Default::default(),
                metadata: None,
            },
            Message {
                keys: Arc::from(vec!["second".into()]),
                tags: None,
                value: "world".into(),
                offset: Some(Offset::String(StringOffset::new("1".to_string(), 1))),
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

        let (input_tx, input_rx) = mpsc::channel(10);
        let input_stream = ReceiverStream::new(input_rx);

        for message in messages {
            input_tx.send(message).await.unwrap();
        }
        drop(input_tx);

        let (_output_stream, map_handle) = mapper.streaming_map(input_stream).await?;

        // Await the join handle and expect an error due to the panic
        let result = map_handle.await.unwrap();
        assert!(result.is_err(), "Expected an error due to panic");

        // we need to drop the mapper, because if there are any in-flight requests
        // server fails to shut down. https://github.com/numaproj/numaflow-rs/issues/85
        drop(mapper);

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
            tx: Sender<mapstream::Message>,
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
    async fn map_stream_operations() -> Result<()> {
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let tmp_dir = TempDir::new().unwrap();
        let sock_file = tmp_dir.path().join("map_stream.sock");
        let server_info_file = tmp_dir.path().join("map_stream-server-info");

        let server_info = server_info_file.clone();
        let server_socket = sock_file.clone();
        let _handle = tokio::spawn(async move {
            mapstream::Server::new(FlatmapStream)
                .with_socket_file(server_socket)
                .with_server_info_file(server_info)
                .start_with_shutdown(shutdown_rx)
                .await
                .expect("server failed");
        });

        // wait for the server to start
        tokio::time::sleep(Duration::from_millis(100)).await;
        let tracker_handle = TrackerHandle::new(None);

        let client = MapClient::new(create_rpc_channel(sock_file).await?);
        let mapper = MapHandle::new(
            MapMode::Stream,
            500,
            Duration::from_millis(1000),
            10,
            client,
            tracker_handle.clone(),
        )
        .await?;

        let message = Message {
            keys: Arc::from(vec!["first".into()]),
            tags: None,
            value: "test,map,stream".into(),
            offset: Some(Offset::String(StringOffset::new("0".to_string(), 0))),
            event_time: chrono::Utc::now(),
            id: MessageID {
                vertex_name: "vertex_name".to_string().into(),
                offset: "0".to_string().into(),
                index: 0,
            },
            headers: Default::default(),
            metadata: None,
        };

        let (input_tx, input_rx) = mpsc::channel(10);
        let input_stream = ReceiverStream::new(input_rx);

        input_tx.send(message).await.unwrap();
        drop(input_tx);

        let (mut output_stream, map_handle) = mapper.streaming_map(input_stream).await?;

        let mut responses = vec![];
        while let Some(response) = output_stream.next().await {
            responses.push(response);
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
        drop(mapper);

        shutdown_tx
            .send(())
            .expect("failed to send shutdown signal");
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(
            map_handle.is_finished(),
            "Expected mapper to have shut down"
        );
        Ok(())
    }

    struct PanicFlatmapStream;

    #[tonic::async_trait]
    impl mapstream::MapStreamer for PanicFlatmapStream {
        async fn map_stream(
            &self,
            _input: mapstream::MapStreamRequest,
            _tx: Sender<mapstream::Message>,
        ) {
            panic!("PanicFlatmapStream panicked!");
        }
    }

    #[tokio::test]
    async fn map_stream_panic_case() -> Result<()> {
        let (_shutdown_tx, shutdown_rx) = oneshot::channel();
        let tmp_dir = TempDir::new().unwrap();
        let sock_file = tmp_dir.path().join("map_stream_panic.sock");
        let server_info_file = tmp_dir.path().join("map_stream_panic-server-info");

        let server_info = server_info_file.clone();
        let server_socket = sock_file.clone();
        let handle = tokio::spawn(async move {
            mapstream::Server::new(PanicFlatmapStream)
                .with_socket_file(server_socket)
                .with_server_info_file(server_info)
                .start_with_shutdown(shutdown_rx)
                .await
                .expect("server failed");
        });

        // wait for the server to start
        tokio::time::sleep(Duration::from_millis(100)).await;

        let client = MapClient::new(create_rpc_channel(sock_file).await?);
        let tracker_handle = TrackerHandle::new(None);
        let mapper = MapHandle::new(
            MapMode::Stream,
            500,
            Duration::from_millis(1000),
            10,
            client,
            tracker_handle,
        )
        .await?;

        let message = Message {
            keys: Arc::from(vec!["first".into()]),
            tags: None,
            value: "panic".into(),
            offset: Some(Offset::String(StringOffset::new("0".to_string(), 0))),
            event_time: chrono::Utc::now(),
            id: MessageID {
                vertex_name: "vertex_name".to_string().into(),
                offset: "0".to_string().into(),
                index: 0,
            },
            headers: Default::default(),
            metadata: None,
        };

        let (input_tx, input_rx) = mpsc::channel(10);
        let input_stream = ReceiverStream::new(input_rx);

        input_tx.send(message).await.unwrap();

        let (_output_stream, map_handle) = mapper.streaming_map(input_stream).await?;

        // Await the join handle and expect an error due to the panic
        let result = map_handle.await.unwrap();
        assert!(result.is_err(), "Expected an error due to panic");
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("PanicFlatmapStream panicked!"));

        // we need to drop the client, because if there are any in-flight requests
        // server fails to shut down. https://github.com/numaproj/numaflow-rs/issues/85
        drop(mapper);

        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(
            handle.is_finished(),
            "Expected gRPC server to have shut down"
        );
        Ok(())
    }
}
