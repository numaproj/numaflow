use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
use numaflow_pb::clients::map::{self, MapRequest, MapResponse, map_client::MapClient};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio_stream::StreamExt;
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;
use tonic::transport::Channel;
use tonic::{Request, Streaming};
use tracing::{error, info, warn};

use crate::config::get_vertex_name;
use crate::config::pipeline::map::MapMode;
use crate::error::{self, Error};
use crate::message::{AckHandle, Message, MessageID, Offset};
use crate::metadata::Metadata;
use crate::shared::grpc::prost_timestamp_from_utc;
use crate::tracker::Tracker;

pub(super) mod batch;
pub(super) mod stream;
pub(super) mod unary;

use batch::UserDefinedBatchMap;
use stream::UserDefinedStreamMap;
use unary::UserDefinedUnaryMap;

// Type aliases
type ResponseSenderMap = Arc<
    Mutex<
        HashMap<
            String,
            (
                ParentMessageInfo,
                oneshot::Sender<error::Result<Vec<Message>>>,
            ),
        >,
    >,
>;

type StreamResponseSenderMap =
    Arc<Mutex<HashMap<String, (ParentMessageInfo, mpsc::Sender<error::Result<Message>>)>>>;

// Shared struct for parent message information
pub(crate) struct ParentMessageInfo {
    pub(crate) offset: Offset,
    pub(crate) event_time: DateTime<Utc>,
    pub(crate) is_late: bool,
    pub(crate) headers: Arc<HashMap<String, String>>,
    pub(crate) start_time: Instant,
    /// this remains 0 for all except map-streaming because in map-streaming there could be more than
    /// one response for a single request.
    pub(crate) current_index: i32,
    pub(crate) metadata: Option<Arc<Metadata>>,
    pub(crate) ack_handle: Option<Arc<AckHandle>>,
}

// Conversion from Message to MapRequest
impl From<Message> for MapRequest {
    fn from(message: Message) -> Self {
        Self {
            request: Some(map::map_request::Request {
                keys: message.keys.to_vec(),
                value: message.value.to_vec(),
                event_time: Some(prost_timestamp_from_utc(message.event_time)),
                watermark: message.watermark.map(prost_timestamp_from_utc),
                headers: Arc::unwrap_or_clone(message.headers),
                metadata: message.metadata.map(|m| Arc::unwrap_or_clone(m).into()),
            }),
            id: message.offset.to_string(),
            handshake: None,
            status: None,
        }
    }
}

// Helper struct for converting UDF responses to Messages
struct UserDefinedMessage<'a>(map::map_response::Result, &'a ParentMessageInfo, i32);

impl From<UserDefinedMessage<'_>> for Message {
    fn from(value: UserDefinedMessage<'_>) -> Self {
        Message {
            typ: Default::default(),
            id: MessageID {
                vertex_name: get_vertex_name().to_string().into(),
                index: value.2,
                offset: value.1.offset.to_string().into(),
            },
            keys: Arc::from(value.0.keys),
            tags: Some(Arc::from(value.0.tags)),
            value: value.0.value.into(),
            offset: value.1.offset.clone(),
            event_time: value.1.event_time,
            headers: Arc::clone(&value.1.headers),
            watermark: None,
            is_late: value.1.is_late,
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
            ack_handle: value.1.ack_handle.clone(),
        }
    }
}

/// Performs handshake with the server and returns the response stream to receive responses.
async fn create_response_stream(
    read_tx: mpsc::Sender<MapRequest>,
    read_rx: mpsc::Receiver<MapRequest>,
    client: &mut MapClient<Channel>,
) -> error::Result<Streaming<MapResponse>> {
    let handshake_request = MapRequest {
        request: None,
        id: "".to_string(),
        handshake: Some(map::Handshake { sot: true }),
        status: None,
    };

    read_tx
        .send(handshake_request)
        .await
        .map_err(|e| Error::Mapper(format!("failed to send handshake request: {e}")))?;

    let mut resp_stream = client
        .map_fn(Request::new(ReceiverStream::new(read_rx)))
        .await
        .map_err(|e| Error::Grpc(Box::new(e)))?
        .into_inner();

    let handshake_response = resp_stream
        .message()
        .await
        .map_err(|e| Error::Grpc(Box::new(e)))?
        .ok_or(Error::Mapper(
            "failed to receive handshake response".to_string(),
        ))?;

    if handshake_response.handshake.is_none_or(|h| !h.sot) {
        return Err(Error::Mapper("invalid handshake response".to_string()));
    }

    Ok(resp_stream)
}

/// MapperType is an enum to store the different types of mappers.
#[derive(Clone)]
enum MapperType {
    Unary(UserDefinedUnaryMap),
    Batch(UserDefinedBatchMap),
    Stream(UserDefinedStreamMap),
}

/// MapHandle is responsible for reading messages from the stream and invoke the map operation on
/// those messages and send the mapped messages to the output stream.
///
/// Error handling: There can be critical non-retryable errors in this component like udf failures
/// etc., since we do concurrent processing of messages, the moment we encounter an error from any
/// of the tasks, we will go to shut-down mode. We cancel the token to let upstream know that we are
/// shutting down. We drain the input stream, nack the messages, and exit when the stream is
/// closed. We will drop the downstream stream so that the downstream components can shut down.
/// Structured concurrency is honoured here, we wait for all the concurrent tokio tasks to exit.
/// before shutting down the component.
#[derive(Clone)]
pub(crate) struct MapHandle {
    batch_size: usize,
    read_timeout: Duration,
    graceful_shutdown_time: Duration,
    concurrency: usize,
    tracker: Tracker,
    mapper: MapperType,
    /// this the final state of the component (any error will set this as Err)
    final_result: crate::Result<()>,
    /// The moment we see an error, we will set this to true.
    shutting_down_on_err: bool,
    health_checker: Option<MapClient<Channel>>,
}

/// Response channel size for streaming map.
const STREAMING_MAP_RESP_CHANNEL_SIZE: usize = 10;

impl MapHandle {
    /// Creates a new mapper with the given batch size, concurrency, client, and
    /// tracker handle. It creates the appropriate mapper based on the map mode.
    pub(crate) async fn new(
        map_mode: MapMode,
        batch_size: usize,
        read_timeout: Duration,
        graceful_timeout: Duration,
        concurrency: usize,
        client: MapClient<Channel>,
        tracker: Tracker,
    ) -> error::Result<Self> {
        // Based on the map mode, create the appropriate mapper
        let mapper = match map_mode {
            MapMode::Unary => {
                MapperType::Unary(UserDefinedUnaryMap::new(batch_size, client.clone()).await?)
            }
            MapMode::Batch => {
                MapperType::Batch(UserDefinedBatchMap::new(batch_size, client.clone()).await?)
            }
            MapMode::Stream => {
                MapperType::Stream(UserDefinedStreamMap::new(batch_size, client.clone()).await?)
            }
        };

        Ok(Self {
            mapper,
            batch_size,
            read_timeout,
            graceful_shutdown_time: graceful_timeout,
            concurrency,
            tracker,
            final_result: Ok(()),
            shutting_down_on_err: false,
            health_checker: Some(client),
        })
    }

    // Returns true if the mapper is ready to accept messages.
    pub(crate) async fn ready(&mut self) -> bool {
        if let Some(client) = &mut self.health_checker {
            match client.is_ready(tonic::Request::new(())).await {
                Ok(response) => response.into_inner().ready,
                Err(e) => {
                    error!(?e, "Map Client is not ready");
                    false
                }
            }
        } else {
            false
        }
    }

    /// Maps the input stream of messages and returns the output stream and the
    /// handle to the background task. In case of critical errors it stops
    /// reading from the input stream and returns the error using the join
    /// handle.
    pub(crate) async fn streaming_map(
        mut self,
        input_stream: ReceiverStream<Message>,
        cln_token: CancellationToken,
    ) -> error::Result<(ReceiverStream<Message>, JoinHandle<error::Result<()>>)> {
        let (output_tx, output_rx) = mpsc::channel(self.batch_size);
        let (error_tx, mut error_rx) = mpsc::channel(self.batch_size);
        let semaphore = Arc::new(Semaphore::new(self.concurrency));

        // we spawn one of the 3 map types
        let handle = tokio::spawn(async move {
            let parent_cln_token = cln_token.clone();

            // create a new cancellation token for the map component, this token is used for hard
            // shutdown, the parent token is used for graceful shutdown.
            let hard_shutdown_token = CancellationToken::new();
            // the one that calls shutdown
            let hard_shutdown_token_owner = hard_shutdown_token.clone();
            let graceful_timeout = self.graceful_shutdown_time;

            // spawn a task to cancel the token after graceful timeout when the main token is cancelled
            let shutdown_handle = tokio::spawn(async move {
                // initiate graceful shutdown
                parent_cln_token.cancelled().await;
                // wait for graceful timeout
                tokio::time::sleep(graceful_timeout).await;
                // cancel the token to hard shutdown
                hard_shutdown_token_owner.cancel();
            });

            let mut input_stream = input_stream;
            // we capture the first error that triggered the map component shutdown
            // based on the map mode, send the message to the appropriate mapper.
            match &self.mapper {
                MapperType::Unary(mapper) => loop {
                    // we need tokio select here because we have to listen to both the input stream
                    // and the error channel. If there is an error, we need to discard all the
                    // messages in the tracker and stop processing the input
                    // stream.
                    tokio::select! {
                        biased;
                        Some(error) = error_rx.recv() => {
                            if self.final_result.is_ok() {
                                error!(?error, "error received while performing unary map operation");
                                cln_token.cancel();
                                self.final_result = Err(error);
                                self.shutting_down_on_err = true;
                            } else {
                                // store the error so that latest error will be propagated
                                // to the UI.
                                self.final_result = Err(error);
                            }
                        },
                        read_msg = input_stream.next() => {
                            let Some(read_msg) = read_msg else {
                                break;
                            };

                            // if there are errors then we need to drain the stream and nack
                            if self.shutting_down_on_err {
                                warn!(offset = ?read_msg.offset, error = ?self.final_result, "Map component is shutting down because of an error, not accepting the message");
                                read_msg.ack_handle.as_ref().expect("ack handle should be present").is_failed.store(true, Ordering::Relaxed);
                            } else {
                                let permit = Arc::clone(&semaphore).acquire_owned()
                                    .await.map_err(|e| Error::Mapper(format!("failed to acquire semaphore: {e}" )))?;
                                Self::unary(
                                    mapper.clone(),
                                    permit,
                                    read_msg,
                                    output_tx.clone(),
                                    self.tracker.clone(),
                                    error_tx.clone(),
                                    hard_shutdown_token.clone(),
                                ).await;
                            }
                        },
                    }
                },

                MapperType::Batch(mapper) => {
                    let timeout_duration = self.read_timeout;
                    let chunked_stream =
                        input_stream.chunks_timeout(self.batch_size, timeout_duration);
                    tokio::pin!(chunked_stream);
                    // we don't need to tokio spawn here because, unlike unary and stream, batch is
                    // a blocking operation, and we process one batch at a time.
                    while let Some(batch) = chunked_stream.next().await {
                        // if there are errors then we need to drain the stream and nack
                        if self.shutting_down_on_err {
                            for msg in batch {
                                warn!(offset = ?msg.offset, error = ?self.final_result, "Map component is shutting down because of an error, not accepting the message");
                                msg.ack_handle
                                    .as_ref()
                                    .expect("ack handle should be present")
                                    .is_failed
                                    .store(true, Ordering::Relaxed);
                            }
                            continue;
                        }

                        let ack_handles: Vec<Option<Arc<AckHandle>>> =
                            batch.iter().map(|msg| msg.ack_handle.clone()).collect();

                        if !batch.is_empty()
                            && let Err(e) = Self::batch(
                                mapper.clone(),
                                batch,
                                output_tx.clone(),
                                self.tracker.clone(),
                            )
                            .await
                        {
                            error!(?e, "error received while performing batch map operation");
                            // if there is an error, discard all the messages in the tracker and
                            // return the error.
                            for ack_handle in ack_handles {
                                ack_handle
                                    .as_ref()
                                    .expect("ack handle should be present")
                                    .is_failed
                                    .store(true, Ordering::Relaxed);
                            }
                            cln_token.cancel();
                            self.shutting_down_on_err = true;
                            self.final_result = Err(e);
                        }
                    }
                }

                MapperType::Stream(mapper) => loop {
                    // we need tokio select here because we have to listen to both the input stream
                    // and the error channel. If there is an error, we need to discard all the
                    // messages in the tracker and stop processing the input
                    // stream.
                    tokio::select! {
                        biased;
                       Some(error) = error_rx.recv() => {
                            // when we get an error we cancel the token to signal the upstream to stop
                            // sending new messages, and we empty the input stream and return the error.
                            if self.final_result.is_ok() {
                                error!(?error, "error received while performing stream map operation");
                                cln_token.cancel();
                                // stop further reading since we have seen an error
                                self.final_result = Err(error);
                                self.shutting_down_on_err = true;
                            }
                        },
                        read_msg = input_stream.next() => {
                            let Some(read_msg) = read_msg else {
                                break;
                            };

                            if self.shutting_down_on_err {
                                warn!(offset = ?read_msg.offset, error = ?self.final_result, "Map component is shutting down because of an error, not accepting the message");
                                read_msg.ack_handle.as_ref().expect("ack handle should be present").is_failed.store(true, Ordering::Relaxed);
                            } else {
                                let permit = Arc::clone(&semaphore).acquire_owned().await.map_err(|e| Error::Mapper(format!("failed to acquire semaphore: {e}")))?;
                                let error_tx = error_tx.clone();
                                Self::stream(
                                    mapper.clone(),
                                    permit,
                                    read_msg,
                                    output_tx.clone(),
                                    self.tracker.clone(),
                                    error_tx,
                                    cln_token.clone(),
                                ).await;
                            }
                        },
                    }
                },
            }

            // wait for all the spawned tasks to finish before returning the final result
            info!("Map input stream ended, waiting for inflight messages to finish");
            let _permit = Arc::clone(&semaphore)
                .acquire_many_owned(self.concurrency as u32)
                .await
                .map_err(|e| Error::Mapper(format!("failed to acquire semaphore: {e}")))?;
            info!(status=?self.final_result, "Map component is completed with status");

            // abort the shutdown handle since we are done processing, no need to wait for the
            // hard shutdown
            if !shutdown_handle.is_finished() {
                shutdown_handle.abort();
            }

            self.final_result
        });

        Ok((ReceiverStream::new(output_rx), handle))
    }

    /// performs unary map operation on the given message and sends the mapped
    /// messages to the output stream. It updates the tracker with the
    /// number of messages sent. If there are any errors, it sends the error
    /// to the error channel.
    ///
    /// We use permit to limit the number of concurrent map unary operations, so
    /// that at any point in time we don't have more than `concurrency`
    /// number of map operations running.
    async fn unary(
        mapper: UserDefinedUnaryMap,
        permit: OwnedSemaphorePermit,
        read_msg: Message,
        output_tx: mpsc::Sender<Message>,
        tracker: Tracker,
        error_tx: mpsc::Sender<Error>,
        cln_token: CancellationToken,
    ) {
        let output_tx = output_tx.clone();

        // short-lived tokio spawns we don't need structured concurrency here
        tokio::spawn(async move {
            let _permit = permit;

            let offset = read_msg.offset.clone();
            let (sender, receiver) = oneshot::channel();

            // Call the mapper directly instead of sending to an actor
            mapper.unary_map(read_msg.clone(), sender).await;

            tokio::select! {
                result = receiver => {
                    match result {
                        Ok(Ok(mapped_messages)) => {
                            // update the tracker with the number of messages sent and send the mapped messages
                            tracker
                                .serving_update(
                                    &offset,
                                    mapped_messages.iter().map(|m| m.tags.clone()).collect(),
                                )
                                .await
                                .expect("failed to update tracker");

                            // send messages downstream
                            for mapped_message in mapped_messages {
                                output_tx
                                    .send(mapped_message)
                                    .await
                                    .expect("failed to send response");
                            }
                        }
                        Ok(Err(map_err)) => {
                            error!(err=?map_err, ?offset, "failed to map message");
                            read_msg
                                .ack_handle
                                .as_ref()
                                .expect("ack handle should be present")
                                .is_failed
                                .store(true, Ordering::Relaxed);
                            info!("writing error to error channel");
                            let _ = error_tx.send(map_err).await;
                        }
                        Err(err) => {
                            error!(?err, ?offset, "failed to receive message");
                            read_msg
                                .ack_handle
                                .as_ref()
                                .expect("ack handle should be present")
                                .is_failed
                                .store(true, Ordering::Relaxed);
                            let _ = error_tx
                                .send(Error::Mapper(format!("failed to receive message: {err}")))
                                .await;
                        }
                    }
                },
                _ = cln_token.cancelled() => {
                    error!(?offset, "Cancellation token received, discarding message");
                    read_msg
                        .ack_handle
                        .as_ref()
                        .expect("ack handle should be present")
                        .is_failed
                        .store(true, Ordering::Relaxed);
                }
            }
        });
    }

    /// performs batch map operation on the given batch of messages and sends
    /// the mapped messages to the output stream. It updates the tracker
    /// with the number of messages sent.
    async fn batch(
        mapper: UserDefinedBatchMap,
        batch: Vec<Message>,
        output_tx: mpsc::Sender<Message>,
        tracker: Tracker,
    ) -> error::Result<()> {
        let (senders, receivers): (Vec<_>, Vec<_>) =
            batch.iter().map(|_| oneshot::channel()).unzip();

        // Call the mapper directly instead of sending to an actor
        mapper.batch_map(batch, senders).await;

        for receiver in receivers {
            match receiver.await {
                Ok(Ok(mapped_messages)) => {
                    let mut offset: Option<Offset> = None;
                    for message in mapped_messages.iter() {
                        if offset.is_none() {
                            offset = Some(message.offset.clone());
                        }
                    }

                    if let Some(offset) = offset {
                        tracker
                            .serving_update(
                                &offset,
                                mapped_messages.iter().map(|m| m.tags.clone()).collect(),
                            )
                            .await?;
                    }
                    for mapped_message in mapped_messages {
                        output_tx
                            .send(mapped_message)
                            .await
                            .expect("failed to send response");
                    }
                }
                Ok(Err(_map_err)) => {
                    error!(err=?_map_err, "failed to map message");
                    return Err(_map_err);
                }
                Err(e) => {
                    error!(?e, "failed to receive message");
                    return Err(Error::Mapper(format!("failed to receive message: {e}")));
                }
            }
        }
        Ok(())
    }

    /// performs stream map operation on the given message and sends the mapped
    /// messages to the output stream. It updates the tracker with the
    /// number of messages sent. If there are any errors, it sends the error
    /// to the error channel.
    ///
    /// We use permit to limit the number of concurrent map unary operations, so
    /// that at any point in time we don't have more than `concurrency`
    /// number of map operations running.
    async fn stream(
        mapper: UserDefinedStreamMap,
        permit: OwnedSemaphorePermit,
        read_msg: Message,
        output_tx: mpsc::Sender<Message>,
        tracker: Tracker,
        error_tx: mpsc::Sender<Error>,
        cln_token: CancellationToken,
    ) {
        let output_tx = output_tx.clone();
        tokio::spawn(async move {
            let _permit = permit;

            let (sender, mut receiver) = mpsc::channel(STREAMING_MAP_RESP_CHANNEL_SIZE);

            // Call the mapper directly instead of sending to an actor
            mapper.stream_map(read_msg.clone(), sender).await;

            // we need update the tracker with no responses, because unlike unary and batch, we cannot update the
            // responses here we will have to append the responses.
            tracker
                .serving_refresh(read_msg.offset.clone())
                .await
                .expect("failed to reset tracker");
            loop {
                tokio::select! {
                    result = receiver.recv() => {
                        match result {
                            Some(Ok(mapped_message)) => {
                                tracker
                                    .serving_append(mapped_message.offset.clone(), mapped_message.tags.clone())
                                    .await
                                    .expect("failed to update tracker");
                                output_tx.send(mapped_message).await.expect("failed to send response");
                            }
                            Some(Err(e)) => {
                                error!(?e, "failed to map message");
                                read_msg
                                    .ack_handle
                                    .as_ref()
                                    .expect("ack handle should be present")
                                    .is_failed
                                    .store(true, Ordering::Relaxed);
                                let _ = error_tx.send(e).await;
                                return;
                            }
                            None => break,
                        }
                    },
                    _ = cln_token.cancelled() => {
                        error!(?read_msg.offset, "Cancellation token received, will not wait for the response");
                        read_msg
                            .ack_handle
                            .as_ref()
                            .expect("ack handle should be present")
                            .is_failed
                            .store(true, Ordering::Relaxed);
                        let _ = error_tx
                            .send(Error::Mapper("Operation cancelled".to_string()))
                            .await;
                        return;
                    }
                }
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::{
        Result,
        message::{MessageID, Offset, StringOffset},
        shared::grpc::create_rpc_channel,
    };
    use numaflow::map;
    use numaflow::shared::ServerExtras;
    use numaflow_pb::clients::map::map_client::MapClient;
    use tempfile::TempDir;
    use tokio::sync::oneshot;

    struct SimpleMapper;

    #[tonic::async_trait]
    impl map::Mapper for SimpleMapper {
        async fn map(&self, input: map::MapRequest) -> Vec<map::Message> {
            let message = map::Message::new(input.value)
                .with_keys(input.keys)
                .with_tags(vec!["test".to_string()]);
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
        let tracker = Tracker::new(None, CancellationToken::new());

        let client = MapClient::new(create_rpc_channel(sock_file).await?);
        let mapper = MapHandle::new(
            MapMode::Unary,
            500,
            Duration::from_millis(1000),
            Duration::from_secs(10),
            10,
            client,
            tracker.clone(),
        )
        .await?;

        let message = Message {
            typ: Default::default(),
            keys: Arc::from(vec!["first".into()]),
            tags: None,
            value: "hello".into(),
            offset: Offset::String(StringOffset::new("0".to_string(), 0)),
            event_time: chrono::Utc::now(),
            watermark: None,
            id: MessageID {
                vertex_name: "vertex_name".to_string().into(),
                offset: "0".to_string().into(),
                index: 0,
            },
            ..Default::default()
        };

        let (output_tx, mut output_rx) = mpsc::channel(10);

        let semaphore = Arc::new(Semaphore::new(10));
        let permit = semaphore.acquire_owned().await.unwrap();
        let (error_tx, mut error_rx) = mpsc::channel(1);

        // Extract the mapper from the MapperType enum
        let unary_mapper = match &mapper.mapper {
            MapperType::Unary(m) => m.clone(),
            _ => panic!("Expected Unary mapper"),
        };

        MapHandle::unary(
            unary_mapper,
            permit,
            message,
            output_tx,
            tracker,
            error_tx,
            CancellationToken::new(),
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
}
