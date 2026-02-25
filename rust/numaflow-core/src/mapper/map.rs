use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
use numaflow_pb::clients::map::{self, MapRequest, MapResponse, map_client::MapClient};
use tokio::sync::{Semaphore, mpsc};
use tokio::task::JoinHandle;
use tokio_stream::StreamExt;
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;
use tonic::transport::Channel;
use tonic::{Request, Streaming};
use tracing::{error, info, warn};

use crate::config::pipeline::map::MapMode;
use crate::config::{get_vertex_name, is_mono_vertex};
use crate::error::{self, Error};
use crate::message::{AckHandle, Message, MessageID, Offset};
use crate::metadata::Metadata;
use crate::shared::grpc::prost_timestamp_from_utc;
use crate::tracker::Tracker;

pub(super) mod batch;
pub(super) mod stream;
pub(super) mod unary;

use crate::config::pipeline::VERTEX_TYPE_MAP_UDF;
use crate::metrics::{
    monovertex_metrics, mvtx_forward_metric_labels, pipeline_metric_labels, pipeline_metrics,
};
use crate::monovertex::bypass_router::MvtxBypassRouter;
use batch::{MapBatchTask, UserDefinedBatchMap};
use stream::{MapStreamTask, UserDefinedStreamMap};
use unary::{MapUnaryTask, UserDefinedUnaryMap};

/// ConcurrentMapper represents mappers that process messages concurrently (one task per message).
/// Both Unary and Stream mappers spawn individual tasks for each message.
#[derive(Clone)]
enum ConcurrentMapper {
    Unary(UserDefinedUnaryMap),
    Stream(UserDefinedStreamMap),
}

/// MapperType is an enum to store the different types of mappers.
#[derive(Clone)]
enum MapperType {
    /// Concurrent mappers (Unary/Stream) process messages individually with concurrency control.
    Concurrent(ConcurrentMapper),
    /// Batch mapper processes messages in batches sequentially.
    Batch(UserDefinedBatchMap),
}

/// MapHandle is responsible for reading messages from the stream and invoke the map operation on
/// those messages and send the mapped messages to the output stream.
///
/// Error handling: There can be critical non-retryable errors in this component like udf failures
/// etc., since we do concurrent processing of messages, the moment we encounter an error from any
/// of the tasks, we will go to shut-down mode. We cancel the token to let upstream know that we are
/// shutting down. We drain the input stream, nack the messages, and exit when the stream is
/// closed. We will drop the downstream stream so that the downstream components can shut down.
/// Structured concurrency is honored here, we wait for all the concurrent tokio tasks to exit.
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
            MapMode::Unary => MapperType::Concurrent(ConcurrentMapper::Unary(
                UserDefinedUnaryMap::new(batch_size, client.clone()).await?,
            )),
            MapMode::Stream => MapperType::Concurrent(ConcurrentMapper::Stream(
                UserDefinedStreamMap::new(batch_size, client.clone()).await?,
            )),
            MapMode::Batch => {
                MapperType::Batch(UserDefinedBatchMap::new(batch_size, client.clone()).await?)
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
            match client.is_ready(Request::new(())).await {
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
        bypass_router: Option<MvtxBypassRouter>,
    ) -> error::Result<(ReceiverStream<Message>, JoinHandle<error::Result<()>>)> {
        let (output_tx, output_rx) = mpsc::channel(self.batch_size);
        let (error_tx, error_rx) = mpsc::channel(self.batch_size);
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

            // spawn a task to cancel the token after graceful timeout when the main token is canceled
            let shutdown_handle = tokio::spawn(async move {
                // initiate graceful shutdown
                parent_cln_token.cancelled().await;
                // wait for graceful timeout
                tokio::time::sleep(graceful_timeout).await;
                // cancel the token to hard shutdown
                hard_shutdown_token_owner.cancel();
            });

            // based on the map mode, send the message to the appropriate mapper.
            match &self.mapper {
                MapperType::Concurrent(concurrent_mapper) => {
                    let shared_ctx = Arc::new(SharedMapTaskContext {
                        output_tx,
                        error_tx,
                        tracker: self.tracker.clone(),
                        bypass_router,
                        hard_shutdown_token,
                        is_mono_vertex: is_mono_vertex(),
                    });
                    let ctx = ConcurrentMapContext {
                        error_rx,
                        semaphore: Arc::clone(&semaphore),
                        cln_token,
                        concurrent_mapper: concurrent_mapper.clone(),
                        shared_ctx,
                    };
                    self.process_concurrent_messages(input_stream, ctx).await?;
                }
                MapperType::Batch(batch_mapper) => {
                    let ctx = BatchMapContext {
                        output_tx,
                        cln_token,
                        bypass_router,
                        batch_mapper: batch_mapper.clone(),
                    };
                    self.process_batch_messages(input_stream, ctx).await;
                }
            }

            // wait for all the spawned tasks to finish before returning the final result
            info!("Map input stream ended, waiting for inflight messages to finish");
            let _permit = semaphore
                .acquire_many_owned(self.concurrency as u32)
                .await
                .map_err(|e| Error::Mapper(format!("failed to acquire semaphore: {e}")))?;
            info!(status=?self.final_result, "Map component is completed");

            // abort the shutdown handle since we are done processing, no need to wait for the
            // hard shutdown
            if !shutdown_handle.is_finished() {
                shutdown_handle.abort();
            }

            self.final_result
        });

        Ok((ReceiverStream::new(output_rx), handle))
    }

    /// Processes messages using concurrent (unary or stream) mappers.
    /// Each message is processed in a separate spawned task.
    async fn process_concurrent_messages(
        &mut self,
        input_stream: ReceiverStream<Message>,
        mut ctx: ConcurrentMapContext,
    ) -> error::Result<()> {
        let mut input_stream = input_stream;
        loop {
            // we need tokio select here because we have to listen to both the input stream
            // and the error channel. If there is an error, we need to discard all the
            // messages in the tracker and stop processing the input stream.
            tokio::select! {
                biased;
                Some(error) = ctx.error_rx.recv() => {
                    error!(?error, "error received while performing map operation");

                    if self.final_result.is_ok() {
                        // cancel the token to let the upstream know that we are shutting down so
                        // that they stop sending new messages.
                        ctx.cln_token.cancel();
                        self.final_result = Err(error);
                        self.shutting_down_on_err = true;
                    } else {
                        // store the error so that latest error will be propagated to the UI.
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
                        let permit = Arc::clone(&ctx.semaphore).acquire_owned()
                            .await.map_err(|e| Error::Mapper(format!("failed to acquire semaphore: {e}")))?;

                        // Spawn the appropriate task based on mapper type
                        match &ctx.concurrent_mapper {
                            ConcurrentMapper::Unary(mapper) => {
                                MapUnaryTask {
                                    mapper: mapper.clone(),
                                    permit,
                                    message: read_msg,
                                    shared_ctx: Arc::clone(&ctx.shared_ctx),
                                }
                                .spawn();
                            }
                            ConcurrentMapper::Stream(mapper) => {
                                MapStreamTask {
                                    mapper: mapper.clone(),
                                    permit,
                                    message: read_msg,
                                    shared_ctx: Arc::clone(&ctx.shared_ctx),
                                }
                                .spawn();
                            }
                        }
                    }
                },
            }
        }
        Ok(())
    }

    /// Processes messages using batch mapper.
    /// Messages are collected into batches and processed synchronously.
    async fn process_batch_messages(
        &mut self,
        input_stream: ReceiverStream<Message>,
        ctx: BatchMapContext,
    ) {
        let timeout_duration = self.read_timeout;
        let chunked_stream = input_stream.chunks_timeout(self.batch_size, timeout_duration);
        tokio::pin!(chunked_stream);

        let is_mono_vertex = is_mono_vertex();
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
                && let Err(e) = (MapBatchTask {
                    mapper: ctx.batch_mapper.clone(),
                    batch,
                    output_tx: ctx.output_tx.clone(),
                    tracker: self.tracker.clone(),
                    bypass_router: ctx.bypass_router.clone(),
                    is_mono_vertex,
                    cln_token: ctx.cln_token.clone(),
                })
                .execute()
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
                ctx.cln_token.cancel();
                self.shutting_down_on_err = true;
                self.final_result = Err(e);
            }
        }
    }
}

/// Shared context for concurrent map tasks.
pub(in crate::mapper) struct SharedMapTaskContext {
    pub output_tx: mpsc::Sender<Message>,
    pub error_tx: mpsc::Sender<Error>,
    pub tracker: Tracker,
    pub bypass_router: Option<MvtxBypassRouter>,
    pub hard_shutdown_token: CancellationToken,
    pub is_mono_vertex: bool,
}

/// Context for concurrent (unary/stream) map processing.
struct ConcurrentMapContext {
    error_rx: mpsc::Receiver<Error>,
    semaphore: Arc<Semaphore>,
    cln_token: CancellationToken,
    concurrent_mapper: ConcurrentMapper,
    shared_ctx: Arc<SharedMapTaskContext>,
}

/// Context for batch map processing.
struct BatchMapContext {
    output_tx: mpsc::Sender<Message>,
    cln_token: CancellationToken,
    bypass_router: Option<MvtxBypassRouter>,
    batch_mapper: UserDefinedBatchMap,
}

fn update_udf_error_metric(is_mono_vertex: bool) {
    if is_mono_vertex {
        monovertex_metrics()
            .udf
            .errors_total
            .get_or_create(mvtx_forward_metric_labels())
            .inc();
    } else {
        pipeline_metrics()
            .forwarder
            .udf_error_total
            .get_or_create(pipeline_metric_labels(VERTEX_TYPE_MAP_UDF))
            .inc();
    }
}

fn update_udf_process_time_metric(is_mono_vertex: bool) {
    if is_mono_vertex {
        monovertex_metrics()
            .udf
            .time
            .get_or_create(mvtx_forward_metric_labels())
            .observe(Instant::now().elapsed().as_micros() as f64);
    } else {
        pipeline_metrics()
            .forwarder
            .udf_processing_time
            .get_or_create(pipeline_metric_labels(VERTEX_TYPE_MAP_UDF))
            .observe(Instant::now().elapsed().as_micros() as f64);
    }
}

fn update_udf_write_only_metric(is_mono_vertex: bool) {
    if !is_mono_vertex {
        pipeline_metrics()
            .forwarder
            .udf_write_total
            .get_or_create(pipeline_metric_labels(VERTEX_TYPE_MAP_UDF))
            .inc();
    }
}

fn update_udf_write_metric(is_mono_vertex: bool, msg_info: &ParentMessageInfo, message_count: u64) {
    if is_mono_vertex {
        monovertex_metrics()
            .udf
            .time
            .get_or_create(mvtx_forward_metric_labels())
            .observe(msg_info.start_time.elapsed().as_micros() as f64);
    } else {
        pipeline_metrics()
            .forwarder
            .udf_write_total
            .get_or_create(pipeline_metric_labels(VERTEX_TYPE_MAP_UDF))
            .inc_by(message_count);

        pipeline_metrics()
            .forwarder
            .udf_processing_time
            .get_or_create(pipeline_metric_labels(VERTEX_TYPE_MAP_UDF))
            .observe(msg_info.start_time.elapsed().as_micros() as f64);
    }
}

fn update_udf_read_metric(is_mono_vertex: bool) {
    if !is_mono_vertex {
        pipeline_metrics()
            .forwarder
            .udf_read_total
            .get_or_create(pipeline_metric_labels(VERTEX_TYPE_MAP_UDF))
            .inc();
    }
}

/// ParentMessageInfo is used to store the information of the parent message. This is propagated to
/// all the downstream messages.
#[derive(Clone)]
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

impl From<&Message> for ParentMessageInfo {
    fn from(message: &Message) -> Self {
        Self {
            offset: message.offset.clone(),
            event_time: message.event_time,
            headers: Arc::clone(&message.headers),
            is_late: message.is_late,
            start_time: Instant::now(),
            current_index: 0,
            metadata: message.metadata.clone(),
            ack_handle: message.ack_handle.clone(),
        }
    }
}

/// Conversion from Message to MapRequest
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

/// Helper struct for converting UDF responses to Messages
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

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::mapper::test_utils::MapperTestHandle;
    use crate::message::ReadAck;
    use crate::{
        Result,
        message::{MessageID, Offset, StringOffset},
        shared::grpc::create_rpc_channel,
    };
    use numaflow::shared::ServerExtras;
    use numaflow::{batchmap, map, mapstream};
    use numaflow_pb::clients::map::map_client::MapClient;
    use tempfile::TempDir;
    use tokio::sync::mpsc::Sender;
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
            event_time: Utc::now(),
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
            MapperType::Concurrent(ConcurrentMapper::Unary(m)) => m.clone(),
            _ => panic!("Expected Unary mapper"),
        };

        let shared_ctx = Arc::new(SharedMapTaskContext {
            output_tx,
            error_tx,
            tracker,
            bypass_router: None,
            hard_shutdown_token: CancellationToken::new(),
            is_mono_vertex: false,
        });

        MapUnaryTask {
            mapper: unary_mapper,
            permit,
            message,
            shared_ctx,
        }
        .spawn();

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

    struct SimpleBatchMap;

    #[tonic::async_trait]
    impl batchmap::BatchMapper for SimpleBatchMap {
        async fn batchmap(
            &self,
            mut input: mpsc::Receiver<batchmap::Datum>,
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
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
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
        let tracker = Tracker::new(None, CancellationToken::new());

        let client = MapClient::new(create_rpc_channel(sock_file).await?);
        let mapper = MapHandle::new(
            MapMode::Batch,
            500,
            Duration::from_millis(1000),
            Duration::from_secs(10),
            10,
            client,
            tracker.clone(),
        )
        .await?;

        let messages = vec![
            Message {
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
            },
            Message {
                typ: Default::default(),
                keys: Arc::from(vec!["second".into()]),
                tags: None,
                value: "world".into(),
                offset: Offset::String(StringOffset::new("1".to_string(), 1)),
                event_time: chrono::Utc::now(),
                watermark: None,
                id: MessageID {
                    vertex_name: "vertex_name".to_string().into(),
                    offset: "1".to_string().into(),
                    index: 1,
                },
                ..Default::default()
            },
        ];

        let (input_tx, input_rx) = mpsc::channel(10);
        let input_stream = ReceiverStream::new(input_rx);

        for message in messages {
            input_tx.send(message).await.unwrap();
        }
        drop(input_tx);

        let (output_stream, map_handle) = mapper
            .streaming_map(input_stream, CancellationToken::new(), None)
            .await?;
        let mut output_rx = output_stream.into_inner();

        let mapped_message1 = output_rx.recv().await.unwrap();
        assert_eq!(mapped_message1.value, "hello");

        let mapped_message2 = output_rx.recv().await.unwrap();
        assert_eq!(mapped_message2.value, "world");

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
                    .with_keys(input.keys.clone())
                    .with_tags(vec![]);
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
        let tracker = Tracker::new(None, CancellationToken::new());

        let client = MapClient::new(create_rpc_channel(sock_file).await?);
        let mapper = MapHandle::new(
            MapMode::Stream,
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
            value: "test,map,stream".into(),
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

        let (input_tx, input_rx) = mpsc::channel(10);
        let input_stream = ReceiverStream::new(input_rx);

        input_tx.send(message).await.unwrap();
        drop(input_tx);

        let (mut output_stream, map_handle) = mapper
            .streaming_map(input_stream, CancellationToken::new(), None)
            .await?;

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

    struct PanicCat;

    #[tonic::async_trait]
    impl map::Mapper for PanicCat {
        async fn map(&self, _input: map::MapRequest) -> Vec<map::Message> {
            panic!("PanicCat panicked!");
        }
    }

    #[cfg(feature = "global-state-tests")]
    #[tokio::test]
    async fn test_map_with_panic() -> Result<()> {
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

        let (input_tx, input_rx) = mpsc::channel(10);
        let input_stream = ReceiverStream::new(input_rx);
        let cln_token = CancellationToken::new();
        let (_output_stream, map_handle) = mapper
            .streaming_map(input_stream, cln_token.clone(), None)
            .await?;
        let mut ack_rxs = vec![];
        // send 10 requests to the mapper
        for i in 0..10 {
            let (ack_tx, ack_rx) = oneshot::channel();
            let message = Message {
                typ: Default::default(),
                keys: Arc::from(vec![format!("key_{}", i)]),
                tags: None,
                value: format!("value_{}", i).into(),
                offset: Offset::String(StringOffset::new(i.to_string(), 0)),
                event_time: chrono::Utc::now(),
                watermark: None,
                id: MessageID {
                    vertex_name: "vertex_name".to_string().into(),
                    offset: i.to_string().into(),
                    index: i,
                },
                ack_handle: Some(Arc::new(AckHandle::new(ack_tx))),
                ..Default::default()
            };
            input_tx.send(message).await.unwrap();
            ack_rxs.push(ack_rx);
        }

        cln_token.cancelled().await;
        drop(input_tx);
        // Await the join handle and expect an error due to the panic
        let result = map_handle.await.unwrap();
        assert!(result.is_err(), "Expected an error due to panic");
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("PanicCat panicked!")
        );

        for ack_rx in ack_rxs {
            let ack = ack_rx.await.unwrap();
            assert_eq!(ack, ReadAck::Nak);
        }

        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(
            handle.is_finished(),
            "Expected gRPC server to have shut down"
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

    #[cfg(feature = "global-state-tests")]
    #[tokio::test]
    async fn test_batch_map_with_panic() -> Result<()> {
        let cln_token = CancellationToken::new();
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

        let tracker = Tracker::new(None, cln_token.clone());
        let client = MapClient::new(create_rpc_channel(sock_file).await?);
        let mapper = MapHandle::new(
            MapMode::Batch,
            500,
            Duration::from_millis(1000),
            Duration::from_secs(10),
            10,
            client,
            tracker.clone(),
        )
        .await?;

        let (ack_tx1, ack_rx1) = oneshot::channel();
        let (ack_tx2, ack_rx2) = oneshot::channel();
        let messages = vec![
            Message {
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
                ack_handle: Some(Arc::new(AckHandle::new(ack_tx1))),
                ..Default::default()
            },
            Message {
                typ: Default::default(),
                keys: Arc::from(vec!["second".into()]),
                tags: None,
                value: "world".into(),
                offset: Offset::String(StringOffset::new("1".to_string(), 1)),
                event_time: chrono::Utc::now(),
                watermark: None,
                id: MessageID {
                    vertex_name: "vertex_name".to_string().into(),
                    offset: "1".to_string().into(),
                    index: 1,
                },
                ack_handle: Some(Arc::new(AckHandle::new(ack_tx2))),
                ..Default::default()
            },
        ];

        let (input_tx, input_rx) = mpsc::channel(10);
        let input_stream = ReceiverStream::new(input_rx);

        for message in messages {
            input_tx.send(message).await.unwrap();
        }

        let (_output_stream, map_handle) = mapper
            .streaming_map(input_stream, cln_token.clone(), None)
            .await?;

        drop(input_tx);

        let ack1 = ack_rx1.await.unwrap();
        let ack2 = ack_rx2.await.unwrap();
        assert_eq!(ack1, ReadAck::Nak);
        assert_eq!(ack2, ReadAck::Nak);

        // Await the join handle and expect an error due to the panic
        let result = map_handle.await.unwrap();
        assert!(result.is_err(), "Expected an error due to panic");

        // FIXME: server should shutdown because of panic
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(
            handle.is_finished(),
            "Expected gRPC server to have shut down"
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

    #[cfg(feature = "global-state-tests")]
    #[tokio::test]
    async fn test_map_stream_with_panic() -> Result<()> {
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
        let cln_token = CancellationToken::new();

        // wait for the server to start
        tokio::time::sleep(Duration::from_millis(100)).await;

        let client = MapClient::new(create_rpc_channel(sock_file).await?);
        let tracker = Tracker::new(None, cln_token.clone());
        let mapper = MapHandle::new(
            MapMode::Stream,
            500,
            Duration::from_millis(1000),
            Duration::from_secs(10),
            10,
            client,
            tracker,
        )
        .await?;

        let (input_tx, input_rx) = mpsc::channel(10);
        let input_stream = ReceiverStream::new(input_rx);

        let (_output_stream, map_handle) = mapper
            .streaming_map(input_stream, cln_token.clone(), None)
            .await?;

        let mut ack_rxs = vec![];
        // send 10 requests to the mapper
        for i in 0..10 {
            let (ack_tx, ack_rx) = oneshot::channel();
            let message = Message {
                typ: Default::default(),
                keys: Arc::from(vec![format!("key_{}", i)]),
                tags: None,
                value: format!("value_{}", i).into(),
                offset: Offset::String(StringOffset::new(i.to_string(), 0)),
                event_time: chrono::Utc::now(),
                watermark: None,
                id: MessageID {
                    vertex_name: "vertex_name".to_string().into(),
                    offset: i.to_string().into(),
                    index: i,
                },
                ack_handle: Some(Arc::new(AckHandle::new(ack_tx))),
                ..Default::default()
            };
            ack_rxs.push(ack_rx);
            input_tx.send(message).await.unwrap();
        }

        cln_token.cancelled().await;
        drop(input_tx);
        // Await the join handle and expect an error due to the panic
        let result = map_handle.await.unwrap();
        assert!(result.is_err(), "Expected an error due to panic");
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("PanicFlatmapStream panicked!")
        );
        for ack_rx in ack_rxs {
            let ack = ack_rx.await.unwrap();
            assert_eq!(ack, ReadAck::Nak);
        }

        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(
            handle.is_finished(),
            "Expected gRPC server to have shut down"
        );
        Ok(())
    }

    fn create_default_msg(i: i32, ack_tx: oneshot::Sender<ReadAck>) -> Message {
        Message {
            typ: Default::default(),
            keys: Arc::from(vec![format!("key_{}", i)]),
            tags: None,
            value: format!("value_{}", i).into(),
            offset: Offset::String(StringOffset::new(i.to_string(), 0)),
            event_time: chrono::Utc::now(),
            watermark: None,
            id: MessageID {
                vertex_name: "vertex_name".to_string().into(),
                offset: i.to_string().into(),
                index: i,
            },
            ack_handle: Some(Arc::new(AckHandle::new(ack_tx))),
            ..Default::default()
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_threaded_map_with_panic() -> Result<()> {
        let cln_token = CancellationToken::new();
        let tracker = Tracker::new(None, cln_token.clone());
        let batch_size = 500;

        // create a mapper and start map server
        let MapperTestHandle {
            server_handle: _map_server_handle,
            mapper,
        } = MapperTestHandle::create_mapper(
            PanicCat,
            tracker,
            MapMode::Unary,
            batch_size,
            Duration::from_secs(1000),
            Duration::from_secs(10),
            10,
        )
        .await;

        let (input_tx, input_rx) = mpsc::channel(10);
        let input_stream = ReceiverStream::new(input_rx);
        let cln_token = CancellationToken::new();
        let (_output_stream, map_handle) = mapper
            .streaming_map(input_stream, cln_token.clone(), None)
            .await?;
        let mut ack_rxs = vec![];
        // send 10 requests to the mapper
        for i in 0..10 {
            let (ack_tx, ack_rx) = oneshot::channel();
            let message = create_default_msg(i, ack_tx);
            input_tx.send(message).await.unwrap();
            ack_rxs.push(ack_rx);
        }

        cln_token.cancelled().await;
        drop(input_tx);
        // Await the join handle and expect an error due to the panic
        let result = map_handle.await.unwrap();
        assert!(result.is_err(), "Expected an error due to panic");

        for ack_rx in ack_rxs {
            let ack = ack_rx.await.unwrap();
            assert_eq!(ack, ReadAck::Nak);
        }

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_threaded_batch_map_with_panic() -> Result<()> {
        let cln_token = CancellationToken::new();
        let tracker = Tracker::new(None, cln_token.clone());
        let batch_size = 500;

        // create a mapper and start map server
        let MapperTestHandle {
            server_handle: _map_server_handle,
            mapper,
        } = MapperTestHandle::create_batch_mapper(
            PanicBatchMap,
            tracker,
            MapMode::Batch,
            batch_size,
            Duration::from_secs(1000),
            Duration::from_secs(10),
            10,
        )
        .await;

        let (ack_tx1, ack_rx1) = oneshot::channel();
        let (ack_tx2, ack_rx2) = oneshot::channel();
        let messages = vec![
            Message {
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
                ack_handle: Some(Arc::new(AckHandle::new(ack_tx1))),
                ..Default::default()
            },
            Message {
                typ: Default::default(),
                keys: Arc::from(vec!["second".into()]),
                tags: None,
                value: "world".into(),
                offset: Offset::String(StringOffset::new("1".to_string(), 1)),
                event_time: chrono::Utc::now(),
                watermark: None,
                id: MessageID {
                    vertex_name: "vertex_name".to_string().into(),
                    offset: "1".to_string().into(),
                    index: 1,
                },
                ack_handle: Some(Arc::new(AckHandle::new(ack_tx2))),
                ..Default::default()
            },
        ];

        let (input_tx, input_rx) = mpsc::channel(10);
        let input_stream = ReceiverStream::new(input_rx);

        for message in messages {
            input_tx.send(message).await.unwrap();
        }

        let (_output_stream, map_handle) = mapper
            .streaming_map(input_stream, cln_token.clone(), None)
            .await?;

        drop(input_tx);

        let ack1 = ack_rx1.await.unwrap();
        let ack2 = ack_rx2.await.unwrap();
        assert_eq!(ack1, ReadAck::Nak);
        assert_eq!(ack2, ReadAck::Nak);

        // Await the join handle and expect an error due to the panic
        let result = map_handle.await.unwrap();
        assert!(result.is_err(), "Expected an error due to panic");

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_threaded_stream_with_panic() -> Result<()> {
        let cln_token = CancellationToken::new();
        let tracker = Tracker::new(None, cln_token.clone());
        let batch_size = 500;

        // create a mapper and start map server
        let MapperTestHandle {
            server_handle: _map_server_handle,
            mapper,
        } = MapperTestHandle::create_map_streamer(
            PanicFlatmapStream,
            tracker,
            MapMode::Stream,
            batch_size,
            Duration::from_secs(1000),
            Duration::from_secs(10),
            10,
        )
        .await;

        let (input_tx, input_rx) = mpsc::channel(10);
        let input_stream = ReceiverStream::new(input_rx);

        let (_output_stream, map_handle) = mapper
            .streaming_map(input_stream, cln_token.clone(), None)
            .await?;

        let mut ack_rxs = vec![];
        // send 10 requests to the mapper
        for i in 0..10 {
            let (ack_tx, ack_rx) = oneshot::channel();
            let message = create_default_msg(i, ack_tx);
            ack_rxs.push(ack_rx);
            input_tx
                .send(message)
                .await
                .expect("Send message to map stream");
        }

        cln_token.cancelled().await;
        drop(input_tx);
        // Await the join handle and expect an error due to the panic
        let result = map_handle.await.expect("failed to await map handle");
        assert!(result.is_err(), "Expected an error due to panic");

        for ack_rx in ack_rxs {
            let ack = ack_rx.await.expect("Failed to await ack rx");
            assert_eq!(ack, ReadAck::Nak, "Expected Nak due to panic");
        }

        Ok(())
    }

    #[test]
    #[serial_test::serial]
    fn test_update_udf_error_metric_mono_vertex() {
        let before = monovertex_metrics()
            .udf
            .errors_total
            .get_or_create(mvtx_forward_metric_labels())
            .get();

        update_udf_error_metric(true);

        let after = monovertex_metrics()
            .udf
            .errors_total
            .get_or_create(mvtx_forward_metric_labels())
            .get();

        assert_eq!(
            after,
            before + 1,
            "monovertex udf errors_total should be incremented by 1"
        );
    }

    #[test]
    #[serial_test::serial]
    fn test_update_udf_error_metric_pipeline() {
        let before = pipeline_metrics()
            .forwarder
            .udf_error_total
            .get_or_create(pipeline_metric_labels(VERTEX_TYPE_MAP_UDF))
            .get();

        update_udf_error_metric(false);

        let after = pipeline_metrics()
            .forwarder
            .udf_error_total
            .get_or_create(pipeline_metric_labels(VERTEX_TYPE_MAP_UDF))
            .get();

        assert_eq!(
            after,
            before + 1,
            "pipeline udf_error_total should be incremented by 1"
        );
    }

    #[test]
    #[serial_test::serial]
    fn test_update_udf_write_only_metric_mono_vertex() {
        let before = pipeline_metrics()
            .forwarder
            .udf_write_total
            .get_or_create(pipeline_metric_labels(VERTEX_TYPE_MAP_UDF))
            .get();

        update_udf_write_only_metric(true);

        let after = pipeline_metrics()
            .forwarder
            .udf_write_total
            .get_or_create(pipeline_metric_labels(VERTEX_TYPE_MAP_UDF))
            .get();

        assert_eq!(
            after, before,
            "pipeline udf_write_total should NOT be incremented when is_mono_vertex is true"
        );
    }

    #[test]
    #[serial_test::serial]
    fn test_update_udf_write_only_metric_pipeline() {
        let before = pipeline_metrics()
            .forwarder
            .udf_write_total
            .get_or_create(pipeline_metric_labels(VERTEX_TYPE_MAP_UDF))
            .get();

        update_udf_write_only_metric(false);

        let after = pipeline_metrics()
            .forwarder
            .udf_write_total
            .get_or_create(pipeline_metric_labels(VERTEX_TYPE_MAP_UDF))
            .get();

        assert_eq!(
            after,
            before + 1,
            "pipeline udf_write_total should be incremented by 1"
        );
    }

    #[test]
    #[serial_test::serial]
    fn test_update_udf_read_metric_mono_vertex() {
        let before = pipeline_metrics()
            .forwarder
            .udf_read_total
            .get_or_create(pipeline_metric_labels(VERTEX_TYPE_MAP_UDF))
            .get();

        update_udf_read_metric(true);

        let after = pipeline_metrics()
            .forwarder
            .udf_read_total
            .get_or_create(pipeline_metric_labels(VERTEX_TYPE_MAP_UDF))
            .get();

        assert_eq!(
            after, before,
            "pipeline udf_read_total should NOT be incremented when is_mono_vertex is true"
        );
    }

    #[test]
    #[serial_test::serial]
    fn test_update_udf_read_metric_pipeline() {
        let before = pipeline_metrics()
            .forwarder
            .udf_read_total
            .get_or_create(pipeline_metric_labels(VERTEX_TYPE_MAP_UDF))
            .get();

        update_udf_read_metric(false);

        let after = pipeline_metrics()
            .forwarder
            .udf_read_total
            .get_or_create(pipeline_metric_labels(VERTEX_TYPE_MAP_UDF))
            .get();

        assert_eq!(
            after,
            before + 1,
            "pipeline udf_read_total should be incremented by 1"
        );
    }

    #[test]
    #[serial_test::serial]
    fn test_update_udf_write_metric_mono_vertex() {
        let pipeline_write_before = pipeline_metrics()
            .forwarder
            .udf_write_total
            .get_or_create(pipeline_metric_labels(VERTEX_TYPE_MAP_UDF))
            .get();

        let msg_info = ParentMessageInfo {
            offset: Default::default(),
            event_time: Default::default(),
            is_late: false,
            headers: Arc::new(Default::default()),
            start_time: std::time::Instant::now(),
            current_index: 0,
            metadata: None,
            ack_handle: None,
        };

        update_udf_write_metric(true, &msg_info, 5);

        let pipeline_write_after = pipeline_metrics()
            .forwarder
            .udf_write_total
            .get_or_create(pipeline_metric_labels(VERTEX_TYPE_MAP_UDF))
            .get();

        assert_eq!(
            pipeline_write_after, pipeline_write_before,
            "pipeline udf_write_total should NOT be incremented when is_mono_vertex is true"
        );
    }

    #[test]
    #[serial_test::serial]
    fn test_update_udf_write_metric_pipeline() {
        let before = pipeline_metrics()
            .forwarder
            .udf_write_total
            .get_or_create(pipeline_metric_labels(VERTEX_TYPE_MAP_UDF))
            .get();

        let msg_info = ParentMessageInfo {
            offset: Default::default(),
            event_time: Default::default(),
            is_late: false,
            headers: Arc::new(Default::default()),
            start_time: std::time::Instant::now(),
            current_index: 0,
            metadata: None,
            ack_handle: None,
        };

        update_udf_write_metric(false, &msg_info, 5);

        let after = pipeline_metrics()
            .forwarder
            .udf_write_total
            .get_or_create(pipeline_metric_labels(VERTEX_TYPE_MAP_UDF))
            .get();

        assert_eq!(
            after,
            before + 5,
            "pipeline udf_write_total should be incremented by message_count"
        );
    }

    #[test]
    #[serial_test::serial]
    fn test_update_udf_process_time_metric_mono_vertex() {
        // Currently only ensuring that this should not panic
        update_udf_process_time_metric(true);
    }

    #[test]
    #[serial_test::serial]
    fn test_update_udf_process_time_metric_pipeline() {
        // Currently only ensuring that this should not panic
        update_udf_process_time_metric(false);
    }
}
