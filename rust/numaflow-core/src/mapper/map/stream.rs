use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::sync::Arc;
use std::sync::Mutex;

use crate::config::is_mono_vertex;
use crate::error::{Error, Result};
use crate::message::{Message, MessageHandle};
use crate::{mark_failed, mark_success};
use numaflow_pb::clients::map::{self, MapRequest, MapResponse, map_client::MapClient};
use tokio::sync::{OwnedSemaphorePermit, mpsc};
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;
use tokio_util::task::AbortOnDropHandle;
use tonic::Streaming;
use tonic::transport::Channel;
use tracing::{error, warn};

use super::{
    ParentMessageInfo, STREAMING_MAP_RESP_CHANNEL_SIZE, SharedMapTaskContext, UserDefinedMessage,
    create_response_stream, update_udf_error_metric, update_udf_process_time_metric,
    update_udf_read_metric, update_udf_write_only_metric,
};

/// Type alias for the stream response - raw results from the UDF
pub(in crate::mapper) type StreamMapResponse = Vec<map::map_response::Result>;

/// Type aliases for HashMap used to track the oneshot response sender for each request keyed by
/// message id.
type StreamResponseSenderMap = HashMap<String, mpsc::Sender<Result<StreamMapResponse>>>;

/// Shared state for tracking batch map senders between the sender and the receiver tasks.
/// We have BiDi gRPC stream so we have 2 different set of tasks for sending and receiving.
#[derive(Default)]
pub(in crate::mapper) struct StreamSenderMapState {
    /// Map of oneshot response senders keyed by message id.
    map: StreamResponseSenderMap,
    /// Flag to indicate whether the rx task has closed the stream and cleared the `map`.
    /// This is because `tx.send()` could return `Ok()` even after the receiver task has closed the
    /// stream.
    closed: bool,
}

/// MapStreamTask encapsulates all the context needed to execute a stream map operation.
pub(in crate::mapper) struct MapStreamTask {
    pub mapper: UserDefinedStreamMap,
    // Held for the lifetime of execute(); dropping self releases the slot back to the semaphore.
    #[allow(dead_code)]
    pub permit: OwnedSemaphorePermit,
    pub msg_handle: MessageHandle,
    pub shared_ctx: Arc<SharedMapTaskContext>,
}

impl MapStreamTask {
    /// Spawns the stream map task as a tokio task.
    /// The task will process the message through the UDF and send results downstream.
    pub fn spawn(self) {
        tokio::spawn(async move {
            self.execute().await;
        });
    }

    /// Executes the stream map operation.
    async fn execute(self) {
        // Store parent message info before sending to UDF
        // parent_info contains offset, so we don't need to clone it separately
        let mut parent_info: ParentMessageInfo = self.msg_handle.message().into();

        let request: MapRequest = self.msg_handle.message().clone().into();
        update_udf_read_metric(self.shared_ctx.is_mono_vertex);

        // Call the UDF and get receiver for raw results
        let mut receiver = self
            .mapper
            .stream(request, self.shared_ctx.hard_shutdown_token.clone())
            .await;

        // We need to update the tracker with no responses, because unlike unary and batch,
        // we cannot update the responses here - we will have to append the responses.
        // Use parent_info.offset instead of cloning offset separately
        self.shared_ctx
            .tracker
            .serving_refresh(parent_info.offset.clone())
            .await
            .expect("failed to reset tracker");

        loop {
            match receiver.recv().await {
                Some(Ok(results)) => {
                    self.process_results(&mut parent_info, results).await;
                }
                Some(Err(e)) => {
                    error!(msg_id = ?self.msg_handle.message.id, ?e, "failed to map message");
                    mark_failed!(self.msg_handle, &e);
                    let _ = self.shared_ctx.error_tx.send(e).await;
                    return;
                }
                None => {
                    // Channel closed — stream ended cleanly (e.g., UDF returned empty results or
                    // finished after sending all results). Fall through to mark_success below.
                    break;
                }
            }
        }

        // Decrement the original ref_count now that we've accounted for all downstream messages.
        mark_success!(self.msg_handle);
    }

    /// Materializes each raw UDF result into a [`Message`] (stamped with a monotonic
    /// `current_index` for unique child offsets), create a child msg_handle from child
    /// message and parent msg_handle so the parent's ACK is deferred until all children
    /// are written, then either routes it through `bypass_router` (consumed if bypassed)
    /// or forwards it to `output_tx`.
    ///
    /// `parent_info.current_index` is mutated and persists across calls for the same parent.
    async fn process_results(
        &self,
        parent_info: &mut ParentMessageInfo,
        results: Vec<map::map_response::Result>,
    ) {
        for result in results {
            let mapped_message: Message =
                UserDefinedMessage(result, parent_info, parent_info.current_index).into();
            parent_info.current_index += 1;

            update_udf_write_only_metric(self.shared_ctx.is_mono_vertex);

            self.shared_ctx
                .tracker
                .serving_append(mapped_message.offset.clone(), mapped_message.tags.clone())
                .await
                .expect("failed to update tracker");

            // Each downstream handle shares the original ack tracking — ACK is
            // deferred until all mapped messages are written to ISB/sink.
            let msg_handle = self.msg_handle.with_message(mapped_message);

            // Try to bypass the message. If bypassed, try_bypass takes ownership and returns None.
            // If not bypassed, it returns Some(msg_handle) for us to send downstream.
            let msg_handle = if let Some(ref bypass_router) = self.shared_ctx.bypass_router {
                match bypass_router
                    .try_bypass(msg_handle)
                    .await
                    .expect("failed to send message to bypass channel")
                {
                    Some(msg) => msg,
                    None => continue, // Message was bypassed, move to next
                }
            } else {
                msg_handle
            };

            self.shared_ctx
                .output_tx
                .send(msg_handle)
                .await
                .expect("failed to send response");
        }
    }
}

/// UserDefinedStreamMap is a grpc client that sends stream requests to the map server
#[derive(Clone)]
pub(in crate::mapper) struct UserDefinedStreamMap {
    read_tx: mpsc::Sender<MapRequest>,
    senders: Arc<Mutex<StreamSenderMapState>>,
    _handle: Arc<AbortOnDropHandle<()>>,
}

impl UserDefinedStreamMap {
    /// Performs handshake with the server and creates a new UserDefinedMap.
    pub(in crate::mapper) async fn new(
        batch_size: usize,
        mut client: MapClient<Channel>,
    ) -> Result<Self> {
        let (read_tx, read_rx) = mpsc::channel(batch_size);
        let resp_stream = create_response_stream(read_tx.clone(), read_rx, &mut client).await?;

        // map to track the mpsc response sender for each request
        let sender_map = Arc::new(Mutex::new(StreamSenderMapState::default()));

        // background task to receive responses from the server and send them to the appropriate
        // mpsc sender based on the id
        let sender_map_clone = Arc::clone(&sender_map);
        let handle = tokio::spawn(async move {
            Self::receive_stream_responses(sender_map_clone, resp_stream).await;
        });

        let mapper = Self {
            read_tx,
            senders: sender_map,
            _handle: Arc::new(AbortOnDropHandle::new(handle)),
        };
        Ok(mapper)
    }

    /// Broadcasts a gRPC error to all pending senders and records error metrics.
    async fn broadcast_error(sender_map: &Arc<Mutex<StreamSenderMapState>>, error: tonic::Status) {
        // Force dropping the sender_guard by moving it out of the scope
        // Using `drop(sender_guard)` here doesn't satisfy the borrow checker since it assumes it is
        // still in use across await calls for some reason.
        let senders = {
            let mut sender_guard = sender_map.lock().expect("failed to acquire poisoned lock");
            sender_guard.closed = true;
            std::mem::take(&mut sender_guard.map)
        };

        for (_, sender) in senders {
            let _ = sender.send(Err(Error::Grpc(Box::new(error.clone())))).await;
            update_udf_error_metric(is_mono_vertex());
        }
    }

    /// receive responses from the server and gets the corresponding mpsc sender from the map
    /// and sends the response.
    async fn receive_stream_responses(
        sender_map: Arc<Mutex<StreamSenderMapState>>,
        mut resp_stream: Streaming<MapResponse>,
    ) {
        while let Some(resp) = resp_stream.next().await {
            match resp {
                Ok(resp) => {
                    if let Err(e) = Self::process_response(&sender_map, resp).await {
                        warn!("received error while processing stream response: {}", e);
                    }
                }
                Err(e) => {
                    error!(?e, "Error reading message from stream map gRPC stream");
                    Self::broadcast_error(&sender_map, e).await;
                }
            }
        }

        // broadcast error for all pending senders that might've gotten
        // added while the stream was draining
        Self::broadcast_error(
            &sender_map,
            tonic::Status::aborted("receiver stream dropped"),
        )
        .await;
    }

    /// This method processes the response received from the stream map udf server
    ///
    /// The reason this method hasn't been subdivided is that we don't want to yield
    /// back to the tokio runtime at any point between these three operations:
    /// * remove id from sender map
    /// * check for EOT
    /// * re-insert id into sender map
    async fn process_response(
        sender_map: &Arc<Mutex<StreamSenderMapState>>,
        resp: MapResponse,
    ) -> Result<()> {
        let response_sender_entry = {
            sender_map
                .lock()
                .expect("failed to acquire poisoned lock")
                .map
                .remove(&resp.id)
        };

        // once we get eot, we can drop the sender to let the callee
        // know that we are done sending responses
        if let Some(map::TransmissionStatus { eot: true }) = resp.status {
            update_udf_process_time_metric(is_mono_vertex());
            return Ok(());
        }

        if let Some(response_sender) = response_sender_entry {
            // move the senders_guard out of the scope to drop the guard before sending the response
            // Insert the sender back into the SenderMap to avoid yielding back control
            // to the tokio runtime during send on the actual sender
            let mapper_closed = {
                let mut senders_guard = sender_map.lock().expect("failed to acquire poisoned lock");
                if !senders_guard.closed {
                    // Write the sender back to the map, because we need to send
                    // more responses for the same request
                    senders_guard
                        .map
                        .insert(resp.id.clone(), response_sender.clone());
                }
                senders_guard.closed
            };

            if mapper_closed {
                let _ = response_sender
                    .send(Err(Error::Mapper("stream mapper closed".to_string())))
                    .await;
                return Ok(());
            }

            if response_sender.send(Ok(resp.results)).await.is_err() {
                {
                    let mut sender_guard =
                        sender_map.lock().expect("failed to acquire poisoned lock");

                    // Remove the sender to avoid keeping the corresponding receiver waiting
                    sender_guard.map.remove(&resp.id);
                }

                return Err(Error::Mapper(format!(
                    "Failed to send map response to stream task for ID: {:?}",
                    resp.id
                )));
            }
        } else {
            return Err(Error::Mapper(format!(
                "No such req/resp ID found in StreamResponseSenderMap: {}",
                resp.id
            )));
        }

        Ok(())
    }

    /// Sends a request to the UDF and returns a receiver for raw response results.
    /// If the cancellation token is already cancelled, returns a receiver that
    /// immediately yields an error indicating the operation was cancelled.
    pub(in crate::mapper) async fn stream(
        &self,
        request: MapRequest,
        cln_token: CancellationToken,
    ) -> mpsc::Receiver<Result<StreamMapResponse>> {
        let (tx, rx) = mpsc::channel(STREAMING_MAP_RESP_CHANNEL_SIZE);

        // Check if already canceled before sending
        if cln_token.is_cancelled() {
            let _ = tx
                .send(Err(Error::Mapper(
                    "stream map operation cancelled".to_string(),
                )))
                .await;
            return rx;
        }

        let key = request.id.clone();

        // Move the senders_guard out of the scope to drop the guard before sending the response.
        // Do this before we send the message to the server to avoid the race condition
        // where the server processes the message faster than the corresponding sender
        // is added to the SenderMap.
        //
        // If an entry for `key` already exists, another task is in flight for the same
        // request id (typically caused by a duplicate source read). We MUST NOT overwrite —
        // doing so silently destroys the in-flight task's response sender and reroutes its
        // remaining gRPC frames to this task's channel. Fail fast on this rx instead and
        // let the caller NAK upstream.
        let early_error = {
            let mut senders_guard = self
                .senders
                .lock()
                .expect("failed to acquire poisoned lock");
            if senders_guard.closed {
                Some("stream mapper closed".to_string())
            } else {
                match senders_guard.map.entry(key.clone()) {
                    Entry::Occupied(_) => Some(format!(
                        "duplicate in-flight request id: {key}; refusing to overwrite existing sender"
                    )),
                    Entry::Vacant(slot) => {
                        slot.insert(tx.clone());
                        None
                    }
                }
            }
        };

        if let Some(reason) = early_error {
            let _ = tx.send(Err(Error::Mapper(reason))).await;
            return rx;
        }

        // Sender is already registered in the map; forward the request to the UDF server.
        if let Err(e) = self.read_tx.send(request).await {
            error!(?e, "Failed to send message to map stream udf server");
            // We should ideally remove the resp.id from the SenderMap to avoid potential
            // memory leaks as well as to avoid holding the corresponding receiver waiting.
            // We don't care about the return value since we already have access to the 'tx'
            {
                let _ = self
                    .senders
                    .lock()
                    .expect("failed to acquire poisoned lock")
                    .map
                    .remove(&key);
            };

            // send error on 'tx'
            let _ = tx
                .send(Err(Error::Mapper(format!(
                    "failed to send message to map stream server: {e}"
                ))))
                .await
                .inspect_err(|_| warn!("failed to send error to receiver"));
        }

        rx
    }
}

#[cfg(test)]
mod tests {
    use crate::error::Error as MapError;
    use crate::mapper::map::stream::{StreamSenderMapState, UserDefinedStreamMap};
    use crate::shared::grpc::create_rpc_channel;
    use numaflow::mapstream;
    use numaflow::shared::ServerExtras;
    use numaflow_pb::clients::map::map_client::MapClient;
    use numaflow_pb::clients::map::{MapRequest, MapResponse, TransmissionStatus};
    use std::error::Error;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;
    use tempfile::TempDir;
    use tokio::sync::mpsc;
    use tokio_util::sync::CancellationToken;
    use tokio_util::task::AbortOnDropHandle;

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
                    .with_keys(input.keys.clone())
                    .with_tags(vec![]);
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

        let client =
            UserDefinedStreamMap::new(500, MapClient::new(create_rpc_channel(sock_file).await?))
                .await?;

        // Create a MapRequest directly instead of a Message
        let request = numaflow_pb::clients::map::MapRequest {
            request: Some(numaflow_pb::clients::map::map_request::Request {
                keys: vec!["first".into()],
                value: "test,map,stream".into(),
                event_time: None,
                watermark: None,
                headers: Default::default(),
                metadata: None,
            }),
            id: "0".to_string(),
            handshake: None,
            status: None,
        };

        let cln_token = tokio_util::sync::CancellationToken::new();
        let mut rx = client.stream(request, cln_token).await;

        // Collect all response batches
        let mut all_results = vec![];
        while let Some(response) = rx.recv().await {
            let results = response?;
            all_results.extend(results);
        }

        assert_eq!(all_results.len(), 3);
        // convert the bytes value to string and compare
        let values: Vec<String> = all_results
            .iter()
            .map(|r| String::from_utf8(r.value.clone()).unwrap())
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

    fn make_response(id: &str, eot: bool) -> MapResponse {
        MapResponse {
            results: vec![],
            id: id.to_string(),
            handshake: None,
            status: if eot {
                Some(TransmissionStatus { eot: true })
            } else {
                None
            },
        }
    }

    #[tokio::test]
    async fn process_response_returns_error_when_no_sender_entry() {
        let sender_map = Arc::new(Mutex::new(StreamSenderMapState::default()));

        let result =
            UserDefinedStreamMap::process_response(&sender_map, make_response("missing", false))
                .await;

        let err = result.expect_err("expected error when sender entry missing");
        assert!(matches!(err, MapError::Mapper(_)));
        assert!(
            err.to_string()
                .contains("No such req/resp ID found in StreamResponseSenderMap"),
            "unexpected error message: {err}"
        );
        assert!(sender_map.lock().unwrap().map.is_empty());
    }

    #[tokio::test]
    async fn process_response_returns_error_when_response_sender_send_fails() {
        let sender_map = Arc::new(Mutex::new(StreamSenderMapState::default()));
        let (tx, rx) = mpsc::channel(1);
        // Drop the receiver so the next send on tx fails.
        drop(rx);
        sender_map.lock().unwrap().map.insert("0".to_string(), tx);

        let result =
            UserDefinedStreamMap::process_response(&sender_map, make_response("0", false)).await;

        let err = result.expect_err("expected error when send to response sender fails");
        assert!(matches!(err, MapError::Mapper(_)));
        assert!(
            err.to_string()
                .contains("Failed to send map response to stream task"),
            "unexpected error message: {err}"
        );
        // The entry must be removed so the wrapper does not leak senders.
        assert!(
            !sender_map.lock().unwrap().map.contains_key("0"),
            "sender entry should have been removed after send failure"
        );
    }

    #[tokio::test]
    async fn process_response_signals_closed_mapper_on_response_sender() {
        let sender_map = Arc::new(Mutex::new(StreamSenderMapState::default()));
        let (tx, mut rx) = mpsc::channel(1);
        {
            let mut guard = sender_map.lock().unwrap();
            guard.map.insert("0".to_string(), tx);
            guard.closed = true;
        }

        let result =
            UserDefinedStreamMap::process_response(&sender_map, make_response("0", false)).await;
        assert!(result.is_ok(), "expected Ok when mapper is closed");

        // Caller should see a "mapper closed" error on the response channel.
        let received = rx.recv().await.expect("expected error on response channel");
        let err = received.expect_err("expected Err variant");
        assert!(matches!(err, MapError::Mapper(_)));
        assert!(err.to_string().contains("mapper closed"));

        // Closed-mapper path must not re-insert the sender.
        assert!(
            !sender_map.lock().unwrap().map.contains_key("0"),
            "sender should not be re-inserted when mapper is closed"
        );
    }

    #[tokio::test]
    async fn process_response_drops_sender_on_eot_without_error() {
        let sender_map = Arc::new(Mutex::new(StreamSenderMapState::default()));
        let (tx, mut rx) = mpsc::channel(1);
        sender_map.lock().unwrap().map.insert("0".to_string(), tx);

        let result =
            UserDefinedStreamMap::process_response(&sender_map, make_response("0", true)).await;
        assert!(result.is_ok(), "EOT path should return Ok");

        // EOT does not deliver a payload to the response sender.
        assert!(matches!(
            rx.try_recv(),
            Err(mpsc::error::TryRecvError::Empty | mpsc::error::TryRecvError::Disconnected)
        ));

        // EOT removes the sender from the map (and never re-inserts it),
        // letting the corresponding receiver loop terminate.
        assert!(
            !sender_map.lock().unwrap().map.contains_key("0"),
            "sender entry should be removed after EOT"
        );
    }

    #[tokio::test]
    async fn broadcast_error_marks_closed_and_errors_all_senders() {
        let sender_map = Arc::new(Mutex::new(StreamSenderMapState::default()));
        let (tx_a, mut rx_a) = mpsc::channel(1);
        let (tx_b, mut rx_b) = mpsc::channel(1);
        {
            let mut guard = sender_map.lock().unwrap();
            guard.map.insert("a".to_string(), tx_a);
            guard.map.insert("b".to_string(), tx_b);
        }

        UserDefinedStreamMap::broadcast_error(&sender_map, tonic::Status::aborted("test")).await;

        {
            let guard = sender_map.lock().unwrap();
            assert!(guard.closed, "broadcast_error should set closed = true");
            assert!(guard.map.is_empty(), "all senders should be drained");
        }

        for rx in [&mut rx_a, &mut rx_b] {
            let received = rx.recv().await.expect("expected error broadcast");
            let err = received.expect_err("expected Err variant");
            assert!(matches!(err, MapError::Grpc(_)));
        }
    }

    #[tokio::test]
    async fn stream_method_cleans_up_on_read_tx_send_failure() {
        // Build a UserDefinedStreamMap whose read_tx receiver has been dropped,
        // so that read_tx.send(..) inside `stream` fails immediately.
        let (read_tx, read_rx) = mpsc::channel::<MapRequest>(10);
        drop(read_rx);

        let dummy_handle = tokio::spawn(async {});
        let _abort_handle = Arc::new(AbortOnDropHandle::new(dummy_handle));

        let senders = Arc::new(Mutex::new(StreamSenderMapState::default()));
        let mapper = UserDefinedStreamMap {
            read_tx,
            senders: Arc::clone(&senders),
            _handle: _abort_handle,
        };

        let request = MapRequest {
            request: Some(numaflow_pb::clients::map::map_request::Request {
                keys: vec!["k".into()],
                value: b"v".to_vec(),
                event_time: None,
                watermark: None,
                headers: Default::default(),
                metadata: None,
            }),
            id: "42".to_string(),
            handshake: None,
            status: None,
        };

        let mut rx = mapper.stream(request, CancellationToken::new()).await;

        // The receiver must observe the read_tx failure as a Mapper error.
        let first = rx.recv().await.expect("expected error on stream rx");
        let err = first.expect_err("expected Err variant");
        assert!(matches!(err, MapError::Mapper(_)));
        assert!(
            err.to_string()
                .contains("failed to send message to map stream server"),
            "unexpected error message: {err}"
        );

        // The senders map must no longer contain the failed request id.
        assert!(
            !senders.lock().unwrap().map.contains_key("42"),
            "senders map should be cleaned up on read_tx send failure"
        );
    }

    #[tokio::test]
    async fn stream_method_rejects_duplicate_request_id() {
        // Regression: two concurrent stream() calls for the same request id used to
        // silently overwrite each other in `senders.map`. The first task's response
        // sender was dropped and its in-flight gRPC frames were rerouted to the second
        // task — leading to duplicate downstream child ids and a partial-ack incident
        // (see misc/investigations/map-stream-loss.md).
        //
        // Expected behavior: the in-flight sender is preserved and the duplicate call
        // returns an error on its own rx without touching the existing entry.

        let (read_tx, mut read_rx) = mpsc::channel::<MapRequest>(10);

        let dummy_handle = tokio::spawn(async {});
        let abort_handle = Arc::new(AbortOnDropHandle::new(dummy_handle));

        let senders = Arc::new(Mutex::new(StreamSenderMapState::default()));
        let mapper = UserDefinedStreamMap {
            read_tx,
            senders: Arc::clone(&senders),
            _handle: abort_handle,
        };

        let make_request = |id: &str| MapRequest {
            request: Some(numaflow_pb::clients::map::map_request::Request {
                keys: vec!["k".into()],
                value: b"v".to_vec(),
                event_time: None,
                watermark: None,
                headers: Default::default(),
                metadata: None,
            }),
            id: id.to_string(),
            handshake: None,
            status: None,
        };

        let cln_token = CancellationToken::new();

        // First stream() call — inserts a sender for "dup-id" and forwards the request
        // to the server side (here, read_rx). We hold rx_first so the channel stays
        // alive and the map entry remains.
        let mut rx_first = mapper
            .stream(make_request("dup-id"), cln_token.clone())
            .await;
        let first_req = read_rx
            .recv()
            .await
            .expect("first request should be forwarded to read_rx");
        assert_eq!(first_req.id, "dup-id");

        // Snapshot the sender pointer for "dup-id" so we can assert it was NOT replaced.
        let first_sender_ptr = {
            let guard = senders.lock().unwrap();
            let sender = guard
                .map
                .get("dup-id")
                .expect("first sender must be in the map");
            sender.clone()
        };

        // Second stream() call with the SAME id — must fast-fail on its own rx without
        // overwriting the existing entry or forwarding a request to the server.
        let mut rx_dup = mapper
            .stream(make_request("dup-id"), cln_token.clone())
            .await;

        let dup = tokio::time::timeout(Duration::from_millis(500), rx_dup.recv())
            .await
            .expect("duplicate rx must yield within 500ms (no hang on collision)")
            .expect("duplicate rx must yield Some(err)");
        let err = dup.expect_err("duplicate rx must yield Err variant");
        assert!(matches!(err, MapError::Mapper(_)));
        assert!(
            err.to_string().contains("duplicate in-flight request id"),
            "unexpected error message: {err}"
        );

        // The original sender must still be in the map AND must be the same Sender —
        // not a fresh one inserted by the duplicate call.
        {
            let guard = senders.lock().unwrap();
            let still = guard
                .map
                .get("dup-id")
                .expect("first sender must remain in the map after collision is rejected");
            assert!(
                still.same_channel(&first_sender_ptr),
                "duplicate stream() must not replace the in-flight sender"
            );
        }

        // First rx must be untouched — no spurious error pushed onto it by the collision.
        let no_error = tokio::time::timeout(Duration::from_millis(50), rx_first.recv()).await;
        assert!(
            no_error.is_err(),
            "first rx must not receive anything when a duplicate stream() is rejected"
        );

        // The duplicate call must NOT have forwarded a request to the server.
        let no_req = tokio::time::timeout(Duration::from_millis(50), read_rx.recv()).await;
        assert!(
            no_req.is_err(),
            "duplicate stream() must not forward its request to the server"
        );
    }
}
