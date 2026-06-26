use super::{
    ParentMessageInfo, UserDefinedMessage, create_response_stream, grpc_error_to_redrive,
    map_redrive_error, reconnect_mapper_client, update_udf_error_metric,
    update_udf_process_time_metric, update_udf_read_metric, update_udf_write_metric,
};
use crate::config::is_mono_vertex;
use crate::error::{Error, Result};
use crate::message::{Message, MessageHandle};
use crate::monovertex::bypass_router::MvtxBypassRouter;
use crate::shared::grpc::UdfReconnectConfig;
use crate::shared::otel;
use crate::tracker::Tracker;
use crate::{mark_failed, mark_success};
use bytes::Bytes;
use numaflow_pb::clients::map::{self, MapRequest, MapResponse, map_client::MapClient};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use tokio::sync::{Mutex as AsyncMutex, mpsc, oneshot};
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;
use tokio_util::task::AbortOnDropHandle;
use tonic::Status;
use tonic::Streaming;
use tonic::transport::Channel;
use tracing::{error, warn};

/// Type alias for the batch response - raw results from the UDF
pub(in crate::mapper) type BatchMapResponse = Vec<map::map_response::Result>;

/// Type aliases for HashMap used to track the oneshot response sender for each request keyed by
/// message id.
type ResponseSenderMap = HashMap<String, oneshot::Sender<Result<BatchMapResponse>>>;

/// Shared state for tracking batch map senders between the sender and the receiver tasks.
/// We have BiDi gRPC stream so we have 2 different set of tasks for sending and receiving.
#[derive(Default)]
pub(in crate::mapper) struct BatchSenderMapState {
    /// Map of oneshot response senders keyed by message id.
    map: ResponseSenderMap,
    /// Flag to indicate whether the rx task has closed the stream and cleared the `map`.
    /// This is because `tx.send()` could return `Ok()` even after the receiver task has closed the
    /// stream.
    closed: bool,
}

/// MapBatchTask encapsulates all the context needed to execute a batch map operation.
pub(in crate::mapper) struct MapBatchTask {
    pub mapper: UserDefinedBatchMap,
    pub msg_handles: Vec<MessageHandle>,
    pub output_tx: mpsc::Sender<MessageHandle>,
    pub tracker: Tracker,
    pub bypass_router: Option<MvtxBypassRouter>,
    pub is_mono_vertex: bool,
    pub cln_token: CancellationToken,
}

pub(in crate::mapper) enum BatchMapTaskError {
    Redrive {
        error: Error,
        msg_handles: Vec<MessageHandle>,
    },
    Terminal(Error),
}

impl MapBatchTask {
    /// Executes the batch map operation.
    /// Returns an error if any message in the batch fails to be processed.
    pub async fn execute(mut self) -> std::result::Result<(), BatchMapTaskError> {
        // Store parent message info for each message before sending to UDF
        let parent_infos: Vec<ParentMessageInfo> = self
            .msg_handles
            .iter()
            .map(|rm| rm.message().into())
            .collect();

        let results = {
            // Create per-message map spans via the OTel SDK API.
            // Each span's parent is that message's `vertex.process` context (from
            // sys_metadata["tracing"]). We inject the map span context into
            // sys_metadata["tracing_udf"] so the UDF creates its processing span as a child.
            // Lexical scope ends spans after the batch UDF call.
            //
            // Invariant: tracing_udf is removed from result messages below; on error, input
            // messages are dropped, so tracing_udf never propagates further.
            //
            // When no OTel layer is registered, `inject_stage_span` returns non-recording spans
            // and the sys_metadata copy-on-write is skipped — no need to gate this call.
            let _stage_spans = otel::inject_stage_spans!(
                self.msg_handles.iter_mut().map(MessageHandle::message_mut),
                otel::TraceTopology::current(),
                otel::TraceStage::Map,
            );

            // Convert Messages to MapRequests
            let requests: Vec<MapRequest> = self
                .msg_handles
                .iter()
                .map(|rm| rm.message().clone().into())
                .collect();

            // Update read metrics for each request
            for _ in &requests {
                update_udf_read_metric(self.is_mono_vertex);
            }

            // Call the UDF and get results directly
            self.mapper.batch(requests, self.cln_token).await
        };

        if let Some(error) = results.iter().find_map(|result| match result {
            Err(Error::UdfRedrive(status)) => Some(Error::UdfRedrive(status.clone())),
            _ => None,
        }) {
            return Err(BatchMapTaskError::Redrive {
                error,
                msg_handles: self.msg_handles,
            });
        }

        for (result, (msg_handle, parent_info)) in results
            .into_iter()
            .zip(self.msg_handles.into_iter().zip(parent_infos))
        {
            match result {
                Ok(results) => {
                    // Convert raw results to Messages using parent info.
                    // Remove tracing_udf from each result (map stage is done).
                    let mapped_messages: Vec<Message> = results
                        .into_iter()
                        .enumerate()
                        .map(|(i, result)| {
                            let mut mapped_msg: Message =
                                UserDefinedMessage(result, &parent_info, i as i32).into();
                            // No-op when no `tracing_udf` was injected (noop tracer path).
                            mapped_msg.strip_tracing_udf();
                            mapped_msg
                        })
                        .collect();

                    let offset = parent_info.offset.clone();

                    update_udf_write_metric(
                        self.is_mono_vertex,
                        &parent_info,
                        mapped_messages.len() as u64,
                    );

                    self.tracker
                        .serving_update(
                            &offset,
                            mapped_messages.iter().map(|m| m.tags.clone()).collect(),
                        )
                        .await
                        .map_err(BatchMapTaskError::Terminal)?;

                    for mapped_message in mapped_messages {
                        // Each downstream handle shares the original ack tracking — ACK is
                        // deferred until all mapped messages are written to ISB/sink.
                        let downstream_handle = msg_handle.with_message(mapped_message);

                        // Try to bypass the message. If bypassed, try_bypass takes ownership and returns None.
                        // If not bypassed, it returns Some(downstream_handle) for us to send downstream.
                        let downstream_handle = if let Some(ref bypass_router) = self.bypass_router
                        {
                            match bypass_router
                                .try_bypass(downstream_handle)
                                .await
                                .expect("failed to send message to bypass channel")
                            {
                                Some(msg) => msg,
                                None => continue, // Message was bypassed, move to next
                            }
                        } else {
                            downstream_handle
                        };

                        self.output_tx
                            .send(downstream_handle)
                            .await
                            .expect("failed to send response");
                    }

                    // Decrement the original ref_count for this message now that all downstream
                    // handles have been created and sent.
                    mark_success!(msg_handle);
                }
                Err(e) => {
                    error!(err=?e, "failed to map message");
                    mark_failed!(msg_handle, &e);
                    return Err(BatchMapTaskError::Terminal(e));
                }
            }
        }

        Ok(())
    }
}

/// UserDefinedBatchMap is a grpc client that sends batch requests to the map server
/// and forwards the responses.
#[derive(Clone)]
pub(in crate::mapper) struct UserDefinedBatchMap {
    batch_size: usize,
    connection: Arc<AsyncMutex<BatchMapConnection>>,
    reconnect_config: Option<UdfReconnectConfig>,
}

struct BatchMapConnection {
    read_tx: mpsc::Sender<MapRequest>,
    senders: Arc<Mutex<BatchSenderMapState>>,
    _handle: Arc<AbortOnDropHandle<()>>,
}

impl UserDefinedBatchMap {
    /// Performs handshake with the server and creates a new UserDefinedBatchMap.
    pub(in crate::mapper) async fn new(
        batch_size: usize,
        mut client: MapClient<Channel>,
        reconnect_config: Option<UdfReconnectConfig>,
    ) -> Result<Self> {
        let connection = Self::create_connection(batch_size, &mut client).await?;

        Ok(Self {
            batch_size,
            connection: Arc::new(AsyncMutex::new(connection)),
            reconnect_config,
        })
    }

    async fn create_connection(
        batch_size: usize,
        client: &mut MapClient<Channel>,
    ) -> Result<BatchMapConnection> {
        let (read_tx, read_rx) = mpsc::channel(batch_size);
        let resp_stream = create_response_stream(read_tx.clone(), read_rx, client).await?;

        // map to track the oneshot response sender for each request
        let sender_map = Arc::new(Mutex::new(BatchSenderMapState::default()));

        // background task to receive responses from the server and send them to the appropriate
        // oneshot response sender based on the id
        let sender_map_clone = Arc::clone(&sender_map);
        let handle = tokio::spawn(async move {
            Self::receive_batch_responses(sender_map_clone, resp_stream).await;
        });

        Ok(BatchMapConnection {
            read_tx,
            senders: sender_map,
            _handle: Arc::new(AbortOnDropHandle::new(handle)),
        })
    }

    /// Broadcasts a batch map gRPC error to all pending senders and records error metrics.
    fn broadcast_error(sender_map: &Arc<Mutex<BatchSenderMapState>>, error: tonic::Status) {
        let senders = {
            let mut sender_guard = sender_map.lock().expect("failed to acquire poisoned lock");
            sender_guard.closed = true;
            std::mem::take(&mut sender_guard.map)
        };

        let error = map_redrive_error(error);
        for (_, sender) in senders {
            let _ = sender.send(Err(error.clone()));
            update_udf_error_metric(is_mono_vertex())
        }
    }

    /// receive responses from the server and gets the corresponding oneshot response sender from the map
    /// and sends the response.
    async fn receive_batch_responses(
        sender_map: Arc<Mutex<BatchSenderMapState>>,
        mut resp_stream: Streaming<MapResponse>,
    ) {
        while let Some(resp) = resp_stream.next().await {
            match resp {
                Ok(resp) => {
                    if let Some(map::TransmissionStatus { eot: true }) = resp.status {
                        if !sender_map
                            .lock()
                            .expect("failed to acquire poisoned lock")
                            .map
                            .is_empty()
                        {
                            error!(
                                "received EOT but not all responses have been received, gracefully exiting"
                            );
                            Self::broadcast_error(
                                &sender_map,
                                Status::with_details(
                                    tonic::Code::Internal,
                                    "UDF_PARTIAL_RESPONSE(batch_map)",
                                    Bytes::from_static(
                                        b"received End-Of-Transmission (EOT) before all responses are received from the batch map. \
                                        This indicates that there is a bug in the user-code. Please check whether you are accidentally \
                                        skipping the messages.",
                                    ),
                                ),
                            );
                            return;
                        }
                        update_udf_process_time_metric(is_mono_vertex());
                        continue;
                    }

                    if let Err(e) = Self::process_response(&sender_map, resp) {
                        warn!("received error while processing batch response: {}", e);
                    }
                }
                Err(e) => {
                    error!(?e, "Error reading message from batch map gRPC stream");
                    Self::broadcast_error(&sender_map, e);
                }
            }
        }

        Self::broadcast_error(
            &sender_map,
            tonic::Status::aborted("receiver stream dropped"),
        );
    }

    /// Processes the response from the server and sends it to the appropriate oneshot sender
    /// based on the message id entry in the map.
    fn process_response(
        sender_map: &Arc<Mutex<BatchSenderMapState>>,
        resp: MapResponse,
    ) -> Result<()> {
        let msg_id = resp.id;

        let sender_entry = {
            sender_map
                .lock()
                .expect("failed to acquire poisoned lock")
                .map
                .remove(&msg_id)
        };

        if let Some(sender) = sender_entry {
            if sender.send(Ok(resp.results)).is_err() {
                return Err(Error::Mapper(format!(
                    "Failed to send server response from receiver to batch task for ID: {}",
                    msg_id
                )));
            };
        } else {
            return Err(Error::Mapper(format!(
                "No such req/resp ID found in batch ResponseSenderMap: {}",
                msg_id
            )));
        }
        Ok(())
    }

    /// Sends a batch of messages to the UDF and returns the raw response results.
    /// Returns early with an error if any request fails or if the cancellation token is cancelled.
    pub(in crate::mapper) async fn batch(
        &self,
        requests: Vec<MapRequest>,
        cln_token: CancellationToken,
    ) -> Vec<Result<BatchMapResponse>> {
        let results = self.batch_once(requests.clone(), cln_token.clone()).await;
        if results
            .iter()
            .any(|result| matches!(result, Err(Error::UdfRedrive(_))))
        {
            if let Err(e) = self.reconnect().await {
                return vec![Err(e)];
            }
            return self.batch_once(requests, cln_token).await;
        }
        results
    }

    async fn batch_once(
        &self,
        requests: Vec<MapRequest>,
        cln_token: CancellationToken,
    ) -> Vec<Result<BatchMapResponse>> {
        let (senders, receivers): (Vec<_>, Vec<_>) =
            requests.iter().map(|_| oneshot::channel()).unzip();
        let (sender_map, read_tx) = {
            let connection = self.connection.lock().await;
            (Arc::clone(&connection.senders), connection.read_tx.clone())
        };

        for (request, sender) in requests.into_iter().zip(senders) {
            let key = request.id.clone();

            // Move the senders_guard out of the scope to drop the guard when done.
            // Do this before we send the message to the server to avoid the race condition
            // where the server processes the message faster than the corresponding sender
            // is added to the SenderMap.
            {
                let mut senders_guard = sender_map.lock().expect("failed to acquire poisoned lock");
                if !senders_guard.closed {
                    senders_guard.map.insert(key.clone(), sender);
                } else {
                    return vec![Err(map_redrive_error(Status::unavailable(
                        "batch map stream closed",
                    )))];
                }
            };

            // send the message to the server
            if let Err(e) = read_tx.send(request).await {
                warn!(?e, "Failed to send message to server");
                // We should ideally remove the resp.id from the SenderMap to avoid potential
                // memory leaks as well as to avoid holding the corresponding receiver waiting
                let sender_entry = {
                    sender_map
                        .lock()
                        .expect("failed to acquire poisoned lock")
                        .map
                        .remove(&key)
                };

                // We should send error on the sender so the first receiver receiving the error
                // returns early with the collected results.
                let error = map_redrive_error(Status::unavailable(format!(
                    "failed to send message to batch map server: {e}"
                )));
                if let Some(sender) = sender_entry {
                    let _ = sender
                        .send(Err(error.clone()))
                        .inspect_err(|_| warn!("failed to send error to oneshot receiver"));
                }
                Self::drain_senders(&sender_map, error.clone());
                return vec![Err(error)];
            }
        }

        // send eot request
        if let Err(e) = read_tx
            .send(MapRequest {
                request: None,
                id: "".to_string(),
                handshake: None,
                status: Some(map::TransmissionStatus { eot: true }),
            })
            .await
        {
            error!(
                ?e,
                "Failed to send eot request to server, batch map operation should have failed"
            );
            let error = map_redrive_error(Status::unavailable(format!(
                "failed to send eot request to batch map server: {e}"
            )));
            Self::drain_senders(&sender_map, error.clone());
            return vec![Err(error)];
        }

        let mut results = Vec::with_capacity(receivers.len());
        for receiver in receivers {
            let result = tokio::select! {
                recv_result = receiver => {
                    recv_result.unwrap_or_else(|e| Err(Error::ActorPatternRecv(e.to_string())))
                }
                _ = cln_token.cancelled() => {
                    return vec![Err(Error::Mapper("batch map operation cancelled".to_string()))];
                }
            };

            // Return early on first error
            if result.is_err() {
                return vec![result];
            }
            results.push(result);
        }

        results
    }

    async fn reconnect(&self) -> Result<()> {
        let Some(reconnect_config) = &self.reconnect_config else {
            return Err(map_redrive_error(Status::unavailable(
                "batch map reconnect config missing",
            )));
        };

        let mut connection = self.connection.lock().await;
        Self::drain_senders(
            &connection.senders,
            map_redrive_error(Status::unavailable("batch map reconnecting")),
        );

        let mut client = reconnect_mapper_client(reconnect_config).await?;

        *connection =
            grpc_error_to_redrive(Self::create_connection(self.batch_size, &mut client).await)?;

        Ok(())
    }

    fn drain_senders(sender_map: &Arc<Mutex<BatchSenderMapState>>, error: Error) {
        let senders = {
            let mut sender_guard = sender_map.lock().expect("failed to acquire poisoned lock");
            sender_guard.closed = true;
            std::mem::take(&mut sender_guard.map)
        };

        for (_, sender) in senders {
            let _ = sender.send(Err(error.clone()));
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::error::Error as MapError;
    use crate::mapper::map::batch::{BatchSenderMapState, UserDefinedBatchMap};
    use crate::shared::grpc::create_rpc_channel;
    use numaflow::batchmap;
    use numaflow::batchmap::Server;
    use numaflow::shared::ServerExtras;
    use numaflow_pb::clients::map::map_client::MapClient;
    use numaflow_pb::clients::map::{MapRequest, MapResponse};
    use std::error::Error;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;
    use tempfile::TempDir;
    use tokio::sync::{mpsc, oneshot};
    use tokio_util::sync::CancellationToken;
    use tokio_util::task::AbortOnDropHandle;

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

    struct PartialBatchMap;

    #[tonic::async_trait]
    impl batchmap::BatchMapper for PartialBatchMap {
        async fn batchmap(
            &self,
            mut input: tokio::sync::mpsc::Receiver<batchmap::Datum>,
        ) -> Vec<batchmap::BatchResponse> {
            let Some(datum) = input.recv().await else {
                return vec![];
            };

            let mut response = batchmap::BatchResponse::from_id(datum.id);
            response.append(batchmap::Message {
                keys: Some(datum.keys),
                value: datum.value,
                tags: None,
            });
            // Intentionally skip the rest of the batch to force partial EOT.
            vec![response]
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

        let client = UserDefinedBatchMap::new(
            500,
            MapClient::new(create_rpc_channel(sock_file).await?),
            None,
        )
        .await?;

        // Create MapRequests directly instead of Messages
        let requests = vec![
            numaflow_pb::clients::map::MapRequest {
                request: Some(numaflow_pb::clients::map::map_request::Request {
                    keys: vec!["first".into()],
                    value: "hello".into(),
                    event_time: None,
                    watermark: None,
                    headers: Default::default(),
                    metadata: None,
                }),
                id: "0".to_string(),
                handshake: None,
                status: None,
            },
            numaflow_pb::clients::map::MapRequest {
                request: Some(numaflow_pb::clients::map::map_request::Request {
                    keys: vec!["second".into()],
                    value: "world".into(),
                    event_time: None,
                    watermark: None,
                    headers: Default::default(),
                    metadata: None,
                }),
                id: "1".to_string(),
                handshake: None,
                status: None,
            },
        ];

        let cln_token = tokio_util::sync::CancellationToken::new();
        let results =
            tokio::time::timeout(Duration::from_secs(2), client.batch(requests, cln_token)).await?;

        assert_eq!(results.len(), 2);
        assert!(
            results
                .first()
                .expect("Expected at least one result")
                .is_ok()
        );
        assert!(results.get(1).expect("Expected second result").is_ok());
        assert_eq!(
            results
                .first()
                .expect("Expected at least one result")
                .as_ref()
                .expect("Expected Ok result")
                .len(),
            1
        );
        assert_eq!(
            results
                .get(1)
                .expect("Expected second result")
                .as_ref()
                .expect("Expected Ok result")
                .len(),
            1
        );

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

    #[tokio::test]
    async fn batch_partial_eot_returns_udf_redrive() -> Result<(), Box<dyn Error>> {
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let tmp_dir = TempDir::new()?;
        let sock_file = tmp_dir.path().join("partial_batch_map.sock");
        let server_info_file = tmp_dir.path().join("partial_batch_map-server-info");

        let server_info = server_info_file.clone();
        let server_socket = sock_file.clone();
        let handle = tokio::spawn(async move {
            Server::new(PartialBatchMap)
                .with_socket_file(server_socket)
                .with_server_info_file(server_info)
                .start_with_shutdown(shutdown_rx)
                .await
                .expect("server failed");
        });

        tokio::time::sleep(Duration::from_millis(100)).await;

        let client = UserDefinedBatchMap::new(
            500,
            MapClient::new(create_rpc_channel(sock_file).await?),
            None,
        )
        .await?;

        let request = |id: &str| MapRequest {
            request: Some(numaflow_pb::clients::map::map_request::Request {
                keys: vec!["k".into()],
                value: id.as_bytes().to_vec(),
                event_time: None,
                watermark: None,
                headers: Default::default(),
                metadata: None,
            }),
            id: id.to_string(),
            handshake: None,
            status: None,
        };

        let results = client
            .batch_once(vec![request("0"), request("1")], CancellationToken::new())
            .await;
        let err = results
            .into_iter()
            .next()
            .expect("expected one error result")
            .expect_err("partial EOT should be recoverable");
        assert!(matches!(err, MapError::UdfRedrive(_)));

        drop(client);
        shutdown_tx
            .send(())
            .expect("failed to send shutdown signal");
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(handle.is_finished(), "expected server to shut down");

        Ok(())
    }

    fn make_response(id: &str) -> MapResponse {
        MapResponse {
            results: vec![],
            id: id.to_string(),
            handshake: None,
            status: None,
        }
    }

    #[tokio::test]
    async fn process_response_returns_error_when_no_sender_entry() {
        let sender_map = Arc::new(Mutex::new(BatchSenderMapState::default()));

        let result = UserDefinedBatchMap::process_response(&sender_map, make_response("missing"));

        let err = result.expect_err("expected error when sender entry missing");
        assert!(matches!(err, MapError::Mapper(_)));
        assert!(
            err.to_string()
                .contains("No such req/resp ID found in batch ResponseSenderMap"),
            "unexpected error message: {err}"
        );
    }

    #[tokio::test]
    async fn process_response_returns_error_when_oneshot_send_fails() {
        let sender_map = Arc::new(Mutex::new(BatchSenderMapState::default()));
        let (tx, rx) = oneshot::channel();
        // Drop the receiver so the next send on tx fails.
        drop(rx);
        sender_map.lock().unwrap().map.insert("0".to_string(), tx);

        let result = UserDefinedBatchMap::process_response(&sender_map, make_response("0"));

        let err = result.expect_err("expected error when oneshot send fails");
        assert!(matches!(err, MapError::Mapper(_)));
        assert!(
            err.to_string()
                .contains("Failed to send server response from receiver to batch task"),
            "unexpected error message: {err}"
        );
    }

    #[tokio::test]
    async fn broadcast_error_marks_closed_and_errors_all_senders() {
        let sender_map = Arc::new(Mutex::new(BatchSenderMapState::default()));
        let (tx_a, rx_a) = oneshot::channel();
        let (tx_b, rx_b) = oneshot::channel();
        {
            let mut guard = sender_map.lock().unwrap();
            guard.map.insert("a".to_string(), tx_a);
            guard.map.insert("b".to_string(), tx_b);
        }

        UserDefinedBatchMap::broadcast_error(&sender_map, tonic::Status::aborted("test"));

        {
            let guard = sender_map.lock().unwrap();
            assert!(guard.closed, "broadcast_error should set closed = true");
            assert!(guard.map.is_empty(), "all senders should be drained");
        }

        for rx in [rx_a, rx_b] {
            let received = rx.await.expect("oneshot sender should have delivered");
            let err = received.expect_err("expected Err variant");
            assert!(matches!(err, MapError::UdfRedrive(_)));
        }
    }

    #[tokio::test]
    async fn batch_method_cleans_up_on_read_tx_send_failure() {
        // Build a UserDefinedBatchMap whose read_tx receiver has been dropped,
        // so that read_tx.send(..) inside `batch` fails immediately.
        let (read_tx, read_rx) = mpsc::channel::<MapRequest>(10);
        drop(read_rx);

        let dummy_handle = tokio::spawn(async {});
        let _abort_handle = Arc::new(AbortOnDropHandle::new(dummy_handle));

        let senders = Arc::new(Mutex::new(BatchSenderMapState::default()));
        let mapper = UserDefinedBatchMap {
            batch_size: 10,
            connection: Arc::new(tokio::sync::Mutex::new(super::BatchMapConnection {
                read_tx,
                senders: Arc::clone(&senders),
                _handle: _abort_handle,
            })),
            reconnect_config: None,
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

        let results = mapper
            .batch_once(vec![request], CancellationToken::new())
            .await;

        // batch() returns vec![error] on first failure (early return).
        assert_eq!(results.len(), 1);
        let err = results
            .into_iter()
            .next()
            .unwrap()
            .expect_err("expected UdfRedrive error from batch()");
        assert!(matches!(err, MapError::UdfRedrive(_)));
        assert!(
            err.to_string()
                .contains("failed to send message to batch map server"),
            "unexpected error message: {err}"
        );

        // The senders map must no longer contain the failed request id.
        assert!(
            !senders.lock().unwrap().map.contains_key("42"),
            "senders map should be cleaned up on read_tx send failure"
        );
    }

    #[tokio::test]
    async fn batch_eot_send_failure_returns_udf_redrive() {
        let (read_tx, mut read_rx) = mpsc::channel::<MapRequest>(1);
        let receiver_handle = tokio::spawn(async move {
            let _ = read_rx.recv().await;
        });

        let dummy_handle = tokio::spawn(async {});
        let abort_handle = Arc::new(AbortOnDropHandle::new(dummy_handle));

        let senders = Arc::new(Mutex::new(BatchSenderMapState::default()));
        let mapper = UserDefinedBatchMap {
            batch_size: 10,
            connection: Arc::new(tokio::sync::Mutex::new(super::BatchMapConnection {
                read_tx,
                senders: Arc::clone(&senders),
                _handle: abort_handle,
            })),
            reconnect_config: None,
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

        let results = mapper
            .batch_once(vec![request], CancellationToken::new())
            .await;
        let err = results
            .into_iter()
            .next()
            .expect("expected one error result")
            .expect_err("expected UdfRedrive error from eot send failure");
        assert!(matches!(err, MapError::UdfRedrive(_)));
        assert!(
            err.to_string()
                .contains("failed to send eot request to batch map server"),
            "unexpected error message: {err}"
        );
        assert!(senders.lock().unwrap().map.is_empty());

        receiver_handle.await.expect("receiver task should finish");
    }
}
