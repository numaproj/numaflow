use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;

use crate::config::is_mono_vertex;
use crate::error::{Error, Result};
use crate::message::{Message, MessageHandle};
use crate::shared::grpc::UdfReconnectConfig;
use crate::shared::otel;
use crate::{mark_failed, mark_success};
use numaflow_pb::clients::map::{self, MapRequest, MapResponse, map_client::MapClient};
use tokio::sync::{Mutex as AsyncMutex, OwnedSemaphorePermit, mpsc, oneshot};
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;
use tokio_util::task::AbortOnDropHandle;
use tonic::Status;
use tonic::Streaming;
use tonic::transport::Channel;
use tracing::{error, warn};

use super::{
    ParentMessageInfo, SharedMapTaskContext, UserDefinedMessage, create_response_stream,
    grpc_error_to_redrive, map_redrive_error, reconnect_mapper_client, update_udf_error_metric,
    update_udf_read_metric, update_udf_write_metric, wait_before_map_redrive,
};

/// Type alias for the response - raw results from the UDF
pub(in crate::mapper) type UnaryMapResponse = Vec<map::map_response::Result>;

/// Type aliases for HashMap used to track the oneshot response sender for each request keyed by
/// message id.
type ResponseSenderMap = HashMap<String, oneshot::Sender<Result<UnaryMapResponse>>>;

/// Shared state for tracking batch map senders between the sender and the receiver tasks.
/// We have BiDi gRPC stream so we have 2 different set of tasks for sending and receiving.
#[derive(Default)]
pub(in crate::mapper) struct UnarySenderMapState {
    /// Map of oneshot response senders keyed by message id.
    map: ResponseSenderMap,
    /// Flag to indicate whether the rx task has closed the stream and cleared the `map`.
    /// This is because `tx.send()` could return `Ok()` even after the receiver task has closed the
    /// stream.
    closed: bool,
}

/// MapUnaryTask encapsulates all the context needed to execute a unary map operation per message.
pub(in crate::mapper) struct MapUnaryTask {
    pub mapper: UserDefinedUnaryMap,
    /// Permit to achieve structured concurrency by ensuring we do not exceed the concurrency limit
    /// and all the tasks are cleaned up when the component is shutting down.
    pub permit: OwnedSemaphorePermit,
    pub msg_handle: MessageHandle,
    pub shared_ctx: Arc<SharedMapTaskContext>,
}

impl MapUnaryTask {
    /// Spawns the unary map task as a tokio task per [MapUnaryTask].
    /// The task will process the message through the UDF and send results downstream.
    pub fn spawn(self) {
        tokio::spawn(async move {
            self.execute().await;
        });
    }

    /// Executes the unary map operation.
    ///
    /// Creates a per-message `numaflow.{topology}.map` span via the OTel SDK API, scoped to the
    /// UDF call. The span's context is written into `sys_metadata["tracing_udf"]` so the UDF
    /// sees it as the parent; we strip the key from result messages before they leave the mapper.
    /// When no OTel layer is registered, span creation and metadata writes are skipped via the
    /// noop tracer path.
    async fn execute(mut self) {
        // Hold the permit until the task completes
        let _permit = self.permit;

        // Store parent message info before sending to UDF
        // parent_info contains offset, so we don't need to clone it separately
        let parent_info: ParentMessageInfo = self.msg_handle.message().into();

        let results = {
            // RAII map span: ends when this scope exits.
            let _stage_span = otel::inject_stage_span(
                self.msg_handle.message_mut(),
                otel::TraceTopology::current(),
                otel::TraceStage::Map,
            );

            let request: MapRequest = self.msg_handle.message().clone().into();
            update_udf_read_metric(self.shared_ctx.is_mono_vertex);

            loop {
                match self
                    .mapper
                    .unary(request.clone(), self.shared_ctx.hard_shutdown_token.clone())
                    .await
                {
                    Ok(results) => break results,
                    Err(Error::UdfRedrive(status)) => {
                        warn!(?status, offset = ?parent_info.offset, "redriving unary map message after UDF reconnect");
                        if wait_before_map_redrive(&self.shared_ctx.hard_shutdown_token)
                            .await
                            .is_err()
                        {
                            return;
                        }
                    }
                    Err(e) => {
                        error!(?e, offset = ?parent_info.offset, "failed to map message");
                        mark_failed!(self.msg_handle, &e, None);
                        let _ = self.shared_ctx.error_tx.send(e).await;
                        return;
                    }
                }
            }
        };

        // Convert raw results to Messages using parent info.
        // The UserDefinedMessage -> Message conversion copies sys_metadata from parent (so
        // vertex.process context stays in "tracing"). We strip tracing_udf here because the map
        // stage is done — downstream sink will inject its own tracing_udf. No-op when no key
        // was injected.
        let results_len = results.len();
        let mut mapped_messages: Vec<Message> = Vec::with_capacity(results_len);
        for (i, result) in results.into_iter().enumerate() {
            let mut mapped_msg: Message = UserDefinedMessage(result, &parent_info, i as i32).into();
            mapped_msg.strip_tracing_udf();
            mapped_messages.push(mapped_msg);
        }

        update_udf_write_metric(
            self.shared_ctx.is_mono_vertex,
            &parent_info,
            mapped_messages.len() as u64,
        );

        // Update the tracker with the number of messages sent
        // Use parent_info.offset instead of cloning offset separately
        self.shared_ctx
            .tracker
            .serving_update(
                &parent_info.offset,
                mapped_messages.iter().map(|m| m.tags.clone()).collect(),
            )
            .await
            .expect("failed to update tracker");

        for mapped_message in mapped_messages {
            // Each downstream handle shares the original ack tracking — ACK is deferred until
            // all mapped messages are written to ISB/sink.
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
                    None => {
                        // Message was bypassed (already acked by bypass_router), move to next.
                        continue;
                    }
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

        // Decrement the original ref_count now that we've accounted for all downstream messages.
        // The original msg_handle held ref_count=1; mark_success brings it to 0 contribution,
        // and the downstream handles will each call mark_success when written to ISB/sink.
        mark_success!(self.msg_handle);
    }
}

/// UserDefinedUnaryMap is a grpc client that sends unary requests to the map server
/// and forwards the responses.
#[derive(Clone)]
pub(in crate::mapper) struct UserDefinedUnaryMap {
    batch_size: usize,
    connection: Arc<AsyncMutex<UnaryMapConnection>>,
    reconnect_config: Option<UdfReconnectConfig>,
}

struct UnaryMapConnection {
    generation: u64,
    read_tx: mpsc::Sender<MapRequest>,
    senders: Arc<Mutex<UnarySenderMapState>>,
    _handle: Arc<AbortOnDropHandle<()>>,
}

impl UserDefinedUnaryMap {
    /// Performs handshake with the server and creates a new UserDefinedUnaryMap.
    pub(in crate::mapper) async fn new(
        batch_size: usize,
        mut client: MapClient<Channel>,
        reconnect_config: Option<UdfReconnectConfig>,
    ) -> Result<Self> {
        let connection = Self::create_connection(batch_size, &mut client, 0).await?;

        Ok(Self {
            batch_size,
            connection: Arc::new(AsyncMutex::new(connection)),
            reconnect_config,
        })
    }

    async fn create_connection(
        batch_size: usize,
        client: &mut MapClient<Channel>,
        generation: u64,
    ) -> Result<UnaryMapConnection> {
        let (read_tx, read_rx) = mpsc::channel(batch_size);
        let resp_stream = create_response_stream(read_tx.clone(), read_rx, client).await?;

        // map to track the oneshot sender for each request
        let sender_map = Arc::new(Mutex::new(UnarySenderMapState::default()));

        // background task to receive responses from the server and send them to the appropriate
        // oneshot sender based on the message id
        let sender_map_clone = Arc::clone(&sender_map);
        let handle = tokio::spawn(async move {
            Self::receive_unary_responses(sender_map_clone, resp_stream).await;
        });

        Ok(UnaryMapConnection {
            generation,
            read_tx,
            senders: sender_map,
            _handle: Arc::new(AbortOnDropHandle::new(handle)),
        })
    }

    /// Broadcasts a unary gRPC error to all pending senders and records error metrics.
    fn broadcast_error(sender_map: &Arc<Mutex<UnarySenderMapState>>, error: tonic::Status) {
        let senders = {
            let mut sender_guard = sender_map.lock().expect("failed to acquire poisoned lock");
            sender_guard.closed = true;
            std::mem::take(&mut sender_guard.map)
        };

        let error = map_redrive_error(error);
        for (_, sender) in senders {
            let _ = sender.send(Err(error.clone()));
            update_udf_error_metric(is_mono_vertex());
        }
    }

    /// receive responses from the server and gets the corresponding oneshot response sender from the map
    /// and sends the response.
    async fn receive_unary_responses(
        sender_map: Arc<Mutex<UnarySenderMapState>>,
        mut resp_stream: Streaming<MapResponse>,
    ) {
        while let Some(resp) = resp_stream.next().await {
            match resp {
                Ok(resp) => {
                    if let Err(e) = Self::process_unary_response(&sender_map, resp) {
                        error!(
                            "received error while processing unary response: {}. \
                            Exiting receiver task",
                            e
                        );
                    }
                }
                Err(e) => {
                    error!(?e, "Error reading message from unary map gRPC stream");
                    Self::broadcast_error(&sender_map, e);
                }
            };
        }

        // broadcast error for all pending senders that might've gotten added while the stream was draining
        Self::broadcast_error(
            &sender_map,
            tonic::Status::aborted("receiver stream dropped"),
        );
    }

    /// Sends a message to the UDF and returns the raw response results.
    /// If the cancellation token is cancelled while waiting for the response,
    /// returns an error indicating the operation was cancelled.
    pub(in crate::mapper) async fn unary(
        &self,
        request: MapRequest,
        cln_token: CancellationToken,
    ) -> Result<UnaryMapResponse> {
        let (generation, result) = self
            .unary_once_with_generation(request.clone(), cln_token.clone())
            .await;
        match result {
            Err(Error::UdfRedrive(_)) => {
                self.reconnect(generation).await?;
                self.unary_once(request, cln_token).await
            }
            result => result,
        }
    }

    async fn unary_once(
        &self,
        request: MapRequest,
        cln_token: CancellationToken,
    ) -> Result<UnaryMapResponse> {
        self.unary_once_with_generation(request, cln_token).await.1
    }

    async fn unary_once_with_generation(
        &self,
        request: MapRequest,
        cln_token: CancellationToken,
    ) -> (u64, Result<UnaryMapResponse>) {
        let (tx, rx) = oneshot::channel();
        let key = request.id.clone();
        let (generation, senders, read_tx) = {
            let connection = self.connection.lock().await;
            (
                connection.generation,
                Arc::clone(&connection.senders),
                connection.read_tx.clone(),
            )
        };

        // Move the senders_guard out of the scope to drop the guard when done
        // Do this before we send the message to the server to avoid the race condition
        // where the server processes the message faster than the corresponding sender
        // is added to the SenderMap.
        {
            let mut senders_guard = senders.lock().expect("failed to acquire poisoned lock");
            if !senders_guard.closed {
                senders_guard.map.insert(key.clone(), tx);
            } else {
                return (
                    generation,
                    Err(map_redrive_error(Status::unavailable(
                        "unary map stream closed",
                    ))),
                );
            }
        };

        // send message to the server
        if let Err(e) = read_tx.send(request).await {
            error!(?e, "Failed to send message to server");
            // We should remove the resp.id from the SenderMap to avoid potential
            // memory leaks as well as to avoid holding the corresponding receiver waiting.
            // We don't care about sending the error on the sender popped
            // from the map since we're returning early with error anyway.
            {
                let _ = senders
                    .lock()
                    .expect("failed to acquire poisoned lock")
                    .map
                    .remove(&key);
            };
            return (
                generation,
                Err(map_redrive_error(Status::unavailable(format!(
                    "failed to send message to unary map server: {e}"
                )))),
            );
        }

        let result = tokio::select! {
            result = rx => {
                // we don't have to remove the sender from the map, because the response handler
                // will do it.
                result.unwrap_or_else(|e: oneshot::error::RecvError| {
                    Err(Error::ActorPatternRecv(e.to_string()))
                })
            }
            _ = cln_token.cancelled() => {
                // Remove the sender from the map since we're cancelling
                senders.lock().expect("failed to acquire poisoned lock").map.remove(&key);
                Err(Error::Mapper("unary map operation cancelled".to_string()))
            }
        };
        (generation, result)
    }

    async fn reconnect(&self, failed_generation: u64) -> Result<()> {
        let Some(reconnect_config) = &self.reconnect_config else {
            return Err(map_redrive_error(Status::unavailable(
                "unary map reconnect config missing",
            )));
        };

        let mut connection = self.connection.lock().await;
        if connection.generation != failed_generation {
            return Ok(());
        }
        Self::drain_senders(
            &connection.senders,
            map_redrive_error(Status::unavailable("unary map reconnecting")),
        );
        let next_generation = connection.generation.saturating_add(1);

        let mut client = reconnect_mapper_client(reconnect_config).await?;

        *connection = grpc_error_to_redrive(
            Self::create_connection(self.batch_size, &mut client, next_generation).await,
        )?;

        Ok(())
    }

    fn drain_senders(sender_map: &Arc<Mutex<UnarySenderMapState>>, error: Error) {
        let senders = {
            let mut sender_guard = sender_map.lock().expect("failed to acquire poisoned lock");
            sender_guard.closed = true;
            std::mem::take(&mut sender_guard.map)
        };

        for (_, sender) in senders {
            let _ = sender.send(Err(error.clone()));
        }
    }

    /// Processes the response from the server and sends it to the appropriate oneshot sender
    /// based on the message id entry in the map.
    pub(super) fn process_unary_response(
        sender_map: &Arc<Mutex<UnarySenderMapState>>,
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
            if let Err(e) = sender.send(Ok(resp.results)) {
                return Err(Error::Mapper(format!(
                    "Failed to send server response from receiver to unary task \
                    for ID: {} with error: {:?}",
                    msg_id, e
                )));
            };
        } else {
            return Err(Error::Mapper(format!(
                "No such req/resp ID found in unary ResponseSenderMap: {}",
                msg_id
            )));
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::error::Error as MapError;
    use crate::mapper::map::unary::{UnarySenderMapState, UserDefinedUnaryMap};
    use crate::shared::grpc::create_rpc_channel;
    use numaflow::map;
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

    struct Cat;

    #[tonic::async_trait]
    impl map::Mapper for Cat {
        async fn map(&self, input: map::MapRequest) -> Vec<map::Message> {
            let message = map::Message::new(input.value)
                .with_keys(input.keys)
                .with_tags(vec![]);
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

        let client = UserDefinedUnaryMap::new(
            500,
            MapClient::new(create_rpc_channel(sock_file).await?),
            None,
        )
        .await?;

        // Create a MapRequest directly instead of a Message
        let request = numaflow_pb::clients::map::MapRequest {
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
        };

        let cln_token = CancellationToken::new();
        let results =
            tokio::time::timeout(Duration::from_secs(2), client.unary(request, cln_token)).await?;

        assert!(results.is_ok());
        assert_eq!(results?.len(), 1);

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

    fn make_response(id: &str) -> MapResponse {
        MapResponse {
            results: vec![],
            id: id.to_string(),
            handshake: None,
            status: None,
        }
    }

    #[tokio::test]
    async fn process_unary_response_returns_error_when_no_sender_entry() {
        let sender_map = Arc::new(Mutex::new(UnarySenderMapState::default()));

        let result =
            UserDefinedUnaryMap::process_unary_response(&sender_map, make_response("missing"));

        let err = result.expect_err("expected error when sender entry missing");
        assert!(matches!(err, MapError::Mapper(_)));
        assert!(
            err.to_string()
                .contains("No such req/resp ID found in unary ResponseSenderMap"),
            "unexpected error message: {err}"
        );
    }

    #[tokio::test]
    async fn process_unary_response_returns_error_when_oneshot_send_fails() {
        let sender_map = Arc::new(Mutex::new(UnarySenderMapState::default()));
        let (tx, rx) = oneshot::channel();
        // Drop the receiver so the next send on tx fails.
        drop(rx);
        sender_map.lock().unwrap().map.insert("0".to_string(), tx);

        let result = UserDefinedUnaryMap::process_unary_response(&sender_map, make_response("0"));

        let err = result.expect_err("expected error when oneshot send fails");
        assert!(matches!(err, MapError::Mapper(_)));
        assert!(
            err.to_string()
                .contains("Failed to send server response from receiver to unary task"),
            "unexpected error message: {err}"
        );
    }

    #[tokio::test]
    async fn broadcast_error_marks_closed_and_errors_all_senders() {
        let sender_map = Arc::new(Mutex::new(UnarySenderMapState::default()));
        let (tx_a, rx_a) = oneshot::channel();
        let (tx_b, rx_b) = oneshot::channel();
        {
            let mut guard = sender_map.lock().unwrap();
            guard.map.insert("a".to_string(), tx_a);
            guard.map.insert("b".to_string(), tx_b);
        }

        UserDefinedUnaryMap::broadcast_error(&sender_map, tonic::Status::aborted("test"));

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
    async fn test_map_unary_emits_nack() {
        use numaflow::map;

        struct NackMap;
        #[tonic::async_trait]
        impl map::Mapper for NackMap {
            async fn map(&self, _input: map::MapRequest) -> Vec<map::Message> {
                vec![map::Message::message_to_nack(Some(
                    numaflow::shared::NackOptions {
                        delay: Some(5000),
                        max_deliveries: Some(3),
                        reason: Some("udf nack".to_string()),
                    },
                ))]
            }
        }

        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let tmp_dir = TempDir::new().unwrap();
        let sock_file = tmp_dir.path().join("map.sock");
        let server_info_file = tmp_dir.path().join("map-server-info");
        let (ss, si) = (sock_file.clone(), server_info_file.clone());
        let handle = tokio::spawn(async move {
            map::Server::new(NackMap)
                .with_socket_file(ss)
                .with_server_info_file(si)
                .start_with_shutdown(shutdown_rx)
                .await
                .expect("server failed");
        });
        tokio::time::sleep(Duration::from_millis(100)).await;

        let client = UserDefinedUnaryMap::new(
            500,
            MapClient::new(create_rpc_channel(sock_file).await.unwrap()),
            None,
        )
        .await
        .unwrap();

        let request = numaflow_pb::clients::map::MapRequest {
            request: Some(numaflow_pb::clients::map::map_request::Request {
                keys: vec!["k".into()],
                value: "hello".into(),
                event_time: None,
                watermark: None,
                headers: Default::default(),
                metadata: None,
            }),
            id: "0".to_string(),
            handshake: None,
            status: None,
        };

        let results = client
            .unary(request, CancellationToken::new())
            .await
            .unwrap();
        let result = results.first().expect("one result");
        assert!(result.tags.contains(&"U+005C__NACK__".to_string()));
        assert_eq!(
            result.nack_options,
            Some(numaflow_pb::common::nack_options::NackOptions {
                reason: Some("udf nack".to_string()),
                max_deliveries: Some(3),
                delay: Some(5000),
            })
        );

        drop(client);
        shutdown_tx.send(()).expect("send shutdown");
        tokio::time::sleep(Duration::from_millis(50)).await;
        handle.await.expect("server task should finish cleanly");
    }

    #[tokio::test]
    async fn unary_method_cleans_up_on_read_tx_send_failure() {
        // Build a UserDefinedUnaryMap whose read_tx receiver has been dropped,
        // so that read_tx.send(..) inside `unary` fails immediately.
        let (read_tx, read_rx) = mpsc::channel::<MapRequest>(10);
        drop(read_rx);

        let dummy_handle = tokio::spawn(async {});
        let _abort_handle = Arc::new(AbortOnDropHandle::new(dummy_handle));

        let senders = Arc::new(Mutex::new(UnarySenderMapState::default()));
        let mapper = UserDefinedUnaryMap {
            batch_size: 10,
            connection: Arc::new(tokio::sync::Mutex::new(super::UnaryMapConnection {
                generation: 0,
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

        let result = mapper.unary_once(request, CancellationToken::new()).await;
        let err = result.expect_err("expected UdfRedrive error from unary()");
        assert!(matches!(err, MapError::UdfRedrive(_)));
        assert!(
            err.to_string()
                .contains("failed to send message to unary map server"),
            "unexpected error message: {err}"
        );

        // The senders map must no longer contain the failed request id.
        assert!(
            !senders.lock().unwrap().map.contains_key("42"),
            "senders map should be cleaned up on read_tx send failure"
        );
    }
}
