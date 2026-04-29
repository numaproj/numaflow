use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;

use crate::config::is_mono_vertex;
use crate::error::{Error, Result};
use crate::message::{Message, MessageHandle};
use crate::shared::otel;
use crate::{mark_failed, mark_success};
use numaflow_pb::clients::map::{self, MapRequest, MapResponse, map_client::MapClient};
use tokio::sync::{OwnedSemaphorePermit, mpsc, oneshot};
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;
use tokio_util::task::AbortOnDropHandle;
use tonic::Streaming;
use tonic::transport::Channel;
use tracing::{Instrument, error, warn};
use tracing_opentelemetry::OpenTelemetrySpanExt;

use super::{
    ParentMessageInfo, SharedMapTaskContext, UserDefinedMessage, create_response_stream,
    remove_tracing_udf_if_enabled, update_udf_error_metric, update_udf_read_metric,
    update_udf_write_metric,
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
    /// With distributed tracing, extracts the `vertex.process` context from
    /// `sys_metadata["tracing"]`, creates a topology-specific map span as its child,
    /// and instruments the actual UDF call with that span so the span duration covers
    /// the map stage.
    async fn execute(self) {
        // Pipeline wiring can reuse this pattern when map spans are enabled there.
        if is_mono_vertex() && otel::tracing_enabled() {
            let parent_cx =
                otel::parent_context_from_metadata(self.msg_handle.message().metadata.as_deref());

            let msg_id = self.msg_handle.message().offset.to_string();
            let map_span = tracing::info_span!(
                "numaflow.monovertex.map",
                otel.kind = "INTERNAL",
                { otel::ATTR_MESSAGING_SYSTEM } = "numaflow",
                { otel::ATTR_MESSAGING_OPERATION_NAME } = "map",
                { otel::ATTR_MESSAGING_MESSAGE_ID } = %msg_id,
                { otel::ATTR_NUMAFLOW_TOPOLOGY } = otel::TraceTopology::MonoVertex.as_str(),
                { otel::ATTR_NUMAFLOW_PIPELINE_NAME } = crate::config::get_pipeline_name(),
                { otel::ATTR_NUMAFLOW_VERTEX_NAME } = crate::config::get_vertex_name(),
            );
            let _ = map_span.set_parent(parent_cx);
            self.execute_inner().instrument(map_span).await;
        } else {
            self.execute_inner().await;
        }
    }

    /// The core unary map execution. When called under `.instrument(map_span)`, the current
    /// tracing context is the map span — we inject it into `sys_metadata["tracing_udf"]` so the
    /// UDF sees the map span as its parent. The key is removed from result messages before they
    /// leave the mapper.
    async fn execute_inner(mut self) {
        // Hold the permit until the task completes
        let _permit = self.permit;
        let tracing_enabled = is_mono_vertex() && otel::tracing_enabled();

        // Tracing: inject the current `map` span's context into
        // sys_metadata["tracing_udf"] so the UDF creates its processing span as a child.
        // Note: `sys_metadata["tracing"]` is NOT overwritten — it still holds `vertex.process`.
        // tracing_udf is written to the input message here and removed from every
        // result message below. On UDF error, we return early without producing result messages,
        // and the input message is dropped — so tracing_udf never propagates downstream.
        if tracing_enabled && let Some(ref mut metadata) = self.msg_handle.message_mut().metadata {
            let map_cx = tracing::Span::current().context();
            otel::inject_context_into_metadata(
                Arc::make_mut(metadata),
                otel::TRACING_UDF_METADATA_KEY,
                &map_cx,
            );
        }

        // Store parent message info before sending to UDF
        // parent_info contains offset, so we don't need to clone it separately
        let parent_info: ParentMessageInfo = self.msg_handle.message().into();

        let request: MapRequest = self.msg_handle.message().clone().into();
        update_udf_read_metric(self.shared_ctx.is_mono_vertex);

        // Call the UDF and get raw results
        let results = match self
            .mapper
            .unary(request, self.shared_ctx.hard_shutdown_token.clone())
            .await
        {
            Ok(results) => results,
            Err(e) => {
                error!(?e, offset = ?parent_info.offset, "failed to map message");
                mark_failed!(self.msg_handle, &e);
                let _ = self.shared_ctx.error_tx.send(e).await;
                return;
            }
        };

        // Convert raw results to Messages using parent info.
        // The UserDefinedMessage -> Message conversion copies sys_metadata from parent (so
        // vertex.process context stays in "tracing"). We remove tracing_udf here because the
        // map stage is done — downstream sink will inject its own tracing_udf.
        let results_len = results.len();
        let mut mapped_messages: Vec<Message> = Vec::with_capacity(results_len);
        for (i, result) in results.into_iter().enumerate() {
            let mut mapped_msg: Message = UserDefinedMessage(result, &parent_info, i as i32).into();
            remove_tracing_udf_if_enabled(&mut mapped_msg, tracing_enabled);
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
    read_tx: mpsc::Sender<MapRequest>,
    senders: Arc<Mutex<UnarySenderMapState>>,
    _handle: Arc<AbortOnDropHandle<()>>,
}

impl UserDefinedUnaryMap {
    /// Performs handshake with the server and creates a new UserDefinedUnaryMap.
    pub(in crate::mapper) async fn new(
        batch_size: usize,
        mut client: MapClient<Channel>,
    ) -> Result<Self> {
        let (read_tx, read_rx) = mpsc::channel(batch_size);
        let resp_stream = create_response_stream(read_tx.clone(), read_rx, &mut client).await?;

        // map to track the oneshot sender for each request
        let sender_map = Arc::new(Mutex::new(UnarySenderMapState::default()));

        // background task to receive responses from the server and send them to the appropriate
        // oneshot sender based on the message id
        let sender_map_clone = Arc::clone(&sender_map);
        let handle = tokio::spawn(async move {
            Self::receive_unary_responses(sender_map_clone, resp_stream).await;
        });

        let mapper = Self {
            read_tx,
            senders: sender_map,
            _handle: Arc::new(AbortOnDropHandle::new(handle)),
        };

        Ok(mapper)
    }

    /// Broadcasts a unary gRPC error to all pending senders and records error metrics.
    fn broadcast_error(sender_map: &Arc<Mutex<UnarySenderMapState>>, error: tonic::Status) {
        let senders = {
            let mut sender_guard = sender_map.lock().expect("failed to acquire poisoned lock");
            sender_guard.closed = true;
            std::mem::take(&mut sender_guard.map)
        };

        for (_, sender) in senders {
            let _ = sender.send(Err(Error::Grpc(Box::new(error.clone()))));
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
                Ok(resp) => Self::process_unary_response(&sender_map, resp).await,
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
        let (tx, rx) = oneshot::channel();
        let key = request.id.clone();

        // only insert if we are able to send the message to the server
        if let Err(e) = self.read_tx.send(request).await {
            error!(?e, "Failed to send message to server");
            return Err(Error::Mapper(format!(
                "failed to send message to unary map server: {e}"
            )));
        }

        // move the senders_guard out of the scope to drop the guard when done
        {
            let mut senders_guard = self
                .senders
                .lock()
                .expect("failed to acquire poisoned lock");
            if !senders_guard.closed {
                senders_guard.map.insert(key.clone(), tx);
            } else {
                let _ = tx
                    .send(Err(Error::Mapper("mapper closed".to_string())))
                    .inspect(|_| warn!("failed to send error to oneshot receiver"));
            }
        };

        tokio::select! {
            result = rx => {
                // we don't have to remove the sender from the map, because the response handler
                // will do it.
                result.map_err(|e: oneshot::error::RecvError| Error::ActorPatternRecv(e.to_string()))?
            }
            _ = cln_token.cancelled() => {
                // Remove the sender from the map since we're cancelling
                self.senders
                    .lock()
                    .expect("failed to acquire poisoned lock")
                    .map
                    .remove(&key);
                Err(Error::Mapper("unary map operation cancelled".to_string()))
            }
        }
    }

    /// Processes the response from the server and sends it to the appropriate oneshot sender
    /// based on the message id entry in the map.
    pub(super) async fn process_unary_response(
        sender_map: &Arc<Mutex<UnarySenderMapState>>,
        resp: MapResponse,
    ) {
        let msg_id = resp.id;

        let sender_entry = sender_map
            .lock()
            .expect("failed to acquire poisoned lock")
            .map
            .remove(&msg_id);

        if let Some(sender) = sender_entry {
            sender
                .send(Ok(resp.results))
                .expect("failed to send response");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::remove_tracing_udf_if_enabled;
    use crate::mapper::map::unary::UserDefinedUnaryMap;
    use crate::message::Message;
    use crate::metadata::{KeyValueGroup, Metadata};
    use crate::shared::grpc::create_rpc_channel;
    use crate::shared::otel;
    use bytes::Bytes;
    use numaflow::map;
    use numaflow::shared::ServerExtras;
    use numaflow_pb::clients::map::map_client::MapClient;
    use std::collections::HashMap;
    use std::error::Error;
    use std::sync::Arc;
    use std::time::Duration;
    use tempfile::TempDir;
    use tokio_util::sync::CancellationToken;

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

    fn message_with_tracing_udf() -> Message {
        let mut metadata = Metadata::default();
        metadata.sys_metadata.insert(
            otel::TRACING_UDF_METADATA_KEY.to_string(),
            KeyValueGroup {
                key_value: HashMap::from([(
                    "traceparent".to_string(),
                    Bytes::from_static(b"traceparent"),
                )]),
            },
        );

        Message {
            metadata: Some(Arc::new(metadata)),
            ..Default::default()
        }
    }

    fn has_tracing_udf(message: &Message) -> bool {
        message.metadata.as_deref().is_some_and(|metadata| {
            metadata
                .sys_metadata
                .contains_key(otel::TRACING_UDF_METADATA_KEY)
        })
    }

    #[test]
    fn remove_tracing_udf_helper_removes_only_when_enabled() {
        let mut enabled_message = message_with_tracing_udf();
        remove_tracing_udf_if_enabled(&mut enabled_message, true);
        assert!(!has_tracing_udf(&enabled_message));

        let mut disabled_message = message_with_tracing_udf();
        remove_tracing_udf_if_enabled(&mut disabled_message, false);
        assert!(has_tracing_udf(&disabled_message));
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

        let client =
            UserDefinedUnaryMap::new(500, MapClient::new(create_rpc_channel(sock_file).await?))
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
            tokio::time::timeout(Duration::from_secs(2), client.unary(request, cln_token))
                .await
                .unwrap();

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
}
