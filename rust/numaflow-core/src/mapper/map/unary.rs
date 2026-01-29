use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::Ordering;

use crate::config::is_mono_vertex;
use crate::error::{Error, Result};
use crate::message::Message;
use crate::monovertex::bypass_router::MvtxBypassRouter;
use crate::tracker::Tracker;
use numaflow_pb::clients::map::{self, MapRequest, MapResponse, map_client::MapClient};
use tokio::sync::{OwnedSemaphorePermit, mpsc, oneshot};
use tokio_util::sync::CancellationToken;
use tokio_util::task::AbortOnDropHandle;
use tonic::Streaming;
use tonic::transport::Channel;
use tracing::error;

use super::{
    ParentMessageInfo, UserDefinedMessage, create_response_stream, update_udf_error_metric,
    update_udf_read_metric, update_udf_write_metric,
};

/// Type alias for the response - raw results from the UDF
pub(in crate::mapper) type UnaryMapResponse = Vec<map::map_response::Result>;

type ResponseSenderMap = Arc<Mutex<HashMap<String, oneshot::Sender<Result<UnaryMapResponse>>>>>;

/// MapUnaryTask encapsulates all the context needed to execute a unary map operation.
pub(in crate::mapper) struct MapUnaryTask {
    pub mapper: UserDefinedUnaryMap,
    pub permit: OwnedSemaphorePermit,
    pub message: Message,
    pub output_tx: mpsc::Sender<Message>,
    pub tracker: Tracker,
    pub error_tx: mpsc::Sender<Error>,
    pub cln_token: CancellationToken,
    pub bypass_router: Option<MvtxBypassRouter>,
    pub is_mono_vertex: bool,
}

impl MapUnaryTask {
    /// Spawns the unary map task as a tokio task.
    /// The task will process the message through the UDF and send results downstream.
    pub fn spawn(self) {
        tokio::spawn(async move {
            self.execute().await;
        });
    }

    /// Executes the unary map operation.
    async fn execute(self) {
        // Hold the permit until the task completes
        let _permit = self.permit;

        // Store parent message info before sending to UDF
        let parent_info: ParentMessageInfo = (&self.message).into();
        let offset = self.message.offset.clone();

        // Convert Message to MapRequest
        let request: MapRequest = self.message.clone().into();

        update_udf_read_metric(self.is_mono_vertex);

        // Call the UDF and get raw results
        let results = match self.mapper.unary(request, self.cln_token).await {
            Ok(results) => results,
            Err(e) => {
                error!(?e, ?offset, "failed to map message");
                self.message
                    .ack_handle
                    .as_ref()
                    .expect("ack handle should be present")
                    .is_failed
                    .store(true, Ordering::Relaxed);
                let _ = self.error_tx.send(e).await;
                return;
            }
        };

        // Convert raw results to Messages using parent info
        let mapped_messages: Vec<Message> = results
            .into_iter()
            .enumerate()
            .map(|(i, result)| UserDefinedMessage(result, &parent_info, i as i32).into())
            .collect();

        update_udf_write_metric(
            self.is_mono_vertex,
            parent_info,
            mapped_messages.len() as u64,
        );

        // Update the tracker with the number of messages sent
        self.tracker
            .serving_update(
                &offset,
                mapped_messages.iter().map(|m| m.tags.clone()).collect(),
            )
            .await
            .expect("failed to update tracker");

        // Send messages downstream
        for mapped_message in mapped_messages {
            let bypassed = if let Some(ref bypass_router) = self.bypass_router {
                bypass_router
                    .try_bypass(mapped_message.clone())
                    .await
                    .expect("failed to send message to bypass channel")
            } else {
                false
            };

            if !bypassed {
                self.output_tx
                    .send(mapped_message)
                    .await
                    .expect("failed to send response");
            }
        }
    }
}

/// UserDefinedUnaryMap is a grpc client that sends unary requests to the map server
/// and forwards the responses.
#[derive(Clone)]
pub(in crate::mapper) struct UserDefinedUnaryMap {
    read_tx: mpsc::Sender<MapRequest>,
    senders: ResponseSenderMap,
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
        let sender_map = Arc::new(Mutex::new(HashMap::new()));

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
    fn broadcast_error(sender_map: &ResponseSenderMap, error: tonic::Status) {
        let senders =
            std::mem::take(&mut *sender_map.lock().expect("failed to acquire poisoned lock"));

        for (_, sender) in senders {
            let _ = sender.send(Err(Error::Grpc(Box::new(error.clone()))));
            update_udf_error_metric(is_mono_vertex());
        }
    }

    /// receive responses from the server and gets the corresponding oneshot response sender from the map
    /// and sends the response.
    async fn receive_unary_responses(
        sender_map: ResponseSenderMap,
        mut resp_stream: Streaming<MapResponse>,
    ) {
        loop {
            let resp = match resp_stream.message().await {
                Ok(Some(message)) => message,
                Ok(None) => break,
                Err(e) => {
                    error!(?e, "Error reading message from unary map gRPC stream");
                    Self::broadcast_error(&sender_map, e);
                    break;
                }
            };

            Self::process_unary_response(&sender_map, resp).await
        }
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

        // insert the sender into the map
        self.senders
            .lock()
            .expect("failed to acquire poisoned lock")
            .insert(key.clone(), tx);

        tokio::select! {
            result = rx => {
                // we don't have to remove the sender from the map, because the response handler
                // will do it.
                result.map_err(|e| Error::ActorPatternRecv(e.to_string()))?
            }
            _ = cln_token.cancelled() => {
                // Remove the sender from the map since we're cancelling
                self.senders
                    .lock()
                    .expect("failed to acquire poisoned lock")
                    .remove(&key);
                Err(Error::Mapper("unary map operation cancelled".to_string()))
            }
        }
    }

    /// Processes the response from the server and sends it to the appropriate oneshot sender
    /// based on the message id entry in the map.
    pub(super) async fn process_unary_response(sender_map: &ResponseSenderMap, resp: MapResponse) {
        let msg_id = resp.id;

        let sender_entry = sender_map
            .lock()
            .expect("failed to acquire poisoned lock")
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
    use crate::mapper::map::unary::UserDefinedUnaryMap;
    use crate::shared::grpc::create_rpc_channel;
    use numaflow::map;
    use numaflow::shared::ServerExtras;
    use numaflow_pb::clients::map::map_client::MapClient;
    use std::error::Error;
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
