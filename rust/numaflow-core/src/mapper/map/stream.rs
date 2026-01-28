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
use tokio::sync::{OwnedSemaphorePermit, mpsc};
use tokio_util::sync::CancellationToken;
use tokio_util::task::AbortOnDropHandle;
use tonic::Streaming;
use tonic::transport::Channel;
use tracing::error;

use super::{
    ParentMessageInfo, STREAMING_MAP_RESP_CHANNEL_SIZE, UserDefinedMessage, create_response_stream,
    update_udf_error_metric, update_udf_process_time_metric, update_udf_read_metric,
    update_udf_write_only_metric,
};

/// Type alias for the stream response - raw results from the UDF
pub(in crate::mapper) type StreamMapResponse = Vec<map::map_response::Result>;

type StreamResponseSenderMap = Arc<Mutex<HashMap<String, mpsc::Sender<Result<StreamMapResponse>>>>>;

/// MapStreamTask encapsulates all the context needed to execute a stream map operation.
/// This reduces the number of arguments passed around and makes the code more readable.
pub(in crate::mapper) struct MapStreamTask {
    pub mapper: UserDefinedStreamMap,
    pub permit: OwnedSemaphorePermit,
    pub message: Message,
    pub output_tx: mpsc::Sender<Message>,
    pub tracker: Tracker,
    pub error_tx: mpsc::Sender<Error>,
    pub cln_token: CancellationToken,
    pub bypass_router: Option<MvtxBypassRouter>,
    pub is_mono_vertex: bool,
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
        // Hold the permit until the task completes
        let _permit = self.permit;

        // Store parent message info before sending to UDF
        let mut parent_info: ParentMessageInfo = (&self.message).into();
        let offset = self.message.offset.clone();

        // Convert Message to MapRequest
        let request: MapRequest = self.message.clone().into();

        update_udf_read_metric(self.is_mono_vertex);

        // Call the UDF and get receiver for raw results
        let mut receiver = self.mapper.stream(request, self.cln_token).await;

        // We need to update the tracker with no responses, because unlike unary and batch,
        // we cannot update the responses here - we will have to append the responses.
        self.tracker
            .serving_refresh(offset.clone())
            .await
            .expect("failed to reset tracker");

        loop {
            let result = receiver.recv().await;
            match result {
                Some(Ok(results)) => {
                    // Convert raw results to Messages using parent info
                    for result in results {
                        let mapped_message: Message =
                            UserDefinedMessage(result, &parent_info, parent_info.current_index)
                                .into();
                        parent_info.current_index += 1;

                        update_udf_write_only_metric(self.is_mono_vertex);

                        self.tracker
                            .serving_append(
                                mapped_message.offset.clone(),
                                mapped_message.tags.clone(),
                            )
                            .await
                            .expect("failed to update tracker");

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
                Some(Err(e)) => {
                    error!(?e, "failed to map message");
                    self.message
                        .ack_handle
                        .as_ref()
                        .expect("ack handle should be present")
                        .is_failed
                        .store(true, Ordering::Relaxed);
                    let _ = self.error_tx.send(e).await;
                    return;
                }
                None => break,
            }
        }
    }
}

/// UserDefinedStreamMap is a grpc client that sends stream requests to the map server
#[derive(Clone)]
pub(in crate::mapper) struct UserDefinedStreamMap {
    read_tx: mpsc::Sender<MapRequest>,
    senders: StreamResponseSenderMap,
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
        let sender_map = Arc::new(Mutex::new(HashMap::new()));

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
    async fn broadcast_error(sender_map: &StreamResponseSenderMap, error: tonic::Status) {
        let senders =
            std::mem::take(&mut *sender_map.lock().expect("failed to acquire poisoned lock"));

        for (_, sender) in senders {
            let _ = sender.send(Err(Error::Grpc(Box::new(error.clone())))).await;
            update_udf_error_metric(is_mono_vertex());
        }
    }

    /// receive responses from the server and gets the corresponding mpsc sender from the map
    /// and sends the response.
    async fn receive_stream_responses(
        sender_map: StreamResponseSenderMap,
        mut resp_stream: Streaming<MapResponse>,
    ) {
        loop {
            let resp = match resp_stream.message().await {
                Ok(Some(message)) => message,
                Ok(None) => break,
                Err(e) => {
                    error!(?e, "Error reading message from stream map gRPC stream");
                    Self::broadcast_error(&sender_map, e).await;
                    break;
                }
            };

            let response_sender = sender_map
                .lock()
                .expect("failed to acquire poisoned lock")
                .remove(&resp.id)
                .expect("map entry should always be present");

            // once we get eot, we can drop the sender to let the callee
            // know that we are done sending responses
            if let Some(map::TransmissionStatus { eot: true }) = resp.status {
                update_udf_process_time_metric(is_mono_vertex());
                continue;
            }

            Self::process_stream_response(&sender_map, resp.id, response_sender, resp.results)
                .await;
        }
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

        // only insert if we are able to send the message to the server
        if let Err(e) = self.read_tx.send(request).await {
            error!(?e, "Failed to send message to server");
            let _ = tx
                .send(Err(Error::Mapper(format!(
                    "failed to send message to stream map server: {e}"
                ))))
                .await;
            return rx;
        }

        self.senders
            .lock()
            .expect("failed to acquire poisoned lock")
            .insert(key, tx);

        rx
    }

    /// Processes stream responses and sends them to the appropriate mpsc sender
    async fn process_stream_response(
        sender_map: &StreamResponseSenderMap,
        msg_id: String,
        response_sender: mpsc::Sender<Result<StreamMapResponse>>,
        results: Vec<map::map_response::Result>,
    ) {
        response_sender
            .send(Ok(results))
            .await
            .expect("failed to send response");

        // Write the sender back to the map, because we need to send
        // more responses for the same request
        sender_map
            .lock()
            .expect("failed to acquire poisoned lock")
            .insert(msg_id, response_sender);
    }
}

#[cfg(test)]
mod tests {
    use crate::mapper::map::stream::UserDefinedStreamMap;
    use crate::shared::grpc::create_rpc_channel;
    use numaflow::mapstream;
    use numaflow::shared::ServerExtras;
    use numaflow_pb::clients::map::map_client::MapClient;
    use std::error::Error;
    use std::time::Duration;
    use tempfile::TempDir;

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
}
