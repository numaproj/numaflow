use super::{
    ParentMessageInfo, UserDefinedMessage, create_response_stream, update_udf_error_metric,
    update_udf_process_time_metric, update_udf_read_metric, update_udf_write_metric,
};
use crate::config::is_mono_vertex;
use crate::config::pipeline::VERTEX_TYPE_MAP_UDF;
use crate::error::{Error, Result};
use crate::message::{Message, MessageHandle};
use crate::monovertex::bypass_router::MvtxBypassRouter;
use crate::tracker::Tracker;
use crate::mark_success_batch;
use numaflow_pb::clients::map::{self, MapRequest, MapResponse, map_client::MapClient};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use tokio::sync::{mpsc, oneshot};
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;
use tokio_util::task::AbortOnDropHandle;
use tonic::Streaming;
use tonic::transport::Channel;
use tracing::error;

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
    pub read_batch: Vec<MessageHandle>,
    pub output_tx: mpsc::Sender<MessageHandle>,
    pub tracker: Tracker,
    pub bypass_router: Option<MvtxBypassRouter>,
    pub is_mono_vertex: bool,
    pub cln_token: CancellationToken,
}

impl MapBatchTask {
    /// Executes the batch map operation.
    /// Returns an error if any message in the batch fails to be processed.
    pub async fn execute(self) -> Result<()> {
        // Store parent message info for each message before sending to UDF
        let parent_infos: Vec<ParentMessageInfo> = self
            .read_batch
            .iter()
            .map(|rm| rm.message().into())
            .collect();

        // Convert Messages to MapRequests
        let requests: Vec<MapRequest> = self
            .read_batch
            .iter()
            .map(|rm| rm.message().clone().into())
            .collect();

        // Update read metrics for each request
        for _ in &requests {
            update_udf_read_metric(self.is_mono_vertex);
        }

        // Call the UDF and get results directly
        let results = self.mapper.batch(requests, self.cln_token).await;

        for (idx, (result, parent_info)) in results
            .into_iter()
            .zip(parent_infos.into_iter())
            .enumerate()
        {
            match result {
                Ok(results) => {
                    // Convert raw results to Messages using parent info
                    let mapped_messages: Vec<Message> = results
                        .into_iter()
                        .enumerate()
                        .map(|(i, result)| {
                            UserDefinedMessage(result, &parent_info, i as i32).into()
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
                        .await?;

                    // Send messages downstream wrapped in MessageHandle sharing the same AckHandle.
                    // Each output message increments ref_count, and downstream will call mark_success()
                    // when the message is successfully written.
                    for mapped_message in mapped_messages {
                        let read_msg = self.read_batch[idx].clone_with_message(mapped_message);

                        // Try to bypass the message. If bypassed, try_bypass takes ownership and returns None.
                        // If not bypassed, it returns Some(read_msg) for us to send downstream.
                        let read_msg = if let Some(ref bypass_router) = self.bypass_router {
                            match bypass_router
                                .try_bypass(read_msg)
                                .await
                                .expect("failed to send message to bypass channel")
                            {
                                Some(msg) => msg,
                                None => continue, // Message was bypassed, move to next
                            }
                        } else {
                            read_msg
                        };

                        self.output_tx
                            .send(read_msg)
                            .await
                            .expect("failed to send response");
                    }
                }
                Err(e) => {
                    error!(err=?e, "failed to map message");
                    // read_batch will be dropped without mark_success, causing NAK
                    return Err(e);
                }
            }
        }

        // we have successfully processed the batch, mark the batch as success.
        mark_success_batch!(self.read_batch);

        Ok(())
    }
}

/// UserDefinedBatchMap is a grpc client that sends batch requests to the map server
/// and forwards the responses.
#[derive(Clone)]
pub(in crate::mapper) struct UserDefinedBatchMap {
    read_tx: mpsc::Sender<MapRequest>,
    senders: Arc<Mutex<BatchSenderMapState>>,
    _handle: Arc<AbortOnDropHandle<()>>,
}

impl UserDefinedBatchMap {
    /// Performs handshake with the server and creates a new UserDefinedBatchMap.
    pub(in crate::mapper) async fn new(
        batch_size: usize,
        mut client: MapClient<Channel>,
    ) -> Result<Self> {
        let (read_tx, read_rx) = mpsc::channel(batch_size);
        let resp_stream = create_response_stream(read_tx.clone(), read_rx, &mut client).await?;

        // map to track the oneshot response sender for each request
        let sender_map = Arc::new(Mutex::new(BatchSenderMapState::default()));

        // background task to receive responses from the server and send them to the appropriate
        // oneshot response sender based on the id
        let sender_map_clone = Arc::clone(&sender_map);
        let handle = tokio::spawn(async move {
            Self::receive_batch_responses(sender_map_clone, resp_stream).await;
        });

        let mapper = Self {
            read_tx,
            senders: sender_map,
            _handle: Arc::new(AbortOnDropHandle::new(handle)),
        };
        Ok(mapper)
    }

    /// Broadcasts a batch map gRPC error to all pending senders and records error metrics.
    fn broadcast_error(sender_map: &Arc<Mutex<BatchSenderMapState>>, error: tonic::Status) {
        let senders = {
            let mut sender_guard = sender_map.lock().expect("failed to acquire poisoned lock");
            sender_guard.closed = true;
            std::mem::take(&mut sender_guard.map)
        };

        for (_, sender) in senders {
            let _ = sender.send(Err(Error::Grpc(Box::new(error.clone()))));
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
                            error!("received EOT but not all responses have been received");
                            critical_error!(VERTEX_TYPE_MAP_UDF, "eot_received_from_map");
                        }
                        update_udf_process_time_metric(is_mono_vertex());
                        continue;
                    }

                    Self::process_response(&sender_map, resp).await
                }
                Err(e) => {
                    error!(?e, "Error reading message from batch map gRPC stream");
                    Self::broadcast_error(&sender_map, e);
                }
            }
        }
    }

    /// Processes the response from the server and sends it to the appropriate oneshot sender
    /// based on the message id entry in the map.
    async fn process_response(sender_map: &Arc<Mutex<BatchSenderMapState>>, resp: MapResponse) {
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

    /// Sends a batch of messages to the UDF and returns the raw response results.
    /// Returns early with an error if any request fails or if the cancellation token is cancelled.
    pub(in crate::mapper) async fn batch(
        &self,
        requests: Vec<MapRequest>,
        cln_token: CancellationToken,
    ) -> Vec<Result<BatchMapResponse>> {
        let (senders, receivers): (Vec<_>, Vec<_>) =
            requests.iter().map(|_| oneshot::channel()).unzip();

        for (request, sender) in requests.into_iter().zip(senders) {
            let key = request.id.clone();

            // only insert if we are able to send the message to the server
            if let Err(e) = self.read_tx.send(request).await {
                error!(?e, "Failed to send message to server");
                let _ = sender.send(Err(Error::Mapper(format!(
                    "failed to send message to batch map server: {e}"
                ))));
                // Continue collecting results for remaining receivers
                break;
            }

            let mut senders_guard = self
                .senders
                .lock()
                .expect("failed to acquire poisoned lock");

            if senders_guard.closed {
                let _ = sender.send(Err(Error::Mapper("mapper closed".to_string())));
                continue;
            }

            senders_guard.map.insert(key, sender);
        }

        // send eot request
        if let Err(e) = self
            .read_tx
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
}

#[cfg(test)]
mod tests {
    use crate::mapper::map::batch::UserDefinedBatchMap;
    use crate::shared::grpc::create_rpc_channel;
    use numaflow::batchmap;
    use numaflow::batchmap::Server;
    use numaflow::shared::ServerExtras;
    use numaflow_pb::clients::map::map_client::MapClient;
    use std::error::Error;
    use std::time::Duration;
    use tempfile::TempDir;

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

        let client =
            UserDefinedBatchMap::new(500, MapClient::new(create_rpc_channel(sock_file).await?))
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
}
