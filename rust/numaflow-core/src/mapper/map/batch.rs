use super::{
    ParentMessageInfo, UserDefinedMessage, create_response_stream, update_udf_error_metric,
    update_udf_process_time_metric, update_udf_read_metric, update_udf_write_metric,
};
use crate::config::is_mono_vertex;
use crate::config::pipeline::VERTEX_TYPE_MAP_UDF;
use crate::error::{Error, Result};
use crate::message::Message;
use crate::monovertex::bypass_router::MvtxBypassRouter;
use crate::tracker::Tracker;
use numaflow_pb::clients::map::{self, MapRequest, MapResponse, map_client::MapClient};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;
use tokio_util::task::AbortOnDropHandle;
use tonic::Streaming;
use tonic::transport::Channel;
use tracing::error;

/// Type alias for the batch response - raw results from the UDF
pub(in crate::mapper) type BatchMapResponse = Vec<map::map_response::Result>;

/// Type aliases
type ResponseSenderMap = Arc<Mutex<HashMap<String, oneshot::Sender<Result<BatchMapResponse>>>>>;

/// MapBatchTask encapsulates all the context needed to execute a batch map operation.
pub(in crate::mapper) struct MapBatchTask {
    pub mapper: UserDefinedBatchMap,
    pub batch: Vec<Message>,
    pub output_tx: mpsc::Sender<Message>,
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
        let parent_infos: Vec<ParentMessageInfo> = self.batch.iter().map(|m| m.into()).collect();

        // Convert Messages to MapRequests
        let requests: Vec<MapRequest> = self.batch.into_iter().map(|m| m.into()).collect();

        // Update read metrics for each request
        for _ in &requests {
            update_udf_read_metric(self.is_mono_vertex);
        }

        // Call the UDF and get results directly
        let results = self.mapper.batch(requests, self.cln_token).await;

        for (result, parent_info) in results.into_iter().zip(parent_infos.into_iter()) {
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
                        parent_info,
                        mapped_messages.len() as u64,
                    );

                    self.tracker
                        .serving_update(
                            &offset,
                            mapped_messages.iter().map(|m| m.tags.clone()).collect(),
                        )
                        .await?;

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
                Err(e) => {
                    error!(err=?e, "failed to map message");
                    return Err(e);
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
    read_tx: mpsc::Sender<MapRequest>,
    senders: ResponseSenderMap,
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
        let sender_map = Arc::new(Mutex::new(HashMap::new()));

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
    fn broadcast_error(sender_map: &ResponseSenderMap, error: tonic::Status) {
        let senders =
            std::mem::take(&mut *sender_map.lock().expect("failed to acquire poisoned lock"));

        for (_, sender) in senders {
            let _ = sender.send(Err(Error::Grpc(Box::new(error.clone()))));
            update_udf_error_metric(is_mono_vertex())
        }
    }

    /// receive responses from the server and gets the corresponding oneshot response sender from the map
    /// and sends the response.
    async fn receive_batch_responses(
        sender_map: ResponseSenderMap,
        mut resp_stream: Streaming<MapResponse>,
    ) {
        loop {
            let resp = match resp_stream.message().await {
                Ok(Some(message)) => message,
                Ok(None) => break,
                Err(e) => {
                    error!(?e, "Error reading message from batch map gRPC stream");
                    Self::broadcast_error(&sender_map, e);
                    break;
                }
            };

            if let Some(map::TransmissionStatus { eot: true }) = resp.status {
                if !sender_map
                    .lock()
                    .expect("failed to acquire poisoned lock")
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
    }

    /// Processes the response from the server and sends it to the appropriate oneshot sender
    /// based on the message id entry in the map.
    async fn process_response(sender_map: &ResponseSenderMap, resp: MapResponse) {
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

    /// Sends a batch of messages to the UDF and returns the raw response results.
    /// If the cancellation token is cancelled while waiting for responses,
    /// remaining messages will return an error indicating the operation was cancelled.
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

            self.senders
                .lock()
                .expect("failed to acquire poisoned lock")
                .insert(key, sender);
        }

        // send eot request
        self.read_tx
            .send(MapRequest {
                request: None,
                id: "".to_string(),
                handshake: None,
                status: Some(map::TransmissionStatus { eot: true }),
            })
            .await
            .expect("failed to send eot request");

        let total_receivers = receivers.len();
        let mut results = Vec::with_capacity(total_receivers);
        for receiver in receivers {
            let result = tokio::select! {
                recv_result = receiver => {
                    recv_result.unwrap_or_else(|e| Err(Error::ActorPatternRecv(e.to_string())))
                }
                _ = cln_token.cancelled() => {
                    Err(Error::Mapper("batch map operation cancelled".to_string()))
                }
            };
            results.push(result);

            // If cancelled, fill remaining results with cancellation errors
            if cln_token.is_cancelled() {
                break;
            }
        }

        // Fill any remaining slots with cancellation errors if we broke early
        while results.len() < total_receivers {
            results.push(Err(Error::Mapper(
                "batch map operation cancelled".to_string(),
            )));
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
