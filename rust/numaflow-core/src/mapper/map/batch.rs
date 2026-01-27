use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;

use crate::config::is_mono_vertex;
use crate::error::{Error, Result};
use numaflow_pb::clients::map::{self, MapRequest, MapResponse, map_client::MapClient};
use tokio::sync::{mpsc, oneshot};
use tokio_util::task::AbortOnDropHandle;
use tonic::Streaming;
use tonic::transport::Channel;
use tracing::error;

use super::{create_response_stream, update_udf_error_metric, update_udf_process_time_metric};

/// Type alias for the batch response - raw results from the UDF
pub(in crate::mapper) type BatchMapResponse = Vec<map::map_response::Result>;

/// Type aliases
type ResponseSenderMap = Arc<Mutex<HashMap<String, oneshot::Sender<Result<BatchMapResponse>>>>>;

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
    /// This method handles oneshot channel creation internally.
    pub(in crate::mapper) async fn batch(
        &self,
        requests: Vec<MapRequest>,
    ) -> Vec<Result<BatchMapResponse>> {
        let (senders, receivers): (Vec<_>, Vec<_>) =
            requests.iter().map(|_| oneshot::channel()).unzip();

        self.batch_map(requests, senders).await;

        let mut results = Vec::with_capacity(receivers.len());
        for receiver in receivers {
            let result = match receiver.await {
                Ok(result) => result,
                Err(e) => Err(Error::ActorPatternRecv(e.to_string())),
            };
            results.push(result);
        }
        results
    }

    /// Handles the incoming message and sends it to the server for mapping.
    async fn batch_map(
        &self,
        requests: Vec<MapRequest>,
        respond_to: Vec<oneshot::Sender<Result<BatchMapResponse>>>,
    ) {
        for (request, respond_to) in requests.into_iter().zip(respond_to) {
            let key = request.id.clone();

            // only insert if we are able to send the message to the server
            if let Err(e) = self.read_tx.send(request).await {
                error!(?e, "Failed to send message to server");
                let _ = respond_to.send(Err(Error::Mapper(format!(
                    "failed to send message to batch map server: {e}"
                ))));
                return;
            }

            self.senders
                .lock()
                .expect("failed to acquire poisoned lock")
                .insert(key, respond_to);
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

        let results =
            tokio::time::timeout(Duration::from_secs(2), client.batch(requests)).await?;

        assert_eq!(results.len(), 2);
        assert!(results[0].is_ok());
        assert!(results[1].is_ok());
        assert_eq!(results[0].as_ref().unwrap().len(), 1);
        assert_eq!(results[1].as_ref().unwrap().len(), 1);

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
