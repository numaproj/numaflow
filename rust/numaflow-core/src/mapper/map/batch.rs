use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Instant;

use numaflow_pb::clients::map::{self, MapRequest, MapResponse, map_client::MapClient};
use tokio::sync::{mpsc, oneshot};
use tonic::transport::Channel;
use tonic::Streaming;
use tracing::error;

use crate::config::pipeline::VERTEX_TYPE_MAP_UDF;
use crate::error::{Error, Result};
use crate::message::Message;
use crate::metrics::{pipeline_metric_labels, pipeline_metrics};

use super::{ParentMessageInfo, ResponseSenderMap, create_response_stream};
use super::unary::process_unary_response;

/// UserDefinedBatchMap is a grpc client that sends batch requests to the map server
/// and forwards the responses.
#[derive(Clone)]
pub(in crate::mapper) struct UserDefinedBatchMap {
    read_tx: mpsc::Sender<MapRequest>,
    senders: ResponseSenderMap,
}

impl UserDefinedBatchMap {
    /// Performs handshake with the server and creates a new UserDefinedBatchMap.
    pub(in crate::mapper) async fn new(
        batch_size: usize,
        mut client: MapClient<Channel>,
    ) -> Result<Self> {
        let (read_tx, read_rx) = mpsc::channel(batch_size);
        let resp_stream = create_response_stream(read_tx.clone(), read_rx, &mut client).await?;

        // map to track the oneshot response sender for each request along with the message info
        let sender_map = Arc::new(Mutex::new(HashMap::new()));

        // background task to receive responses from the server and send them to the appropriate
        // oneshot response sender based on the id
        let sender_map_clone = Arc::clone(&sender_map);
        tokio::spawn(async move {
            Self::receive_batch_responses(sender_map_clone, resp_stream).await;
        });

        let mapper = Self {
            read_tx,
            senders: sender_map,
        };
        Ok(mapper)
    }

    /// receive responses from the server and gets the corresponding oneshot response sender from the map
    /// and sends the response.
    async fn receive_batch_responses(
        sender_map: ResponseSenderMap,
        mut resp_stream: Streaming<MapResponse>,
    ) {
        while let Some(resp) = match resp_stream.message().await {
            Ok(message) => message,
            Err(e) => {
                let senders = {
                    let mut senders = sender_map.lock().expect("failed to acquire poisoned lock");
                    senders.drain().collect::<Vec<_>>()
                };

                for (_, (_, sender)) in senders {
                    sender
                        .send(Err(Error::Grpc(Box::new(e.clone()))))
                        .expect("failed to send error response");
                    pipeline_metrics()
                        .forwarder
                        .udf_error_total
                        .get_or_create(pipeline_metric_labels(VERTEX_TYPE_MAP_UDF))
                        .inc();
                }
                None
            }
        } {
            if let Some(map::TransmissionStatus { eot: true }) = resp.status {
                if !sender_map
                    .lock()
                    .expect("failed to acquire poisoned lock")
                    .is_empty()
                {
                    error!("received EOT but not all responses have been received");
                }
                pipeline_metrics()
                    .forwarder
                    .udf_processing_time
                    .get_or_create(pipeline_metric_labels(VERTEX_TYPE_MAP_UDF))
                    .observe(Instant::now().elapsed().as_micros() as f64);
                continue;
            }

            process_unary_response(&sender_map, resp).await
        }
    }

    /// Handles the incoming message and sends it to the server for mapping.
    pub(in crate::mapper) async fn batch_map(
        &self,
        messages: Vec<Message>,
        respond_to: Vec<oneshot::Sender<Result<Vec<Message>>>>,
    ) {
        for (message, respond_to) in messages.into_iter().zip(respond_to) {
            let key = message.offset.clone().to_string();
            let msg_info = ParentMessageInfo {
                offset: message.offset.clone(),
                event_time: message.event_time,
                headers: Arc::clone(&message.headers),
                is_late: message.is_late,
                start_time: Instant::now(),
                current_index: 0,
                metadata: message.metadata.clone(),
                ack_handle: message.ack_handle.clone(),
            };

            pipeline_metrics()
                .forwarder
                .udf_read_total
                .get_or_create(pipeline_metric_labels(VERTEX_TYPE_MAP_UDF))
                .inc();

            // only insert if we are able to send the message to the server
            if let Err(e) = self.read_tx.send(message.into()).await {
                error!(?e, "Failed to send message to server");
                let _ = respond_to.send(Err(Error::Mapper(format!(
                    "failed to send message to batch map server: {e}"
                ))));
                return;
            }

            self.senders
                .lock()
                .expect("failed to acquire poisoned lock")
                .insert(key.clone(), (msg_info, respond_to));
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

