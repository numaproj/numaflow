use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Instant;

use numaflow_pb::clients::map::{self, MapRequest, MapResponse, map_client::MapClient};
use tokio::sync::mpsc;
use tonic::transport::Channel;
use tonic::Streaming;
use tracing::error;

use crate::config::pipeline::VERTEX_TYPE_MAP_UDF;
use crate::error::{Error, Result};
use crate::message::Message;
use crate::metrics::{pipeline_metric_labels, pipeline_metrics};

use super::{ParentMessageInfo, StreamResponseSenderMap, create_response_stream};

/// Processes stream responses and sends them to the appropriate mpsc sender
async fn process_stream_response(
    sender_map: &StreamResponseSenderMap,
    msg_id: String,
    mut message_info: ParentMessageInfo,
    response_sender: mpsc::Sender<crate::error::Result<Message>>,
    results: Vec<map::map_response::Result>,
) {
    for result in results.into_iter() {
        response_sender
            .send(Ok(super::UserDefinedMessage(
                result,
                &message_info,
                message_info.current_index,
            )
            .into()))
            .await
            .expect("failed to send response");
        message_info.current_index += 1;
        pipeline_metrics()
            .forwarder
            .udf_write_total
            .get_or_create(pipeline_metric_labels(VERTEX_TYPE_MAP_UDF))
            .inc();
    }

    // Write the sender back to the map, because we need to send
    // more responses for the same request
    sender_map
        .lock()
        .expect("failed to acquire poisoned lock")
        .insert(msg_id, (message_info, response_sender));
}

/// UserDefinedStreamMap is a grpc client that sends stream requests to the map server
#[derive(Clone)]
pub(in crate::mapper) struct UserDefinedStreamMap {
    read_tx: mpsc::Sender<MapRequest>,
    senders: StreamResponseSenderMap,
}

impl UserDefinedStreamMap {
    /// Performs handshake with the server and creates a new UserDefinedStreamMap.
    pub(in crate::mapper) async fn new(
        batch_size: usize,
        mut client: MapClient<Channel>,
    ) -> Result<Self> {
        let (read_tx, read_rx) = mpsc::channel(batch_size);
        let resp_stream = create_response_stream(read_tx.clone(), read_rx, &mut client).await?;

        // map to track the oneshot response sender for each request along with the message info
        let sender_map = Arc::new(Mutex::new(HashMap::new()));

        // background task to receive responses from the server and send them to the appropriate
        // mpsc sender based on the id
        let sender_map_clone = Arc::clone(&sender_map);
        tokio::spawn(async move {
            Self::receive_stream_responses(sender_map_clone, resp_stream).await;
        });

        let mapper = Self {
            read_tx,
            senders: sender_map,
        };
        Ok(mapper)
    }

    /// receive responses from the server and gets the corresponding oneshot sender from the map
    /// and sends the response.
    async fn receive_stream_responses(
        sender_map: StreamResponseSenderMap,
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
                    let _ = sender.send(Err(Error::Grpc(Box::new(e.clone())))).await;
                    pipeline_metrics()
                        .forwarder
                        .udf_error_total
                        .get_or_create(pipeline_metric_labels(VERTEX_TYPE_MAP_UDF))
                        .inc();
                }
                None
            }
        } {
            let (message_info, response_sender) = sender_map
                .lock()
                .expect("failed to acquire poisoned lock")
                .remove(&resp.id)
                .expect("map entry should always be present");

            // once we get eot, we can drop the sender to let the callee
            // know that we are done sending responses
            if let Some(map::TransmissionStatus { eot: true }) = resp.status {
                pipeline_metrics()
                    .forwarder
                    .udf_processing_time
                    .get_or_create(pipeline_metric_labels(VERTEX_TYPE_MAP_UDF))
                    .observe(message_info.start_time.elapsed().as_micros() as f64);
                continue;
            }

            process_stream_response(&sender_map, resp.id, message_info, response_sender, resp.results).await;
        }
    }

    /// Handles the incoming message and sends it to the server for mapping.
    pub(in crate::mapper) async fn stream_map(
        &self,
        message: Message,
        respond_to: mpsc::Sender<Result<Message>>,
    ) {
        let key = message.offset.clone().to_string();
        let msg_info = ParentMessageInfo {
            offset: message.offset.clone(),
            event_time: message.event_time,
            headers: Arc::clone(&message.headers),
            start_time: Instant::now(),
            is_late: message.is_late,
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
            let _ = respond_to
                .send(Err(Error::Mapper(format!(
                    "failed to send message to stream map server: {e}"
                ))))
                .await;
            return;
        }

        self.senders
            .lock()
            .expect("failed to acquire poisoned lock")
            .insert(key.clone(), (msg_info, respond_to));
    }
}

