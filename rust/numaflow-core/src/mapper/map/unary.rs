use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Instant;

use numaflow_pb::clients::map::{MapRequest, MapResponse, map_client::MapClient};
use tokio::sync::{mpsc, oneshot};
use tonic::transport::Channel;
use tonic::Streaming;
use tracing::error;

use crate::config::pipeline::VERTEX_TYPE_MAP_UDF;
use crate::error::{Error, Result};
use crate::message::Message;
use crate::metrics::{pipeline_metric_labels, pipeline_metrics};

use super::{ParentMessageInfo, ResponseSenderMap, create_response_stream};

/// Processes the response from the server and sends it to the appropriate oneshot sender
/// based on the message id entry in the map.
pub(super) async fn process_unary_response(sender_map: &ResponseSenderMap, resp: MapResponse) {
    let msg_id = resp.id;

    let sender_entry = sender_map
        .lock()
        .expect("failed to acquire poisoned lock")
        .remove(&msg_id);

    if let Some((msg_info, sender)) = sender_entry {
        let mut response_messages = vec![];
        for (i, result) in resp.results.into_iter().enumerate() {
            response_messages.push(super::UserDefinedMessage(result, &msg_info, i as i32).into());
        }

        pipeline_metrics()
            .forwarder
            .udf_write_total
            .get_or_create(pipeline_metric_labels(VERTEX_TYPE_MAP_UDF))
            .inc_by(response_messages.len() as u64);

        pipeline_metrics()
            .forwarder
            .udf_processing_time
            .get_or_create(pipeline_metric_labels(VERTEX_TYPE_MAP_UDF))
            .observe(msg_info.start_time.elapsed().as_micros() as f64);

        sender
            .send(Ok(response_messages))
            .expect("failed to send response");
    }
}

/// UserDefinedUnaryMap is a grpc client that sends unary requests to the map server
/// and forwards the responses.
#[derive(Clone)]
pub(in crate::mapper) struct UserDefinedUnaryMap {
    read_tx: mpsc::Sender<MapRequest>,
    senders: ResponseSenderMap,
}

impl UserDefinedUnaryMap {
    /// Performs handshake with the server and creates a new UserDefinedUnaryMap.
    pub(in crate::mapper) async fn new(
        batch_size: usize,
        mut client: MapClient<Channel>,
    ) -> Result<Self> {
        let (read_tx, read_rx) = mpsc::channel(batch_size);
        let resp_stream = create_response_stream(read_tx.clone(), read_rx, &mut client).await?;

        // map to track the oneshot sender for each request along with the message info
        let sender_map = Arc::new(Mutex::new(HashMap::new()));

        // background task to receive responses from the server and send them to the appropriate
        // oneshot sender based on the message id
        let sender_map_clone = Arc::clone(&sender_map);
        tokio::spawn(async move {
            Self::receive_unary_responses(sender_map_clone, resp_stream).await;
        });

        let mapper = Self {
            read_tx,
            senders: sender_map,
        };

        Ok(mapper)
    }

    /// receive responses from the server and gets the corresponding oneshot response sender from the map
    /// and sends the response.
    async fn receive_unary_responses(
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
                    let _ = sender.send(Err(Error::Grpc(Box::new(e.clone()))));
                    pipeline_metrics()
                        .forwarder
                        .udf_error_total
                        .get_or_create(pipeline_metric_labels(VERTEX_TYPE_MAP_UDF))
                        .inc();
                }
                None
            }
        } {
            process_unary_response(&sender_map, resp).await
        }
    }

    /// Handles the incoming message and sends it to the server for mapping.
    pub(in crate::mapper) async fn unary_map(
        &self,
        message: Message,
        respond_to: oneshot::Sender<Result<Vec<Message>>>,
    ) {
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
                "failed to send message to unary map server: {e}"
            ))));
            return;
        }

        // insert the sender into the map
        self.senders
            .lock()
            .expect("failed to acquire poisoned lock")
            .insert(key.clone(), (msg_info, respond_to));
    }
}

