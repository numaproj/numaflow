use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Instant;

use numaflow_pb::clients::map::{MapRequest, MapResponse, map_client::MapClient};
use tokio::sync::{mpsc, oneshot};
use tokio_util::task::AbortOnDropHandle;
use tonic::Streaming;
use tonic::transport::Channel;
use tracing::error;

use crate::config::pipeline::VERTEX_TYPE_MAP_UDF;
use crate::error::{Error, Result};
use crate::message::Message;
use crate::metrics::{pipeline_metric_labels, pipeline_metrics};

use super::{ParentMessageInfo, ResponseSenderMap, create_response_stream};

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

        // map to track the oneshot sender for each request along with the message info
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
            Self::process_unary_response(&sender_map, resp).await
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
                response_messages
                    .push(super::UserDefinedMessage(result, &msg_info, i as i32).into());
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
}

#[cfg(test)]
mod tests {
    use crate::mapper::map::unary::UserDefinedUnaryMap;
    use crate::message::{MessageID, StringOffset};
    use crate::shared::grpc::create_rpc_channel;
    use numaflow::map;
    use numaflow::shared::ServerExtras;
    use numaflow_pb::clients::map::map_client::MapClient;
    use std::error::Error;
    use std::sync::Arc;
    use std::time::Duration;
    use tempfile::TempDir;

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

        let message = crate::message::Message {
            typ: Default::default(),
            keys: Arc::from(vec!["first".into()]),
            tags: None,
            value: "hello".into(),
            offset: crate::message::Offset::String(StringOffset::new("0".to_string(), 0)),
            event_time: chrono::Utc::now(),
            watermark: None,
            id: MessageID {
                vertex_name: "vertex_name".to_string().into(),
                offset: "0".to_string().into(),
                index: 0,
            },
            ..Default::default()
        };

        let (tx, rx) = tokio::sync::oneshot::channel();

        tokio::time::timeout(Duration::from_secs(2), client.unary_map(message, tx))
            .await
            .unwrap();

        let messages = rx.await.unwrap();
        assert!(messages.is_ok());
        assert_eq!(messages?.len(), 1);

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
