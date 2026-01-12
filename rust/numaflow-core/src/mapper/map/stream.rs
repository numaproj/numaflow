use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Instant;

use numaflow_pb::clients::map::{self, MapRequest, MapResponse, map_client::MapClient};
use tokio::sync::mpsc;
use tokio_util::task::AbortOnDropHandle;
use tonic::Streaming;
use tonic::transport::Channel;
use tracing::error;

use crate::config::pipeline::VERTEX_TYPE_MAP_UDF;
use crate::error::{Error, Result};
use crate::message::Message;
use crate::metrics::{pipeline_metric_labels, pipeline_metrics};

use super::{
    ParentMessageInfo, StreamResponseSenderMap, UserDefinedMessage, create_response_stream,
};

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

        // map to track the oneshot response sender for each request along with the message info
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

            Self::process_stream_response(
                &sender_map,
                resp.id,
                message_info,
                response_sender,
                resp.results,
            )
            .await;
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

    /// Processes stream responses and sends them to the appropriate mpsc sender
    async fn process_stream_response(
        sender_map: &StreamResponseSenderMap,
        msg_id: String,
        mut message_info: ParentMessageInfo,
        response_sender: mpsc::Sender<Result<Message>>,
        results: Vec<map::map_response::Result>,
    ) {
        for result in results.into_iter() {
            response_sender
                .send(Ok(UserDefinedMessage(
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
}

#[cfg(test)]
mod tests {
    use crate::mapper::map::stream::UserDefinedStreamMap;
    use crate::message::{MessageID, StringOffset};
    use crate::shared::grpc::create_rpc_channel;
    use numaflow::mapstream;
    use numaflow::shared::ServerExtras;
    use numaflow_pb::clients::map::map_client::MapClient;
    use std::error::Error;
    use std::sync::Arc;
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

        let message = crate::message::Message {
            typ: Default::default(),
            keys: Arc::from(vec!["first".into()]),
            tags: None,
            value: "test,map,stream".into(),
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

        let (tx, mut rx) = tokio::sync::mpsc::channel(3);

        tokio::time::timeout(Duration::from_secs(2), client.stream_map(message, tx)).await?;

        let mut responses = vec![];
        while let Some(response) = rx.recv().await {
            responses.push(response?);
        }

        assert_eq!(responses.len(), 3);
        // convert the bytes value to string and compare
        let values: Vec<String> = responses
            .iter()
            .map(|r| String::from_utf8(Vec::from(r.value.clone())).unwrap())
            .collect();
        assert_eq!(values, vec!["test", "map", "stream"]);

        // Verify that message indices are properly incremented
        let indices: Vec<i32> = responses.iter().map(|r| r.id.index).collect();
        assert_eq!(indices, vec![0, 1, 2]);

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
