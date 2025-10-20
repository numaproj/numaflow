use crate::config::{get_vertex_name, get_vertex_replica};
use crate::message::{IntOffset, Message, MessageID, Offset};
use crate::reduce::reducer::unaligned::windower::{
    UnalignedWindowMessage, UnalignedWindowOperation, Window,
};
use crate::shared::grpc::{prost_timestamp_from_utc, utc_from_timestamp};
use numaflow_pb::clients::accumulator::accumulator_client::AccumulatorClient;
use numaflow_pb::clients::accumulator::{
    AccumulatorRequest, AccumulatorResponse, accumulator_request,
};

use numaflow_pb::clients::accumulator;
use std::sync::Arc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;
use tonic::transport::Channel;
use tracing::info;

impl From<Message> for accumulator::Payload {
    fn from(msg: Message) -> Self {
        Self {
            keys: msg.keys.to_vec(),
            value: msg.value.to_vec(),
            event_time: Some(prost_timestamp_from_utc(msg.event_time)),
            watermark: msg.watermark.map(prost_timestamp_from_utc),
            id: msg.id.to_string(),
            headers: Arc::unwrap_or_clone(msg.headers),
            metadata: msg.metadata.map(|m| Arc::unwrap_or_clone(m).into()),
        }
    }
}

impl From<Window> for accumulator::KeyedWindow {
    fn from(window: Window) -> Self {
        Self {
            start: Some(prost_timestamp_from_utc(window.start_time)),
            end: Some(prost_timestamp_from_utc(window.end_time)),
            slot: "0".to_string(),
            keys: window.keys.to_vec(),
        }
    }
}

impl From<UnalignedWindowMessage> for AccumulatorRequest {
    fn from(value: UnalignedWindowMessage) -> Self {
        match value.operation {
            UnalignedWindowOperation::Open { message, window } => {
                let operation = Some(accumulator_request::WindowOperation {
                    event: accumulator_request::window_operation::Event::Open as i32,
                    keyed_window: Some(window.into()),
                });

                AccumulatorRequest {
                    payload: Some(message.into()),
                    operation,
                }
            }
            UnalignedWindowOperation::Close { window } => {
                let operation = Some(accumulator_request::WindowOperation {
                    event: accumulator_request::window_operation::Event::Close as i32,
                    keyed_window: Some(window.into()),
                });

                AccumulatorRequest {
                    payload: None,
                    operation,
                }
            }
            UnalignedWindowOperation::Append { message, window } => {
                let operation = Some(accumulator_request::WindowOperation {
                    event: accumulator_request::window_operation::Event::Append as i32,
                    keyed_window: Some(window.into()),
                });

                AccumulatorRequest {
                    payload: Some(message.into()),
                    operation,
                }
            }
            _ => panic!("Unsupported operation for accumulator window"),
        }
    }
}

impl From<AccumulatorResponse> for Message {
    fn from(response: AccumulatorResponse) -> Self {
        let result = response.payload.unwrap_or_default();
        let tags = response.tags;

        Message {
            typ: Default::default(),
            keys: Arc::from(result.keys),
            tags: (!tags.is_empty()).then(|| Arc::from(tags)),
            value: result.value.into(),
            offset: Offset::Int(IntOffset::new(0, 0)),
            event_time: result
                .event_time
                .map(utc_from_timestamp)
                .expect("event time should be present"),
            watermark: result.watermark.map(utc_from_timestamp),
            id: MessageID {
                vertex_name: format!("{}-{}", get_vertex_name(), get_vertex_replica()).into(),
                offset: result.id.into(),
                index: 0,
            },
            headers: Arc::new(result.headers),
            metadata: None,
            is_late: false,
            ack_handle: None,
        }
    }
}

/// User-defined accumulator client.
#[derive(Clone)]
pub(crate) struct UserDefinedAccumulator {
    client: AccumulatorClient<Channel>,
}

impl UserDefinedAccumulator {
    pub(crate) async fn new(client: AccumulatorClient<Channel>) -> Self {
        UserDefinedAccumulator { client }
    }

    /// Calls the reduce_fn on the user-defined accumulator on a separate tokio task.
    /// If the cancellation token is triggered, it will stop processing and return early.
    pub(crate) async fn accumulator_fn(
        &mut self,
        stream: ReceiverStream<AccumulatorRequest>,
        cln_token: CancellationToken,
    ) -> crate::error::Result<(
        ReceiverStream<AccumulatorResponse>,
        tokio::task::JoinHandle<crate::error::Result<()>>,
    )> {
        // Create a channel for responses
        let (result_tx, result_rx) = tokio::sync::mpsc::channel(500);
        let mut client = self.client.clone();

        // Start a background task to process responses
        let handle = tokio::spawn(async move {
            // Call the gRPC reduce_fn with the stream
            let mut response_stream = client
                .accumulate_fn(stream)
                .await
                .map_err(|e| crate::Error::Grpc(Box::new(e)))?
                .into_inner();

            loop {
                tokio::select! {
                    // Check for cancellation
                    _ = cln_token.cancelled() => {
                        info!("Cancellation detected while processing responses, stopping");
                        return Err(crate::Error::Cancelled());
                    }

                    // Process next response
                    response = response_stream.message() => {
                        let response = response.map_err(|e| crate::Error::Grpc(Box::new(e)))?;
                        let Some(response) = response else {
                            break;
                        };

                        if result_tx.send(response).await.is_err() {
                            return Err(crate::Error::Reduce("failed to send response".to_string()));
                        }
                    }
                }
            }

            Ok(())
        });

        // Return the receiver stream and handle immediately
        Ok((ReceiverStream::new(result_rx), handle))
    }

    pub(crate) async fn ready(&mut self) -> bool {
        match self.client.is_ready(tonic::Request::new(())).await {
            Ok(response) => response.into_inner().ready,
            Err(_) => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use chrono::{TimeZone, Utc};
    use numaflow::accumulator;
    use numaflow::shared::ServerExtras;
    use numaflow_pb::clients::accumulator::accumulator_client::AccumulatorClient;
    use tempfile::TempDir;
    use tokio::sync::mpsc;
    use tokio_stream::wrappers::ReceiverStream;
    use tokio_util::sync::CancellationToken;

    use super::*;
    use crate::message::{MessageID, Offset, StringOffset};
    use crate::reduce::reducer::unaligned::windower::{
        UnalignedWindowMessage, UnalignedWindowOperation, Window,
    };
    use crate::shared::grpc::create_rpc_channel;
    use tokio_stream::StreamExt;

    /// Simple accumulator that fires a message every 3 messages with updated key
    struct MessageCounter {
        count: Arc<std::sync::atomic::AtomicU32>,
    }

    struct MessageCounterCreator {}

    impl accumulator::AccumulatorCreator for MessageCounterCreator {
        type A = MessageCounter;

        fn create(&self) -> Self::A {
            MessageCounter::new()
        }
    }

    impl MessageCounter {
        fn new() -> Self {
            Self {
                count: Arc::new(std::sync::atomic::AtomicU32::new(0)),
            }
        }
    }

    #[tonic::async_trait]
    impl accumulator::Accumulator for MessageCounter {
        async fn accumulate(
            &self,
            mut input: mpsc::Receiver<accumulator::AccumulatorRequest>,
            output: mpsc::Sender<accumulator::Message>,
        ) {
            while let Some(request) = input.recv().await {
                // Increment count for each message
                let current_count = self
                    .count
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
                    + 1;

                // Fire a message every 3 messages with updated key
                if current_count % 3 == 0 {
                    let keys = request.keys.clone();
                    let updated_key = format!("{}_{}", keys.join(":"), current_count);

                    let mut message = accumulator::Message::from_accumulator_request(request);
                    message = message.with_keys(vec![updated_key]);
                    message = message.with_value(format!("count_{}", current_count).into_bytes());

                    output.send(message).await.unwrap();
                }
            }
        }
    }

    #[tokio::test]
    async fn test_accumulator_basic() -> crate::Result<()> {
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let tmp_dir = TempDir::new().unwrap();
        let sock_file = tmp_dir.path().join("accumulator.sock");
        let server_info_file = tmp_dir.path().join("accumulator-server-info");

        let server_info = server_info_file.clone();
        let server_socket = sock_file.clone();
        let handle = tokio::spawn(async move {
            accumulator::Server::new(MessageCounterCreator {})
                .with_socket_file(server_socket)
                .with_server_info_file(server_info)
                .start_with_shutdown(shutdown_rx)
                .await
                .expect("server failed");
        });

        // wait for the server to start
        tokio::time::sleep(Duration::from_millis(100)).await;

        let mut client = UserDefinedAccumulator::new(AccumulatorClient::new(
            create_rpc_channel(sock_file).await?,
        ))
        .await;

        // Create test messages
        let message1 = Message {
            typ: Default::default(),
            keys: Arc::from(vec!["key1".into()]),
            tags: None,
            value: "value1".into(),
            offset: Offset::String(StringOffset::new("0".to_string(), 0)),
            event_time: Utc.with_ymd_and_hms(2023, 1, 1, 0, 0, 10).unwrap(),
            watermark: Some(Utc.with_ymd_and_hms(2023, 1, 1, 0, 0, 0).unwrap()),
            id: MessageID {
                vertex_name: "vertex_name".to_string().into(),
                offset: "0".to_string().into(),
                index: 0,
            },
            ..Default::default()
        };

        let message2 = Message {
            typ: Default::default(),
            keys: Arc::from(vec!["key1".into()]),
            tags: None,
            value: "value2".into(),
            offset: Offset::String(StringOffset::new("1".to_string(), 1)),
            event_time: Utc.with_ymd_and_hms(2023, 1, 1, 0, 0, 20).unwrap(),
            watermark: Some(Utc.with_ymd_and_hms(2023, 1, 1, 0, 0, 0).unwrap()),
            id: MessageID {
                vertex_name: "vertex_name".to_string().into(),
                offset: "1".to_string().into(),
                index: 1,
            },
            ..Default::default()
        };

        let message3 = Message {
            typ: Default::default(),
            keys: Arc::from(vec!["key1".into()]),
            tags: None,
            value: "value3".into(),
            offset: Offset::String(StringOffset::new("2".to_string(), 2)),
            event_time: Utc.with_ymd_and_hms(2023, 1, 1, 0, 0, 30).unwrap(),
            watermark: Some(Utc.with_ymd_and_hms(2023, 1, 1, 0, 0, 0).unwrap()),
            id: MessageID {
                vertex_name: "vertex_name".to_string().into(),
                offset: "2".to_string().into(),
                index: 2,
            },
            ..Default::default()
        };

        // Create an accumulator window
        let window = Window::new(
            Utc.with_ymd_and_hms(2023, 1, 1, 0, 0, 0).unwrap(),
            Utc.with_ymd_and_hms(2023, 1, 1, 0, 1, 0).unwrap(),
            Arc::from(vec!["key1".to_string()]),
        );

        // Create window operations
        let open_message = UnalignedWindowMessage {
            operation: UnalignedWindowOperation::Open {
                message: message1.clone(),
                window: window.clone(),
            },
            pnf_slot: "GLOBAL_SLOT",
        };

        let append_message1 = UnalignedWindowMessage {
            operation: UnalignedWindowOperation::Append {
                message: message2.clone(),
                window: window.clone(),
            },
            pnf_slot: "GLOBAL_SLOT",
        };

        let append_message2 = UnalignedWindowMessage {
            operation: UnalignedWindowOperation::Append {
                message: message3.clone(),
                window: window.clone(),
            },
            pnf_slot: "GLOBAL_SLOT",
        };

        let close_message = UnalignedWindowMessage {
            operation: UnalignedWindowOperation::Close {
                window: window.clone(),
            },
            pnf_slot: "GLOBAL_SLOT",
        };

        // Create a channel to send window messages
        let (window_tx, window_rx) = mpsc::channel(10);
        window_tx.send(open_message.into()).await.unwrap();
        window_tx.send(append_message1.into()).await.unwrap();
        window_tx.send(append_message2.into()).await.unwrap();
        window_tx.send(close_message.into()).await.unwrap();
        drop(window_tx); // Close the channel to signal end of input

        // Create a cancellation token
        let cln_token = CancellationToken::new();

        // Call reduce_fn
        let (mut response_stream, accumulator_handle) = client
            .accumulator_fn(ReceiverStream::new(window_rx), cln_token)
            .await
            .expect("reduce_fn failed");

        // Wait for the result (should get one message after 3 inputs)
        let result = tokio::time::timeout(Duration::from_secs(5), response_stream.next())
            .await
            .expect("timeout waiting for result")
            .expect("no result received");

        // Convert response to message
        let result_message: Message = result.into();

        // Verify the result - should have updated key and count value
        assert_eq!(result_message.keys.to_vec(), vec!["key1_3"]);
        assert_eq!(
            String::from_utf8(result_message.value.to_vec()).unwrap(),
            "count_3"
        );

        // Shutdown the server
        shutdown_tx
            .send(())
            .expect("failed to send shutdown signal");

        tokio::time::sleep(Duration::from_millis(100)).await;

        accumulator_handle
            .await
            .expect("accumulator handle failed")
            .expect("accumulator failed");

        // Wait for the handle to complete
        handle.await.expect("handle failed");

        Ok(())
    }

    #[tokio::test]
    async fn test_accumulator_multiple_keys() -> crate::Result<()> {
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let tmp_dir = TempDir::new().unwrap();
        let sock_file = tmp_dir.path().join("accumulator_multi.sock");
        let server_info_file = tmp_dir.path().join("accumulator_multi-server-info");

        let server_info = server_info_file.clone();
        let server_socket = sock_file.clone();
        let _handle = tokio::spawn(async move {
            accumulator::Server::new(MessageCounterCreator {})
                .with_socket_file(server_socket)
                .with_server_info_file(server_info)
                .start_with_shutdown(shutdown_rx)
                .await
                .expect("server failed");
        });

        // wait for the server to start
        tokio::time::sleep(Duration::from_millis(100)).await;

        let mut client = UserDefinedAccumulator::new(AccumulatorClient::new(
            create_rpc_channel(sock_file).await?,
        ))
        .await;

        // Create test messages with different keys
        let messages = vec![
            Message {
                typ: Default::default(),
                keys: Arc::from(vec!["keyA".into()]),
                tags: None,
                value: "valueA1".into(),
                offset: Offset::String(StringOffset::new("0".to_string(), 0)),
                event_time: Utc.with_ymd_and_hms(2023, 1, 1, 0, 0, 10).unwrap(),
                watermark: Some(Utc.with_ymd_and_hms(2023, 1, 1, 0, 0, 0).unwrap()),
                id: MessageID {
                    vertex_name: "vertex_name".to_string().into(),
                    offset: "0".to_string().into(),
                    index: 0,
                },
                ..Default::default()
            },
            Message {
                typ: Default::default(),
                keys: Arc::from(vec!["keyB".into()]),
                tags: None,
                value: "valueB1".into(),
                offset: Offset::String(StringOffset::new("1".to_string(), 1)),
                event_time: Utc.with_ymd_and_hms(2023, 1, 1, 0, 0, 20).unwrap(),
                watermark: Some(Utc.with_ymd_and_hms(2023, 1, 1, 0, 0, 0).unwrap()),
                id: MessageID {
                    vertex_name: "vertex_name".to_string().into(),
                    offset: "1".to_string().into(),
                    index: 1,
                },
                ..Default::default()
            },
            Message {
                typ: Default::default(),
                keys: Arc::from(vec!["keyA".into()]),
                tags: None,
                value: "valueA2".into(),
                offset: Offset::String(StringOffset::new("2".to_string(), 2)),
                event_time: Utc.with_ymd_and_hms(2023, 1, 1, 0, 0, 30).unwrap(),
                watermark: Some(Utc.with_ymd_and_hms(2023, 1, 1, 0, 0, 0).unwrap()),
                id: MessageID {
                    vertex_name: "vertex_name".to_string().into(),
                    offset: "2".to_string().into(),
                    index: 2,
                },
                ..Default::default()
            },
            Message {
                typ: Default::default(),
                keys: Arc::from(vec!["keyA".into()]),
                tags: None,
                value: "valueA3".into(),
                offset: Offset::String(StringOffset::new("3".to_string(), 3)),
                event_time: Utc.with_ymd_and_hms(2023, 1, 1, 0, 0, 40).unwrap(),
                watermark: Some(Utc.with_ymd_and_hms(2023, 1, 1, 0, 0, 0).unwrap()),
                id: MessageID {
                    vertex_name: "vertex_name".to_string().into(),
                    offset: "3".to_string().into(),
                    index: 3,
                },
                ..Default::default()
            },
            Message {
                typ: Default::default(),
                keys: Arc::from(vec!["keyB".into()]),
                tags: None,
                value: "valueB2".into(),
                offset: Offset::String(StringOffset::new("4".to_string(), 4)),
                event_time: Utc.with_ymd_and_hms(2023, 1, 1, 0, 0, 50).unwrap(),
                watermark: Some(Utc.with_ymd_and_hms(2023, 1, 1, 0, 0, 0).unwrap()),
                id: MessageID {
                    vertex_name: "vertex_name".to_string().into(),
                    offset: "4".to_string().into(),
                    index: 4,
                },
                ..Default::default()
            },
            Message {
                typ: Default::default(),
                keys: Arc::from(vec!["keyB".into()]),
                tags: None,
                value: "valueB3".into(),
                offset: Offset::String(StringOffset::new("5".to_string(), 5)),
                event_time: Utc.with_ymd_and_hms(2023, 1, 1, 0, 1, 0).unwrap(),
                watermark: Some(Utc.with_ymd_and_hms(2023, 1, 1, 0, 0, 0).unwrap()),
                id: MessageID {
                    vertex_name: "vertex_name".to_string().into(),
                    offset: "5".to_string().into(),
                    index: 5,
                },
                ..Default::default()
            },
        ];

        // Create an accumulator window
        let window = Window::new(
            Utc.with_ymd_and_hms(2023, 1, 1, 0, 0, 0).unwrap(),
            Utc.with_ymd_and_hms(2023, 1, 1, 0, 2, 0).unwrap(),
            Arc::from(vec!["mixed_keys".to_string()]),
        );

        // Create a channel to send window messages
        let (window_tx, window_rx) = mpsc::channel(20);

        // Send open message with first message
        window_tx
            .send(
                UnalignedWindowMessage {
                    operation: UnalignedWindowOperation::Open {
                        message: messages[0].clone(),
                        window: window.clone(),
                    },
                    pnf_slot: "GLOBAL_SLOT",
                }
                .into(),
            )
            .await
            .unwrap();

        // Send append messages for the rest
        for msg in &messages[1..] {
            window_tx
                .send(
                    UnalignedWindowMessage {
                        operation: UnalignedWindowOperation::Append {
                            message: msg.clone(),
                            window: window.clone(),
                        },
                        pnf_slot: "GLOBAL_SLOT",
                    }
                    .into(),
                )
                .await
                .unwrap();
        }

        // Send close message
        window_tx
            .send(
                UnalignedWindowMessage {
                    operation: UnalignedWindowOperation::Close {
                        window: window.clone(),
                    },
                    pnf_slot: "GLOBAL_SLOT",
                }
                .into(),
            )
            .await
            .unwrap();

        drop(window_tx); // Close the channel to signal end of input

        // Create a cancellation token
        let cln_token = CancellationToken::new();

        // Call reduce_fn
        let (mut response_stream, handle) = client
            .accumulator_fn(ReceiverStream::new(window_rx), cln_token)
            .await
            .expect("reduce_fn failed");

        // Collect results - should get 2 messages (at count 3 and 6)
        let mut results = Vec::new();
        while let Some(result) =
            tokio::time::timeout(Duration::from_secs(5), response_stream.next())
                .await
                .expect("timeout waiting for result")
        {
            results.push(result);
            if results.len() >= 2 {
                break;
            }
        }

        // Should have 2 results
        assert_eq!(results.len(), 2);

        // Convert responses to messages
        let result_messages: Vec<Message> = results.into_iter().map(Into::into).collect();

        // First result should be at count 3
        assert_eq!(result_messages[0].keys.to_vec(), vec!["keyA_3"]);
        assert_eq!(
            String::from_utf8(result_messages[0].value.to_vec()).unwrap(),
            "count_3"
        );

        // Second result should be at count 6
        assert_eq!(result_messages[1].keys.to_vec(), vec!["keyB_6"]);
        assert_eq!(
            String::from_utf8(result_messages[1].value.to_vec()).unwrap(),
            "count_6"
        );

        // Wait for the handle to complete
        handle
            .await
            .expect("handle failed")
            .expect("reduce_fn task failed");

        // Shutdown the server
        shutdown_tx
            .send(())
            .expect("failed to send shutdown signal");
        tokio::time::sleep(Duration::from_millis(50)).await;

        Ok(())
    }
}
