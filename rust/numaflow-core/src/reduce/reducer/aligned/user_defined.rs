use crate::config::{get_vertex_name, get_vertex_replica};
use crate::message::{IntOffset, Message, MessageID, Offset};
use crate::reduce::reducer::aligned::windower::{AlignedWindowMessage, AlignedWindowOperation};
use crate::shared::grpc::{prost_timestamp_from_utc, utc_from_timestamp};
use crate::{Result, jh_abort_guard};
use numaflow_pb::clients::reduce::reduce_client::ReduceClient;
use numaflow_pb::clients::reduce::{ReduceRequest, ReduceResponse, reduce_request};
use std::ops::Sub;
use std::sync::Arc;
use tokio::task::JoinHandle;
use tokio_stream::StreamExt;
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;
use tonic::transport::Channel;
use tracing::info;

impl From<Message> for reduce_request::Payload {
    fn from(msg: Message) -> Self {
        Self {
            keys: msg.keys.to_vec(),
            value: msg.value.to_vec(),
            event_time: Some(prost_timestamp_from_utc(msg.event_time)),
            watermark: msg.watermark.map(prost_timestamp_from_utc),
            headers: Arc::unwrap_or_clone(msg.headers),
            metadata: msg.metadata.map(|m| Arc::unwrap_or_clone(m).into()),
        }
    }
}

impl From<&Message> for reduce_request::Payload {
    fn from(msg: &Message) -> Self {
        Self {
            keys: msg.keys.to_vec(),
            value: msg.value.to_vec(),
            event_time: Some(prost_timestamp_from_utc(msg.event_time)),
            watermark: msg.watermark.map(prost_timestamp_from_utc),
            headers: (*msg.headers).clone(),
            metadata: msg.metadata.clone().map(|m| (*m).clone().into()),
        }
    }
}

impl From<AlignedWindowMessage> for ReduceRequest {
    fn from(value: AlignedWindowMessage) -> Self {
        // Process the operation and extract window and message
        match value.operation {
            AlignedWindowOperation::Open { message, window } => {
                let window_obj = numaflow_pb::clients::reduce::Window {
                    start: Some(prost_timestamp_from_utc(window.start_time)),
                    end: Some(prost_timestamp_from_utc(window.end_time)),
                    slot: "0".to_string(),
                };

                let operation = Some(reduce_request::WindowOperation {
                    event: reduce_request::window_operation::Event::Open as i32,
                    windows: vec![window_obj],
                });

                ReduceRequest {
                    payload: Some(message.into()),
                    operation,
                }
            }
            AlignedWindowOperation::Close { window } => {
                let window_obj = numaflow_pb::clients::reduce::Window {
                    start: Some(prost_timestamp_from_utc(window.start_time)),
                    end: Some(prost_timestamp_from_utc(window.end_time)),
                    slot: "0".to_string(),
                };

                let operation = Some(reduce_request::WindowOperation {
                    event: reduce_request::window_operation::Event::Close as i32,
                    windows: vec![window_obj],
                });

                // For Close operations, we still need to set payload to Some
                // even though there's no message, to ensure consistent behavior
                ReduceRequest {
                    payload: Some(reduce_request::Payload {
                        keys: vec![],
                        value: vec![],
                        event_time: None,
                        watermark: None,
                        headers: Default::default(),
                        metadata: None,
                    }),
                    operation,
                }
            }
            AlignedWindowOperation::Append { message, window } => {
                let window_obj = numaflow_pb::clients::reduce::Window {
                    start: Some(prost_timestamp_from_utc(window.start_time)),
                    end: Some(prost_timestamp_from_utc(window.end_time)),
                    slot: "0".to_string(),
                };

                let operation = Some(reduce_request::WindowOperation {
                    event: reduce_request::window_operation::Event::Append as i32,
                    windows: vec![window_obj],
                });

                ReduceRequest {
                    payload: Some(message.into()),
                    operation,
                }
            }
        }
    }
}

/// Wrapper for ReduceResponse that includes index and vertex name.
struct UdReducerResponse {
    pub response: ReduceResponse,
    pub index: i32,
    pub vertex_name: &'static str,
    pub vertex_replica: u16,
}

impl From<UdReducerResponse> for Message {
    fn from(wrapper: UdReducerResponse) -> Self {
        let result = wrapper.response.result.unwrap_or_default();
        let window = wrapper.response.window.unwrap_or_default();

        // Create offset from window start and end time
        let offset_str = format!(
            "{}-{}",
            utc_from_timestamp(window.start.expect("window start missing")).timestamp_millis(),
            utc_from_timestamp(window.end.expect("window end missing")).timestamp_millis()
        );

        Message {
            typ: Default::default(),
            keys: Arc::from(result.keys),
            tags: if result.tags.is_empty() {
                None
            } else {
                Some(Arc::from(result.tags))
            },
            value: result.value.into(),
            offset: Offset::Int(IntOffset::new(0, 0)),
            event_time: utc_from_timestamp(window.end.unwrap())
                .sub(chrono::Duration::milliseconds(1)),
            watermark: window
                .end
                .map(|ts| utc_from_timestamp(ts) - chrono::Duration::milliseconds(1)),
            // this will be unique for each response which will be used for dedup (index is used because
            // each window can have multiple reduce responses)
            id: MessageID {
                vertex_name: format!("{}-{}", wrapper.vertex_name, wrapper.vertex_replica).into(),
                offset: offset_str.into(),
                index: wrapper.index,
            },
            ..Default::default()
        }
    }
}

/// User-defined aligned reduce client.
#[derive(Clone)]
pub(crate) struct UserDefinedAlignedReduce {
    client: ReduceClient<Channel>,
}

impl UserDefinedAlignedReduce {
    pub(crate) async fn new(client: ReduceClient<Channel>) -> Self {
        UserDefinedAlignedReduce { client }
    }

    /// Calls the reduce_fn on the user-defined reducer on a separate tokio task.
    /// If the cancellation token is triggered, it will stop processing and return early.
    pub(crate) async fn reduce_fn(
        &mut self,
        stream: ReceiverStream<AlignedWindowMessage>,
        result_tx: tokio::sync::mpsc::Sender<Message>,
        cln_token: CancellationToken,
    ) -> Result<()> {
        // Convert AlignedWindowMessage stream to ReduceRequest stream
        let (req_tx, req_rx) = tokio::sync::mpsc::channel(500);

        // Spawn a task to convert AlignedWindowMessages to ReduceRequests and send them to req_tx
        // NOTE: - This is not really required (for client side streaming reduce), we do this because
        //         `tonic` does not return a stream from the reduce_fn unless there is some output.
        //       - This implementation works for reduce bidi streaming.
        let request_handle = tokio::spawn(async move {
            let mut stream = stream;
            while let Some(window_msg) = stream.next().await {
                let reduce_req: ReduceRequest = window_msg.into();
                if req_tx.send(reduce_req).await.is_err() {
                    break;
                }
            }
        });

        // Create a guard that will automatically abort the request handle when this function returns
        let _guard = jh_abort_guard!(request_handle);

        // Call the gRPC reduce_fn with the converted stream, but also watch for cancellation
        let mut response_stream = tokio::select! {
            // Wait for the gRPC call to complete
            result = self.client.reduce_fn(ReceiverStream::new(req_rx)) => {
                match result {
                    Ok(response) => response.into_inner(),
                    Err(e) => {
                        return Err(crate::Error::Grpc(Box::new(e)));
                    }
                }
            }

            // Check for cancellation
            _ = cln_token.cancelled() => {
                info!("Cancellation detected while waiting for reduce_fn response");
                return Err(crate::Error::Cancelled());
            }
        };

        // Process the response stream
        let vertex_name = get_vertex_name();
        let mut index = 0;

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

                    if response.eof {
                        break;
                    }

                    // convert to Message so it can be sent to the ISB write channel
                    let message: Message = UdReducerResponse {
                        response,
                        index,
                        vertex_name,
                        vertex_replica: *get_vertex_replica(),
                    }
                    .into();

                    result_tx
                        .send(message)
                        .await
                        .expect("failed to send response");

                    index += 1;
                }
            }
        }

        Ok(())
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
    use numaflow::reduce;
    use numaflow::shared::ServerExtras;
    use numaflow_pb::clients::reduce::reduce_client::ReduceClient;
    use tempfile::TempDir;
    use tokio::sync::mpsc;
    use tokio_stream::wrappers::ReceiverStream;
    use tokio_util::sync::CancellationToken;

    use super::*;
    use crate::message::{MessageID, StringOffset};
    use crate::reduce::reducer::aligned::windower::{
        AlignedWindowOperation, Window, window_pnf_slot,
    };
    use crate::shared::grpc::create_rpc_channel;

    struct Counter {}

    struct CounterCreator {}

    impl reduce::ReducerCreator for CounterCreator {
        type R = Counter;

        fn create(&self) -> Self::R {
            Counter::new()
        }
    }

    impl Counter {
        fn new() -> Self {
            Self {}
        }
    }

    #[tonic::async_trait]
    impl reduce::Reducer for Counter {
        async fn reduce(
            &self,
            keys: Vec<String>,
            mut input: mpsc::Receiver<reduce::ReduceRequest>,
            _md: &reduce::Metadata,
        ) -> Vec<reduce::Message> {
            let mut counter = 0;
            // the loop exits when input is closed which will happen only on close of book.
            while input.recv().await.is_some() {
                counter += 1;
            }
            vec![reduce::Message::new(counter.to_string().into_bytes()).with_keys(keys.clone())]
        }
    }

    #[tokio::test]
    async fn test_reduce_operations() -> Result<()> {
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let tmp_dir = TempDir::new().unwrap();
        let sock_file = tmp_dir.path().join("reduce.sock");
        let server_info_file = tmp_dir.path().join("reduce-server-info");

        let server_info = server_info_file.clone();
        let server_socket = sock_file.clone();
        let handle = tokio::spawn(async move {
            reduce::Server::new(CounterCreator {})
                .with_socket_file(server_socket)
                .with_server_info_file(server_info)
                .start_with_shutdown(shutdown_rx)
                .await
                .expect("server failed");
        });

        // wait for the server to start
        tokio::time::sleep(Duration::from_millis(100)).await;

        let mut client =
            UserDefinedAlignedReduce::new(ReduceClient::new(create_rpc_channel(sock_file).await?))
                .await;

        // Create a window
        let window_start = Utc.with_ymd_and_hms(2023, 1, 1, 0, 0, 0).unwrap();
        let window_end = Utc.with_ymd_and_hms(2023, 1, 1, 0, 1, 0).unwrap();
        let window = Window::new(window_start, window_end);

        // Create messages
        let messages = vec![
            Message {
                typ: Default::default(),
                keys: Arc::from(vec!["key1".into()]),
                tags: None,
                value: "value1".into(),
                offset: Offset::String(StringOffset::new("0".to_string(), 0)),
                event_time: window_start + chrono::Duration::seconds(10),
                watermark: None,
                id: MessageID {
                    vertex_name: "vertex_name".to_string().into(),
                    offset: "0".to_string().into(),
                    index: 0,
                },
                ..Default::default()
            },
            Message {
                typ: Default::default(),
                keys: Arc::from(vec!["key1".into()]),
                tags: None,
                value: "value2".into(),
                offset: Offset::String(StringOffset::new("1".to_string(), 1)),
                event_time: window_start + chrono::Duration::seconds(20),
                watermark: None,
                id: MessageID {
                    vertex_name: "vertex_name".to_string().into(),
                    offset: "1".to_string().into(),
                    index: 1,
                },
                ..Default::default()
            },
            Message {
                typ: Default::default(),
                keys: Arc::from(vec!["key1".into()]),
                tags: None,
                value: "value3".into(),
                offset: Offset::String(StringOffset::new("2".to_string(), 2)),
                event_time: window_start + chrono::Duration::seconds(30),
                watermark: None,
                id: MessageID {
                    vertex_name: "vertex_name".to_string().into(),
                    offset: "2".to_string().into(),
                    index: 2,
                },
                ..Default::default()
            },
        ];

        // Create window messages
        let window_messages = vec![
            // Open window
            AlignedWindowMessage {
                operation: AlignedWindowOperation::Open {
                    message: messages[0].clone(),
                    window: window.clone(),
                },
                pnf_slot: window_pnf_slot(&window),
            },
            // Append messages
            AlignedWindowMessage {
                operation: AlignedWindowOperation::Append {
                    message: messages[1].clone(),
                    window: window.clone(),
                },
                pnf_slot: window_pnf_slot(&window),
            },
            AlignedWindowMessage {
                operation: AlignedWindowOperation::Append {
                    message: messages[2].clone(),
                    window: window.clone(),
                },
                pnf_slot: window_pnf_slot(&window),
            },
        ];

        // Create a channel to send window messages
        let (window_tx, window_rx) = mpsc::channel(10);
        for msg in window_messages {
            window_tx.send(msg).await.unwrap();
        }
        drop(window_tx); // Close the channel to signal end of input

        // Create a channel to receive results
        let (result_tx, mut result_rx) = mpsc::channel(10);

        // Create a cancellation token
        let cln_token = CancellationToken::new();

        // Call reduce_fn
        let reduce_handle = tokio::spawn(async move {
            client
                .reduce_fn(ReceiverStream::new(window_rx), result_tx, cln_token)
                .await
                .expect("reduce_fn failed");
        });

        // Wait for the result
        let result = tokio::time::timeout(Duration::from_secs(5), result_rx.recv())
            .await
            .expect("timeout waiting for result")
            .expect("no result received");

        // Verify the result
        assert_eq!(result.keys.to_vec(), vec!["key1"]);
        assert_eq!(String::from_utf8(result.value.to_vec()).unwrap(), "3"); // Counter should be 3

        // Shutdown the server
        shutdown_tx
            .send(())
            .expect("failed to send shutdown signal");
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Wait for the reduce handle to complete
        reduce_handle.await.expect("reduce handle failed");

        // Wait for the server to shut down
        assert!(
            handle.is_finished(),
            "Expected gRPC server to have shut down"
        );

        Ok(())
    }

    #[tokio::test]
    async fn reduce_operations_with_multiple_keys() -> Result<()> {
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let tmp_dir = TempDir::new().unwrap();
        let sock_file = tmp_dir.path().join("reduce_multi.sock");
        let server_info_file = tmp_dir.path().join("reduce_multi-server-info");

        let server_info = server_info_file.clone();
        let server_socket = sock_file.clone();
        let handle = tokio::spawn(async move {
            reduce::Server::new(CounterCreator {})
                .with_socket_file(server_socket)
                .with_server_info_file(server_info)
                .start_with_shutdown(shutdown_rx)
                .await
                .expect("server failed");
        });

        // wait for the server to start
        tokio::time::sleep(Duration::from_millis(100)).await;

        let mut client =
            UserDefinedAlignedReduce::new(ReduceClient::new(create_rpc_channel(sock_file).await?))
                .await;

        // Create two windows for different keys
        let window_start = Utc.with_ymd_and_hms(2023, 1, 1, 0, 0, 0).unwrap();
        let window_end = Utc.with_ymd_and_hms(2023, 1, 1, 0, 1, 0).unwrap();
        let window = Window::new(window_start, window_end);

        // Create messages for key1
        let key1_messages = vec![
            Message {
                typ: Default::default(),
                keys: Arc::from(vec!["key1".into()]),
                tags: None,
                value: "value1".into(),
                offset: Offset::String(StringOffset::new("0".to_string(), 0)),
                event_time: window_start + chrono::Duration::seconds(10),
                watermark: None,
                id: MessageID {
                    vertex_name: "vertex_name".to_string().into(),
                    offset: "0".to_string().into(),
                    index: 0,
                },
                ..Default::default()
            },
            Message {
                typ: Default::default(),
                keys: Arc::from(vec!["key1".into()]),
                tags: None,
                value: "value2".into(),
                offset: Offset::String(StringOffset::new("1".to_string(), 1)),
                event_time: window_start + chrono::Duration::seconds(20),
                watermark: None,
                id: MessageID {
                    vertex_name: "vertex_name".to_string().into(),
                    offset: "1".to_string().into(),
                    index: 1,
                },
                ..Default::default()
            },
        ];

        // Create messages for key2
        let key2_messages = vec![
            Message {
                typ: Default::default(),
                keys: Arc::from(vec!["key2".into()]),
                tags: None,
                value: "value3".into(),
                offset: Offset::String(StringOffset::new("2".to_string(), 2)),
                event_time: window_start + chrono::Duration::seconds(30),
                watermark: None,
                id: MessageID {
                    vertex_name: "vertex_name".to_string().into(),
                    offset: "2".to_string().into(),
                    index: 2,
                },
                ..Default::default()
            },
            Message {
                typ: Default::default(),
                keys: Arc::from(vec!["key2".into()]),
                tags: None,
                value: "value4".into(),
                offset: Offset::String(StringOffset::new("3".to_string(), 3)),
                event_time: window_start + chrono::Duration::seconds(40),
                watermark: None,
                id: MessageID {
                    vertex_name: "vertex_name".to_string().into(),
                    offset: "3".to_string().into(),
                    index: 3,
                },
                ..Default::default()
            },
            Message {
                typ: Default::default(),
                keys: Arc::from(vec!["key2".into()]),
                tags: None,
                value: "value5".into(),
                offset: Offset::String(StringOffset::new("4".to_string(), 4)),
                event_time: window_start + chrono::Duration::seconds(50),
                watermark: None,
                id: MessageID {
                    vertex_name: "vertex_name".to_string().into(),
                    offset: "4".to_string().into(),
                    index: 4,
                },
                ..Default::default()
            },
        ];

        // Create window messages
        let window_messages = vec![
            // Open window for key1
            AlignedWindowMessage {
                operation: AlignedWindowOperation::Open {
                    message: key1_messages[0].clone(),
                    window: window.clone(),
                },
                pnf_slot: window_pnf_slot(&window),
            },
            // Append message for key1
            AlignedWindowMessage {
                operation: AlignedWindowOperation::Append {
                    message: key1_messages[1].clone(),
                    window: window.clone(),
                },
                pnf_slot: window_pnf_slot(&window),
            },
            // Open window for key2
            AlignedWindowMessage {
                operation: AlignedWindowOperation::Open {
                    message: key2_messages[0].clone(),
                    window: window.clone(),
                },
                pnf_slot: window_pnf_slot(&window),
            },
            // Append messages for key2
            AlignedWindowMessage {
                operation: AlignedWindowOperation::Append {
                    message: key2_messages[1].clone(),
                    window: window.clone(),
                },
                pnf_slot: window_pnf_slot(&window),
            },
            AlignedWindowMessage {
                operation: AlignedWindowOperation::Append {
                    message: key2_messages[2].clone(),
                    window: window.clone(),
                },
                pnf_slot: window_pnf_slot(&window),
            },
        ];

        // Create a channel to send window messages
        let (window_tx, window_rx) = mpsc::channel(100);
        for msg in window_messages {
            window_tx.send(msg).await.unwrap();
        }
        drop(window_tx); // Close the channel to signal end of input

        // Create a channel to receive results
        let (result_tx, mut result_rx) = mpsc::channel(100);

        // Create a cancellation token
        let cln_token = CancellationToken::new();

        // Call reduce_fn
        let reduce_handle = tokio::spawn(async move {
            client
                .reduce_fn(ReceiverStream::new(window_rx), result_tx, cln_token)
                .await
                .expect("reduce_fn failed");
        });

        // Collect all results
        let mut results = Vec::new();
        while let Some(result) = tokio::time::timeout(Duration::from_secs(2), result_rx.recv())
            .await
            .expect("timeout waiting for result")
        {
            results.push(result);
        }

        // Verify the results
        assert_eq!(results.len(), 2);

        // Sort results by key for deterministic testing
        results.sort_by(|a, b| a.keys.to_vec().cmp(&b.keys.to_vec()));

        // Check key1 result
        assert_eq!(results[0].keys.to_vec(), vec!["key1"]);
        assert_eq!(String::from_utf8(results[0].value.to_vec()).unwrap(), "2"); // Counter should be 2

        // Check key2 result
        assert_eq!(results[1].keys.to_vec(), vec!["key2"]);
        assert_eq!(String::from_utf8(results[1].value.to_vec()).unwrap(), "3"); // Counter should be 3

        // Shutdown the server
        shutdown_tx
            .send(())
            .expect("failed to send shutdown signal");
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Wait for the reduce handle to complete
        reduce_handle.await.expect("reduce handle failed");

        // Wait for the server to shut down
        assert!(
            handle.is_finished(),
            "Expected gRPC server to have shut down"
        );

        Ok(())
    }

    #[tokio::test]
    async fn reduce_operations_with_sliding_window() -> Result<()> {
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let tmp_dir = TempDir::new().unwrap();
        let sock_file = tmp_dir.path().join("reduce_sliding.sock");
        let server_info_file = tmp_dir.path().join("reduce_sliding-server-info");

        let server_info = server_info_file.clone();
        let server_socket = sock_file.clone();
        let handle = tokio::spawn(async move {
            reduce::Server::new(CounterCreator {})
                .with_socket_file(server_socket)
                .with_server_info_file(server_info)
                .start_with_shutdown(shutdown_rx)
                .await
                .expect("server failed");
        });

        // wait for the server to start
        tokio::time::sleep(Duration::from_millis(100)).await;

        let mut client =
            UserDefinedAlignedReduce::new(ReduceClient::new(create_rpc_channel(sock_file).await?))
                .await;

        // Create a sliding window
        let window_start = Utc.with_ymd_and_hms(2023, 1, 1, 0, 0, 0).unwrap();
        let window_end = Utc.with_ymd_and_hms(2023, 1, 1, 0, 1, 0).unwrap();
        let window = Window::new(window_start, window_end);

        // Create messages
        let messages = vec![
            Message {
                typ: Default::default(),
                keys: Arc::from(vec!["key1".into()]),
                tags: None,
                value: "value1".into(),
                offset: Offset::String(StringOffset::new("0".to_string(), 0)),
                event_time: window_start + chrono::Duration::seconds(10),
                watermark: None,
                id: MessageID {
                    vertex_name: "vertex_name".to_string().into(),
                    offset: "0".to_string().into(),
                    index: 0,
                },
                ..Default::default()
            },
            Message {
                typ: Default::default(),
                keys: Arc::from(vec!["key1".into()]),
                tags: None,
                value: "value2".into(),
                offset: Offset::String(StringOffset::new("1".to_string(), 1)),
                event_time: window_start + chrono::Duration::seconds(20),
                watermark: None,
                id: MessageID {
                    vertex_name: "vertex_name".to_string().into(),
                    offset: "1".to_string().into(),
                    index: 1,
                },
                ..Default::default()
            },
            Message {
                typ: Default::default(),
                keys: Arc::from(vec!["key1".into()]),
                tags: None,
                value: "value3".into(),
                offset: Offset::String(StringOffset::new("2".to_string(), 2)),
                event_time: window_start + chrono::Duration::seconds(30),
                watermark: None,
                id: MessageID {
                    vertex_name: "vertex_name".to_string().into(),
                    offset: "2".to_string().into(),
                    index: 2,
                },
                ..Default::default()
            },
        ];

        // Create window messages
        let window_messages = vec![
            // Open window
            AlignedWindowMessage {
                operation: AlignedWindowOperation::Open {
                    message: messages[0].clone(),
                    window: window.clone(),
                },
                pnf_slot: window_pnf_slot(&window),
            },
            // Append messages
            AlignedWindowMessage {
                operation: AlignedWindowOperation::Append {
                    message: messages[1].clone(),
                    window: window.clone(),
                },
                pnf_slot: window_pnf_slot(&window),
            },
            AlignedWindowMessage {
                operation: AlignedWindowOperation::Append {
                    message: messages[2].clone(),
                    window: window.clone(),
                },
                pnf_slot: window_pnf_slot(&window),
            },
        ];

        // Create a channel to send window messages
        let (window_tx, window_rx) = mpsc::channel(100);
        for msg in window_messages {
            window_tx.send(msg).await.unwrap();
        }
        drop(window_tx); // Close the channel to signal end of input

        // Create a channel to receive results
        let (result_tx, mut result_rx) = mpsc::channel(100);

        // Create a cancellation token
        let cln_token = CancellationToken::new();

        // Call reduce_fn
        let reduce_handle = tokio::spawn(async move {
            client
                .reduce_fn(ReceiverStream::new(window_rx), result_tx, cln_token)
                .await
                .expect("reduce_fn failed");
        });

        // Wait for the result
        let result = tokio::time::timeout(Duration::from_secs(2), result_rx.recv())
            .await
            .expect("timeout waiting for result")
            .expect("no result received");

        // Verify the result
        assert_eq!(result.keys.to_vec(), vec!["key1"]);
        assert_eq!(String::from_utf8(result.value.to_vec()).unwrap(), "3"); // Counter should be 3

        // Shutdown the server
        shutdown_tx
            .send(())
            .expect("failed to send shutdown signal");

        tokio::time::sleep(Duration::from_millis(50)).await;

        // Wait for the reduce handle to complete
        reduce_handle.await.expect("reduce handle failed");

        // Wait for the server to shut down
        assert!(
            handle.is_finished(),
            "Expected gRPC server to have shut down"
        );

        Ok(())
    }
}
