use crate::config::{get_vertex_name, get_vertex_replica};
use crate::message::{IntOffset, Message, MessageHandle, MessageID, Offset};
use crate::reduce::reducer::aligned::windower::{AlignedWindowMessage, AlignedWindowOperation};
use crate::shared::udf::udf_datum_from_message;
use crate::{Result, jh_abort_guard};
use numaflow_pb::clients::reduce::reduce_client::ReduceClient;
use numaflow_udf_client::{AlignedReduceBook, AlignedReduceResult, UdfClientError};
use std::ops::Sub;
use std::sync::Arc;
use tokio::task::JoinHandle;
use tokio_stream::StreamExt;
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;
use tonic::transport::Channel;
use tracing::info;

/// Effective request channel capacity for one aligned reduce book (matches prior adapter).
const REDUCE_REQUEST_BUFFER: usize = 500;

fn reduce_udf_client_error(error: UdfClientError) -> crate::Error {
    match error {
        UdfClientError::ReduceFnStart(status) | UdfClientError::ReduceGrpc(status) => {
            crate::Error::Grpc(Box::new(status))
        }
        other => crate::Error::Reduce(other.to_string()),
    }
}

/// Wrapper for reduce results that includes index and vertex name.
struct UdReducerResponse {
    pub result: AlignedReduceResult,
    pub index: i32,
    pub vertex_name: &'static str,
    pub vertex_replica: u16,
}

impl From<UdReducerResponse> for Message {
    fn from(wrapper: UdReducerResponse) -> Self {
        let result = wrapper.result;
        let window = result.window;

        let offset_str = format!(
            "{}-{}",
            window.start_time.timestamp_millis(),
            window.end_time.timestamp_millis()
        );

        Message {
            typ: Default::default(),
            keys: Arc::from(result.keys),
            tags: if result.tags.is_empty() {
                None
            } else {
                Some(Arc::from(result.tags))
            },
            value: result.value,
            offset: Offset::Int(IntOffset::new(0, 0)),
            event_time: window.end_time.sub(chrono::Duration::milliseconds(1)),
            watermark: Some(window.end_time - chrono::Duration::milliseconds(1)),
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
        result_tx: tokio::sync::mpsc::Sender<MessageHandle>,
        cln_token: CancellationToken,
    ) -> Result<()> {
        let mut stream = stream;

        let first = stream.next().await.ok_or_else(|| {
            crate::Error::Reduce("aligned reduce input stream ended before first message".into())
        })?;

        let (message, window) = match first.operation {
            AlignedWindowOperation::Open { message, window } => (message, window),
            // Replay recovery converts APPEND to OPEN at the actor; accept APPEND as book start.
            AlignedWindowOperation::Append { message, window } => (message, window),
            AlignedWindowOperation::Close { .. } => {
                return Err(crate::Error::Reduce(
                    "aligned reduce input stream started with Close".into(),
                ));
            }
        };

        // NOTE: `AlignedReduceBook::open` returns immediately after queuing OPEN and starting the
        // response pump. That avoids the tonic client-streaming deadlock where `reduce_fn` does not
        // return a response stream until the server has read request data.
        let (book, mut receiver) = AlignedReduceBook::open(
            self.client.clone(),
            window,
            udf_datum_from_message(message),
            REDUCE_REQUEST_BUFFER,
        )
        .await
        .map_err(reduce_udf_client_error)?;

        // Pump remaining window messages into the book; stream end closes the ReduceFn request side.
        let request_handle = tokio::spawn(async move {
            while let Some(window_msg) = stream.next().await {
                match window_msg.operation {
                    AlignedWindowOperation::Append { message, .. } => {
                        if book.append(udf_datum_from_message(message)).await.is_err() {
                            break;
                        }
                    }
                    AlignedWindowOperation::Open { message, .. } => {
                        if book.append(udf_datum_from_message(message)).await.is_err() {
                            break;
                        }
                    }
                    AlignedWindowOperation::Close { .. } => break,
                }
            }
            book.close();
        });

        let _guard = jh_abort_guard!(request_handle);

        let vertex_name = get_vertex_name();
        let mut index = 0;

        loop {
            tokio::select! {
                _ = cln_token.cancelled() => {
                    info!("Cancellation detected while processing reduce responses, stopping");
                    return Err(crate::Error::Cancelled());
                }

                item = receiver.recv() => {
                    let Some(item) = item else {
                        break;
                    };

                    let result = item.map_err(reduce_udf_client_error)?;

                    let message: Message = UdReducerResponse {
                        result,
                        index,
                        vertex_name,
                        vertex_replica: *get_vertex_replica(),
                    }
                    .into();

                    result_tx
                        .send(message.into())
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
        AlignedWindowOperation, Window, window_to_pnf_slot,
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

    struct MultiResult;
    struct MultiResultCreator;

    impl reduce::ReducerCreator for MultiResultCreator {
        type R = MultiResult;

        fn create(&self) -> Self::R {
            MultiResult
        }
    }

    #[tonic::async_trait]
    impl reduce::Reducer for MultiResult {
        async fn reduce(
            &self,
            keys: Vec<String>,
            mut input: mpsc::Receiver<reduce::ReduceRequest>,
            _md: &reduce::Metadata,
        ) -> Vec<reduce::Message> {
            while input.recv().await.is_some() {}
            vec![
                reduce::Message::new(b"first".to_vec()).with_keys(keys.clone()),
                reduce::Message::new(b"second".to_vec()).with_keys(keys),
            ]
        }
    }

    async fn run_reduce_on_messages(
        client: &mut UserDefinedAlignedReduce,
        window: Window,
        messages: &[Message],
    ) -> Vec<MessageHandle> {
        let mut window_messages = Vec::with_capacity(messages.len());
        for (i, message) in messages.iter().enumerate() {
            let operation = if i == 0 {
                AlignedWindowOperation::Open {
                    message: message.clone(),
                    window: window.clone(),
                }
            } else {
                AlignedWindowOperation::Append {
                    message: message.clone(),
                    window: window.clone(),
                }
            };
            window_messages.push(AlignedWindowMessage {
                operation,
                pnf_slot: window_to_pnf_slot(&window),
            });
        }

        let (window_tx, window_rx) = mpsc::channel(10);
        for msg in window_messages {
            window_tx.send(msg).await.unwrap();
        }
        drop(window_tx);

        let (result_tx, mut result_rx) = mpsc::channel(10);
        let cln_token = CancellationToken::new();

        client
            .reduce_fn(ReceiverStream::new(window_rx), result_tx, cln_token)
            .await
            .expect("reduce_fn failed");

        let mut results = Vec::new();
        while let Some(result) = tokio::time::timeout(Duration::from_secs(5), result_rx.recv())
            .await
            .expect("timeout waiting for result")
        {
            results.push(result);
        }
        results
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

        tokio::time::sleep(Duration::from_millis(100)).await;

        let mut client =
            UserDefinedAlignedReduce::new(ReduceClient::new(create_rpc_channel(sock_file).await?))
                .await;

        let window_start = Utc.with_ymd_and_hms(2023, 1, 1, 0, 0, 0).unwrap();
        let window_end = Utc.with_ymd_and_hms(2023, 1, 1, 0, 1, 0).unwrap();
        let window = Window::new(window_start, window_end);

        let messages = [
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

        let results = run_reduce_on_messages(&mut client, window.clone(), &messages).await;
        assert_eq!(results.len(), 1);
        let result = results.first().expect("one result");

        assert_eq!(result.message().keys.to_vec(), vec!["key1"]);
        assert_eq!(
            String::from_utf8(result.message().value.to_vec()).unwrap(),
            "3"
        );
        assert!(result.message().tags.is_none());
        assert!(matches!(result.message().offset, Offset::Int(_)));
        assert_eq!(
            result.message().event_time,
            window_end - chrono::Duration::milliseconds(1)
        );
        assert_eq!(
            result.message().watermark,
            Some(window_end - chrono::Duration::milliseconds(1))
        );
        assert_eq!(
            String::from_utf8_lossy(&result.message().id.offset),
            format!(
                "{}-{}",
                window.start_time.timestamp_millis(),
                window.end_time.timestamp_millis()
            )
        );
        assert_eq!(result.message().id.index, 0);

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

    #[tokio::test]
    async fn multiple_results_preserve_core_message_identity() -> Result<()> {
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let tmp_dir = TempDir::new().unwrap();
        let sock_file = tmp_dir.path().join("reduce_multi_result.sock");
        let server_info_file = tmp_dir.path().join("reduce_multi_result-server-info");

        let server_socket = sock_file.clone();
        let handle = tokio::spawn(async move {
            reduce::Server::new(MultiResultCreator)
                .with_socket_file(server_socket)
                .with_server_info_file(server_info_file)
                .start_with_shutdown(shutdown_rx)
                .await
                .expect("server failed");
        });
        tokio::time::sleep(Duration::from_millis(100)).await;

        let mut client =
            UserDefinedAlignedReduce::new(ReduceClient::new(create_rpc_channel(sock_file).await?))
                .await;
        let window_start = Utc.with_ymd_and_hms(2023, 1, 1, 0, 0, 0).unwrap();
        let window_end = Utc.with_ymd_and_hms(2023, 1, 1, 0, 1, 0).unwrap();
        let window = Window::new(window_start, window_end);
        let messages = [Message {
            keys: Arc::from(vec!["key1".into()]),
            value: "value".into(),
            event_time: window_start + chrono::Duration::seconds(10),
            id: MessageID {
                vertex_name: "vertex_name".to_string().into(),
                offset: "0".to_string().into(),
                index: 0,
            },
            ..Default::default()
        }];

        let results = run_reduce_on_messages(&mut client, window.clone(), &messages).await;
        assert_eq!(results.len(), 2);
        assert_eq!(
            results
                .first()
                .expect("first result")
                .message()
                .value
                .as_ref(),
            b"first"
        );
        assert_eq!(
            results
                .get(1)
                .expect("second result")
                .message()
                .value
                .as_ref(),
            b"second"
        );
        for (index, result) in results.iter().enumerate() {
            assert_eq!(result.message().id.index, index as i32);
            assert_eq!(
                String::from_utf8_lossy(&result.message().id.offset),
                format!(
                    "{}-{}",
                    window.start_time.timestamp_millis(),
                    window.end_time.timestamp_millis()
                )
            );
            assert_eq!(
                result.message().event_time,
                window.end_time - chrono::Duration::milliseconds(1)
            );
            assert_eq!(
                result.message().watermark,
                Some(result.message().event_time)
            );
        }

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

        tokio::time::sleep(Duration::from_millis(100)).await;

        let mut client =
            UserDefinedAlignedReduce::new(ReduceClient::new(create_rpc_channel(sock_file).await?))
                .await;

        let window_start = Utc.with_ymd_and_hms(2023, 1, 1, 0, 0, 0).unwrap();
        let window_end = Utc.with_ymd_and_hms(2023, 1, 1, 0, 1, 0).unwrap();
        let window = Window::new(window_start, window_end);

        let key1_messages = [
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

        let key2_messages = [
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

        let messages: Vec<Message> = key1_messages.into_iter().chain(key2_messages).collect();
        let mut results = run_reduce_on_messages(&mut client, window, &messages).await;
        results.sort_by_key(|result| result.message().keys.to_vec());
        assert_eq!(results.len(), 2);

        let result_key1 = results.first().expect("key1 result");
        assert_eq!(result_key1.message().keys.to_vec(), vec!["key1"]);
        assert_eq!(
            String::from_utf8(result_key1.message().value.to_vec()).unwrap(),
            "2"
        );

        let result_key2 = results.get(1).expect("key2 result");
        assert_eq!(result_key2.message().keys.to_vec(), vec!["key2"]);
        assert_eq!(
            String::from_utf8(result_key2.message().value.to_vec()).unwrap(),
            "3"
        );

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

        tokio::time::sleep(Duration::from_millis(100)).await;

        let mut client =
            UserDefinedAlignedReduce::new(ReduceClient::new(create_rpc_channel(sock_file).await?))
                .await;

        let window_start = Utc.with_ymd_and_hms(2023, 1, 1, 0, 0, 0).unwrap();
        let window_end = Utc.with_ymd_and_hms(2023, 1, 1, 0, 1, 0).unwrap();
        let window = Window::new(window_start, window_end);

        let messages = [
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

        let results = run_reduce_on_messages(&mut client, window, &messages).await;
        assert_eq!(results.len(), 1);
        let result = results.first().expect("one result");

        assert_eq!(result.message().keys.to_vec(), vec!["key1"]);
        assert_eq!(
            String::from_utf8(result.message().value.to_vec()).unwrap(),
            "3"
        );

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
