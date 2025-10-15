use crate::config::{get_vertex_name, get_vertex_replica};
use crate::message::{IntOffset, Message, MessageID, Offset};
use crate::reduce::reducer::unaligned::windower::{
    UnalignedWindowMessage, UnalignedWindowOperation, Window,
};
use crate::shared::grpc::{prost_timestamp_from_utc, utc_from_timestamp};
use numaflow_pb::clients::sessionreduce;
use numaflow_pb::clients::sessionreduce::session_reduce_client::SessionReduceClient;
use numaflow_pb::clients::sessionreduce::{
    SessionReduceRequest, SessionReduceResponse, session_reduce_request,
};
use std::collections::HashMap;
use std::ops::Sub;
use std::sync::Arc;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;
use tonic::transport::Channel;
use tracing::info;

impl From<Message> for session_reduce_request::Payload {
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

impl From<Window> for sessionreduce::KeyedWindow {
    fn from(window: Window) -> Self {
        Self {
            start: Some(prost_timestamp_from_utc(window.start_time)),
            end: Some(prost_timestamp_from_utc(window.end_time)),
            slot: "0".to_string(),
            keys: window.keys.to_vec(),
        }
    }
}

impl From<UnalignedWindowMessage> for SessionReduceRequest {
    fn from(value: UnalignedWindowMessage) -> Self {
        match value.operation {
            UnalignedWindowOperation::Open { message, window } => {
                let operation = Some(session_reduce_request::WindowOperation {
                    event: session_reduce_request::window_operation::Event::Open as i32,
                    keyed_windows: vec![window.into()],
                });

                SessionReduceRequest {
                    payload: Some(message.into()),
                    operation,
                }
            }
            UnalignedWindowOperation::Close { window } => {
                let operation = Some(session_reduce_request::WindowOperation {
                    event: session_reduce_request::window_operation::Event::Close as i32,
                    keyed_windows: vec![window.into()],
                });

                SessionReduceRequest {
                    payload: None,
                    operation,
                }
            }
            UnalignedWindowOperation::Append { message, window } => {
                let operation = Some(session_reduce_request::WindowOperation {
                    event: session_reduce_request::window_operation::Event::Append as i32,
                    keyed_windows: vec![window.into()],
                });

                SessionReduceRequest {
                    payload: Some(message.into()),
                    operation,
                }
            }
            UnalignedWindowOperation::Merge { windows } => {
                let operation = Some(session_reduce_request::WindowOperation {
                    event: session_reduce_request::window_operation::Event::Merge as i32,
                    keyed_windows: windows.into_iter().map(Into::into).collect(),
                });

                SessionReduceRequest {
                    payload: None,
                    operation,
                }
            }
            UnalignedWindowOperation::Expand { message, windows } => {
                let operation = Some(session_reduce_request::WindowOperation {
                    event: session_reduce_request::window_operation::Event::Expand as i32,
                    keyed_windows: windows.into_iter().map(Into::into).collect(),
                });

                SessionReduceRequest {
                    payload: Some(message.into()),
                    operation,
                }
            }
        }
    }
}

/// Wrapper for SessionReduceResponse that includes index and vertex name.
pub(crate) struct UserDefinedSessionResponse {
    pub response: SessionReduceResponse,
    /// monotonically increasing index for each response per window key combination
    pub index: u32,
    /// vertex name for creating the message ID
    pub vertex_name: &'static str,
    /// vertex replica for creating the message ID
    pub vertex_replica: u16,
}

impl From<UserDefinedSessionResponse> for Message {
    fn from(user_response: UserDefinedSessionResponse) -> Self {
        let response = user_response.response;
        let result = response.result.unwrap_or_default();
        let window = response.keyed_window.unwrap_or_default();

        // Create offset from window start and end time
        let offset_str = format!(
            "{}-{}-{}",
            utc_from_timestamp(window.start.expect("window start missing")).timestamp_millis(),
            utc_from_timestamp(window.end.expect("window end missing")).timestamp_millis(),
            window.keys.join(":")
        );

        Message {
            typ: Default::default(),
            keys: Arc::from(result.keys),
            tags: (!result.tags.is_empty()).then(|| Arc::from(result.tags)),
            value: result.value.into(),
            offset: Offset::Int(IntOffset::new(0, 0)),
            event_time: utc_from_timestamp(window.end.unwrap())
                .sub(chrono::Duration::milliseconds(1)),
            watermark: window
                .end
                .map(utc_from_timestamp)
                .map(|ts| ts - chrono::Duration::milliseconds(1)),
            // this will be unique for each response which will be used for dedup (index is used because
            // each window can have multiple reduce responses)
            id: MessageID {
                vertex_name: format!(
                    "{}-{}",
                    user_response.vertex_name, user_response.vertex_replica
                )
                .into(),
                offset: offset_str.into(),
                index: user_response.index as i32,
            },
            headers: Arc::new(HashMap::new()), // reset headers since it is a new message
            metadata: None,
            is_late: false,
            ack_handle: None,
        }
    }
}

/// User-defined session reduce client.
#[derive(Clone)]
pub(crate) struct UserDefinedSessionReduce {
    client: SessionReduceClient<Channel>,
}

impl UserDefinedSessionReduce {
    pub(crate) async fn new(client: SessionReduceClient<Channel>) -> Self {
        UserDefinedSessionReduce { client }
    }

    /// Calls the reduce_fn on the user-defined session reducer on a separate tokio task.
    /// If the cancellation token is triggered, it will stop processing and return early.
    pub(crate) async fn session_reduce_fn(
        &mut self,
        stream: ReceiverStream<SessionReduceRequest>,
        cln_token: CancellationToken,
    ) -> crate::Result<(
        ReceiverStream<UserDefinedSessionResponse>,
        JoinHandle<crate::Result<()>>,
    )> {
        // Create a channel for responses
        let (result_tx, result_rx) = tokio::sync::mpsc::channel(100);

        let mut client = self.client.clone();
        let vertex_name = get_vertex_name();

        // Start a background task to process responses
        let handle = tokio::spawn(async move {
            let mut response_stream = client
                .session_reduce_fn(stream)
                .await
                .map_err(|e| crate::Error::Grpc(Box::new(e)))?
                .into_inner();

            // Track response indices per window key combination which will be used for constructing
            // message id which is required for dedup.
            let mut response_indices: HashMap<Vec<String>, u32> = HashMap::new();

            loop {
                tokio::select! {
                    // Check for cancellation
                    _ = cln_token.cancelled() => {
                        info!("Cancellation detected while processing responses, stopping");
                        return Err(crate::Error::Cancelled());
                    }

                    // Process next response
                    response = response_stream.message() => {
                        let response = response
                            .map_err(|e| crate::Error::Grpc(Box::new(e)))?;

                        let Some(response) = response else {
                            break;
                        };

                        let window_keys = response.keyed_window
                            .as_ref()
                            .map(|w| w.keys.clone())
                            .unwrap_or_default();

                        // If EOF, remove the index for the window keys, else increment the index
                        let index = if response.eof {
                            response_indices.remove(&window_keys);
                            0
                        } else {
                            let current_index = response_indices.entry(window_keys).or_insert(0);
                            let index = *current_index;
                            *current_index += 1;
                            index
                        };

                        let user_response = UserDefinedSessionResponse {
                            response,
                            index,
                            vertex_name,
                            vertex_replica: *get_vertex_replica(),
                        };

                        if result_tx.send(user_response).await.is_err() {
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
    use numaflow::session_reduce;
    use numaflow::shared::ServerExtras;
    use numaflow_pb::clients::sessionreduce::session_reduce_client::SessionReduceClient;
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

    struct Counter {
        count: Arc<std::sync::atomic::AtomicU32>,
    }

    struct CounterCreator {}

    impl session_reduce::SessionReducerCreator for CounterCreator {
        type R = Counter;

        fn create(&self) -> Self::R {
            Counter::new()
        }
    }

    impl Counter {
        fn new() -> Self {
            Self {
                count: Arc::new(std::sync::atomic::AtomicU32::new(0)),
            }
        }
    }

    #[tonic::async_trait]
    impl session_reduce::SessionReducer for Counter {
        async fn session_reduce(
            &self,
            keys: Vec<String>,
            mut input: mpsc::Receiver<session_reduce::SessionReduceRequest>,
            output: mpsc::Sender<session_reduce::Message>,
        ) {
            // Count all incoming messages in this session
            while input.recv().await.is_some() {
                self.count
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }

            // Send the current count as the result
            let count_value = self.count.load(std::sync::atomic::Ordering::Relaxed);
            let message =
                session_reduce::Message::new(count_value.to_string().into_bytes()).with_keys(keys);

            output.send(message).await.unwrap();
        }

        async fn accumulator(&self) -> Vec<u8> {
            // Return the current count as bytes for accumulator
            let count = self.count.load(std::sync::atomic::Ordering::Relaxed);
            count.to_string().into_bytes()
        }

        async fn merge_accumulator(&self, accumulator: Vec<u8>) {
            // Parse the accumulator value and add it to our count
            if let Ok(accumulator_str) = String::from_utf8(accumulator)
                && let Ok(accumulator_count) = accumulator_str.parse::<u32>()
            {
                self.count
                    .fetch_add(accumulator_count, std::sync::atomic::Ordering::Relaxed);
            }
        }
    }

    #[tokio::test]
    async fn test_session_reduce_basic() -> crate::Result<()> {
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let tmp_dir = TempDir::new().unwrap();
        let sock_file = tmp_dir.path().join("session_reduce.sock");
        let server_info_file = tmp_dir.path().join("session_reduce-server-info");

        let server_info = server_info_file.clone();
        let server_socket = sock_file.clone();
        let handle = tokio::spawn(async move {
            session_reduce::Server::new(CounterCreator {})
                .with_socket_file(server_socket)
                .with_server_info_file(server_info)
                .start_with_shutdown(shutdown_rx)
                .await
                .expect("server failed");
        });

        // wait for the server to start
        tokio::time::sleep(Duration::from_millis(100)).await;

        let mut client = UserDefinedSessionReduce::new(SessionReduceClient::new(
            create_rpc_channel(sock_file).await?,
        ))
        .await;

        // Create a simple test message
        let message = Message {
            typ: Default::default(),
            keys: Arc::from(vec!["key1".into()]),
            tags: None,
            value: "value1".into(),
            offset: Offset::String(StringOffset::new("0".to_string(), 0)),
            event_time: Utc.with_ymd_and_hms(2023, 1, 1, 0, 0, 10).unwrap(),
            watermark: None,
            id: MessageID {
                vertex_name: "vertex_name".to_string().into(),
                offset: "0".to_string().into(),
                index: 0,
            },
            ..Default::default()
        };

        // Create a session window
        let window = Window::new(
            Utc.with_ymd_and_hms(2023, 1, 1, 0, 0, 0).unwrap(),
            Utc.with_ymd_and_hms(2023, 1, 1, 0, 1, 0).unwrap(),
            Arc::from(vec!["key1".to_string()]),
        );

        // Create window operation
        let window_message = UnalignedWindowMessage {
            operation: UnalignedWindowOperation::Open {
                message: message.clone(),
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
        window_tx.send(window_message.into()).await.unwrap();
        window_tx.send(close_message.into()).await.unwrap();
        drop(window_tx); // Close the channel to signal end of input

        // Create a cancellation token
        let cln_token = CancellationToken::new();

        // Call reduce_fn
        let (mut response_stream, session_reduce_handle) = client
            .session_reduce_fn(ReceiverStream::new(window_rx), cln_token)
            .await
            .expect("reduce_fn failed");

        // Wait for the result
        let result = tokio::time::timeout(Duration::from_secs(5), response_stream.next())
            .await
            .expect("timeout waiting for result")
            .expect("no result received");

        // Convert response to message
        let result_message: Message = result.into();

        // Verify the result
        assert_eq!(result_message.keys.to_vec(), vec!["key1"]);
        assert_eq!(
            String::from_utf8(result_message.value.to_vec()).unwrap(),
            "1"
        ); // Counter should be 1

        // Shutdown the server
        shutdown_tx
            .send(())
            .expect("failed to send shutdown signal");

        tokio::time::sleep(Duration::from_millis(100)).await;

        session_reduce_handle
            .await
            .expect("session reduce handle failed")
            .expect("session reduce failed");

        // Wait for the handle to complete
        handle.await.expect("handle failed");

        Ok(())
    }

    #[tokio::test]
    async fn test_session_reduce_multiple_messages() -> crate::Result<()> {
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let tmp_dir = TempDir::new().unwrap();
        let sock_file = tmp_dir.path().join("session_multi.sock");
        let server_info_file = tmp_dir.path().join("session_multi-server-info");

        let server_info = server_info_file.clone();
        let server_socket = sock_file.clone();
        let _handle = tokio::spawn(async move {
            session_reduce::Server::new(CounterCreator {})
                .with_socket_file(server_socket)
                .with_server_info_file(server_info)
                .start_with_shutdown(shutdown_rx)
                .await
                .expect("server failed");
        });

        // wait for the server to start
        tokio::time::sleep(Duration::from_millis(100)).await;

        let mut client = UserDefinedSessionReduce::new(SessionReduceClient::new(
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
            watermark: None,
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
            watermark: None,
            id: MessageID {
                vertex_name: "vertex_name".to_string().into(),
                offset: "1".to_string().into(),
                index: 1,
            },
            ..Default::default()
        };

        // Create a session window
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

        let append_message = UnalignedWindowMessage {
            operation: UnalignedWindowOperation::Append {
                message: message2.clone(),
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
        window_tx.send(append_message.into()).await.unwrap();
        window_tx.send(close_message.into()).await.unwrap();
        drop(window_tx); // Close the channel to signal end of input

        // Create a cancellation token
        let cln_token = CancellationToken::new();

        // Call reduce_fn
        let (mut response_stream, handle) = client
            .session_reduce_fn(ReceiverStream::new(window_rx), cln_token)
            .await
            .expect("reduce_fn failed");

        // Wait for the result
        let result = tokio::time::timeout(Duration::from_secs(5), response_stream.next())
            .await
            .expect("timeout waiting for result")
            .expect("no result received");

        // Convert response to message
        let result_message: Message = result.into();

        // Verify the result
        assert_eq!(result_message.keys.to_vec(), vec!["key1"]);
        assert_eq!(
            String::from_utf8(result_message.value.to_vec()).unwrap(),
            "2"
        ); // Counter should be 2

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

    #[tokio::test]
    async fn test_session_reduce_with_merging() -> crate::Result<()> {
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let tmp_dir = TempDir::new().unwrap();
        let sock_file = tmp_dir.path().join("session_merge.sock");
        let server_info_file = tmp_dir.path().join("session_merge-server-info");

        let server_info = server_info_file.clone();
        let server_socket = sock_file.clone();
        let handle = tokio::spawn(async move {
            session_reduce::Server::new(CounterCreator {})
                .with_socket_file(server_socket)
                .with_server_info_file(server_info)
                .start_with_shutdown(shutdown_rx)
                .await
                .expect("server failed");
        });

        // Wait for the server to start
        tokio::time::sleep(Duration::from_millis(100)).await;

        let mut client = UserDefinedSessionReduce::new(SessionReduceClient::new(
            create_rpc_channel(sock_file).await?,
        ))
        .await;

        // Create base time
        let base_time = Utc.with_ymd_and_hms(2023, 1, 1, 0, 0, 0).unwrap();

        // Create test messages with different keys
        let message1 = Message {
            typ: Default::default(),
            keys: Arc::from(vec!["key1".into(), "subkey1".into()]),
            tags: None,
            value: "value1".into(),
            offset: Offset::String(StringOffset::new("0".to_string(), 0)),
            event_time: base_time + chrono::Duration::seconds(10),
            watermark: None,
            id: MessageID {
                vertex_name: "vertex_name".to_string().into(),
                offset: "0".to_string().into(),
                index: 0,
            },
            ..Default::default()
        };

        let message2 = Message {
            typ: Default::default(),
            keys: Arc::from(vec!["key1".into(), "subkey1".into()]),
            tags: None,
            value: "value2".into(),
            offset: Offset::String(StringOffset::new("1".to_string(), 1)),
            event_time: base_time + chrono::Duration::seconds(40),
            watermark: None,
            id: MessageID {
                vertex_name: "vertex_name".to_string().into(),
                offset: "1".to_string().into(),
                index: 1,
            },
            ..Default::default()
        };

        let message3 = Message {
            typ: Default::default(),
            keys: Arc::from(vec!["key2".into(), "subkey2".into()]),
            tags: None,
            value: "value3".into(),
            offset: Offset::String(StringOffset::new("2".to_string(), 2)),
            event_time: base_time + chrono::Duration::seconds(20),
            watermark: None,
            id: MessageID {
                vertex_name: "vertex_name".to_string().into(),
                offset: "2".to_string().into(),
                index: 2,
            },
            ..Default::default()
        };

        // Create windows with overlapping times for the same key
        let window1 = Window::new(
            base_time,
            base_time + chrono::Duration::seconds(30),
            Arc::from(vec!["key1".to_string(), "subkey1".to_string()]),
        );

        let window2 = Window::new(
            base_time + chrono::Duration::seconds(25),
            base_time + chrono::Duration::seconds(55),
            Arc::from(vec!["key1".to_string(), "subkey1".to_string()]),
        );

        // Window for different key
        let window3 = Window::new(
            base_time + chrono::Duration::seconds(15),
            base_time + chrono::Duration::seconds(45),
            Arc::from(vec!["key2".to_string(), "subkey2".to_string()]),
        );

        // Create merged window (result of merging window1 and window2)
        let merged_window = Window::new(
            base_time,
            base_time + chrono::Duration::seconds(55),
            Arc::from(vec!["key1".to_string(), "subkey1".to_string()]),
        );

        // Create a channel to send window messages
        let (window_tx, window_rx) = mpsc::channel(10);

        // Send operations for first key
        window_tx
            .send(
                UnalignedWindowMessage {
                    operation: UnalignedWindowOperation::Open {
                        message: message1.clone(),
                        window: window1.clone(),
                    },
                    pnf_slot: "GLOBAL_SLOT",
                }
                .into(),
            )
            .await
            .unwrap();

        // Send operations for second key
        window_tx
            .send(
                UnalignedWindowMessage {
                    operation: UnalignedWindowOperation::Open {
                        message: message3.clone(),
                        window: window3.clone(),
                    },
                    pnf_slot: "GLOBAL_SLOT",
                }
                .into(),
            )
            .await
            .unwrap();

        // Open second window for first key
        window_tx
            .send(
                UnalignedWindowMessage {
                    operation: UnalignedWindowOperation::Open {
                        message: message2.clone(),
                        window: window2.clone(),
                    },
                    pnf_slot: "GLOBAL_SLOT",
                }
                .into(),
            )
            .await
            .unwrap();

        // Merge windows for first key
        window_tx
            .send(
                UnalignedWindowMessage {
                    operation: UnalignedWindowOperation::Merge {
                        windows: vec![window1.clone(), window2.clone()],
                    },
                    pnf_slot: "GLOBAL_SLOT",
                }
                .into(),
            )
            .await
            .unwrap();

        // Close windows
        window_tx
            .send(
                UnalignedWindowMessage {
                    operation: UnalignedWindowOperation::Close {
                        window: merged_window.clone(),
                    },
                    pnf_slot: "GLOBAL_SLOT",
                }
                .into(),
            )
            .await
            .unwrap();

        window_tx
            .send(
                UnalignedWindowMessage {
                    operation: UnalignedWindowOperation::Close {
                        window: window3.clone(),
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
        let (mut response_stream, session_handle) = client
            .session_reduce_fn(ReceiverStream::new(window_rx), cln_token)
            .await
            .expect("reduce_fn failed");

        // Collect all results
        let mut results = Vec::new();
        while let Some(result) =
            tokio::time::timeout(Duration::from_secs(5), response_stream.next())
                .await
                .expect("timeout waiting for result")
        {
            if result.response.eof {
                continue;
            }
            results.push(result);
        }

        // We should have 2 results  (one for each key group)
        assert_eq!(results.len(), 2);

        // Convert responses to messages and verify
        let result_messages: Vec<Message> = results.into_iter().map(Into::into).collect();

        // Sort results by keys for consistent testing
        let mut result_messages = result_messages;
        result_messages.sort_by(|a, b| a.keys.cmp(&b.keys));

        // First result should be for key1:subkey1 with count 2
        assert_eq!(result_messages[0].keys.to_vec(), vec!["key1", "subkey1"]);
        assert_eq!(
            String::from_utf8(result_messages[0].value.to_vec()).unwrap(),
            "2"
        );

        // Second result should be for key2:subkey2 with count 1
        assert_eq!(result_messages[1].keys.to_vec(), vec!["key2", "subkey2"]);
        assert_eq!(
            String::from_utf8(result_messages[1].value.to_vec()).unwrap(),
            "1"
        );

        // Wait for the handle to complete
        session_handle
            .await
            .expect("handle failed")
            .expect("reduce_fn task failed");

        // Shutdown the server
        shutdown_tx
            .send(())
            .expect("failed to send shutdown signal");

        // wait for server to shutdown
        handle.await.expect("handle failed");

        Ok(())
    }

    #[tokio::test]
    async fn test_session_reduce_with_expand_operation() -> crate::Result<()> {
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let tmp_dir = TempDir::new().unwrap();
        let sock_file = tmp_dir.path().join("session_expand.sock");
        let server_info_file = tmp_dir.path().join("session_expand-server-info");

        let server_info = server_info_file.clone();
        let server_socket = sock_file.clone();
        let handle = tokio::spawn(async move {
            session_reduce::Server::new(CounterCreator {})
                .with_socket_file(server_socket)
                .with_server_info_file(server_info)
                .start_with_shutdown(shutdown_rx)
                .await
                .expect("server failed");
        });

        // Wait for the server to start
        tokio::time::sleep(Duration::from_millis(100)).await;

        let mut client = UserDefinedSessionReduce::new(SessionReduceClient::new(
            create_rpc_channel(sock_file).await?,
        ))
        .await;

        // Create base time
        let base_time = Utc.with_ymd_and_hms(2023, 1, 1, 0, 0, 0).unwrap();

        // Create messages with different event times
        let message1 = Message {
            typ: Default::default(),
            keys: Arc::from(vec!["key1".into(), "subkey1".into()]),
            tags: None,
            value: "value1".into(),
            offset: Offset::String(StringOffset::new("0".to_string(), 0)),
            event_time: base_time + chrono::Duration::seconds(10),
            watermark: None,
            id: MessageID {
                vertex_name: "vertex_name".to_string().into(),
                offset: "0".to_string().into(),
                index: 0,
            },
            ..Default::default()
        };

        let message2 = Message {
            typ: Default::default(),
            keys: Arc::from(vec!["key1".into(), "subkey1".into()]),
            tags: None,
            value: "value2".into(),
            offset: Offset::String(StringOffset::new("1".to_string(), 1)),
            event_time: base_time + chrono::Duration::seconds(70),
            watermark: None,
            id: MessageID {
                vertex_name: "vertex_name".to_string().into(),
                offset: "1".to_string().into(),
                index: 1,
            },
            ..Default::default()
        };

        // Create initial window
        let window1 = Window::new(
            base_time,
            base_time + chrono::Duration::seconds(30),
            Arc::from(vec!["key1".to_string(), "subkey1".to_string()]),
        );

        // Create expanded window
        let expanded_window = Window::new(
            base_time,
            base_time + chrono::Duration::seconds(100),
            Arc::from(vec!["key1".to_string(), "subkey1".to_string()]),
        );

        // Create a channel to send window messages
        let (window_tx, window_rx) = mpsc::channel(10);

        // Open initial window
        window_tx
            .send(
                UnalignedWindowMessage {
                    operation: UnalignedWindowOperation::Open {
                        message: message1.clone(),
                        window: window1.clone(),
                    },
                    pnf_slot: "GLOBAL_SLOT",
                }
                .into(),
            )
            .await
            .unwrap();

        // Expand window with second message
        window_tx
            .send(
                UnalignedWindowMessage {
                    operation: UnalignedWindowOperation::Expand {
                        message: message2.clone(),
                        windows: vec![window1.clone(), expanded_window.clone()],
                    },
                    pnf_slot: "GLOBAL_SLOT",
                }
                .into(),
            )
            .await
            .unwrap();

        // Close expanded window
        window_tx
            .send(
                UnalignedWindowMessage {
                    operation: UnalignedWindowOperation::Close {
                        window: expanded_window.clone(),
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
        let (mut response_stream, session_handle) = client
            .session_reduce_fn(ReceiverStream::new(window_rx), cln_token)
            .await
            .expect("reduce_fn failed");

        // Get the result
        let result = tokio::time::timeout(Duration::from_secs(5), response_stream.next())
            .await
            .expect("timeout waiting for result")
            .expect("no result received");

        // Convert response to message
        let result_message: Message = result.into();

        // Verify the result
        assert_eq!(result_message.keys.to_vec(), vec!["key1", "subkey1"]);
        assert_eq!(
            String::from_utf8(result_message.value.to_vec()).unwrap(),
            "2"
        ); // Counter should be 2

        // Wait for the session handle to complete
        session_handle
            .await
            .expect("reduce_fn task failed")
            .unwrap();

        // Shutdown the server
        shutdown_tx
            .send(())
            .expect("failed to send shutdown signal");

        // wait for the server to complete
        handle.await.expect("handle failed");
        Ok(())
    }

    #[tokio::test]
    async fn test_session_reduce_indexing() -> crate::Result<()> {
        // This test verifies that the indexing is working correctly for multiple responses
        // from the same window key combination
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let tmp_dir = TempDir::new().unwrap();
        let sock_file = tmp_dir.path().join("session_indexing.sock");
        let server_info_file = tmp_dir.path().join("session_indexing-server-info");

        let server_info = server_info_file.clone();
        let server_socket = sock_file.clone();
        let handle = tokio::spawn(async move {
            session_reduce::Server::new(CounterCreator {})
                .with_socket_file(server_socket)
                .with_server_info_file(server_info)
                .start_with_shutdown(shutdown_rx)
                .await
                .expect("server failed");
        });

        // Wait for the server to start
        tokio::time::sleep(Duration::from_millis(100)).await;

        let mut client = UserDefinedSessionReduce::new(SessionReduceClient::new(
            create_rpc_channel(sock_file).await?,
        ))
        .await;

        // Create a simple test message
        let message = Message {
            typ: Default::default(),
            keys: Arc::from(vec!["key1".into()]),
            tags: None,
            value: "value1".into(),
            offset: Offset::String(StringOffset::new("0".to_string(), 0)),
            event_time: Utc.with_ymd_and_hms(2023, 1, 1, 0, 0, 10).unwrap(),
            watermark: None,
            id: MessageID {
                vertex_name: "vertex_name".to_string().into(),
                offset: "0".to_string().into(),
                index: 0,
            },
            ..Default::default()
        };

        // Create a session window
        let window = Window::new(
            Utc.with_ymd_and_hms(2023, 1, 1, 0, 0, 0).unwrap(),
            Utc.with_ymd_and_hms(2023, 1, 1, 0, 1, 0).unwrap(),
            Arc::from(vec!["key1".to_string()]),
        );

        // Create window operation
        let window_message = UnalignedWindowMessage {
            operation: UnalignedWindowOperation::Open {
                message: message.clone(),
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
        window_tx.send(window_message.into()).await.unwrap();
        window_tx.send(close_message.into()).await.unwrap();
        drop(window_tx); // Close the channel to signal end of input

        // Create a cancellation token
        let cln_token = CancellationToken::new();

        // Call reduce_fn
        let (mut response_stream, session_reduce_handle) = client
            .session_reduce_fn(ReceiverStream::new(window_rx), cln_token)
            .await
            .expect("reduce_fn failed");

        // Wait for the result
        let result = tokio::time::timeout(Duration::from_secs(5), response_stream.next())
            .await
            .expect("timeout waiting for result")
            .expect("no result received");

        // Verify the indexing - first response should have index 0
        assert_eq!(result.index, 0);
        assert_eq!(result.vertex_name, get_vertex_name());

        // Convert response to message and verify the message ID has the correct index
        let result_message: Message = result.into();
        assert_eq!(result_message.id.index, 0);

        // Shutdown the server
        shutdown_tx
            .send(())
            .expect("failed to send shutdown signal");

        tokio::time::sleep(Duration::from_millis(100)).await;

        session_reduce_handle
            .await
            .expect("session reduce handle failed")
            .expect("session reduce failed");

        // Wait for the handle to complete
        handle.await.expect("handle failed");

        Ok(())
    }
}
