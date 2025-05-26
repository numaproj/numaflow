use crate::error::Error;
use crate::message::Message;
use crate::pipeline::isb::jetstream::writer::JetstreamWriter;
use crate::reduce::reducer::aligned::user_defined::UserDefinedAlignedReduce;
use crate::reduce::reducer::aligned::windower::{
    AlignedWindowMessage, Window, WindowManager, WindowOperation,
};
use crate::reduce::wal::segment::append::{AppendOnlyWal, SegmentWriteMessage};
use bytes::Bytes;
use chrono::{DateTime, Utc};
use numaflow_pb::objects::wal::GcEvent;
use std::collections::HashMap;
use std::ops::Sub;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_stream::StreamExt;
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info};

/// Represents an active reduce stream for a window.
struct ActiveStream {
    /// Sender for window messages. Messages are sent to this channel is received by the unique reduce
    /// task for that window.
    message_tx: mpsc::Sender<AlignedWindowMessage>,
    /// Handle to the task processing the window
    task_handle: JoinHandle<()>,
}

/// Represents a reduce task for a window. It is responsible for calling the user-defined reduce
/// function for the given window and writing the output to JetStream and publishing the watermark.
/// Also writes the GC events to the WAL if configured.
struct ReduceTask {
    client: UserDefinedAlignedReduce,
    js_writer: JetstreamWriter,
    gc_wal_tx: Option<mpsc::Sender<SegmentWriteMessage>>,
    error_tx: mpsc::Sender<Error>,
    window: Window,
    window_manager: WindowManager,
}

impl ReduceTask {
    /// Creates a new ReduceTask with the given configuration
    fn new(
        client: UserDefinedAlignedReduce,
        js_writer: JetstreamWriter,
        gc_wal_tx: Option<mpsc::Sender<SegmentWriteMessage>>,
        error_tx: mpsc::Sender<Error>,
        window: Window,
        window_manager: WindowManager,
    ) -> Self {
        Self {
            client,
            js_writer,
            gc_wal_tx,
            error_tx,
            window,
            window_manager,
        }
    }

    /// starts a task to process the window stream and returns the task handle
    async fn start(
        mut self,
        message_stream: ReceiverStream<AlignedWindowMessage>,
        cln_token: CancellationToken,
    ) -> JoinHandle<()> {
        tokio::spawn(async move {
            let (result_tx, result_rx) = mpsc::channel(100);
            let result_stream = ReceiverStream::new(result_rx);

            // Spawn a task to write results to JetStream
            let writer_handle = match self
                .js_writer
                .streaming_write(result_stream, cln_token.clone())
                .await
            {
                Ok(handle) => handle,
                Err(e) => {
                    error!(?e, "Failed to start JetStream writer");
                    return;
                }
            };

            // Call the reduce function. This is a blocking call and will return only once the window
            // is closed, cancellation is detected, or on error. The output is sent to the result_tx
            // channel which is consumed by the writer task and published to JetStream.
            let result = self
                .client
                .reduce_fn(message_stream, result_tx, cln_token)
                .await;

            if let Err(e) = result {
                // Check if this is a cancellation error
                if let Error::Cancelled() = &e {
                    info!("Cancellation detected while doing reduce operation");
                    return;
                }

                // For other errors, log and send to error channel to signal the reduce actor to stop
                // consuming new messages and exit with error.
                error!(?e, "Error while doing reduce operation");
                let _ = self.error_tx.send(e).await;
                return;
            }

            // Wait for the writer to complete, write takes care of publishing the watermark.
            if let Err(e) = writer_handle.await.expect("join failed for js writer task") {
                error!(?e, "Error while writing results to JetStream");
                let _ = self.error_tx.send(e).await;
                return;
            }

            // oldest window is used to determine the GC event incase of sliding windows, unlike fixed
            // messages can be part of multiple windows in sliding, so we can only gc the messages
            // that are less than the oldest window's start time.
            let oldest_window = self
                .window_manager
                .oldest_window()
                .expect("no oldest window found");

            // we can safely delete the window from the window manager since the results are
            // successfully written to jetstream and watermark is published.
            self.window_manager.delete_window(self.window.clone());

            // now that the processing is done, we can add this window to the GC WAL.
            let Some(gc_wal_tx) = &self.gc_wal_tx else {
                // return if the GC WAL is not configured
                return;
            };

            // Send GC event if WAL is configured
            let gc_event: GcEvent = if let WindowManager::Sliding(_) = self.window_manager {
                // for sliding window a message can be part of multiple windows, we can only delete the
                // messages that are less than the oldest window's start time.
                Window {
                    start_time: oldest_window.start_time,
                    end_time: oldest_window.start_time,
                    id: oldest_window.id,
                }
                .into()
            } else {
                // messages of fixed window are not part of multiple windows, so we can delete all the
                // messages that are less than the window's end time.
                self.window.clone().into()
            };

            debug!(?gc_event, "Sending GC event to WAL");
            gc_wal_tx
                .send(SegmentWriteMessage::WriteData {
                    offset: None,
                    data: prost::Message::encode_to_vec(&gc_event).into(),
                })
                .await
                .expect("failed to write gc event to wal");
        })
    }
}

/// Actor that manages multiple window reduction streams. It manages [ActiveStream]s and manages the
/// lifecycle of the reduce tasks.
struct AlignedReduceActor {
    /// It multiplexes the messages to the receiver to the reduce tasks through the corresponding
    /// tx in [ActiveStream].
    receiver: mpsc::Receiver<AlignedWindowMessage>,
    /// Client for user-defined reduce operations.
    client: UserDefinedAlignedReduce,
    /// Map of [ActiveStream]s keyed by window ID (pnf_slot).
    active_streams: HashMap<Bytes, ActiveStream>,
    /// JetStream writer for writing results of reduce operation.
    js_writer: JetstreamWriter,
    /// Sender for error messages.
    error_tx: mpsc::Sender<Error>,
    /// Sender for GC WAL messages. It is optional since users can specify not to use WAL.
    gc_wal_tx: Option<mpsc::Sender<SegmentWriteMessage>>,
    /// WindowManager for assigning windows to messages and closing windows.
    window_manager: WindowManager,
    /// Cancellation token to signal tasks to stop
    cln_token: CancellationToken,
}

impl AlignedReduceActor {
    /// Waits for all active tasks to complete
    async fn wait_for_all_tasks(&mut self) {
        info!(
            "Waiting for {} active reduce tasks to complete",
            self.active_streams.len()
        );

        // Collect all active streams to avoid borrowing issues
        let active_streams: Vec<_> = self.active_streams.drain().collect();

        for (window_id, active_stream) in active_streams {
            // Wait for the task to complete
            if let Err(e) = active_stream.task_handle.await {
                error!(?window_id, err = ?e, "Reduce task for window failed during shutdown");
            }
            info!(?window_id, "Reduce task for window completed");
        }

        info!("All reduce tasks completed");
    }

    pub(crate) async fn new(
        client: UserDefinedAlignedReduce,
        receiver: mpsc::Receiver<AlignedWindowMessage>,
        js_writer: JetstreamWriter,
        error_tx: mpsc::Sender<Error>,
        gc_wal_tx: Option<mpsc::Sender<SegmentWriteMessage>>,
        window_manager: WindowManager,
        cln_token: CancellationToken,
    ) -> Self {
        Self {
            client,
            receiver,
            active_streams: HashMap::new(),
            js_writer,
            error_tx,
            gc_wal_tx,
            window_manager,
            cln_token,
        }
    }

    /// Runs the actor, listening for messages and multiplexing them to the reduce tasks.
    async fn run(mut self) {
        while let Some(msg) = self.receiver.recv().await {
            self.handle_window_message(msg).await;
        }
        self.wait_for_all_tasks().await;
    }

    /// Handle a window message based on its operation type
    async fn handle_window_message(&mut self, window_msg: AlignedWindowMessage) {
        match &window_msg.operation {
            WindowOperation::Open(_) => self.window_open(window_msg).await,
            WindowOperation::Append(_) => self.window_append(window_msg).await,
            WindowOperation::Close => self.window_close(window_msg).await,
        }
    }

    /// Creates a new reduce task for the window and sends the initial Open command with the
    /// first message.
    async fn window_open(&mut self, window_msg: AlignedWindowMessage) {
        // Create a new channel for this window's messages
        let (message_tx, message_rx) = mpsc::channel(100);
        let message_stream = ReceiverStream::new(message_rx);
        let window = window_msg.window.clone();

        // Create a ReduceTask
        let reduce_task = ReduceTask::new(
            self.client.clone(),
            self.js_writer.clone(),
            self.gc_wal_tx.clone(),
            self.error_tx.clone(),
            window.clone(),
            self.window_manager.clone(),
        );

        // start the reduce task and store the handle and the sender so that we can send messages
        // and wait for it to complete.
        let task_handle = reduce_task
            .start(message_stream, self.cln_token.clone())
            .await;

        self.active_streams.insert(
            window.pnf_slot(),
            ActiveStream {
                message_tx: message_tx.clone(),
                task_handle,
            },
        );

        // Send the open command with the first message
        message_tx.send(window_msg).await.expect("Receiver dropped");
    }

    /// sends the message to the reduce task for the window.
    async fn window_append(&mut self, window_msg: AlignedWindowMessage) {
        let window = window_msg.window.clone();
        let window_id = window.pnf_slot();

        // Get the existing stream or log error if not found create a new one.
        let Some(active_stream) = self.active_streams.get(&window_id) else {
            // windows may not be found during replay, because the windower doesn't send the open
            // message for the active windows that got replayed, hence we create a new one.
            // this happens because of out-of-order messages and we have to ensure that the (t+1)th
            // message is sent to the window that could be created by (t)th message iff (t+1)th message
            // belongs to that window created by (t)th message.
            self.window_open(window_msg).await;
            return;
        };

        // Send the append message
        active_stream
            .message_tx
            .send(window_msg)
            .await
            .expect("Receiver dropped");
    }

    /// Closes the reduce task for the window.
    async fn window_close(&mut self, window_msg: AlignedWindowMessage) {
        let window_id = window_msg.window.pnf_slot();

        // Get the existing stream or log error if not found
        let Some(active_stream) = self.active_streams.remove(&window_id) else {
            error!("No active stream found for window {:?}", window_id);
            return;
        };

        // we don't need to write the close message to the client, stream closing
        // is considered as close for aligned windows.
        // Drop the sender to signal completion
        drop(active_stream.message_tx);

        // Wait for the task to complete
        if let Err(e) = active_stream.task_handle.await {
            error!(?window_id, err = ?e,"Reduce task for window failed");
        }
    }
}

/// Processes messages and forwards results to the next stage.
pub(crate) struct AlignedReducer {
    client: UserDefinedAlignedReduce,
    /// Window manager for assigning windows to messages and closing windows.
    window_manager: WindowManager,
    /// Writer for writing results to JetStream
    js_writer: JetstreamWriter,
    /// Final state of the component (any error will set this as Err).
    final_result: crate::Result<()>,
    /// Set to true when shutting down due to an error.
    shutting_down_on_err: bool,
    /// WAL for writing GC events
    gc_wal: Option<AppendOnlyWal>,
    /// Allowed lateness for the messages to be accepted and delay the close of book.
    allowed_lateness: Duration,
    /// current watermark for the reduce vertex.
    current_watermark: DateTime<Utc>,
}

impl AlignedReducer {
    pub(crate) async fn new(
        client: UserDefinedAlignedReduce,
        window_manager: WindowManager,
        js_writer: JetstreamWriter,
        gc_wal: Option<AppendOnlyWal>,
        allowed_lateness: Duration,
    ) -> Self {
        Self {
            client,
            window_manager,
            js_writer,
            final_result: Ok(()),
            shutting_down_on_err: false,
            gc_wal,
            allowed_lateness,
            current_watermark: DateTime::from_timestamp_millis(-1).expect("Invalid timestamp"),
        }
    }

    /// Starts the reduce component and returns a handle to the main task.
    pub(crate) async fn start(
        mut self,
        input_stream: ReceiverStream<Message>,
        cln_token: CancellationToken,
    ) -> crate::Result<JoinHandle<crate::Result<()>>> {
        // Set up error and GC channels
        let (error_tx, mut error_rx) = mpsc::channel(10);
        let gc_wal_handle = self.setup_gc_wal().await?;

        // Create the actor channel and start the actor
        let (actor_tx, actor_rx) = mpsc::channel(100);
        let actor = AlignedReduceActor::new(
            self.client.clone(),
            actor_rx,
            self.js_writer.clone(),
            error_tx.clone(),
            gc_wal_handle,
            self.window_manager.clone(),
            cln_token.clone(),
        )
        .await;

        // start the reduce actor
        let actor_handle = tokio::spawn(async move {
            actor.run().await;
        });

        // Start the main task
        let handle = tokio::spawn(async move {
            let mut input_stream = input_stream;

            loop {
                tokio::select! {
                    // Check for errors from reduce tasks
                    Some(err) = error_rx.recv() => {
                        self.handle_error(err, &cln_token);
                    }

                    // Process input messages
                    read_msg = input_stream.next() => {
                        let Some(msg) = read_msg else {
                            break;
                        };

                        // update the watermark.
                        // we cannot simply assign incoming message's watermark as the current watermark,
                        // because it can be -1. watermark will never regress, so use max.
                        self.current_watermark = self.current_watermark.max(msg.watermark.unwrap_or_default());

                        // only drop the message if it is late and the event time is before the watermark - allowed lateness
                        if msg.is_late && msg.event_time < self.current_watermark.sub(self.allowed_lateness) {
                            // TODO(ajain): add a metric for this
                            continue;
                        }

                        if self.current_watermark > msg.event_time {
                            error!(current_watermark=?self.current_watermark, message_event_time=?msg.event_time, "Old message popped up, Watermark is behind the event time");
                            continue;
                        }

                        // If shutting down, drain the stream
                        if self.shutting_down_on_err {
                            info!("Reduce component is shutting down due to an error, not accepting the message");
                            continue;
                        }

                        // check if any windows can be closed
                        let window_messages = self.window_manager.close_windows(self.current_watermark.sub(self.allowed_lateness));
                        for window_msg in window_messages {
                            actor_tx.send(window_msg).await.expect("Receiver dropped");
                        }

                        // assign windows to the message
                        let window_messages = self.window_manager.assign_windows(msg);

                        // Send each window message to the actor for processing
                        for window_msg in window_messages {
                            actor_tx.send(window_msg).await.expect("Receiver dropped");
                        }
                    }
                }
            }

            // Drop the sender to signal the actor to stop
            info!("Reduce component is shutting down, waiting for active reduce tasks to complete");
            drop(actor_tx);

            // Wait for the actor to complete
            if let Err(e) = actor_handle.await {
                error!("Error waiting for actor to complete: {:?}", e);
            }

            // For sliding: we need to make sure to store the window manager state before exiting
            // from the reducer component
            if let WindowManager::Sliding(manager) = self.window_manager {
                if let Err(e) = manager.save_state() {
                    error!("Failed to save window state: {:?}", e);
                }
            }

            info!(status=?self.final_result, "Reduce component successfully completed");
            self.final_result
        });

        Ok(handle)
    }

    /// Set up the GC WAL if configured.
    async fn setup_gc_wal(&mut self) -> crate::Result<Option<mpsc::Sender<SegmentWriteMessage>>> {
        if let Some(gc_wal) = self.gc_wal.take() {
            let (gc_tx, gc_rx) = mpsc::channel(100);
            gc_wal.streaming_write(ReceiverStream::new(gc_rx)).await?;
            Ok(Some(gc_tx))
        } else {
            Ok(None)
        }
    }

    /// Handles errors from reduce tasks, cancels the token to signal the upstream to stop sending
    /// new messages and updates the final result and shutting_down_on_err flags so that we can
    /// go to shut down mode by draining the input stream and exit.
    fn handle_error(&mut self, error: Error, cln_token: &CancellationToken) {
        if self.final_result.is_ok() {
            error!(?error, "Error received while performing reduce operation");
            cln_token.cancel();
            self.final_result = Err(error);
            self.shutting_down_on_err = true;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::sync::Arc;
    use std::time::Duration;

    use super::*;
    use crate::config::pipeline::ToVertexConfig;
    use crate::config::pipeline::isb::{BufferWriterConfig, Stream};
    use crate::message::{Message, MessageID, Offset, StringOffset};
    use crate::pipeline::isb::jetstream::writer::JetstreamWriter;
    use crate::reduce::reducer::aligned::user_defined::UserDefinedAlignedReduce;
    use crate::reduce::reducer::aligned::windower::fixed::FixedWindowManager;
    use crate::reduce::reducer::aligned::windower::sliding::SlidingWindowManager;
    use crate::shared::grpc::create_rpc_channel;
    use crate::tracker::TrackerHandle;
    use async_nats::jetstream::consumer::PullConsumer;
    use async_nats::jetstream::{self, consumer, stream};
    use chrono::{TimeZone, Utc};
    use numaflow::reduce;
    use numaflow_pb::clients::reduce::reduce_client::ReduceClient;
    use prost::Message as ProstMessage;
    use tempfile::TempDir;
    use tokio::sync::mpsc;
    use tokio_stream::wrappers::ReceiverStream;
    use tokio_util::sync::CancellationToken;

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

    #[cfg(feature = "nats-tests")]
    #[tokio::test]
    async fn test_aligned_reducer_with_fixed_window() -> crate::Result<()> {
        // Set up the reducer server
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let tmp_dir = TempDir::new().unwrap();
        let sock_file = tmp_dir.path().join("reduce_fixed.sock");
        let server_info_file = tmp_dir.path().join("reduce_fixed-server-info");

        let server_info = server_info_file.clone();
        let server_socket = sock_file.clone();
        let server_handle = tokio::spawn(async move {
            reduce::Server::new(CounterCreator {})
                .with_socket_file(server_socket)
                .with_server_info_file(server_info)
                .start_with_shutdown(shutdown_rx)
                .await
                .expect("server failed");
        });

        // Wait for the server to start
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Create the client
        let client =
            UserDefinedAlignedReduce::new(ReduceClient::new(create_rpc_channel(sock_file).await?))
                .await;

        // Create a fixed window manager with 60s window length
        let windower = FixedWindowManager::new(Duration::from_secs(60));

        // Set up JetStream
        let js_url = "localhost:4222";
        let nats_client = async_nats::connect(js_url).await.unwrap();
        let js_context = jetstream::new(nats_client);

        // Create output stream
        let stream = Stream::new("test_aligned_reducer_fixed", "test", 0);
        // Delete stream if it exists
        let _ = js_context.delete_stream(stream.name).await;

        let _stream = js_context
            .get_or_create_stream(stream::Config {
                name: stream.name.to_string(),
                subjects: vec![stream.name.to_string()],
                ..Default::default()
            })
            .await
            .unwrap();

        // Create consumer
        let _consumer = js_context
            .create_consumer_on_stream(
                consumer::Config {
                    name: Some(stream.name.to_string()),
                    ack_policy: consumer::AckPolicy::Explicit,
                    ..Default::default()
                },
                stream.name,
            )
            .await
            .unwrap();

        // Create JetstreamWriter
        let cln_token = CancellationToken::new();
        let tracker_handle = TrackerHandle::new(None, None);
        let js_writer = JetstreamWriter::new(
            vec![ToVertexConfig {
                name: "test-vertex",
                partitions: 1,
                writer_config: BufferWriterConfig {
                    streams: vec![stream.clone()],
                    ..Default::default()
                },
                conditions: None,
            }],
            js_context.clone(),
            100,
            tracker_handle.clone(),
            cln_token.clone(),
            None,
            "Reduce".to_string(),
        );

        // Create the AlignedReducer
        let reducer = AlignedReducer::new(
            client,
            WindowManager::Fixed(windower),
            js_writer,
            None, // No GC WAL for testing
            Duration::from_secs(0),
        )
        .await;

        // Create a channel for input messages
        let (input_tx, input_rx) = mpsc::channel(10);
        let input_stream = ReceiverStream::new(input_rx);

        // Start the reducer
        let reducer_handle = reducer.start(input_stream, cln_token.clone()).await?;

        // Create test messages
        let base_time = Utc.with_ymd_and_hms(2023, 1, 1, 0, 0, 0).unwrap();

        // Message 1: Within the first window
        let msg1 = Message {
            typ: Default::default(),
            keys: Arc::from(vec!["key1".into()]),
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

        // Message 2: Within the first window
        let msg2 = Message {
            typ: Default::default(),
            keys: Arc::from(vec!["key1".into()]),
            tags: None,
            value: "value2".into(),
            offset: Offset::String(StringOffset::new("1".to_string(), 1)),
            event_time: base_time + chrono::Duration::seconds(30),
            watermark: None,
            id: MessageID {
                vertex_name: "vertex_name".to_string().into(),
                offset: "1".to_string().into(),
                index: 1,
            },
            ..Default::default()
        };

        // Message 3: Within the first window
        let msg3 = Message {
            typ: Default::default(),
            keys: Arc::from(vec!["key1".into()]),
            tags: None,
            value: "value3".into(),
            offset: Offset::String(StringOffset::new("2".to_string(), 2)),
            event_time: base_time + chrono::Duration::seconds(50),
            watermark: Some(base_time + chrono::Duration::seconds(40)),
            id: MessageID {
                vertex_name: "vertex_name".to_string().into(),
                offset: "2".to_string().into(),
                index: 2,
            },
            ..Default::default()
        };

        // Message 4: Within the first window but with watermark past window end
        let msg4 = Message {
            typ: Default::default(),
            keys: Arc::from(vec!["key1".into()]),
            tags: None,
            value: "value3".into(),
            offset: Offset::String(StringOffset::new("2".to_string(), 2)),
            event_time: base_time + chrono::Duration::seconds(80),
            watermark: Some(base_time + chrono::Duration::seconds(70)), // Past window end
            id: MessageID {
                vertex_name: "vertex_name".to_string().into(),
                offset: "2".to_string().into(),
                index: 2,
            },
            ..Default::default()
        };

        // Send the messages
        input_tx.send(msg1).await.unwrap();
        input_tx.send(msg2).await.unwrap();

        // Send message with watermark past window end to trigger window close
        input_tx.send(msg3).await.unwrap();
        input_tx.send(msg4).await.unwrap();

        // Create a consumer to read the results
        let consumer: PullConsumer = js_context
            .get_consumer_from_stream(&stream.name, &stream.name)
            .await
            .unwrap();

        // Read messages from the stream
        let mut messages = consumer
            .fetch()
            .expires(Duration::from_secs(1))
            .messages()
            .await
            .unwrap();

        let mut result_count = 0;

        while let Some(msg) = messages.next().await {
            let msg = msg.unwrap();

            let data = msg.payload.to_vec();

            // Acknowledge the message
            msg.ack().await.unwrap();

            // Parse the message
            let message: numaflow_pb::objects::isb::Message =
                prost::Message::decode(data.as_ref()).unwrap();

            // Verify the result
            assert_eq!(message.header.unwrap().keys.to_vec(), vec!["key1"]);
            assert_eq!(message.body.unwrap().payload.to_vec(), b"3".to_vec());

            result_count += 1;
        }

        assert_eq!(result_count, 1, "Expected exactly one result message");

        cln_token.cancel();
        drop(input_tx);

        // Wait for the reducer to complete
        reducer_handle.await.expect("reducer handle failed")?;

        // Shutdown the server
        shutdown_tx
            .send(())
            .expect("failed to send shutdown signal");

        tokio::time::sleep(Duration::from_millis(50)).await;

        // Wait for the server to shut down
        assert!(
            server_handle.is_finished(),
            "Expected gRPC server to have shut down"
        );

        // Clean up JetStream
        js_context.delete_stream(stream.name).await.unwrap();

        Ok(())
    }

    #[cfg(feature = "nats-tests")]
    #[tokio::test]
    async fn test_aligned_reducer_with_sliding_window() -> crate::Result<()> {
        // Set up the reducer server
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let tmp_dir = TempDir::new().unwrap();
        let sock_file = tmp_dir.path().join("reduce_sliding.sock");
        let server_info_file = tmp_dir.path().join("reduce_sliding-server-info");

        let server_info = server_info_file.clone();
        let server_socket = sock_file.clone();
        let server_handle = tokio::spawn(async move {
            reduce::Server::new(CounterCreator {})
                .with_socket_file(server_socket)
                .with_server_info_file(server_info)
                .start_with_shutdown(shutdown_rx)
                .await
                .expect("server failed");
        });

        // Wait for the server to start
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Create the client
        let client =
            UserDefinedAlignedReduce::new(ReduceClient::new(create_rpc_channel(sock_file).await?))
                .await;

        // Create a sliding window manager with 60s window length and 20s slide
        let windower =
            SlidingWindowManager::new(Duration::from_secs(60), Duration::from_secs(20), None);

        // Set up JetStream
        let js_url = "localhost:4222";
        let nats_client = async_nats::connect(js_url).await.unwrap();
        let js_context = jetstream::new(nats_client);

        // Create output stream
        let stream = Stream::new("test_aligned_reducer_sliding", "test", 0);
        // Delete stream if it exists
        let _ = js_context.delete_stream(stream.name).await;
        let _stream = js_context
            .get_or_create_stream(stream::Config {
                name: stream.name.to_string(),
                subjects: vec![stream.name.to_string()],
                ..Default::default()
            })
            .await
            .unwrap();

        // Create consumer
        let _consumer = js_context
            .create_consumer_on_stream(
                consumer::Config {
                    name: Some(stream.name.to_string()),
                    ack_policy: consumer::AckPolicy::Explicit,
                    ..Default::default()
                },
                stream.name,
            )
            .await
            .unwrap();

        // Create JetstreamWriter
        let cln_token = CancellationToken::new();
        let tracker_handle = TrackerHandle::new(None, None);
        let js_writer = JetstreamWriter::new(
            vec![ToVertexConfig {
                name: "test-vertex",
                partitions: 1,
                writer_config: BufferWriterConfig {
                    streams: vec![stream.clone()],
                    ..Default::default()
                },
                conditions: None,
            }],
            js_context.clone(),
            100,
            tracker_handle.clone(),
            cln_token.clone(),
            None,
            "Reduce".to_string(),
        );

        // Create the AlignedReducer
        let reducer = AlignedReducer::new(
            client,
            WindowManager::Sliding(windower),
            js_writer,
            None, // No GC WAL for testing
            Duration::from_secs(0),
        )
        .await;

        // Create a channel for input messages
        let (input_tx, input_rx) = mpsc::channel(100);
        let input_stream = ReceiverStream::new(input_rx);

        // Start the reducer
        let reducer_handle = reducer.start(input_stream, cln_token.clone()).await?;

        // Create test messages
        let base_time = Utc.with_ymd_and_hms(2023, 1, 1, 0, 0, 0).unwrap();

        // Message 1: Within the first set of sliding windows
        let msg1 = Message {
            typ: Default::default(),
            keys: Arc::from(vec!["key1".into()]),
            tags: None,
            value: "value1".into(),
            offset: Offset::String(StringOffset::new("0".to_string(), 0)),
            event_time: base_time,
            watermark: None,
            id: MessageID {
                vertex_name: "vertex_name".to_string().into(),
                offset: "0".to_string().into(),
                index: 0,
            },
            ..Default::default()
        };

        // Message 2: Within the first set of sliding windows
        let msg2 = Message {
            typ: Default::default(),
            keys: Arc::from(vec!["key1".into()]),
            tags: None,
            value: "value2".into(),
            offset: Offset::String(StringOffset::new("1".to_string(), 1)),
            event_time: base_time,
            watermark: None,
            id: MessageID {
                vertex_name: "vertex_name".to_string().into(),
                offset: "1".to_string().into(),
                index: 1,
            },
            ..Default::default()
        };

        // Message 3: Within the first window
        let msg3 = Message {
            typ: Default::default(),
            keys: Arc::from(vec!["key1".into()]),
            tags: None,
            value: "value3".into(),
            offset: Offset::String(StringOffset::new("2".to_string(), 2)),
            event_time: base_time,
            watermark: None,
            id: MessageID {
                vertex_name: "vertex_name".to_string().into(),
                offset: "2".to_string().into(),
                index: 2,
            },
            ..Default::default()
        };

        // Message 4: Within the first window but with watermark past window end
        let msg4 = Message {
            typ: Default::default(),
            keys: Arc::from(vec!["key1".into()]),
            tags: None,
            value: "value3".into(),
            offset: Offset::String(StringOffset::new("3".to_string(), 2)),
            event_time: base_time + chrono::Duration::seconds(120),
            watermark: Some(base_time + chrono::Duration::seconds(100)), // Past window end
            id: MessageID {
                vertex_name: "vertex_name".to_string().into(),
                offset: "3".to_string().into(),
                index: 3,
            },
            ..Default::default()
        };

        // Send the messages
        input_tx.send(msg1).await.unwrap();
        input_tx.send(msg2).await.unwrap();
        input_tx.send(msg3).await.unwrap();
        // Send message with watermark past window end to trigger window close
        input_tx.send(msg4).await.unwrap();

        // Create a consumer to read the results
        let consumer: PullConsumer = js_context
            .get_consumer_from_stream(&stream.name, &stream.name)
            .await
            .unwrap();

        // Read messages from the stream
        let mut messages = consumer
            .batch()
            .expires(Duration::from_secs(1))
            .max_messages(3)
            .messages()
            .await
            .unwrap();

        let mut result_count = 0;

        while let Some(msg) = messages.next().await {
            let msg = msg.unwrap();
            let data = msg.payload.to_vec();

            // Acknowledge the message
            msg.ack().await.unwrap();

            // Parse the message
            let proto_message = numaflow_pb::objects::isb::Message::decode(&data[..]).unwrap();

            // Verify the result
            assert_eq!(proto_message.header.unwrap().keys.to_vec(), vec!["key1"]);
            assert_eq!(
                String::from_utf8(proto_message.body.unwrap().payload.to_vec()).unwrap(),
                "3"
            ); // Counter should be 3

            result_count += 1;
        }

        assert_eq!(
            result_count, 3,
            "Expected exactly three result messages for sliding windows"
        );

        cln_token.cancel();
        drop(input_tx);

        // Wait for the reducer to complete
        reducer_handle.await.expect("reducer handle failed")?;

        // Shutdown the server
        shutdown_tx
            .send(())
            .expect("failed to send shutdown signal");

        tokio::time::sleep(Duration::from_millis(50)).await;

        // Wait for the server to shut down
        assert!(
            server_handle.is_finished(),
            "Expected gRPC server to have shut down"
        );

        // Clean up JetStream
        js_context.delete_stream(stream.name).await.unwrap();

        Ok(())
    }

    #[cfg(feature = "nats-tests")]
    #[tokio::test]
    async fn test_aligned_reducer_with_multiple_keys() -> crate::Result<()> {
        // Set up the reducer server
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let tmp_dir = TempDir::new().unwrap();
        let sock_file = tmp_dir.path().join("reduce_multi_keys.sock");
        let server_info_file = tmp_dir.path().join("reduce_multi_keys-server-info");

        let server_info = server_info_file.clone();
        let server_socket = sock_file.clone();
        let server_handle = tokio::spawn(async move {
            reduce::Server::new(CounterCreator {})
                .with_socket_file(server_socket)
                .with_server_info_file(server_info)
                .start_with_shutdown(shutdown_rx)
                .await
                .expect("server failed");
        });

        // Wait for the server to start
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Create the client
        let client =
            UserDefinedAlignedReduce::new(ReduceClient::new(create_rpc_channel(sock_file).await?))
                .await;

        // Create a fixed window manager with 60s window length
        let windower = FixedWindowManager::new(Duration::from_secs(60));

        // Set up JetStream
        let js_url = "localhost:4222";
        let nats_client = async_nats::connect(js_url).await.unwrap();
        let js_context = jetstream::new(nats_client);

        // Create output stream
        let stream = Stream::new("test_aligned_reducer_multi_keys", "test", 0);
        // Delete stream if it exists
        let _ = js_context.delete_stream(stream.name).await;
        let _stream = js_context
            .get_or_create_stream(stream::Config {
                name: stream.name.to_string(),
                subjects: vec![stream.name.to_string()],
                ..Default::default()
            })
            .await
            .unwrap();

        // Create consumer
        let _consumer = js_context
            .create_consumer_on_stream(
                consumer::Config {
                    name: Some(stream.name.to_string()),
                    ack_policy: consumer::AckPolicy::Explicit,
                    ..Default::default()
                },
                stream.name,
            )
            .await
            .unwrap();

        // Create JetstreamWriter
        let cln_token = CancellationToken::new();
        let tracker_handle = TrackerHandle::new(None, None);
        let js_writer = JetstreamWriter::new(
            vec![ToVertexConfig {
                name: "test-vertex",
                partitions: 1,
                writer_config: BufferWriterConfig {
                    streams: vec![stream.clone()],
                    ..Default::default()
                },
                conditions: None,
            }],
            js_context.clone(),
            100,
            tracker_handle.clone(),
            cln_token.clone(),
            None,
            "Reduce".to_string(),
        );

        // Create the AlignedReducer
        let reducer = AlignedReducer::new(
            client,
            WindowManager::Fixed(windower),
            js_writer,
            None, // No GC WAL for testing
            Duration::from_secs(0),
        )
        .await;

        // Create a channel for input messages
        let (input_tx, input_rx) = mpsc::channel(100);
        let input_stream = ReceiverStream::new(input_rx);

        // Start the reducer
        let reducer_handle = reducer.start(input_stream, cln_token.clone()).await?;

        // Create test messages
        let base_time = Utc.with_ymd_and_hms(2023, 1, 1, 0, 0, 0).unwrap();

        // Message 1: Within the first window for key1
        let msg1 = Message {
            typ: Default::default(),
            keys: Arc::from(vec!["key1".into()]),
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

        // Message 2: Within the first window for key2
        let msg2 = Message {
            typ: Default::default(),
            keys: Arc::from(vec!["key2".into()]),
            tags: None,
            value: "value2".into(),
            offset: Offset::String(StringOffset::new("1".to_string(), 1)),
            event_time: base_time + chrono::Duration::seconds(20),
            watermark: None,
            id: MessageID {
                vertex_name: "vertex_name".to_string().into(),
                offset: "1".to_string().into(),
                index: 1,
            },
            ..Default::default()
        };

        // Message 3: Within the first window for key1
        let msg3 = Message {
            typ: Default::default(),
            keys: Arc::from(vec!["key1".into()]),
            tags: None,
            value: "value3".into(),
            offset: Offset::String(StringOffset::new("2".to_string(), 2)),
            event_time: base_time + chrono::Duration::seconds(30),
            watermark: None,
            id: MessageID {
                vertex_name: "vertex_name".to_string().into(),
                offset: "2".to_string().into(),
                index: 2,
            },
            ..Default::default()
        };

        // Message 4: Within the first window for key2
        let msg4 = Message {
            typ: Default::default(),
            keys: Arc::from(vec!["key2".into()]),
            tags: None,
            value: "value4".into(),
            offset: Offset::String(StringOffset::new("3".to_string(), 3)),
            event_time: base_time + chrono::Duration::seconds(40),
            watermark: None,
            id: MessageID {
                vertex_name: "vertex_name".to_string().into(),
                offset: "3".to_string().into(),
                index: 3,
            },
            ..Default::default()
        };

        // Message 5: With watermark past the first window end for key1
        let msg5 = Message {
            typ: Default::default(),
            keys: Arc::from(vec!["key1".into()]),
            tags: None,
            value: "value5".into(),
            offset: Offset::String(StringOffset::new("4".to_string(), 4)),
            event_time: base_time + chrono::Duration::seconds(90),
            watermark: Some(base_time + chrono::Duration::seconds(70)), // Past window end
            id: MessageID {
                vertex_name: "vertex_name".to_string().into(),
                offset: "4".to_string().into(),
                index: 4,
            },
            ..Default::default()
        };

        // Message 6: With watermark past the first window end for key2
        let msg6 = Message {
            typ: Default::default(),
            keys: Arc::from(vec!["key2".into()]),
            tags: None,
            value: "value6".into(),
            offset: Offset::String(StringOffset::new("5".to_string(), 5)),
            event_time: base_time + chrono::Duration::seconds(90),
            watermark: Some(base_time + chrono::Duration::seconds(80)), // Past window end
            id: MessageID {
                vertex_name: "vertex_name".to_string().into(),
                offset: "5".to_string().into(),
                index: 5,
            },
            ..Default::default()
        };

        // Send the messages
        input_tx.send(msg1).await.unwrap();
        input_tx.send(msg2).await.unwrap();
        input_tx.send(msg3).await.unwrap();
        input_tx.send(msg4).await.unwrap();
        input_tx.send(msg5).await.unwrap();
        input_tx.send(msg6).await.unwrap();

        // Create a consumer to read the results
        let consumer: PullConsumer = js_context
            .get_consumer_from_stream(&stream.name, &stream.name)
            .await
            .unwrap();

        // Read messages from the stream
        // Read messages from the stream
        let mut messages = consumer
            .batch()
            .expires(Duration::from_secs(1))
            .max_messages(2)
            .messages()
            .await
            .unwrap();

        let mut result_count = 0;
        let mut received_keys = HashSet::new();

        while let Some(msg) = messages.next().await {
            let msg = msg.unwrap();
            let data = msg.payload.to_vec();

            // Acknowledge the message
            msg.ack().await.unwrap();

            // Parse the message
            let proto_message = numaflow_pb::objects::isb::Message::decode(&data[..]).unwrap();

            // Extract and store the key
            let key = proto_message.header.unwrap().keys[0].clone();
            received_keys.insert(key);

            result_count += 1;
        }

        // Verify we received results for both keys
        assert!(received_keys.contains("key1"), "Missing result for key1");
        assert!(received_keys.contains("key2"), "Missing result for key2");
        assert_eq!(
            result_count, 2,
            "Expected exactly two result messages for two keys"
        );

        cln_token.cancel();
        drop(input_tx);

        // Wait for the reducer to complete
        reducer_handle.await.expect("reducer handle failed")?;

        // Shutdown the server
        shutdown_tx
            .send(())
            .expect("failed to send shutdown signal");
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Wait for the server to shut down
        assert!(
            server_handle.is_finished(),
            "Expected gRPC server to have shut down"
        );

        // Clean up JetStream
        js_context.delete_stream(stream.name).await.unwrap();

        Ok(())
    }
}
