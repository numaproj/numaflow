use crate::error::Error;
use crate::message::Message;
use crate::pipeline::isb::jetstream::writer::JetstreamWriter;
use crate::reduce::reducer::unaligned::user_defined::accumulator::UserDefinedAccumulator;
use crate::reduce::reducer::unaligned::user_defined::session::UserDefinedSessionReduce;
use crate::reduce::reducer::unaligned::windower::{
    UnalignedWindowManager, UnalignedWindowMessage, Window, WindowOperation,
};
use crate::reduce::wal::segment::append::{AppendOnlyWal, SegmentWriteMessage};
use bytes::Bytes;
use numaflow_pb::objects::wal::GcEvent;
use std::collections::HashMap;
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
    message_tx: mpsc::Sender<UnalignedWindowMessage>,
    /// Handle to the task processing the window
    task_handle: JoinHandle<()>,
}

/// Represents a reduce task for a window. It is responsible for calling the user-defined reduce
/// function for the given window and writing the output to JetStream and publishing the watermark.
/// Also writes the GC events to the WAL if configured.
struct ReduceTask {
    /// Client for user-defined reduce operations.
    client_type: UnalignedReduceClientType,
    /// JetStream writer for writing results of reduce operation.
    js_writer: JetstreamWriter,
    /// Sender for GC WAL messages. It is optional since users can specify not to use WAL.
    gc_wal_tx: Option<mpsc::Sender<SegmentWriteMessage>>,
    /// Sender for error messages.
    error_tx: mpsc::Sender<e>,
    /// Window being processed by this task.
    window: Window,
    /// Window manager for assigning windows to messages and closing windows.
    window_manager: UnalignedWindowManager,
}

impl ReduceTask {
    /// Creates a new ReduceTask with the given configuration for Accumulator
    fn new_accumulator(
        client: UserDefinedAccumulator,
        js_writer: JetstreamWriter,
        gc_wal_tx: Option<mpsc::Sender<SegmentWriteMessage>>,
        error_tx: mpsc::Sender<e>,
        window: Window,
        window_manager: UnalignedWindowManager,
    ) -> Self {
        Self {
            client_type: UnalignedReduceClientType::Accumulator(client),
            js_writer,
            gc_wal_tx,
            error_tx,
            window,
            window_manager,
        }
    }

    /// Creates a new ReduceTask with the given configuration for Session
    fn new_session(
        client: UserDefinedSessionReduce,
        js_writer: JetstreamWriter,
        gc_wal_tx: Option<mpsc::Sender<SegmentWriteMessage>>,
        error_tx: mpsc::Sender<e>,
        window: Window,
        window_manager: UnalignedWindowManager,
    ) -> Self {
        Self {
            client_type: UnalignedReduceClientType::Session(client),
            js_writer,
            gc_wal_tx,
            error_tx,
            window,
            window_manager,
        }
    }

    /// starts a task to process the window stream and returns the task handle
    async fn start(
        self,
        message_stream: ReceiverStream<UnalignedWindowMessage>,
        cln_token: CancellationToken,
    ) -> JoinHandle<()> {
        tokio::spawn(async move {
            let (result_tx, result_rx) = mpsc::channel(100);
            let result_stream = ReceiverStream::new(result_rx);

            // Call the appropriate reduce_fn based on the client type
            let client_result = match &self.client_type {
                UnalignedReduceClientType::Accumulator(client) => {
                    let mut client_clone = client.clone();
                    client_clone
                        .reduce_fn(message_stream, result_tx, cln_token.clone())
                        .await
                }
                UnalignedReduceClientType::Session(client) => {
                    let mut client_clone = client.clone();
                    client_clone
                        .reduce_fn(message_stream, result_tx, cln_token.clone())
                        .await
                }
            };
            
            // Extract the fields we need for the rest of the function
            let js_writer = self.js_writer;
            let gc_wal_tx = self.gc_wal_tx;
            let error_tx = self.error_tx;
            let window = self.window;
            let window_manager = self.window_manager;

            // Spawn a task to write results to JetStream
            let writer_handle = match js_writer
                .streaming_write(result_stream, cln_token.clone())
                .await
            {
                Ok(handle) => handle,
                Err(e) => {
                    error!(?e, "Failed to start JetStream writer");
                    return;
                }
            };

            if let Err(e) = client_result {
                // Check if this is a cancellation error
                if let Error::Cancelled() = &e {
                    info!("Cancellation detected while doing reduce operation");
                    return;
                }

                // For other errors, log and send to error channel to signal the reduce actor to stop
                // consuming new messages and exit with error.
                error!(?e, "Error while doing reduce operation");
                let _ = error_tx.send(e).await;
                return;
            }

            // Write GC event to WAL if configured
            if let Some(gc_wal_tx) = gc_wal_tx {
                let gc_event = GcEvent {
                    window_end_time: Some(prost_types::Timestamp {
                        seconds: window.end_time.timestamp(),
                        nanos: window.end_time.timestamp_subsec_nanos() as i32,
                    }),
                    keys: window.keys.to_vec(),
                };

                let gc_event_bytes = gc_event.encode_to_vec();
                let _ = gc_wal_tx
                    .send(SegmentWriteMessage {
                        data: gc_event_bytes,
                    })
                    .await;
            }

            // Delete the window from the window manager
            window_manager.delete_closed_window(window);

            // Wait for the writer to complete
            if let Err(e) = writer_handle.await {
                error!(?e, "JetStream writer task failed");
            }
        })
    }
}

/// Actor that manages multiple window reduction streams. It manages [ActiveStream]s and manages the
/// lifecycle of the reduce tasks.
struct UnalignedReduceActor {
    /// It multiplexes the messages to the receiver to the reduce tasks through the corresponding
    /// tx in [ActiveStream].
    receiver: mpsc::Receiver<UnalignedWindowMessage>,
    /// Client for user-defined reduce operations.
    client_type: UnalignedReduceClientType,
    /// Map of [ActiveStream]s keyed by window ID.
    active_streams: HashMap<Bytes, ActiveStream>,
    /// JetStream writer for writing results of reduce operation.
    js_writer: JetstreamWriter,
    /// Sender for error messages.
    error_tx: mpsc::Sender<e>,
    /// Sender for GC WAL messages. It is optional since users can specify not to use WAL.
    gc_wal_tx: Option<mpsc::Sender<SegmentWriteMessage>>,
    /// WindowManager for assigning windows to messages and closing windows.
    window_manager: UnalignedWindowManager,
    /// Cancellation token to signal tasks to stop
    cln_token: CancellationToken,
}

/// Enum to hold the different types of unaligned reduce clients
#[derive(Clone)]
enum UnalignedReduceClientType {
    Accumulator(UserDefinedAccumulator),
    Session(UserDefinedSessionReduce),
}

impl UnalignedReduceActor {
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
        client_type: UnalignedReduceClientType,
        receiver: mpsc::Receiver<UnalignedWindowMessage>,
        js_writer: JetstreamWriter,
        error_tx: mpsc::Sender<e>,
        gc_wal_tx: Option<mpsc::Sender<SegmentWriteMessage>>,
        window_manager: UnalignedWindowManager,
        cln_token: CancellationToken,
    ) -> Self {
        Self {
            client_type,
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
            if let Err(e) = self.handle_window_message(msg).await {
                error!(?e, "Error handling window message");
            }
        }
        self.wait_for_all_tasks().await;
    }

    /// Handle a window message
    async fn handle_window_message(&mut self, msg: UnalignedWindowMessage) -> crate::Result<()> {
        match &msg.operation {
            WindowOperation::Open(_) => {
                // For Open operations, we need to create a new reduce task
                let window = &msg.windows[0];
                self.window_open(window.clone(), msg).await?;
            }
            WindowOperation::Close => {
                // For Close operations, we need to close the reduce task
                let window = &msg.windows[0];
                self.window_close(window.id.clone()).await;
            }
            WindowOperation::Append(_) => {
                // For Append operations, we need to send the message to the existing reduce task
                let window = &msg.windows[0];
                self.window_append(window.id.clone(), msg).await?;
            }
            WindowOperation::Merge(_) => {
                // For Merge operations, we need to handle merging windows
                self.window_merge(msg).await?;
            }
            WindowOperation::Expand(_) => {
                // For Expand operations, we need to handle expanding windows
                let window = &msg.windows[0];
                self.window_expand(window.id.clone(), msg).await?;
            }
        }
        Ok(())
    }

    /// Creates a new reduce task for the window and sends the initial Open command
    async fn window_open(
        &mut self,
        window: Window,
        msg: UnalignedWindowMessage,
    ) -> crate::Result<()> {
        // Create a new channel for this window's messages
        let (message_tx, message_rx) = mpsc::channel(100);
        let message_stream = ReceiverStream::new(message_rx);

        // Create a ReduceTask based on the client type
        let reduce_task = match &self.client_type {
            UnalignedReduceClientType::Accumulator(client) => ReduceTask::new_accumulator(
                client.clone(),
                self.js_writer.clone(),
                self.gc_wal_tx.clone(),
                self.error_tx.clone(),
                window.clone(),
                self.window_manager.clone(),
            ),
            UnalignedReduceClientType::Session(client) => ReduceTask::new_session(
                client.clone(),
                self.js_writer.clone(),
                self.gc_wal_tx.clone(),
                self.error_tx.clone(),
                window.clone(),
                self.window_manager.clone(),
            ),
        };

        // Start the reduce task and store the handle and the sender
        let task_handle = reduce_task
            .start(message_stream, self.cln_token.clone())
            .await;

        self.active_streams.insert(
            window.id.clone(),
            ActiveStream {
                message_tx: message_tx.clone(),
                task_handle,
            },
        );

        // Send the open command with the first message
        message_tx
            .send(msg)
            .await
            .map_err(|_| Error::Reduce("Failed to send open message to reduce task".to_string()))?;

        Ok(())
    }

    /// Sends the message to the reduce task for the window
    async fn window_append(
        &mut self,
        window_id: Bytes,
        msg: UnalignedWindowMessage,
    ) -> crate::Result<()> {
        // Get the existing stream or create a new one if not found
        if let Some(active_stream) = self.active_streams.get(&window_id) {
            // Send the append message
            active_stream.message_tx.send(msg).await.map_err(|_| {
                Error::Reduce("Failed to send append message to reduce task".to_string())
            })?;
        } else {
            // This can happen if the window was closed or if we're receiving messages out of order
            error!(
                ?window_id,
                "No active stream found for window during append"
            );
            // We could potentially create a new window here, but for now we'll just log an error
        }

        Ok(())
    }

    /// Closes the reduce task for the window
    async fn window_close(&mut self, window_id: Bytes) {
        // Get the existing stream or log error if not found
        let Some(active_stream) = self.active_streams.remove(&window_id) else {
            error!(?window_id, "No active stream found for window during close");
            return;
        };

        // Drop the sender to signal completion
        drop(active_stream.message_tx);

        // Wait for the task to complete
        if let Err(e) = active_stream.task_handle.await {
            error!(?window_id, err = ?e, "Reduce task for window failed");
        }
    }

    /// Handles merging windows (specific to session windows)
    async fn window_merge(&mut self, msg: UnalignedWindowMessage) -> crate::Result<()> {
        // For merge operations, we need to:
        // 1. Create a new window for the merged result
        // 2. Close the windows being merged
        // 3. Send the merge message to the new window

        // The first window in the list is the target window (result of merge)
        let target_window = &msg.windows[0];

        // Create a new window for the merged result if it doesn't exist
        if !self.active_streams.contains_key(&target_window.id) {
            self.window_open(target_window.clone(), msg.clone()).await?;
        } else {
            // If the target window already exists, just send the merge message
            let active_stream = self.active_streams.get(&target_window.id).unwrap();
            active_stream
                .message_tx
                .send(msg.clone())
                .await
                .map_err(|_| {
                    Error::Reduce("Failed to send merge message to reduce task".to_string())
                })?;
        }

        // Close the windows being merged (all except the first one)
        for window in msg.windows.iter().skip(1) {
            self.window_close(window.id.clone()).await;
        }

        Ok(())
    }

    /// Handles expanding windows (specific to session windows)
    async fn window_expand(
        &mut self,
        window_id: Bytes,
        msg: UnalignedWindowMessage,
    ) -> crate::Result<()> {
        // For expand operations, we just need to send the message to the existing window
        if let Some(active_stream) = self.active_streams.get(&window_id) {
            active_stream.message_tx.send(msg).await.map_err(|_| {
                Error::Reduce("Failed to send expand message to reduce task".to_string())
            })?;
        } else {
            error!(
                ?window_id,
                "No active stream found for window during expand"
            );
        }

        Ok(())
    }
}

/// Processes messages and forwards results to the next stage.
pub(crate) struct UnalignedReducer {
    /// Client type for user-defined reduce operations
    client_type: UnalignedReduceClientType,
    /// Window manager for assigning windows to messages and closing windows.
    window_manager: UnalignedWindowManager,
    /// Writer for writing results to JetStream
    js_writer: JetstreamWriter,
    /// Final state of the component (any error will set this as Err).
    final_result: crate::Result<()>,
    /// Set to true when shutting down due to an error.
    shutting_down_on_err: bool,
    /// WAL for writing GC events
    gc_wal: Option<AppendOnlyWal>,
}

impl UnalignedReducer {
    pub(crate) async fn new_accumulator(
        client: UserDefinedAccumulator,
        window_manager: UnalignedWindowManager,
        js_writer: JetstreamWriter,
        gc_wal: Option<AppendOnlyWal>,
    ) -> Self {
        Self {
            client_type: UnalignedReduceClientType::Accumulator(client),
            window_manager,
            js_writer,
            final_result: Ok(()),
            shutting_down_on_err: false,
            gc_wal,
        }
    }

    pub(crate) async fn new_session(
        client: UserDefinedSessionReduce,
        window_manager: UnalignedWindowManager,
        js_writer: JetstreamWriter,
        gc_wal: Option<AppendOnlyWal>,
    ) -> Self {
        Self {
            client_type: UnalignedReduceClientType::Session(client),
            window_manager,
            js_writer,
            final_result: Ok(()),
            shutting_down_on_err: false,
            gc_wal,
        }
    }

    /// Starts the reduce component and returns a handle to the main task.
    pub(crate) async fn start(
        mut self,
        mut input_stream: ReceiverStream<Message>,
        cln_token: CancellationToken,
    ) -> crate::Result<JoinHandle<crate::Result<()>>> {
        // Set up error and GC channels
        let (error_tx, mut error_rx) = mpsc::channel(10);
        let gc_wal_handle = self.setup_gc_wal().await?;

        // Create the actor channel and start the actor
        let (actor_tx, actor_rx) = mpsc::channel(100);
        let actor = UnalignedReduceActor::new(
            self.client_type.clone(),
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
            // Main processing loop
            loop {
                tokio::select! {
                    // Check for errors from reduce tasks
                    Some(error) = error_rx.recv() => {
                        self.handle_error(error, &cln_token);
                        if self.shutting_down_on_err {
                            break;
                        }
                    }

                    // Process input messages
                    read_msg = input_stream.next() => {
                        let Some(msg) = read_msg else {
                            // End of stream
                            break;
                        };
                        // Process the message
                        if let Err(e) = self.process_message(msg, &actor_tx, &cln_token).await {
                            self.handle_error(e, &cln_token);
                            if self.shutting_down_on_err {
                                break;
                            }
                        }
                    }
                }
            }
            // Wait for the actor to finish
            actor_handle.await.expect("failed");
            // Return the final result
            self.final_result
        });

        Ok(handle)
    }

    /// Process a single message
    async fn process_message(
        &mut self,
        msg: Message,
        actor_tx: &mpsc::Sender<UnalignedWindowMessage>,
        cln_token: &CancellationToken,
    ) -> crate::Result<()> {
        // Convert the message to UnalignedWindowMessage
        let window_messages = self.window_manager.assign_windows(&msg).await?;
        for window_msg in window_messages {
            // Send the message to the actor
            actor_tx
                .send(window_msg)
                .await
                .map_err(|_| Error::Reduce("Failed to send message to reduce actor".to_string()))?;
        }
        Ok(())
    }

    /// Set up the GC WAL if needed
    async fn setup_gc_wal(&mut self) -> crate::Result<Option<mpsc::Sender<SegmentWriteMessage>>> {
        if let Some(wal) = &self.gc_wal {
            let (gc_tx, gc_rx) = mpsc::channel(100);
            let gc_handle = wal.start_gc_writer(gc_rx, cln_token.clone()).await?;
            Ok(Some(gc_tx))
        } else {
            Ok(None)
        }
    }

    /// Handle an error from the reduce tasks
    fn handle_error(&mut self, error: Error, cln_token: &CancellationToken) {
        error!(?error, "Error in reduce component");
        self.final_result = Err(error);
        self.shutting_down_on_err = true;
        cln_token.cancel();
    }
}
