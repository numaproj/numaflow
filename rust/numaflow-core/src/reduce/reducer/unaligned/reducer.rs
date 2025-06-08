use crate::error::Error;
use crate::message::Message;
use crate::pipeline::isb::jetstream::writer::JetstreamWriter;
use crate::reduce::reducer::unaligned::user_defined::UserDefinedUnalignedReduce;
use crate::reduce::reducer::unaligned::user_defined::accumulator::UserDefinedAccumulator;
use crate::reduce::reducer::unaligned::user_defined::session::UserDefinedSessionReduce;
use crate::reduce::reducer::unaligned::windower::{
    UnalignedWindowManager, UnalignedWindowMessage, Window,
};
use crate::reduce::wal::segment::append::{AppendOnlyWal, SegmentWriteMessage};

use chrono::{DateTime, Utc};
use numaflow_pb::clients::accumulator::AccumulatorRequest;
use numaflow_pb::clients::sessionreduce::SessionReduceRequest;
use numaflow_pb::objects::wal::GcEvent;
use prost::Message as ProstMessage;
use std::collections::HashMap;
use std::ops::Sub;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_stream::StreamExt;
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info};

/// Represents an active reduce stream for a pnf slot.
struct ActiveStream {
    /// Sender for window messages. Messages are sent to this channel is received by the unique reduce
    /// task for that pnf slot.
    message_tx: mpsc::Sender<UnalignedWindowMessage>,
    /// Handle to the task processing the pnf slot.
    task_handle: JoinHandle<crate::Result<()>>,
}

/// Represents a reduce task for a pnf slot. It is responsible for calling the user-defined reduce
/// function for the given slot and writing the output to JetStream and publishing the watermark.
/// Also writes the GC events to the WAL if configured.
struct ReduceTask {
    /// Client for user-defined reduce operations.
    client_type: UserDefinedUnalignedReduce,
    /// JetStream writer for writing results of reduce operation.
    js_writer: JetstreamWriter,
    /// Sender for GC WAL messages. It is optional since users can specify not to use WAL.
    gc_wal_tx: Option<mpsc::Sender<SegmentWriteMessage>>,
    /// Sender for error messages.
    error_tx: mpsc::Sender<Error>,
    /// Window manager for assigning windows to messages and closing windows.
    window_manager: UnalignedWindowManager,
    /// Maximum time to wait before writing a batch
    batch_timeout: Duration,
    /// Map to track windows for each key combination
    /// For session: stores the actual window for that keys
    /// For accumulator: stores a window with max end time (same start and end time)
    tracked_windows: HashMap<Vec<String>, Window>,
}

impl ReduceTask {
    /// Creates a new ReduceTask with the given configuration for Accumulator
    fn new(
        client: UserDefinedUnalignedReduce,
        js_writer: JetstreamWriter,
        gc_wal_tx: Option<mpsc::Sender<SegmentWriteMessage>>,
        error_tx: mpsc::Sender<Error>,
        window_manager: UnalignedWindowManager,
        batch_timeout: Duration,
    ) -> Self {
        Self {
            client_type: client,
            js_writer,
            gc_wal_tx,
            error_tx,
            window_manager,
            batch_timeout,
            tracked_windows: HashMap::new(),
        }
    }

    /// accumulator reduce
    async fn accumulator_reduce(
        &mut self,
        client: UserDefinedAccumulator,
        mut message_stream: ReceiverStream<UnalignedWindowMessage>,
        cln_token: CancellationToken,
    ) -> crate::Result<()> {
        let (request_tx, request_rx) = mpsc::channel(500);
        let request_stream = ReceiverStream::new(request_rx);

        let (mut writer_tx, writer_rx) = mpsc::channel(500);
        let writer_stream = ReceiverStream::new(writer_rx);
        let mut writer_handle = self
            .js_writer
            .clone()
            .streaming_write(writer_stream, cln_token.clone())
            .await?;

        // Spawn a task to convert UnalignedWindowMessages to ReduceRequests and send them to req_tx
        let _request_handle = tokio::spawn(async move {
            while let Some(window_msg) = message_stream.next().await {
                let reduce_req: AccumulatorRequest = window_msg.into();
                if request_tx.send(reduce_req).await.is_err() {
                    break;
                }
            }
        });

        let mut client_clone = client.clone();
        let (mut response_stream, handle) = client_clone
            .reduce_fn(request_stream, cln_token.clone())
            .await?;

        // we periodically wait for the js writer to finish writing so that we can delete the tracked
        // windows and publish the watermark.
        let mut batch_timer = tokio::time::interval(self.batch_timeout);
        batch_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                _ = batch_timer.tick() => {

                    drop(writer_tx);
                    // wait for the writer to finish writing the current batch
                    if let Err(e) = writer_handle.await {
                        error!(?e, "Error while writing results to JetStream");
                        return Err(Error::Reduce(format!("Writer task failed: {}", e)));
                    }

                    // Create a new channel for the next batch
                    let (new_writer_tx, new_writer_rx) = mpsc::channel(100);
                    let writer_stream = ReceiverStream::new(new_writer_rx);
                    writer_tx = new_writer_tx;

                    // Start a new writer
                    writer_handle = self
                        .js_writer
                        .clone()
                        .streaming_write(writer_stream, cln_token.clone())
                        .await?;

                    // Write GC events for tracked windows
                    self.write_gc_events().await;

                    // Delete tracked windows after writing GC events
                    self.delete_tracked_windows().await;
                }
                response = response_stream.next() => {
                    let Some(response) = response else {
                        break;
                    };

                    // Process the response
                    if response.eof {
                        break;
                    }

                    let window = response.window.clone().expect("Window not set in response");
                    let window : Window = window.into();
                    writer_tx
                        .send(response.into())
                        .await
                        .expect("Failed to send response to writer");
                    self.tracked_windows.insert(window.keys.to_vec(), window);
                }
            }
        }

        drop(writer_tx);
        // Final cleanup: wait for writer to complete
        if let Err(e) = writer_handle.await {
            error!(?e, "Error while writing final results to JetStream");
            return Err(Error::Reduce(format!("Writer task failed: {}", e)));
        }

        // Write final GC events
        self.write_gc_events().await;

        // Delete tracked windows
        self.delete_tracked_windows().await;

        match handle.await.expect("Reduce task failed") {
            Err(Error::Cancelled()) => {
                info!("Accumulator task cancelled");
                Ok(())
            }
            Err(e) => {
                error!(?e, "Error while doing reduce operation");
                Err(e)
            }
            Ok(_) => Ok(()),
        }
    }

    /// session reduce
    async fn session_reduce(
        &mut self,
        client: UserDefinedSessionReduce,
        mut message_stream: ReceiverStream<UnalignedWindowMessage>,
        cln_token: CancellationToken,
    ) -> crate::Result<()> {
        let (request_tx, request_rx) = mpsc::channel(100);
        let request_stream = ReceiverStream::new(request_rx);

        let (mut writer_tx, writer_rx) = mpsc::channel(100);
        let writer_stream = ReceiverStream::new(writer_rx);
        let mut writer_handle = self
            .js_writer
            .clone()
            .streaming_write(writer_stream, cln_token.clone())
            .await?;

        let _request_handle = tokio::spawn(async move {
            while let Some(window_msg) = message_stream.next().await {
                let reduce_req: SessionReduceRequest = window_msg.into();
                if request_tx.send(reduce_req).await.is_err() {
                    break;
                }
            }
        });

        let mut client_clone = client.clone();
        let (mut response_stream, handle) = client_clone
            .reduce_fn(request_stream, cln_token.clone())
            .await?;

        // Set up batch timer for periodic flushing
        let mut batch_timer = tokio::time::interval(self.batch_timeout);
        batch_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                _ = batch_timer.tick() => {

                    drop(writer_tx);

                    // Wait for the writer to finish writing the current batch and start a new one
                    if let Err(e) = writer_handle.await {
                        error!(?e, "Error while writing results to JetStream");
                        return Err(Error::Reduce(format!("Writer task failed: {}", e)));
                    }

                    let (new_writer_tx, new_writer_rx) = mpsc::channel(100);
                    let writer_stream = ReceiverStream::new(new_writer_rx);
                    writer_tx = new_writer_tx;

                    writer_handle = self
                        .js_writer
                        .clone()
                        .streaming_write(writer_stream, cln_token.clone())
                        .await?;

                    // Write GC events for tracked windows
                    self.write_gc_events().await;

                    // Delete tracked windows after writing GC events
                    self.delete_tracked_windows().await;
                }
                response = response_stream.next() => {
                    let Some(response) = response else {
                        break;
                    };

                    if response.eof {
                        // add to the tracked windows, so that we can gc them after the timeout
                        let session_window: Window = response.keyed_window.expect("Window not set in response").into();
                        self.tracked_windows.insert(session_window.keys.to_vec(), session_window);
                        continue;
                    }

                    writer_tx
                        .send(response.into())
                        .await
                        .expect("Failed to send response to writer");
                }
            }
        }

        drop(writer_tx);
        // Final cleanup: wait for writer to complete
        if let Err(e) = writer_handle.await {
            error!(?e, "Error while writing final results to JetStream");
            return Err(Error::Reduce(format!("Writer task failed: {}", e)));
        }

        // Write final GC events
        self.write_gc_events().await;

        // Delete tracked windows
        self.delete_tracked_windows().await;

        match handle.await.expect("Reduce task failed") {
            Err(Error::Cancelled()) => {
                info!("Session reduce task cancelled");
                Ok(())
            }
            Err(e) => {
                error!(?e, "Error while doing reduce operation");
                Err(e)
            }
            Ok(_) => Ok(()),
        }
    }

    /// starts a task to process the window stream and returns the task handle
    async fn start(
        mut self,
        message_stream: ReceiverStream<UnalignedWindowMessage>,
        cln_token: CancellationToken,
    ) -> JoinHandle<crate::Result<()>> {
        tokio::spawn(async move {
            // Call the appropriate reduce_fn based on the client type
            let result = match &self.client_type {
                UserDefinedUnalignedReduce::Accumulator(client) => {
                    self.accumulator_reduce(client.clone(), message_stream, cln_token.clone())
                        .await
                }
                UserDefinedUnalignedReduce::Session(client) => {
                    self.session_reduce(client.clone(), message_stream, cln_token.clone())
                        .await
                }
            };

            if let Err(e) = result {
                self.error_tx.send(e).await.expect("Failed to send error");
            }
            Ok(())
        })
    }

    /// write the tracked windows to the gc wal, so that the messages gets compacted.
    async fn write_gc_events(&self) {
        if let Some(gc_wal_tx) = &self.gc_wal_tx {
            for (keys, window) in &self.tracked_windows {
                let gc_event: GcEvent = window.into();
                let gc_event_bytes = gc_event.encode_to_vec();
                if let Err(e) = gc_wal_tx
                    .send(SegmentWriteMessage::WriteData {
                        offset: None,
                        data: gc_event_bytes.into(),
                    })
                    .await
                {
                    error!(?e, ?keys, "Failed to send GC event to WAL");
                }
            }
        }
    }

    /// delete all tracked windows after writing GC events
    async fn delete_tracked_windows(&mut self) {
        for (_keys, window) in self.tracked_windows.drain() {
            self.window_manager.delete_window(window);
        }
    }
}

/// Actor that manages multiple window reduction streams. It manages [ActiveStream]s and manages the
/// lifecycle of the reduce tasks.
struct UnalignedReduceActor {
    /// It multiplexes the messages to the receiver to the reduce tasks through the corresponding
    /// tx in [ActiveStream].
    receiver: mpsc::Receiver<UnalignedWindowMessage>,
    /// Client for user-defined reduce operations.
    client_type: UserDefinedUnalignedReduce,
    /// Map of [ActiveStream]s keyed by window ID.
    active_streams: HashMap<&'static str, ActiveStream>,
    /// JetStream writer for writing results of reduce operation.
    js_writer: JetstreamWriter,
    /// Sender for error messages.
    error_tx: mpsc::Sender<Error>,
    /// Sender for GC WAL messages. It is optional since users can specify not to use WAL.
    gc_wal_tx: Option<mpsc::Sender<SegmentWriteMessage>>,
    /// WindowManager for assigning windows to messages and closing windows.
    window_manager: UnalignedWindowManager,
    /// Cancellation token to signal tasks to stop
    cln_token: CancellationToken,
}

impl UnalignedReduceActor {
    /// Waits for all active tasks to complete
    async fn wait_for_all_tasks(&mut self) {
        info!(
            "Waiting for {} active reduce tasks to complete",
            self.active_streams.len()
        );

        for (window_id, active_stream) in self.active_streams.drain() {
            // Wait for the task to complete
            if let Err(e) = active_stream.task_handle.await.expect("task failed") {
                error!(?window_id, err = ?e, "Reduce task for window failed during shutdown");
            }
            info!(?window_id, "Reduce task for window completed");
        }

        info!("All reduce tasks completed");
    }

    pub(crate) async fn new(
        client_type: UserDefinedUnalignedReduce,
        receiver: mpsc::Receiver<UnalignedWindowMessage>,
        js_writer: JetstreamWriter,
        error_tx: mpsc::Sender<Error>,
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
        // if the active stream already exist, we can write to it
        if let Some(active_stream) = self.active_streams.get(&msg.pnf_slot) {
            let _ =
                active_stream.message_tx.send(msg).await.inspect_err(|e| {
                    error!(?e, "Failed to send message reduce task, task aborted")
                });
        } else {
            // Slot not present, create new active stream
            self.create_active_stream(msg).await?;
        }
        Ok(())
    }

    /// Creates a new active stream for the pnf_slot and sends the message
    async fn create_active_stream(&mut self, msg: UnalignedWindowMessage) -> crate::Result<()> {
        let pnf_slot = msg.pnf_slot;

        // Create a new channel for this window's messages
        let (message_tx, message_rx) = mpsc::channel(100);
        let message_stream = ReceiverStream::new(message_rx);

        // Create a ReduceTask based on the client type
        let reduce_task = ReduceTask::new(
            self.client_type.clone(),
            self.js_writer.clone(),
            self.gc_wal_tx.clone(),
            self.error_tx.clone(),
            self.window_manager.clone(),
            Duration::from_secs(1), // Default batch timeout
        );

        // Start the reduce task and store the handle and the sender
        let task_handle = reduce_task
            .start(message_stream, self.cln_token.clone())
            .await;

        self.active_streams.insert(
            pnf_slot,
            ActiveStream {
                message_tx: message_tx.clone(),
                task_handle,
            },
        );

        // Send the message to the newly created stream
        message_tx
            .send(msg)
            .await
            .map_err(|_| Error::Reduce("Failed to send message to new reduce task".to_string()))?;

        Ok(())
    }
}

/// Processes messages and forwards results to the next stage.
pub(crate) struct UnalignedReducer {
    /// Client type for user-defined reduce operations
    client: UserDefinedUnalignedReduce,
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
    /// Allowed lateness for the messages to be accepted and delay the close of book.
    allowed_lateness: Duration,
    /// current watermark for the reduce vertex.
    current_watermark: DateTime<Utc>,
}

impl UnalignedReducer {
    pub(crate) async fn new(
        client: UserDefinedUnalignedReduce,
        window_manager: UnalignedWindowManager,
        js_writer: JetstreamWriter,
        allowed_lateness: Duration,
        gc_wal: Option<AppendOnlyWal>,
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
        mut input_stream: ReceiverStream<Message>,
        cln_token: CancellationToken,
    ) -> crate::Result<JoinHandle<crate::Result<()>>> {
        // Set up error and GC channels
        let (error_tx, mut error_rx) = mpsc::channel(10);
        let gc_wal_handle = self.setup_gc_wal().await?;

        // Create the actor channel and start the actor
        let (actor_tx, actor_rx) = mpsc::channel(100);
        let actor = UnalignedReduceActor::new(
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

        let handle = tokio::spawn(async move {
            loop {
                tokio::select! {
                    // listen for errors from any running tasks
                    Some(error) = error_rx.recv() => {
                        self.handle_error(error, &cln_token);
                    }

                    read_msg = input_stream.next() => {
                        if self.shutting_down_on_err {
                            info!("Unaligned reducer is in shutdown mode, ignoring the message");
                            continue;
                        }

                        let Some(msg) = read_msg else {
                            // end of stream we can exit
                            break;
                        };

                         // update the watermark.
                        // we cannot simply assign incoming message's watermark as the current watermark,
                        // because it can be -1. watermark will never regress, so use max.
                        self.current_watermark = self.current_watermark.max(msg.watermark.unwrap_or_default());

                        // check if any windows can be closed based on the current watermark
                        let close_window_messages = self.window_manager.close_windows(self.current_watermark);
                        for window_msg in close_window_messages {
                            actor_tx
                                .send(window_msg)
                                .await
                                .expect("failed to send message to actor");
                        }

                        // only drop the message if it is late and the event time is before the watermark - allowed lateness
                        if msg.is_late && msg.event_time < self.current_watermark.sub(self.allowed_lateness) {
                            debug!(event_time = ?msg.event_time.timestamp_millis(), watermark = ?self.current_watermark.timestamp_millis(), "Late message detected, dropping");
                            // TODO(ajain): add a metric for this
                            continue;
                        }

                        if self.current_watermark > msg.event_time {
                            error!(current_watermark=?self.current_watermark, message_event_time=?msg.event_time, "Old message popped up, Watermark is behind the event time");
                            continue;
                        }

                        // Convert the message to UnalignedWindowMessage
                        let window_messages = self.window_manager.assign_windows(msg);
                        for window_msg in window_messages {
                            // Send the message to the actor
                            actor_tx
                                .send(window_msg)
                                .await
                                .expect("failed to send message to actor");
                        }
                    }
                }
            }

            // Drop the sender to signal the actor to stop
            drop(actor_tx);
            info!(
                "Unaligned Reduce component is shutting down, waiting for active reduce tasks to complete"
            );

            // Wait for the actor to complete
            if let Err(e) = actor_handle.await {
                error!("Error waiting for actor to complete: {:?}", e);
            }

            info!(status=?self.final_result, "UnalignedReduce component successfully completed");
            self.final_result
        });

        Ok(handle)
    }

    /// set up the gc wal if configured
    async fn setup_gc_wal(&mut self) -> crate::Result<Option<mpsc::Sender<SegmentWriteMessage>>> {
        if let Some(gc_wal) = self.gc_wal.take() {
            let (gc_tx, gc_rx) = mpsc::channel(100);
            gc_wal.streaming_write(ReceiverStream::new(gc_rx)).await?;
            Ok(Some(gc_tx))
        } else {
            Ok(None)
        }
    }

    /// handle errors from the reduce tasks by cancelling token so that upstream knows there is an
    /// issue and stops sending new messages.
    fn handle_error(&mut self, error: Error, cln_token: &CancellationToken) {
        error!(?error, "Error in reduce component");
        self.final_result = Err(error);
        self.shutting_down_on_err = true;
        cln_token.cancel();
    }
}
