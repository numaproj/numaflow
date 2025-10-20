use crate::error::Error;
use crate::message::{Message, MessageType};
use crate::pipeline::isb::writer::ISBWriter;
use crate::reduce::reducer::unaligned::user_defined::UserDefinedUnalignedReduce;
use crate::reduce::reducer::unaligned::user_defined::accumulator::UserDefinedAccumulator;
use crate::reduce::reducer::unaligned::user_defined::session::UserDefinedSessionReduce;
use crate::reduce::reducer::unaligned::windower::{
    UnalignedWindowManager, UnalignedWindowMessage, Window,
};
use crate::reduce::wal::segment::append::{AppendOnlyWal, SegmentWriteMessage};

use crate::jh_abort_guard;
use chrono::{DateTime, Utc};
use numaflow_pb::clients::accumulator::AccumulatorRequest;
use numaflow_pb::clients::sessionreduce::SessionReduceRequest;
use numaflow_pb::objects::wal::GcEvent;
use prost::Message as ProstMessage;
use std::collections::HashMap;
use std::ops::Sub;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_stream::StreamExt;
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info};

const DEFAULT_KEY_FOR_NON_KEYED_STREAM: &str = "NON_KEYED_STREAM";

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
    /// ISB writer for writing results of reduce operation.
    isb_writer: ISBWriter,
    /// Sender for GC WAL messages. It is optional since users can specify not to use WAL.
    gc_wal_tx: Option<mpsc::Sender<SegmentWriteMessage>>,
    /// Sender for error messages.
    error_tx: mpsc::Sender<Error>,
    /// Window manager for assigning windows to messages and closing windows.
    window_manager: UnalignedWindowManager,
    /// Maximum time to wait before writing a batch
    batch_timeout: Duration,
    /// Map to track windows for each key combination. It is an optimization to avoid writing GC
    /// events for every message. The tracked windows are deleted after writing GC events which happens
    /// after each batch write to the ISB is completed.
    /// For session: stores the window that got closed for that keys.
    /// For accumulator: stores a window with max end time (same start and end time)
    tracked_windows: HashMap<Vec<String>, Window>,
}

impl ReduceTask {
    /// Creates a new ReduceTask with the given configuration for Accumulator
    fn new(
        client: UserDefinedUnalignedReduce,
        isb_writer: ISBWriter,
        gc_wal_tx: Option<mpsc::Sender<SegmentWriteMessage>>,
        error_tx: mpsc::Sender<Error>,
        window_manager: UnalignedWindowManager,
        batch_timeout: Duration,
    ) -> Self {
        Self {
            client_type: client,
            isb_writer,
            gc_wal_tx,
            error_tx,
            window_manager,
            batch_timeout,
            tracked_windows: HashMap::new(),
        }
    }

    /// accumulator reduce implementation. It internally batches writes to JetStream to checkpoint
    /// that the writes have been persisted. Once persisted it deletes the window and watermark can
    /// progress.
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
            .isb_writer
            .clone()
            .streaming_write(writer_stream, cln_token.clone())
            .await?;

        // Spawn a task to convert UnalignedWindowMessages to ReduceRequests and send them to req_tx
        let _jh_guard = jh_abort_guard!(tokio::spawn(async move {
            while let Some(window_msg) = message_stream.next().await {
                let reduce_req: AccumulatorRequest = window_msg.into();
                if request_tx.send(reduce_req).await.is_err() {
                    break;
                }
            }
        }));

        let mut client_clone = client.clone();
        let (mut response_stream, handle) = client_clone
            .accumulator_fn(request_stream, cln_token.clone())
            .await?;

        // we periodically wait for the js writer to finish writing so that we can delete the tracked
        // windows and publish the watermark.
        let mut batch_timer = tokio::time::interval(self.batch_timeout);
        batch_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                _ = batch_timer.tick() => {
                    // Drop the current writer channel to signal the writer to stop so we can wait
                    drop(writer_tx);
                    // wait for the writer to finish writing the current batch
                    if let Err(e) = writer_handle.await {
                        error!(?e, "Error while writing results to JetStream");
                        return Err(Error::Reduce(format!("Writer task failed: {e}")));
                    }

                    // Create a new channel for the next batch
                    let (new_writer_tx, new_writer_rx) = mpsc::channel(500);
                    let writer_stream = ReceiverStream::new(new_writer_rx);
                    writer_tx = new_writer_tx;

                    // Start a new writer
                    writer_handle = self
                        .isb_writer
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
                        continue;
                    }

                    // the response window is monotonic with start-time as 0 and endtime as monotonically
                    // increasing WM. This is because each message is an independent entity since
                    // accumulator acts like a Global Window.
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

        // final drop the writer channel to signal the writer to stop so we can wait for it to finish
        drop(writer_tx);
        // wait for writer to complete
        if let Err(e) = writer_handle.await {
            error!(?e, "Error while writing final results to JetStream");
            return Err(Error::Reduce(format!("Writer task failed: {e}")));
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

    /// session reduce  implementation. It internally batches writes to JetStream to checkpoint
    /// that the writes have been persisted. Once persisted it deletes the window and watermark can
    /// progress. In session window we add to the tracked only when the reduce function returns EOF.
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
            .isb_writer
            .clone()
            .streaming_write(writer_stream, cln_token.clone())
            .await?;

        let _jh_guard = jh_abort_guard!(tokio::spawn(async move {
            while let Some(window_msg) = message_stream.next().await {
                let reduce_req: SessionReduceRequest = window_msg.into();
                if request_tx.send(reduce_req).await.is_err() {
                    break;
                }
            }
        }));

        let mut client_clone = client.clone();
        let (mut response_stream, handle) = client_clone
            .session_reduce_fn(request_stream, cln_token.clone())
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
                        return Err(Error::Reduce(format!("Writer task failed: {e}")));
                    }

                    let (new_writer_tx, new_writer_rx) = mpsc::channel(100);
                    let writer_stream = ReceiverStream::new(new_writer_rx);
                    writer_tx = new_writer_tx;

                    writer_handle = self
                        .isb_writer
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

                    if response.response.eof {
                        // add to the tracked windows, so that we can gc them after the timeout
                        let session_window: Window = response.response.keyed_window.expect("Window not set in response").into();
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
            return Err(Error::Reduce(format!("Writer task failed: {e}")));
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
                    .send(SegmentWriteMessage::WriteGcEvent {
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
    /// ISB writer for writing results of reduce operation.
    isb_writer: ISBWriter,
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
        isb_writer: ISBWriter,
        error_tx: mpsc::Sender<Error>,
        gc_wal_tx: Option<mpsc::Sender<SegmentWriteMessage>>,
        window_manager: UnalignedWindowManager,
        cln_token: CancellationToken,
    ) -> Self {
        Self {
            client_type,
            receiver,
            active_streams: HashMap::new(),
            isb_writer,
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
            self.isb_writer.clone(),
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
    isb_writer: ISBWriter,
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
    /// Whether the reduce is keyed or not.
    keyed: bool,
    /// Graceful shutdown timeout duration.
    graceful_timeout: Duration,
}

impl UnalignedReducer {
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn new(
        client: UserDefinedUnalignedReduce,
        window_manager: UnalignedWindowManager,
        isb_writer: ISBWriter,
        allowed_lateness: Duration,
        gc_wal: Option<AppendOnlyWal>,
        graceful_timeout: Duration,
        keyed: bool,
    ) -> Self {
        Self {
            client,
            window_manager,
            isb_writer,
            final_result: Ok(()),
            shutting_down_on_err: false,
            gc_wal,
            allowed_lateness,
            current_watermark: DateTime::from_timestamp_millis(-1).expect("Invalid timestamp"),
            keyed,
            graceful_timeout,
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

        let parent_cln_token = cln_token.clone();
        // create a new cancellation token for the map component, this token is used for hard
        // shutdown, the parent token is used for graceful shutdown.
        let hard_shutdown_token = CancellationToken::new();
        // the one that calls shutdown
        let hard_shutdown_token_owner = hard_shutdown_token.clone();
        let graceful_timeout = self.graceful_timeout;
        let keyed = self.keyed;

        // spawn a task to cancel the token after graceful timeout when the main token is cancelled
        let shutdown_handle = tokio::spawn(async move {
            // initiate graceful shutdown
            parent_cln_token.cancelled().await;
            // wait for graceful timeout
            tokio::time::sleep(graceful_timeout).await;
            // cancel the token to hard shutdown
            hard_shutdown_token_owner.cancel();
        });

        // Create the actor channel and start the actor
        let (actor_tx, actor_rx) = mpsc::channel(100);
        let actor = UnalignedReduceActor::new(
            self.client.clone(),
            actor_rx,
            self.isb_writer.clone(),
            error_tx.clone(),
            gc_wal_handle,
            self.window_manager.clone(),
            hard_shutdown_token.clone(),
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

                    // Process input messages with timeout
                    read_msg = input_stream.next() => {
                        match read_msg {
                            Some(mut msg) => {
                                // If shutting down, drain the stream
                                if self.shutting_down_on_err {
                                    info!("Unaligned reducer is in shutdown mode, ignoring the message");
                                    continue;
                                }

                                // Handle WMB messages with idle watermarks
                                if let MessageType::WMB = msg.typ {
                                    if let Some(idle_watermark) = msg.watermark {
                                        // Only close windows if the idle watermark is greater than current watermark
                                        if idle_watermark > self.current_watermark {
                                            self.current_watermark = idle_watermark;
                                            self.close_windows_by_wm(idle_watermark, &actor_tx).await;
                                        }
                                    }
                                    continue; // Skip further processing for WMB messages
                                }


                                // If the stream is not keyed, set all messages to have the same key
                                if !keyed {
                                    msg.keys = Arc::new([DEFAULT_KEY_FOR_NON_KEYED_STREAM.to_string()]);
                                }

                                // Update the watermark - use max to ensure it never regresses
                                self.current_watermark = self
                                    .current_watermark
                                    .max(msg.watermark.unwrap_or_default());

                                self.assign_and_close_windows(msg, &actor_tx).await;
                            }
                            None => {
                                // Stream ended
                                break;
                            }
                        }
                    }
                }
            }

            // Drop the sender to signal the actor to stop
            info!(
                "Unaligned Reduce component is shutting down, waiting for active reduce tasks to complete"
            );
            drop(actor_tx);

            // Wait for the actor to complete
            if let Err(e) = actor_handle.await {
                error!("Error waiting for actor to complete: {:?}", e);
            }

            // abort the shutdown handle since we are done processing, no need to wait for the
            // hard shutdown.
            shutdown_handle.abort();

            info!(status=?self.final_result, "Unaligned Reduce component successfully completed");
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
        if self.final_result.is_ok() {
            error!(?error, "Error received while performing reduce operation");
            cln_token.cancel();
            self.final_result = Err(error);
            self.shutting_down_on_err = true;
        }
    }

    /// Closes windows based on the provided watermark and sends close messages to the actor.
    async fn close_windows_by_wm(
        &self,
        watermark: DateTime<Utc>,
        actor_tx: &mpsc::Sender<UnalignedWindowMessage>,
    ) {
        let window_messages = self
            .window_manager
            .close_windows(watermark.sub(self.allowed_lateness));

        for window_msg in window_messages {
            actor_tx
                .send(window_msg)
                .await
                .expect("Failed to send close window message");
        }
    }

    /// Assigns windows to a message and sends the messages to the actor, also closes any windows
    /// that can be closed based on the current watermark.
    async fn assign_and_close_windows(
        &mut self,
        msg: Message,
        actor_tx: &mpsc::Sender<UnalignedWindowMessage>,
    ) {
        // Drop late messages
        if msg.is_late && msg.event_time < self.current_watermark.sub(self.allowed_lateness) {
            debug!(event_time = ?msg.event_time.timestamp_millis(), watermark = ?self.current_watermark.timestamp_millis(), "Late message detected, dropping");
            // TODO(ajain): add a metric for this
            return;
        }

        // Validate message event time against current watermark
        if self.current_watermark > msg.event_time {
            error!(
                current_watermark = ?self.current_watermark.timestamp_millis(),
                message_event_time = ?msg.event_time.timestamp_millis(),
                is_late = ?msg.is_late,
                offset = ?msg.offset,
                "Old message popped up, Watermark is behind the event time"
            );
            return;
        }

        // Close windows based on current watermark
        self.close_windows_by_wm(self.current_watermark, actor_tx)
            .await;

        // Assign windows to the message
        let window_messages = self.window_manager.assign_windows(msg);

        // Send each window message to the actor for processing
        for window_msg in window_messages {
            actor_tx
                .send(window_msg)
                .await
                .expect("Failed to send window message");
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::sync::Arc;
    use std::time::Duration;

    use super::*;
    use crate::config::pipeline::isb::{BufferWriterConfig, Stream};
    use crate::config::pipeline::{ToVertexConfig, VertexType};
    use crate::message::{Message, MessageID, Offset, StringOffset};
    use crate::pipeline::isb::writer::{ISBWriter, ISBWriterComponents};
    use crate::reduce::reducer::unaligned::user_defined::accumulator::UserDefinedAccumulator;
    use crate::reduce::reducer::unaligned::user_defined::session::UserDefinedSessionReduce;
    use crate::reduce::reducer::unaligned::windower::session::SessionWindowManager;
    use crate::shared::grpc::create_rpc_channel;
    use async_nats::jetstream::consumer::PullConsumer;
    use async_nats::jetstream::{self, consumer, stream};
    use chrono::{TimeZone, Utc};
    use numaflow::shared::ServerExtras;
    use numaflow::{accumulator, session_reduce};
    use numaflow_pb::clients::sessionreduce::session_reduce_client::SessionReduceClient;
    use prost::Message as ProstMessage;
    use tempfile::TempDir;
    use tokio::sync::mpsc;
    use tokio_stream::wrappers::ReceiverStream;
    use tokio_util::sync::CancellationToken;

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

    /// Simple accumulator that fires a message every 3 messages with updated key
    struct AccumulatorCounter {
        count: Arc<std::sync::atomic::AtomicU32>,
    }

    struct AccumulatorCounterCreator {}

    impl accumulator::AccumulatorCreator for AccumulatorCounterCreator {
        type A = AccumulatorCounter;

        fn create(&self) -> Self::A {
            AccumulatorCounter::new()
        }
    }

    impl AccumulatorCounter {
        fn new() -> Self {
            Self {
                count: Arc::new(std::sync::atomic::AtomicU32::new(0)),
            }
        }
    }

    #[tonic::async_trait]
    impl accumulator::Accumulator for AccumulatorCounter {
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

    #[cfg(feature = "nats-tests")]
    #[tokio::test]
    async fn test_unaligned_session_reducer_basic() -> crate::Result<()> {
        // Set up the session reducer server
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let tmp_dir = TempDir::new().unwrap();
        let sock_file = tmp_dir.path().join("session_reduce_basic.sock");
        let server_info_file = tmp_dir.path().join("session_reduce_basic-server-info");

        let server_info = server_info_file.clone();
        let server_socket = sock_file.clone();
        let server_handle = tokio::spawn(async move {
            session_reduce::Server::new(CounterCreator {})
                .with_socket_file(server_socket)
                .with_server_info_file(server_info)
                .start_with_shutdown(shutdown_rx)
                .await
                .expect("server failed");
        });

        // Wait for the server to start
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Create the session client
        let session_client = UserDefinedSessionReduce::new(SessionReduceClient::new(
            create_rpc_channel(sock_file).await?,
        ))
        .await;

        let client = UserDefinedUnalignedReduce::Session(session_client);

        // Create a session window manager with 60s timeout
        let windower = SessionWindowManager::new(Duration::from_secs(60));
        let window_manager = UnalignedWindowManager::Session(windower);

        // Set up JetStream
        let js_url = "localhost:4222";
        let nats_client = async_nats::connect(js_url).await.unwrap();
        let js_context = jetstream::new(nats_client);

        // Create output stream
        let stream = Stream::new("test_unaligned_session_basic", "test", 0);
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
        let writer_config = BufferWriterConfig {
            streams: vec![stream.clone()],
            ..Default::default()
        };

        let mut writers = std::collections::HashMap::new();
        writers.insert(
            stream.name,
            crate::pipeline::isb::jetstream::js_writer::JetStreamWriter::new(
                stream.clone(),
                js_context.clone(),
                writer_config.clone(),
                None,
                cln_token.clone(),
            )
            .await?,
        );

        let writer_components = ISBWriterComponents {
            config: vec![ToVertexConfig {
                name: "test-vertex",
                partitions: 1,
                writer_config,
                conditions: None,
                to_vertex_type: VertexType::Sink,
            }],
            writers,
            paf_concurrency: 100,
            watermark_handle: None,
            vertex_type: VertexType::ReduceUDF,
        };
        let isb_writer = ISBWriter::new(writer_components);

        // Create the UnalignedReducer
        let reducer = UnalignedReducer::new(
            client,
            window_manager,
            isb_writer,
            Duration::from_secs(0), // No allowed lateness for testing
            None,                   // No GC WAL for testing
            Duration::from_millis(50),
            true, // No watermark handle for testing
        )
        .await;

        // Create a channel for input messages
        let (input_tx, input_rx) = mpsc::channel(10);
        let input_stream = ReceiverStream::new(input_rx);

        // Start the reducer
        let reducer_handle = reducer.start(input_stream, cln_token.clone()).await?;

        // Create test messages
        let base_time = Utc.with_ymd_and_hms(2023, 1, 1, 0, 0, 0).unwrap();

        // Message 1: Within the session window
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

        // Message 2: Within the same session window
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

        // Message 3: With watermark past session timeout to trigger window close
        let msg3 = Message {
            typ: Default::default(),
            keys: Arc::from(vec!["key1".into()]),
            tags: None,
            value: "value3".into(),
            offset: Offset::String(StringOffset::new("2".to_string(), 2)),
            event_time: base_time + chrono::Duration::seconds(120),
            watermark: Some(base_time + chrono::Duration::seconds(100)), // Past session timeout
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
        input_tx.send(msg3).await.unwrap();

        // Create a consumer to read the results
        let consumer: PullConsumer = js_context
            .get_consumer_from_stream(&stream.name, &stream.name)
            .await
            .unwrap();

        // Read messages from the stream
        let mut messages = consumer
            .fetch()
            .expires(Duration::from_secs(2))
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
            assert_eq!(message.body.unwrap().payload.to_vec(), b"2".to_vec()); // Counter should be 2

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

        server_handle.await.expect("server handle failed");

        // Clean up JetStream
        js_context.delete_stream(stream.name).await.unwrap();

        Ok(())
    }

    #[cfg(feature = "nats-tests")]
    #[tokio::test]
    async fn test_unaligned_session_reducer_multiple_keys() -> crate::Result<()> {
        // Set up the session reducer server
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let tmp_dir = TempDir::new().unwrap();
        let sock_file = tmp_dir.path().join("session_reduce_multi_keys.sock");
        let server_info_file = tmp_dir.path().join("session_reduce_multi_keys-server-info");

        let server_info = server_info_file.clone();
        let server_socket = sock_file.clone();
        let server_handle = tokio::spawn(async move {
            session_reduce::Server::new(CounterCreator {})
                .with_socket_file(server_socket)
                .with_server_info_file(server_info)
                .start_with_shutdown(shutdown_rx)
                .await
                .expect("server failed");
        });

        // Wait for the server to start
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Create the session client
        let session_client = UserDefinedSessionReduce::new(SessionReduceClient::new(
            create_rpc_channel(sock_file).await?,
        ))
        .await;

        let client = UserDefinedUnalignedReduce::Session(session_client);

        // Create a session window manager with 60s timeout
        let windower = SessionWindowManager::new(Duration::from_secs(60));
        let window_manager = UnalignedWindowManager::Session(windower);

        // Set up JetStream
        let js_url = "localhost:4222";
        let nats_client = async_nats::connect(js_url).await.unwrap();
        let js_context = jetstream::new(nats_client);

        // Create output stream
        let stream = Stream::new("test_unaligned_session_multi_keys", "test", 0);
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
        let writer_config = BufferWriterConfig {
            streams: vec![stream.clone()],
            ..Default::default()
        };

        let mut writers = HashMap::new();
        writers.insert(
            stream.name,
            crate::pipeline::isb::jetstream::js_writer::JetStreamWriter::new(
                stream.clone(),
                js_context.clone(),
                writer_config.clone(),
                None,
                cln_token.clone(),
            )
            .await?,
        );

        let writer_components = ISBWriterComponents {
            config: vec![ToVertexConfig {
                name: "test-vertex",
                partitions: 1,
                writer_config,
                conditions: None,
                to_vertex_type: VertexType::Sink,
            }],
            writers,
            paf_concurrency: 100,
            watermark_handle: None,
            vertex_type: VertexType::ReduceUDF,
        };
        let isb_writer = ISBWriter::new(writer_components);

        // Create the UnalignedReducer
        let reducer = UnalignedReducer::new(
            client,
            window_manager,
            isb_writer,
            Duration::from_secs(0), // No allowed lateness for testing
            None,                   // No GC WAL for testing
            Duration::from_millis(50),
            true, // No watermark handle for testing
        )
        .await;

        // Create a channel for input messages
        let (input_tx, input_rx) = mpsc::channel(10);
        let input_stream = ReceiverStream::new(input_rx);

        // Start the reducer
        let reducer_handle = reducer.start(input_stream, cln_token.clone()).await?;

        // Create test messages
        let base_time = Utc.with_ymd_and_hms(2023, 1, 1, 0, 0, 0).unwrap();

        // Messages for key1
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

        let msg2 = Message {
            typ: Default::default(),
            keys: Arc::from(vec!["key1".into()]),
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

        // Messages for key2
        let msg3 = Message {
            typ: Default::default(),
            keys: Arc::from(vec!["key2".into()]),
            tags: None,
            value: "value3".into(),
            offset: Offset::String(StringOffset::new("2".to_string(), 2)),
            event_time: base_time + chrono::Duration::seconds(15),
            watermark: None,
            id: MessageID {
                vertex_name: "vertex_name".to_string().into(),
                offset: "2".to_string().into(),
                index: 2,
            },
            ..Default::default()
        };

        let msg4 = Message {
            typ: Default::default(),
            keys: Arc::from(vec!["key2".into()]),
            tags: None,
            value: "value4".into(),
            offset: Offset::String(StringOffset::new("3".to_string(), 3)),
            event_time: base_time + chrono::Duration::seconds(25),
            watermark: None,
            id: MessageID {
                vertex_name: "vertex_name".to_string().into(),
                offset: "3".to_string().into(),
                index: 3,
            },
            ..Default::default()
        };

        // Message with watermark to trigger window close
        let msg5 = Message {
            typ: Default::default(),
            keys: Arc::from(vec!["key3".into()]),
            tags: None,
            value: "value5".into(),
            offset: Offset::String(StringOffset::new("4".to_string(), 4)),
            event_time: base_time + chrono::Duration::seconds(120),
            watermark: Some(base_time + chrono::Duration::seconds(100)), // Past session timeout
            id: MessageID {
                vertex_name: "vertex_name".to_string().into(),
                offset: "4".to_string().into(),
                index: 4,
            },
            ..Default::default()
        };

        // Send the messages
        input_tx.send(msg1).await.unwrap();
        input_tx.send(msg2).await.unwrap();
        input_tx.send(msg3).await.unwrap();
        input_tx.send(msg4).await.unwrap();
        input_tx.send(msg5).await.unwrap();

        // Create a consumer to read the results
        let consumer: PullConsumer = js_context
            .get_consumer_from_stream(&stream.name, &stream.name)
            .await
            .unwrap();

        // Read messages from the stream
        let mut messages = consumer
            .batch()
            .expires(Duration::from_secs(2))
            .max_messages(3)
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
            received_keys.insert(key.clone());

            // Verify the count based on the key
            let expected_count = match key.as_str() {
                "key1" | "key2" => "2", // Each key has 2 messages
                _ => panic!("Unexpected key: {}", key),
            };

            assert_eq!(
                String::from_utf8(proto_message.body.unwrap().payload.to_vec()).unwrap(),
                expected_count
            );

            result_count += 1;
        }

        // Verify we received results for all keys
        assert!(received_keys.contains("key1"), "Missing result for key1");
        assert!(received_keys.contains("key2"), "Missing result for key2");
        assert_eq!(
            result_count, 2,
            "Expected exactly two result messages for three keys"
        );

        cln_token.cancel();
        drop(input_tx);

        // Wait for the reducer to complete
        reducer_handle.await.expect("reducer handle failed")?;

        // Shutdown the server
        shutdown_tx
            .send(())
            .expect("failed to send shutdown signal");

        server_handle.await.expect("server handle failed");

        // Clean up JetStream
        js_context.delete_stream(stream.name).await.unwrap();

        Ok(())
    }

    #[cfg(feature = "nats-tests")]
    #[tokio::test]
    async fn test_unaligned_accumulator_reducer_basic() -> crate::Result<()> {
        // Set up the accumulator reducer server
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let tmp_dir = TempDir::new().unwrap();
        let sock_file = tmp_dir.path().join("accumulator_reduce_basic.sock");
        let server_info_file = tmp_dir.path().join("accumulator_reduce_basic-server-info");

        let server_info = server_info_file.clone();
        let server_socket = sock_file.clone();
        let server_handle = tokio::spawn(async move {
            numaflow::accumulator::Server::new(AccumulatorCounterCreator {})
                .with_socket_file(server_socket)
                .with_server_info_file(server_info)
                .start_with_shutdown(shutdown_rx)
                .await
                .expect("server failed");
        });

        // Wait for the server to start
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Create the accumulator client
        let accumulator_client = UserDefinedAccumulator::new(
            numaflow_pb::clients::accumulator::accumulator_client::AccumulatorClient::new(
                create_rpc_channel(sock_file).await?,
            ),
        )
        .await;

        let client = UserDefinedUnalignedReduce::Accumulator(accumulator_client);

        // Create a simple accumulator window manager with 60s timeout
        let windower =
            crate::reduce::reducer::unaligned::windower::accumulator::AccumulatorWindowManager::new(
                Duration::from_secs(60),
            );
        let window_manager = UnalignedWindowManager::Accumulator(windower);

        // Set up JetStream
        let js_url = "localhost:4222";
        let nats_client = async_nats::connect(js_url).await.unwrap();
        let js_context = jetstream::new(nats_client);

        // Create output stream
        let stream = Stream::new("test_unaligned_accumulator_basic", "test", 0);
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
        let writer_config = BufferWriterConfig {
            streams: vec![stream.clone()],
            ..Default::default()
        };

        let mut writers = HashMap::new();
        writers.insert(
            stream.name,
            crate::pipeline::isb::jetstream::js_writer::JetStreamWriter::new(
                stream.clone(),
                js_context.clone(),
                writer_config.clone(),
                None,
                cln_token.clone(),
            )
            .await?,
        );

        let writer_components = ISBWriterComponents {
            config: vec![ToVertexConfig {
                name: "test-vertex",
                partitions: 1,
                writer_config,
                conditions: None,
                to_vertex_type: VertexType::Sink,
            }],
            writers,
            paf_concurrency: 100,
            watermark_handle: None,
            vertex_type: VertexType::ReduceUDF,
        };
        let isb_writer = ISBWriter::new(writer_components);

        // Create the UnalignedReducer
        let reducer = UnalignedReducer::new(
            client,
            window_manager,
            isb_writer,
            Duration::from_secs(0), // No allowed lateness for testing
            None,                   // No GC WAL for testing
            Duration::from_millis(50),
            true,
        )
        .await;

        // Create a channel for input messages
        let (input_tx, input_rx) = mpsc::channel(10);
        let input_stream = ReceiverStream::new(input_rx);

        // Start the reducer
        let reducer_handle = reducer.start(input_stream, cln_token.clone()).await?;

        // Create test messages - accumulator fires every 3 messages
        let base_time = Utc.with_ymd_and_hms(2023, 1, 1, 0, 0, 0).unwrap();

        // Send 6 messages to trigger 2 accumulator outputs
        for i in 0..6 {
            let msg = Message {
                typ: Default::default(),
                keys: Arc::from(vec!["key1".into()]),
                tags: None,
                value: format!("value{}", i + 1).into(),
                offset: Offset::String(StringOffset::new(i.to_string(), i)),
                event_time: base_time + chrono::Duration::seconds(((i + 1) * 10) as i64),
                watermark: Some(base_time + chrono::Duration::seconds((i * 10) as i64)),
                id: MessageID {
                    vertex_name: "vertex_name".to_string().into(),
                    offset: i.to_string().into(),
                    index: i as i32,
                },
                ..Default::default()
            };
            input_tx.send(msg).await.unwrap();
        }

        // Create a consumer to read the results
        let consumer: PullConsumer = js_context
            .get_consumer_from_stream(&stream.name, &stream.name)
            .await
            .unwrap();

        // Read messages from the stream
        let mut messages = consumer
            .batch()
            .expires(Duration::from_secs(3))
            .max_messages(2)
            .messages()
            .await
            .unwrap();

        let mut result_count = 0;
        let expected_counts = vec!["count_3", "count_6"];

        while let Some(msg) = messages.next().await {
            let msg = msg.unwrap();
            let data = msg.payload.to_vec();

            // Acknowledge the message
            msg.ack().await.unwrap();

            // Parse the message
            let message: numaflow_pb::objects::isb::Message =
                prost::Message::decode(data.as_ref()).unwrap();

            // Verify the result
            let expected_key = format!("key1_{}", if result_count == 0 { 3 } else { 6 });
            assert_eq!(message.header.unwrap().keys.to_vec(), vec![expected_key]);
            assert_eq!(
                message.body.unwrap().payload.to_vec(),
                expected_counts[result_count].as_bytes().to_vec()
            );

            result_count += 1;
        }

        assert_eq!(result_count, 2, "Expected exactly two result messages");

        cln_token.cancel();
        drop(input_tx);

        // Wait for the reducer to complete
        reducer_handle.await.expect("reducer handle failed")?;

        // Shutdown the server
        shutdown_tx
            .send(())
            .expect("failed to send shutdown signal");

        server_handle.await.expect("server handle failed");

        // Clean up JetStream
        js_context.delete_stream(stream.name).await.unwrap();

        Ok(())
    }

    #[cfg(feature = "nats-tests")]
    #[tokio::test]
    async fn test_unaligned_accumulator_reducer_multiple_keys() -> crate::Result<()> {
        // Set up the accumulator reducer server
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let tmp_dir = TempDir::new().unwrap();
        let sock_file = tmp_dir.path().join("accumulator_reduce_multi_keys.sock");
        let server_info_file = tmp_dir
            .path()
            .join("accumulator_reduce_multi_keys-server-info");

        let server_info = server_info_file.clone();
        let server_socket = sock_file.clone();
        let server_handle = tokio::spawn(async move {
            accumulator::Server::new(AccumulatorCounterCreator {})
                .with_socket_file(server_socket)
                .with_server_info_file(server_info)
                .start_with_shutdown(shutdown_rx)
                .await
                .expect("server failed");
        });

        // Wait for the server to start
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Create the accumulator client
        let accumulator_client = UserDefinedAccumulator::new(
            numaflow_pb::clients::accumulator::accumulator_client::AccumulatorClient::new(
                create_rpc_channel(sock_file).await?,
            ),
        )
        .await;

        let client = UserDefinedUnalignedReduce::Accumulator(accumulator_client);

        // Create a simple accumulator window manager with 60s timeout
        let windower =
            crate::reduce::reducer::unaligned::windower::accumulator::AccumulatorWindowManager::new(
                Duration::from_secs(60),
            );
        let window_manager = UnalignedWindowManager::Accumulator(windower);

        // Set up JetStream
        let js_url = "localhost:4222";
        let nats_client = async_nats::connect(js_url).await.unwrap();
        let js_context = jetstream::new(nats_client);

        // Create output stream
        let stream = Stream::new("test_unaligned_accumulator_multi_keys", "test", 0);
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
        let writer_config = BufferWriterConfig {
            streams: vec![stream.clone()],
            ..Default::default()
        };

        let mut writers = HashMap::new();
        writers.insert(
            stream.name,
            crate::pipeline::isb::jetstream::js_writer::JetStreamWriter::new(
                stream.clone(),
                js_context.clone(),
                writer_config.clone(),
                None,
                cln_token.clone(),
            )
            .await?,
        );

        let writer_components = ISBWriterComponents {
            config: vec![ToVertexConfig {
                name: "test-vertex",
                partitions: 1,
                writer_config,
                conditions: None,
                to_vertex_type: VertexType::Sink,
            }],
            writers,
            paf_concurrency: 100,
            watermark_handle: None,
            vertex_type: VertexType::ReduceUDF,
        };
        let isb_writer = ISBWriter::new(writer_components);

        // Create the UnalignedReducer
        let reducer = UnalignedReducer::new(
            client,
            window_manager,
            isb_writer,
            Duration::from_secs(0), // No allowed lateness for testing
            None,                   // No GC WAL for testing
            Duration::from_millis(50),
            true,
        )
        .await;

        // Create a channel for input messages
        let (input_tx, input_rx) = mpsc::channel(10);
        let input_stream = ReceiverStream::new(input_rx);

        // Start the reducer
        let reducer_handle = reducer.start(input_stream, cln_token.clone()).await?;

        // Create test messages with the same key - accumulator fires every 3 messages
        let base_time = Utc.with_ymd_and_hms(2023, 1, 1, 0, 0, 0).unwrap();

        // Send 6 messages to the same key to get 2 accumulator outputs (at 3 and 6)
        for i in 0..6 {
            let msg = Message {
                typ: Default::default(),
                keys: Arc::from(vec!["key-1".into()]),
                tags: None,
                value: format!("value_multi_{}", i + 1).into(),
                offset: Offset::String(StringOffset::new(i.to_string(), i)),
                event_time: base_time + chrono::Duration::seconds(((i + 1) * 10) as i64),
                watermark: Some(base_time + chrono::Duration::seconds((i * 10) as i64)),
                id: MessageID {
                    vertex_name: "id-one".to_string().into(),
                    offset: i.to_string().into(),
                    index: i as i32,
                },
                ..Default::default()
            };
            input_tx.send(msg).await.unwrap();
            let msg = Message {
                typ: Default::default(),
                keys: Arc::from(vec!["key-2".into()]),
                tags: None,
                value: format!("value_multi_{}", i + 1).into(),
                offset: Offset::String(StringOffset::new(i.to_string(), i)),
                event_time: base_time + chrono::Duration::seconds(((i + 1) * 10) as i64),
                watermark: Some(base_time + chrono::Duration::seconds((i * 10) as i64)),
                id: MessageID {
                    vertex_name: "id-two".to_string().into(),
                    offset: i.to_string().into(),
                    index: i as i32,
                },
                ..Default::default()
            };
            input_tx.send(msg).await.unwrap();
        }

        // Create a consumer to read the results
        let consumer: PullConsumer = js_context
            .get_consumer_from_stream(&stream.name, &stream.name)
            .await
            .unwrap();

        // Read messages from the stream
        let mut messages = consumer
            .batch()
            .expires(Duration::from_secs(1))
            .max_messages(4)
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
            let message: numaflow_pb::objects::isb::Message =
                prost::Message::decode(data.as_ref()).unwrap();

            // Extract and store the key
            let key = message.header.unwrap().keys[0].clone();
            received_keys.insert(key.clone());

            // Verify the count based on the key - accumulator fires at count 3 and 6
            let expected_count = if key.ends_with("_3") {
                "count_3"
            } else if key.ends_with("_6") {
                "count_6"
            } else {
                panic!("Unexpected key: {}", key)
            };

            assert_eq!(
                String::from_utf8(message.body.unwrap().payload.to_vec()).unwrap(),
                expected_count
            );

            result_count += 1;
        }

        // Verify we received results for both counts
        let has_count_3 = received_keys.iter().any(|k| k.ends_with("_3"));
        let has_count_6 = received_keys.iter().any(|k| k.ends_with("_6"));
        assert!(
            has_count_3,
            "Missing result for count_3, received keys: {:?}",
            received_keys
        );
        assert!(
            has_count_6,
            "Missing result for count_6, received keys: {:?}",
            received_keys
        );
        assert_eq!(result_count, 4, "Expected exactly two result messages");

        cln_token.cancel();
        drop(input_tx);

        // Wait for the reducer to complete
        reducer_handle.await.expect("reducer handle failed")?;

        // Shutdown the server
        shutdown_tx
            .send(())
            .expect("failed to send shutdown signal");

        server_handle.await.expect("server handle failed");

        // Clean up JetStream
        js_context.delete_stream(stream.name).await.unwrap();

        Ok(())
    }

    #[cfg(feature = "nats-tests")]
    #[tokio::test]
    async fn test_unaligned_session_reducer_with_session_merging() -> crate::Result<()> {
        // Set up the session reducer server
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let tmp_dir = TempDir::new().unwrap();
        let sock_file = tmp_dir.path().join("session_reduce_merging.sock");
        let server_info_file = tmp_dir.path().join("session_reduce_merging-server-info");

        let server_info = server_info_file.clone();
        let server_socket = sock_file.clone();
        let server_handle = tokio::spawn(async move {
            session_reduce::Server::new(CounterCreator {})
                .with_socket_file(server_socket)
                .with_server_info_file(server_info)
                .start_with_shutdown(shutdown_rx)
                .await
                .expect("server failed");
        });

        // Wait for the server to start
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Create the session client
        let session_client = UserDefinedSessionReduce::new(SessionReduceClient::new(
            create_rpc_channel(sock_file).await?,
        ))
        .await;

        let client = UserDefinedUnalignedReduce::Session(session_client);

        // Create a session window manager with 30s timeout
        let windower = SessionWindowManager::new(Duration::from_secs(30));
        let window_manager = UnalignedWindowManager::Session(windower);

        // Set up JetStream
        let js_url = "localhost:4222";
        let nats_client = async_nats::connect(js_url).await.unwrap();
        let js_context = jetstream::new(nats_client);

        // Create output stream
        let stream = Stream::new("test_unaligned_session_merging", "test", 0);
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
        let writer_config = BufferWriterConfig {
            streams: vec![stream.clone()],
            ..Default::default()
        };

        let mut writers = HashMap::new();
        writers.insert(
            stream.name,
            crate::pipeline::isb::jetstream::js_writer::JetStreamWriter::new(
                stream.clone(),
                js_context.clone(),
                writer_config.clone(),
                None,
                cln_token.clone(),
            )
            .await?,
        );

        let writer_components = ISBWriterComponents {
            config: vec![ToVertexConfig {
                name: "test-vertex",
                partitions: 1,
                writer_config,
                conditions: None,
                to_vertex_type: VertexType::Sink,
            }],
            writers,
            paf_concurrency: 100,
            watermark_handle: None,
            vertex_type: VertexType::ReduceUDF,
        };
        let isb_writer = ISBWriter::new(writer_components);

        // Create the UnalignedReducer
        let reducer = UnalignedReducer::new(
            client,
            window_manager,
            isb_writer,
            Duration::from_secs(0), // No allowed lateness for testing
            None,                   // No GC WAL for testing
            Duration::from_millis(50),
            true,
        )
        .await;

        // Create a channel for input messages
        let (input_tx, input_rx) = mpsc::channel(10);
        let input_stream = ReceiverStream::new(input_rx);

        // Start the reducer
        let reducer_handle = reducer.start(input_stream, cln_token.clone()).await?;

        // Create test messages that will create overlapping session windows
        let base_time = Utc.with_ymd_and_hms(2023, 1, 1, 0, 0, 0).unwrap();

        // Message 1: Creates first session window
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

        // Message 2: Creates second session window that will merge with first
        let msg2 = Message {
            typ: Default::default(),
            keys: Arc::from(vec!["key1".into()]),
            tags: None,
            value: "value2".into(),
            offset: Offset::String(StringOffset::new("1".to_string(), 1)),
            event_time: base_time + chrono::Duration::seconds(35), // Within merge range of first window
            watermark: None,
            id: MessageID {
                vertex_name: "vertex_name".to_string().into(),
                offset: "1".to_string().into(),
                index: 1,
            },
            ..Default::default()
        };

        // Message 3: Adds to the merged session
        let msg3 = Message {
            typ: Default::default(),
            keys: Arc::from(vec!["key1".into()]),
            tags: None,
            value: "value3".into(),
            offset: Offset::String(StringOffset::new("2".to_string(), 2)),
            event_time: base_time + chrono::Duration::seconds(50),
            watermark: None,
            id: MessageID {
                vertex_name: "vertex_name".to_string().into(),
                offset: "2".to_string().into(),
                index: 2,
            },
            ..Default::default()
        };

        // Message 4: With watermark to trigger window close
        let msg4 = Message {
            typ: Default::default(),
            keys: Arc::from(vec!["key2".into()]),
            tags: None,
            value: "value4".into(),
            offset: Offset::String(StringOffset::new("3".to_string(), 3)),
            event_time: base_time + chrono::Duration::seconds(120),
            watermark: Some(base_time + chrono::Duration::seconds(100)), // Past session timeout
            id: MessageID {
                vertex_name: "vertex_name".to_string().into(),
                offset: "3".to_string().into(),
                index: 3,
            },
            ..Default::default()
        };

        // Message 5: With watermark to trigger window close
        let msg5 = Message {
            typ: Default::default(),
            keys: Arc::from(vec!["key3".into()]),
            tags: None,
            value: "value5".into(),
            offset: Offset::String(StringOffset::new("4".to_string(), 4)),
            event_time: base_time + chrono::Duration::seconds(200),
            watermark: Some(base_time + chrono::Duration::seconds(180)), // Past key-2 session timeout
            id: MessageID {
                vertex_name: "vertex_name".to_string().into(),
                offset: "4".to_string().into(),
                index: 4,
            },
            ..Default::default()
        };

        // Send the messages
        input_tx.send(msg1).await.unwrap();
        input_tx.send(msg2).await.unwrap();
        input_tx.send(msg3).await.unwrap();
        input_tx.send(msg4).await.unwrap();
        input_tx.send(msg5).await.unwrap();

        // Create a consumer to read the results
        let consumer: PullConsumer = js_context
            .get_consumer_from_stream(&stream.name, &stream.name)
            .await
            .unwrap();

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
            received_keys.insert(key.clone());

            // Verify the count based on the key
            let expected_count = match key.as_str() {
                "key1" => "3", // Three messages merged into one session
                "key2" => "1", // One message in separate session
                _ => panic!("Unexpected key: {}", key),
            };

            assert_eq!(
                String::from_utf8(proto_message.body.unwrap().payload.to_vec()).unwrap(),
                expected_count
            );

            result_count += 1;
        }

        // Verify we received results for both keys
        assert!(received_keys.contains("key1"), "Missing result for key1");
        assert!(received_keys.contains("key2"), "Missing result for key2");
        assert_eq!(
            result_count, 2,
            "Expected exactly two result messages for session merging"
        );

        cln_token.cancel();
        drop(input_tx);

        // Wait for the reducer to complete
        reducer_handle.await.expect("reducer handle failed")?;

        // Shutdown the server
        shutdown_tx
            .send(())
            .expect("failed to send shutdown signal");

        server_handle.await.expect("server handle failed");

        // Clean up JetStream
        js_context.delete_stream(stream.name).await.unwrap();

        Ok(())
    }
}
