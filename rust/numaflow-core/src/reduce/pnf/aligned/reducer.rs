use crate::error::Error;
use crate::message::Message;
use crate::pipeline::isb::jetstream::writer::JetstreamWriter;
use crate::reduce::pnf::aligned::user_defined::UserDefinedAlignedReduce;
use crate::reduce::pnf::aligned::windower::{
    AlignedWindowMessage, FixedWindowMessage, Window, WindowOperation, Windower,
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
use tracing::{error, info, warn};

/// Represents an active reduce stream for a window
struct ActiveStream {
    /// Sender for window messages
    message_tx: mpsc::Sender<AlignedWindowMessage>,
    /// Handle to the task processing the window
    task_handle: JoinHandle<()>,
}

/// Actor that manages multiple window reduction streams
struct AlignedReduceActor {
    receiver: mpsc::Receiver<AlignedWindowMessage>,
    client: UserDefinedAlignedReduce,
    /// Map of active streams keyed by window ID (pnf_slot)
    active_streams: HashMap<Bytes, ActiveStream>,
    /// JetStream writer for writing results
    js_writer: JetstreamWriter,
    /// Sender for error messages
    error_tx: mpsc::Sender<Error>,
    /// Sender for GC WAL messages
    gc_wal_tx: Option<mpsc::Sender<SegmentWriteMessage>>,
}

impl AlignedReduceActor {
    pub(crate) async fn new(
        client: UserDefinedAlignedReduce,
        receiver: mpsc::Receiver<AlignedWindowMessage>,
        js_writer: JetstreamWriter,
        error_tx: mpsc::Sender<Error>,
        gc_wal_tx: Option<mpsc::Sender<SegmentWriteMessage>>,
    ) -> Self {
        Self {
            client,
            receiver,
            active_streams: HashMap::new(),
            js_writer,
            error_tx,
            gc_wal_tx,
        }
    }

    async fn run(mut self) {
        while let Some(msg) = self.receiver.recv().await {
            self.handle_message(msg).await;
        }
    }

    async fn handle_message(&mut self, msg: AlignedWindowMessage) {
        match msg {
            AlignedWindowMessage::Fixed(msg) => {
                self.handle_window_message(msg.window, msg.operation).await
            }
            AlignedWindowMessage::Sliding(msg) => {
                self.handle_window_message(msg.window, msg.operation).await
            }
        }
    }

    /// Handle a window message based on its operation type
    async fn handle_window_message(&mut self, window: Window, operation: WindowOperation) {
        let window_id = window.pnf_slot();
        match operation {
            WindowOperation::Open(msg) => self.window_open(window, window_id, msg).await,
            WindowOperation::Append(msg) => self.window_append(window, window_id, msg).await,
            WindowOperation::Close => self.window_close(window, window_id).await,
        }
    }

    async fn window_open(&mut self, window: Window, window_id: Bytes, msg: Message) {
        // Create a new channel for this window's messages
        let (message_tx, message_rx) = mpsc::channel(100);
        let message_stream = ReceiverStream::new(message_rx);

        // Clone what we need for the task
        let client = self.client.clone();
        let js_writer = self.js_writer.clone();
        let gc_wal_tx_clone = self.gc_wal_tx.clone();
        let error_tx = self.error_tx.clone();
        let window_clone = window.clone();

        // Create the initial window message
        let window_msg = AlignedWindowMessage::Fixed(FixedWindowMessage {
            operation: WindowOperation::Open(msg),
            window: window.clone(),
        });

        // Spawn a task to process this window's messages
        let task_handle = tokio::spawn(async move {
            // Process the window and send results to JetStream
            Self::process_window_stream(
                client,
                message_stream,
                js_writer,
                gc_wal_tx_clone,
                error_tx,
                window_clone,
            )
            .await;
        });

        // Store the stream and task handle
        self.active_streams.insert(
            window_id,
            ActiveStream {
                message_tx: message_tx.clone(),
                task_handle,
            },
        );

        // Send the open message
        let _ = message_tx.send(window_msg).await;
    }

    /// Process a window stream and write results to JetStream
    async fn process_window_stream(
        mut client: UserDefinedAlignedReduce,
        message_stream: ReceiverStream<AlignedWindowMessage>,
        js_writer: JetstreamWriter,
        gc_wal_tx: Option<mpsc::Sender<SegmentWriteMessage>>,
        error_tx: mpsc::Sender<Error>,
        window: Window,
    ) {
        // Create a local channel for results
        let (result_tx, result_rx) = mpsc::channel(100);
        let result_stream = ReceiverStream::new(result_rx);

        // Spawn a task to write results to JetStream
        let writer_handle = match js_writer
            .streaming_write(result_stream, CancellationToken::new())
            .await
        {
            Ok(handle) => handle,
            Err(e) => {
                error!(?e, "Failed to start JetStream writer");
                return;
            }
        };

        // Send the window messages to the client for processing
        let result = client.reduce_fn(message_stream, result_tx).await;

        if let Err(e) = result {
            error!(?e, "Error while doing reduce operation");
            let _ = error_tx.send(e).await;
            return;
        }

        // Wait for the writer to complete
        if let Err(e) = writer_handle.await.expect("join failed for js writer task") {
            error!(?e, "Error while writing results to JetStream");
            let _ = error_tx.send(e).await;
            return;
        }

        let Some(gc_wal_tx) = gc_wal_tx else {
            return;
        };

        // Send GC event if WAL is configured
        let gc_event: GcEvent = window.into();
        gc_wal_tx
            .send(SegmentWriteMessage::WriteData {
                offset: None,
                data: prost::Message::encode_to_vec(&gc_event).into(),
            })
            .await
            .expect("failed to write gc event to wal");
    }

    async fn window_append(&mut self, window: Window, window_id: Bytes, msg: Message) {
        // Get the existing stream or log error if not found
        let Some(active_stream) = self.active_streams.get(&window_id) else {
            error!("No active stream found for window {:?}", window_id);
            return;
        };

        // Create the append window message
        let window_msg = AlignedWindowMessage::Fixed(FixedWindowMessage {
            operation: WindowOperation::Append(msg),
            window,
        });

        // Send the append message
        let _ = active_stream.message_tx.send(window_msg).await;
    }

    async fn window_close(&mut self, window: Window, window_id: Bytes) {
        // Get the existing stream or log error if not found
        let Some(active_stream) = self.active_streams.remove(&window_id) else {
            error!("No active stream found for window {:?}", window_id);
            return;
        };

        // Create the close window message
        let window_msg = AlignedWindowMessage::Fixed(FixedWindowMessage {
            operation: WindowOperation::Close,
            window,
        });

        // Send the close message
        let _ = active_stream.message_tx.send(window_msg).await;

        // Drop the sender to signal completion
        drop(active_stream.message_tx);

        // Wait for the task to complete
        if let Err(e) = active_stream.task_handle.await {
            error!("Reduce task for window {:?} failed: {}", window_id, e);
        }
    }
}

/// Processes messages and forwards results to the next stage
pub(crate) struct ProcessAndForward<W: Windower + Send + Sync + Clone + 'static> {
    client: UserDefinedAlignedReduce,
    windower: W,
    js_writer: JetstreamWriter,
    /// Final state of the component (any error will set this as Err)
    final_result: crate::Result<()>,
    /// Set to true when shutting down due to an error
    shutting_down_on_err: bool,
    gc_wal: Option<AppendOnlyWal>,
}

impl<W: Windower + Send + Sync + Clone + 'static> ProcessAndForward<W> {
    pub(crate) async fn new(
        client: UserDefinedAlignedReduce,
        windower: W,
        js_writer: JetstreamWriter,
        gc_wal: Option<AppendOnlyWal>,
    ) -> Self {
        Self {
            client,
            windower,
            js_writer,
            final_result: Ok(()),
            shutting_down_on_err: false,
            gc_wal,
        }
    }

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
        )
        .await;

        tokio::spawn(async move {
            actor.run().await;
        });

        // Spawn the main processing task
        let handle = tokio::spawn(async move {
            let mut input_stream = input_stream;

            loop {
                tokio::select! {
                    Some(error) = error_rx.recv() => {
                        self.handle_error(error, &cln_token);
                    },
                    read_msg = input_stream.next() => {
                        let Some(msg) = read_msg else {
                            break;
                        };

                        // If shutting down, drain the stream
                        if self.shutting_down_on_err {
                            warn!("Reduce component is shutting down due to an error, not accepting the message");
                            continue;
                        }

                        // Process the message through the windower
                        let window_messages = self.windower.assign_windows(msg);

                        // Send each window message to the actor
                        for window_msg in window_messages {
                            actor_tx.send(window_msg).await.expect("Receiver dropped");
                        }
                    }
                }
            }

            info!(status=?self.final_result, "Reduce component completed");
            self.final_result
        });

        Ok(handle)
    }

    /// Set up the GC WAL if configured
    async fn setup_gc_wal(&mut self) -> crate::Result<Option<mpsc::Sender<SegmentWriteMessage>>> {
        if let Some(gc_wal) = self.gc_wal.take() {
            let (gc_tx, gc_rx) = mpsc::channel(100);
            gc_wal.streaming_write(ReceiverStream::new(gc_rx)).await?;
            Ok(Some(gc_tx))
        } else {
            Ok(None)
        }
    }

    /// Handle an error by canceling the token and updating state
    fn handle_error(&mut self, error: Error, cln_token: &CancellationToken) {
        if self.final_result.is_ok() {
            error!(?error, "Error received while performing reduce operation");
            cln_token.cancel();
            self.final_result = Err(error);
            self.shutting_down_on_err = true;
        }
    }
}
