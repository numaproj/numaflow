use crate::error;
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
use tracing::{info, warn};

/// Represents an active reduce stream for a window
struct ActiveStream {
    /// Sender for window messages
    message_tx: mpsc::Sender<AlignedWindowMessage>,
    /// Handle to the task processing the window
    task_handle: JoinHandle<()>,
}

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
        AlignedReduceActor {
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
            if let Err(e) = self.handle_message(msg).await {
                error!("Error handling message: {}", e);
                // Send the error to the error channel
                if let Err(send_err) = self.error_tx.send(e).await {
                    error!("Failed to send error to error channel: {}", send_err);
                }
            }
        }
    }

    async fn handle_message(&mut self, msg: AlignedWindowMessage) -> error::Result<()> {
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
    async fn handle_window_message(
        &mut self,
        window: Window,
        operation: WindowOperation,
    ) -> error::Result<()> {
        let window_id = window.pnf_slot();
        match operation {
            WindowOperation::Open(msg) => {
                self.window_open(window.clone(), window_id, msg).await;
            }
            WindowOperation::Append(msg) => {
                self.window_append(window.clone(), window_id, msg).await;
            }
            WindowOperation::Close => {
                self.window_close(window.clone(), window_id).await;
            }
        }

        Ok(())
    }

    async fn window_open(&mut self, window: Window, window_id: Bytes, msg: Message) {
        // Create a new channel for this window's messages
        let (message_tx, message_rx) = mpsc::channel(100);
        let message_stream = ReceiverStream::new(message_rx);

        // Clone what we need for the task
        let mut client = self.client.clone();
        let js_writer = self.js_writer.clone();
        let error_tx_clone = self.error_tx.clone();
        let gc_wal_tx_clone = self.gc_wal_tx.clone();

        // Create the initial window message
        let window_msg = AlignedWindowMessage::Fixed(FixedWindowMessage {
            operation: WindowOperation::Open(msg),
            window: window.clone(),
        });

        // Spawn a task to process this window's messages
        let task_handle = tokio::spawn(async move {
            // Create a local channel for results
            let (result_tx, result_rx) = mpsc::channel(100);
            let result_stream = ReceiverStream::new(result_rx);

            // Spawn a task to write results to JetStream
            let writer_handle = js_writer
                .streaming_write(result_stream, CancellationToken::new())
                .await
                .expect("Failed to start JetStream writer");

            // Send the window messages to the client for processing
            if let Err(e) = client.reduce_fn(message_stream, result_tx).await {
                // Send the error to the error channel to signal failure
                error_tx_clone
                    .send(e.clone())
                    .await
                    .expect("failed to send error");
            }

            // Wait for the writer to complete
            if let Err(e) = writer_handle.await {
                error!(?e, "Error in JetStream writer task");
            }

            if let Some(gc_wal_tx) = gc_wal_tx_clone {
                let gc_event: GcEvent = window.into();
                gc_wal_tx
                    .send(SegmentWriteMessage::WriteData {
                        offset: None,
                        data: prost::Message::encode_to_vec(&gc_event).into(),
                    })
                    .await
                    .expect("Failed to send GC message");
            }
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

    async fn window_append(&mut self, window: Window, window_id: Bytes, msg: Message) {
        // Get the existing stream - this should always exist
        let active_stream = self
            .active_streams
            .get(&window_id)
            .unwrap_or_else(|| panic!("no active stream for window {:?}", window_id));

        // Create the append window message
        let window_msg = AlignedWindowMessage::Fixed(FixedWindowMessage {
            operation: WindowOperation::Append(msg),
            window,
        });

        // Send the append message
        let _ = active_stream.message_tx.send(window_msg).await;
    }

    async fn window_close(&mut self, window: Window, window_id: Bytes) {
        // Get the existing stream - this should always exist
        let active_stream = self
            .active_streams
            .remove(&window_id)
            .unwrap_or_else(|| panic!("no active stream for window {:?}", window_id));

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
        active_stream
            .task_handle
            .await
            .unwrap_or_else(|_| panic!("reduce task for window {:?} failed", window_id));
    }
}

pub(crate) struct ProcessAndForward<W: Windower + Send + Sync + Clone + 'static> {
    client: UserDefinedAlignedReduce,
    windower: W,
    js_writer: JetstreamWriter,
    /// this the final state of the component (any error will set this as Err)
    final_result: error::Result<()>,
    /// The moment we see an error, we will set this to true.
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
        ProcessAndForward {
            client,
            windower,
            js_writer,
            final_result: Ok(()),
            shutting_down_on_err: false,
            gc_wal,
        }
    }

    // Update the start method to only return a JoinHandle
    pub(crate) async fn start(
        mut self,
        input_stream: ReceiverStream<Message>,
        cln_token: CancellationToken,
    ) -> crate::Result<JoinHandle<error::Result<()>>> {
        let (error_tx, mut error_rx) = mpsc::channel(10);
        let gc_wal_handle = if let Some(gc_wal) = self.gc_wal {
            let (gc_tx, gc_rx) = mpsc::channel(100);
            gc_wal.streaming_write(ReceiverStream::new(gc_rx)).await?;
            Some(gc_tx)
        } else {
            None
        };

        // Create the actor channel
        let (actor_tx, actor_rx) = mpsc::channel(100);

        // Create and start the actor
        let actor = AlignedReduceActor::new(
            self.client,
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
                        // When we get an error, cancel the token to signal upstream to stop sending
                        // new messages, and we empty the input stream and exit.
                        if self.final_result.is_ok() {
                            error!(?error, "Error received while performing reduce operation");
                            cln_token.cancel();
                            // We mark that we are in error state
                            self.final_result = Err(error);
                            self.shutting_down_on_err = true;
                        }
                    },
                    read_msg = input_stream.next() => {
                        let Some(msg) = read_msg else {
                            break;
                        };

                        // If there are errors then we need to drain the stream
                        if self.shutting_down_on_err {
                            warn!(
                                "Reduce component is shutting down because of an error, not accepting the message"
                            );
                            continue;
                        }

                        // Process the message through the windower
                        let window_msgs = self.windower.assign_windows(msg);

                        for window_msg in window_msgs {
                            // Send to the actor for processing
                            if let Err(e) = actor_tx.send(window_msg).await {
                                error!(?e, "Failed to send message to reduce actor");
                                let _ = error_tx.send(Error::Reduce(format!(
                                    "Failed to send message to reduce actor: {}", e
                                ))).await;
                            }
                        }
                    }
                }
            }

            info!(status=?self.final_result, "Reduce component is completed with status");
            self.final_result
        });

        Ok(handle)
    }
}
