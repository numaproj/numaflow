use crate::Result;
use crate::error::Error;
use crate::message::Message;
use crate::reduce::client::aligned::UserDefinedAlignedReduce;
use chrono::{DateTime, Utc};
use std::collections::HashMap;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_stream::StreamExt;
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

#[derive(Debug, Clone)]
pub(crate) enum WindowKind {
    Fixed,
    Sliding,
}

#[derive(Debug, Clone)]
pub(crate) struct Window {
    window_kind: WindowKind,
    pub(crate) start_time: DateTime<Utc>,
    pub(crate) end_time: DateTime<Utc>,
}

impl Window {
    pub(crate) fn new(
        window_kind: WindowKind,
        start_time: DateTime<Utc>,
        end_time: DateTime<Utc>,
    ) -> Self {
        Self {
            window_kind,
            start_time,
            end_time,
        }
    }

    pub(crate) fn pnf_slot(&self) -> String {
        format!(
            "{}-{}",
            self.start_time.timestamp_millis(),
            self.end_time.timestamp_millis(),
        )
    }
}

#[derive(Debug, Clone)]
pub(in crate::reduce) enum WindowOperation {
    Open(Message),
    Close,
    Append(Message),
}

#[derive(Debug, Clone)]
pub(in crate::reduce) enum AlignedWindowMessage {
    Fixed(FixedWindowMessage),
    Sliding(SlidingWindowMessage),
}

#[derive(Debug, Clone)]
pub(in crate::reduce) struct FixedWindowMessage {
    pub(in crate::reduce) operation: WindowOperation,
    pub(in crate::reduce) window: Window,
}

#[derive(Debug, Clone)]
pub(crate) struct SlidingWindowMessage {
    pub(crate) operation: WindowOperation,
    pub(crate) window: Window,
}

#[derive(Debug, Clone)]
pub(crate) struct FixedWindower {}

impl FixedWindower {
    pub(crate) async fn new() -> Self {
        FixedWindower {}
    }

    pub(crate) async fn assign_windows(&self, _msg: Message) -> FixedWindowMessage {
        unimplemented!()
    }
}

pub(crate) enum AlignedReduceActorMessage {
    Fixed { msg: FixedWindowMessage },
    Sliding { msg: SlidingWindowMessage },
}

/// Represents an active reduce stream for a window
struct ActiveStream {
    /// Sender for window messages
    message_tx: mpsc::Sender<AlignedWindowMessage>,
    /// Handle to the task processing the window
    task_handle: JoinHandle<()>,
}

pub(crate) struct AlignedReduceActor {
    pub(crate) receiver: mpsc::Receiver<AlignedReduceActorMessage>,
    pub(crate) client: UserDefinedAlignedReduce,
    /// Map of active streams keyed by window ID (pnf_slot)
    active_streams: HashMap<String, ActiveStream>,
    /// Sender for output messages
    result_tx: mpsc::Sender<Message>,
    /// Sender for error messages
    error_tx: mpsc::Sender<Error>,
}

impl AlignedReduceActor {
    pub(crate) async fn new(
        client: UserDefinedAlignedReduce,
        receiver: mpsc::Receiver<AlignedReduceActorMessage>,
        result_tx: mpsc::Sender<Message>,
        error_tx: mpsc::Sender<Error>,
    ) -> Self {
        AlignedReduceActor {
            client,
            receiver,
            active_streams: HashMap::new(),
            result_tx,
            error_tx,
        }
    }

    pub(crate) async fn run(mut self) {
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

    pub(crate) async fn handle_message(&mut self, msg: AlignedReduceActorMessage) -> Result<()> {
        match msg {
            AlignedReduceActorMessage::Fixed { msg } => {
                self.handle_window_message(msg.window, msg.operation).await
            }
            AlignedReduceActorMessage::Sliding { msg } => {
                self.handle_window_message(msg.window, msg.operation).await
            }
        }
    }

    /// Handle a window message based on its operation type
    async fn handle_window_message(
        &mut self,
        window: Window,
        operation: WindowOperation,
    ) -> Result<()> {
        let window_id = window.pnf_slot();
        match operation {
            WindowOperation::Open(msg) => {
                // Create a new channel for this window's messages
                let (message_tx, message_rx) = mpsc::channel(100);
                let message_stream = ReceiverStream::new(message_rx);

                // Clone what we need for the task
                let mut client = self.client.clone();
                let result_tx_clone = self.result_tx.clone();
                let error_tx_clone = self.error_tx.clone();

                // Create the initial window message
                let window_msg = AlignedWindowMessage::Fixed(FixedWindowMessage {
                    operation: WindowOperation::Open(msg),
                    window: window.clone(),
                });

                // Spawn a task to process this window's messages
                let task_handle = tokio::spawn(async move {
                    // Send the window messages to the client for processing
                    if let Err(e) = client.reduce_fn(message_stream, result_tx_clone).await {
                        // Send the error to the error channel to signal failure
                        error_tx_clone
                            .send(e.clone())
                            .await
                            .expect("failed to send error");

                        // TODO: rethink whether we should write the responses to js writer here
                        // instead of sending it to a next component (we have to write gc events etc)
                    }
                });

                // Store the stream and task handle
                self.active_streams.insert(
                    window_id.clone(),
                    ActiveStream {
                        message_tx: message_tx.clone(),
                        task_handle,
                    },
                );

                // Send the open message
                let _ = message_tx.send(window_msg).await;
            }
            WindowOperation::Append(msg) => {
                // Get the existing stream - this should always exist
                let active_stream = self
                    .active_streams
                    .get(&window_id)
                    .unwrap_or_else(|| panic!("no active stream for window {}", window_id));

                // Create the append window message
                let window_msg = AlignedWindowMessage::Fixed(FixedWindowMessage {
                    operation: WindowOperation::Append(msg),
                    window,
                });

                // Send the append message
                let _ = active_stream.message_tx.send(window_msg).await;
            }
            WindowOperation::Close => {
                // Get the existing stream - this should always exist
                let active_stream = self
                    .active_streams
                    .remove(&window_id)
                    .unwrap_or_else(|| panic!("no active stream for window {}", window_id));

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
                    .unwrap_or_else(|_| panic!("reduce task for window {} failed", window_id));
            }
        }

        Ok(())
    }
}

pub(crate) struct ProcessAndForward {
    client: UserDefinedAlignedReduce,
    windower: FixedWindower,
    /// this the final state of the component (any error will set this as Err)
    final_result: Result<()>,
    /// The moment we see an error, we will set this to true.
    shutting_down_on_err: bool,
}

impl ProcessAndForward {
    pub(crate) async fn new(client: UserDefinedAlignedReduce, windower: FixedWindower) -> Self {
        ProcessAndForward {
            client,
            windower,
            final_result: Ok(()),
            shutting_down_on_err: false,
        }
    }

    pub(crate) async fn start(
        mut self,
        input_stream: ReceiverStream<Message>,
        cln_token: CancellationToken,
    ) -> Result<(ReceiverStream<Message>, JoinHandle<Result<()>>)> {
        let (output_tx, output_rx) = mpsc::channel(100);
        let (error_tx, mut error_rx) = mpsc::channel(10);

        // Create the actor channel
        let (actor_tx, actor_rx) = mpsc::channel(100);

        // Create and start the actor
        let actor =
            AlignedReduceActor::new(self.client, actor_rx, output_tx.clone(), error_tx.clone())
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
                        let window_msg = self.windower.assign_windows(msg).await;

                        // Send to the actor for processing
                        if let Err(e) = actor_tx.send(AlignedReduceActorMessage::Fixed {
                            msg: window_msg,
                        }).await {
                            error!(?e, "Failed to send message to reduce actor");
                            let _ = error_tx.send(Error::Reduce(format!(
                                "Failed to send message to reduce actor: {}", e
                            ))).await;
                        }
                    }
                }
            }

            info!(status=?self.final_result, "Reduce component is completed with status");
            self.final_result
        });

        Ok((ReceiverStream::new(output_rx), handle))
    }
}
