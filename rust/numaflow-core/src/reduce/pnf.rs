use crate::Result;
use crate::message::Message;
use crate::reduce::client::aligned::UserDefinedAlignedReduce;
use chrono::{DateTime, Utc};
use std::collections::HashMap;
use tokio::task::JoinHandle;
use tokio_stream::StreamExt;
use tokio_stream::wrappers::ReceiverStream;

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
pub(crate) enum WindowOperation {
    Open(Message),
    Close,
    Append(Message),
}

#[derive(Debug, Clone)]
pub(crate) enum AlignedWindowMessage {
    Fixed(FixedWindowMessage),
    Sliding(SlidingWindowMessage),
}

#[derive(Debug, Clone)]
pub(crate) struct FixedWindowMessage {
    pub(crate) operation: WindowOperation,
    pub(crate) window: Window,
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

    pub(crate) async fn assign_windows(_msg: Message) -> FixedWindowMessage {
        unimplemented!()
    }
}

pub(crate) enum AlignedReduceActorMessage {
    Fixed {
        msg: FixedWindowMessage,
        tx: tokio::sync::mpsc::Sender<Message>,
    },
    Sliding {
        msg: SlidingWindowMessage,
        tx: tokio::sync::mpsc::Sender<Message>,
    },
}

/// Represents an active reduce stream for a window
struct ActiveStream {
    /// Sender for window messages
    message_tx: tokio::sync::mpsc::Sender<AlignedWindowMessage>,
    /// Handle to the task processing the window
    task_handle: JoinHandle<()>,
}

pub(crate) struct AlignedReduceActor {
    pub(crate) receiver: tokio::sync::mpsc::Receiver<AlignedReduceActorMessage>,
    pub(crate) client: UserDefinedAlignedReduce,
    /// Map of active streams keyed by window ID (pnf_slot)
    active_streams: HashMap<String, ActiveStream>,
}

impl AlignedReduceActor {
    pub(crate) async fn new(
        client: UserDefinedAlignedReduce,
        receiver: tokio::sync::mpsc::Receiver<AlignedReduceActorMessage>,
    ) -> Self {
        AlignedReduceActor {
            client,
            receiver,
            active_streams: HashMap::new(),
        }
    }

    pub(crate) async fn run(mut self) {
        while let Some(msg) = self.receiver.recv().await {
            if let Err(e) = self.handle_message(msg).await {
                tracing::error!("Error handling message: {}", e);
            }
        }
    }

    pub(crate) async fn handle_message(&mut self, msg: AlignedReduceActorMessage) -> Result<()> {
        match msg {
            AlignedReduceActorMessage::Fixed { msg, tx } => {
                self.handle_window_message(msg.window, msg.operation, tx)
                    .await
            }
            AlignedReduceActorMessage::Sliding { msg, tx } => {
                self.handle_window_message(msg.window, msg.operation, tx)
                    .await
            }
        }
    }

    /// Handle a window message based on its operation type
    async fn handle_window_message(
        &mut self,
        window: Window,
        operation: WindowOperation,
        result_tx: tokio::sync::mpsc::Sender<Message>,
    ) -> Result<()> {
        let window_id = window.pnf_slot();

        match operation {
            WindowOperation::Open(msg) => {
                // Create a new channel for this window's messages
                let (message_tx, message_rx) = tokio::sync::mpsc::channel(100);
                let message_stream = ReceiverStream::new(message_rx);

                // Clone what we need for the task
                let mut client = self.client.clone();
                let result_tx_clone = result_tx.clone();

                // Create the initial window message
                let window_msg = AlignedWindowMessage::Fixed(FixedWindowMessage {
                    operation: WindowOperation::Open(msg),
                    window: window.clone(),
                });

                // Spawn a task to process this window's messages
                let task_handle = tokio::spawn(async move {
                    // Send the window messages to the client for processing
                    if let Err(e) = client.reduce_fn(message_stream, result_tx_clone).await {
                        tracing::error!("Error in reduce_fn: {}", e);
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
                message_tx.send(window_msg).await.map_err(|e| {
                    crate::Error::Reduce(format!("failed to send open message: {}", e))
                })?;
            }
            WindowOperation::Append(msg) => {
                // Get the existing stream
                if let Some(active_stream) = self.active_streams.get(&window_id) {
                    // Create the append window message
                    let window_msg = AlignedWindowMessage::Fixed(FixedWindowMessage {
                        operation: WindowOperation::Append(msg),
                        window,
                    });

                    // Send the append message
                    active_stream
                        .message_tx
                        .send(window_msg)
                        .await
                        .map_err(|e| {
                            crate::Error::Reduce(format!("failed to send append message: {}", e))
                        })?;
                } else {
                    return Err(crate::Error::Reduce(format!(
                        "no active stream for window {}",
                        window_id
                    )));
                }
            }
            WindowOperation::Close => {
                // Get the existing stream
                if let Some(active_stream) = self.active_streams.remove(&window_id) {
                    // Create the close window message
                    let window_msg = AlignedWindowMessage::Fixed(FixedWindowMessage {
                        operation: WindowOperation::Close,
                        window,
                    });

                    // Send the close message
                    if let Err(e) = active_stream.message_tx.send(window_msg).await {
                        tracing::warn!("Failed to send close message: {}", e);
                    }

                    // Drop the sender to signal completion
                    drop(active_stream.message_tx);

                    // Wait for the task to complete
                    if let Err(e) = active_stream.task_handle.await {
                        tracing::warn!("Error waiting for reduce task: {}", e);
                    }
                } else {
                    return Err(crate::Error::Reduce(format!(
                        "no active stream for window {}",
                        window_id
                    )));
                }
            }
        }

        Ok(())
    }
}

pub(crate) struct ProcessAndForward {
    pub(crate) actor_tx: tokio::sync::mpsc::Sender<AlignedReduceActorMessage>,
    pub(crate) windower: FixedWindower,
}

impl ProcessAndForward {
    pub(crate) async fn new(client: UserDefinedAlignedReduce, windower: FixedWindower) -> Self {
        let (tx, rx) = tokio::sync::mpsc::channel(100);
        tokio::spawn(async move {
            let actor = AlignedReduceActor::new(client, rx).await;
            actor.run().await;
        });
        ProcessAndForward {
            windower,
            actor_tx: tx,
        }
    }

    pub(crate) async fn start(
        self,
        mut stream: ReceiverStream<Message>,
    ) -> Result<(ReceiverStream<Message>, JoinHandle<Result<()>>)> {
        let (tx, rx) = tokio::sync::mpsc::channel(100);
        let handle = tokio::spawn(async move {
            while let Some(msg) = stream.next().await {
                let window_msg = FixedWindower::assign_windows(msg).await;
                self.actor_tx
                    .send(AlignedReduceActorMessage::Fixed {
                        msg: window_msg,
                        tx: tx.clone(),
                    })
                    .await
                    .expect("Failed to send message to actor");
            }
            Ok(())
        });
        Ok((ReceiverStream::new(rx), handle))
    }
}
