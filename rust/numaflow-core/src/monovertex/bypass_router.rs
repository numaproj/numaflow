//! The concept of bypass router is relevant only for MonoVertex. The bypass router allows sending
//! messages to different sinks directly from Source and UDF components of MonoVertex based on
//! bypass conditions.
//!
//! The bypass router contains information regarding the channels
//! to which bypass messages will be sent by the source and mapper, as well as the
//! sink writer handle using which bypass router will write messages to the respective sinks.
//!
//! The bypass router struct is initialized to be passed to the methods of source and mapper handles.
//! The bypass router wraps the message to be sent to the bypass channel in [MessageToSink] enum
//! so that the receiver task can determine which sink the message should be sent to.
//!
//! The source and mapper handle methods use methods on bypass router to determine if a message
//! should be bypassed to a sink, and if so, the message is sent to the bypass channel held
//! by the router accordingly.
//!
//! The bypass router initialization also starts a background task which is responsible for
//! reading messages from the bypass channel and writing them to the respective sinks.
//!
//! ```text
//! +==========================================================================+
//! |                                MonoVertex                                |
//! |                                                                          |
//! |                                                                          |
//! |    +-----------+          +--------------------+          +-----------+  |
//! |    |  Source   |  ----->  |   Map (optional)   |  ----->  |           |  |
//! |    |           |          |                    |          |    Sink   |  |
//! |    |  Bypass   |          |      Bypass        |          |           |  |
//! |    |  Router   |          |      Router        |          |           |  |
//! |    +----|------+          +-------|------------+          +-----------+  |
//! |         |                         |                                      |
//! |         |                         |                                      |
//! |         v                         v                                      |
//! |  +----BypassRouterTask-------------------------------------------------+ |
//! |  |                                                                     | |
//! |  |  primary      ----------------------------------------------------> | |
//! |  |                                                                     | |
//! |  |  fallback     ----------------------------------------------------> | |
//! |  |                                                                     | |
//! |  |  on_success   ----------------------------------------------------> | |
//! |  |                                                                     | |
//! |  +---------------------------------------------------------------------+ |
//! |                                                                          |
//! +==========================================================================+
//! ```

use crate::config::monovertex::BypassConditions;
use crate::error;
use crate::error::Error;
use crate::message::{AckHandle, Message};
use crate::shared::forward::should_forward;
use crate::sinker::sink::SinkWriter;
use numaflow_models::models::ForwardConditions;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;
use tokio::pin;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_stream::StreamExt;
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;
use tracing::info;

/// Message Wrapper introduced to allow sink component to route the messages to the
/// appropriate sink based on the bypass condition.
#[derive(Debug, Clone)]
pub enum MessageToSink {
    Primary(Message),
    Fallback(Message),
    OnSuccess(Message),
}

/// Returns a reference to the inner message wrapped by the enum.
impl MessageToSink {
    pub fn inner(&self) -> &Message {
        match self {
            MessageToSink::Primary(msg)
            | MessageToSink::Fallback(msg)
            | MessageToSink::OnSuccess(msg) => msg,
        }
    }
}

pub(crate) struct BypassRouterConfig {
    bypass_conditions: BypassConditions,
    batch_size: usize,
    chunk_timeout: Duration,
}

impl BypassRouterConfig {
    pub(crate) fn new(
        bypass_conditions: BypassConditions,
        batch_size: usize,
        chunk_timeout: Duration,
    ) -> Self {
        Self {
            bypass_conditions,
            batch_size,
            chunk_timeout,
        }
    }
}

/// This enum is used to store the bypass conditions in a vector
/// so that it can be easily iterated over only the ones that were set.
#[derive(Clone)]
enum BypassConditionState {
    Sink(Box<ForwardConditions>),
    Fallback(Box<ForwardConditions>),
    OnSuccess(Box<ForwardConditions>),
}

/// [MvtxBypassRouter] is used by source and udf components for routing any bypassed messages to
/// different sinks based on bypass conditions.
#[derive(Clone)]
pub(crate) struct MvtxBypassRouter {
    bypass_tx: mpsc::Sender<MessageToSink>,
    bypass_conditions: Vec<BypassConditionState>,
}

impl MvtxBypassRouter {
    /// Initializes the bypass router as well as starts a tokio task for writing bypassed messages
    /// to different sinks based on bypass conditions.
    /// Returns the initialized bypass router and a join handle for the tokio task started
    /// to write bypassed messages to sink.
    pub(crate) async fn initialize(
        config: BypassRouterConfig,
        sink_writer: SinkWriter,
        cln_token: CancellationToken,
    ) -> (Self, error::Result<JoinHandle<error::Result<()>>>) {
        // Create bypass channels based on bypass conditions
        let (tx, rx) = mpsc::channel(config.batch_size);

        // Initialize the bypass router with the created channels
        let bypass_router = MvtxBypassRouter {
            bypass_tx: tx,
            bypass_conditions: MvtxBypassRouter::create_bypass_condition_state(
                config.bypass_conditions.clone(),
            ),
        };

        let bypass_receiver = BypassRouterReceiver {
            bypass_conditions: config.bypass_conditions.clone(),
            batch_size: config.batch_size,
            sink_writer,
            chunk_timeout: config.chunk_timeout,
            shutting_down_on_err: false,
            final_result: Ok(()),
        };

        let router_join_handle = bypass_receiver
            .streaming_bypass_write(ReceiverStream::new(rx), cln_token.clone())
            .await;

        (bypass_router, router_join_handle)
    }

    /// Checks if the message should be bypassed based on the bypass conditions and routes it to
    /// the appropriate sink.
    /// Returns a boolean wrapped in a Result. Returns Ok(true) if the message was bypassed,
    /// Ok(false) if the message was not bypassed, and Err if the messages supposed to be bypassed
    /// but there was an error in sending the message to the bypass channel.
    pub(crate) async fn try_bypass(&self, msg: Message) -> error::Result<bool> {
        for bypass_condition in self.bypass_conditions.clone() {
            match bypass_condition {
                BypassConditionState::Sink(sink) => {
                    if should_forward(msg.tags.clone(), Some(sink)) {
                        return self.route(MessageToSink::Primary(msg)).await.map(|_| true);
                    }
                }
                BypassConditionState::Fallback(fallback) => {
                    if should_forward(msg.tags.clone(), Some(fallback)) {
                        return self.route(MessageToSink::Fallback(msg)).await.map(|_| true);
                    }
                }
                BypassConditionState::OnSuccess(on_success) => {
                    if should_forward(msg.tags.clone(), Some(on_success)) {
                        return self
                            .route(MessageToSink::OnSuccess(msg))
                            .await
                            .map(|_| true);
                    }
                }
            }
        }
        Ok(false)
    }

    /// [route] method calls the bypass_tx send method.
    /// Returns a Result. Returns Ok(()) if the message was sent successfully,
    /// and Err if there was an error in sending the message to the bypass channel.
    async fn route(&self, msg: MessageToSink) -> error::Result<()> {
        self.bypass_tx.send(msg).await.map_err(|e| {
            Error::BypassRouter(format!("Failed to send message through bypass router: {e}"))
        })
    }

    /// Method to create the bypass condition state vector to be stored in the bypass router.
    /// Returns a vector of [BypassConditionState].
    fn create_bypass_condition_state(
        bypass_conditions: BypassConditions,
    ) -> Vec<BypassConditionState> {
        let mut bypass_condition_states = vec![];

        if let Some(sink) = bypass_conditions.sink {
            bypass_condition_states.push(BypassConditionState::Sink(sink));
        }
        if let Some(fallback) = bypass_conditions.fallback {
            bypass_condition_states.push(BypassConditionState::Fallback(fallback));
        }
        if let Some(on_success) = bypass_conditions.on_success {
            bypass_condition_states.push(BypassConditionState::OnSuccess(on_success));
        }

        bypass_condition_states
    }
}

/// [BypassRouterReceiver] starts the background task for receiving the bypassed messages from
/// the bypass channel and writing them to the different sinks.
///
/// [BypassRouterReceiver] allows creating separation of concern between the bypass router, which is
/// responsible for sending data to bypass channel and the bypass router receiver, which is
/// responsible for receiving the said data from bypass channel and sending it to the different
/// sinks in a tokio task.
struct BypassRouterReceiver {
    bypass_conditions: BypassConditions,
    batch_size: usize,
    sink_writer: SinkWriter,
    chunk_timeout: Duration,
    shutting_down_on_err: bool,
    final_result: error::Result<()>,
}

impl BypassRouterReceiver {
    /// Initializes the bypass router receiver and starts the background task for receiving the
    /// bypassed messages from the bypass channel and writing them to the different sinks.
    async fn streaming_bypass_write(
        mut self,
        messages_stream: ReceiverStream<MessageToSink>,
        cln_token: CancellationToken,
    ) -> error::Result<JoinHandle<error::Result<()>>> {
        Ok(tokio::spawn({
            async move {
                info!(?self.batch_size, ?self.chunk_timeout, "Starting sink writer in Bypass Mode");

                // Combine chunking and timeout into a stream
                let chunk_stream =
                    messages_stream.chunks_timeout(self.batch_size, self.chunk_timeout);
                pin!(chunk_stream);

                // Main processing loop
                while let Some(batch) = chunk_stream.next().await {
                    // filter out messages that are marked for drop
                    let batch: Vec<_> = batch
                        .into_iter()
                        .filter(|msg| !msg.inner().dropped())
                        .collect();

                    // skip if all were dropped
                    if batch.is_empty() {
                        continue;
                    }

                    let mut primary_messages: Vec<Message> = vec![];
                    let mut primary_ack_handles = vec![];
                    let mut fallback_messages: Vec<Message> = vec![];
                    let mut fallback_ack_handles = vec![];
                    let mut on_success_messages: Vec<Message> = vec![];
                    let mut on_success_ack_handles = vec![];

                    // Convert MessageToSink to Message and create respective
                    // vectors of Messages and ack handles
                    for msg in batch {
                        match msg {
                            MessageToSink::Primary(msg) => {
                                primary_ack_handles.push(msg.ack_handle.clone());
                                primary_messages.push(msg);
                            }
                            MessageToSink::Fallback(msg) => {
                                fallback_ack_handles.push(msg.ack_handle.clone());
                                fallback_messages.push(msg)
                            }
                            MessageToSink::OnSuccess(msg) => {
                                on_success_ack_handles.push(msg.ack_handle.clone());
                                on_success_messages.push(msg)
                            }
                        }
                    }

                    // we are in shutting down mode, we will not be writing to the sink,
                    // mark the messages as failed, and on Drop they will be nack'ed.
                    // If we're in shutdown mode, we don't want to continue to write to any of the sinks
                    if self.shutting_down_on_err {
                        self.mark_msgs_failed(&primary_messages);
                        self.mark_msgs_failed(&fallback_messages);
                        self.mark_msgs_failed(&on_success_messages);
                        continue;
                    }

                    // perform the primary write operation if sink condition exists
                    if self.bypass_conditions.sink.is_some() {
                        self.selective_write(
                            MessageToSink::Primary(Message::default()),
                            primary_messages,
                            primary_ack_handles,
                            cln_token.clone(),
                        )
                        .await?;
                    }

                    // we are in shutting down mode, we will not be writing to the sink,
                    // mark the messages as failed, and on Drop they will be nack'ed.
                    // If we're in shutdown mode, we don't want to continue to write to fallback and on-success sinks
                    if self.shutting_down_on_err {
                        self.mark_msgs_failed(&fallback_messages);
                        self.mark_msgs_failed(&on_success_messages);
                        continue;
                    }

                    // perform the fallback write operation if fallback sink condition exists
                    if self.bypass_conditions.fallback.is_some() {
                        self.selective_write(
                            MessageToSink::Fallback(Message::default()),
                            fallback_messages,
                            fallback_ack_handles,
                            cln_token.clone(),
                        )
                        .await?;
                    }

                    // we are in shutting down mode, we will not be writing to the sink,
                    // mark the messages as failed, and on Drop they will be nack'ed.
                    // If we're in shutdown mode, we don't want to continue to write to on-success sink
                    if self.shutting_down_on_err {
                        self.mark_msgs_failed(&on_success_messages);
                        continue;
                    }

                    // perform the on_success write operation if on-success condition exists
                    if self.bypass_conditions.on_success.is_some() {
                        self.selective_write(
                            MessageToSink::OnSuccess(Message::default()),
                            on_success_messages,
                            on_success_ack_handles,
                            cln_token.clone(),
                        )
                        .await?;
                    }
                }

                // finalize
                self.final_result
            }
        }))
    }

    /// Used to mark messages as failed. Used in [SinkWriter::streaming_bypass_write]
    /// when we are in shutting down mode. Added to reduce code redundancy.
    fn mark_msgs_failed(&self, batch: &Vec<Message>) {
        for msg in batch {
            msg.ack_handle
                .as_ref()
                .expect("ack handle should be present")
                .is_failed
                .store(true, Ordering::Relaxed);
        }
    }

    /// Writes messages to the appropriate sink based on the enum variant.
    /// Used in [SinkWriter::streaming_bypass_write] to remove redundant code.
    async fn selective_write(
        &mut self,
        msg_type: MessageToSink,
        batch: Vec<Message>,
        ack_handles: Vec<Option<Arc<AckHandle>>>,
        cln_token: CancellationToken,
    ) -> error::Result<()> {
        if let Err(e) = match msg_type {
            MessageToSink::Primary(_) => {
                self.sink_writer
                    .write_to_sink(batch, cln_token.clone())
                    .await
            }
            MessageToSink::Fallback(_) => {
                self.sink_writer
                    .write_to_fallback(batch, cln_token.clone())
                    .await
            }
            MessageToSink::OnSuccess(_) => {
                self.sink_writer
                    .write_to_on_success(batch, cln_token.clone())
                    .await
            }
        } {
            // critical error, cancel upstream and mark all acks as failed
            error!(?e, "Error writing to sink, initiating shutdown.");
            cln_token.cancel();

            for ack_handle in ack_handles {
                ack_handle
                    .as_ref()
                    .expect("ack handle should be present")
                    .is_failed
                    .store(true, Ordering::Relaxed);
            }

            self.final_result = Err(e);
            self.shutting_down_on_err = true;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::message::{IntOffset, MessageID, Offset, ReadAck};
    use bytes::Bytes;
    use chrono::Utc;
    use numaflow_models::models::{ForwardConditions, TagConditions};
    use std::collections::HashMap;
    use std::sync::Arc;
    use tokio::sync::oneshot;

    /// Creates a test message with optional tags and ack handle.
    fn create_test_message(
        id: i32,
        tags: Option<Vec<String>>,
        with_ack_handle: bool,
    ) -> (Message, Option<oneshot::Receiver<ReadAck>>) {
        let (ack_handle, ack_rx) = if with_ack_handle {
            let (tx, rx) = oneshot::channel();
            (Some(Arc::new(AckHandle::new(tx))), Some(rx))
        } else {
            (None, None)
        };

        let msg = Message {
            typ: Default::default(),
            keys: Arc::from(vec![format!("key_{}", id)]),
            tags: tags.map(|t| Arc::from(t)),
            value: Bytes::from(format!("message {}", id)),
            offset: Offset::Int(IntOffset::new(id as i64, 0)),
            event_time: Utc::now(),
            watermark: None,
            id: MessageID {
                vertex_name: "vertex".to_string().into(),
                offset: format!("offset_{}", id).into(),
                index: id,
            },
            headers: Arc::new(HashMap::new()),
            metadata: None,
            is_late: false,
            ack_handle,
        };
        (msg, ack_rx)
    }

    // ==================== MessageToSink Tests ====================

    #[test]
    fn test_message_to_sink_inner_primary() {
        let (msg, _) = create_test_message(1, None, false);
        let msg_to_sink = MessageToSink::Primary(msg.clone());
        assert_eq!(msg_to_sink.inner().id, msg.id);
        assert_eq!(msg_to_sink.inner().value, msg.value);
    }

    #[test]
    fn test_message_to_sink_inner_fallback() {
        let (msg, _) = create_test_message(2, None, false);
        let msg_to_sink = MessageToSink::Fallback(msg.clone());
        assert_eq!(msg_to_sink.inner().id, msg.id);
        assert_eq!(msg_to_sink.inner().value, msg.value);
    }

    #[test]
    fn test_message_to_sink_inner_on_success() {
        let (msg, _) = create_test_message(3, None, false);
        let msg_to_sink = MessageToSink::OnSuccess(msg.clone());
        assert_eq!(msg_to_sink.inner().id, msg.id);
        assert_eq!(msg_to_sink.inner().value, msg.value);
    }

    // ==================== BypassRouterConfig Tests ====================

    #[test]
    fn test_bypass_router_config_new() {
        let conditions = BypassConditions {
            sink: None,
            fallback: None,
            on_success: None,
        };
        let config = BypassRouterConfig::new(conditions.clone(), 100, Duration::from_secs(1));

        assert_eq!(config.batch_size, 100);
        assert_eq!(config.chunk_timeout, Duration::from_secs(1));
        assert_eq!(config.bypass_conditions, conditions);
    }
}
