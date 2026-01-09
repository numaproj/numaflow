//! The concept of bypass router is relevant only for MonoVertex. The bypass router allows sending
//! messages to different sinks directly from Source and UDF components of MonoVertex based on
//! bypass conditions.
//!
//! The bypass router contains information regarding the channels
//! to which bypass messages will be sent by the source and mapper, as well as the
//! sink writer handle using which bypass router will write messages to the respective sinks.
//!
//! The bypass router struct is initialized to be passed to the methods of source and mapper handles.
//!
//! The source and mapper handle methods use helper methods for bypass router to determine if a message
//! should be bypassed to a sink, and if so, the message is sent to one of the bypass channels held
//! by the router accordingly.
//!
//! The background router task is responsible for reading messages from the bypass channels and
//! writing them to the respective sinks.
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
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;
use tokio::pin;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_stream::StreamExt;
use tokio_stream::adapters::ChunksTimeout;
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

/// [BypassRouter] is used by source and udf components for routing any bypassed messages to
/// different sinks based on bypass conditions.
#[derive(Clone)]
pub(crate) struct BypassRouter {
    bypass_tx: mpsc::Sender<MessageToSink>,
    bypass_conditions: BypassConditions,
    batch_size: usize,
    sink_writer: SinkWriter,
    chunk_timeout: Duration,
    shutting_down_on_err: bool,
    final_result: error::Result<()>,
}

impl BypassRouter {
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
        let bypass_router = BypassRouter {
            bypass_tx: tx,
            bypass_conditions: config.bypass_conditions.clone(),
            batch_size: config.batch_size,
            sink_writer,
            chunk_timeout: config.chunk_timeout,
            shutting_down_on_err: false,
            final_result: Ok(()),
        };

        let router_join_handle = bypass_router
            .clone()
            .streaming_bypass_write(ReceiverStream::new(rx), cln_token.clone())
            .await;

        (bypass_router, router_join_handle)
    }

    pub(crate) fn get_routed_message(&self, msg: Message) -> Option<MessageToSink> {
        let sink_condition_exists = self.bypass_conditions.sink.is_some();
        let fallback_condition_exists = self.bypass_conditions.fallback.is_some();
        let on_success_condition_exists = self.bypass_conditions.on_success.is_some();

        if sink_condition_exists
            && should_forward(msg.tags.clone(), self.bypass_conditions.sink.clone())
        {
            Some(MessageToSink::Primary(msg))
        } else if fallback_condition_exists
            && should_forward(msg.tags.clone(), self.bypass_conditions.fallback.clone())
        {
            Some(MessageToSink::Fallback(msg))
        } else if on_success_condition_exists
            && should_forward(msg.tags.clone(), self.bypass_conditions.on_success.clone())
        {
            Some(MessageToSink::OnSuccess(msg))
        } else {
            None
        }
    }

    pub(crate) async fn route(&self, msg: MessageToSink) -> error::Result<()> {
        self.bypass_tx.send(msg).await.map_err(|e| {
            Error::BypassRouter(format!("Failed to send message through bypass router: {e}"))
        })
    }

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

    /// Helper method for marking messages as failed. Used in [SinkWriter::streaming_bypass_write]
    /// when we are in shutting down mode.
    fn mark_msgs_failed(&self, batch: &Vec<Message>) {
        for msg in batch {
            msg.ack_handle
                .as_ref()
                .expect("ack handle should be present")
                .is_failed
                .store(true, Ordering::Relaxed);
        }
    }

    /// Helper method for writing messages to the appropriate sink based on the enum variant.
    /// Used in [SinkWriter::streaming_bypass_write] to remove redundant code.
    async fn selective_write(
        &mut self,
        msg_type: MessageToSink,
        batch: Vec<Message>,
        ack_handles: Vec<Option<Arc<AckHandle>>>,
        cln_token: CancellationToken,
    ) -> error::Result<()> {
        if let Err(e) = match msg_type {
            MessageToSink::Primary(_) => self.sink_writer.write(batch, cln_token.clone()).await,
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
