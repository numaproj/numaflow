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

use crate::config::is_mono_vertex;
use crate::config::monovertex::BypassConditions;
use crate::error;
use crate::error::Error;
use crate::message::Message;
use crate::shared::forward::should_forward;
use crate::sinker::sink::{SinkWriter, send_drop_metrics};
use numaflow_models::models::ForwardConditions;
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
                    // we are in shutting down mode, we will not be writing to any sink,
                    // mark the messages as failed, and on Drop they will be nack'ed.
                    if self.shutting_down_on_err {
                        for msg in &batch {
                            match msg {
                                MessageToSink::Primary(msg)
                                | MessageToSink::Fallback(msg)
                                | MessageToSink::OnSuccess(msg) => msg.ack_handle.as_ref(),
                            }
                            .expect("ack handle should be present")
                            .is_failed
                            .store(true, Ordering::Relaxed);
                        }
                        continue;
                    }

                    let mut dropped_message_count = batch.len();
                    // filter out messages that are marked for drop
                    let batch: Vec<_> = batch
                        .into_iter()
                        .filter(|msg| !msg.inner().dropped())
                        .collect();
                    dropped_message_count -= batch.len();

                    // skip if all were dropped
                    if batch.is_empty() {
                        continue;
                    }

                    let mut primary_messages: Vec<Message> = vec![];
                    let mut fallback_messages: Vec<Message> = vec![];
                    let mut on_success_messages: Vec<Message> = vec![];
                    let mut ack_handles = vec![];

                    // Convert MessageToSink to Message and create respective
                    // vectors of Messages and ack handles
                    for msg in batch {
                        match msg {
                            MessageToSink::Primary(msg) => {
                                ack_handles.push(msg.ack_handle.clone());
                                primary_messages.push(msg);
                            }
                            MessageToSink::Fallback(msg) => {
                                ack_handles.push(msg.ack_handle.clone());
                                fallback_messages.push(msg)
                            }
                            MessageToSink::OnSuccess(msg) => {
                                ack_handles.push(msg.ack_handle.clone());
                                on_success_messages.push(msg)
                            }
                        }
                    }

                    if let Err(e) = self
                        .perform_write(
                            primary_messages,
                            fallback_messages,
                            on_success_messages,
                            cln_token.clone(),
                        )
                        .await
                    {
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
                    send_drop_metrics(is_mono_vertex(), dropped_message_count);
                }

                // finalize
                self.final_result
            }
        }))
    }

    /// Call the different write methods on the sink writer for different types of message vectors.
    /// If any of the write methods fail, return immediately, ack_handles are marked as failed in the
    /// caller.
    async fn perform_write(
        &mut self,
        primary_messages: Vec<Message>,
        fallback_messages: Vec<Message>,
        ons_messages: Vec<Message>,
        cln_token: CancellationToken,
    ) -> error::Result<()> {
        self.sink_writer
            .write_to_sink(primary_messages, cln_token.clone())
            .await?;
        self.sink_writer
            .write_to_fallback(fallback_messages, cln_token.clone())
            .await?;
        self.sink_writer
            .write_to_on_success(ons_messages, cln_token)
            .await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::message::{AckHandle, IntOffset, MessageID, Offset, ReadAck};
    use crate::message::{IntOffset, MessageID, Offset, ReadAck};
    use crate::shared::grpc::create_rpc_channel;
    use crate::sinker::sink::{SinkClientType, SinkWriterBuilder};
    use crate::tracker::Tracker;
    use bytes::Bytes;
    use chrono::Utc;
    use numaflow::shared::{DROP, ServerExtras};
    use numaflow::sink;
    use numaflow_models::models::{ForwardConditions, TagConditions};
    use numaflow_pb::clients::sink::sink_client::SinkClient;
    use sink::Server;
    use std::collections::HashMap;
    use std::sync::Arc;
    use tokio::sync::mpsc::Receiver;
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

    struct PanicSink;
    #[tonic::async_trait]
    impl sink::Sinker for PanicSink {
        async fn sink(&self, mut input: Receiver<sink::SinkRequest>) -> Vec<sink::Response> {
            while let Some(_) = input.recv().await {
                panic!("This sink can't receive messages, received one, so panicking now");
            }
            vec![]
        }
    }

    #[tokio::test]
    /// Tests the streaming bypass write functionality where all the messages are dropped
    /// Defines a user defined sink that panics on receiving any message.
    /// All messages are marked to be dropped, hence the sink shouldn't receive any messages.
    async fn test_dropped_streaming_bypass_write() {
        let cln_token = CancellationToken::new();
        let tracker = Tracker::new(None, cln_token.clone());
        let batch_size = 10;

        let sink_tags = vec!["sink".to_string()];
        let fallback_tags = vec!["fallback".to_string()];
        let on_success_tags = vec!["on_success".to_string()];
        let conditions = BypassConditions {
            sink: None,
            fallback: Some(Box::new(ForwardConditions::new(TagConditions {
                values: fallback_tags.clone(),
                operator: Some("or".to_string()),
            }))),
            on_success: Some(Box::new(ForwardConditions::new(TagConditions {
                values: on_success_tags.clone(),
                operator: Some("or".to_string()),
            }))),
        };
        let bypass_router_config =
            BypassRouterConfig::new(conditions, batch_size, Duration::from_millis(1000));

        // start the server
        let (_shutdown_tx, shutdown_rx) = oneshot::channel();
        let tmp_dir = tempfile::TempDir::new().unwrap();
        let sock_file = tmp_dir.path().join("sink.sock");
        let server_info_file = tmp_dir.path().join("sink-server-info");

        let server_socket = sock_file.clone();
        let _server_handle = tokio::spawn(async move {
            Server::new(PanicSink)
                .with_socket_file(server_socket)
                .with_server_info_file(server_info_file)
                .start_with_shutdown(shutdown_rx)
                .await
                .expect("failed to start sink server");
        });

        let sink_writer = SinkWriterBuilder::new(
            batch_size,
            Duration::from_millis(100),
            SinkClientType::UserDefined(SinkClient::new(
                create_rpc_channel(sock_file).await.unwrap(),
            )),
        )
        .build()
        .await
        .unwrap();

        let (router, handle) = MvtxBypassRouter::initialize(
            bypass_router_config,
            sink_writer.clone(),
            cln_token.clone(),
        )
        .await;

        let mut ack_rxs = vec![];
        let messages: Vec<Message> = (0..10)
            .map(|i| {
                let (ack_tx, ack_rx) = oneshot::channel();
                ack_rxs.push(ack_rx);
                Message {
                    typ: Default::default(),
                    keys: Arc::from(vec![format!("key_{}", i)]),
                    tags: Some(Arc::from(vec![DROP.to_string()])),
                    value: format!("message {}", i).as_bytes().to_vec().into(),
                    offset: Offset::Int(IntOffset::new(i, 0)),
                    event_time: Utc::now(),
                    watermark: None,
                    id: MessageID {
                        vertex_name: "vertex".to_string().into(),
                        offset: format!("offset_{}", i).into(),
                        index: i as i32,
                    },
                    ack_handle: Some(Arc::new(AckHandle::new(ack_tx))),
                    ..Default::default()
                }
            })
            .collect();

        for msg in messages {
            let _ = router.bypass_tx.send(MessageToSink::Primary(msg)).await;
        }

        drop(router);

        let _ = handle.unwrap().await;
        _shutdown_tx.send(()).unwrap();

        for ack_rx in ack_rxs {
            assert_eq!(ack_rx.await.unwrap(), ReadAck::Ack);
        }
        // check if the tracker is empty
        assert!(tracker.is_empty().await.unwrap());
    }
}
