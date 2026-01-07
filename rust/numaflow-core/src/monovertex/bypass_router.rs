//! The concept of bypass router is relevant only for MonoVertex. The bypass router allows sending
//! messages to different sinks directly from Source and UDF components of MonoVertex based on
//! bypass conditions.
//!
//! The bypass manager is responsible for initializing the bypass router and starting the
//! background router task. The bypass router contains information regarding the channels
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
//! |    |  Source   |  ----->  |   Map (optional)   |  ----->  |  Sink v2  |  |
//! |    |           |          |                    |          |           |  |
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
use crate::message::Message;
use crate::shared::forward::should_forward;
use crate::sinker::sink::SinkWriter;
use numaflow_models::models::ForwardConditions;
use tokio::sync::mpsc;
use tokio::sync::mpsc::Receiver;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;

pub(crate) enum BypassSink {
    Sink,
    Fallback,
    OnSuccess,
}

/// [BypassRouterManager] is responsible for initializing the bypass router and starting the
/// background router task.
pub(crate) struct BypassRouterManager {
    bypass_conditions: Option<BypassConditions>,
    batch_size: usize,
    sink_handle: SinkWriter,
    cln_token: CancellationToken,
}

impl BypassRouterManager {
    /// Initializes the bypass router manager.
    pub(crate) fn new(
        bypass_conditions: Option<BypassConditions>,
        batch_size: usize,
        sink_handle: SinkWriter,
        cln_token: CancellationToken,
    ) -> Option<Self> {
        Some(Self {
            bypass_conditions,
            batch_size,
            sink_handle,
            cln_token,
        })
    }

    /// Initializes the bypass router as well as starts a tokio task for writing bypassed messages
    /// to different sinks based on bypass conditions.
    /// Returns the initialized bypass router and a join handle for the tokio task started
    /// to write bypassed messages to sinks.
    pub(crate) async fn start(
        self,
    ) -> (
        Option<BypassRouter>,
        error::Result<JoinHandle<error::Result<()>>>,
    ) {
        match self.bypass_conditions {
            Some(ref bypass_conditions) => {
                let (sink_tx, sink_rx) = self.create_bypass_channels(&bypass_conditions.sink);
                let (fallback_tx, fallback_rx) =
                    self.create_bypass_channels(&bypass_conditions.fallback);
                let (on_success_tx, on_success_rx) =
                    self.create_bypass_channels(&bypass_conditions.on_success);

                let bypass_router = BypassRouter::new(
                    sink_tx,
                    fallback_tx,
                    on_success_tx,
                    bypass_conditions.clone(),
                    self.sink_handle,
                );

                let router_join_handle = bypass_router
                    .start(sink_rx, fallback_rx, on_success_rx, self.cln_token)
                    .await;
                (Some(bypass_router), router_join_handle)
            }
            None => (None, Ok(tokio::task::spawn(async { Ok(()) }))),
        }
    }

    /// Helper function to create tx, rx channels based on presence of bypass condition
    /// Added to reduce redundant code
    fn create_bypass_channels(
        &self,
        bypass_condition: &Option<Box<ForwardConditions>>,
    ) -> (
        Option<mpsc::Sender<Message>>,
        Option<mpsc::Receiver<Message>>,
    ) {
        match bypass_condition {
            None => (None, None),
            Some(_) => {
                let (tx, rx) = mpsc::channel(self.batch_size);
                (Some(tx), Some(rx))
            }
        }
    }
}

/// [BypassRouter] is used by source and udf components for routing any bypassed messages to
/// different sinks based on bypass conditions.
#[derive(Clone)]
pub(crate) struct BypassRouter {
    sink: Option<mpsc::Sender<Message>>,
    fallback: Option<mpsc::Sender<Message>>,
    on_success: Option<mpsc::Sender<Message>>,
    bypass_conditions: BypassConditions,
    sink_handle: SinkWriter,
}

impl BypassRouter {
    /// Returns the channel to send the bypassed message in case of bypass conditions match.
    /// If no bypass conditions match, returns None.
    pub(crate) fn get_bypass_channel(&self, msg: Message) -> Option<mpsc::Sender<Message>> {
        let sink_condition_exists = self.bypass_conditions.sink.is_some();
        let fallback_condition_exists = self.bypass_conditions.fallback.is_some();
        let on_success_condition_exists = self.bypass_conditions.on_success.is_some();

        if sink_condition_exists
            && should_forward(msg.tags.clone(), self.bypass_conditions.sink.clone())
        {
            self.sink.clone()
        } else if fallback_condition_exists
            && should_forward(msg.tags.clone(), self.bypass_conditions.fallback.clone())
        {
            self.fallback.clone()
        } else if on_success_condition_exists
            && should_forward(msg.tags.clone(), self.bypass_conditions.on_success.clone())
        {
            self.on_success.clone()
        } else {
            None
        }
    }

    /// Initializes the bypass router.
    fn new(
        sink: Option<mpsc::Sender<Message>>,
        fallback: Option<mpsc::Sender<Message>>,
        on_success: Option<mpsc::Sender<Message>>,
        bypass_conditions: BypassConditions,
        sink_handle: SinkWriter,
    ) -> Self {
        Self {
            sink,
            fallback,
            on_success,
            bypass_conditions,
            sink_handle,
        }
    }

    /// Starts a tokio task to write bypassed messages to different sinks based on bypass conditions.
    /// Returns a tokio join handle for the task.
    async fn start(
        &self,
        sink_rx: Option<Receiver<Message>>,
        fallback_rx: Option<Receiver<Message>>,
        on_success_rx: Option<Receiver<Message>>,
        cln_token: CancellationToken,
    ) -> error::Result<JoinHandle<error::Result<()>>> {
        let sink_join_handle = self
            .get_bypass_write_handle(sink_rx, BypassSink::Sink, cln_token.clone())
            .await?;
        let fallback_join_handle = self
            .get_bypass_write_handle(fallback_rx, BypassSink::Fallback, cln_token.clone())
            .await?;
        let onsuccess_join_handle = self
            .get_bypass_write_handle(on_success_rx, BypassSink::OnSuccess, cln_token.clone())
            .await?;

        // Join the mapper and second splitter handle and returns a single handle
        let joined_handle: JoinHandle<error::Result<()>> = tokio::spawn(async move {
            let (sink_result, fallback_result, onsuccess_result) = tokio::try_join!(
                sink_join_handle,
                fallback_join_handle,
                onsuccess_join_handle
            )
            .map_err(|e| {
                error!(
                    ?e,
                    "Error while joining primary, fallback and on-success bypass handles"
                );
                Error::Forwarder(format!(
                    "Error while joining primary, fallback and on-success bypass handles: {e:?}"
                ))
            })?;

            sink_result.inspect_err(|e| {
                error!(?e, "Error while writing bypass messages to sink");
            })?;

            fallback_result.inspect_err(|e| {
                error!(?e, "Error while writing bypass messages to fallback sink");
            })?;

            onsuccess_result.inspect_err(|e| {
                error!(?e, "Error while writing bypass messages to on-success sink");
            })?;

            Ok(())
        });

        Ok(joined_handle)
    }

    /// Helper function for [start_router_receiver] to start a tokio task to write bypassed messages to a specific sink.
    async fn get_bypass_write_handle(
        &self,
        rx: Option<Receiver<Message>>,
        bypass_sink: BypassSink,
        cln_token: CancellationToken,
    ) -> error::Result<JoinHandle<error::Result<()>>> {
        if let Some(rx) = rx {
            self.sink_handle
                .clone()
                .streaming_bypass_write(ReceiverStream::new(rx), bypass_sink, cln_token.clone())
                .await
        } else {
            Ok(tokio::task::spawn(async { Ok(()) }))
        }
    }
}
