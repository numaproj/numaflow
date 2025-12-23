//! Bypass is responsible for routing the input stream messages based on the bypass conditions.
//! The bypass conditions lets you route the messages from source or UDF to one of the sinks based on
//! the tags.
//! The message that is sent to the primary sink can still be directed to fallback or on-success sink
//! based on the ResponseType returned by the sink (the original behavior).

use crate::config::monovertex::BypassConditions;
use crate::error;
use crate::error::Error;
use crate::message::Message;
use crate::shared::forward::should_forward;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::sync::mpsc::Sender;
use tokio::task::JoinHandle;
use tokio_stream::StreamExt;
use tokio_stream::wrappers::ReceiverStream;

/// Enum to represent the different types of messages to be sent to the sink.
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

/// Router is a component that routes the input stream based on the bypass conditions.
/// In the case of when bypass conditions are specified in the monovertex spec,
/// After every component starting from the source and except for the sink, the router will inspect
/// the message to see whether it can be short-circuited (routed) to one of the sinks.
/// Eg: If both Source with SourceTransformer and UDF components are specified, then router will try to route
/// the messages from output of Source and UDF.
pub(crate) struct Router {
    pub(crate) batch_size: usize,
    chunk_timeout: Duration,
    bypass_conditions: BypassConditions,
}

impl Router {
    /// Creates a new [Router] instance.
    pub(crate) fn new(
        batch_size: usize,
        chunk_timeout: Duration,
        bypass_conditions: BypassConditions,
    ) -> Self {
        Self {
            batch_size,
            chunk_timeout,
            bypass_conditions,
        }
    }

    /// The Router's `route` method takes an `input_stream` and a `sink_tx` channel as arguments.
    /// The input stream is the stream of original messages sent from either the Source or the UDF.
    /// The Router reads the `input_stream` and when the bypass condition is applicable on the message read,
    /// it sends the message to the `sink_tx` channel wrapped in [MessageToSink].
    /// Otherwise, it returns a stream to read the messages not matching the bypass conditions.
    pub(crate) async fn route(
        &self,
        input_stream: ReceiverStream<Message>,
        sink_tx: Sender<MessageToSink>,
    ) -> error::Result<(ReceiverStream<Message>, JoinHandle<error::Result<()>>)> {
        let (message_tx, message_rx) = mpsc::channel(self.batch_size);
        let bypass_conditions = self.bypass_conditions.clone();
        let batch_size = self.batch_size;
        let chunk_timeout = self.chunk_timeout;
        let handle = tokio::spawn(async move {
            let sink_condition_exists = bypass_conditions.sink.is_some();
            let fallback_condition_exists = bypass_conditions.fallback.is_some();
            let on_success_condition_exists = bypass_conditions.on_success.is_some();

            // Read from a chunked stream of messages
            let chunked_stream = input_stream.chunks_timeout(batch_size, chunk_timeout);
            tokio::pin!(chunked_stream);

            while let Some(msgs) = chunked_stream.next().await {
                // for each message read from the chunked stream determine which sink it should be sent to
                // based on the bypass conditions and wrap it in respective MessageToSink enum value
                for msg in msgs {
                    let msg_clone = msg.clone();
                    let message_to_sink = if sink_condition_exists
                        && should_forward(msg.tags.clone(), bypass_conditions.sink.clone())
                    {
                        Some(MessageToSink::Primary(msg_clone))
                    } else if fallback_condition_exists
                        && should_forward(msg.tags.clone(), bypass_conditions.fallback.clone())
                    {
                        Some(MessageToSink::Fallback(msg_clone))
                    } else if on_success_condition_exists
                        && should_forward(msg.tags.clone(), bypass_conditions.on_success.clone())
                    {
                        Some(MessageToSink::OnSuccess(msg_clone))
                    } else {
                        None
                    };

                    // FIXME: nack the messages if send fails?
                    match message_to_sink {
                        Some(msg_to_sink) => {
                            sink_tx.send(msg_to_sink).await.map_err(|e| {
                                Error::Forwarder(format!(
                                    "Error while sending message to bypass channel: {e:?}"
                                ))
                            })?;
                        }
                        None => {
                            message_tx.send(msg).await.map_err(|e| {
                                Error::Forwarder(format!(
                                    "Error while sending message to message channel: {e:?}"
                                ))
                            })?;
                        }
                    }
                }
            }
            Ok(())
        });

        Ok((ReceiverStream::new(message_rx), handle))
    }

    /// At the end of the component chain and their subsequent routing calls, there could be some
    /// messages left in the input stream. This could be because either they didn't match the
    /// bypass conditions for primary sink or the bypass conditions for primary sink are absent.
    ///
    /// The [Router::process_non_routable_msgs] method hence performs either one of the following actions:
    /// - If bypass condition for primary sink is absent, it writes all messages to the primary sink via sink_tx.
    /// - If bypass condition for primary sink is present, it should already be written to the primary sink (via sink_tx).
    ///   So this method simply drops these messages from the stream.
    ///
    /// This method ingests the input stream after the last [Router::route] call.
    pub(crate) async fn process_non_routable_msgs(
        &self,
        input_stream: ReceiverStream<Message>,
        sink_tx: Sender<MessageToSink>,
    ) -> error::Result<JoinHandle<error::Result<()>>> {
        let mut input_stream = input_stream;
        let handle = match self.bypass_conditions.sink {
            // If bypass condition for primary sink is absent, all the messages are wrapped in
            // MessageToSink::Primary and sent to `sink_tx`.
            // This is done since the bypass spec is common across all components.
            // So, let's say, if the user only wants to short-circuit to fallback sink, we shouldn't
            // drop the messages which are on the normal route to primary sink.
            None => tokio::spawn(async move {
                while let Some(msg) = input_stream.next().await {
                    sink_tx
                        .send(MessageToSink::Primary(msg))
                        .await
                        .map_err(|e| {
                            Error::Forwarder(format!(
                                "Error while sending message to bypass channel: {e:?}"
                            ))
                        })?;
                }
                Ok(())
            }),
            // If bypass condition for primary sink is present, the pending messages in the input stream
            // should've already been written to the primary sink (via sink_tx). If they don't conform to
            // bypass conditions for primary sink, they will be simply dropped here.
            Some(_) => tokio::spawn(async move {
                while let Some(_) = input_stream.next().await {
                    // FIXME: determine what to do with the message
                    //   We can drop the message, but we have to make sure it gets acked by the source.
                    //   We have to log it and also have a metric with tag: "not_routable"
                    continue;
                }
                Ok(())
            }),
        };
        Ok(handle)
    }
}
