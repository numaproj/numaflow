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

impl MessageToSink {
    pub fn inner(&self) -> &Message {
        match self {
            MessageToSink::Primary(msg)
            | MessageToSink::Fallback(msg)
            | MessageToSink::OnSuccess(msg) => msg,
        }
    }
}

/// Splitter is a component that splits the input stream based on the bypass conditions.
/// In the case of when bypass conditions are specified in the monovertex spec,
/// the splitter component will be run after every component starting from the source and except for the sink.
/// Eg: If both Source with SourceTransformer and UDF components are specified, then splitter will be run after
/// both Source and UDF.
pub(crate) struct Splitter {
    pub(crate) batch_size: usize,
    chunk_timeout: Duration,
    bypass_conditions: BypassConditions,
}

impl Splitter {
    /// Creates a new splitter instance.
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

    /// The splitter's `run` method takes an `input_stream` and a `bypass_tx` channel as arguments.
    /// The input stream is the stream of original messages sent from either the source or the udf present before the splitter.
    /// The splitter reads the `input_stream` and when the bypass condition is applicable on the message read,
    /// it sends the message to the `bypass_tx` channel wrapped in `MessageToSink` enum.
    /// Otherwise, it returns a stream to read the messages not matching the bypass conditions.
    ///
    /// TODO: use cancellation tokens for shutting down the spawned task
    pub(crate) async fn run(
        &self,
        input_stream: ReceiverStream<Message>,
        bypass_tx: Sender<MessageToSink>,
    ) -> error::Result<(ReceiverStream<Message>, JoinHandle<error::Result<()>>)> {
        let (message_tx, message_rx) = mpsc::channel(self.batch_size);
        let bypass_conditions = self.bypass_conditions.clone();
        let handle = tokio::spawn(async move {
            let mut input_stream = input_stream;
            let sink_condition_exists = bypass_conditions.sink.is_some();
            let fallback_condition_exists = bypass_conditions.fallback.is_some();
            let on_success_condition_exists = bypass_conditions.on_success.is_some();
            while let Some(msg) = input_stream.next().await {
                if sink_condition_exists
                    && should_forward(msg.tags.clone(), bypass_conditions.sink.clone())
                {
                    bypass_tx
                        .send(MessageToSink::Primary(msg))
                        .await
                        .map_err(|e| {
                            Error::Forwarder(format!(
                                "Error while sending message to bypass channel: {e:?}"
                            ))
                        })?;
                } else if fallback_condition_exists
                    && should_forward(msg.tags.clone(), bypass_conditions.fallback.clone())
                {
                    bypass_tx
                        .send(MessageToSink::Fallback(msg))
                        .await
                        .map_err(|e| {
                            Error::Forwarder(format!(
                                "Error while sending message to bypass channel: {e:?}"
                            ))
                        })?;
                } else if on_success_condition_exists
                    && should_forward(msg.tags.clone(), bypass_conditions.on_success.clone())
                {
                    bypass_tx
                        .send(MessageToSink::OnSuccess(msg))
                        .await
                        .map_err(|e| {
                            Error::Forwarder(format!(
                                "Error while sending message to bypass channel: {e:?}"
                            ))
                        })?;
                } else {
                    message_tx.send(msg).await.map_err(|e| {
                        Error::Forwarder(format!(
                            "Error while sending message to message channel: {e:?}"
                        ))
                    })?;
                }
            }
            Ok(())
        });

        Ok((ReceiverStream::new(message_rx), handle))
    }

    /// Converts a normal message stream into conditioned message stream in one of the following ways:
    /// - If bypass condition for primary sink is present, it writes no messages to bypass_tx
    /// - If bypass condition for primary sink is absent, it writes all messages to bypass_tx
    ///
    /// The converter ingests the input stream from the last splitter's run method in the component chain.
    pub(crate) async fn converter(
        &self,
        input_stream: ReceiverStream<Message>,
        bypass_tx: Sender<MessageToSink>,
    ) -> error::Result<JoinHandle<error::Result<()>>> {
        let mut input_stream = input_stream;
        let handle = match self.bypass_conditions.sink {
            // If bypass condition for primary sink is absent, all the messages are wrapped in
            // MessageToSink::Primary and sent to `bypass_tx`.
            // This is done since the bypass spec is common across all components.
            // So, let's say, if the user only wants to short-circuit to fallback sink, we shouldn't
            // drop the messages which are on the normal route to primary sink.
            None => tokio::spawn(async move {
                while let Some(msg) = input_stream.next().await {
                    bypass_tx
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
            Some(_) => tokio::spawn(async move {
                while let Some(_) = input_stream.next().await {
                    // TODO: determine what to do with the message
                    continue;
                }
                Ok(())
            }),
        };
        Ok(handle)
    }
}
