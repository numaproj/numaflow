use crate::config::monovertex::BypassConditions;
use crate::error;
use crate::error::Error;
use crate::message::Message;
use crate::shared::forward::should_forward;
use numaflow_models::models::MonoVertexBypassCondition;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::sync::mpsc::Sender;
use tokio::task::JoinHandle;
use tokio_stream::StreamExt;
use tokio_stream::wrappers::ReceiverStream;

/// Enum to represent the different types of
#[derive(Debug, Clone)]
pub enum MessageToSink {
    Primary(Message),
    Fallback(Message),
    OnSuccess(Message),
}

struct Splitter {
    batch_size: usize,
    chunk_timeout: Duration,
    bypass_conditions: BypassConditions,
}

impl Splitter {
    async fn new(
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

    async fn run(
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
}
