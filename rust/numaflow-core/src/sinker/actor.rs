use backoff::strategy::exponential::Exponential;
use std::collections::HashMap;
use tokio::sync::mpsc::Receiver;
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;
use tracing::warn;

use crate::Result;
use crate::config::components::sink::{OnFailureStrategy, RetryConfig};
use crate::message::Message;
use crate::sinker::sink::{ResponseStatusFromSink, Sink};

/// Response from the sink actor containing categorized messages
#[derive(Default)]
pub(super) struct SinkActorResponse {
    pub(super) success: Vec<Message>,
    pub(super) failed: Vec<Message>,
    pub(super) fallback: Vec<Message>,
    pub(super) serving: Vec<Message>,
    pub(super) dropped: Vec<Message>,
}

/// SinkActorMessage is a message that is sent to the SinkActor.
pub(super) struct SinkActorMessage {
    pub(super) messages: Vec<Message>,
    pub(super) respond_to: oneshot::Sender<Result<SinkActorResponse>>,
    pub(super) cancel: CancellationToken,
}

/// SinkActor is an actor that handles messages sent to the Sink.
pub(super) struct SinkActor<T> {
    actor_messages: Receiver<SinkActorMessage>,
    sink: T,
    retry_config: RetryConfig,
}

impl<T> SinkActor<T>
where
    T: Sink,
{
    pub(super) fn new(
        actor_messages: Receiver<SinkActorMessage>,
        sink: T,
        retry_config: RetryConfig,
    ) -> Self {
        Self {
            actor_messages,
            sink,
            retry_config,
        }
    }

    /// Invokes the sink and retries only failed messages until success or retry limit.
    /// Returns categorized messages: success, failed, fallback, serving, and dropped.
    async fn write_messages_with_retry(
        &mut self,
        messages: Vec<Message>,
        cancel: &CancellationToken,
    ) -> Result<SinkActorResponse> {
        if messages.is_empty() {
            return Ok(SinkActorResponse::default());
        }

        // State to accumulate outcomes across retries
        let mut messages_to_retry = messages.clone();
        let mut success_msgs = Vec::new();
        let mut fallback_msgs = Vec::new();
        let mut serving_msgs = Vec::new();
        let mut dropped_msgs = Vec::new();

        // Build backoff iterator from retry config
        let backoff = Exponential::from_millis(
            self.retry_config.sink_initial_retry_interval_in_ms,
            self.retry_config.sink_max_retry_interval_in_ms,
            self.retry_config.sink_retry_factor,
            self.retry_config.sink_retry_jitter,
            Some(self.retry_config.sink_max_retry_attempts),
        );

        // Manual retry loop with backoff
        let mut backoff_iter = backoff.into_iter();

        loop {
            // send batch to sink
            let responses = self.sink.sink(messages_to_retry.clone()).await?;

            // Create a map of id to result
            let result_map = responses
                .into_iter()
                .map(|resp| (resp.id, resp.status))
                .collect::<HashMap<_, _>>();

            // Classify messages based on responses
            let mut failed_ids = Vec::new();

            messages_to_retry.retain(|msg| {
                match result_map.get(&msg.id.to_string()) {
                    Some(ResponseStatusFromSink::Success) => {
                        success_msgs.push(msg.clone());
                        false // remove from retry list
                    }
                    Some(ResponseStatusFromSink::Failed(_)) => {
                        failed_ids.push(msg.id.to_string());
                        true // keep for retry
                    }
                    Some(ResponseStatusFromSink::Fallback) => {
                        fallback_msgs.push(msg.clone());
                        false // remove from retry list
                    }
                    Some(ResponseStatusFromSink::Serve) => {
                        serving_msgs.push(msg.clone());
                        false // remove from retry list
                    }
                    None => false, // remove if no response
                }
            });

            if messages_to_retry.is_empty() {
                // success path, all messages processed
                break;
            }

            // Check if we should retry
            if cancel.is_cancelled() {
                warn!("Cancellation received, stopping retry loop");
                // Remaining messages are failed
                return Ok(SinkActorResponse {
                    success: success_msgs,
                    failed: messages_to_retry,
                    fallback: fallback_msgs,
                    serving: serving_msgs,
                    dropped: dropped_msgs,
                });
            }

            // Get next backoff delay
            if let Some(delay) = backoff_iter.next() {
                warn!(
                    remaining = messages_to_retry.len(),
                    delay_ms = delay.as_millis(),
                    "Retrying failed messages"
                );
                tokio::time::sleep(delay).await;
            } else {
                match self.retry_config.sink_retry_on_fail_strategy {
                    OnFailureStrategy::Fallback => {
                        fallback_msgs.append(&mut messages_to_retry);
                    }
                    OnFailureStrategy::Drop => {
                        dropped_msgs.append(&mut messages_to_retry);
                    }
                    _ => {}
                }
                // No more retries, remaining messages are failed
                warn!(remaining = messages_to_retry.len(), "Retries exhausted");
                break;
            }
        }

        Ok(SinkActorResponse {
            success: success_msgs,
            failed: messages_to_retry,
            fallback: fallback_msgs,
            serving: serving_msgs,
            dropped: dropped_msgs,
        })
    }

    async fn handle_message(&mut self, msg: SinkActorMessage) {
        let result = self
            .write_messages_with_retry(msg.messages, &msg.cancel)
            .await;
        let _ = msg.respond_to.send(result);
    }

    pub(super) async fn run(mut self) {
        while let Some(msg) = self.actor_messages.recv().await {
            self.handle_message(msg).await;
        }
    }
}
