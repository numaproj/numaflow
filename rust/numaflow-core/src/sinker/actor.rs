use crate::Result;
use crate::config::components::sink::{OnFailureStrategy, RetryConfig};
use crate::message::Message;
use crate::metadata::Metadata;
use crate::sinker::sink::{ResponseStatusFromSink, Sink};
use backoff::strategy::exponential::Exponential;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc::Receiver;
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;
use tracing::warn;

/// Response from the sink actor containing categorized messages
#[derive(Default)]
pub(super) struct SinkActorResponse {
    pub(super) failed: Vec<Message>,
    pub(super) fallback: Vec<Message>,
    pub(super) serving: Vec<Message>,
    pub(super) dropped: Vec<Message>,
    pub(super) on_success: Vec<Message>,
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
        let mut fallback_messages = Vec::new();
        let mut serving_messages = Vec::new();
        let mut dropped_messages = Vec::new();
        let mut on_success_messages = Vec::new();

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
        let mut retry_attempt = 0;
        let mut error_map = HashMap::new();

        loop {
            // send batch to sink
            let responses = self.sink.sink(messages_to_retry.clone()).await?;

            // Create a map of id to result
            let mut result_map = responses
                .into_iter()
                .map(|resp| (resp.id, resp.status))
                .collect::<HashMap<_, _>>();

            // Classify messages based on responses
            let mut failed_ids = Vec::new();

            messages_to_retry.retain_mut(|msg| {
                match result_map.remove(&msg.id.to_string()) {
                    Some(ResponseStatusFromSink::Success) => {
                        false // remove from retry list
                    }
                    Some(ResponseStatusFromSink::Failed(err_msg)) => {
                        failed_ids.push(msg.id.to_string());
                        *error_map.entry(err_msg.clone()).or_insert(0) += 1;
                        true // keep for retry
                    }
                    Some(ResponseStatusFromSink::Fallback) => {
                        fallback_messages.push(msg.clone());
                        false // remove from retry list
                    }
                    Some(ResponseStatusFromSink::Serve(serve_response)) => {
                        if let Some(serve_response) = serve_response {
                            msg.value = serve_response.into();
                        }
                        serving_messages.push(msg.clone());
                        false // remove from retry list
                    }
                    Some(ResponseStatusFromSink::OnSuccess(on_success_msg)) => {
                        if let Some(on_success_msg) = on_success_msg {
                            let on_success_md: Option<Metadata> =
                                on_success_msg.metadata.map(|md| md.into());
                            let new_md = match &mut msg.metadata {
                                // Following clones are required explicitly since Arc doesn't allow
                                // interior mutability, so we cannot move the required fields out of Arc
                                // without cloning, unless we can guarantee there is only a single reference to Arc
                                // TODO: Explore safety of Arc::get_mut usage here to avoid explicit cloning.
                                Some(prev_md) => Metadata {
                                    user_metadata: on_success_md
                                        .map_or(prev_md.user_metadata.clone(), |md| {
                                            md.user_metadata
                                        }),
                                    sys_metadata: prev_md.sys_metadata.clone(),
                                    previous_vertex: prev_md.previous_vertex.clone(),
                                },
                                None => Metadata {
                                    user_metadata: on_success_md
                                        .map_or(HashMap::new(), |md| md.user_metadata),
                                    ..Default::default()
                                },
                            };
                            let new_msg = Message {
                                value: on_success_msg.value.into(),
                                keys: on_success_msg.keys.into(),
                                metadata: Some(Arc::new(new_md)),
                                ..msg.clone()
                            };
                            on_success_messages.push(new_msg.clone());
                        } else {
                            // Send the original message if no payload was provided to the onSuccess sink
                            on_success_messages.push(msg.clone());
                        }
                        false // remove from retry list
                    }
                    None => unreachable!("should have response for all messages"), // remove if no response
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
                    failed: messages_to_retry,
                    fallback: fallback_messages,
                    serving: serving_messages,
                    dropped: dropped_messages,
                    on_success: on_success_messages,
                });
            }

            // sleep for the next backoff duration
            match backoff_iter.next() {
                Some(delay) => {
                    retry_attempt += 1;
                    warn!(
                        ?retry_attempt,
                        ?error_map,
                        "Retrying due to retryable error."
                    );
                    error_map.clear();
                    tokio::time::sleep(delay).await;
                }
                None => {
                    // retries exhausted, handle the remaining messages based on the strategy
                    match self.retry_config.sink_retry_on_fail_strategy {
                        // if strategy is fallback, move the remaining messages to fallback_messages
                        // so that they can be sent to the fallback sink.
                        OnFailureStrategy::Fallback => {
                            warn!(
                                retry_attempts = ?retry_attempt,
                                errors = ?error_map,
                                "Retries exhausted, forwarding to fallback."
                            );
                            fallback_messages.append(&mut messages_to_retry);
                        }
                        // if strategy is drop, drop and move the remaining messages to dropped_messages
                        OnFailureStrategy::Drop => {
                            warn!(
                                retry_attempts = ?retry_attempt,
                                errors = ?error_map,
                                "Retries exhausted, dropping messages."
                            );
                            dropped_messages.append(&mut messages_to_retry);
                        }
                        // if the strategy is retry, we don't have to do anything because the retry
                        // limit is set to int::MAX.
                        OnFailureStrategy::Retry => {}
                    }
                    // No more retries, remaining messages are failed
                    warn!(remaining = messages_to_retry.len(), "Retries exhausted");
                    break;
                }
            }
        }

        Ok(SinkActorResponse {
            failed: messages_to_retry,
            fallback: fallback_messages,
            serving: serving_messages,
            dropped: dropped_messages,
            on_success: on_success_messages,
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

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::ops::Deref;
    use std::ops::DerefMut;
    use std::sync::Arc;

    #[derive(Debug, Clone)]
    struct Inner {
        value: usize,
        map: HashMap<String, usize>,
    }

    #[derive(Debug, Clone)]
    struct Outer {
        value: Arc<Inner>,
    }

    #[test]
    fn test_metadata_clone() {
        let inner = Inner {
            value: 42,
            map: HashMap::new(),
        };
        let mut outer = Outer {
            value: Arc::new(inner),
        };
        let cloned = Inner {
            value: outer.value.value,
            map: Arc::make_mut(&mut outer.value).map.clone(),
        };
    }
}
