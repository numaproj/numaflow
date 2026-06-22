use crate::config::components::sink::{OnFailureStrategy, RetryConfig};
use crate::message::Message;
use crate::metadata::Metadata;
use crate::sinker::sink::{ResponseStatusFromSink, Sink};
use crate::{Error, Result};
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
    pub(super) nacked: Vec<Message>,
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
        let mut messages_to_retry = messages;
        let mut fallback_messages = Vec::new();
        let mut serving_messages = Vec::new();
        let mut dropped_messages = Vec::new();
        let mut on_success_messages = Vec::new();
        let mut nacked_messages = Vec::new();

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
        let mut retry_attempt: u64 = 0;
        let mut error_map = HashMap::new();

        loop {
            // Stamp the current retry attempt count into each message's system metadata so
            // that user-defined sinks can observe how many times a message has been retried.
            // It is 0 on the first attempt and incremented for every subsequent retry.
            for msg in &mut messages_to_retry {
                msg.set_sink_retry_count(retry_attempt);
            }

            // send batch to sink
            let responses = self.sink.sink(messages_to_retry.clone()).await?;

            // Create a map of id to result. Duplicate response IDs are a data-plane invariant
            // violation because we cannot safely classify per-message outcomes.
            let mut result_map = HashMap::with_capacity(responses.len());
            for resp in responses {
                if result_map.insert(resp.id.clone(), resp.status).is_some() {
                    return Err(Error::Sink(format!(
                        "duplicate response id from sink: {}",
                        resp.id
                    )));
                }
            }

            // Classify messages based on responses, preserving original ordering.
            // Take ownership of the current batch so we can iterate by move and push
            // failed messages back into messages_to_retry in their original order.
            for msg in std::mem::take(&mut messages_to_retry) {
                let msg_id = msg.id.to_string();
                match result_map.remove(&msg_id) {
                    Some(ResponseStatusFromSink::Success) => {
                        // Message successfully sunk, nothing more to do
                    }
                    Some(ResponseStatusFromSink::Failed(err_msg)) => {
                        *error_map.entry(err_msg).or_insert(0) += 1;
                        messages_to_retry.push(msg); // keep for retry
                    }
                    Some(ResponseStatusFromSink::Fallback) => {
                        fallback_messages.push(msg);
                    }
                    Some(ResponseStatusFromSink::Serve(serve_response)) => {
                        let mut msg = msg;
                        if let Some(serve_response) = serve_response {
                            msg.value = serve_response.into();
                        }
                        serving_messages.push(msg);
                    }
                    Some(ResponseStatusFromSink::OnSuccess(on_success_msg)) => {
                        if let Some(on_success_msg) = on_success_msg {
                            let on_success_md: Option<Metadata> =
                                on_success_msg.metadata.map(|md| md.into());
                            let new_md = match &msg.metadata {
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
                                ..msg
                            };
                            on_success_messages.push(new_msg.clone());
                        } else {
                            // Send the original message if no payload was provided to the onSuccess sink
                            on_success_messages.push(msg);
                        }
                    }
                    Some(ResponseStatusFromSink::Nack(options)) => {
                        // Any message that was nacked upstream eg: in mono-vertex's udf,
                        // would've been filtered out already before messages were sent
                        // to sink for writing. So we can overwrite nack options here.
                        let mut msg = msg;
                        msg.nack_options = options;
                        nacked_messages.push(msg);
                    }
                    None => {
                        return Err(Error::Sink(format!(
                            "missing response from sink for message id: {msg_id}"
                        )));
                    }
                }
            }

            if !result_map.is_empty() {
                let mut ids = result_map.into_keys().collect::<Vec<_>>();
                ids.sort();
                return Err(Error::Sink(format!(
                    "received responses for unknown message ids from sink: {}",
                    ids.join(", ")
                )));
            }

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
                    nacked: nacked_messages,
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
            nacked: nacked_messages,
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
    use super::*;
    use crate::message::{Message, MessageID};
    use crate::sinker::sink::ResponseFromSink;

    struct StaticResponseSink {
        responses: Vec<ResponseFromSink>,
    }

    impl Sink for StaticResponseSink {
        async fn sink(&mut self, _messages: Vec<Message>) -> Result<Vec<ResponseFromSink>> {
            Ok(std::mem::take(&mut self.responses))
        }
    }

    fn message(id: &str) -> Message {
        Message {
            id: MessageID {
                vertex_name: "vertex".to_string().into(),
                offset: id.to_string().into(),
                index: 0,
            },
            ..Default::default()
        }
    }

    fn response(id: &str) -> ResponseFromSink {
        ResponseFromSink {
            id: format!("vertex-{id}-0"),
            status: ResponseStatusFromSink::Success,
        }
    }

    async fn write_with_static_responses(
        messages: Vec<Message>,
        responses: Vec<ResponseFromSink>,
    ) -> Result<SinkActorResponse> {
        let (_tx, rx) = tokio::sync::mpsc::channel(1);
        let mut actor =
            SinkActor::new(rx, StaticResponseSink { responses }, RetryConfig::default());
        actor
            .write_messages_with_retry(messages, &CancellationToken::new())
            .await
    }

    #[tokio::test]
    async fn duplicate_sink_response_ids_return_error() {
        let result =
            write_with_static_responses(vec![message("1")], vec![response("1"), response("1")])
                .await;

        assert!(matches!(
            result,
            Err(Error::Sink(message)) if message.contains("duplicate response id from sink")
        ));
    }

    #[tokio::test]
    async fn missing_sink_response_ids_return_error() {
        let result =
            write_with_static_responses(vec![message("1"), message("2")], vec![response("1")])
                .await;

        assert!(matches!(
            result,
            Err(Error::Sink(message)) if message.contains("missing response from sink")
        ));
    }

    /// A sink that fails the message until `succeed_on_attempt` calls have been made,
    /// recording the retry_count observed in each message's system metadata on every call.
    struct RetryCountRecordingSink {
        attempt: usize,
        succeed_on_attempt: usize,
        observed_retry_counts: Arc<std::sync::Mutex<Vec<Option<String>>>>,
    }

    impl Sink for RetryCountRecordingSink {
        async fn sink(&mut self, messages: Vec<Message>) -> Result<Vec<ResponseFromSink>> {
            // Record the retry_count observed on the first message of this batch.
            let observed = messages.first().and_then(|m| {
                m.metadata.as_ref().and_then(|md| {
                    md.sys_metadata
                        .get(crate::metadata::SINK_METADATA_GROUP)
                        .and_then(|g| g.key_value.get(crate::metadata::RETRY_COUNT_KEY))
                        .map(|v| String::from_utf8_lossy(v).to_string())
                })
            });
            self.observed_retry_counts.lock().unwrap().push(observed);

            self.attempt += 1;
            let succeed = self.attempt >= self.succeed_on_attempt;
            Ok(messages
                .into_iter()
                .map(|m| ResponseFromSink {
                    id: m.id.to_string(),
                    status: if succeed {
                        ResponseStatusFromSink::Success
                    } else {
                        ResponseStatusFromSink::Failed("retryable".to_string())
                    },
                })
                .collect())
        }
    }

    #[tokio::test]
    async fn retry_count_is_stamped_into_sys_metadata() {
        let observed_retry_counts = Arc::new(std::sync::Mutex::new(Vec::new()));
        let sink = RetryCountRecordingSink {
            attempt: 0,
            // fail twice (attempts 1 and 2), succeed on the third attempt
            succeed_on_attempt: 3,
            observed_retry_counts: Arc::clone(&observed_retry_counts),
        };

        // Use tiny intervals so the test does not actually sleep noticeably.
        let retry_config = RetryConfig {
            sink_max_retry_attempts: 5,
            sink_initial_retry_interval_in_ms: 1,
            sink_max_retry_interval_in_ms: 1,
            ..RetryConfig::default()
        };

        let (_tx, rx) = tokio::sync::mpsc::channel(1);
        let mut actor = SinkActor::new(rx, sink, retry_config);
        let result = actor
            .write_messages_with_retry(vec![message("1")], &CancellationToken::new())
            .await
            .unwrap();

        assert!(result.failed.is_empty());

        // The sink was invoked 3 times with retry_count 0, 1, 2 respectively.
        let observed = observed_retry_counts.lock().unwrap().clone();
        assert_eq!(
            observed,
            vec![
                Some("0".to_string()),
                Some("1".to_string()),
                Some("2".to_string()),
            ]
        );
    }
}
