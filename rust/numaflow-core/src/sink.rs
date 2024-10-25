use crate::config::components::sink::{OnFailureStrategy, RetryConfig};
use crate::config::pipeline::SinkVtxConfig;
use crate::error::Error;
use crate::message::{Message, ReadAck, ReadMessage, ResponseFromSink, ResponseStatusFromSink};
use crate::Result;
use numaflow_pb::clients::sink::sink_client::SinkClient;
use std::collections::HashMap;
use tokio::sync::mpsc::Receiver;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio::time::sleep;
use tokio::{pin, time};
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;
use tonic::transport::Channel;
use tracing::{debug, error, warn};
use user_defined::UserDefinedSink;

mod blackhole;
mod log;
/// [User-Defined Sink] extends Numaflow to add custom sources supported outside the builtins.
///
/// [User-Defined Sink]: https://numaflow.numaproj.io/user-guide/sinks/user-defined-sinks/
mod user_defined;

/// Set of items to be implemented be a Numaflow Sink.
///
/// [Sink]: https://numaflow.numaproj.io/user-guide/sinks/overview/
#[trait_variant::make(Sink: Send)]
#[allow(unused)]
pub(crate) trait LocalSink {
    /// Write the messages to the Sink.
    async fn sink(&mut self, messages: Vec<Message>) -> Result<Vec<ResponseFromSink>>;
}

enum ActorMessage {
    Sink {
        messages: Vec<Message>,
        respond_to: oneshot::Sender<Result<Vec<ResponseFromSink>>>,
    },
}

struct SinkActor<T> {
    actor_messages: mpsc::Receiver<ActorMessage>,
    sink: T,
}

impl<T> SinkActor<T>
where
    T: Sink,
{
    fn new(actor_messages: mpsc::Receiver<ActorMessage>, sink: T) -> Self {
        Self {
            actor_messages,
            sink,
        }
    }

    async fn handle_message(&mut self, msg: ActorMessage) {
        match msg {
            ActorMessage::Sink {
                messages,
                respond_to,
            } => {
                let response = self.sink.sink(messages).await;
                let _ = respond_to.send(response);
            }
        }
    }
}

#[derive(Clone)]
pub(crate) struct SinkHandle {
    sender: mpsc::Sender<ActorMessage>,
}

pub(crate) enum SinkClientType {
    Log,
    Blackhole,
    UserDefined(SinkClient<Channel>),
}

impl SinkHandle {
    pub(crate) async fn new(sink_client: SinkClientType, batch_size: usize) -> Result<Self> {
        let (sender, receiver) = mpsc::channel(batch_size);
        match sink_client {
            SinkClientType::Log => {
                let log_sink = log::LogSink;
                tokio::spawn(async {
                    let mut actor = SinkActor::new(receiver, log_sink);
                    while let Some(msg) = actor.actor_messages.recv().await {
                        actor.handle_message(msg).await;
                    }
                });
            }
            SinkClientType::Blackhole => {
                let blackhole_sink = blackhole::BlackholeSink;
                tokio::spawn(async {
                    let mut actor = SinkActor::new(receiver, blackhole_sink);
                    while let Some(msg) = actor.actor_messages.recv().await {
                        actor.handle_message(msg).await;
                    }
                });
            }
            SinkClientType::UserDefined(sink_client) => {
                let sink = UserDefinedSink::new(sink_client).await?;
                tokio::spawn(async {
                    let mut actor = SinkActor::new(receiver, sink);
                    while let Some(msg) = actor.actor_messages.recv().await {
                        actor.handle_message(msg).await;
                    }
                });
            }
        };
        Ok(Self { sender })
    }

    pub(crate) async fn sink(&self, messages: Vec<Message>) -> Result<Vec<ResponseFromSink>> {
        let (tx, rx) = oneshot::channel();
        let msg = ActorMessage::Sink {
            messages,
            respond_to: tx,
        };
        let _ = self.sender.send(msg).await;
        rx.await.unwrap()
    }
}

#[derive(Clone)]
pub(super) struct SinkWriter {
    batch_size: usize,
    read_timeout: time::Duration,
    config: SinkVtxConfig,
    sink_handle: SinkHandle,
    fb_sink_handle: Option<SinkHandle>,
}

impl SinkWriter {
    pub(super) async fn new(
        batch_size: usize,
        read_timeout: time::Duration,
        config: SinkVtxConfig,
        sink_handle: SinkHandle,
        fb_sink_handle: Option<SinkHandle>,
    ) -> Result<Self> {
        Ok(Self {
            batch_size,
            read_timeout,
            config,
            sink_handle,
            fb_sink_handle,
        })
    }

    pub(super) async fn start(
        &self,
        messages_rx: Receiver<ReadMessage>,
        cancellation_token: CancellationToken,
    ) -> Result<JoinHandle<Result<()>>> {
        let handle: JoinHandle<Result<()>> = tokio::spawn({
            let mut this = self.clone();
            async move {
                let chunk_stream = tokio_stream::wrappers::ReceiverStream::new(messages_rx)
                    .chunks_timeout(this.batch_size, this.read_timeout);

                pin!(chunk_stream);

                while let Some(batch) = chunk_stream.next().await {
                    if batch.is_empty() {
                        continue;
                    }

                    let messages: Vec<Message> =
                        batch.iter().map(|rm| rm.message.clone()).collect();

                    match this
                        .write_to_sink(messages, cancellation_token.clone())
                        .await
                    {
                        Ok(_) => {
                            for rm in batch {
                                let _ = rm.ack.send(ReadAck::Ack);
                            }
                        }
                        Err(e) => {
                            error!(?e, "Error writing to sink");
                            for rm in batch {
                                let _ = rm.ack.send(ReadAck::Nak);
                            }
                        }
                    }

                    if cancellation_token.is_cancelled() {
                        warn!("Cancellation token is cancelled. Exiting SinkWriter");
                        break;
                    }
                }

                Ok(())
            }
        });
        Ok(handle)
    }

    async fn collect_batch(&self, messages_rx: &mut Receiver<ReadMessage>) -> Vec<ReadMessage> {
        let mut batch = Vec::with_capacity(self.batch_size);
        let timeout = time::sleep(self.read_timeout);
        pin!(timeout);

        loop {
            tokio::select! {
                Some(read_message) = messages_rx.recv() => {
                    batch.push(read_message);
                    if batch.len() >= self.batch_size {
                        break;
                    }
                }
                _ = &mut timeout => {
                    break;
                }
            }
        }

        batch
    }

    // Writes the messages to the sink and handles fallback messages if present
    async fn write_to_sink(
        &mut self,
        messages: Vec<Message>,
        cln_token: CancellationToken,
    ) -> Result<()> {
        let msg_count = messages.len() as u64;

        if messages.is_empty() {
            return Ok(());
        }

        // this start time is for tracking the total time taken
        let start_time_e2e = tokio::time::Instant::now();

        let mut attempts = 0;
        let mut error_map = HashMap::new();
        let mut fallback_msgs = Vec::new();
        // start with the original set of message to be sent.
        // we will overwrite this vec with failed messages and will keep retrying.
        let mut messages_to_send = messages;

        // only breaks out of this loop based on the retry strategy unless all the messages have been written to sink
        // successfully.
        let retry_config = &self
            .config
            .sink_config
            .retry_config
            .clone()
            .unwrap_or_default();

        loop {
            while attempts < retry_config.sink_max_retry_attempts {
                let status = self
                    .write_to_sink_once(
                        &mut error_map,
                        &mut fallback_msgs,
                        &mut messages_to_send,
                        retry_config,
                    )
                    .await;
                match status {
                    Ok(true) => break,
                    Ok(false) => {
                        attempts += 1;
                        warn!(
                            "Retry attempt {} due to retryable error. Errors: {:?}",
                            attempts, error_map
                        );
                    }
                    Err(e) => Err(e)?,
                }

                // if we are shutting down, stop the retry
                if cln_token.is_cancelled() {
                    return Err(Error::Sink(
                        "Cancellation token triggered during retry".to_string(),
                    ));
                }
            }

            // If after the retries we still have messages to process, handle the post retry failures
            let need_retry = self.handle_sink_post_retry(
                &mut attempts,
                &mut error_map,
                &mut fallback_msgs,
                &mut messages_to_send,
                retry_config,
            );

            match need_retry {
                // if we are done with the messages, break the loop
                Ok(false) => break,
                // if we need to retry, reset the attempts and error_map
                Ok(true) => {
                    attempts = 0;
                    error_map.clear();
                }
                Err(e) => Err(e)?,
            }
        }

        // If there are fallback messages, write them to the fallback sink
        if !fallback_msgs.is_empty() {
            self.handle_fallback_messages(fallback_msgs, retry_config)
                .await?;
        }

        Ok(())
    }

    /// Handles the post retry failures based on the configured strategy,
    /// returns true if we need to retry, else false.
    fn handle_sink_post_retry(
        &mut self,
        attempts: &mut u16,
        error_map: &mut HashMap<String, i32>,
        fallback_msgs: &mut Vec<Message>,
        messages_to_send: &mut Vec<Message>,
        retry_config: &RetryConfig,
    ) -> Result<bool> {
        // if we are done with the messages, break the loop
        if messages_to_send.is_empty() {
            return Ok(false);
        }
        // check what is the failure strategy in the config
        let strategy = retry_config.sink_retry_on_fail_strategy.clone();
        match strategy {
            // if we need to retry, return true
            OnFailureStrategy::Retry => {
                warn!(
                    "Using onFailure Retry, Retry attempts {} completed",
                    attempts
                );
                return Ok(true);
            }
            // if we need to drop the messages, log and return false
            OnFailureStrategy::Drop => {
                // log that we are dropping the messages as requested
                warn!(
                    "Dropping messages after {} attempts. Errors: {:?}",
                    attempts, error_map
                );
            }
            // if we need to move the messages to the fallback, return false
            OnFailureStrategy::Fallback => {
                // log that we are moving the messages to the fallback as requested
                warn!(
                    "Moving messages to fallback after {} attempts. Errors: {:?}",
                    attempts, error_map
                );
                // move the messages to the fallback messages
                fallback_msgs.append(messages_to_send);
            }
        }
        // if we are done with the messages, break the loop
        Ok(false)
    }

    /// Writes to sink once and will return true if successful, else false. Please note that it
    /// mutates is incoming fields.
    async fn write_to_sink_once(
        &mut self,
        error_map: &mut HashMap<String, i32>,
        fallback_msgs: &mut Vec<Message>,
        messages_to_send: &mut Vec<Message>,
        retry_config: &RetryConfig,
    ) -> Result<bool> {
        let start_time = time::Instant::now();
        match self.sink_handle.sink(messages_to_send.clone()).await {
            Ok(response) => {
                debug!("Sink latency - {}ms", start_time.elapsed().as_millis());

                // create a map of id to result, since there is no strict requirement
                // for the udsink to return the results in the same order as the requests
                let result_map = response
                    .into_iter()
                    .map(|resp| (resp.id, resp.status))
                    .collect::<HashMap<_, _>>();

                error_map.clear();
                // drain all the messages that were successfully written
                // and keep only the failed messages to send again
                // construct the error map for the failed messages
                messages_to_send.retain(|msg| {
                    if let Some(result) = result_map.get(&msg.id.to_string()) {
                        return match result {
                            ResponseStatusFromSink::Success => false,
                            ResponseStatusFromSink::Failed(err_msg) => {
                                *error_map.entry(err_msg.clone()).or_insert(0) += 1;
                                true
                            }
                            ResponseStatusFromSink::Fallback => {
                                fallback_msgs.push(msg.clone());
                                false
                            }
                        };
                    }
                    false
                });

                // if all messages are successfully written, break the loop
                if messages_to_send.is_empty() {
                    return Ok(true);
                }

                sleep(tokio::time::Duration::from_millis(
                    retry_config.sink_retry_interval_in_ms as u64,
                ))
                .await;

                // we need to retry
                Ok(false)
            }
            Err(e) => Err(e),
        }
    }

    // Writes the fallback messages to the fallback sink
    async fn handle_fallback_messages(
        &mut self,
        fallback_msgs: Vec<Message>,
        retry_config: &RetryConfig,
    ) -> Result<()> {
        if self.fb_sink_handle.is_none() {
            return Err(Error::Sink(
                "Response contains fallback messages but no fallback sink is configured"
                    .to_string(),
            ));
        }

        let fallback_client = self.fb_sink_handle.as_mut().unwrap();
        let mut attempts = 0;
        let mut fallback_error_map = HashMap::new();
        // start with the original set of message to be sent.
        // we will overwrite this vec with failed messages and will keep retrying.
        let mut messages_to_send = fallback_msgs;
        let fb_msg_count = messages_to_send.len() as u64;

        let default_retry = retry_config
            .sink_default_retry_strategy
            .clone()
            .backoff
            .unwrap();
        let max_attempts = default_retry.steps.unwrap();
        let sleep_interval = default_retry.interval.unwrap();

        while attempts < max_attempts {
            let start_time = tokio::time::Instant::now();
            match fallback_client.sink(messages_to_send.clone()).await {
                Ok(fb_response) => {
                    debug!(
                        "Fallback sink latency - {}ms",
                        start_time.elapsed().as_millis()
                    );

                    // create a map of id to result, since there is no strict requirement
                    // for the udsink to return the results in the same order as the requests
                    let result_map = fb_response
                        .into_iter()
                        .map(|resp| (resp.id, resp.status))
                        .collect::<HashMap<_, _>>();

                    let mut contains_fallback_status = false;

                    fallback_error_map.clear();
                    // drain all the messages that were successfully written
                    // and keep only the failed messages to send again
                    // construct the error map for the failed messages
                    messages_to_send.retain(|msg| {
                        if let Some(result) = result_map.get(&msg.id.to_string()) {
                            return match result {
                                ResponseStatusFromSink::Success => false,
                                ResponseStatusFromSink::Failed(err_msg) => {
                                    *fallback_error_map.entry(err_msg.clone()).or_insert(0) += 1;
                                    true
                                }
                                ResponseStatusFromSink::Fallback => {
                                    contains_fallback_status = true;
                                    false
                                }
                            };
                        } else {
                            false
                        }
                    });

                    // specifying fallback status in fallback response is not allowed
                    if contains_fallback_status {
                        return Err(Error::Sink(
                            "Fallback response contains fallback status".to_string(),
                        ));
                    }

                    attempts += 1;

                    if messages_to_send.is_empty() {
                        break;
                    }

                    warn!(
                        "Retry attempt {} due to retryable error. Errors: {:?}",
                        attempts, fallback_error_map
                    );
                    sleep(tokio::time::Duration::from(sleep_interval)).await;
                }
                Err(e) => return Err(e),
            }
        }
        if !messages_to_send.is_empty() {
            return Err(Error::Sink(format!(
                "Failed to write messages to fallback sink after {} attempts. Errors: {:?}",
                attempts, fallback_error_map
            )));
        }
        Ok(())
    }
}
