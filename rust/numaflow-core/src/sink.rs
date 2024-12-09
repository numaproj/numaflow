use std::collections::HashMap;
use std::time::Duration;

use numaflow_pb::clients::sink::sink_client::SinkClient;
use tokio::sync::mpsc::Receiver;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio::time::sleep;
use tokio::{pin, time};
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;
use tonic::transport::Channel;
use tracing::{debug, error, info, warn};
use user_defined::UserDefinedSink;

use crate::config::components::sink::{OnFailureStrategy, RetryConfig};
use crate::error::Error;
use crate::message::{Message, ReadAck, ReadMessage, ResponseFromSink, ResponseStatusFromSink};
use crate::Result;

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
#[allow(dead_code)]
pub(crate) trait LocalSink {
    /// Write the messages to the Sink.
    async fn sink(&mut self, messages: Vec<Message>) -> Result<Vec<ResponseFromSink>>;
}

/// ActorMessage is a message that is sent to the SinkActor.
enum ActorMessage {
    Sink {
        messages: Vec<Message>,
        respond_to: oneshot::Sender<Result<Vec<ResponseFromSink>>>,
    },
}

/// SinkActor is an actor that handles messages sent to the Sink.
struct SinkActor<T> {
    actor_messages: Receiver<ActorMessage>,
    sink: T,
}

impl<T> SinkActor<T>
where
    T: Sink,
{
    fn new(actor_messages: Receiver<ActorMessage>, sink: T) -> Self {
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

pub(crate) enum SinkClientType {
    Log,
    Blackhole,
    UserDefined(SinkClient<Channel>),
}

/// SinkWriter is a writer that writes messages to the Sink.
#[derive(Clone)]
pub(super) struct SinkWriter {
    batch_size: usize,
    chunk_timeout: Duration,
    retry_config: RetryConfig,
    sink_handle: mpsc::Sender<ActorMessage>,
    fb_sink_handle: Option<mpsc::Sender<ActorMessage>>,
}

/// SinkWriterBuilder is a builder to build a SinkWriter.
pub struct SinkWriterBuilder {
    batch_size: usize,
    chunk_timeout: Duration,
    retry_config: RetryConfig,
    sink_client: SinkClientType,
    fb_sink_client: Option<SinkClientType>,
}

impl SinkWriterBuilder {
    pub fn new(batch_size: usize, chunk_timeout: Duration, sink_type: SinkClientType) -> Self {
        Self {
            batch_size,
            chunk_timeout,
            retry_config: RetryConfig::default(),
            sink_client: sink_type,
            fb_sink_client: None,
        }
    }

    pub fn retry_config(mut self, retry_config: RetryConfig) -> Self {
        self.retry_config = retry_config;
        self
    }

    pub fn fb_sink_client(mut self, fb_sink_client: SinkClientType) -> Self {
        self.fb_sink_client = Some(fb_sink_client);
        self
    }

    /// Build the SinkWriter, it also starts the SinkActor to handle messages.
    pub async fn build(self) -> Result<SinkWriter> {
        let (sender, receiver) = mpsc::channel(self.batch_size);

        match self.sink_client {
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

        let fb_sink_handle = if let Some(fb_sink_client) = self.fb_sink_client {
            let (fb_sender, fb_receiver) = mpsc::channel(self.batch_size);
            match fb_sink_client {
                SinkClientType::Log => {
                    let log_sink = log::LogSink;
                    tokio::spawn(async {
                        let mut actor = SinkActor::new(fb_receiver, log_sink);
                        while let Some(msg) = actor.actor_messages.recv().await {
                            actor.handle_message(msg).await;
                        }
                    });
                }
                SinkClientType::Blackhole => {
                    let blackhole_sink = blackhole::BlackholeSink;
                    tokio::spawn(async {
                        let mut actor = SinkActor::new(fb_receiver, blackhole_sink);
                        while let Some(msg) = actor.actor_messages.recv().await {
                            actor.handle_message(msg).await;
                        }
                    });
                }
                SinkClientType::UserDefined(sink_client) => {
                    let sink = UserDefinedSink::new(sink_client).await?;
                    tokio::spawn(async {
                        let mut actor = SinkActor::new(fb_receiver, sink);
                        while let Some(msg) = actor.actor_messages.recv().await {
                            actor.handle_message(msg).await;
                        }
                    });
                }
            };
            Some(fb_sender)
        } else {
            None
        };

        Ok(SinkWriter {
            batch_size: self.batch_size,
            chunk_timeout: self.chunk_timeout,
            retry_config: self.retry_config,
            sink_handle: sender,
            fb_sink_handle,
        })
    }
}

impl SinkWriter {
    /// Sink the messages to the Sink.
    async fn sink(&self, messages: Vec<Message>) -> Result<Vec<ResponseFromSink>> {
        let (tx, rx) = oneshot::channel();
        let msg = ActorMessage::Sink {
            messages,
            respond_to: tx,
        };
        let _ = self.sink_handle.send(msg).await;
        rx.await.unwrap()
    }

    /// Sink the messages to the Fallback Sink.
    async fn fb_sink(&self, messages: Vec<Message>) -> Result<Vec<ResponseFromSink>> {
        if self.fb_sink_handle.is_none() {
            return Err(Error::Sink(
                "Response contains fallback messages but no fallback sink is configured"
                    .to_string(),
            ));
        }

        let (tx, rx) = oneshot::channel();
        let msg = ActorMessage::Sink {
            messages,
            respond_to: tx,
        };
        let _ = self.fb_sink_handle.as_ref().unwrap().send(msg).await;
        rx.await.unwrap()
    }

    /// Streaming write the messages to the Sink, it will keep writing messages until the stream is
    /// closed or the cancellation token is triggered.
    pub(super) async fn streaming_write(
        &self,
        messages_stream: ReceiverStream<ReadMessage>,
        cancellation_token: CancellationToken,
    ) -> Result<JoinHandle<Result<()>>> {
        let handle: JoinHandle<Result<()>> = tokio::spawn({
            let mut this = self.clone();
            async move {
                let chunk_stream =
                    messages_stream.chunks_timeout(this.batch_size, this.chunk_timeout);

                pin!(chunk_stream);

                let mut processed_msgs_count: usize = 0;
                let mut last_logged_at = std::time::Instant::now();

                loop {
                    let batch = match chunk_stream.next().await {
                        Some(batch) => batch,
                        None => {
                            break;
                        }
                    };

                    if batch.is_empty() {
                        continue;
                    }

                    let n = batch.len();
                    let (messages, senders): (Vec<_>, Vec<_>) =
                        batch.into_iter().map(|rm| (rm.message, rm.ack)).unzip();

                    match this.write(messages, cancellation_token.clone()).await {
                        Ok(_) => {
                            for sender in senders {
                                let _ = sender.send(ReadAck::Ack);
                            }
                        }
                        Err(e) => {
                            error!(?e, "Error writing to sink");
                            for sender in senders {
                                let _ = sender.send(ReadAck::Nak);
                            }
                        }
                    }

                    processed_msgs_count += n;
                    if last_logged_at.elapsed().as_millis() >= 1000 {
                        info!(
                            "Processed {} messages at {:?}",
                            processed_msgs_count,
                            std::time::Instant::now()
                        );
                        processed_msgs_count = 0;
                        last_logged_at = std::time::Instant::now();
                    }
                }

                Ok(())
            }
        });
        Ok(handle)
    }

    /// Write the messages to the Sink.
    pub(crate) async fn write(
        &mut self,
        messages: Vec<Message>,
        cln_token: CancellationToken,
    ) -> Result<()> {
        if messages.is_empty() {
            return Ok(());
        }

        let mut attempts = 0;
        let mut error_map = HashMap::new();
        let mut fallback_msgs = Vec::new();
        // start with the original set of message to be sent.
        // we will overwrite this vec with failed messages and will keep retrying.
        let mut messages_to_send = messages;

        // only breaks out of this loop based on the retry strategy unless all the messages have been written to sink
        // successfully.
        let retry_config = &self.retry_config.clone();

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
            let need_retry = Self::handle_sink_post_retry(
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
        match self.sink(messages_to_send.clone()).await {
            Ok(response) => {
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

                sleep(Duration::from_millis(
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

        let mut attempts = 0;
        let mut fallback_error_map = HashMap::new();
        // start with the original set of message to be sent.
        // we will overwrite this vec with failed messages and will keep retrying.
        let mut messages_to_send = fallback_msgs;

        let default_retry = retry_config
            .sink_default_retry_strategy
            .clone()
            .backoff
            .unwrap();
        let max_attempts = default_retry.steps.unwrap();
        let sleep_interval = default_retry.interval.unwrap();

        while attempts < max_attempts {
            let start_time = time::Instant::now();
            match self.fb_sink(messages_to_send.clone()).await {
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

impl Drop for SinkWriter {
    fn drop(&mut self) {}
}

#[cfg(test)]
mod tests {
    use chrono::Utc;
    use numaflow::sink;
    use tokio::time::Duration;
    use tokio_util::sync::CancellationToken;

    use super::*;
    use crate::message::{Message, MessageID};
    use crate::shared::grpc::create_rpc_channel;

    struct SimpleSink;
    #[tonic::async_trait]
    impl sink::Sinker for SimpleSink {
        async fn sink(&self, mut input: Receiver<sink::SinkRequest>) -> Vec<sink::Response> {
            let mut responses: Vec<sink::Response> = Vec::new();
            while let Some(datum) = input.recv().await {
                if datum.keys.first().unwrap() == "fallback" {
                    responses.push(sink::Response::fallback(datum.id));
                    continue;
                } else if datum.keys.first().unwrap() == "error" {
                    responses.push(sink::Response::failure(
                        datum.id,
                        "simple error".to_string(),
                    ));
                } else {
                    responses.push(sink::Response::ok(datum.id));
                }
            }
            responses
        }
    }

    #[tokio::test]
    async fn test_write() {
        let mut sink_writer =
            SinkWriterBuilder::new(10, Duration::from_secs(1), SinkClientType::Log)
                .build()
                .await
                .unwrap();

        let messages: Vec<Message> = (0..5)
            .map(|i| Message {
                keys: vec![format!("key_{}", i)],
                value: format!("message {}", i).as_bytes().to_vec().into(),
                offset: None,
                event_time: Utc::now(),
                id: MessageID {
                    vertex_name: "vertex".to_string(),
                    offset: format!("offset_{}", i),
                    index: i,
                },
                headers: HashMap::new(),
            })
            .collect();

        let result = sink_writer
            .write(messages.clone(), CancellationToken::new())
            .await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_streaming_write() {
        let sink_writer =
            SinkWriterBuilder::new(10, Duration::from_millis(100), SinkClientType::Log)
                .build()
                .await
                .unwrap();

        let messages: Vec<Message> = (0..10)
            .map(|i| Message {
                keys: vec![format!("key_{}", i)],
                value: format!("message {}", i).as_bytes().to_vec().into(),
                offset: None,
                event_time: Utc::now(),
                id: MessageID {
                    vertex_name: "vertex".to_string(),
                    offset: format!("offset_{}", i),
                    index: i,
                },
                headers: HashMap::new(),
            })
            .collect();

        let (tx, rx) = mpsc::channel(10);
        let mut ack_rxs = vec![];
        for msg in messages {
            let (ack_tx, ack_rx) = oneshot::channel();
            let _ = tx
                .send(ReadMessage {
                    message: msg,
                    ack: ack_tx,
                })
                .await;
            ack_rxs.push(ack_rx);
        }
        drop(tx);

        let handle = sink_writer
            .streaming_write(ReceiverStream::new(rx), CancellationToken::new())
            .await
            .unwrap();

        let _ = handle.await.unwrap();
        for ack_rx in ack_rxs {
            assert_eq!(ack_rx.await.unwrap(), ReadAck::Ack);
        }
    }

    #[tokio::test]
    async fn test_streaming_write_error() {
        // start the server
        let (_shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let tmp_dir = tempfile::TempDir::new().unwrap();
        let sock_file = tmp_dir.path().join("sink.sock");
        let server_info_file = tmp_dir.path().join("sink-server-info");

        let server_info = server_info_file.clone();
        let server_socket = sock_file.clone();

        let _server_handle = tokio::spawn(async move {
            sink::Server::new(SimpleSink)
                .with_socket_file(server_socket)
                .with_server_info_file(server_info)
                .start_with_shutdown(shutdown_rx)
                .await
                .expect("failed to start sink server");
        });

        // wait for the server to start
        sleep(Duration::from_millis(100)).await;

        let sink_writer = SinkWriterBuilder::new(
            10,
            Duration::from_millis(100),
            SinkClientType::UserDefined(SinkClient::new(
                create_rpc_channel(sock_file).await.unwrap(),
            )),
        )
        .build()
        .await
        .unwrap();

        let messages: Vec<Message> = (0..10)
            .map(|i| Message {
                keys: vec!["error".to_string()],
                value: format!("message {}", i).as_bytes().to_vec().into(),
                offset: None,
                event_time: Utc::now(),
                id: MessageID {
                    vertex_name: "vertex".to_string(),
                    offset: format!("offset_{}", i),
                    index: i,
                },
                headers: HashMap::new(),
            })
            .collect();

        let (tx, rx) = mpsc::channel(10);
        let mut ack_rxs = vec![];
        for msg in messages {
            let (ack_tx, ack_rx) = oneshot::channel();
            let _ = tx
                .send(ReadMessage {
                    message: msg,
                    ack: ack_tx,
                })
                .await;
            ack_rxs.push(ack_rx);
        }
        drop(tx);
        let cln_token = CancellationToken::new();
        let handle = sink_writer
            .streaming_write(ReceiverStream::new(rx), cln_token.clone())
            .await
            .unwrap();

        // cancel the token after 1 second to exit from the retry loop
        tokio::spawn(async move {
            sleep(Duration::from_secs(1)).await;
            cln_token.cancel();
        });

        let _ = handle.await.unwrap();
        // since the writes fail, all the messages will be NAKed
        for ack_rx in ack_rxs {
            assert_eq!(ack_rx.await.unwrap(), ReadAck::Nak);
        }
    }

    #[tokio::test]
    async fn test_fallback_write() {
        // start the server
        let (_shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let tmp_dir = tempfile::TempDir::new().unwrap();
        let sock_file = tmp_dir.path().join("sink.sock");
        let server_info_file = tmp_dir.path().join("sink-server-info");

        let server_info = server_info_file.clone();
        let server_socket = sock_file.clone();

        let _server_handle = tokio::spawn(async move {
            sink::Server::new(SimpleSink)
                .with_socket_file(server_socket)
                .with_server_info_file(server_info)
                .start_with_shutdown(shutdown_rx)
                .await
                .expect("failed to start sink server");
        });

        // wait for the server to start
        sleep(Duration::from_millis(100)).await;

        let sink_writer = SinkWriterBuilder::new(
            10,
            Duration::from_millis(100),
            SinkClientType::UserDefined(SinkClient::new(
                create_rpc_channel(sock_file).await.unwrap(),
            )),
        )
        .fb_sink_client(SinkClientType::Log)
        .build()
        .await
        .unwrap();

        let messages: Vec<Message> = (0..20)
            .map(|i| Message {
                keys: vec!["fallback".to_string()],
                value: format!("message {}", i).as_bytes().to_vec().into(),
                offset: None,
                event_time: Utc::now(),
                id: MessageID {
                    vertex_name: "vertex".to_string(),
                    offset: format!("offset_{}", i),
                    index: i,
                },
                headers: HashMap::new(),
            })
            .collect();

        let (tx, rx) = mpsc::channel(20);
        let mut ack_rxs = vec![];
        for msg in messages {
            let (ack_tx, ack_rx) = oneshot::channel();
            let _ = tx
                .send(ReadMessage {
                    message: msg,
                    ack: ack_tx,
                })
                .await;
            ack_rxs.push(ack_rx);
        }
        drop(tx);
        let cln_token = CancellationToken::new();
        let handle = sink_writer
            .streaming_write(ReceiverStream::new(rx), cln_token.clone())
            .await
            .unwrap();

        let _ = handle.await.unwrap();
        for ack_rx in ack_rxs {
            assert_eq!(ack_rx.await.unwrap(), ReadAck::Ack);
        }
    }
}
