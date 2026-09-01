//! Implementation of the SQS message source using an actor-based architecture.
//!
//! Key design features:
//! - Actor model for thread-safe state management
//! - Batched message handling for efficiency
//! - Robust error handling and retry logic
//! - Configurable timeouts and batch sizes

use std::collections::{HashMap, VecDeque};
use std::str::FromStr;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use aws_sdk_sqs::Client;
use aws_sdk_sqs::types::{
    DeleteMessageBatchRequestEntry, MessageSystemAttributeName, QueueAttributeName,
};
use bytes::Bytes;
use chrono::{DateTime, TimeZone, Utc};
use tokio::sync::{mpsc, oneshot};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use tracing::error;

use crate::{
    AssumeRoleConfig, Error, SQS_METADATA_KEY, SqsConfig, SqsSourceError, extract_aws_error,
};

pub const SQS_DEFAULT_REGION: &str = "us-west-2";

/// Consecutive `ReceiveMessage` failures tolerated on a single queue before it is
/// declared terminal. Below this, a failure is reported as an empty poll so a
/// transient throttle does not restart the vertex.
const MAX_CONSECUTIVE_RECEIVE_FAILURES: usize = 10;

pub type Result<T> = std::result::Result<T, SqsSourceError>;

/// First terminal queue failure seen by any actor. Set once and never cleared:
/// the source is expected to stop so the vertex restarts and re-resolves every
/// queue URL. Written by the owning actor rather than carried in a read response,
/// because a read may return before that actor replies.
type TerminalError = Arc<OnceLock<String>>;

/// Configuration for an SQS message source.
///
/// All queue names share the same region, owner account, credentials, and
/// tuning settings.
#[derive(Debug, Clone, PartialEq)]
pub struct SqsSourceConfig {
    // Required fields
    pub region: &'static str,
    pub queue_names: Vec<&'static str>,
    pub queue_owner_aws_account_id: &'static str,

    // Optional fields
    pub visibility_timeout: Option<i32>,
    pub max_number_of_messages: Option<i32>,
    pub wait_time_seconds: Option<i32>,
    pub endpoint_url: Option<String>,
    pub attribute_names: Vec<String>,
    pub message_attribute_names: Vec<String>,
    pub assume_role_config: Option<AssumeRoleConfig>,
}
#[derive(Debug)]
pub struct SqsNack {
    /// Queue the receipt handle came from. Decoded from an offset at runtime,
    /// hence owned rather than `&'static str`.
    pub queue_name: String,
    pub receipt_handle: Bytes,
    pub visibility_timeout: i32,
}
/// Internal message types for the actor implementation.
///
/// The actor pattern is used to:
/// - Ensure thread-safe access to the SQS client
/// - Manage connection state and retries
/// - Handle concurrent requests without locks
enum SQSActorMessage {
    /// Responses go to a channel shared by every queue in one read, so the reader
    /// can consume them in completion order and walk away from the stragglers.
    Receive {
        respond_to: mpsc::Sender<ReceiveResponse>,
        count: i32,
        timeout_at: Instant,
    },
    Delete {
        respond_to: oneshot::Sender<Result<()>>,
        offsets: Vec<Bytes>,
    },
    Nack {
        respond_to: oneshot::Sender<Result<()>>,
        offsets: Vec<SqsNack>,
    },
    GetPending {
        respond_to: oneshot::Sender<Result<Option<usize>>>,
    },
}

/// A single queue's answer to one `Receive`, tagged with the queue it came from.
type ReceiveResponse = (&'static str, Option<Result<Vec<SqsMessage>>>);

/// A message received from SQS.
#[derive(Debug)]
pub struct SqsMessage {
    pub key: String,
    pub payload: Bytes,
    pub offset: String,
    pub event_time: DateTime<Utc>,
    /// Queue this message was read from. Used both to stamp downstream
    /// sys_metadata and to route the ack/nack back, since a receipt handle is
    /// only valid on the queue that issued it.
    pub queue_name: &'static str,
    /// SQS system attributes (SentTimestamp, MessageGroupId, etc.)
    pub system_attributes: HashMap<String, String>,
    /// User-defined message attributes from `message_attributes`, keyed by namespace.
    pub custom_attributes: HashMap<String, HashMap<String, Vec<u8>>>,
}

/// Internal actor implementation for managing SQS interactions.
///
/// The actor maintains:
/// - Single SQS client instance
/// - Message channel for handling concurrent requests
struct SqsActor {
    handler_rx: mpsc::Receiver<SQSActorMessage>,
    client: Client,
    /// Per-queue URL from GetQueueUrl. Not shared across actors.
    queue_url: String,
    queue_name: &'static str,
    config: SqsSourceConfig,
    cancel_token: CancellationToken,
    /// Messages already received from SQS whose reader gave up waiting. Holding
    /// them here means they are served on the next read instead of sitting
    /// invisible until their visibility timeout expires.
    buffered: VecDeque<SqsMessage>,
    /// Reset on every successful receive; only an unbroken run escalates.
    consecutive_receive_failures: usize,
    terminal_error: TerminalError,
}

impl SqsActor {
    fn new(
        handler_rx: mpsc::Receiver<SQSActorMessage>,
        client: Client,
        queue_url: String,
        queue_name: &'static str,
        config: SqsSourceConfig,
        cancel_token: CancellationToken,
        terminal_error: TerminalError,
    ) -> Self {
        Self {
            handler_rx,
            client,
            queue_url,
            queue_name,
            config,
            cancel_token,
            buffered: VecDeque::new(),
            consecutive_receive_failures: 0,
            terminal_error,
        }
    }

    async fn run(&mut self) {
        while let Some(msg) = self.handler_rx.recv().await {
            self.handle_message(msg).await;
        }
    }

    async fn handle_message(&mut self, msg: SQSActorMessage) {
        match msg {
            SQSActorMessage::Receive {
                respond_to,
                count,
                timeout_at,
            } => {
                // Serve what was already fetched before spending another ReceiveMessage call.
                let response = if self.buffered.is_empty() {
                    self.get_messages(count, timeout_at).await
                } else {
                    let take = (count as usize).min(self.buffered.len());
                    Some(Ok(self.buffered.drain(..take).collect()))
                };

                if let Err(mpsc::error::SendError((_, undelivered))) =
                    respond_to.send((self.queue_name, response)).await
                    && let Some(Ok(messages)) = undelivered
                {
                    self.restore_unclaimed(messages);
                }
            }

            SQSActorMessage::Delete {
                respond_to,
                offsets,
            } => {
                let status = self.delete_messages(offsets).await;
                let _ = respond_to.send(status);
            }

            SQSActorMessage::Nack {
                respond_to,
                offsets,
            } => {
                let status = self.nack_messages(offsets).await;
                let _ = respond_to.send(status);
            }

            SQSActorMessage::GetPending { respond_to } => {
                let status = self.get_pending_messages().await;
                let _ = respond_to.send(status);
            }
        }
    }

    /// Puts back messages the reader never took. They go to the front so the
    /// queue's original receive order survives an abandoned read: a partial
    /// drain followed by a push to the back would turn `[A, B, C, D]` into
    /// `[C, D, A, B]`, which FIFO message groups cannot tolerate.
    fn restore_unclaimed(&mut self, messages: Vec<SqsMessage>) {
        for message in messages.into_iter().rev() {
            self.buffered.push_front(message);
        }
    }

    /// Retrieves messages from SQS with timeout and batching.
    ///
    /// Implementation details:
    /// - Respects timeout for long polling
    /// - Processes message attributes and system metadata
    /// - Returns messages in a normalized format
    async fn get_messages(
        &mut self,
        count: i32,
        timeout_at: Instant,
    ) -> Option<Result<Vec<SqsMessage>>> {
        if self.cancel_token.is_cancelled() {
            return None;
        }

        let remaining_time = timeout_at - Instant::now();

        // default to one second if remaining time is less than one second
        // as sqs sdk requires wait_time_seconds to be at least 1
        // https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-short-and-long-polling.html#sqs-short-long-polling-differences
        // TODO: find a better way to handle user input timeout. should allow the users
        // to choose long/short polling. For now, we default to 1 second (long polling).
        let wait_time = if remaining_time.as_millis() < 1000 {
            1
        } else {
            (remaining_time.as_secs() as i32).min(20) // SQS max wait time is 20 seconds
        };

        // Honor both the fair per-queue budget and SQS's maximum batch size.
        let max_messages = count
            .min(self.config.max_number_of_messages.unwrap_or(10))
            .clamp(1, 10);

        let mut receive_message_builder = self
            .client
            .receive_message()
            .queue_url(&self.queue_url)
            .max_number_of_messages(max_messages)
            .wait_time_seconds(wait_time);

        // Apply visibility timeout if configured
        if let Some(visibility_timeout) = self.config.visibility_timeout {
            receive_message_builder =
                receive_message_builder.visibility_timeout(visibility_timeout);
        }

        // Apply attribute names if configured
        if !self.config.attribute_names.is_empty() {
            for attr in &self.config.attribute_names {
                let attr_name = MessageSystemAttributeName::from_str(attr);
                match attr_name {
                    Ok(attr_name) => {
                        receive_message_builder =
                            receive_message_builder.message_system_attribute_names(attr_name);
                    }
                    Err(err) => {
                        tracing::error!(?err, "failed to parse attribute name");
                    }
                }
            }
        } else {
            receive_message_builder = receive_message_builder
                .message_system_attribute_names(MessageSystemAttributeName::All);
        }

        // Apply message attribute names if configured
        if !self.config.message_attribute_names.is_empty() {
            for attr in &self.config.message_attribute_names {
                receive_message_builder = receive_message_builder.message_attribute_names(attr);
            }
        } else {
            receive_message_builder = receive_message_builder.message_attribute_names("All");
        }

        let sdk_response = receive_message_builder.send().await;

        let receive_message_output = match sdk_response {
            Ok(output) => output,
            Err(err) => {
                self.consecutive_receive_failures += 1;
                let failures = self.consecutive_receive_failures;
                let detail = extract_aws_error(&err);
                tracing::error!(
                    ?err,
                    queue_url = self.queue_url,
                    queue_name = self.queue_name,
                    failures,
                    "failed to receive messages from SQS"
                );

                if failures > MAX_CONSECUTIVE_RECEIVE_FAILURES {
                    // Recorded here rather than carried in the response: the reader
                    // may have already returned and dropped its receiver for this round.
                    let _ = self.terminal_error.set(format!(
                        "SQS queue {} failed {failures} consecutive receives: {detail}",
                        self.queue_name
                    ));
                    return Some(Err(SqsSourceError::from(Error::Sqs(detail))));
                }

                // Below the threshold a failure looks like an empty poll, so a
                // transient throttle does not take the vertex down.
                return Some(Ok(Vec::new()));
            }
        };

        self.consecutive_receive_failures = 0;

        let messages = receive_message_output
            .messages
            .unwrap_or_default()
            .iter()
            .map(|msg| {
                let key = msg.message_id.clone().unwrap_or_default();
                let payload = Bytes::from(msg.body.clone().unwrap_or_default());
                let offset = msg.receipt_handle.clone().unwrap_or_default();

                // event_time is set to match the SentTimestamp attribute if available
                // see: https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_ReceiveMessage.html#API_ReceiveMessage_RequestSyntax
                let event_time = msg
                    .attributes
                    .as_ref()
                    .and_then(|attrs| attrs.get(&MessageSystemAttributeName::SentTimestamp))
                    .and_then(|timestamp| timestamp.parse::<i64>().ok())
                    .and_then(|timestamp| Utc.timestamp_millis_opt(timestamp).single())
                    .unwrap_or_else(Utc::now);

                let system_attributes: HashMap<String, String> = msg
                    .attributes
                    .as_ref()
                    .map(|attrs| {
                        attrs
                            .iter()
                            .map(|(k, v)| (k.as_str().to_string(), v.clone()))
                            .collect()
                    })
                    .unwrap_or_default();

                let mut custom_attributes = HashMap::new();
                if let Some(msg_attrs) = &msg.message_attributes {
                    let mut sqs_attrs = HashMap::new();
                    for (k, v) in msg_attrs {
                        if let Some(val) = &v.string_value {
                            sqs_attrs.insert(k.clone(), val.clone().into_bytes());
                        }
                    }
                    custom_attributes.insert(SQS_METADATA_KEY.to_string(), sqs_attrs);
                }

                SqsMessage {
                    key,
                    payload,
                    offset,
                    event_time,
                    queue_name: self.queue_name,
                    system_attributes,
                    custom_attributes,
                }
            })
            .collect();

        Some(Ok(messages))
    }

    /// deletes batch of messages from SQS, serves as Numaflow source ack.
    async fn delete_messages(&mut self, offsets: Vec<Bytes>) -> Result<()> {
        let receipt_handles = offsets
            .iter()
            .map(|offset| {
                std::str::from_utf8(offset).map_err(|err| {
                    error!(?err, ?offset, "failed to parse offset");
                    SqsSourceError::from(Error::Other("failed to parse offset".to_string()))
                })
            })
            .collect::<Result<Vec<_>>>()?;

        // DeleteMessageBatch accepts at most 10 entries. Validate every receipt
        // handle before sending any request, then split larger Numaflow ack
        // groups into SQS-sized batches.
        for receipt_handle_batch in receipt_handles.chunks(10) {
            let entries = receipt_handle_batch
                .iter()
                .enumerate()
                .map(|(id, receipt_handle)| {
                    DeleteMessageBatchRequestEntry::builder()
                        .receipt_handle(*receipt_handle)
                        .id(id.to_string())
                        .build()
                        .map_err(|err| {
                            error!(?err, "Failed to build DeleteMessageBatchRequestEntry");
                            SqsSourceError::from(Error::Other(format!(
                                "Failed to build delete request: {err}"
                            )))
                        })
                })
                .collect::<Result<Vec<_>>>()?;

            let output = self
                .client
                .delete_message_batch()
                .queue_url(&self.queue_url)
                .set_entries(Some(entries))
                .send()
                .await
                .map_err(|err| {
                    error!(
                        ?err,
                        queue_url = self.queue_url,
                        "Failed to delete messages from SQS"
                    );
                    SqsSourceError::from(Error::Sqs(extract_aws_error(&err)))
                })?;

            if !output.failed.is_empty() {
                let failures = output
                    .failed
                    .iter()
                    .map(|failure| {
                        format!(
                            "id={} code={} message={}",
                            failure.id,
                            failure.code,
                            failure.message.as_deref().unwrap_or_default()
                        )
                    })
                    .collect::<Vec<_>>()
                    .join(", ");
                error!(
                    queue_url = self.queue_url,
                    failures, "SQS failed to delete one or more messages"
                );
                return Err(SqsSourceError::from(Error::Other(format!(
                    "SQS failed to delete one or more messages: {failures}"
                ))));
            }
        }

        Ok(())
    }

    /// Changes visibility timeout for messages, serves as Numaflow source nack.
    async fn nack_messages(&mut self, offsets: Vec<SqsNack>) -> Result<()> {
        for nack in offsets {
            let receipt_handle = std::str::from_utf8(&nack.receipt_handle).map_err(|err| {
                error!(?err, "failed to parse receipt handle");
                SqsSourceError::from(Error::Other("failed to parse receipt handle".to_string()))
            })?;

            let request = self
                .client
                .change_message_visibility()
                .queue_url(&self.queue_url)
                .receipt_handle(receipt_handle)
                .visibility_timeout(nack.visibility_timeout);

            if let Err(err) = request.send().await {
                error!(
                    ?err,
                    queue_url = self.queue_url,
                    "Failed to change message visibility"
                );

                return Err(SqsSourceError::from(Error::Sqs(extract_aws_error(&err))));
            }
        }

        Ok(())
    }
    /// get the pending message count from SQS using the ApproximateNumberOfMessages attribute
    /// Note: The ApproximateNumberOfMessages metrics may not achieve consistency until at least
    /// 1 minute after the producers stop sending messages.
    /// This period is required for the queue metadata to reach eventual consistency.
    async fn get_pending_messages(&mut self) -> Result<Option<usize>> {
        let sdk_response = self
            .client
            .get_queue_attributes()
            .queue_url(self.queue_url.clone())
            .attribute_names(QueueAttributeName::ApproximateNumberOfMessages)
            .send()
            .await;

        let get_queue_attributes_output = match sdk_response {
            Ok(output) => output,
            Err(err) => {
                tracing::error!(
                    ?err,
                    queue_url = ?self.queue_url,
                    "failed to get queue attributes from SQS"
                );
                return Err(SqsSourceError::from(Error::Sqs(extract_aws_error(&err))));
            }
        };

        let attributes = match get_queue_attributes_output.attributes {
            Some(attributes) => attributes,
            None => return Ok(None),
        };

        let value = match attributes.get(&QueueAttributeName::ApproximateNumberOfMessages) {
            Some(value) => value,
            None => return Ok(None),
        };

        let approx_pending_messages_count = match value.parse::<usize>() {
            Ok(count) => count,
            Err(err) => {
                tracing::error!(?err, "failed to parse ApproximateNumberOfMessages");
                return Err(SqsSourceError::from(Error::Other(
                    "failed to parse ApproximateNumberOfMessages".to_string(),
                )));
            }
        };
        Ok(Some(approx_pending_messages_count))
    }
}

/// Public interface for interacting with SQS queues.
///
/// Design principles:
/// - Thread-safe through actor model
/// - Configurable batch sizes and timeouts
/// - Clean abstraction of SQS complexity
/// - Efficient message processing
#[derive(Clone)]
pub struct SqsSource {
    batch_size: usize,
    /// timeout for each batch read request
    timeout: Duration,
    /// One actor per configured queue name, keyed by name for ack/nack routing.
    actors: Arc<HashMap<&'static str, mpsc::Sender<SQSActorMessage>>>,
    /// Queue names in configuration order, for the round-robin batch split.
    queue_order: Arc<Vec<&'static str>>,
    /// Rotates remainder allocation when a batch does not divide evenly.
    read_cursor: Arc<std::sync::atomic::AtomicUsize>,
    /// First terminal queue failure, shared with every actor. Once set, reads fail.
    terminal_error: TerminalError,
    vertex_replica: u16,
}

/// Builder for creating an `SqsSource`.
///
/// This builder allows for configuring the SQS source with various parameters
/// such as region, queue name, batch size, timeout, and an optional SQS client.
#[derive(Clone)]
pub struct SqsSourceBuilder {
    config: SqsSourceConfig,
    batch_size: usize,
    timeout: Duration,
    client: Option<Client>,
    /// Test hook: mock interceptors are not safe for concurrent requests on a
    /// shared client, so unit tests may inject one client per queue.
    clients: Option<Vec<Client>>,
    vertex_replica: u16,
}

impl Default for SqsSourceBuilder {
    fn default() -> Self {
        Self::new(SqsSourceConfig {
            region: SQS_DEFAULT_REGION,
            queue_names: vec![""],
            queue_owner_aws_account_id: "",
            visibility_timeout: None,
            max_number_of_messages: None,
            wait_time_seconds: None,
            endpoint_url: None,
            attribute_names: Vec::new(),
            message_attribute_names: Vec::new(),
            assume_role_config: None,
        })
    }
}

impl SqsSourceBuilder {
    pub fn new(config: SqsSourceConfig) -> Self {
        Self {
            config,
            batch_size: 1,
            timeout: Duration::from_secs(1),
            client: None,
            clients: None,
            vertex_replica: 0,
        }
    }
    pub fn config(mut self, config: SqsSourceConfig) -> Self {
        self.config = config;
        self
    }

    pub fn batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }

    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    pub fn client(mut self, client: Client) -> Self {
        self.client = Some(client);
        self
    }

    #[cfg(test)]
    fn clients(mut self, clients: Vec<Client>) -> Self {
        self.clients = Some(clients);
        self
    }

    pub fn vertex_replica(mut self, vertex_replica: u16) -> Self {
        self.vertex_replica = vertex_replica;
        self
    }

    /// Builds an `SqsSource` instance with the provided configuration.
    ///
    /// This method consumes `self`, initializes one shared SQS client, resolves
    /// every queue URL, and spawns one actor per queue name. Startup fails if
    /// any queue cannot be resolved.
    ///
    /// # Returns
    /// - `Ok(SqsSource)` if the source is successfully built.
    /// - `Err(Error)` if there is an error during the initialization process.
    pub async fn build(self, cancel_token: CancellationToken) -> Result<SqsSource> {
        if self.config.queue_names.is_empty() {
            return Err(SqsSourceError::from(Error::InvalidConfig(
                "at least one queue name must be configured for the SQS source".to_string(),
            )));
        }

        // Names are the ack/nack routing key, so they must be unique and usable.
        let mut seen = std::collections::HashSet::new();
        for queue_name in &self.config.queue_names {
            if queue_name.is_empty() {
                return Err(SqsSourceError::from(Error::InvalidConfig(
                    "SQS queue name must not be empty".to_string(),
                )));
            }
            if !seen.insert(*queue_name) {
                return Err(SqsSourceError::from(Error::InvalidConfig(format!(
                    "duplicate SQS queue name: {queue_name}"
                ))));
            }
        }

        let shared_client = match self.client {
            Some(client) => Some(client),
            None if self.clients.is_none() => {
                Some(crate::create_sqs_client(SqsConfig::Source(self.config.clone())).await?)
            }
            None => None,
        };

        let mut resolved_queues = Vec::with_capacity(self.config.queue_names.len());
        for (queue_index, queue_name) in self.config.queue_names.iter().enumerate() {
            // Clone the shared AWS client; each actor still gets its own queue URL.
            let sqs_client = self
                .clients
                .as_ref()
                .and_then(|clients| clients.get(queue_index))
                .cloned()
                .or_else(|| shared_client.clone())
                .ok_or_else(|| {
                    Error::InvalidConfig(format!("missing SQS client for queue {queue_name}"))
                })?;

            let get_queue_url_output = sqs_client
                .get_queue_url()
                .queue_name(*queue_name)
                .queue_owner_aws_account_id(self.config.queue_owner_aws_account_id)
                .send()
                .await
                .map_err(|err| Error::Sqs(extract_aws_error(&err)))?;

            let queue_url = get_queue_url_output
                .queue_url
                .ok_or_else(|| Error::Other("Queue URL not found".to_string()))?;

            tracing::info!(
                queue_url,
                queue_name = *queue_name,
                region = self.config.region,
                "Queue URL found"
            );

            resolved_queues.push((*queue_name, sqs_client, queue_url));
        }

        let terminal_error: TerminalError = Arc::new(OnceLock::new());
        let mut actors = HashMap::with_capacity(resolved_queues.len());
        let mut queue_order = Vec::with_capacity(resolved_queues.len());
        for (queue_name, sqs_client, queue_url) in resolved_queues {
            let (handler_tx, handler_rx) = mpsc::channel(10);
            let config = self.config.clone();
            let actor_cancel_token = cancel_token.clone();
            let actor_terminal_error = Arc::clone(&terminal_error);
            tokio::spawn(async move {
                let mut actor = SqsActor::new(
                    handler_rx,
                    sqs_client,
                    queue_url,
                    queue_name,
                    config,
                    actor_cancel_token,
                    actor_terminal_error,
                );
                actor.run().await;
            });
            actors.insert(queue_name, handler_tx);
            queue_order.push(queue_name);
        }

        Ok(SqsSource {
            batch_size: self.batch_size,
            timeout: self.timeout,
            actors: Arc::new(actors),
            queue_order: Arc::new(queue_order),
            read_cursor: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            terminal_error,
            vertex_replica: self.vertex_replica,
        })
    }
}

impl SqsSource {
    /// Read messages from every queue under one shared deadline, returning as soon
    /// as any queue has data. An idle or slow queue no longer holds back the batch;
    /// whatever it eventually receives is buffered by its actor and served first on
    /// the next read.
    ///
    /// The batch budget is split fairly across queues and remainder allocation
    /// rotates, so no queue is permanently favored.
    pub async fn read_messages(&self) -> Option<Result<Vec<SqsMessage>>> {
        tracing::debug!("Reading messages from SQS");

        // Checked before fanning out: nothing is in hand yet, so a queue that went
        // terminal on an earlier read can fail the source without losing messages.
        // It also means a terminal queue's buffered messages are never drained —
        // correct, since the vertex is stopping and those handles will redeliver
        // once their visibility timeout expires.
        if let Some(err) = self.terminal_error.get() {
            return Some(Err(SqsSourceError::from(Error::Sqs(err.clone()))));
        }

        let start = Instant::now();
        let timeout_at = start + self.timeout;
        let num_queues = self.queue_order.len();
        if num_queues == 0 {
            return Some(Err(SqsSourceError::from(Error::InvalidConfig(
                "at least one SQS queue actor is required".to_string(),
            ))));
        }

        let base = self.batch_size / num_queues;
        let remainder = self.batch_size % num_queues;
        let cursor = self
            .read_cursor
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        // Send all requests before awaiting replies so long polls run concurrently.
        // One channel shared by every queue means responses arrive in completion
        // order instead of configuration order.
        let (tx, mut rx) = mpsc::channel(num_queues);
        let mut expected = 0usize;
        for (idx, queue_name) in self.queue_order.iter().enumerate() {
            let extra =
                usize::from((idx + num_queues - cursor % num_queues) % num_queues < remainder);
            let count = base + extra;
            if count == 0 {
                continue;
            }

            let Some(actor_tx) = self.actors.get(queue_name) else {
                continue;
            };
            let msg = SQSActorMessage::Receive {
                respond_to: tx.clone(),
                count: count as i32,
                timeout_at,
            };
            // try_send, not send().await: once the reader stops waiting for
            // stragglers it can lap a slow actor, and a full inbox would put the
            // barrier back on the send side.
            match actor_tx.try_send(msg) {
                Ok(()) => expected += 1,
                Err(mpsc::error::TrySendError::Full(_)) => {
                    tracing::debug!(queue_name, "SQS actor busy, skipping this read");
                }
                Err(mpsc::error::TrySendError::Closed(_)) => {
                    let _ = self
                        .terminal_error
                        .set(format!("SQS actor for queue {queue_name} terminated"));
                }
            }
        }
        // Dropped so the receiver sees closure once every actor has replied.
        drop(tx);

        // SQS requires wait_time_seconds >= 1, so an actor's long poll can outlast a
        // shorter configured timeout. The reader must not give up before then or it
        // would never see a response; it still returns the moment a queue has data.
        let deadline = timeout_at.max(start + Duration::from_secs(1));

        let mut messages = Vec::new();
        let mut responded = 0usize;
        let mut all_cancelled = true;
        while responded < expected {
            let Ok(Some((queue_name, response))) =
                tokio::time::timeout_at(deadline, rx.recv()).await
            else {
                break; // deadline reached, or every actor already replied
            };
            responded += 1;
            Self::collect_response(queue_name, response, &mut messages, &mut all_cancelled);

            if !messages.is_empty() {
                // Take whatever else already landed, then stop waiting on stragglers.
                while let Ok((queue_name, response)) = rx.try_recv() {
                    responded += 1;
                    Self::collect_response(queue_name, response, &mut messages, &mut all_cancelled);
                }
                break;
            }
        }

        if !messages.is_empty() {
            // A queue may have gone terminal during this round. Returning what we
            // have is safe: the slot is sticky, so the next read surfaces it.
            return Some(Ok(messages));
        }

        if let Some(err) = self.terminal_error.get() {
            return Some(Err(SqsSourceError::from(Error::Sqs(err.clone()))));
        }

        // Every actor that was asked reported cancellation, i.e. the source is
        // shutting down. Skipped actors do not count: a round where every inbox
        // happened to be busy is an empty read, not the end of the stream.
        if all_cancelled && expected > 0 && responded == expected {
            return None;
        }

        Some(Ok(Vec::new()))
    }

    /// Folds one queue's response into the batch. An `Err` payload is dropped on
    /// purpose: the terminal-error slot is the only escalation path, because a
    /// response can always arrive after the reader has walked away.
    fn collect_response(
        queue_name: &'static str,
        response: Option<Result<Vec<SqsMessage>>>,
        messages: &mut Vec<SqsMessage>,
        all_cancelled: &mut bool,
    ) {
        match response {
            Some(Ok(mut queue_messages)) => {
                *all_cancelled = false;
                messages.append(&mut queue_messages);
            }
            Some(Err(err)) => {
                *all_cancelled = false;
                tracing::warn!(?err, queue_name, "SQS receive failed for queue");
            }
            None => {}
        }
    }

    /// Acknowledge receipt handles against the queues that issued them.
    pub async fn ack_offsets(&self, offsets: Vec<(String, Bytes)>) -> Result<()> {
        tracing::debug!(offsets = ?offsets, "Acknowledging offsets");

        let mut grouped = HashMap::<String, Vec<Bytes>>::new();
        for (queue_name, receipt_handle) in offsets {
            grouped.entry(queue_name).or_default().push(receipt_handle);
        }

        let mut receivers = Vec::with_capacity(grouped.len());
        for (queue_name, receipt_handles) in grouped {
            let actor_tx = self.actors.get(queue_name.as_str()).ok_or_else(|| {
                SqsSourceError::from(Error::Other(format!("unknown SQS queue {queue_name}")))
            })?;
            let (tx, rx) = oneshot::channel();
            actor_tx
                .send(SQSActorMessage::Delete {
                    offsets: receipt_handles,
                    respond_to: tx,
                })
                .await
                .map_err(|_| {
                    SqsSourceError::from(Error::Other(format!(
                        "SQS actor for queue {queue_name} terminated"
                    )))
                })?;
            receivers.push(rx);
        }

        for rx in receivers {
            rx.await.map_err(Error::ActorTaskTerminated)??;
        }
        Ok(())
    }

    /// Change message visibility for the provided offsets.
    pub async fn nack_offsets(&self, offsets: Vec<SqsNack>) -> Result<()> {
        tracing::debug!(?offsets, "Nacking offsets");

        let mut grouped = HashMap::<String, Vec<SqsNack>>::new();
        for nack in offsets {
            grouped
                .entry(nack.queue_name.clone())
                .or_default()
                .push(nack);
        }

        let mut receivers = Vec::with_capacity(grouped.len());
        for (queue_name, queue_nacks) in grouped {
            let actor_tx = self.actors.get(queue_name.as_str()).ok_or_else(|| {
                SqsSourceError::from(Error::Other(format!("unknown SQS queue {queue_name}")))
            })?;
            let (tx, rx) = oneshot::channel();
            actor_tx
                .send(SQSActorMessage::Nack {
                    offsets: queue_nacks,
                    respond_to: tx,
                })
                .await
                .map_err(|_| {
                    SqsSourceError::from(Error::Other(format!(
                        "SQS actor for queue {queue_name} terminated"
                    )))
                })?;
            receivers.push(rx);
        }

        for rx in receivers {
            rx.await.map_err(Error::ActorTaskTerminated)??;
        }
        Ok(())
    }

    /// get the pending message count from SQS
    /// corresponding sqs sdk method is get_queue_attributes
    /// with the attribute name ApproximateNumberOfMessages
    pub async fn pending_count(&self) -> Option<usize> {
        let mut receivers = Vec::with_capacity(self.queue_order.len());
        for queue_name in self.queue_order.iter() {
            let actor_tx = self.actors.get(queue_name)?;
            let (tx, rx) = oneshot::channel();
            if actor_tx
                .send(SQSActorMessage::GetPending { respond_to: tx })
                .await
                .is_err()
            {
                return None;
            }
            receivers.push(rx);
        }

        let mut total = 0usize;
        for rx in receivers {
            match rx.await {
                Ok(Ok(Some(count))) => total = total.saturating_add(count),
                // Any queue failure returns None so autoscaling never sees a partial sum.
                _ => return None,
            }
        }
        tracing::debug!(pending_count = total, "Pending message count retrieved");
        Some(total)
    }

    /// Returns the partitions for the SQS source.
    ///
    /// This method is currently unimplemented in this module.
    /// Note: It is implemented in the core to return the current vertex replica.
    /// See `numaflow-core/src/source/sqs.rs` for the implementation.
    pub fn partitions(&self) -> Vec<u16> {
        vec![self.vertex_replica]
    }
}

#[cfg(test)]
mod tests {
    use aws_sdk_sqs::Config;
    use aws_sdk_sqs::types::MessageAttributeValue;
    use aws_smithy_mocks::{MockResponseInterceptor, Rule, RuleMode, mock};
    use aws_smithy_types::error::ErrorMetadata;
    use test_log::test;

    use super::*;

    #[tokio::test]
    async fn test_client_creation_with_defaults() {
        let config = SqsSourceConfig {
            region: "us-west-2",
            queue_names: vec!["test-queue"],
            queue_owner_aws_account_id: "123456789012",
            visibility_timeout: None,
            max_number_of_messages: None,
            wait_time_seconds: None,
            endpoint_url: None,
            attribute_names: vec![],
            message_attribute_names: vec![],
            assume_role_config: None,
        };

        let result = crate::create_sqs_client(SqsConfig::Source(config.clone())).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_client_creation_with_custom_endpoint() {
        let mut config = SqsSourceConfig {
            region: "us-west-2",
            queue_names: vec!["test-queue"],
            queue_owner_aws_account_id: "123456789012",
            visibility_timeout: Some(30),
            max_number_of_messages: Some(5),
            wait_time_seconds: Some(10),
            endpoint_url: Some("http://localhost:4566".to_string()),
            attribute_names: vec!["All".to_string()],
            message_attribute_names: vec!["All".to_string()],
            assume_role_config: None,
        };

        let result = crate::create_sqs_client(SqsConfig::Source(config.clone())).await;
        assert!(result.is_ok());

        // Test with invalid endpoint
        config.endpoint_url = Some("invalid-url".to_string());
        let result = crate::create_sqs_client(SqsConfig::Source(config)).await;
        assert!(result.is_ok()); // The URL is validated when making requests, not during client creation
    }

    #[test(tokio::test)]
    async fn test_sqssourcehandle_read() {
        let queue_url_output = get_queue_url_output();

        let receive_message_output = get_receive_message_output();

        let sqs_operation_mocks = MockResponseInterceptor::new()
            .rule_mode(RuleMode::MatchAny)
            .with_rule(&queue_url_output)
            .with_rule(&receive_message_output);

        let sqs_mock_client =
            Client::from_conf(get_test_config_with_interceptor(sqs_operation_mocks));

        let source = SqsSourceBuilder::new(SqsSourceConfig {
            region: SQS_DEFAULT_REGION,
            queue_names: vec!["test-q"],
            queue_owner_aws_account_id: "123456789012",
            visibility_timeout: Some(300),
            max_number_of_messages: None,
            wait_time_seconds: None,
            endpoint_url: None,
            attribute_names: vec![MessageSystemAttributeName::SentTimestamp.to_string()],
            message_attribute_names: vec![MessageSystemAttributeName::AwsTraceHeader.to_string()],
            assume_role_config: None,
        })
        .batch_size(1)
        .timeout(Duration::from_secs(0))
        .client(sqs_mock_client)
        .build(CancellationToken::new())
        .await
        .unwrap();

        // Read messages from the source
        let messages = source.read_messages().await.unwrap().unwrap();

        // Assert we got the expected number of messages
        assert_eq!(messages.len(), 1, "Should receive exactly 1 message");

        // Verify first message
        let msg1 = messages.first().expect("Expected message 1");
        assert_eq!(msg1.key, "219f8380-5770-4cc2-8c3e-5c715e145f5e");
        assert_eq!(msg1.payload, "This is a test message");
        assert_eq!(
            msg1.offset,
            "AQEBaZ+j5qUoOAoxlmrCQPkBm9njMWXqemmIG6shMHCO6fV20JrQYg/AiZ8JELwLwOu5U61W+aIX5Qzu7GGofxJuvzymr4Ph53RiR0mudj4InLSgpSspYeTRDteBye5tV/txbZDdNZxsi+qqZA9xPnmMscKQqF6pGhnGIKrnkYGl45Nl6GPIZv62LrIRb6mSqOn1fn0yqrvmWuuY3w2UzQbaYunJWGxpzZze21EOBtywknU3Je/g7G9is+c6K9hGniddzhLkK1tHzZKjejOU4jokaiB4nmi0dF3JqLzDsQuPF0Gi8qffhEvw56nl8QCbluSJScFhJYvoagGnDbwOnd9z50L239qtFIgETdpKyirlWwl/NGjWJ45dqWpiW3d2Ws7q"
        );
        assert_eq!(msg1.system_attributes.len(), 1);
        assert!(msg1.system_attributes.contains_key("SentTimestamp"));

        assert!(msg1.custom_attributes.contains_key("sqs"));
        let sqs_attrs = msg1.custom_attributes.get("sqs").unwrap();
        assert!(sqs_attrs.contains_key("AwsTraceHeader"));

        // test another config

        let sqs_operation_mocks = MockResponseInterceptor::new()
            .rule_mode(RuleMode::MatchAny)
            .with_rule(&queue_url_output)
            .with_rule(&receive_message_output);
        let sqs_mock_client =
            Client::from_conf(get_test_config_with_interceptor(sqs_operation_mocks));
        let source = SqsSourceBuilder::new(SqsSourceConfig {
            region: SQS_DEFAULT_REGION,
            queue_names: vec!["test-q"],
            queue_owner_aws_account_id: "123456789012",
            visibility_timeout: Some(300),
            max_number_of_messages: None,
            wait_time_seconds: None,
            endpoint_url: None,
            attribute_names: vec![],
            message_attribute_names: vec![],
            assume_role_config: None,
        })
        .batch_size(1)
        .timeout(Duration::from_secs(0))
        .client(sqs_mock_client)
        .build(CancellationToken::new())
        .await
        .unwrap();

        // Read messages from the source
        let messages = source.read_messages().await.unwrap().unwrap();

        // Assert we got the expected number of messages
        assert_eq!(messages.len(), 1, "Should receive exactly 1 message");
    }

    /// A single receive failure is now absorbed: it is reported as an empty poll so
    /// a transient SQS error does not restart the vertex.
    #[test(tokio::test)]
    async fn test_sqssourcehandle_read_error() {
        let queue_url_output = get_queue_url_output();

        let receive_message_output = get_receive_message_error();

        let sqs_operation_mocks = MockResponseInterceptor::new()
            .rule_mode(RuleMode::MatchAny)
            .with_rule(&queue_url_output)
            .with_rule(&receive_message_output);

        let sqs_mock_client =
            Client::from_conf(get_test_config_with_interceptor(sqs_operation_mocks));

        let source = SqsSourceBuilder::new(SqsSourceConfig {
            region: SQS_DEFAULT_REGION,
            queue_names: vec!["test-q"],
            queue_owner_aws_account_id: "123456789012",
            visibility_timeout: Some(300),
            max_number_of_messages: None,
            wait_time_seconds: None,
            endpoint_url: None,
            attribute_names: vec![MessageSystemAttributeName::SentTimestamp.to_string()],
            message_attribute_names: vec![MessageSystemAttributeName::AwsTraceHeader.to_string()],
            assume_role_config: None,
        })
        .batch_size(1)
        .timeout(Duration::from_secs(0))
        .client(sqs_mock_client)
        .build(CancellationToken::new())
        .await
        .unwrap();

        let messages = source.read_messages().await;
        assert!(
            matches!(messages, Some(Ok(ref batch)) if batch.is_empty()),
            "a single receive failure should look like an empty poll, got {messages:?}"
        );
    }

    /// A queue that never recovers still fails the vertex, but only once it has
    /// burned through the retry budget.
    #[test(tokio::test)]
    async fn test_receive_failures_surface_only_after_threshold() {
        let queue_url_output = get_queue_url_output();
        let receive_message_error = get_receive_message_error();

        let sqs_mock_client = Client::from_conf(get_test_config_with_interceptor(
            MockResponseInterceptor::new()
                .rule_mode(RuleMode::MatchAny)
                .with_rule(&queue_url_output)
                .with_rule(&receive_message_error),
        ));

        let source = SqsSourceBuilder::new(SqsSourceConfig {
            region: SQS_DEFAULT_REGION,
            queue_names: vec!["test-q"],
            queue_owner_aws_account_id: "123456789012",
            visibility_timeout: Some(300),
            max_number_of_messages: None,
            wait_time_seconds: None,
            endpoint_url: None,
            attribute_names: vec![],
            message_attribute_names: vec![],
            assume_role_config: None,
        })
        .batch_size(1)
        .timeout(Duration::from_secs(0))
        .client(sqs_mock_client)
        .build(CancellationToken::new())
        .await
        .unwrap();

        for attempt in 1..=MAX_CONSECUTIVE_RECEIVE_FAILURES {
            let result = source.read_messages().await;
            assert!(
                matches!(result, Some(Ok(ref batch)) if batch.is_empty()),
                "failure {attempt} should still be tolerated, got {result:?}"
            );
        }

        let result = source.read_messages().await;
        assert!(matches!(result, Some(Err(err)) if err.to_string().contains("InvalidAddress")));

        // The error is sticky: the source stays failed rather than silently recovering.
        let result = source.read_messages().await;
        assert!(matches!(result, Some(Err(err)) if err.to_string().contains("InvalidAddress")));
    }

    #[test(tokio::test)]
    async fn test_sqssource_ack() {
        let queue_url_output = get_queue_url_output();
        let delete_message_output = get_delete_message_output();

        let sqs_operation_mocks = MockResponseInterceptor::new()
            .rule_mode(RuleMode::MatchAny)
            .with_rule(&queue_url_output)
            .with_rule(&delete_message_output);

        let sqs_mock_client =
            Client::from_conf(get_test_config_with_interceptor(sqs_operation_mocks));

        let source = SqsSourceBuilder::new(SqsSourceConfig {
            region: SQS_DEFAULT_REGION,
            queue_names: vec!["test-q"],
            queue_owner_aws_account_id: "123456789012",
            visibility_timeout: None,
            max_number_of_messages: None,
            wait_time_seconds: None,
            endpoint_url: None,
            attribute_names: vec![],
            message_attribute_names: vec![],
            assume_role_config: None,
        })
        .batch_size(1)
        .timeout(Duration::from_secs(0))
        .client(sqs_mock_client)
        .build(CancellationToken::new())
        .await
        .unwrap();

        // Test acknowledgment
        let offset = "AQEBaZ+j5qUoOAoxlmrCQPkBm9njMWXqemmIG6shMHCO6fV20JrQYg/AiZ8JELwLwOu5U61W+aIX5Qzu7GGofxJuvzymr4Ph53RiR0mudj4InLSgpSspYeTRDteBye5tV/txbZDdNZxsi+qqZA9xPnmMscKQqF6pGhnGIKrnkYGl45Nl6GPIZv62LrIRb6mSqOn1fn0yqrvmWuuY3w2UzQbaYunJWGxpzZze21EOBtywknU3Je/g7G9is+c6K9hGniddzhLkK1tHzZKjejOU4jokaiB4nmi0dF3JqLzDsQuPF0Gi8qffhEvw56nl8QCbluSJScFhJYvoagGnDbwOnd9z50L239qtFIgETdpKyirlWwl/NGjWJ45dqWpiW3d2Ws7q";
        let result = source
            .ack_offsets(vec![("test-q".to_string(), Bytes::from(offset))])
            .await;
        assert!(result.is_ok());
    }

    #[test(tokio::test)]
    async fn test_sqssource_ack_error() {
        let queue_url_output = get_queue_url_output();
        let delete_message_output = get_delete_message_error();

        let sqs_operation_mocks = MockResponseInterceptor::new()
            .rule_mode(RuleMode::MatchAny)
            .with_rule(&queue_url_output)
            .with_rule(&delete_message_output);

        let sqs_mock_client =
            Client::from_conf(get_test_config_with_interceptor(sqs_operation_mocks));

        let source = SqsSourceBuilder::new(SqsSourceConfig {
            region: SQS_DEFAULT_REGION,
            queue_names: vec!["test-q"],
            queue_owner_aws_account_id: "123456789012",
            visibility_timeout: None,
            max_number_of_messages: None,
            wait_time_seconds: None,
            endpoint_url: None,
            attribute_names: vec![],
            message_attribute_names: vec![],
            assume_role_config: None,
        })
        .batch_size(1)
        .timeout(Duration::from_secs(0))
        .client(sqs_mock_client)
        .build(CancellationToken::new())
        .await
        .unwrap();

        // Test acknowledgment
        let offset = "AQEBaZ+j5qUoOAoxlmrCQPkBm9njMWXqemmIG6shMHCO6fV20JrQYg/AiZ8JELwLwOu5U61W+aIX5Qzu7GGofxJuvzymr4Ph53RiR0mudj4InLSgpSspYeTRDteBye5tV/txbZDdNZxsi+qqZA9xPnmMscKQqF6pGhnGIKrnkYGl45Nl6GPIZv62LrIRb6mSqOn1fn0yqrvmWuuY3w2UzQbaYunJWGxpzZze21EOBtywknU3Je/g7G9is+c6K9hGniddzhLkK1tHzZKjejOU4jokaiB4nmi0dF3JqLzDsQuPF0Gi8qffhEvw56nl8QCbluSJScFhJYvoagGnDbwOnd9z50L239qtFIgETdpKyirlWwl/NGjWJ45dqWpiW3d2Ws7q";
        let result = source
            .ack_offsets(vec![("test-q".to_string(), Bytes::from(offset))])
            .await;
        assert!(result.is_err());
    }

    #[test(tokio::test)]
    async fn test_sqssource_ack_reports_partial_batch_failure() {
        let queue_url_output = get_queue_url_output();
        let partial_failure = mock!(aws_sdk_sqs::Client::delete_message_batch)
            .match_requests(|input| input.entries().len() == 1)
            .then_output(|| {
                aws_sdk_sqs::operation::delete_message_batch::DeleteMessageBatchOutput::builder()
                    .set_successful(Some(Vec::new()))
                    .failed(
                        aws_sdk_sqs::types::BatchResultErrorEntry::builder()
                            .id("0")
                            .code("InternalError")
                            .message("delete failed")
                            .sender_fault(false)
                            .build()
                            .unwrap(),
                    )
                    .build()
                    .unwrap()
            });
        let client = Client::from_conf(get_test_config_with_interceptor(
            MockResponseInterceptor::new()
                .rule_mode(RuleMode::MatchAny)
                .with_rule(&queue_url_output)
                .with_rule(&partial_failure),
        ));
        let source = SqsSourceBuilder::new(SqsSourceConfig {
            region: SQS_DEFAULT_REGION,
            queue_names: vec!["test-q"],
            queue_owner_aws_account_id: "123456789012",
            visibility_timeout: None,
            max_number_of_messages: None,
            wait_time_seconds: None,
            endpoint_url: None,
            attribute_names: vec![],
            message_attribute_names: vec![],
            assume_role_config: None,
        })
        .client(client)
        .build(CancellationToken::new())
        .await
        .unwrap();

        let err = source
            .ack_offsets(vec![("test-q".to_string(), Bytes::from("receipt"))])
            .await
            .unwrap_err();
        assert!(err.to_string().contains("InternalError"));
    }

    #[test(tokio::test)]
    async fn test_sqssource_pending_count() {
        let queue_url_output = get_queue_url_output();
        let queue_attrs_output = get_queue_attributes_output();

        let sqs_operation_mocks = MockResponseInterceptor::new()
            .rule_mode(RuleMode::MatchAny)
            .with_rule(&queue_url_output)
            .with_rule(&queue_attrs_output);

        let sqs_mock_client =
            Client::from_conf(get_test_config_with_interceptor(sqs_operation_mocks));

        let source = SqsSourceBuilder::new(SqsSourceConfig {
            region: SQS_DEFAULT_REGION,
            queue_names: vec!["test-q"],
            queue_owner_aws_account_id: "123456789012",
            visibility_timeout: None,
            max_number_of_messages: None,
            wait_time_seconds: None,
            endpoint_url: None,
            attribute_names: vec![],
            message_attribute_names: vec![],
            assume_role_config: None,
        })
        .batch_size(1)
        .timeout(Duration::from_secs(0))
        .client(sqs_mock_client)
        .build(CancellationToken::new())
        .await
        .unwrap();

        let count = source.pending_count().await;
        assert_eq!(count, Some(0));
    }

    #[test(tokio::test)]
    async fn test_sqssource_pending_count_error() {
        let queue_url_output = get_queue_url_output();
        let queue_attrs_output = get_queue_attributes_error();

        let sqs_operation_mocks = MockResponseInterceptor::new()
            .rule_mode(RuleMode::MatchAny)
            .with_rule(&queue_url_output)
            .with_rule(&queue_attrs_output);

        let sqs_mock_client =
            Client::from_conf(get_test_config_with_interceptor(sqs_operation_mocks));

        let source = SqsSourceBuilder::new(SqsSourceConfig {
            region: SQS_DEFAULT_REGION,
            queue_names: vec!["test-q"],
            queue_owner_aws_account_id: "123456789012",
            visibility_timeout: None,
            max_number_of_messages: None,
            wait_time_seconds: None,
            endpoint_url: None,
            attribute_names: vec![],
            message_attribute_names: vec![],
            assume_role_config: None,
        })
        .batch_size(1)
        .timeout(Duration::from_secs(0))
        .client(sqs_mock_client)
        .build(CancellationToken::new())
        .await
        .unwrap();

        let count = source.pending_count().await;
        assert_eq!(count, None);
    }

    #[test(tokio::test)]
    async fn test_error_cases() {
        // Test invalid region error
        let sqs_operation_mocks = MockResponseInterceptor::new()
            .rule_mode(RuleMode::MatchAny)
            .with_rule(&get_queue_url_output_err());

        let sqs_mock_client =
            Client::from_conf(get_test_config_with_interceptor(sqs_operation_mocks));

        let source = SqsSourceBuilder::new(SqsSourceConfig {
            region: SQS_DEFAULT_REGION,
            queue_names: vec!["test-q"],
            queue_owner_aws_account_id: "123456789012",
            visibility_timeout: None,
            max_number_of_messages: None,
            wait_time_seconds: None,
            endpoint_url: None,
            attribute_names: vec![],
            message_attribute_names: vec![],
            assume_role_config: None,
        })
        .batch_size(1)
        .timeout(Duration::from_secs(0))
        .client(sqs_mock_client)
        .build(CancellationToken::new())
        .await;
        assert!(source.is_err());
    }

    /// Assembles an `SqsSource` around caller-supplied actor channels so read and
    /// routing behavior can be exercised without an SQS mock behind every queue.
    fn source_with_actors(
        queues: Vec<(&'static str, mpsc::Sender<SQSActorMessage>)>,
        batch_size: usize,
        timeout: Duration,
        vertex_replica: u16,
    ) -> SqsSource {
        let queue_order: Vec<&'static str> = queues.iter().map(|(name, _)| *name).collect();
        SqsSource {
            batch_size,
            timeout,
            actors: Arc::new(queues.into_iter().collect()),
            queue_order: Arc::new(queue_order),
            read_cursor: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            terminal_error: Arc::new(OnceLock::new()),
            vertex_replica,
        }
    }

    fn fake_message(queue_name: &'static str, id: &str) -> SqsMessage {
        SqsMessage {
            key: id.to_string(),
            payload: Bytes::from(id.to_string()),
            offset: format!("receipt-{id}"),
            event_time: Utc::now(),
            queue_name,
            system_attributes: HashMap::new(),
            custom_attributes: HashMap::new(),
        }
    }

    /// Answers every `Receive` with a single message, immediately.
    fn spawn_responsive_actor(queue_name: &'static str) -> mpsc::Sender<SQSActorMessage> {
        let (tx, mut rx) = mpsc::channel(10);
        tokio::spawn(async move {
            while let Some(msg) = rx.recv().await {
                if let SQSActorMessage::Receive { respond_to, .. } = msg {
                    let batch = vec![fake_message(queue_name, queue_name)];
                    let _ = respond_to.send((queue_name, Some(Ok(batch)))).await;
                }
            }
        });
        tx
    }

    #[tokio::test]
    async fn test_partitions_unimplemented() {
        let source = source_with_actors(
            vec![("test-q", mpsc::channel(1).0)],
            1,
            Duration::from_secs(0),
            1,
        );
        assert_eq!(source.partitions(), vec![1]);
    }

    /// The regression test for a silent queue holding up a queue that has data.
    #[tokio::test]
    async fn test_read_returns_without_waiting_for_silent_queue() {
        // Receiver kept alive but never polled, so this queue never answers.
        let (silent_tx, _silent_rx) = mpsc::channel(10);
        let source = source_with_actors(
            vec![
                ("orders-queue", spawn_responsive_actor("orders-queue")),
                ("refunds-queue", silent_tx),
            ],
            2,
            Duration::from_secs(30),
            0,
        );

        let messages = tokio::time::timeout(Duration::from_secs(2), source.read_messages())
            .await
            .expect("read should not wait for the silent queue")
            .unwrap()
            .unwrap();
        let [message] = messages.as_slice() else {
            panic!("expected exactly one message, got {messages:?}");
        };
        assert_eq!(message.queue_name, "orders-queue");
    }

    /// A backed-up actor inbox must be skipped for the round rather than blocking
    /// the fan-out, which would put the barrier back on the send side.
    #[tokio::test]
    async fn test_stuck_actor_does_not_stall_healthy_queue() {
        let (stuck_tx, _stuck_rx) = mpsc::channel(1);
        // Fill the inbox so every later try_send reports Full.
        stuck_tx
            .send(SQSActorMessage::GetPending {
                respond_to: oneshot::channel().0,
            })
            .await
            .unwrap();

        let source = source_with_actors(
            vec![
                ("orders-queue", spawn_responsive_actor("orders-queue")),
                ("refunds-queue", stuck_tx),
            ],
            2,
            Duration::from_secs(30),
            0,
        );

        for _ in 0..3 {
            let messages = tokio::time::timeout(Duration::from_secs(2), source.read_messages())
                .await
                .expect("read should skip the busy actor")
                .unwrap()
                .unwrap();
            let [message] = messages.as_slice() else {
                panic!("expected exactly one message, got {messages:?}");
            };
            assert_eq!(message.queue_name, "orders-queue");
        }
    }

    #[tokio::test]
    async fn test_sqs_source_builder() {
        // test default
        let builder = SqsSourceBuilder::default();
        assert_eq!(builder.batch_size, 1);
        assert_eq!(builder.timeout, Duration::from_secs(1));

        // test with vertex replica
        let builder = SqsSourceBuilder::default().vertex_replica(2);
        assert_eq!(builder.vertex_replica, 2);

        // test with custom config
        let config = SqsSourceConfig {
            region: "us-east-2",
            queue_names: vec!["test-queue-custom"],
            queue_owner_aws_account_id: "123456789012",
            visibility_timeout: Some(300),
            max_number_of_messages: Some(2000),
            wait_time_seconds: Some(10),
            endpoint_url: None,
            attribute_names: vec![],
            message_attribute_names: vec![],
            assume_role_config: None,
        };
        let builder = SqsSourceBuilder::default().config(config);
        assert_eq!(builder.config.region, "us-east-2");
        assert_eq!(builder.config.queue_names, vec!["test-queue-custom"]);
        assert_eq!(builder.config.queue_owner_aws_account_id, "123456789012");
        assert_eq!(builder.config.visibility_timeout, Some(300));
        assert_eq!(builder.config.max_number_of_messages, Some(2000));
        assert_eq!(builder.config.wait_time_seconds, Some(10));
    }

    const ORDERS_URL: &str = "https://sqs.us-west-2.amazonaws.com/111111111111/orders-queue/";
    const REFUNDS_URL: &str = "https://sqs.us-west-2.amazonaws.com/111111111111/refunds-queue/";

    fn multi_queue_config() -> SqsSourceConfig {
        SqsSourceConfig {
            region: SQS_DEFAULT_REGION,
            queue_names: vec!["orders-queue", "refunds-queue"],
            queue_owner_aws_account_id: "111111111111",
            visibility_timeout: None,
            max_number_of_messages: None,
            wait_time_seconds: None,
            endpoint_url: None,
            attribute_names: vec![],
            message_attribute_names: vec![],
            assume_role_config: None,
        }
    }

    fn get_queue_url_output_for(queue_name: &'static str, queue_url: &'static str) -> Rule {
        mock!(aws_sdk_sqs::Client::get_queue_url)
            .match_requests(move |input| {
                input.queue_name() == Some(queue_name)
                    && input.queue_owner_aws_account_id() == Some("111111111111")
            })
            .then_output(move || {
                aws_sdk_sqs::operation::get_queue_url::GetQueueUrlOutput::builder()
                    .queue_url(queue_url)
                    .build()
            })
    }

    fn get_receive_message_output_for(queue_url: &'static str, body: &'static str) -> Rule {
        mock!(aws_sdk_sqs::Client::receive_message)
            .match_requests(move |input| input.queue_url() == Some(queue_url))
            .then_output(move || {
                aws_sdk_sqs::operation::receive_message::ReceiveMessageOutput::builder()
                    .messages(
                        aws_sdk_sqs::types::Message::builder()
                            .message_id(body)
                            .body(body)
                            .receipt_handle(format!("receipt-{body}"))
                            .attributes(MessageSystemAttributeName::SentTimestamp, "1677112427387")
                            .build(),
                    )
                    .build()
            })
    }

    fn get_delete_message_output_for(queue_url: &'static str) -> Rule {
        mock!(aws_sdk_sqs::Client::delete_message_batch)
            .match_requests(move |input| input.queue_url() == Some(queue_url))
            .then_output(|| {
                aws_sdk_sqs::operation::delete_message_batch::DeleteMessageBatchOutput::builder()
                    .successful(
                        aws_sdk_sqs::types::DeleteMessageBatchResultEntry::builder()
                            .id("0")
                            .build()
                            .unwrap(),
                    )
                    .set_failed(Some(Vec::new()))
                    .build()
                    .unwrap()
            })
    }

    fn get_change_visibility_output_for(
        queue_url: &'static str,
        receipt_handle: &'static str,
    ) -> Rule {
        mock!(aws_sdk_sqs::Client::change_message_visibility)
            .match_requests(move |input| {
                input.queue_url() == Some(queue_url)
                    && input.receipt_handle() == Some(receipt_handle)
                    && input.visibility_timeout() == Some(30)
            })
            .then_output(|| {
                aws_sdk_sqs::operation::change_message_visibility::ChangeMessageVisibilityOutput::builder()
                    .build()
            })
    }

    fn get_queue_attributes_output_for(queue_url: &'static str, count: &'static str) -> Rule {
        mock!(aws_sdk_sqs::Client::get_queue_attributes)
            .match_requests(move |input| input.queue_url() == Some(queue_url))
            .then_output(move || {
                aws_sdk_sqs::operation::get_queue_attributes::GetQueueAttributesOutput::builder()
                    .attributes(
                        aws_sdk_sqs::types::QueueAttributeName::ApproximateNumberOfMessages,
                        count,
                    )
                    .build()
            })
    }

    #[test(tokio::test)]
    async fn test_multi_queue_read_merges_messages_and_routes_acks() {
        let orders_url = get_queue_url_output_for("orders-queue", ORDERS_URL);
        let refunds_url = get_queue_url_output_for("refunds-queue", REFUNDS_URL);
        let orders_receive = get_receive_message_output_for(ORDERS_URL, "orders-message");
        let refunds_receive = get_receive_message_output_for(REFUNDS_URL, "refunds-message");
        let orders_delete = get_delete_message_output_for(ORDERS_URL);
        let refunds_delete = get_delete_message_output_for(REFUNDS_URL);

        let orders_mocks = MockResponseInterceptor::new()
            .rule_mode(RuleMode::MatchAny)
            .with_rule(&orders_url)
            .with_rule(&orders_receive)
            .with_rule(&orders_delete);
        let refunds_mocks = MockResponseInterceptor::new()
            .rule_mode(RuleMode::MatchAny)
            .with_rule(&refunds_url)
            .with_rule(&refunds_receive)
            .with_rule(&refunds_delete);

        let source = SqsSourceBuilder::new(multi_queue_config())
            .clients(vec![
                Client::from_conf(get_test_config_with_interceptor(orders_mocks)),
                Client::from_conf(get_test_config_with_interceptor(refunds_mocks)),
            ])
            .batch_size(10)
            .timeout(Duration::from_secs(0))
            .build(CancellationToken::new())
            .await
            .unwrap();

        // A read returns as soon as any queue answers, so a sibling's first message
        // can land on a later read.
        let mut messages = Vec::new();
        for _ in 0..5 {
            messages.extend(source.read_messages().await.unwrap().unwrap());
            if messages.iter().any(|m| m.queue_name == "orders-queue")
                && messages.iter().any(|m| m.queue_name == "refunds-queue")
            {
                break;
            }
        }

        let orders = messages
            .iter()
            .find(|message| message.payload == "orders-message")
            .expect("orders queue should deliver");
        let refunds = messages
            .iter()
            .find(|message| message.payload == "refunds-message")
            .expect("refunds queue should deliver");
        assert_eq!(orders.queue_name, "orders-queue");
        assert_eq!(refunds.queue_name, "refunds-queue");
        assert_eq!(orders.system_attributes.len(), 1);
        assert!(!orders.system_attributes.contains_key("queue_name"));
        assert!(!orders.system_attributes.contains_key("aws_region"));
        assert!(!orders.system_attributes.contains_key("aws_account"));

        let offsets = [orders, refunds]
            .into_iter()
            .map(|message| {
                (
                    message.queue_name.to_string(),
                    Bytes::from(message.offset.clone()),
                )
            })
            .collect();
        source.ack_offsets(offsets).await.unwrap();
        assert_eq!(orders_delete.num_calls(), 1);
        assert_eq!(refunds_delete.num_calls(), 1);
    }

    #[test(tokio::test)]
    async fn test_multi_queue_small_batch_rotates() {
        let orders_url = get_queue_url_output_for("orders-queue", ORDERS_URL);
        let refunds_url = get_queue_url_output_for("refunds-queue", REFUNDS_URL);
        let orders_receive = get_receive_message_output_for(ORDERS_URL, "orders-message");
        let refunds_receive = get_receive_message_output_for(REFUNDS_URL, "refunds-message");

        let source = SqsSourceBuilder::new(multi_queue_config())
            .clients(vec![
                Client::from_conf(get_test_config_with_interceptor(
                    MockResponseInterceptor::new()
                        .rule_mode(RuleMode::MatchAny)
                        .with_rule(&orders_url)
                        .with_rule(&orders_receive),
                )),
                Client::from_conf(get_test_config_with_interceptor(
                    MockResponseInterceptor::new()
                        .rule_mode(RuleMode::MatchAny)
                        .with_rule(&refunds_url)
                        .with_rule(&refunds_receive),
                )),
            ])
            .batch_size(1)
            .timeout(Duration::from_secs(0))
            .build(CancellationToken::new())
            .await
            .unwrap();

        let first = source.read_messages().await.unwrap().unwrap();
        let second = source.read_messages().await.unwrap().unwrap();
        assert_eq!(first.len(), 1);
        assert_eq!(second.len(), 1);
        assert_ne!(first[0].queue_name, second[0].queue_name);
        assert_eq!(orders_receive.num_calls(), 1);
        assert_eq!(refunds_receive.num_calls(), 1);
    }

    #[test(tokio::test)]
    async fn test_multi_queue_nack_routes_to_origin_queue() {
        let orders_url = get_queue_url_output_for("orders-queue", ORDERS_URL);
        let refunds_url = get_queue_url_output_for("refunds-queue", REFUNDS_URL);
        let orders_visibility =
            get_change_visibility_output_for(ORDERS_URL, "receipt-orders-message");
        let refunds_visibility =
            get_change_visibility_output_for(REFUNDS_URL, "receipt-refunds-message");

        let source = SqsSourceBuilder::new(multi_queue_config())
            .clients(vec![
                Client::from_conf(get_test_config_with_interceptor(
                    MockResponseInterceptor::new()
                        .rule_mode(RuleMode::MatchAny)
                        .with_rule(&orders_url)
                        .with_rule(&orders_visibility),
                )),
                Client::from_conf(get_test_config_with_interceptor(
                    MockResponseInterceptor::new()
                        .rule_mode(RuleMode::MatchAny)
                        .with_rule(&refunds_url)
                        .with_rule(&refunds_visibility),
                )),
            ])
            .build(CancellationToken::new())
            .await
            .unwrap();

        source
            .nack_offsets(vec![SqsNack {
                queue_name: "refunds-queue".to_string(),
                receipt_handle: Bytes::from("receipt-refunds-message"),
                visibility_timeout: 30,
            }])
            .await
            .unwrap();
        assert_eq!(orders_visibility.num_calls(), 0);
        assert_eq!(refunds_visibility.num_calls(), 1);
    }

    #[test(tokio::test)]
    async fn test_multi_queue_pending_count_is_summed() {
        let orders_url = get_queue_url_output_for("orders-queue", ORDERS_URL);
        let refunds_url = get_queue_url_output_for("refunds-queue", REFUNDS_URL);
        let orders_pending = get_queue_attributes_output_for(ORDERS_URL, "3");
        let refunds_pending = get_queue_attributes_output_for(REFUNDS_URL, "4");

        let source = SqsSourceBuilder::new(multi_queue_config())
            .clients(vec![
                Client::from_conf(get_test_config_with_interceptor(
                    MockResponseInterceptor::new()
                        .rule_mode(RuleMode::MatchAny)
                        .with_rule(&orders_url)
                        .with_rule(&orders_pending),
                )),
                Client::from_conf(get_test_config_with_interceptor(
                    MockResponseInterceptor::new()
                        .rule_mode(RuleMode::MatchAny)
                        .with_rule(&refunds_url)
                        .with_rule(&refunds_pending),
                )),
            ])
            .build(CancellationToken::new())
            .await
            .unwrap();

        assert_eq!(source.pending_count().await, Some(7));
    }

    #[test(tokio::test)]
    async fn test_multi_queue_startup_fails_if_any_queue_is_missing() {
        let orders_url = get_queue_url_output_for("orders-queue", ORDERS_URL);
        let refunds_error = mock!(aws_sdk_sqs::Client::get_queue_url)
            .match_requests(|input| input.queue_name() == Some("refunds-queue"))
            .then_error(|| {
                aws_sdk_sqs::operation::get_queue_url::GetQueueUrlError::generic(
                    ErrorMetadata::builder().code("QueueDoesNotExist").build(),
                )
            });
        let client = Client::from_conf(get_test_config_with_interceptor(
            MockResponseInterceptor::new()
                .rule_mode(RuleMode::MatchAny)
                .with_rule(&orders_url)
                .with_rule(&refunds_error),
        ));

        let source = SqsSourceBuilder::new(multi_queue_config())
            .client(client)
            .build(CancellationToken::new())
            .await;
        assert!(source.is_err());
    }

    #[test(tokio::test)]
    async fn test_empty_queue_names_are_rejected() {
        let mut config = multi_queue_config();
        config.queue_names.clear();

        let source = SqsSourceBuilder::new(config)
            .build(CancellationToken::new())
            .await;
        assert!(source.is_err());
    }

    #[tokio::test]
    async fn test_ack_rejects_unknown_queue_name() {
        let source = source_with_actors(
            vec![("orders-queue", mpsc::channel(1).0)],
            1,
            Duration::from_secs(0),
            0,
        );

        let err = source
            .ack_offsets(vec![("refunds-queue".to_string(), Bytes::from("receipt"))])
            .await
            .unwrap_err();
        assert!(err.to_string().contains("unknown SQS queue refunds-queue"));
    }

    #[tokio::test]
    async fn test_nack_rejects_unknown_queue_name() {
        let source = source_with_actors(
            vec![("orders-queue", mpsc::channel(1).0)],
            1,
            Duration::from_secs(0),
            0,
        );

        let err = source
            .nack_offsets(vec![SqsNack {
                queue_name: "refunds-queue".to_string(),
                receipt_handle: Bytes::from("receipt"),
                visibility_timeout: 30,
            }])
            .await
            .unwrap_err();
        assert!(err.to_string().contains("unknown SQS queue refunds-queue"));
    }

    #[tokio::test]
    async fn test_empty_ack_and_nack_batches_are_noops() {
        let source = source_with_actors(vec![], 1, Duration::from_secs(0), 0);

        source.ack_offsets(vec![]).await.unwrap();
        source.nack_offsets(vec![]).await.unwrap();
    }

    #[test(tokio::test)]
    async fn test_multi_queue_pending_returns_none_when_one_queue_errors() {
        let orders_url = get_queue_url_output_for("orders-queue", ORDERS_URL);
        let refunds_url = get_queue_url_output_for("refunds-queue", REFUNDS_URL);
        let orders_pending = get_queue_attributes_output_for(ORDERS_URL, "3");
        let refunds_pending_error = mock!(aws_sdk_sqs::Client::get_queue_attributes)
            .match_requests(|input| input.queue_url() == Some(REFUNDS_URL))
            .then_error(|| {
                aws_sdk_sqs::operation::get_queue_attributes::GetQueueAttributesError::generic(
                    ErrorMetadata::builder().code("QueueDoesNotExist").build(),
                )
            });

        let source = SqsSourceBuilder::new(multi_queue_config())
            .clients(vec![
                Client::from_conf(get_test_config_with_interceptor(
                    MockResponseInterceptor::new()
                        .rule_mode(RuleMode::MatchAny)
                        .with_rule(&orders_url)
                        .with_rule(&orders_pending),
                )),
                Client::from_conf(get_test_config_with_interceptor(
                    MockResponseInterceptor::new()
                        .rule_mode(RuleMode::MatchAny)
                        .with_rule(&refunds_url)
                        .with_rule(&refunds_pending_error),
                )),
            ])
            .build(CancellationToken::new())
            .await
            .unwrap();

        assert_eq!(source.pending_count().await, None);
    }

    #[test(tokio::test)]
    async fn test_multi_queue_startup_rejects_missing_injected_client() {
        let orders_url = get_queue_url_output_for("orders-queue", ORDERS_URL);
        let orders_client = Client::from_conf(get_test_config_with_interceptor(
            MockResponseInterceptor::new()
                .rule_mode(RuleMode::MatchAny)
                .with_rule(&orders_url),
        ));

        let result = SqsSourceBuilder::new(multi_queue_config())
            .clients(vec![orders_client])
            .build(CancellationToken::new())
            .await;

        let err = match result {
            Ok(_) => panic!("expected missing injected client to fail startup"),
            Err(err) => err,
        };
        assert!(
            err.to_string()
                .contains("missing SQS client for queue refunds-queue")
        );
    }

    #[test(tokio::test)]
    async fn test_build_rejects_duplicate_queue_names() {
        let mut config = multi_queue_config();
        config.queue_names = vec!["orders-queue", "orders-queue"];

        let err = match SqsSourceBuilder::new(config)
            .build(CancellationToken::new())
            .await
        {
            Ok(_) => panic!("expected duplicate queue names to fail startup"),
            Err(err) => err,
        };
        assert!(
            err.to_string()
                .contains("duplicate SQS queue name: orders-queue")
        );
    }

    /// One queue erroring must not discard what the other queues already returned.
    #[test(tokio::test)]
    async fn test_multi_queue_read_keeps_messages_when_one_queue_errors() {
        let orders_url = get_queue_url_output_for("orders-queue", ORDERS_URL);
        let refunds_url = get_queue_url_output_for("refunds-queue", REFUNDS_URL);
        let orders_receive = get_receive_message_output_for(ORDERS_URL, "orders-message");
        let refunds_receive_error = mock!(aws_sdk_sqs::Client::receive_message)
            .match_requests(|input| input.queue_url() == Some(REFUNDS_URL))
            .then_error(|| {
                aws_sdk_sqs::operation::receive_message::ReceiveMessageError::generic(
                    ErrorMetadata::builder().code("InvalidAddress").build(),
                )
            });

        let source = SqsSourceBuilder::new(multi_queue_config())
            .clients(vec![
                Client::from_conf(get_test_config_with_interceptor(
                    MockResponseInterceptor::new()
                        .rule_mode(RuleMode::MatchAny)
                        .with_rule(&orders_url)
                        .with_rule(&orders_receive),
                )),
                Client::from_conf(get_test_config_with_interceptor(
                    MockResponseInterceptor::new()
                        .rule_mode(RuleMode::MatchAny)
                        .with_rule(&refunds_url)
                        .with_rule(&refunds_receive_error),
                )),
            ])
            .batch_size(2)
            .timeout(Duration::from_secs(0))
            .build(CancellationToken::new())
            .await
            .unwrap();

        let messages = source.read_messages().await.unwrap().unwrap();
        let [message] = messages.as_slice() else {
            panic!("expected exactly one message, got {messages:?}");
        };
        assert_eq!(message.queue_name, "orders-queue");
        assert_eq!(message.payload, "orders-message");
    }

    /// Issues a `Receive` whose response nobody collects, mimicking a read that
    /// returned on another queue's data before this one answered.
    async fn abandoned_receive(actor_tx: &mpsc::Sender<SQSActorMessage>) {
        let (respond_to, abandoned) = mpsc::channel(1);
        drop(abandoned);
        actor_tx
            .send(SQSActorMessage::Receive {
                respond_to,
                count: 1,
                timeout_at: Instant::now() + Duration::from_secs(1),
            })
            .await
            .unwrap();
    }

    /// Issues a `Receive` and waits for its answer. Since the actor inbox is FIFO,
    /// this proves every earlier message has been fully handled.
    async fn collected_receive(actor_tx: &mpsc::Sender<SQSActorMessage>) {
        let (respond_to, mut responses) = mpsc::channel(1);
        actor_tx
            .send(SQSActorMessage::Receive {
                respond_to,
                count: 1,
                timeout_at: Instant::now() + Duration::from_secs(1),
            })
            .await
            .unwrap();
        responses.recv().await.expect("actor should answer");
    }

    /// The escalation path cannot ride the response channel, because the reader is
    /// allowed to walk away from a slow queue every single round.
    #[test(tokio::test)]
    async fn test_terminal_error_recorded_when_reader_gave_up() {
        let queue_url_output = get_queue_url_output();
        let receive_message_error = get_receive_message_error();

        let source = SqsSourceBuilder::new(SqsSourceConfig {
            region: SQS_DEFAULT_REGION,
            queue_names: vec!["test-q"],
            queue_owner_aws_account_id: "123456789012",
            visibility_timeout: None,
            max_number_of_messages: None,
            wait_time_seconds: None,
            endpoint_url: None,
            attribute_names: vec![],
            message_attribute_names: vec![],
            assume_role_config: None,
        })
        .batch_size(1)
        .timeout(Duration::from_secs(0))
        .client(Client::from_conf(get_test_config_with_interceptor(
            MockResponseInterceptor::new()
                .rule_mode(RuleMode::MatchAny)
                .with_rule(&queue_url_output)
                .with_rule(&receive_message_error),
        )))
        .build(CancellationToken::new())
        .await
        .unwrap();

        // Drive the queue past its retry budget with a reader that never collects,
        // so the threshold is crossed on a round whose response is thrown away.
        let actor_tx = source.actors.get("test-q").unwrap().clone();
        for _ in 0..=MAX_CONSECUTIVE_RECEIVE_FAILURES {
            abandoned_receive(&actor_tx).await;
        }
        collected_receive(&actor_tx).await;

        let result = source.read_messages().await;
        assert!(matches!(result, Some(Err(err)) if err.to_string().contains("InvalidAddress")));
    }

    #[test(tokio::test)]
    async fn test_terminal_error_surfaces_on_next_read() {
        let orders_url = get_queue_url_output_for("orders-queue", ORDERS_URL);
        let refunds_url = get_queue_url_output_for("refunds-queue", REFUNDS_URL);
        let orders_receive = get_receive_message_output_for(ORDERS_URL, "orders-message");
        let refunds_receive_error = mock!(aws_sdk_sqs::Client::receive_message)
            .match_requests(|input| input.queue_url() == Some(REFUNDS_URL))
            .then_error(|| {
                aws_sdk_sqs::operation::receive_message::ReceiveMessageError::generic(
                    ErrorMetadata::builder().code("InvalidAddress").build(),
                )
            });

        let source = SqsSourceBuilder::new(multi_queue_config())
            .clients(vec![
                Client::from_conf(get_test_config_with_interceptor(
                    MockResponseInterceptor::new()
                        .rule_mode(RuleMode::MatchAny)
                        .with_rule(&orders_url)
                        .with_rule(&orders_receive),
                )),
                Client::from_conf(get_test_config_with_interceptor(
                    MockResponseInterceptor::new()
                        .rule_mode(RuleMode::MatchAny)
                        .with_rule(&refunds_url)
                        .with_rule(&refunds_receive_error),
                )),
            ])
            .batch_size(2)
            .timeout(Duration::from_secs(0))
            .build(CancellationToken::new())
            .await
            .unwrap();

        // Bring refunds right up to, but not past, its retry budget.
        let refunds_tx = source.actors.get("refunds-queue").unwrap().clone();
        for _ in 1..MAX_CONSECUTIVE_RECEIVE_FAILURES {
            abandoned_receive(&refunds_tx).await;
        }
        collected_receive(&refunds_tx).await;

        // The failing queue does not cost the healthy queue its messages, even
        // though this read is the one that tips refunds over the threshold.
        let messages = source.read_messages().await.unwrap().unwrap();
        let [message] = messages.as_slice() else {
            panic!("expected exactly one message, got {messages:?}");
        };
        assert_eq!(message.queue_name, "orders-queue");

        // Refunds was still mid-receive when that read returned; wait for it to
        // finish so the assertion below is about behavior, not timing.
        collected_receive(&refunds_tx).await;

        let result = source.read_messages().await;
        assert!(matches!(result, Some(Err(err)) if err.to_string().contains("InvalidAddress")));
    }

    /// Messages fetched for a reader that gave up are held by the actor and served
    /// on the next read, rather than sitting invisible until their timeout expires.
    #[test(tokio::test)]
    async fn test_actor_buffers_messages_when_reader_gives_up() {
        let orders_receive = get_receive_message_output_for(ORDERS_URL, "orders-message");
        let (actor_tx, handler_rx) = mpsc::channel(10);
        let mut actor = SqsActor::new(
            handler_rx,
            Client::from_conf(get_test_config_with_interceptor(
                MockResponseInterceptor::new()
                    .rule_mode(RuleMode::MatchAny)
                    .with_rule(&orders_receive),
            )),
            ORDERS_URL.to_string(),
            "orders-queue",
            multi_queue_config(),
            CancellationToken::new(),
            Arc::new(OnceLock::new()),
        );
        tokio::spawn(async move { actor.run().await });

        let (respond_to, abandoned) = mpsc::channel(1);
        drop(abandoned);
        actor_tx
            .send(SQSActorMessage::Receive {
                respond_to,
                count: 1,
                timeout_at: Instant::now() + Duration::from_secs(1),
            })
            .await
            .unwrap();

        let (respond_to, mut responses) = mpsc::channel(1);
        actor_tx
            .send(SQSActorMessage::Receive {
                respond_to,
                count: 1,
                timeout_at: Instant::now() + Duration::from_secs(1),
            })
            .await
            .unwrap();

        let (queue_name, response) = responses.recv().await.unwrap();
        assert_eq!(queue_name, "orders-queue");
        let messages = response.unwrap().unwrap();
        let [message] = messages.as_slice() else {
            panic!("expected exactly one message, got {messages:?}");
        };
        assert_eq!(message.payload, "orders-message");
        assert_eq!(
            orders_receive.num_calls(),
            1,
            "the buffered batch should be served without a second ReceiveMessage"
        );
    }

    /// A partial drain that is never collected must not rotate the queue's order.
    #[test(tokio::test)]
    async fn test_buffer_restore_preserves_receive_order() {
        let orders_receive = get_receive_batch_output_for(ORDERS_URL, ["a", "b", "c", "d"]);
        let (actor_tx, handler_rx) = mpsc::channel(10);
        let mut actor = SqsActor::new(
            handler_rx,
            Client::from_conf(get_test_config_with_interceptor(
                MockResponseInterceptor::new()
                    .rule_mode(RuleMode::MatchAny)
                    .with_rule(&orders_receive),
            )),
            ORDERS_URL.to_string(),
            "orders-queue",
            multi_queue_config(),
            CancellationToken::new(),
            Arc::new(OnceLock::new()),
        );
        tokio::spawn(async move { actor.run().await });

        // Fetch all four, then take two, abandoning the reader both times.
        for count in [4, 2] {
            let (respond_to, abandoned) = mpsc::channel(1);
            drop(abandoned);
            actor_tx
                .send(SQSActorMessage::Receive {
                    respond_to,
                    count,
                    timeout_at: Instant::now() + Duration::from_secs(1),
                })
                .await
                .unwrap();
        }

        let (respond_to, mut responses) = mpsc::channel(1);
        actor_tx
            .send(SQSActorMessage::Receive {
                respond_to,
                count: 4,
                timeout_at: Instant::now() + Duration::from_secs(1),
            })
            .await
            .unwrap();

        let (_, response) = responses.recv().await.unwrap();
        let bodies: Vec<String> = response
            .unwrap()
            .unwrap()
            .iter()
            .map(|message| String::from_utf8(message.payload.to_vec()).unwrap())
            .collect();
        assert_eq!(bodies, vec!["a", "b", "c", "d"]);
        assert_eq!(orders_receive.num_calls(), 1);
    }

    fn get_receive_batch_output_for(queue_url: &'static str, bodies: [&'static str; 4]) -> Rule {
        mock!(aws_sdk_sqs::Client::receive_message)
            .match_requests(move |input| input.queue_url() == Some(queue_url))
            .then_output(move || {
                let mut output =
                    aws_sdk_sqs::operation::receive_message::ReceiveMessageOutput::builder();
                for body in bodies {
                    output = output.messages(
                        aws_sdk_sqs::types::Message::builder()
                            .message_id(body)
                            .body(body)
                            .receipt_handle(format!("receipt-{body}"))
                            .build(),
                    );
                }
                output.build()
            })
    }

    fn get_queue_attributes_output() -> Rule {
        mock!(aws_sdk_sqs::Client::get_queue_attributes)
            .match_requests(|inp| {
                inp.queue_url().unwrap()
                    == "https://sqs.us-west-2.amazonaws.com/926113353675/test-q/"
            })
            .then_output(|| {
                aws_sdk_sqs::operation::get_queue_attributes::GetQueueAttributesOutput::builder()
                    .attributes(
                        aws_sdk_sqs::types::QueueAttributeName::ApproximateNumberOfMessages,
                        "0",
                    )
                    .build()
            })
    }

    fn get_queue_attributes_error() -> Rule {
        mock!(aws_sdk_sqs::Client::get_queue_attributes)
            .match_requests(|inp| {
                inp.queue_url().unwrap()
                    == "https://sqs.us-west-2.amazonaws.com/926113353675/test-q/"
            })
            .then_error(|| {
                aws_sdk_sqs::operation::get_queue_attributes::GetQueueAttributesError::generic(
                    ErrorMetadata::builder()
                        .code("QueueDoesNotExist")
                        .message("The specified queue does not exist for this wsdl version.")
                        .build(),
                )
            })
    }

    fn get_delete_message_output() -> Rule {
        mock!(aws_sdk_sqs::Client::delete_message_batch)
            .match_requests(|inp| {
                inp.queue_url().unwrap()
                    == "https://sqs.us-west-2.amazonaws.com/926113353675/test-q/"
                    && inp.entries.clone().unwrap().len() == 1
            })
            .then_output(|| {
                aws_sdk_sqs::operation::delete_message_batch::DeleteMessageBatchOutput::builder()
                    .successful(
                        aws_sdk_sqs::types::DeleteMessageBatchResultEntry::builder()
                            .id("0")
                            .build()
                            .unwrap(),
                    )
                    .set_failed(Some(Vec::new()))
                    .build()
                    .unwrap()
            })
    }

    fn get_delete_message_error() -> Rule {
        mock!(aws_sdk_sqs::Client::delete_message_batch)
            .match_requests(|inp| {
                inp.queue_url().unwrap()
                    == "https://sqs.us-west-2.amazonaws.com/926113353675/test-q/"
                    && inp.entries().len() == 1
            })
            .then_error(|| {
                aws_sdk_sqs::operation::delete_message_batch::DeleteMessageBatchError::generic(
                    ErrorMetadata::builder()
                        .code("ReceiptHandleIsInvalid")
                        .build(),
                )
            })
    }

    fn get_receive_message_output() -> Rule {
        mock!(aws_sdk_sqs::Client::receive_message)
            .match_requests(|inp| {
                inp.queue_url().unwrap() == "https://sqs.us-west-2.amazonaws.com/926113353675/test-q/"
            })
            .then_output(|| {
                aws_sdk_sqs::operation::receive_message::ReceiveMessageOutput::builder()
                    .messages(
                        aws_sdk_sqs::types::Message::builder()
                            .message_id("219f8380-5770-4cc2-8c3e-5c715e145f5e")
                            .body("This is a test message")
                            .receipt_handle("AQEBaZ+j5qUoOAoxlmrCQPkBm9njMWXqemmIG6shMHCO6fV20JrQYg/AiZ8JELwLwOu5U61W+aIX5Qzu7GGofxJuvzymr4Ph53RiR0mudj4InLSgpSspYeTRDteBye5tV/txbZDdNZxsi+qqZA9xPnmMscKQqF6pGhnGIKrnkYGl45Nl6GPIZv62LrIRb6mSqOn1fn0yqrvmWuuY3w2UzQbaYunJWGxpzZze21EOBtywknU3Je/g7G9is+c6K9hGniddzhLkK1tHzZKjejOU4jokaiB4nmi0dF3JqLzDsQuPF0Gi8qffhEvw56nl8QCbluSJScFhJYvoagGnDbwOnd9z50L239qtFIgETdpKyirlWwl/NGjWJ45dqWpiW3d2Ws7q")
                            .attributes(MessageSystemAttributeName::SentTimestamp, "1677112427387")
                            .message_attributes(
                                "AwsTraceHeader",
                                MessageAttributeValue::builder()
                                    .set_data_type(Some("String".to_string()))
                                    .set_string_value(Some("Root=1-5e4f8a2c-0b2d3e4f8a2c0b2d3e4f8a2c".to_string()))
                                    .build().unwrap()
                            )
                            .build()
                    )
                    .build()
            })
    }

    fn get_receive_message_error() -> Rule {
        mock!(aws_sdk_sqs::Client::receive_message)
            .match_requests(|inp| {
                inp.queue_url().unwrap()
                    == "https://sqs.us-west-2.amazonaws.com/926113353675/test-q/"
            })
            .then_error(|| {
                aws_sdk_sqs::operation::receive_message::ReceiveMessageError::generic(
                    ErrorMetadata::builder().code("InvalidAddress").build(),
                )
            })
    }

    fn get_queue_url_output() -> Rule {
        mock!(aws_sdk_sqs::Client::get_queue_url)
            .match_requests(|inp| inp.queue_name().unwrap() == "test-q")
            .then_output(|| {
                aws_sdk_sqs::operation::get_queue_url::GetQueueUrlOutput::builder()
                    .queue_url("https://sqs.us-west-2.amazonaws.com/926113353675/test-q/")
                    .build()
            })
    }

    fn get_queue_url_output_err() -> Rule {
        mock!(aws_sdk_sqs::Client::get_queue_url).then_error(|| {
            aws_sdk_sqs::operation::get_queue_url::GetQueueUrlError::generic(
                ErrorMetadata::builder().code("InvalidAddress").build(),
            )
        })
    }

    fn get_test_config_with_interceptor(interceptor: MockResponseInterceptor) -> Config {
        aws_sdk_sqs::Config::builder()
            .behavior_version(crate::aws_behavior_version())
            .credentials_provider(make_sqs_test_credentials())
            .region(aws_sdk_sqs::config::Region::new(SQS_DEFAULT_REGION))
            .interceptor(interceptor)
            .build()
    }

    fn make_sqs_test_credentials() -> aws_sdk_sqs::config::Credentials {
        aws_sdk_sqs::config::Credentials::new(
            "ATESTCLIENT",
            "astestsecretkey",
            Some("atestsessiontoken".to_string()),
            None,
            "",
        )
    }
}
