use std::collections::HashMap;

use chrono::Utc;
use log::warn;
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info};

use crate::config::components::sink::{OnFailureStrategy, RetryConfig};
use crate::config::monovertex::MonovertexConfig;
use crate::error;
use crate::message::{Message, Offset, ResponseStatusFromSink};
use crate::monovertex::metrics;
use crate::monovertex::metrics::forward_metrics;
use crate::sink::SinkHandle;
use crate::Error;
use crate::{source::SourceHandle, transformer::user_defined::SourceTransformHandle};

/// Forwarder is responsible for reading messages from the source, applying transformation if
/// transformer is present, writing the messages to the sink, and then acknowledging the messages
/// back to the source.
pub(crate) struct Forwarder {
    source_reader: SourceHandle,
    sink_writer: SinkHandle,
    source_transformer: Option<SourceTransformHandle>,
    fb_sink_writer: Option<SinkHandle>,
    cln_token: CancellationToken,
    common_labels: Vec<(String, String)>,
    mvtx_config: MonovertexConfig,
}

/// ForwarderBuilder is used to build a Forwarder instance with optional fields.
pub(crate) struct ForwarderBuilder {
    source_reader: SourceHandle,
    sink_writer: SinkHandle,
    cln_token: CancellationToken,
    source_transformer: Option<SourceTransformHandle>,
    fb_sink_writer: Option<SinkHandle>,
    mvtx_config: MonovertexConfig,
}

impl ForwarderBuilder {
    /// Create a new builder with mandatory fields
    pub(crate) fn new(
        source_reader: SourceHandle,
        sink_writer: SinkHandle,
        mvtx_config: MonovertexConfig,
        cln_token: CancellationToken,
    ) -> Self {
        Self {
            source_reader,
            sink_writer,
            cln_token,
            source_transformer: None,
            fb_sink_writer: None,
            mvtx_config,
        }
    }

    /// Set the optional transformer client
    pub(crate) fn source_transformer(mut self, transformer_client: SourceTransformHandle) -> Self {
        self.source_transformer = Some(transformer_client);
        self
    }

    /// Set the optional fallback client
    pub(crate) fn fallback_sink_writer(mut self, fallback_client: SinkHandle) -> Self {
        self.fb_sink_writer = Some(fallback_client);
        self
    }

    /// Build the Forwarder instance
    #[must_use]
    pub(crate) fn build(self) -> Forwarder {
        let common_labels = metrics::mvtx_forward_metric_labels(
            self.mvtx_config.name.clone(),
            self.mvtx_config.replica,
        )
        .clone();
        Forwarder {
            source_reader: self.source_reader,
            sink_writer: self.sink_writer,
            source_transformer: self.source_transformer,
            fb_sink_writer: self.fb_sink_writer,
            cln_token: self.cln_token,
            mvtx_config: self.mvtx_config,
            common_labels,
        }
    }
}

impl Forwarder {
    /// start starts the forward-a-chunk loop and exits only after a chunk has been forwarded and ack'ed.
    /// this means that, in the happy path scenario a block is always completely processed.
    /// this function will return on any error and will cause end up in a non-0 exit code.
    pub(crate) async fn start(&mut self) -> error::Result<()> {
        let mut processed_msgs_count: usize = 0;
        let mut last_forwarded_at = std::time::Instant::now();
        info!("Forwarder has started");
        loop {
            let start_time = tokio::time::Instant::now();
            if self.cln_token.is_cancelled() {
                break;
            }

            processed_msgs_count += self.read_and_process_messages().await?;

            // if the last forward was more than 1 second ago, forward a chunk print the number of messages forwarded
            // TODO: add histogram details (p99, etc.)
            if last_forwarded_at.elapsed().as_millis() >= 1000 {
                info!(
                    "Forwarded {} messages at time {}",
                    processed_msgs_count,
                    Utc::now()
                );
                processed_msgs_count = 0;
                last_forwarded_at = std::time::Instant::now();
            }

            forward_metrics()
                .e2e_time
                .get_or_create(&self.common_labels)
                .observe(start_time.elapsed().as_micros() as f64);
        }
        Ok(())
    }

    /// Read messages from the source, apply transformation if transformer is present,
    /// write the messages to the sink, if fallback messages are present write them to the fallback sink,
    /// and then acknowledge the messages back to the source.
    async fn read_and_process_messages(&mut self) -> error::Result<usize> {
        let start_time = tokio::time::Instant::now();
        let messages = self.source_reader.read().await.map_err(|e| {
            Error::Forwarder(format!("Failed to read messages from source {:?}", e))
        })?;

        debug!(
            "Read batch size: {} and latency - {}ms",
            messages.len(),
            start_time.elapsed().as_millis()
        );

        forward_metrics()
            .read_time
            .get_or_create(&self.common_labels)
            .observe(start_time.elapsed().as_micros() as f64);

        // read returned 0 messages, nothing more to be done.
        if messages.is_empty() {
            return Ok(0);
        }

        let msg_count = messages.len() as u64;
        forward_metrics()
            .read_total
            .get_or_create(&self.common_labels)
            .inc_by(msg_count);

        let (offsets, bytes_count): (Vec<Offset>, u64) = messages.iter().try_fold(
            (Vec::with_capacity(messages.len()), 0),
            |(mut offsets, mut bytes_count), msg| {
                if let Some(offset) = &msg.offset {
                    offsets.push(offset.clone());
                    bytes_count += msg.value.len() as u64;
                    Ok((offsets, bytes_count))
                } else {
                    Err(Error::Forwarder("Message offset is missing".to_string()))
                }
            },
        )?;

        forward_metrics()
            .read_bytes_total
            .get_or_create(&self.common_labels)
            .inc_by(bytes_count);

        // Apply transformation if transformer is present
        let transformed_messages = self.apply_transformer(messages).await.map_err(|e| {
            Error::Forwarder(format!(
                "Failed to apply transformation to messages {:?}",
                e
            ))
        })?;

        // Write the messages to the sink
        self.write_to_sink(transformed_messages)
            .await
            .map_err(|e| Error::Forwarder(format!("Failed to write messages to sink {:?}", e)))?;

        // Acknowledge the messages back to the source
        self.acknowledge_messages(offsets).await.map_err(|e| {
            Error::Forwarder(format!(
                "Failed to acknowledge messages back to source {:?}",
                e
            ))
        })?;

        Ok(msg_count as usize)
    }

    // Applies transformation to the messages if transformer is present
    // we concurrently apply transformation to all the messages.
    async fn apply_transformer(&mut self, messages: Vec<Message>) -> error::Result<Vec<Message>> {
        let Some(client) = &mut self.source_transformer else {
            // return early if there is no transformer
            return Ok(messages);
        };

        let start_time = tokio::time::Instant::now();
        let results = client.transform(messages).await?;

        debug!(
            "Transformer latency - {}ms",
            start_time.elapsed().as_millis()
        );
        forward_metrics()
            .transform_time
            .get_or_create(&self.common_labels)
            .observe(start_time.elapsed().as_micros() as f64);

        Ok(results)
    }

    // Writes the messages to the sink and handles fallback messages if present
    async fn write_to_sink(&mut self, messages: Vec<Message>) -> error::Result<()> {
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
            .mvtx_config
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
                        &retry_config,
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
                if self.cln_token.is_cancelled() {
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
                &retry_config,
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
            self.handle_fallback_messages(fallback_msgs, &retry_config)
                .await?;
        }

        forward_metrics()
            .sink_time
            .get_or_create(&self.common_labels)
            .observe(start_time_e2e.elapsed().as_micros() as f64);

        // update the metric for number of messages written to the sink
        // this included primary and fallback sink
        forward_metrics()
            .sink_write_total
            .get_or_create(&self.common_labels)
            .inc_by(msg_count);
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
    ) -> error::Result<bool> {
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
                // update the metrics
                forward_metrics()
                    .dropped_total
                    .get_or_create(&self.common_labels)
                    .inc_by(messages_to_send.len() as u64);
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
    ) -> error::Result<bool> {
        let start_time = tokio::time::Instant::now();
        match self.sink_writer.sink(messages_to_send.clone()).await {
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
    ) -> error::Result<()> {
        if self.fb_sink_writer.is_none() {
            return Err(Error::Sink(
                "Response contains fallback messages but no fallback sink is configured"
                    .to_string(),
            ));
        }

        let fallback_client = self.fb_sink_writer.as_mut().unwrap();
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
        // increment the metric for the fallback sink write
        forward_metrics()
            .fbsink_write_total
            .get_or_create(&self.common_labels)
            .inc_by(fb_msg_count);
        Ok(())
    }

    // Acknowledge the messages back to the source
    async fn acknowledge_messages(&mut self, offsets: Vec<Offset>) -> error::Result<()> {
        let n = offsets.len();
        let start_time = tokio::time::Instant::now();

        self.source_reader.ack(offsets).await?;

        debug!("Ack latency - {}ms", start_time.elapsed().as_millis());

        forward_metrics()
            .ack_time
            .get_or_create(&self.common_labels)
            .observe(start_time.elapsed().as_micros() as f64);

        forward_metrics()
            .ack_total
            .get_or_create(&self.common_labels)
            .inc_by(n as u64);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use chrono::Utc;
    use numaflow::source::{Message, Offset, SourceReadRequest};
    use numaflow::{sink, source, sourcetransform};
    use numaflow_pb::clients::sink::sink_client::SinkClient;
    use numaflow_pb::clients::source::source_client::SourceClient;
    use numaflow_pb::clients::sourcetransformer::source_transform_client::SourceTransformClient;
    use tokio::sync::mpsc;
    use tokio::sync::mpsc::Sender;
    use tokio_util::sync::CancellationToken;

    use crate::config::config;
    use crate::monovertex::forwarder::ForwarderBuilder;
    use crate::monovertex::SourceType;
    use crate::shared::utils::create_rpc_channel;
    use crate::sink::{SinkClientType, SinkHandle};
    use crate::source::user_defined::new_source;
    use crate::source::SourceHandle;
    use crate::transformer::user_defined::SourceTransformHandle;

    struct SimpleSource {
        yet_to_be_acked: std::sync::RwLock<HashSet<String>>,
    }

    impl SimpleSource {
        fn new() -> Self {
            Self {
                yet_to_be_acked: std::sync::RwLock::new(HashSet::new()),
            }
        }
    }

    #[tonic::async_trait]
    impl source::Sourcer for SimpleSource {
        async fn read(&self, request: SourceReadRequest, transmitter: Sender<Message>) {
            let event_time = Utc::now();
            let mut message_offsets = Vec::with_capacity(request.count);
            for i in 0..2 {
                let offset = format!("{}-{}", event_time.timestamp_nanos_opt().unwrap(), i);
                transmitter
                    .send(Message {
                        value: "test-message".as_bytes().to_vec(),
                        event_time,
                        offset: Offset {
                            offset: offset.clone().into_bytes(),
                            partition_id: 0,
                        },
                        keys: vec!["test-key".to_string()],
                        headers: Default::default(),
                    })
                    .await
                    .unwrap();
                message_offsets.push(offset)
            }
            self.yet_to_be_acked
                .write()
                .unwrap()
                .extend(message_offsets)
        }

        async fn ack(&self, offset: Offset) {
            self.yet_to_be_acked
                .write()
                .unwrap()
                .remove(&String::from_utf8(offset.offset).unwrap());
        }

        async fn pending(&self) -> usize {
            self.yet_to_be_acked.read().unwrap().len()
        }

        async fn partitions(&self) -> Option<Vec<i32>> {
            Some(vec![0])
        }
    }

    struct SimpleTransformer;
    #[tonic::async_trait]
    impl sourcetransform::SourceTransformer for SimpleTransformer {
        async fn transform(
            &self,
            input: sourcetransform::SourceTransformRequest,
        ) -> Vec<sourcetransform::Message> {
            let keys = input
                .keys
                .iter()
                .map(|k| k.clone() + "-transformed")
                .collect();
            let message = sourcetransform::Message::new(input.value, Utc::now())
                .keys(keys)
                .tags(vec![]);
            vec![message]
        }
    }

    struct InMemorySink {
        sender: Sender<Message>,
    }

    impl InMemorySink {
        fn new(sender: Sender<Message>) -> Self {
            Self { sender }
        }
    }

    #[tonic::async_trait]
    impl sink::Sinker for InMemorySink {
        async fn sink(&self, mut input: mpsc::Receiver<sink::SinkRequest>) -> Vec<sink::Response> {
            let mut responses: Vec<sink::Response> = Vec::new();
            while let Some(datum) = input.recv().await {
                let response = match std::str::from_utf8(&datum.value) {
                    Ok(_) => {
                        self.sender
                            .send(Message {
                                value: datum.value.clone(),
                                event_time: datum.event_time,
                                offset: Offset {
                                    offset: "test-offset".to_string().into_bytes(),
                                    partition_id: 0,
                                },
                                keys: datum.keys.clone(),
                                headers: Default::default(),
                            })
                            .await
                            .unwrap();
                        sink::Response::ok(datum.id)
                    }
                    Err(e) => {
                        sink::Response::failure(datum.id, format!("Invalid UTF-8 sequence: {}", e))
                    }
                };
                responses.push(response);
            }
            responses
        }
    }

    #[tokio::test]
    async fn test_forwarder_source_sink() {
        let batch_size = 100;
        let timeout_in_ms = 1000;

        let (sink_tx, mut sink_rx) = mpsc::channel(10);

        // Start the source server
        let (source_shutdown_tx, source_shutdown_rx) = tokio::sync::oneshot::channel();
        let tmp_dir = tempfile::TempDir::new().unwrap();
        let source_sock_file = tmp_dir.path().join("source.sock");
        let server_info_file = tmp_dir.path().join("source-server-info");

        let server_info = server_info_file.clone();
        let source_socket = source_sock_file.clone();
        let source_server_handle = tokio::spawn(async move {
            source::Server::new(SimpleSource::new())
                .with_socket_file(source_socket)
                .with_server_info_file(server_info)
                .start_with_shutdown(source_shutdown_rx)
                .await
                .unwrap();
        });

        // Start the sink server
        let (sink_shutdown_tx, sink_shutdown_rx) = tokio::sync::oneshot::channel();
        let sink_tmp_dir = tempfile::TempDir::new().unwrap();
        let sink_sock_file = sink_tmp_dir.path().join("sink.sock");
        let server_info_file = sink_tmp_dir.path().join("sink-server-info");

        let server_info = server_info_file.clone();
        let sink_socket = sink_sock_file.clone();
        let sink_server_handle = tokio::spawn(async move {
            sink::Server::new(InMemorySink::new(sink_tx))
                .with_socket_file(sink_socket)
                .with_server_info_file(server_info)
                .start_with_shutdown(sink_shutdown_rx)
                .await
                .unwrap();
        });

        // Start the transformer server
        let (transformer_shutdown_tx, transformer_shutdown_rx) = tokio::sync::oneshot::channel();
        let tmp_dir = tempfile::TempDir::new().unwrap();
        let transformer_sock_file = tmp_dir.path().join("transformer.sock");
        let server_info_file = tmp_dir.path().join("transformer-server-info");

        let server_info = server_info_file.clone();
        let transformer_socket = transformer_sock_file.clone();
        let transformer_server_handle = tokio::spawn(async move {
            sourcetransform::Server::new(SimpleTransformer)
                .with_socket_file(transformer_socket)
                .with_server_info_file(server_info)
                .start_with_shutdown(transformer_shutdown_rx)
                .await
                .unwrap();
        });

        // Wait for the servers to start
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        let cln_token = CancellationToken::new();

        let (source_read, source_ack, source_lag_reader) = new_source(
            SourceClient::new(create_rpc_channel(source_sock_file.clone()).await.unwrap()),
            batch_size,
            timeout_in_ms,
        )
        .await
        .expect("failed to connect to source server");

        let src_reader = SourceHandle::new(
            SourceType::UserDefinedSource(source_read, source_ack, source_lag_reader),
            batch_size,
        );

        let sink_grpc_client = SinkClient::new(create_rpc_channel(sink_sock_file).await.unwrap());
        let sink_writer =
            SinkHandle::new(SinkClientType::UserDefined(sink_grpc_client), batch_size)
                .await
                .expect("failed to connect to sink server");

        let transformer_client = SourceTransformHandle::new(SourceTransformClient::new(
            create_rpc_channel(transformer_sock_file).await.unwrap(),
        ))
        .await
        .expect("failed to connect to transformer server");

        let mut forwarder = ForwarderBuilder::new(
            src_reader,
            sink_writer,
            Default::default(),
            cln_token.clone(),
        )
        .source_transformer(transformer_client)
        .build();

        // Assert the received message in a different task
        let assert_handle = tokio::spawn(async move {
            let received_message = sink_rx.recv().await.unwrap();
            assert_eq!(received_message.value, "test-message".as_bytes());
            assert_eq!(
                received_message.keys,
                vec!["test-key-transformed".to_string()]
            );
            cln_token.cancel();
        });

        forwarder.start().await.unwrap();

        // Wait for the assertion task to complete
        assert_handle.await.unwrap();

        drop(forwarder);
        // stop the servers
        source_shutdown_tx
            .send(())
            .expect("failed to send shutdown signal");
        source_server_handle
            .await
            .expect("failed to join source server task");

        transformer_shutdown_tx
            .send(())
            .expect("failed to send shutdown signal");
        transformer_server_handle
            .await
            .expect("failed to join transformer server task");

        sink_shutdown_tx
            .send(())
            .expect("failed to send shutdown signal");
        sink_server_handle
            .await
            .expect("failed to join sink server task");
    }

    struct ErrorSink {}

    #[tonic::async_trait]
    impl sink::Sinker for ErrorSink {
        async fn sink(
            &self,
            mut input: tokio::sync::mpsc::Receiver<sink::SinkRequest>,
        ) -> Vec<sink::Response> {
            let mut responses = vec![];
            while let Some(datum) = input.recv().await {
                responses.append(&mut vec![sink::Response::failure(
                    datum.id,
                    "error".to_string(),
                )]);
            }
            responses
        }
    }

    #[tokio::test]
    async fn test_forwarder_sink_error() {
        let batch_size = 100;
        let timeout_in_ms = 1000;

        // Start the source server
        let (source_shutdown_tx, source_shutdown_rx) = tokio::sync::oneshot::channel();
        let tmp_dir = tempfile::TempDir::new().unwrap();
        let source_sock_file = tmp_dir.path().join("source.sock");
        let server_info_file = tmp_dir.path().join("source-server-info");

        let server_info = server_info_file.clone();
        let source_socket = source_sock_file.clone();
        let source_server_handle = tokio::spawn(async move {
            source::Server::new(SimpleSource::new())
                .with_socket_file(source_socket)
                .with_server_info_file(server_info)
                .start_with_shutdown(source_shutdown_rx)
                .await
                .unwrap();
        });

        // Start the sink server
        let (sink_shutdown_tx, sink_shutdown_rx) = tokio::sync::oneshot::channel();
        let sink_tmp_dir = tempfile::TempDir::new().unwrap();
        let sink_sock_file = sink_tmp_dir.path().join("sink.sock");
        let server_info_file = sink_tmp_dir.path().join("sink-server-info");

        let server_info = server_info_file.clone();
        let sink_socket = sink_sock_file.clone();
        let sink_server_handle = tokio::spawn(async move {
            sink::Server::new(ErrorSink {})
                .with_socket_file(sink_socket)
                .with_server_info_file(server_info)
                .start_with_shutdown(sink_shutdown_rx)
                .await
                .unwrap();
        });

        // Wait for the servers to start
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        let cln_token = CancellationToken::new();

        let (source_read, source_ack, lag_reader) = new_source(
            SourceClient::new(create_rpc_channel(source_sock_file.clone()).await.unwrap()),
            batch_size,
            timeout_in_ms,
        )
        .await
        .expect("failed to connect to source server");

        let source_reader = SourceHandle::new(
            SourceType::UserDefinedSource(source_read, source_ack, lag_reader),
            batch_size,
        );

        let sink_client = SinkClient::new(create_rpc_channel(sink_sock_file).await.unwrap());
        let sink_writer = SinkHandle::new(SinkClientType::UserDefined(sink_client), batch_size)
            .await
            .expect("failed to connect to sink server");

        let mut forwarder = ForwarderBuilder::new(
            source_reader,
            sink_writer,
            Default::default(),
            cln_token.clone(),
        )
        .build();

        let cancel_handle = tokio::spawn(async move {
            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
            cln_token.cancel();
        });

        let forwarder_result = forwarder.start().await;
        assert!(forwarder_result.is_err());
        cancel_handle.await.unwrap();

        // stop the servers
        drop(forwarder);
        source_shutdown_tx
            .send(())
            .expect("failed to send shutdown signal");
        source_server_handle
            .await
            .expect("failed to join source server task");

        sink_shutdown_tx
            .send(())
            .expect("failed to send sink shutdown signal");
        sink_server_handle
            .await
            .expect("failed to join sink server task");
    }

    // Sink that returns status fallback
    struct FallbackSender {}

    #[tonic::async_trait]
    impl sink::Sinker for FallbackSender {
        async fn sink(&self, mut input: mpsc::Receiver<sink::SinkRequest>) -> Vec<sink::Response> {
            let mut responses = vec![];
            while let Some(datum) = input.recv().await {
                responses.append(&mut vec![sink::Response::fallback(datum.id)]);
            }
            responses
        }
    }

    #[tokio::test]
    async fn test_fb_sink() {
        let batch_size = 100;
        let timeout_in_ms = 1000;

        let (sink_tx, mut sink_rx) = mpsc::channel(10);

        // Start the source server
        let (source_shutdown_tx, source_shutdown_rx) = tokio::sync::oneshot::channel();
        let tmp_dir = tempfile::TempDir::new().unwrap();
        let source_sock_file = tmp_dir.path().join("source.sock");
        let server_info_file = tmp_dir.path().join("source-server-info");

        let server_info = server_info_file.clone();
        let source_socket = source_sock_file.clone();
        let source_server_handle = tokio::spawn(async move {
            source::Server::new(SimpleSource::new())
                .with_socket_file(source_socket)
                .with_server_info_file(server_info)
                .start_with_shutdown(source_shutdown_rx)
                .await
                .unwrap();
        });

        // Start the primary sink server (which returns status fallback)
        let (sink_shutdown_tx, sink_shutdown_rx) = tokio::sync::oneshot::channel();
        let sink_tmp_dir = tempfile::TempDir::new().unwrap();
        let sink_sock_file = sink_tmp_dir.path().join("sink.sock");
        let server_info_file = sink_tmp_dir.path().join("sink-server-info");

        let server_info = server_info_file.clone();
        let sink_socket = sink_sock_file.clone();
        let sink_server_handle = tokio::spawn(async move {
            sink::Server::new(FallbackSender {})
                .with_socket_file(sink_socket)
                .with_server_info_file(server_info)
                .start_with_shutdown(sink_shutdown_rx)
                .await
                .unwrap();
        });

        // Start the fb sink server
        let (fb_sink_shutdown_tx, fb_sink_shutdown_rx) = tokio::sync::oneshot::channel();
        let fb_sink_tmp_dir = tempfile::TempDir::new().unwrap();
        let fb_sink_sock_file = fb_sink_tmp_dir.path().join("fb-sink.sock");
        let server_info_file = fb_sink_tmp_dir.path().join("fb-sinker-server-info");

        let server_info = server_info_file.clone();
        let fb_sink_socket = fb_sink_sock_file.clone();
        let fb_sink_server_handle = tokio::spawn(async move {
            sink::Server::new(InMemorySink::new(sink_tx))
                .with_socket_file(fb_sink_socket)
                .with_server_info_file(server_info)
                .start_with_shutdown(fb_sink_shutdown_rx)
                .await
                .unwrap();
        });

        // Wait for the servers to start
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        let cln_token = CancellationToken::new();

        let (source_read, source_ack, source_lag_reader) = new_source(
            SourceClient::new(create_rpc_channel(source_sock_file.clone()).await.unwrap()),
            500,
            100,
        )
        .await
        .expect("failed to connect to source server");

        let source = SourceHandle::new(
            SourceType::UserDefinedSource(source_read, source_ack, source_lag_reader),
            batch_size,
        );

        let sink_client = SinkClient::new(create_rpc_channel(sink_sock_file).await.unwrap());
        let sink_writer = SinkHandle::new(SinkClientType::UserDefined(sink_client), batch_size)
            .await
            .expect("failed to connect to sink server");

        let fb_sink_writer = SinkClient::new(create_rpc_channel(fb_sink_sock_file).await.unwrap());
        let fb_sink_writer =
            SinkHandle::new(SinkClientType::UserDefined(fb_sink_writer), batch_size)
                .await
                .expect("failed to connect to fb sink server");

        let mut forwarder =
            ForwarderBuilder::new(source, sink_writer, Default::default(), cln_token.clone())
                .fallback_sink_writer(fb_sink_writer)
                .build();

        let assert_handle = tokio::spawn(async move {
            let received_message = sink_rx.recv().await.unwrap();
            assert_eq!(received_message.value, "test-message".as_bytes());
            assert_eq!(received_message.keys, vec!["test-key".to_string()]);
            cln_token.cancel();
        });

        forwarder.start().await.unwrap();

        assert_handle.await.unwrap();

        drop(forwarder);
        // stop the servers
        source_shutdown_tx
            .send(())
            .expect("failed to send shutdown signal");
        source_server_handle
            .await
            .expect("failed to join source server task");

        sink_shutdown_tx
            .send(())
            .expect("failed to send shutdown signal");
        sink_server_handle
            .await
            .expect("failed to join sink server task");

        fb_sink_shutdown_tx
            .send(())
            .expect("failed to send shutdown signal");
        fb_sink_server_handle
            .await
            .expect("failed to join fb sink server task");
    }
}
