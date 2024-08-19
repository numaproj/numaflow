use crate::config::config;
use crate::error::{Error, Result};
use crate::message::Offset;
use crate::metrics;
use crate::metrics::forward_metrics;
use crate::sink::{proto, SinkClient};
use crate::source::SourceClient;
use crate::transformer::TransformerClient;
use chrono::Utc;
use std::collections::HashMap;
use tokio::task::JoinSet;
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;
use tracing::info;
use tracing::log::warn;

/// Forwarder is responsible for reading messages from the source, applying transformation if
/// transformer is present, writing the messages to the sink, and then acknowledging the messages
/// back to the source.
pub(crate) struct Forwarder {
    source_client: SourceClient,
    sink_client: SinkClient,
    transformer_client: Option<TransformerClient>,
    fallback_client: Option<SinkClient>,
    cln_token: CancellationToken,
    common_labels: Vec<(String, String)>,
}

/// ForwarderBuilder is used to build a Forwarder instance with optional fields.
pub(crate) struct ForwarderBuilder {
    source_client: SourceClient,
    sink_client: SinkClient,
    cln_token: CancellationToken,
    transformer_client: Option<TransformerClient>,
    fb_sink_client: Option<SinkClient>,
}

impl ForwarderBuilder {
    /// Create a new builder with mandatory fields
    pub(crate) fn new(
        source_client: SourceClient,
        sink_client: SinkClient,
        cln_token: CancellationToken,
    ) -> Self {
        Self {
            source_client,
            sink_client,
            cln_token,
            transformer_client: None,
            fb_sink_client: None,
        }
    }

    /// Set the optional transformer client
    pub(crate) fn transformer_client(mut self, transformer_client: TransformerClient) -> Self {
        self.transformer_client = Some(transformer_client);
        self
    }

    /// Set the optional fallback client
    pub(crate) fn fb_sink_client(mut self, fallback_client: SinkClient) -> Self {
        self.fb_sink_client = Some(fallback_client);
        self
    }

    /// Build the Forwarder instance
    #[must_use]
    pub(crate) fn build(self) -> Forwarder {
        let common_labels = metrics::forward_metrics_labels().clone();
        Forwarder {
            source_client: self.source_client,
            sink_client: self.sink_client,
            transformer_client: self.transformer_client,
            fallback_client: self.fb_sink_client,
            cln_token: self.cln_token,
            common_labels,
        }
    }
}

impl Forwarder {
    /// run starts the forward-a-chunk loop and exits only after a chunk has been forwarded and ack'ed.
    /// this means that, in the happy path scenario a block is always completely processed.
    /// this function will return on any error and will cause end up in a non-0 exit code.
    pub(crate) async fn run(&mut self) -> Result<()> {
        let mut messages_count: u64 = 0;
        let mut last_forwarded_at = std::time::Instant::now();
        loop {
            let start_time = tokio::time::Instant::now();
            // two arms, either shutdown or forward-a-chunk
            tokio::select! {
                _ = self.cln_token.cancelled() => {
                    info!("Shutdown signal received, stopping forwarder...");
                    break;
                }

                /*
                    Read messages from the source, apply transformation if transformer is present,
                    write the messages to the sink, if fallback messages are present write them to the fallback sink,
                    and then acknowledge the messages back to the source.
                 */
                result = self.source_client.read_fn(config().batch_size, config().timeout_in_ms) => {
                    let messages = result?;
                    info!("Read batch size: {} and latency - {}ms", messages.len(), start_time.elapsed().as_millis());

                    let msg_count = messages.len() as u64;
                    // nothing more to be done.
                    if msg_count == 0 {
                        continue;
                    }
                    messages_count += messages.len() as u64;

                    // collect all the offsets as the transformer can drop (via filter) messages
                    let offsets = messages.iter().map(|msg| msg.offset.clone()).collect::<Vec<Offset>>();

                    let bytes_count = messages.iter().map(|msg| msg.value.len() as u64).sum::<u64>();
                    forward_metrics().monovtx_read_total.get_or_create(&self.common_labels).inc_by(msg_count);
                    forward_metrics().monovtx_read_bytes_total.get_or_create(&self.common_labels).inc_by(bytes_count);

                    // Apply transformation if transformer is present
                    let transformed_messages = if let Some(transformer_client) = &self.transformer_client {
                        let start_time = tokio::time::Instant::now();
                        let mut jh = JoinSet::new();
                        for message in messages {
                            let mut transformer_client = transformer_client.clone();
                            jh.spawn(async move { transformer_client.transform_fn(message).await });
                        }

                        let mut results = Vec::new();
                        while let Some(task) = jh.join_next().await {
                            let result = task.map_err(|e| Error::TransformerError(format!("{:?}", e)))?;
                            if let Some(result) = result? {
                                results.extend(result);
                            }
                        }
                        info!("Transformer latency - {}ms", start_time.elapsed().as_millis());
                        results
                    } else {
                        messages
                    };

                    let transformed_msg_count = transformed_messages.len() as u64;
                    forward_metrics().monovtx_sink_write_total.get_or_create(&self.common_labels).inc_by(transformed_msg_count);

                    // if all messages are dropped by the transformer, skip the sink
                    if transformed_messages.is_empty() {
                        continue;
                    }

                    let mut retry_msgs = transformed_messages;
                    let mut attempts = 0;
                    let mut error_map = HashMap::new();
                    let mut fallback_msgs = Vec::new();

                    while attempts <= config().sink_max_retry_attempts {
                        let start_time = tokio::time::Instant::now();
                        match self.sink_client.sink_fn(retry_msgs.clone()).await {
                            Ok(response) => {
                                info!("Sink latency - {}ms", start_time.elapsed().as_millis());
                                attempts += 1;

                                fallback_msgs.extend(response.results.iter()
                                    .filter(|result| result.status == proto::Status::Fallback as i32)
                                    .map(|result| retry_msgs.iter().find(|msg| msg.id == result.id).unwrap().clone())
                                    .collect::<Vec<_>>());

                                 retry_msgs = response.results.iter()
                                    .filter(|result| result.status == proto::Status::Failure as i32)
                                    .map(|result| retry_msgs.iter().find(|msg| msg.id == result.id).unwrap().clone())
                                    .collect::<Vec<_>>();

                                if retry_msgs.is_empty() {
                                    break;
                                } else {
                                    // Collect error messages and their counts
                                    error_map.clear();
                                    for result in response.results {
                                        if result.status == proto::Status::Failure as i32 {
                                            *error_map.entry(result.err_msg).or_insert(0) += 1;
                                        }
                                    }

                                    warn!("Retry attempt {} due to retryable error. Errors: {:?}", attempts, error_map);
                                    sleep(tokio::time::Duration::from_millis(config().sink_retry_interval_in_ms as u64)).await;
                                }
                            }
                            Err(e) => return Err(e),
                        }
                    }

                    if !retry_msgs.is_empty() {
                        return Err(Error::SinkError(format!(
                            "Failed to sink messages after {} attempts. Errors: {:?}",
                            attempts, error_map
                        )));
                    }

                    // If there are fallback messages, write them to the fallback sink
                    if !fallback_msgs.is_empty() {
                        match self.fallback_client {
                            Some(ref mut fallback_client) => {
                                let mut retry_fallback_msgs = fallback_msgs;
                                let mut attempts = 0;
                                let mut fallback_error_map = HashMap::new();

                                while attempts <= config().sink_max_retry_attempts {
                                    let start_time = tokio::time::Instant::now();
                                    match fallback_client.sink_fn(retry_fallback_msgs.clone()).await {
                                        Ok(response) => {
                                            info!("Fallback sink latency - {}ms", start_time.elapsed().as_millis());

                                            retry_fallback_msgs = response.results.iter()
                                                .filter(|result| result.status == proto::Status::Failure as i32)
                                                .map(|result| retry_fallback_msgs.iter().find(|msg| msg.id == result.id).unwrap().clone())
                                                .collect::<Vec<_>>();

                                            // if any one of response has status fallback return error saying, can't specify fallback inside fallback sink
                                            if response.results.iter().any(|result| result.status == proto::Status::Fallback as i32) {
                                                return Err(Error::SinkError("Fallback sink can't specify status fallback".to_string()));
                                            }

                                            attempts += 1;

                                            if retry_fallback_msgs.is_empty() {
                                                break;
                                            } else {
                                                // Collect error messages and their counts
                                                fallback_error_map.clear();
                                                for result in response.results {
                                                    if result.status != proto::Status::Success as i32 {
                                                        *fallback_error_map.entry(result.err_msg).or_insert(0) += 1;
                                                    }
                                                }

                                                warn!("Fallback sink retry attempt {} due to retryable error. Errors: {:?}", attempts, fallback_error_map);
                                                sleep(tokio::time::Duration::from_millis(config().sink_retry_interval_in_ms as u64)).await;
                                            }
                                        }
                                        Err(e) => return Err(e),
                                    }
                                }

                                if !retry_fallback_msgs.is_empty() {
                                    return Err(Error::SinkError(format!(
                                        "Failed to write messages to fallback sink after {} attempts. Errors: {:?}",
                                        attempts, fallback_error_map
                                    )));
                                }
                            }
                            None => {
                                return Err(Error::SinkError("Response contains fallback messages but no fallback sink is configured".to_string()));
                            }
                        }
                    }

                    // Acknowledge the messages back to the source
                    let start_time = tokio::time::Instant::now();
                    self.source_client.ack_fn(offsets).await?;
                    info!("Ack latency - {}ms", start_time.elapsed().as_millis());
                    forward_metrics().monovtx_ack_total.get_or_create(&self.common_labels).inc_by(msg_count);
                }
            }
            // if the last forward was more than 1 second ago, forward a chunk print the number of messages forwarded
            if last_forwarded_at.elapsed().as_millis() >= 1000 {
                info!(
                    "Forwarded {} messages at time {}",
                    messages_count,
                    Utc::now()
                );
                messages_count = 0;
                last_forwarded_at = std::time::Instant::now();
            }
            forward_metrics()
                .monovtx_processing_time
                .get_or_create(&self.common_labels)
                .observe(start_time.elapsed().as_micros() as f64);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use crate::error::Result;
    use crate::forwarder::ForwarderBuilder;
    use crate::sink::{SinkClient, SinkConfig};
    use crate::source::{SourceClient, SourceConfig};
    use crate::transformer::{TransformerClient, TransformerConfig};
    use chrono::Utc;
    use numaflow::source::{Message, Offset, SourceReadRequest};
    use numaflow::{sink, source, sourcetransform};
    use tokio::sync::mpsc::Sender;
    use tokio_util::sync::CancellationToken;

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

        async fn ack(&self, offsets: Vec<Offset>) {
            for offset in offsets {
                self.yet_to_be_acked
                    .write()
                    .unwrap()
                    .remove(&String::from_utf8(offset.offset).unwrap());
            }
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
        async fn sink(
            &self,
            mut input: tokio::sync::mpsc::Receiver<sink::SinkRequest>,
        ) -> Vec<sink::Response> {
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
        let (sink_tx, mut sink_rx) = tokio::sync::mpsc::channel(10);

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
        let source_config = SourceConfig {
            socket_path: source_sock_file.to_str().unwrap().to_string(),
            server_info_file: server_info_file.to_str().unwrap().to_string(),
            max_message_size: 4 * 1024 * 1024,
        };

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
        let sink_config = SinkConfig {
            socket_path: sink_sock_file.to_str().unwrap().to_string(),
            server_info_file: server_info_file.to_str().unwrap().to_string(),
            max_message_size: 4 * 1024 * 1024,
        };

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
        let transformer_config = TransformerConfig {
            socket_path: transformer_sock_file.to_str().unwrap().to_string(),
            server_info_file: server_info_file.to_str().unwrap().to_string(),
            max_message_size: 4 * 1024 * 1024,
        };

        // Wait for the servers to start
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        let cln_token = CancellationToken::new();

        let source_client = SourceClient::connect(source_config)
            .await
            .expect("failed to connect to source server");

        let sink_client = SinkClient::connect(sink_config)
            .await
            .expect("failed to connect to sink server");

        let transformer_client = TransformerClient::connect(transformer_config)
            .await
            .expect("failed to connect to transformer server");

        let mut forwarder = ForwarderBuilder::new(source_client, sink_client, cln_token.clone())
            .transformer_client(transformer_client)
            .build();

        let forwarder_handle = tokio::spawn(async move {
            forwarder.run().await.unwrap();
        });

        // Receive messages from the sink
        let received_message = sink_rx.recv().await.unwrap();
        assert_eq!(received_message.value, "test-message".as_bytes());
        assert_eq!(
            received_message.keys,
            vec!["test-key-transformed".to_string()]
        );

        // stop the forwarder
        cln_token.cancel();
        forwarder_handle
            .await
            .expect("failed to join forwarder task");

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
        let source_config = SourceConfig {
            socket_path: source_sock_file.to_str().unwrap().to_string(),
            server_info_file: server_info_file.to_str().unwrap().to_string(),
            max_message_size: 4 * 1024 * 1024,
        };

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
        let sink_config = SinkConfig {
            socket_path: sink_sock_file.to_str().unwrap().to_string(),
            server_info_file: server_info_file.to_str().unwrap().to_string(),
            max_message_size: 4 * 1024 * 1024,
        };

        // Wait for the servers to start
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        let cln_token = CancellationToken::new();

        let source_client = SourceClient::connect(source_config)
            .await
            .expect("failed to connect to source server");

        let sink_client = SinkClient::connect(sink_config)
            .await
            .expect("failed to connect to sink server");

        let mut forwarder =
            ForwarderBuilder::new(source_client, sink_client, cln_token.clone()).build();

        let forwarder_handle = tokio::spawn(async move {
            forwarder.run().await?;
            Ok(())
        });

        // Set a timeout for the forwarder
        let timeout_duration = tokio::time::Duration::from_secs(1);
        let result = tokio::time::timeout(timeout_duration, forwarder_handle).await;
        let result: Result<()> = result.expect("forwarder_handle timed out").unwrap();
        assert!(result.is_err());

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
    }

    // Sink that returns status fallback
    struct FallbackSender {}

    #[tonic::async_trait]
    impl sink::Sinker for FallbackSender {
        async fn sink(
            &self,
            mut input: tokio::sync::mpsc::Receiver<sink::SinkRequest>,
        ) -> Vec<sink::Response> {
            let mut responses = vec![];
            while let Some(datum) = input.recv().await {
                responses.append(&mut vec![sink::Response::fallback(datum.id)]);
            }
            responses
        }
    }

    #[tokio::test]
    async fn test_fb_sink() {
        let (sink_tx, mut sink_rx) = tokio::sync::mpsc::channel(10);

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
        let source_config = SourceConfig {
            socket_path: source_sock_file.to_str().unwrap().to_string(),
            server_info_file: server_info_file.to_str().unwrap().to_string(),
            max_message_size: 4 * 1024 * 1024,
        };

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
        let sink_config = SinkConfig {
            socket_path: sink_sock_file.to_str().unwrap().to_string(),
            server_info_file: server_info_file.to_str().unwrap().to_string(),
            max_message_size: 4 * 1024 * 1024,
        };

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
        let fb_sink_config = SinkConfig {
            socket_path: fb_sink_sock_file.to_str().unwrap().to_string(),
            server_info_file: server_info_file.to_str().unwrap().to_string(),
            max_message_size: 4 * 1024 * 1024,
        };

        // Wait for the servers to start
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        let cln_token = CancellationToken::new();

        let source_client = SourceClient::connect(source_config)
            .await
            .expect("failed to connect to source server");

        let sink_client = SinkClient::connect(sink_config)
            .await
            .expect("failed to connect to sink server");

        let fb_sink_client = SinkClient::connect(fb_sink_config)
            .await
            .expect("failed to connect to fb sink server");

        let mut forwarder = ForwarderBuilder::new(source_client, sink_client, cln_token.clone())
            .fb_sink_client(fb_sink_client)
            .build();

        let forwarder_handle = tokio::spawn(async move {
            forwarder.run().await.unwrap();
        });

        // We should receive the message in the fallback sink, since the primary sink returns status fallback
        let received_message = sink_rx.recv().await.unwrap();
        assert_eq!(received_message.value, "test-message".as_bytes());
        assert_eq!(received_message.keys, vec!["test-key".to_string()]);

        // stop the forwarder
        cln_token.cancel();
        forwarder_handle
            .await
            .expect("failed to join forwarder task");

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
