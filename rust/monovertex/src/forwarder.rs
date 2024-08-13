use chrono::Utc;
use metrics::counter;
use std::collections::HashMap;
use tokio::task::JoinSet;
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;
use tracing::info;

use crate::config::config;
use crate::error::{Error, Result};
use crate::message::Offset;
use crate::metrics::{
    FORWARDER_ACK_TOTAL, FORWARDER_READ_BYTES_TOTAL, FORWARDER_READ_TOTAL, FORWARDER_WRITE_TOTAL,
    MONO_VERTEX_NAME, PARTITION_LABEL, REPLICA_LABEL, VERTEX_TYPE_LABEL,
};
use crate::sink::{proto, SinkClient};
use crate::source::SourceClient;
use crate::transformer::TransformerClient;

const MONO_VERTEX_TYPE: &str = "mono_vertex";

/// Forwarder is responsible for reading messages from the source, applying transformation if
/// transformer is present, writing the messages to the sink, and then acknowledging the messages
/// back to the source.
pub(crate) struct Forwarder {
    source_client: SourceClient,
    sink_client: SinkClient,
    transformer_client: Option<TransformerClient>,
    cln_token: CancellationToken,
    common_labels: Vec<(String, String)>,
}

impl Forwarder {
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn new(
        source_client: SourceClient,
        sink_client: SinkClient,
        transformer_client: Option<TransformerClient>,
        cln_token: CancellationToken,
    ) -> Result<Self> {
        let common_labels = vec![
            (
                MONO_VERTEX_NAME.to_string(),
                config().mono_vertex_name.clone(),
            ),
            (VERTEX_TYPE_LABEL.to_string(), MONO_VERTEX_TYPE.to_string()),
            (REPLICA_LABEL.to_string(), config().replica.to_string()),
            (PARTITION_LABEL.to_string(), "0".to_string()),
        ];

        Ok(Self {
            source_client,
            sink_client,
            transformer_client,
            common_labels,
            cln_token,
        })
    }

    /// run starts the forward-a-chunk loop and exits only after a chunk has been forwarded and ack'ed.
    /// this means that, in the happy path scenario a block is always completely processed.
    /// this function will return on any error and will cause end up in a non-0 exit code.
    pub(crate) async fn run(&mut self) -> Result<()> {
        let mut messages_count: u64 = 0;
        let mut last_forwarded_at = std::time::Instant::now();
        loop {
            // TODO: emit latency metrics, metrics-rs histograms has memory leak issues.
            let start_time = tokio::time::Instant::now();
            // two arms, either shutdown or forward-a-chunk
            tokio::select! {
                _ = self.cln_token.cancelled() => {
                    info!("Shutdown signal received, stopping forwarder...");
                    break;
                }
                result = self.source_client.read_fn(config().batch_size, config().timeout_in_ms) => {
                    // Read messages from the source
                    let messages = result?;
                    info!("Read batch size: {} and latency - {}ms", messages.len(), start_time.elapsed().as_millis());

                    messages_count += messages.len() as u64;
                    let bytes_count = messages.iter().map(|msg| msg.value.len() as u64).sum::<u64>();
                    counter!(FORWARDER_READ_TOTAL, &self.common_labels).increment(messages_count);
                    counter!(FORWARDER_READ_BYTES_TOTAL, &self.common_labels).increment(bytes_count);

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
                            let result = result?;
                            results.extend(result);
                        }
                        info!("Transformer latency - {}ms", start_time.elapsed().as_millis());
                        results
                    } else {
                        messages
                    };

                    // Write messages to the sink
                    // TODO: should we retry writing? what if the error is transient?
                    //    we could rely on gRPC retries and say that any error that is bubbled up is worthy of non-0 exit.
                    //    we need to confirm this via FMEA tests.
                    let mut retry_messages = transformed_messages;
                    let mut attempts = 0;
                    let mut error_map = HashMap::new();

                    while attempts <= config().sink_max_retry_attempts {
                        let start_time = tokio::time::Instant::now();
                        match self.sink_client.sink_fn(retry_messages.clone()).await {
                            Ok(response) => {
                                info!("Sink latency - {}ms", start_time.elapsed().as_millis());

                                let failed_ids: Vec<String> = response.results.iter()
                                    .filter(|result| result.status != proto::Status::Success as i32)
                                    .map(|result| result.id.clone())
                                    .collect();

                                let successful_offsets: Vec<Offset> = retry_messages.iter()
                                    .filter(|msg| !failed_ids.contains(&msg.id))
                                    .map(|msg| msg.offset.clone())
                                    .collect();


                                // ack the successful offsets
                                let n = successful_offsets.len();
                                self.source_client.ack_fn(successful_offsets).await?;
                                counter!(FORWARDER_WRITE_TOTAL, &self.common_labels).increment(n as u64);
                                attempts += 1;

                                if retry_messages.is_empty() {
                                    break;
                                } else {
                                    retry_messages.retain(|msg| failed_ids.contains(&msg.id));

                                    // Collect error messages and their counts
                                    error_map.clear();
                                    for result in response.results {
                                        if result.status != proto::Status::Success as i32 {
                                            *error_map.entry(result.err_msg).or_insert(0) += 1;
                                        }
                                    }

                                    info!("Retry attempt {} due to retryable error. Errors: {:?}", attempts, error_map);
                                    sleep(tokio::time::Duration::from_millis(1)).await;
                                }
                            }
                            Err(e) => return Err(e),
                        }
                    }

                    if !retry_messages.is_empty() {
                        return Err(Error::SinkError(format!(
                            "Failed to sink messages after {} attempts. Errors: {:?}",
                            attempts, error_map
                        )));
                    }

                    counter!(FORWARDER_ACK_TOTAL, &self.common_labels).increment(messages_count);
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
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use crate::error::Error;
    use crate::forwarder::Forwarder;
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
        // Create channels for communication
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

        let mut forwarder = Forwarder::new(
            source_client,
            sink_client,
            Some(transformer_client),
            cln_token.clone(),
        )
        .await
        .expect("failed to create forwarder");

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

        let mut forwarder = Forwarder::new(source_client, sink_client, None, cln_token.clone())
            .await
            .expect("failed to create forwarder");

        let forwarder_handle = tokio::spawn(async move {
            forwarder.run().await?;
            Ok(())
        });

        // Set a timeout for the forwarder
        let timeout_duration = tokio::time::Duration::from_secs(1);
        let result = tokio::time::timeout(timeout_duration, forwarder_handle).await;
        let result: Result<(), Error> = result.expect("forwarder_handle timed out").unwrap();
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
}
