use tokio_util::sync::CancellationToken;

use crate::config::monovertex::MonovertexConfig;
use crate::error;
use crate::sink::SinkWriter;
use crate::source::Source;
use crate::transformer::Transformer;
use crate::Error;

/// Forwarder is responsible for reading messages from the source, applying transformation if
/// transformer is present, writing the messages to the sink, and then acknowledging the messages
/// back to the source.
pub(crate) struct Forwarder {
    streaming_source: Source,
    streaming_transformer: Option<Transformer>,
    streaming_sink: SinkWriter,
    cln_token: CancellationToken,
    #[allow(dead_code)]
    mvtx_config: MonovertexConfig,
}

pub(crate) struct ForwarderBuilder {
    streaming_source: Source,
    streaming_sink: SinkWriter,
    cln_token: CancellationToken,
    streaming_transformer: Option<Transformer>,
    mvtx_config: MonovertexConfig,
}

impl ForwarderBuilder {
    /// Create a new builder with mandatory fields
    pub(crate) fn new(
        streaming_source: Source,
        streaming_sink: SinkWriter,
        mvtx_config: MonovertexConfig,
        cln_token: CancellationToken,
    ) -> Self {
        Self {
            streaming_source,
            streaming_sink,
            cln_token,
            streaming_transformer: None,
            mvtx_config,
        }
    }

    /// Set the optional transformer client
    pub(crate) fn streaming_transformer(mut self, transformer_client: Transformer) -> Self {
        self.streaming_transformer = Some(transformer_client);
        self
    }

    /// Build the StreamingForwarder instance
    #[must_use]
    pub(crate) fn build(self) -> Forwarder {
        Forwarder {
            streaming_source: self.streaming_source,
            streaming_sink: self.streaming_sink,
            streaming_transformer: self.streaming_transformer,
            cln_token: self.cln_token,
            mvtx_config: self.mvtx_config,
        }
    }
}

impl Forwarder {
    pub(crate) async fn start(&self) -> error::Result<()> {
        let (read_messages_rx, reader_handle) = self.streaming_source.streaming_read()?;

        let (transformed_messages_rx, transformer_handle) =
            if let Some(transformer) = &self.streaming_transformer {
                let (transformed_messages_rx, transformer_handle) =
                    transformer.transform_stream(read_messages_rx)?;
                (transformed_messages_rx, Some(transformer_handle))
            } else {
                (read_messages_rx, None)
            };

        let sink_writer_handle = self
            .streaming_sink
            .streaming_write(transformed_messages_rx, self.cln_token.clone())
            .await?;

        match tokio::try_join!(
            reader_handle,
            transformer_handle.unwrap_or_else(|| tokio::spawn(async { Ok(()) })),
            sink_writer_handle,
        ) {
            Ok((reader_result, transformer_result, sink_writer_result)) => {
                reader_result?;
                transformer_result?;
                sink_writer_result?;
                Ok(())
            }
            Err(e) => Err(Error::Forwarder(format!(
                "Error while joining reader, transformer, and sink writer: {:?}",
                e
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::time::Duration;

    use chrono::Utc;
    use numaflow::source::{Message, Offset, SourceReadRequest};
    use numaflow::{sink, source, sourcetransform};
    use numaflow_pb::clients::sink::sink_client::SinkClient;
    use numaflow_pb::clients::source::source_client::SourceClient;
    use numaflow_pb::clients::sourcetransformer::source_transform_client::SourceTransformClient;
    use tokio::sync::mpsc;
    use tokio::sync::mpsc::Sender;
    use tokio_util::sync::CancellationToken;

    use crate::monovertex::forwarder::ForwarderBuilder;
    use crate::shared::utils::create_rpc_channel;
    use crate::sink::SinkClientType;
    use crate::source::user_defined::new_source;
    use crate::source::SourceType;

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
            Duration::from_millis(timeout_in_ms),
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
            Duration::from_millis(timeout_in_ms),
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
        tokio::time::sleep(Duration::from_millis(100)).await;

        let cln_token = CancellationToken::new();

        let (source_read, source_ack, source_lag_reader) = new_source(
            SourceClient::new(create_rpc_channel(source_sock_file.clone()).await.unwrap()),
            500,
            Duration::from_millis(100),
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
