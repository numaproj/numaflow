//! The forwarder for [MonoVertex] at its core orchestrates message movement asynchronously using
//! [Stream] over channels between the components. The messages send over this channel using
//! [Actor Pattern].
//!
//! ```text
//! (source) --[c]--> (transformer)* --[c]--> (sink)
//!
//! [c] - channel
//! * - optional
//!  ```
//!
//! Most of the data move forward except for the ack which can happen only after the Write.
//! ```text
//! (Read) +-------> (UDF) -------> (Write) +
//!        |                                |
//!        |                                |
//!        +-------> {Ack} <----------------+
//!
//! {} -> Listens on a OneShot
//! () -> Streaming Interface
//! ```
//!
//! [MonoVertex]: https://numaflow.numaproj.io/core-concepts/monovertex/
//! [Stream]: https://docs.rs/tokio-stream/latest/tokio_stream/wrappers/struct.ReceiverStream.html
//! [Actor Pattern]: https://ryhl.io/blog/actors-with-tokio/

use tokio_util::sync::CancellationToken;

use crate::error;
use crate::sink::SinkWriter;
use crate::source::Source;
use crate::transformer::Transformer;
use crate::Error;

/// Forwarder is responsible for reading messages from the source, applying transformation if
/// transformer is present, writing the messages to the sink, and then acknowledging the messages
/// back to the source.
pub(crate) struct Forwarder {
    source: Source,
    transformer: Option<Transformer>,
    sink_writer: SinkWriter,
    cln_token: CancellationToken,
}

pub(crate) struct ForwarderBuilder {
    source: Source,
    sink_writer: SinkWriter,
    cln_token: CancellationToken,
    transformer: Option<Transformer>,
}

impl ForwarderBuilder {
    /// Create a new builder with mandatory fields
    pub(crate) fn new(
        streaming_source: Source,
        streaming_sink: SinkWriter,
        cln_token: CancellationToken,
    ) -> Self {
        Self {
            source: streaming_source,
            sink_writer: streaming_sink,
            cln_token,
            transformer: None,
        }
    }

    /// Set the optional transformer client
    pub(crate) fn transformer(mut self, transformer: Transformer) -> Self {
        self.transformer = Some(transformer);
        self
    }

    /// Build the StreamingForwarder instance
    #[must_use]
    pub(crate) fn build(self) -> Forwarder {
        Forwarder {
            source: self.source,
            sink_writer: self.sink_writer,
            transformer: self.transformer,
            cln_token: self.cln_token,
        }
    }
}

impl Forwarder {
    pub(crate) async fn start(&self) -> error::Result<()> {
        let (messages_stream, reader_handle) =
            self.source.streaming_read(self.cln_token.clone())?;

        let (transformed_messages_stream, transformer_handle) =
            if let Some(transformer) = &self.transformer {
                let (transformed_messages_rx, transformer_handle) =
                    transformer.transform_stream(messages_stream)?;
                (transformed_messages_rx, Some(transformer_handle))
            } else {
                (messages_stream, None)
            };

        let sink_writer_handle = self
            .sink_writer
            .streaming_write(transformed_messages_stream, self.cln_token.clone())
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
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use chrono::Utc;
    use numaflow::source::{Message, Offset, SourceReadRequest};
    use numaflow::{source, sourcetransform};
    use numaflow_pb::clients::source::source_client::SourceClient;
    use numaflow_pb::clients::sourcetransformer::source_transform_client::SourceTransformClient;
    use tempfile::TempDir;
    use tokio::sync::mpsc::Sender;
    use tokio::sync::oneshot;
    use tokio::task::JoinHandle;
    use tokio_util::sync::CancellationToken;

    use crate::monovertex::forwarder::ForwarderBuilder;
    use crate::shared::grpc::create_rpc_channel;
    use crate::sink::{SinkClientType, SinkWriterBuilder};
    use crate::source::user_defined::new_source;
    use crate::source::{Source, SourceType};
    use crate::transformer::Transformer;
    use crate::Result;

    struct SimpleSource {
        num: usize,
        sent_count: AtomicUsize,
        yet_to_ack: std::sync::RwLock<HashSet<String>>,
    }

    impl SimpleSource {
        fn new(num: usize) -> Self {
            Self {
                num,
                sent_count: AtomicUsize::new(0),
                yet_to_ack: std::sync::RwLock::new(HashSet::new()),
            }
        }
    }

    #[tonic::async_trait]
    impl source::Sourcer for SimpleSource {
        async fn read(&self, request: SourceReadRequest, transmitter: Sender<Message>) {
            let event_time = Utc::now();
            let mut message_offsets = Vec::with_capacity(request.count);

            for i in 0..request.count {
                if self.sent_count.load(Ordering::SeqCst) >= self.num {
                    return;
                }

                let offset = format!("{}-{}", event_time.timestamp_nanos_opt().unwrap(), i);
                transmitter
                    .send(Message {
                        value: b"hello".to_vec(),
                        event_time,
                        offset: Offset {
                            offset: offset.clone().into_bytes(),
                            partition_id: 0,
                        },
                        keys: vec![],
                        headers: Default::default(),
                    })
                    .await
                    .unwrap();
                message_offsets.push(offset);
                self.sent_count.fetch_add(1, Ordering::SeqCst);
            }
            self.yet_to_ack.write().unwrap().extend(message_offsets);
        }

        async fn ack(&self, offsets: Vec<Offset>) {
            for offset in offsets {
                self.yet_to_ack
                    .write()
                    .unwrap()
                    .remove(&String::from_utf8(offset.offset).unwrap());
            }
        }

        async fn pending(&self) -> usize {
            self.num - self.sent_count.load(Ordering::SeqCst)
                + self.yet_to_ack.read().unwrap().len()
        }

        async fn partitions(&self) -> Option<Vec<i32>> {
            Some(vec![1, 2])
        }
    }

    struct SimpleTransformer;

    #[tonic::async_trait]
    impl sourcetransform::SourceTransformer for SimpleTransformer {
        async fn transform(
            &self,
            input: sourcetransform::SourceTransformRequest,
        ) -> Vec<sourcetransform::Message> {
            let message = sourcetransform::Message::new(input.value, Utc::now()).keys(input.keys);
            vec![message]
        }
    }

    #[tokio::test]
    async fn test_forwarder() {
        // create the source which produces x number of messages
        let cln_token = CancellationToken::new();

        let (src_shutdown_tx, src_shutdown_rx) = oneshot::channel();
        let tmp_dir = TempDir::new().unwrap();
        let sock_file = tmp_dir.path().join("source.sock");
        let server_info_file = tmp_dir.path().join("source-server-info");

        let server_info = server_info_file.clone();
        let server_socket = sock_file.clone();
        let source_handle = tokio::spawn(async move {
            // a simple source which generates total of 100 messages
            source::Server::new(SimpleSource::new(100))
                .with_socket_file(server_socket)
                .with_server_info_file(server_info)
                .start_with_shutdown(src_shutdown_rx)
                .await
                .unwrap()
        });

        // wait for the server to start
        // TODO: flaky
        tokio::time::sleep(Duration::from_millis(100)).await;

        let client = SourceClient::new(create_rpc_channel(sock_file).await.unwrap());

        let (src_read, src_ack, lag_reader) = new_source(client, 5, Duration::from_millis(1000))
            .await
            .map_err(|e| panic!("failed to create source reader: {:?}", e))
            .unwrap();

        let source = Source::new(
            5,
            SourceType::UserDefinedSource(src_read, src_ack, lag_reader),
        );

        // create a transformer
        let (st_shutdown_tx, st_shutdown_rx) = oneshot::channel();
        let tmp_dir = TempDir::new().unwrap();
        let sock_file = tmp_dir.path().join("sourcetransform.sock");
        let server_info_file = tmp_dir.path().join("sourcetransformer-server-info");

        let server_info = server_info_file.clone();
        let server_socket = sock_file.clone();
        let transformer_handle = tokio::spawn(async move {
            sourcetransform::Server::new(SimpleTransformer)
                .with_socket_file(server_socket)
                .with_server_info_file(server_info)
                .start_with_shutdown(st_shutdown_rx)
                .await
                .expect("server failed");
        });

        // wait for the server to start
        tokio::time::sleep(Duration::from_millis(100)).await;

        let client = SourceTransformClient::new(create_rpc_channel(sock_file).await.unwrap());
        let transformer = Transformer::new(10, 10, client).await.unwrap();

        let sink_writer =
            SinkWriterBuilder::new(10, Duration::from_millis(100), SinkClientType::Log)
                .build()
                .await
                .unwrap();

        // create the forwarder with the source, transformer, and writer
        let forwarder = ForwarderBuilder::new(source.clone(), sink_writer, cln_token.clone())
            .transformer(transformer)
            .build();

        let forwarder_handle: JoinHandle<Result<()>> = tokio::spawn(async move {
            forwarder.start().await?;
            Ok(())
        });

        // wait for one sec to check if the pending becomes zero, because all the messages
        // should be read and acked; if it doesn't, then fail the test
        let tokio_result = tokio::time::timeout(Duration::from_secs(1), async move {
            loop {
                let pending = source.pending().await.unwrap();
                if pending == Some(0) {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await;

        assert!(
            tokio_result.is_ok(),
            "Timeout occurred before pending became zero"
        );
        cln_token.cancel();
        forwarder_handle.await.unwrap().unwrap();
        st_shutdown_tx.send(()).unwrap();
        src_shutdown_tx.send(()).unwrap();
        source_handle.await.unwrap();
        transformer_handle.await.unwrap();
    }
}
