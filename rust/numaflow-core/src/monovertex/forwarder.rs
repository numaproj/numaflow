//! The forwarder for [MonoVertex] at its core orchestrates message movement asynchronously using
//! [Stream] over channels between the components. The messages send over this channel using
//! [Actor Pattern].
//!
//! ```text
//! (source) --[c]--> (transformer)* --[c]--> (sink)
//!    |                   |                      |
//!    |                   v                      |
//!    +--------------> tracker <----------------+
//!
//! [c] - channel
//! * - optional
//!  ```
//!
//! Most of the data move forward except for the `ack` which can happen only after the tracker
//! has guaranteed that the processing has completed. Ack is spawned during the reading.
//! ```text
//! (Read) +-------> (UDF) -------> (Write) +
//!        |                                |
//!        |                                |
//!        +-------> {tracker} <------------
//!                     |
//!                     |
//!                     v
//!                   {ack}
//!
//! {} -> Listens on a OneShot
//! () -> Streaming Interface
//! ```
//!
//! [MonoVertex]: https://numaflow.numaproj.io/core-concepts/monovertex/
//! [Stream]: https://docs.rs/tokio-stream/latest/tokio_stream/wrappers/struct.ReceiverStream.html
//! [Actor Pattern]: https://ryhl.io/blog/actors-with-tokio/

use tokio_util::sync::CancellationToken;

use crate::Error;
use crate::error;
use crate::mapper::map::MapHandle;
use crate::sinker::sink::SinkWriter;
use crate::source::Source;

/// Forwarder is responsible for reading messages from the source, applying transformation if
/// transformer is present, writing the messages to the sink, and then acknowledging the messages
/// back to the source.
pub(crate) struct Forwarder<C: crate::typ::NumaflowTypeConfig> {
    source: Source<C>,
    mapper: Option<MapHandle>,
    sink_writer: SinkWriter,
}

impl<C: crate::typ::NumaflowTypeConfig> Forwarder<C> {
    pub(crate) fn new(
        source: Source<C>,
        mapper: Option<MapHandle>,
        sink_writer: SinkWriter,
    ) -> Self {
        Self {
            source,
            mapper,
            sink_writer,
        }
    }

    pub(crate) async fn start(self, cln_token: CancellationToken) -> crate::Result<()> {
        let (read_messages_stream, reader_handle) =
            self.source.streaming_read(cln_token.clone())?;

        let (mapper_stream, mapper_handle) = match self.mapper {
            Some(mapper) => {
                mapper
                    // Performs respective map operation (unary, batch, stream) based on actor_sender
                    .streaming_map(read_messages_stream, cln_token.clone())
                    .await?
            }
            None => (
                read_messages_stream,
                tokio::task::spawn(async { Ok::<(), Error>(()) }),
            ),
        };

        let sink_writer_handle = self
            .sink_writer
            .streaming_write(mapper_stream, cln_token.clone())
            .await?;

        // Join the reader and sink writer
        let (reader_result, mapper_handle_result, sink_writer_result) =
            tokio::try_join!(reader_handle, mapper_handle, sink_writer_handle).map_err(|e| {
                error!(?e, "Error while joining reader, mapper and sink writer");
                Error::Forwarder(format!(
                    "Error while joining reader, mapper and sink writer: {e:?}"
                ))
            })?;

        sink_writer_result.inspect_err(|e| {
            error!(?e, "Error while writing messages");
        })?;

        mapper_handle_result.inspect_err(|e| {
            error!(?e, "Error while applying map to messages");
        })?;

        reader_result.inspect_err(|e| {
            error!(?e, "Error while reading messages");
        })?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::Result;
    use crate::mapper::map::MapHandle;
    use crate::monovertex::forwarder::Forwarder;
    use crate::shared::grpc::create_rpc_channel;
    use crate::sinker::sink::{SinkClientType, SinkWriterBuilder};
    use crate::source::user_defined::new_source;
    use crate::source::{Source, SourceType};
    use crate::tracker::Tracker;
    use crate::transformer::Transformer;
    use chrono::Utc;
    use numaflow::shared::ServerExtras;
    use numaflow::source::{Message, Offset, SourceReadRequest};
    use numaflow::{batchmap, map, mapstream, source, sourcetransform};
    use numaflow_pb::clients::map::map_client::MapClient;
    use numaflow_pb::clients::source::source_client::SourceClient;
    use numaflow_pb::clients::sourcetransformer::source_transform_client::SourceTransformClient;
    use numaflow_shared::server_info::MapMode;
    use std::collections::HashSet;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;
    use tempfile::TempDir;
    use tokio::sync::mpsc::Sender;
    use tokio::sync::oneshot;
    use tokio::task::JoinHandle;
    use tokio_util::sync::CancellationToken;

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
                        value: b"hello,world".to_vec(),
                        event_time,
                        offset: Offset {
                            offset: offset.clone().into_bytes(),
                            partition_id: 0,
                        },
                        keys: vec![],
                        headers: Default::default(),
                        user_metadata: None,
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

        async fn nack(&self, _offsets: Vec<Offset>) {}

        async fn pending(&self) -> Option<usize> {
            Some(
                self.num - self.sent_count.load(Ordering::SeqCst)
                    + self.yet_to_ack.read().unwrap().len(),
            )
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
            let message =
                sourcetransform::Message::new(input.value, Utc::now()).with_keys(input.keys);
            vec![message]
        }
    }

    #[tokio::test]
    async fn test_forwarder() {
        let tracker = Tracker::new(None, CancellationToken::new());

        // create the source which produces x number of messages
        let cln_token = CancellationToken::new();

        // create a transformer for this source
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
        let transformer =
            Transformer::new(10, 10, Duration::from_secs(10), client, tracker.clone())
                .await
                .unwrap();

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

        let client = SourceClient::new(create_rpc_channel(sock_file).await.unwrap());

        let (src_read, src_ack, lag_reader) = new_source(
            client,
            5,
            Duration::from_millis(1000),
            cln_token.clone(),
            true,
        )
        .await
        .map_err(|e| panic!("failed to create source reader: {:?}", e))
        .unwrap();
        let tracker = Tracker::new(None, CancellationToken::new());
        let source: Source<crate::typ::WithoutRateLimiter> = Source::new(
            5,
            SourceType::UserDefinedSource(Box::new(src_read), Box::new(src_ack), lag_reader),
            tracker.clone(),
            true,
            Some(transformer),
            None,
            None,
        )
        .await;

        let sink_writer =
            SinkWriterBuilder::new(10, Duration::from_millis(100), SinkClientType::Log)
                .build()
                .await
                .unwrap();

        // create the forwarder with the source, transformer, and writer
        let forwarder = Forwarder::new(source.clone(), None, sink_writer);

        let cancel_token = cln_token.clone();
        let forwarder_handle: JoinHandle<Result<()>> = tokio::spawn(async move {
            forwarder.start(cancel_token).await?;
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

    struct FlatMapTransformer;

    #[tonic::async_trait]
    impl sourcetransform::SourceTransformer for FlatMapTransformer {
        async fn transform(
            &self,
            _input: sourcetransform::SourceTransformRequest,
        ) -> Vec<sourcetransform::Message> {
            let mut output = vec![];
            for i in 0..5 {
                let message = sourcetransform::Message::new(i.to_string().into_bytes(), Utc::now())
                    .with_keys(vec![format!("key-{}", i)])
                    .with_tags(vec![]);
                output.push(message);
            }
            output
        }
    }

    #[tokio::test]
    async fn test_transformer_flatmap_operation() {
        // create the source which produces x number of messages
        let cln_token = CancellationToken::new();
        let tracker = Tracker::new(None, cln_token.clone());

        // create a transformer
        let (st_shutdown_tx, st_shutdown_rx) = oneshot::channel();
        let tmp_dir = TempDir::new().unwrap();
        let sock_file = tmp_dir.path().join("sourcetransform.sock");
        let server_info_file = tmp_dir.path().join("sourcetransformer-server-info");

        let server_info = server_info_file.clone();
        let server_socket = sock_file.clone();
        let transformer_handle = tokio::spawn(async move {
            sourcetransform::Server::new(FlatMapTransformer)
                .with_socket_file(server_socket)
                .with_server_info_file(server_info)
                .start_with_shutdown(st_shutdown_rx)
                .await
                .expect("server failed");
        });

        let client = SourceTransformClient::new(create_rpc_channel(sock_file).await.unwrap());
        let transformer =
            Transformer::new(10, 10, Duration::from_secs(10), client, tracker.clone())
                .await
                .unwrap();

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

        let client = SourceClient::new(create_rpc_channel(sock_file).await.unwrap());

        let (src_read, src_ack, lag_reader) = new_source(
            client,
            5,
            Duration::from_millis(1000),
            cln_token.clone(),
            true,
        )
        .await
        .map_err(|e| panic!("failed to create source reader: {:?}", e))
        .unwrap();

        let source: Source<crate::typ::WithoutRateLimiter> = Source::new(
            5,
            SourceType::UserDefinedSource(Box::new(src_read), Box::new(src_ack), lag_reader),
            tracker.clone(),
            true,
            Some(transformer),
            None,
            None,
        )
        .await;

        let sink_writer =
            SinkWriterBuilder::new(10, Duration::from_millis(100), SinkClientType::Log)
                .build()
                .await
                .unwrap();

        // create the forwarder with the source, transformer, and writer
        let forwarder = Forwarder::new(source.clone(), None, sink_writer);

        let cancel_token = cln_token.clone();
        let forwarder_handle: JoinHandle<Result<()>> = tokio::spawn(async move {
            forwarder.start(cancel_token).await?;
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

    struct Cat;

    #[tonic::async_trait]
    impl map::Mapper for Cat {
        async fn map(&self, input: map::MapRequest) -> Vec<map::Message> {
            let message = map::Message::new(input.value)
                .with_keys(input.keys)
                .with_tags(vec![]);
            vec![message]
        }
    }

    #[tonic::async_trait]
    impl batchmap::BatchMapper for Cat {
        async fn batchmap(
            &self,
            mut input: tokio::sync::mpsc::Receiver<batchmap::Datum>,
        ) -> Vec<batchmap::BatchResponse> {
            let mut responses: Vec<batchmap::BatchResponse> = Vec::new();
            while let Some(datum) = input.recv().await {
                let mut response = batchmap::BatchResponse::from_id(datum.id);
                response.append(batchmap::Message {
                    keys: Option::from(datum.keys),
                    value: datum.value,
                    tags: None,
                });
                responses.push(response);
            }
            responses
        }
    }

    #[tonic::async_trait]
    impl mapstream::MapStreamer for Cat {
        async fn map_stream(
            &self,
            input: mapstream::MapStreamRequest,
            tx: Sender<mapstream::Message>,
        ) {
            let payload_str = String::from_utf8(input.value).unwrap_or_default();
            let splits: Vec<&str> = payload_str.split(',').collect();

            for split in splits {
                let message = mapstream::Message::new(split.as_bytes().to_vec())
                    .with_keys(input.keys.clone())
                    .with_tags(vec![]);
                if tx.send(message).await.is_err() {
                    break;
                }
            }
        }
    }

    #[tokio::test]
    async fn test_map_operation() {
        // create the source which produces x number of messages
        let cln_token = CancellationToken::new();
        let tracker = Tracker::new(None, cln_token.clone());

        // Create source
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

        let client = SourceClient::new(create_rpc_channel(sock_file).await.unwrap());

        let (src_read, src_ack, lag_reader) = new_source(
            client,
            5,
            Duration::from_millis(1000),
            cln_token.clone(),
            true,
        )
        .await
        .map_err(|e| panic!("failed to create source reader: {:?}", e))
        .unwrap();

        let source: Source<crate::typ::WithoutRateLimiter> = Source::new(
            5,
            SourceType::UserDefinedSource(Box::new(src_read), Box::new(src_ack), lag_reader),
            tracker.clone(),
            true,
            None,
            None,
            None,
        )
        .await;

        // create a mapper
        let (mp_shutdown_tx, mp_shutdown_rx) = oneshot::channel();
        let tmp_dir = TempDir::new().unwrap();
        let sock_file = tmp_dir.path().join("mapper.sock");
        let server_info_file = tmp_dir.path().join("mapper-server-info");

        let server_info = server_info_file.clone();
        let server_socket = sock_file.clone();
        let map_handle = tokio::spawn(async move {
            map::Server::new(Cat)
                .with_socket_file(server_socket)
                .with_server_info_file(server_info)
                .start_with_shutdown(mp_shutdown_rx)
                .await
                .expect("server failed");
        });

        let client = MapClient::new(create_rpc_channel(sock_file).await.unwrap());
        let mapper = MapHandle::new(
            MapMode::Unary,
            10,
            Duration::from_secs(10),
            Duration::from_secs(10),
            10,
            client,
            tracker.clone(),
        )
        .await
        .unwrap();

        let sink_writer =
            SinkWriterBuilder::new(10, Duration::from_millis(100), SinkClientType::Log)
                .build()
                .await
                .unwrap();

        // create the forwarder with the source, transformer, and writer
        let forwarder = Forwarder::new(source.clone(), Some(mapper), sink_writer);

        let cancel_token = cln_token.clone();
        let forwarder_handle: JoinHandle<Result<()>> =
            tokio::spawn(async move { forwarder.start(cancel_token).await });

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
        mp_shutdown_tx.send(()).unwrap();
        src_shutdown_tx.send(()).unwrap();
        source_handle.await.unwrap();
        map_handle.await.unwrap();
    }

    #[tokio::test]
    async fn test_batch_map_operation() {
        // create the source which produces x number of messages
        let cln_token = CancellationToken::new();
        let tracker = Tracker::new(None, cln_token.clone());

        // Create source
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

        let client = SourceClient::new(create_rpc_channel(sock_file).await.unwrap());

        let (src_read, src_ack, lag_reader) = new_source(
            client,
            5,
            Duration::from_millis(1000),
            cln_token.clone(),
            true,
        )
        .await
        .map_err(|e| panic!("failed to create source reader: {:?}", e))
        .unwrap();

        let source: Source<crate::typ::WithoutRateLimiter> = Source::new(
            5,
            SourceType::UserDefinedSource(Box::new(src_read), Box::new(src_ack), lag_reader),
            tracker.clone(),
            true,
            None,
            None,
            None,
        )
        .await;

        // create a mapper
        let (bmp_shutdown_tx, bmp_shutdown_rx) = oneshot::channel();
        let tmp_dir = TempDir::new().unwrap();
        let sock_file = tmp_dir.path().join("batch_map.sock");
        let server_info_file = tmp_dir.path().join("batch_map-server-info");

        let server_info = server_info_file.clone();
        let server_socket = sock_file.clone();
        let map_handle = tokio::spawn(async move {
            batchmap::Server::new(Cat)
                .with_socket_file(server_socket)
                .with_server_info_file(server_info)
                .start_with_shutdown(bmp_shutdown_rx)
                .await
                .expect("server failed");
        });

        let client = MapClient::new(create_rpc_channel(sock_file).await.unwrap());
        let mapper = MapHandle::new(
            MapMode::Batch,
            10,
            Duration::from_secs(10),
            Duration::from_secs(10),
            10,
            client,
            tracker.clone(),
        )
        .await
        .unwrap();

        let sink_writer =
            SinkWriterBuilder::new(10, Duration::from_millis(100), SinkClientType::Log)
                .build()
                .await
                .unwrap();

        // create the forwarder with the source, transformer, and writer
        let forwarder = Forwarder::new(source.clone(), Some(mapper), sink_writer);

        let cancel_token = cln_token.clone();
        let forwarder_handle: JoinHandle<Result<()>> =
            tokio::spawn(async move { forwarder.start(cancel_token).await });

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
        bmp_shutdown_tx.send(()).unwrap();
        src_shutdown_tx.send(()).unwrap();
        source_handle.await.unwrap();
        map_handle.await.unwrap();
    }

    #[tokio::test]
    async fn test_flatmap_stream_operation() {
        // create the source which produces x number of messages
        let cln_token = CancellationToken::new();
        let tracker = Tracker::new(None, cln_token.clone());

        // Create source
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

        let client = SourceClient::new(create_rpc_channel(sock_file).await.unwrap());

        let (src_read, src_ack, lag_reader) = new_source(
            client,
            5,
            Duration::from_millis(1000),
            cln_token.clone(),
            true,
        )
        .await
        .map_err(|e| panic!("failed to create source reader: {:?}", e))
        .unwrap();

        let source: Source<crate::typ::WithoutRateLimiter> = Source::new(
            5,
            SourceType::UserDefinedSource(Box::new(src_read), Box::new(src_ack), lag_reader),
            tracker.clone(),
            true,
            None,
            None,
            None,
        )
        .await;

        // create a mapper
        let (fms_shutdown_tx, fms_shutdown_rx) = oneshot::channel();
        let tmp_dir = TempDir::new().unwrap();
        let sock_file = tmp_dir.path().join("flatmap_stream.sock");
        let server_info_file = tmp_dir.path().join("flatmap_stream-server-info");

        let server_info = server_info_file.clone();
        let server_socket = sock_file.clone();
        let map_handle = tokio::spawn(async move {
            mapstream::Server::new(Cat)
                .with_socket_file(server_socket)
                .with_server_info_file(server_info)
                .start_with_shutdown(fms_shutdown_rx)
                .await
                .expect("server failed");
        });

        let client = MapClient::new(create_rpc_channel(sock_file).await.unwrap());
        let mapper = MapHandle::new(
            MapMode::Stream,
            10,
            Duration::from_secs(10),
            Duration::from_secs(10),
            10,
            client,
            tracker.clone(),
        )
        .await
        .unwrap();

        let sink_writer =
            SinkWriterBuilder::new(10, Duration::from_millis(100), SinkClientType::Log)
                .build()
                .await
                .unwrap();

        // create the forwarder with the source, transformer, and writer
        let forwarder = Forwarder::new(source.clone(), Some(mapper), sink_writer);

        let cancel_token = cln_token.clone();
        let forwarder_handle: JoinHandle<Result<()>> =
            tokio::spawn(async move { forwarder.start(cancel_token).await });

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
        fms_shutdown_tx.send(()).unwrap();
        src_shutdown_tx.send(()).unwrap();
        source_handle.await.unwrap();
        map_handle.await.unwrap();
    }

    #[tokio::test]
    async fn test_source_transformer_map_operation() {
        // create the source which produces x number of messages
        let cln_token = CancellationToken::new();
        let tracker_handle = Tracker::new(None, cln_token.clone());

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

        let client = SourceTransformClient::new(create_rpc_channel(sock_file).await.unwrap());
        let transformer = Transformer::new(
            10,
            10,
            Duration::from_secs(10),
            client,
            tracker_handle.clone(),
        )
        .await
        .unwrap();

        let (src_shutdown_tx, src_shutdown_rx) = oneshot::channel();
        let tmp_dir = TempDir::new().unwrap();
        let sock_file = tmp_dir.path().join("source.sock");
        let server_info_file = tmp_dir.path().join("source-server-info");

        let server_info = server_info_file.clone();
        let server_socket = sock_file.clone();
        let source_handle = tokio::spawn(async move {
            // a simple source which generates total of 100 messages
            source::Server::new(SimpleSource::new(10))
                .with_socket_file(server_socket)
                .with_server_info_file(server_info)
                .start_with_shutdown(src_shutdown_rx)
                .await
                .unwrap()
        });

        let client = SourceClient::new(create_rpc_channel(sock_file).await.unwrap());

        let (src_read, src_ack, lag_reader) = new_source(
            client,
            5,
            Duration::from_millis(1000),
            cln_token.clone(),
            true,
        )
        .await
        .map_err(|e| panic!("failed to create source reader: {:?}", e))
        .unwrap();

        let source: Source<crate::typ::WithoutRateLimiter> = Source::new(
            5,
            SourceType::UserDefinedSource(Box::new(src_read), Box::new(src_ack), lag_reader),
            tracker_handle.clone(),
            true,
            Some(transformer),
            None,
            None,
        )
        .await;

        // create a mapper
        let (mp_shutdown_tx, mp_shutdown_rx) = oneshot::channel();
        let tmp_dir = TempDir::new().unwrap();
        let sock_file = tmp_dir.path().join("mapper.sock");
        let server_info_file = tmp_dir.path().join("mapper-server-info");

        let server_info = server_info_file.clone();
        let server_socket = sock_file.clone();
        let map_handle = tokio::spawn(async move {
            map::Server::new(Cat)
                .with_socket_file(server_socket)
                .with_server_info_file(server_info)
                .start_with_shutdown(mp_shutdown_rx)
                .await
                .expect("server failed");
        });

        let client = MapClient::new(create_rpc_channel(sock_file).await.unwrap());
        let mapper = MapHandle::new(
            MapMode::Unary,
            10,
            Duration::from_secs(10),
            Duration::from_secs(10),
            10,
            client,
            tracker_handle.clone(),
        )
        .await
        .unwrap();

        let sink_writer =
            SinkWriterBuilder::new(10, Duration::from_millis(100), SinkClientType::Log)
                .build()
                .await
                .unwrap();

        // create the forwarder with the source, transformer, and writer
        let forwarder = Forwarder::new(source.clone(), Some(mapper), sink_writer);

        let cancel_token = cln_token.clone();
        let forwarder_handle: JoinHandle<Result<()>> =
            tokio::spawn(async move { forwarder.start(cancel_token).await });

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
        mp_shutdown_tx.send(()).unwrap();
        st_shutdown_tx.send(()).unwrap();
        src_shutdown_tx.send(()).unwrap();
        source_handle.await.unwrap();
        map_handle.await.unwrap();
        transformer_handle.await.unwrap();
    }
}
