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
//! Forwarder with Bypass Router:
//! Forwarder allows initializing a bypass router to directly route/send messages from
//! Source Transformer / UDF to one of the Sinks based on tags.
//! The bypass router is initialized in the forwarder and the bypass router receiver join_handle is
//! awaited with other component handles (reader, mapper, sink writer).
//!
//! [MonoVertex]: https://numaflow.numaproj.io/core-concepts/monovertex/
//! [Stream]: https://docs.rs/tokio-stream/latest/tokio_stream/wrappers/struct.ReceiverStream.html
//! [Actor Pattern]: https://ryhl.io/blog/actors-with-tokio/

use crate::Error;
use crate::error;
use crate::mapper::map::MapHandle;
use crate::monovertex::bypass_router::{BypassRouterConfig, MvtxBypassRouter};
use crate::sinker::sink::SinkWriter;
use crate::source::Source;
use tokio_util::sync::CancellationToken;

/// Forwarder is responsible for reading messages from the source, applying transformation if
/// transformer is present, writing the messages to the sink, and then acknowledging the messages
/// back to the source.
pub(crate) struct Forwarder<C: crate::typ::NumaflowTypeConfig> {
    source: Source<C>,
    mapper: Option<MapHandle>,
    sink_writer: SinkWriter,
    bypass_router_config: Option<BypassRouterConfig>,
}

impl<C: crate::typ::NumaflowTypeConfig> Forwarder<C> {
    pub(crate) fn new(
        source: Source<C>,
        mapper: Option<MapHandle>,
        sink_writer: SinkWriter,
        bypass_router_config: Option<BypassRouterConfig>,
    ) -> Self {
        Self {
            source,
            mapper,
            sink_writer,
            bypass_router_config,
        }
    }

    pub(crate) async fn start(self, cln_token: CancellationToken) -> crate::Result<()> {
        let (bypass_router, router_handle) = match self.bypass_router_config {
            Some(bypass_router_config) => {
                let (router, handle) = MvtxBypassRouter::initialize(
                    bypass_router_config,
                    self.sink_writer.clone(),
                    cln_token.clone(),
                )
                .await;
                (Some(router), handle?)
            }
            None => (None, tokio::task::spawn(async { Ok(()) })),
        };

        let (read_messages_stream, reader_handle) = self
            .source
            .streaming_read(cln_token.clone(), bypass_router.clone())?;

        let (mapper_stream, mapper_handle) = match self.mapper {
            Some(mapper) => {
                mapper
                    // Performs respective map operation (unary, batch, stream) based on actor_sender
                    .streaming_map(read_messages_stream, cln_token.clone(), bypass_router)
                    .await?
            }
            None => (
                read_messages_stream,
                tokio::task::spawn(async move {
                    drop(bypass_router);
                    Ok(())
                }),
            ),
        };

        let sink_writer_handle = self
            .sink_writer
            .streaming_write(mapper_stream, cln_token.clone())
            .await?;

        // Join the reader and sink writer
        let (reader_result, mapper_handle_result, sink_writer_result, bypass_result) =
            tokio::try_join!(
                reader_handle,
                mapper_handle,
                sink_writer_handle,
                router_handle
            )
            .map_err(|e| {
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

        bypass_result.inspect_err(|e| {
            error!(?e, "Error in bypass router receiver background task");
        })?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::Result;
    use crate::config::monovertex::BypassConditions;
    use crate::monovertex::bypass_router::BypassRouterConfig;
    use crate::monovertex::forwarder::Forwarder;
    use crate::shared::test_utils::components::{
        MapperTestHandle, SinkTestHandle, SourceTestHandle,
    };
    use crate::shared::test_utils::components::{NoOpSink, NoOpTransformer, SinkType};
    use crate::shared::test_utils::components::{
        create_batch_mapper, create_map_streamer, create_mapper, create_sink, create_ud_source,
    };
    use crate::sinker::sink::SinkClientType;
    use crate::tracker::Tracker;
    use chrono::Utc;
    use numaflow::sink::{Response, SinkRequest};
    use numaflow::source::{Message, Offset, SourceReadRequest};
    use numaflow::{batchmap, map, mapstream, sink, source, sourcetransform};
    use numaflow_models::models::{ForwardConditions, TagConditions};
    use numaflow_shared::server_info::MapMode;
    use std::collections::HashSet;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;
    use tokio::sync::mpsc::Sender;
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
        // create the source which produces x number of messages
        let cln_token = CancellationToken::new();
        let tracker = Tracker::new(None, cln_token.clone());
        let batch_size = 10;

        // Create the source
        let source_handle = create_ud_source(
            SimpleSource::new(100),
            Some(SimpleTransformer),
            batch_size,
            cln_token.clone(),
            tracker.clone(),
        )
        .await;

        let sink_handle = create_sink(
            SinkType::<NoOpSink>::BuiltIn(SinkClientType::Log),
            None,
            None,
            batch_size,
        )
        .await;

        start_forwarder_test(source_handle, None, sink_handle, None, cln_token).await;
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
        let batch_size = 10;

        // Create the source
        let source_handle = create_ud_source(
            SimpleSource::new(100),
            Some(FlatMapTransformer),
            batch_size,
            cln_token.clone(),
            tracker.clone(),
        )
        .await;

        let sink_handle = create_sink(
            SinkType::<NoOpSink>::BuiltIn(SinkClientType::Log),
            None,
            None,
            batch_size,
        )
        .await;

        start_forwarder_test(source_handle, None, sink_handle, None, cln_token).await;
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
        let batch_size = 10;

        // Create the source
        let source_handle = create_ud_source(
            SimpleSource::new(100),
            Option::<NoOpTransformer>::None,
            batch_size,
            cln_token.clone(),
            tracker.clone(),
        )
        .await;

        // create a mapper
        let mapper_handle = create_mapper(
            Cat,
            tracker,
            MapMode::Unary,
            batch_size,
            Duration::from_secs(10),
            Duration::from_secs(10),
            10,
        )
        .await;

        let sink_handle = create_sink(
            SinkType::<NoOpSink>::BuiltIn(SinkClientType::Log),
            None,
            None,
            batch_size,
        )
        .await;

        start_forwarder_test(
            source_handle,
            Some(mapper_handle),
            sink_handle,
            None,
            cln_token,
        )
        .await;
    }

    #[tokio::test]
    async fn test_batch_map_operation() {
        // create the source which produces x number of messages
        let cln_token = CancellationToken::new();
        let tracker = Tracker::new(None, cln_token.clone());
        let batch_size = 10;

        // Create the source
        let source_handle = create_ud_source(
            SimpleSource::new(100),
            Option::<NoOpTransformer>::None,
            batch_size,
            cln_token.clone(),
            tracker.clone(),
        )
        .await;

        // create a mapper
        let mapper_handle = create_batch_mapper(
            Cat,
            tracker,
            MapMode::Batch,
            batch_size,
            Duration::from_secs(10),
            Duration::from_secs(10),
            10,
        )
        .await;

        let sink_handle = create_sink(
            SinkType::<NoOpSink>::BuiltIn(SinkClientType::Log),
            None,
            None,
            batch_size,
        )
        .await;

        start_forwarder_test(
            source_handle,
            Some(mapper_handle),
            sink_handle,
            None,
            cln_token,
        )
        .await;
    }

    #[tokio::test]
    async fn test_flatmap_stream_operation() {
        // create the source which produces x number of messages
        let cln_token = CancellationToken::new();
        let tracker = Tracker::new(None, cln_token.clone());
        let batch_size = 10;

        // Create the source
        let source_handle = create_ud_source(
            SimpleSource::new(100),
            Option::<NoOpTransformer>::None,
            batch_size,
            cln_token.clone(),
            tracker.clone(),
        )
        .await;

        // create a mapper
        let mapper_handle = create_map_streamer(
            Cat,
            tracker,
            MapMode::Stream,
            batch_size,
            Duration::from_secs(10),
            Duration::from_secs(10),
            10,
        )
        .await;

        let sink_handle = create_sink(
            SinkType::<NoOpSink>::BuiltIn(SinkClientType::Log),
            None,
            None,
            batch_size,
        )
        .await;

        start_forwarder_test(
            source_handle,
            Some(mapper_handle),
            sink_handle,
            None,
            cln_token,
        )
        .await;
    }

    #[tokio::test]
    async fn test_source_transformer_map_operation() {
        // create the source which produces x number of messages
        let cln_token = CancellationToken::new();
        let tracker = Tracker::new(None, cln_token.clone());
        let batch_size = 10;

        // Create the source
        let source_handle = create_ud_source(
            SimpleSource::new(100),
            Some(SimpleTransformer),
            batch_size,
            cln_token.clone(),
            tracker.clone(),
        )
        .await;

        // create a mapper
        let mapper_handle = create_mapper(
            Cat,
            tracker,
            MapMode::Unary,
            batch_size,
            Duration::from_secs(10),
            Duration::from_secs(10),
            10,
        )
        .await;

        let sink_handle = create_sink(
            SinkType::<NoOpSink>::BuiltIn(SinkClientType::Log),
            None,
            None,
            batch_size,
        )
        .await;

        start_forwarder_test(
            source_handle,
            Some(mapper_handle),
            sink_handle,
            None,
            cln_token,
        )
        .await;
    }

    struct ConditionalTransformer {
        sink_max_count: usize,
        fallback_max_count: usize,
        on_success_max_count: usize,
        sink_count: AtomicUsize,
        fallback_count: AtomicUsize,
        on_success_count: AtomicUsize,
        sink_tags: Option<Vec<String>>,
        fallback_tags: Option<Vec<String>>,
        on_success_tags: Option<Vec<String>>,
    }

    impl ConditionalTransformer {
        pub(crate) fn new(
            sink_count: usize,
            fallback_count: usize,
            on_success_count: usize,
            sink_tags: Option<Vec<String>>,
            fallback_tags: Option<Vec<String>>,
            on_success_tags: Option<Vec<String>>,
        ) -> Self {
            Self {
                sink_max_count: sink_count,
                fallback_max_count: fallback_count,
                on_success_max_count: on_success_count,
                sink_count: AtomicUsize::new(0),
                fallback_count: AtomicUsize::new(0),
                on_success_count: AtomicUsize::new(0),
                sink_tags,
                fallback_tags,
                on_success_tags,
            }
        }
    }

    #[tonic::async_trait]
    impl sourcetransform::SourceTransformer for ConditionalTransformer {
        async fn transform(
            &self,
            input: sourcetransform::SourceTransformRequest,
        ) -> Vec<sourcetransform::Message> {
            let message =
                sourcetransform::Message::new(input.value, Utc::now()).with_keys(input.keys);

            let message = if self.sink_count.load(Ordering::SeqCst) < self.sink_max_count {
                self.sink_count.fetch_add(1, Ordering::SeqCst);
                message.with_tags(
                    self.sink_tags
                        .clone()
                        .expect("sink_tags is None when sink_max_count > 0"),
                )
            } else if self.fallback_count.load(Ordering::SeqCst) < self.fallback_max_count {
                self.fallback_count.fetch_add(1, Ordering::SeqCst);
                message.with_tags(
                    self.fallback_tags
                        .clone()
                        .expect("fallback_tags is None when fallback_max_count > 0"),
                )
            } else if self.on_success_count.load(Ordering::SeqCst) < self.on_success_max_count {
                self.on_success_count.fetch_add(1, Ordering::SeqCst);
                message.with_tags(
                    self.on_success_tags
                        .clone()
                        .expect("on_success_tags is None when on_success_max_count > 0"),
                )
            } else {
                message
            };

            vec![message]
        }
    }

    struct SinkLog {
        messages_received: AtomicUsize,
    }

    impl SinkLog {
        fn new() -> Self {
            Self {
                messages_received: AtomicUsize::new(0),
            }
        }
    }

    #[tonic::async_trait]
    impl sink::Sinker for SinkLog {
        async fn sink(&self, mut input: tokio::sync::mpsc::Receiver<SinkRequest>) -> Vec<Response> {
            let mut responses: Vec<Response> = Vec::new();

            while let Some(datum) = input.recv().await {
                // do something better, but for now let's just log it.
                // please note that `from_utf8` is working because the input in this
                // example uses utf-8 data.
                let response = match std::str::from_utf8(&datum.value) {
                    Ok(_) => {
                        self.messages_received.fetch_add(1, Ordering::SeqCst);
                        println!(
                            "Message Count: {}",
                            self.messages_received.load(Ordering::SeqCst)
                        );
                        // record the response
                        Response::ok(datum.id)
                    }
                    Err(e) => Response::failure(datum.id, format!("Invalid UTF-8 sequence: {}", e)),
                };

                // return the responses
                responses.push(response);
            }

            responses
        }
    }

    #[tokio::test]
    async fn test_source_transformer_with_bypass() {
        let tracker = Tracker::new(None, CancellationToken::new());

        // create the source which produces x number of messages
        let cln_token = CancellationToken::new();

        // create the bypass router config to pass to the forwarder
        let batch_size: usize = 10;
        let fallback_tags = vec!["fallback".to_string()];
        let on_success_tags = vec!["on_success".to_string()];
        let conditions = BypassConditions {
            sink: None,
            fallback: Some(Box::new(ForwardConditions::new(TagConditions {
                values: fallback_tags.clone(),
                operator: Some("or".to_string()),
            }))),
            on_success: Some(Box::new(ForwardConditions::new(TagConditions {
                values: on_success_tags.clone(),
                operator: Some("or".to_string()),
            }))),
        };
        let bypass_router_config =
            BypassRouterConfig::new(conditions, batch_size, Duration::from_millis(1000));

        // Create the source
        let source_handle = create_ud_source(
            SimpleSource::new(100),
            Some(ConditionalTransformer::new(
                0,
                10,
                10,
                None,
                Some(fallback_tags),
                Some(on_success_tags),
            )),
            batch_size,
            cln_token.clone(),
            tracker.clone(),
        )
        .await;

        let sink_handle = create_sink(
            SinkType::UserDefined(SinkLog::new()),
            Some(SinkType::BuiltIn(SinkClientType::Log)),
            Some(SinkType::BuiltIn(SinkClientType::Log)),
            batch_size,
        )
        .await;

        start_forwarder_test(
            source_handle,
            None,
            sink_handle,
            Some(bypass_router_config),
            cln_token,
        )
        .await;
    }

    struct BypassCat {
        sink_max_count: usize,
        fallback_max_count: usize,
        on_success_max_count: usize,
        sink_count: AtomicUsize,
        fallback_count: AtomicUsize,
        on_success_count: AtomicUsize,
        sink_tags: Option<Vec<String>>,
        fallback_tags: Option<Vec<String>>,
        on_success_tags: Option<Vec<String>>,
    }

    impl BypassCat {
        pub(crate) fn new(
            sink_count: usize,
            fallback_count: usize,
            on_success_count: usize,
            sink_tags: Option<Vec<String>>,
            fallback_tags: Option<Vec<String>>,
            on_success_tags: Option<Vec<String>>,
        ) -> Self {
            Self {
                sink_max_count: sink_count,
                fallback_max_count: fallback_count,
                on_success_max_count: on_success_count,
                sink_count: AtomicUsize::new(0),
                fallback_count: AtomicUsize::new(0),
                on_success_count: AtomicUsize::new(0),
                sink_tags,
                fallback_tags,
                on_success_tags,
            }
        }
    }

    #[tonic::async_trait]
    impl map::Mapper for BypassCat {
        async fn map(&self, input: map::MapRequest) -> Vec<map::Message> {
            let message = map::Message::new(input.value).with_keys(input.keys);

            let message = if self.sink_count.load(Ordering::SeqCst) < self.sink_max_count {
                self.sink_count.fetch_add(1, Ordering::SeqCst);
                message.with_tags(
                    self.sink_tags
                        .clone()
                        .expect("sink_tags is None when sink_max_count > 0"),
                )
            } else if self.fallback_count.load(Ordering::SeqCst) < self.fallback_max_count {
                self.fallback_count.fetch_add(1, Ordering::SeqCst);
                message.with_tags(
                    self.fallback_tags
                        .clone()
                        .expect("fallback_tags is None when fallback_max_count > 0"),
                )
            } else if self.on_success_count.load(Ordering::SeqCst) < self.on_success_max_count {
                self.on_success_count.fetch_add(1, Ordering::SeqCst);
                message.with_tags(
                    self.on_success_tags
                        .clone()
                        .expect("on_success_tags is None when on_success_max_count > 0"),
                )
            } else {
                message
            };

            vec![message]
        }
    }

    #[tonic::async_trait]
    impl batchmap::BatchMapper for BypassCat {
        async fn batchmap(
            &self,
            mut input: tokio::sync::mpsc::Receiver<batchmap::Datum>,
        ) -> Vec<batchmap::BatchResponse> {
            let mut responses: Vec<batchmap::BatchResponse> = Vec::new();
            while let Some(datum) = input.recv().await {
                let mut response = batchmap::BatchResponse::from_id(datum.id);
                let _some_val: Option<bool> = None;

                let message = batchmap::Message::new(datum.value).with_keys(datum.keys);

                let message = if self.sink_count.load(Ordering::SeqCst) < self.sink_max_count {
                    self.sink_count.fetch_add(1, Ordering::SeqCst);
                    message.with_tags(
                        self.sink_tags
                            .clone()
                            .expect("sink_tags is None when sink_max_count > 0"),
                    )
                } else if self.fallback_count.load(Ordering::SeqCst) < self.fallback_max_count {
                    self.fallback_count.fetch_add(1, Ordering::SeqCst);
                    message.with_tags(
                        self.fallback_tags
                            .clone()
                            .expect("fallback_tags is None when fallback_max_count > 0"),
                    )
                } else if self.on_success_count.load(Ordering::SeqCst) < self.on_success_max_count {
                    self.on_success_count.fetch_add(1, Ordering::SeqCst);
                    message.with_tags(
                        self.on_success_tags
                            .clone()
                            .expect("on_success_tags is None when on_success_max_count > 0"),
                    )
                } else {
                    message
                };

                response.append(message);

                responses.push(response);
            }
            responses
        }
    }

    #[tonic::async_trait]
    impl mapstream::MapStreamer for BypassCat {
        async fn map_stream(
            &self,
            input: mapstream::MapStreamRequest,
            tx: Sender<mapstream::Message>,
        ) {
            let message = mapstream::Message::new(input.value).with_keys(input.keys);

            let message = if self.sink_count.load(Ordering::SeqCst) < self.sink_max_count {
                self.sink_count.fetch_add(1, Ordering::SeqCst);
                message.with_tags(
                    self.sink_tags
                        .clone()
                        .expect("sink_tags is None when sink_max_count > 0"),
                )
            } else if self.fallback_count.load(Ordering::SeqCst) < self.fallback_max_count {
                self.fallback_count.fetch_add(1, Ordering::SeqCst);
                message.with_tags(
                    self.fallback_tags
                        .clone()
                        .expect("fallback_tags is None when fallback_max_count > 0"),
                )
            } else if self.on_success_count.load(Ordering::SeqCst) < self.on_success_max_count {
                self.on_success_count.fetch_add(1, Ordering::SeqCst);
                message.with_tags(
                    self.on_success_tags
                        .clone()
                        .expect("on_success_tags is None when on_success_max_count > 0"),
                )
            } else {
                message
            };

            let _ = tx.send(message).await;
        }
    }

    #[tokio::test]
    async fn test_source_map_with_bypass() {
        let tracker = Tracker::new(None, CancellationToken::new());

        // create the source which produces x number of messages
        let cln_token = CancellationToken::new();

        // create the bypass router config to pass to the forwarder
        let batch_size: usize = 10;
        let fallback_tags = vec!["fallback".to_string()];
        let on_success_tags = vec!["on_success".to_string()];
        let conditions = BypassConditions {
            sink: None,
            fallback: Some(Box::new(ForwardConditions::new(TagConditions {
                values: fallback_tags.clone(),
                operator: Some("or".to_string()),
            }))),
            on_success: Some(Box::new(ForwardConditions::new(TagConditions {
                values: on_success_tags.clone(),
                operator: Some("or".to_string()),
            }))),
        };
        let bypass_router_config =
            BypassRouterConfig::new(conditions, batch_size, Duration::from_millis(1000));

        // Create the source
        let source_handle = create_ud_source(
            SimpleSource::new(100),
            Option::<NoOpTransformer>::None,
            batch_size,
            cln_token.clone(),
            tracker.clone(),
        )
        .await;

        // create a mapper
        let mapper_handle = create_mapper(
            BypassCat::new(0, 10, 10, None, Some(fallback_tags), Some(on_success_tags)),
            tracker,
            MapMode::Unary,
            batch_size,
            Duration::from_secs(10),
            Duration::from_secs(10),
            10,
        )
        .await;

        let sink_handle = create_sink(
            SinkType::UserDefined(SinkLog::new()),
            Some(SinkType::BuiltIn(SinkClientType::Log)),
            Some(SinkType::BuiltIn(SinkClientType::Log)),
            batch_size,
        )
        .await;

        start_forwarder_test(
            source_handle,
            Some(mapper_handle),
            sink_handle,
            Some(bypass_router_config),
            cln_token,
        )
        .await;
    }

    #[tokio::test]
    async fn test_source_batch_map_with_bypass() {
        let tracker = Tracker::new(None, CancellationToken::new());

        // create the source which produces x number of messages
        let cln_token = CancellationToken::new();

        // create the bypass router config to pass to the forwarder
        let batch_size: usize = 10;
        let fallback_tags = vec!["fallback".to_string()];
        let on_success_tags = vec!["on_success".to_string()];
        let conditions = BypassConditions {
            sink: None,
            fallback: Some(Box::new(ForwardConditions::new(TagConditions {
                values: fallback_tags.clone(),
                operator: Some("or".to_string()),
            }))),
            on_success: Some(Box::new(ForwardConditions::new(TagConditions {
                values: on_success_tags.clone(),
                operator: Some("or".to_string()),
            }))),
        };
        let bypass_router_config =
            BypassRouterConfig::new(conditions, batch_size, Duration::from_millis(1000));

        // Create the source
        let source_handle = create_ud_source(
            SimpleSource::new(100),
            Option::<NoOpTransformer>::None,
            batch_size,
            cln_token.clone(),
            tracker.clone(),
        )
        .await;

        // create a mapper
        let mapper_handle = create_batch_mapper(
            BypassCat::new(0, 10, 10, None, Some(fallback_tags), Some(on_success_tags)),
            tracker,
            MapMode::Batch,
            batch_size,
            Duration::from_secs(10),
            Duration::from_secs(10),
            10,
        )
        .await;

        let sink_handle = create_sink(
            SinkType::UserDefined(SinkLog::new()),
            Some(SinkType::BuiltIn(SinkClientType::Log)),
            Some(SinkType::BuiltIn(SinkClientType::Log)),
            batch_size,
        )
        .await;

        start_forwarder_test(
            source_handle,
            Some(mapper_handle),
            sink_handle,
            Some(bypass_router_config),
            cln_token,
        )
        .await;
    }

    #[tokio::test]
    async fn test_source_map_stream_with_bypass() {
        let tracker = Tracker::new(None, CancellationToken::new());

        // create the source which produces x number of messages
        let cln_token = CancellationToken::new();

        // create the bypass router config to pass to the forwarder
        let batch_size: usize = 10;
        let fallback_tags = vec!["fallback".to_string()];
        let on_success_tags = vec!["on_success".to_string()];
        let conditions = BypassConditions {
            sink: None,
            fallback: Some(Box::new(ForwardConditions::new(TagConditions {
                values: fallback_tags.clone(),
                operator: Some("or".to_string()),
            }))),
            on_success: Some(Box::new(ForwardConditions::new(TagConditions {
                values: on_success_tags.clone(),
                operator: Some("or".to_string()),
            }))),
        };
        let bypass_router_config =
            BypassRouterConfig::new(conditions, batch_size, Duration::from_millis(1000));

        // Create the source
        let source_handle = create_ud_source(
            SimpleSource::new(100),
            Option::<NoOpTransformer>::None,
            batch_size,
            cln_token.clone(),
            tracker.clone(),
        )
        .await;

        // create a mapper
        let mapper_handle = create_map_streamer(
            BypassCat::new(0, 10, 10, None, Some(fallback_tags), Some(on_success_tags)),
            tracker,
            MapMode::Stream,
            batch_size,
            Duration::from_secs(10),
            Duration::from_secs(10),
            10,
        )
        .await;

        let sink_handle = create_sink(
            SinkType::UserDefined(SinkLog::new()),
            Some(SinkType::BuiltIn(SinkClientType::Log)),
            Some(SinkType::BuiltIn(SinkClientType::Log)),
            batch_size,
        )
        .await;

        start_forwarder_test(
            source_handle,
            Some(mapper_handle),
            sink_handle,
            Some(bypass_router_config),
            cln_token,
        )
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    #[should_panic]
    /// The bypass conditions are configured for fallback and on success scenarios but
    /// the sink doesn't have any fallback or on success sinks configured.
    /// The test fails because of the timeout in the forwarder.
    async fn test_source_map_with_bypass_fails() {
        let tracker = Tracker::new(None, CancellationToken::new());

        // create the source which produces x number of messages
        let cln_token = CancellationToken::new();

        // create the bypass router config to pass to the forwarder
        let batch_size: usize = 10;
        let fallback_tags = vec!["fallback".to_string()];
        let on_success_tags = vec!["on_success".to_string()];
        let conditions = BypassConditions {
            sink: None,
            fallback: Some(Box::new(ForwardConditions::new(TagConditions {
                values: fallback_tags.clone(),
                operator: Some("or".to_string()),
            }))),
            on_success: Some(Box::new(ForwardConditions::new(TagConditions {
                values: on_success_tags.clone(),
                operator: Some("or".to_string()),
            }))),
        };
        let bypass_router_config =
            BypassRouterConfig::new(conditions, batch_size, Duration::from_millis(1000));

        // Create the source
        let source_handle = create_ud_source(
            SimpleSource::new(100),
            Option::<NoOpTransformer>::None,
            batch_size,
            cln_token.clone(),
            tracker.clone(),
        )
        .await;

        // create a mapper
        let mapper_handle = create_mapper(
            BypassCat::new(0, 1, 1, None, Some(fallback_tags), Some(on_success_tags)),
            tracker,
            MapMode::Unary,
            batch_size,
            Duration::from_secs(10),
            Duration::from_secs(10),
            10,
        )
        .await;

        let sink_handle = create_sink(
            SinkType::UserDefined(SinkLog::new()),
            None,
            None,
            batch_size,
        )
        .await;

        start_forwarder_test(
            source_handle,
            Some(mapper_handle),
            sink_handle,
            Some(bypass_router_config),
            cln_token,
        )
        .await;
    }

    struct PanickingConditionalTransformer {
        sink_max_count: usize,
        fallback_max_count: usize,
        on_success_max_count: usize,
        sink_count: AtomicUsize,
        fallback_count: AtomicUsize,
        on_success_count: AtomicUsize,
        sink_tags: Option<Vec<String>>,
        fallback_tags: Option<Vec<String>>,
    }

    impl PanickingConditionalTransformer {
        pub(crate) fn new(
            sink_count: usize,
            fallback_count: usize,
            on_success_count: usize,
            sink_tags: Option<Vec<String>>,
            fallback_tags: Option<Vec<String>>,
        ) -> Self {
            Self {
                sink_max_count: sink_count,
                fallback_max_count: fallback_count,
                on_success_max_count: on_success_count,
                sink_count: AtomicUsize::new(0),
                fallback_count: AtomicUsize::new(0),
                on_success_count: AtomicUsize::new(0),
                sink_tags,
                fallback_tags,
            }
        }
    }

    #[tonic::async_trait]
    impl sourcetransform::SourceTransformer for PanickingConditionalTransformer {
        async fn transform(
            &self,
            input: sourcetransform::SourceTransformRequest,
        ) -> Vec<sourcetransform::Message> {
            let message =
                sourcetransform::Message::new(input.value, Utc::now()).with_keys(input.keys);

            let message = if self.sink_count.load(Ordering::SeqCst) < self.sink_max_count {
                self.sink_count.fetch_add(1, Ordering::SeqCst);
                message.with_tags(
                    self.sink_tags
                        .clone()
                        .expect("sink_tags is None when sink_max_count > 0"),
                )
            } else if self.fallback_count.load(Ordering::SeqCst) < self.fallback_max_count {
                self.fallback_count.fetch_add(1, Ordering::SeqCst);
                message.with_tags(
                    self.fallback_tags
                        .clone()
                        .expect("fallback_tags is None when fallback_max_count > 0"),
                )
            } else if self.on_success_count.load(Ordering::SeqCst) < self.on_success_max_count {
                self.on_success_count.fetch_add(1, Ordering::SeqCst);
                panic!("on_success_count reached max count");
            } else {
                message
            };

            vec![message]
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    #[should_panic]
    async fn test_source_transformer_with_bypass_panics() {
        let tracker = Tracker::new(None, CancellationToken::new());

        // create the source which produces x number of messages
        let cln_token = CancellationToken::new();

        // create the bypass router config to pass to the forwarder
        let batch_size: usize = 10;
        let fallback_tags = vec!["fallback".to_string()];
        let on_success_tags = vec!["on_success".to_string()];
        let conditions = BypassConditions {
            sink: None,
            fallback: Some(Box::new(ForwardConditions::new(TagConditions {
                values: fallback_tags.clone(),
                operator: Some("or".to_string()),
            }))),
            on_success: Some(Box::new(ForwardConditions::new(TagConditions {
                values: on_success_tags.clone(),
                operator: Some("or".to_string()),
            }))),
        };
        let bypass_router_config =
            BypassRouterConfig::new(conditions, batch_size, Duration::from_millis(1000));

        // Create the source
        let source_handle = create_ud_source(
            SimpleSource::new(100),
            Some(PanickingConditionalTransformer::new(
                0,
                10,
                10,
                None,
                Some(fallback_tags),
            )),
            batch_size,
            cln_token.clone(),
            tracker.clone(),
        )
        .await;

        let sink_handle = create_sink(
            SinkType::UserDefined(SinkLog::new()),
            Some(SinkType::BuiltIn(SinkClientType::Log)),
            Some(SinkType::BuiltIn(SinkClientType::Log)),
            batch_size,
        )
        .await;

        start_forwarder_test(
            source_handle,
            None,
            sink_handle,
            Some(bypass_router_config),
            cln_token,
        )
        .await;
    }

    struct PanickingMapper {
        sink_max_count: usize,
        fallback_max_count: usize,
        on_success_max_count: usize,
        sink_count: AtomicUsize,
        fallback_count: AtomicUsize,
        on_success_count: AtomicUsize,
        sink_tags: Option<Vec<String>>,
        fallback_tags: Option<Vec<String>>,
    }

    impl PanickingMapper {
        pub(crate) fn new(
            sink_count: usize,
            fallback_count: usize,
            on_success_count: usize,
            sink_tags: Option<Vec<String>>,
            fallback_tags: Option<Vec<String>>,
        ) -> Self {
            Self {
                sink_max_count: sink_count,
                fallback_max_count: fallback_count,
                on_success_max_count: on_success_count,
                sink_count: AtomicUsize::new(0),
                fallback_count: AtomicUsize::new(0),
                on_success_count: AtomicUsize::new(0),
                sink_tags,
                fallback_tags,
            }
        }
    }

    #[tonic::async_trait]
    impl map::Mapper for PanickingMapper {
        async fn map(&self, input: map::MapRequest) -> Vec<map::Message> {
            let message = map::Message::new(input.value).with_keys(input.keys);

            let message = if self.sink_count.load(Ordering::SeqCst) < self.sink_max_count {
                self.sink_count.fetch_add(1, Ordering::SeqCst);
                message.with_tags(
                    self.sink_tags
                        .clone()
                        .expect("sink_tags is None when sink_max_count > 0"),
                )
            } else if self.fallback_count.load(Ordering::SeqCst) < self.fallback_max_count {
                self.fallback_count.fetch_add(1, Ordering::SeqCst);
                message.with_tags(
                    self.fallback_tags
                        .clone()
                        .expect("fallback_tags is None when fallback_max_count > 0"),
                )
            } else if self.on_success_count.load(Ordering::SeqCst) < self.on_success_max_count {
                self.on_success_count.fetch_add(1, Ordering::SeqCst);
                panic!("on_success_count reached max count");
            } else {
                message
            };

            vec![message]
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    #[should_panic]
    async fn test_source_map_with_bypass_panics() {
        let tracker = Tracker::new(None, CancellationToken::new());

        // create the source which produces x number of messages
        let cln_token = CancellationToken::new();

        // create the bypass router config to pass to the forwarder
        let batch_size: usize = 10;
        let fallback_tags = vec!["fallback".to_string()];
        let on_success_tags = vec!["on_success".to_string()];
        let conditions = BypassConditions {
            sink: None,
            fallback: Some(Box::new(ForwardConditions::new(TagConditions {
                values: fallback_tags.clone(),
                operator: Some("or".to_string()),
            }))),
            on_success: Some(Box::new(ForwardConditions::new(TagConditions {
                values: on_success_tags.clone(),
                operator: Some("or".to_string()),
            }))),
        };
        let bypass_router_config =
            BypassRouterConfig::new(conditions, batch_size, Duration::from_millis(1000));

        // Create the source
        let source_handle = create_ud_source(
            SimpleSource::new(10000000),
            None::<NoOpTransformer>,
            batch_size,
            cln_token.clone(),
            tracker.clone(),
        )
        .await;

        let map_handle = create_mapper(
            PanickingMapper::new(0, 10, 10, None, Some(fallback_tags)),
            tracker.clone(),
            MapMode::Unary,
            batch_size,
            Duration::from_secs(5),
            Duration::from_secs(3),
            10,
        )
        .await;

        let sink_handle = create_sink(
            SinkType::UserDefined(SinkLog::new()),
            Some(SinkType::BuiltIn(SinkClientType::Log)),
            Some(SinkType::BuiltIn(SinkClientType::Log)),
            batch_size,
        )
        .await;

        start_forwarder_test(
            source_handle,
            Some(map_handle),
            sink_handle,
            Some(bypass_router_config),
            cln_token,
        )
        .await;
    }

    async fn start_forwarder_test(
        source: SourceTestHandle,
        mapper: Option<MapperTestHandle>,
        sink_writer: SinkTestHandle,
        bypass_router_config: Option<BypassRouterConfig>,
        cln_token: CancellationToken,
    ) {
        let SourceTestHandle {
            mut source_transformer_test_handle,
            source: sourcer,
            server_handle: source_server_handle,
        } = source;

        let (mapper, map_server_handle) = match mapper {
            Some(mapper_test_handle) => {
                let MapperTestHandle {
                    server_handle: map_server_handle,
                    mapper,
                } = mapper_test_handle;
                (Some(mapper), Some(map_server_handle))
            }
            None => (None, None),
        };

        let SinkTestHandle {
            sink_writer,
            ud_sink_server_handle,
            fb_ud_sink_server_handle,
            ons_ud_sink_server_handle,
        } = sink_writer;

        // create the forwarder with the source, transformer, and writer
        let forwarder = Forwarder::new(sourcer.clone(), mapper, sink_writer, bypass_router_config);

        let cancel_token = cln_token.clone();
        let forwarder_handle: JoinHandle<Result<()>> = tokio::spawn(async move {
            forwarder.start(cancel_token).await?;
            Ok(())
        });

        // wait for one sec to check if the pending becomes zero, because all the messages
        // should be read and acked; if it doesn't, then fail the test
        let tokio_result = tokio::time::timeout(Duration::from_secs(1), async move {
            loop {
                let pending = sourcer.pending().await.unwrap();
                if pending == Some(0) {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await;

        cln_token.cancel();
        forwarder_handle
            .await
            .expect("Join handle await failed")
            .expect("error from forwarder join handle ");

        assert!(
            tokio_result.is_ok(),
            "Timeout occurred before pending became zero"
        );

        if let Some(source_transformer) = source_transformer_test_handle.take() {
            drop(source_transformer.transformer);
            source_transformer.server_handle.shutdown();
        }
        source_server_handle.shutdown();

        if let Some(server_handle) = map_server_handle {
            server_handle.shutdown();
        }

        if let Some(ud_sink_server_handle) = ud_sink_server_handle {
            ud_sink_server_handle.shutdown();
        }

        if let Some(fb_ud_sink_server_handle) = fb_ud_sink_server_handle {
            fb_ud_sink_server_handle.shutdown();
        }

        if let Some(ons_ud_sink_server_handle) = ons_ud_sink_server_handle {
            ons_ud_sink_server_handle.shutdown();
        }
    }
}
