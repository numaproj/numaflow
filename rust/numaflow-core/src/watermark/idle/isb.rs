//! ISB Idle Manager resolves the following conundrums:
//!
//! > How to decide if the ISB ([Stream]) is idling?
//!
//! If we have not published any WM for that given [Stream] for X duration, then it is considered
//! idling.
//!
//! > When to publish the idle watermark?
//!
//! Once the X duration has passed, an idle WM will be published.
//!
//! > What to publish as the idle watermark?
//!
//! Fetch the `min(wm(Head Idle Offset), wm(smallest offset of inflight messages))` and publish as
//! idle.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use chrono::{DateTime, Utc};

use crate::config::pipeline::ToVertexConfig;
use crate::config::pipeline::isb::Stream;
use crate::message::{IntOffset, Message, MessageType, Offset};
use crate::pipeline::isb::dyn_adapter::ISBWriterRef;

/// State of each partition in the ISB. It has the information required to identify whether the
/// partition is idling or not.
#[derive(Clone)]
struct IdleState {
    stream: Stream,
    last_wm_published_time: DateTime<Utc>,
    /// This offset's WM will keep increasing as long as the [Stream] is idling.
    wmb_msg_offset: Option<i64>,
}

impl Default for IdleState {
    fn default() -> Self {
        IdleState {
            stream: Stream::default(),
            last_wm_published_time: Utc::now(),
            wmb_msg_offset: None,
        }
    }
}

// TODO(vigith): rename ISBIdleDetector to ISBIdleManager, it is not just detecting, but also managing the idle state.

/// ISBIdleDetector detects the idle partitions in the ISB. It keeps track of the last published watermark
/// state to detect the idle partitions, it also keeps track of the ctrl message offset that should
/// be used for publishing the idle watermark.
#[derive(Clone)]
pub(crate) struct ISBIdleDetector {
    /// last published wm state per [Stream].
    last_published_wm_state: Arc<RwLock<HashMap<&'static str, Vec<IdleState>>>>,
    /// Writers used to publish idle WMB control messages. Shared with the forwarder's
    /// writer map — do not create duplicate producers.
    writers: HashMap<&'static str, ISBWriterRef>,
    /// X duration we wait before we start publishing idle WM.
    idle_timeout: Duration,
}

impl ISBIdleDetector {
    /// Creates a new ISBIdleManager.
    pub(crate) async fn new(
        idle_timeout: Duration,
        to_vertex_configs: &[ToVertexConfig],
        writers: HashMap<&'static str, ISBWriterRef>,
    ) -> Self {
        let mut last_published_wm = HashMap::new();

        // for each vertex, we need per stream (branch) idle state
        for config in to_vertex_configs {
            let idle_states = config
                .writer_config
                .streams
                .iter()
                .map(|stream| {
                    let stream = stream.clone();
                    IdleState {
                        stream,
                        ..Default::default()
                    }
                })
                .collect();

            last_published_wm.insert(config.name, idle_states);
        }

        ISBIdleDetector {
            idle_timeout,
            last_published_wm_state: Arc::new(RwLock::new(last_published_wm)),
            writers,
        }
    }

    /// resets the stream's idle metadata by updating the last published time and resets the ctrl
    /// message offset. It implicitly marks that stream as active.
    pub(crate) async fn reset_idle(&mut self, stream: &Stream) {
        let mut write_guard = self
            .last_published_wm_state
            .write()
            .expect("Failed to get write lock");

        let last_published_wm = write_guard
            .get_mut(stream.vertex)
            .unwrap_or_else(|| panic!("Invalid vertex: {}", stream.vertex));

        last_published_wm
            .get_mut(stream.partition as usize)
            .expect("should have partition")
            .last_wm_published_time = Utc::now();
        // setting None for wmb-offset means it is active
        last_published_wm
            .get_mut(stream.partition as usize)
            .expect("should have partition")
            .wmb_msg_offset = None;
    }

    /// Fetches the offset to be used for publishing the idle watermark. Only a WMB (idle=true) can be used
    /// to send idle watermark, hence if no such WMB's are published, we publish a WMB and return its
    /// offset, or return the current "active" WMB's offset.
    pub(crate) async fn fetch_idle_offset(&self, stream: &Stream) -> crate::error::Result<i64> {
        let idle_state = {
            let read_guard = self
                .last_published_wm_state
                .read()
                .expect("Failed to get read lock");
            let last_published_wm = read_guard.get(stream.vertex).expect("Invalid vertex");
            last_published_wm
                .get(stream.partition as usize)
                .expect("should have partition")
                .clone()
        };

        if let Some(offset) = idle_state.wmb_msg_offset {
            return Ok(offset);
        }

        let writer = self.writers.get(idle_state.stream.name).ok_or_else(|| {
            crate::error::Error::Watermark(format!(
                "No ISB writer for stream '{}' when publishing idle WMB",
                idle_state.stream.name
            ))
        })?;

        let ctrl_msg = Message {
            typ: MessageType::WMB,
            ..Default::default()
        };

        let result = writer
            .write(ctrl_msg)
            .await
            .map_err(|e| crate::error::Error::Watermark(e.to_string()))?;

        match result.offset {
            Offset::Int(IntOffset { offset, .. }) => Ok(offset),
            Offset::String(_) => Err(crate::error::Error::Watermark(
                "idle WMB requires an Int offset; String offsets are not supported for watermarks"
                    .to_string(),
            )),
        }
    }

    /// Updates the idle stream's metadata, by setting the ctrl message offset and updates the last published time.
    pub(crate) async fn update_idle_metadata(&mut self, stream: &Stream, offset: i64) {
        let mut write_guard = self
            .last_published_wm_state
            .write()
            .expect("Failed to get write lock");
        let last_published_wm = write_guard
            .get_mut(stream.vertex)
            .unwrap_or_else(|| panic!("Invalid vertex: {}", stream.vertex));

        // setting an offset for wmb-offset means it is idle, and we will do inplace incr of WM for that offset.
        last_published_wm
            .get_mut(stream.partition as usize)
            .expect("should have partition")
            .wmb_msg_offset = Some(offset);
        last_published_wm
            .get_mut(stream.partition as usize)
            .expect("should have partition")
            .last_wm_published_time = Utc::now();
    }

    /// Fetches streams that need watermark publishing. A stream needs publishing if the last
    /// published time is greater than the idle timeout.
    pub(crate) async fn fetch_streams_needing_publish(&self) -> Vec<Stream> {
        let read_guard = self
            .last_published_wm_state
            .read()
            .expect("Failed to get read lock");

        read_guard
            .values()
            .flat_map(|partitions| {
                partitions
                    .iter()
                    .filter(|partition| {
                        Utc::now().timestamp_millis()
                            - partition.last_wm_published_time.timestamp_millis()
                            > self.idle_timeout.as_millis() as i64
                    })
                    .map(|partition| partition.stream.clone())
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::time::Duration;

    use async_nats::jetstream;
    use async_nats::jetstream::stream;
    use tokio::time::sleep;
    use tokio_util::sync::CancellationToken;

    use super::*;
    use crate::config::pipeline::isb::BufferWriterConfig;
    use crate::config::pipeline::isb::Stream;
    use crate::config::pipeline::{ToVertexConfig, VertexType};
    use crate::pipeline::isb::jetstream::js_writer::JetStreamWriter;

    async fn test_writer(
        js_context: jetstream::Context,
        stream: &Stream,
    ) -> HashMap<&'static str, ISBWriterRef> {
        // Ensure a consumer exists so JetStreamWriter's background monitor can start.
        let _ = js_context
            .create_consumer_on_stream(
                async_nats::jetstream::consumer::pull::Config {
                    name: Some(stream.name.to_string()),
                    ..Default::default()
                },
                stream.name,
            )
            .await;

        let writer = JetStreamWriter::new(
            stream.clone(),
            js_context,
            BufferWriterConfig {
                streams: vec![stream.clone()],
                ..Default::default()
            },
            None,
            CancellationToken::new(),
        )
        .await
        .unwrap();
        let mut writers = HashMap::new();
        writers.insert(stream.name, Arc::new(writer) as ISBWriterRef);
        writers
    }

    #[cfg(feature = "nats-tests")]
    #[tokio::test]
    async fn test_mark_active() {
        let stream = Stream::new("test_stream", "test_vertex", 0);
        let to_vertex_config = ToVertexConfig {
            name: "test_vertex",
            partitions: 1,
            writer_config: BufferWriterConfig {
                streams: vec![stream.clone()],
                ..Default::default()
            },
            conditions: None,
            to_vertex_type: VertexType::Sink,
            ordered_processing_enabled: false,
        };

        let mut manager = ISBIdleDetector::new(
            Duration::from_millis(200),
            &[to_vertex_config],
            HashMap::new(),
        )
        .await;

        manager.reset_idle(&stream).await;

        let read_guard = manager
            .last_published_wm_state
            .read()
            .expect("Failed to get read lock");
        let idle_state = read_guard
            .get("test_vertex")
            .expect("Expected test_vertex to exist")
            .first()
            .expect("Expected at least one idle state");
        assert_eq!(idle_state.stream, stream);
        assert!(idle_state.wmb_msg_offset.is_none());
    }

    #[cfg(feature = "nats-tests")]
    #[tokio::test]
    async fn test_fetch_idle_offset() {
        let client = async_nats::connect("localhost:4222").await.unwrap();
        let js_context = jetstream::new(client);
        let stream = Stream::new("test_idle_fetch_stream", "test_vertex", 0);

        // Delete stream if it exists
        let _ = js_context.delete_stream(stream.name).await;
        let _stream = js_context
            .get_or_create_stream(stream::Config {
                name: stream.name.to_string(),
                subjects: vec![stream.name.to_string()],
                ..Default::default()
            })
            .await
            .unwrap();

        let to_vertex_config = ToVertexConfig {
            name: "test_vertex",
            partitions: 1,
            writer_config: BufferWriterConfig {
                streams: vec![stream.clone()],
                ..Default::default()
            },
            conditions: None,
            to_vertex_type: VertexType::Sink,
            ordered_processing_enabled: false,
        };

        let writers = test_writer(js_context, &stream).await;
        let manager =
            ISBIdleDetector::new(Duration::from_millis(200), &[to_vertex_config], writers).await;

        let offset = manager
            .fetch_idle_offset(&stream)
            .await
            .expect("Failed to fetch idle offset");
        assert!(offset > 0);
    }

    /// Two consecutive idle-WMB writes must both land with distinct sequences
    /// (JetStream must not dedup default-constructed WMB message ids).
    #[cfg(feature = "nats-tests")]
    #[tokio::test]
    async fn test_consecutive_idle_wmb_writes_get_distinct_sequences() {
        let client = async_nats::connect("localhost:4222").await.unwrap();
        let js_context = jetstream::new(client);
        let stream = Stream::new("test_idle_wmb_dedup_stream", "test_vertex", 0);

        let _ = js_context.delete_stream(stream.name).await;
        let _stream = js_context
            .get_or_create_stream(stream::Config {
                name: stream.name.to_string(),
                subjects: vec![stream.name.to_string()],
                ..Default::default()
            })
            .await
            .unwrap();

        let to_vertex_config = ToVertexConfig {
            name: "test_vertex",
            partitions: 1,
            writer_config: BufferWriterConfig {
                streams: vec![stream.clone()],
                ..Default::default()
            },
            conditions: None,
            to_vertex_type: VertexType::Sink,
            ordered_processing_enabled: false,
        };

        let writers = test_writer(js_context, &stream).await;
        let mut manager =
            ISBIdleDetector::new(Duration::from_millis(200), &[to_vertex_config], writers).await;

        let offset1 = manager
            .fetch_idle_offset(&stream)
            .await
            .expect("first idle WMB write");
        // Reset so the next fetch publishes a new WMB instead of reusing offset1.
        manager.reset_idle(&stream).await;
        let offset2 = manager
            .fetch_idle_offset(&stream)
            .await
            .expect("second idle WMB write");

        assert_ne!(
            offset1, offset2,
            "consecutive idle WMBs must not be JetStream-deduped"
        );
        assert!(offset2 > offset1);
    }

    #[cfg(feature = "nats-tests")]
    #[tokio::test]
    async fn test_mark_idle() {
        let client = async_nats::connect("localhost:4222").await.unwrap();
        let js_context = jetstream::new(client);

        let stream = Stream::new("test_idle_mark_stream", "test_vertex", 0);
        // Delete stream if it exists
        let _ = js_context.delete_stream(stream.name).await;
        let _stream = js_context
            .get_or_create_stream(stream::Config {
                name: stream.name.to_string(),
                subjects: vec![stream.name.to_string()],
                ..Default::default()
            })
            .await
            .unwrap();

        let to_vertex_config = ToVertexConfig {
            name: "test_vertex",
            partitions: 1,
            writer_config: BufferWriterConfig {
                streams: vec![stream.clone()],
                ..Default::default()
            },
            conditions: None,
            to_vertex_type: VertexType::Sink,
            ordered_processing_enabled: false,
        };

        let writers = test_writer(js_context, &stream).await;
        let mut manager =
            ISBIdleDetector::new(Duration::from_millis(200), &[to_vertex_config], writers).await;

        let offset = manager
            .fetch_idle_offset(&stream)
            .await
            .expect("Failed to fetch idle offset");
        manager.update_idle_metadata(&stream, offset).await;

        let read_guard = manager
            .last_published_wm_state
            .read()
            .expect("Failed to get read lock");
        let idle_state = read_guard
            .get("test_vertex")
            .expect("Expected test_vertex to exist")
            .first()
            .expect("Expected at least one idle state");
        assert_eq!(idle_state.wmb_msg_offset, Some(offset));
    }

    #[cfg(feature = "nats-tests")]
    #[tokio::test]
    async fn test_fetch_streams_needing_publish() {
        let stream = Stream::new("test_stream", "test_vertex", 0);
        let to_vertex_config = ToVertexConfig {
            name: "test_vertex",
            partitions: 1,
            writer_config: BufferWriterConfig {
                streams: vec![stream.clone()],
                ..Default::default()
            },
            conditions: None,
            to_vertex_type: VertexType::Sink,
            ordered_processing_enabled: false,
        };

        let mut manager = ISBIdleDetector::new(
            Duration::from_millis(10),
            &[to_vertex_config],
            HashMap::new(),
        )
        .await;

        // Mark the stream as active first
        manager.reset_idle(&stream).await;

        // Simulate idle timeout
        sleep(Duration::from_millis(20)).await;

        let streams_needing_publish = manager.fetch_streams_needing_publish().await;
        assert_eq!(streams_needing_publish.len(), 1);
        assert_eq!(
            *streams_needing_publish
                .first()
                .expect("Expected at least one stream needing publish"),
            stream
        );
    }
}
