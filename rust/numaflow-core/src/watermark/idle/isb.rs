use crate::config::pipeline::isb::Stream;
use crate::config::pipeline::ToVertexConfig;
use bytes::BytesMut;
use chrono::{DateTime, Utc};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::Duration;

/// State of each partition in the ISB. Has the information required to identify the idle partitions.
#[derive(Clone)]
struct IdleState {
    stream: Stream,
    last_published_time: DateTime<Utc>,
    ctrl_msg_offset: Option<i64>,
}

impl Default for IdleState {
    fn default() -> Self {
        IdleState {
            stream: Stream::default(),
            last_published_time: Utc::now(),
            ctrl_msg_offset: None,
        }
    }
}

/// ISBIdleManager manages the idle partitions in the ISB, keeps track the last published watermark state
/// to detect the idle partitions, also keeps track of the ctrl message offset that should be used for publishing
/// the idle watermark.
#[derive(Clone)]
pub(crate) struct ISBIdleManager {
    last_published_wm: Arc<RwLock<HashMap<&'static str, Vec<IdleState>>>>,
    js_context: async_nats::jetstream::Context,
    idle_timeout: Duration,
}

impl ISBIdleManager {
    /// Creates a new ISBIdleManager.
    pub(crate) async fn new(
        idle_timeout: Duration,
        to_vertex_configs: &[ToVertexConfig],
        js_context: async_nats::jetstream::Context,
    ) -> Self {
        let mut last_published_wm = HashMap::new();
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
        ISBIdleManager {
            idle_timeout,
            last_published_wm: Arc::new(RwLock::new(last_published_wm)),
            js_context,
        }
    }

    /// marks the partition as active, updates the last published time and resets the ctrl message offset.
    pub(crate) async fn mark_active(&mut self, stream: &Stream) {
        let mut write_guard = self
            .last_published_wm
            .write()
            .expect("Failed to get write lock");
        let last_published_wm = write_guard
            .get_mut(stream.vertex)
            .expect(format!("Invalid vertex: {}", stream.vertex).as_str());
        last_published_wm[stream.partition as usize].last_published_time = Utc::now();
        last_published_wm[stream.partition as usize].ctrl_msg_offset = None;
    }

    /// fetches the offset to be used for publishing the idle watermark.
    pub(crate) async fn fetch_idle_offset(&self, stream: &Stream) -> crate::error::Result<i64> {
        let idle_state = {
            let read_guard = self
                .last_published_wm
                .read()
                .expect("Failed to get read lock");
            let last_published_wm = read_guard.get(stream.vertex).expect("Invalid vertex");
            last_published_wm[stream.partition as usize].clone()
        };

        if let Some(offset) = idle_state.ctrl_msg_offset {
            return Ok(offset);
        }

        let ctrl_msg_bytes: BytesMut = crate::message::Message {
            kind: crate::message::MessageKind::WMB,
            ..Default::default()
        }
        .try_into()?;

        let offset = self
            .js_context
            .publish(idle_state.stream.name, ctrl_msg_bytes.freeze())
            .await
            .map_err(|e| crate::error::Error::Watermark(e.to_string()))?
            .await
            .map_err(|e| crate::error::Error::Watermark(e.to_string()))?
            .sequence;

        Ok(offset as i64)
    }

    /// marks the partition as idle, by setting the ctrl message offset and updates the last published time.
    pub(crate) async fn mark_idle(&mut self, stream: &Stream, offset: i64) {
        let mut write_guard = self
            .last_published_wm
            .write()
            .expect("Failed to get write lock");
        let last_published_wm = write_guard.get_mut(stream.vertex).expect("Invalid vertex");
        last_published_wm[stream.partition as usize].ctrl_msg_offset = Some(offset);
        last_published_wm[stream.partition as usize].last_published_time = Utc::now();
    }

    /// fetches the idle streams, we consider a stream as idle if the last published
    /// time is greater than the idle timeout.
    pub(crate) async fn fetch_idle_streams(&self) -> Vec<Stream> {
        let read_guard = self
            .last_published_wm
            .read()
            .expect("Failed to get read lock");

        read_guard
            .iter()
            .flat_map(|(_, partitions)| {
                partitions
                    .iter()
                    .filter(|partition| {
                        Utc::now().timestamp_millis()
                            - partition.last_published_time.timestamp_millis()
                            > self.idle_timeout.as_millis() as i64
                    })
                    .map(move |partition| partition.stream.clone())
            })
            .collect()
    }

    /// fetch all the partitions for the vertices.
    pub(crate) async fn fetch_all_streams(&self) -> Vec<Stream> {
        let read_guard = self
            .last_published_wm
            .read()
            .expect("Failed to get read lock");

        read_guard
            .iter()
            .flat_map(|(_, partitions)| partitions.iter().map(|partition| partition.stream.clone()))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::pipeline::isb::BufferWriterConfig;
    use crate::config::pipeline::isb::Stream;
    use crate::config::pipeline::ToVertexConfig;
    use async_nats::jetstream;
    use async_nats::jetstream::stream;
    use std::time::Duration;
    use tokio::time::sleep;

    #[tokio::test]
    async fn test_mark_active() {
        let client = async_nats::connect("localhost:4222").await.unwrap();
        let js_context = jetstream::new(client);

        let stream = Stream::new("test_stream", "test_vertex", 0);
        let to_vertex_config = ToVertexConfig {
            name: "test_vertex",
            partitions: 1,
            writer_config: BufferWriterConfig {
                streams: vec![stream.clone()],
                ..Default::default()
            },
            conditions: None,
        };

        let mut manager =
            ISBIdleManager::new(Duration::from_millis(100), &[to_vertex_config], js_context).await;

        manager.mark_active(&stream).await;

        let read_guard = manager
            .last_published_wm
            .read()
            .expect("Failed to get read lock");
        let idle_state = &read_guard["test_vertex"][0];
        assert_eq!(idle_state.stream, stream);
        assert!(idle_state.ctrl_msg_offset.is_none());
    }

    #[tokio::test]
    async fn test_fetch_idle_offset() {
        let client = async_nats::connect("localhost:4222").await.unwrap();
        let js_context = jetstream::new(client);
        let stream = Stream::new("test_stream", "test_vertex", 0);

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
        };

        let manager =
            ISBIdleManager::new(Duration::from_millis(100), &[to_vertex_config], js_context).await;

        let offset = manager
            .fetch_idle_offset(&stream)
            .await
            .expect("Failed to fetch idle offset");
        assert!(offset > 0);
    }

    #[tokio::test]
    async fn test_mark_idle() {
        let client = async_nats::connect("localhost:4222").await.unwrap();
        let js_context = jetstream::new(client);

        let stream = Stream::new("test_stream", "test_vertex", 0);
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
        };

        let mut manager =
            ISBIdleManager::new(Duration::from_millis(100), &[to_vertex_config], js_context).await;

        let offset = manager
            .fetch_idle_offset(&stream)
            .await
            .expect("Failed to fetch idle offset");
        manager.mark_idle(&stream, offset).await;

        let read_guard = manager
            .last_published_wm
            .read()
            .expect("Failed to get read lock");
        let idle_state = &read_guard["test_vertex"][0];
        assert_eq!(idle_state.ctrl_msg_offset, Some(offset));
    }

    #[tokio::test]
    async fn test_fetch_idle_streams() {
        let client = async_nats::connect("localhost:4222").await.unwrap();
        let js_context = jetstream::new(client);

        let stream = Stream::new("test_stream", "test_vertex", 0);
        let to_vertex_config = ToVertexConfig {
            name: "test_vertex",
            partitions: 1,
            writer_config: BufferWriterConfig {
                streams: vec![stream.clone()],
                ..Default::default()
            },
            conditions: None,
        };

        let mut manager =
            ISBIdleManager::new(Duration::from_millis(10), &[to_vertex_config], js_context).await;

        // Mark the stream as active first
        manager.mark_active(&stream).await;

        // Simulate idle timeout
        sleep(Duration::from_millis(20)).await;

        let idle_streams = manager.fetch_idle_streams().await;
        assert_eq!(idle_streams.len(), 1);
        assert_eq!(idle_streams[0], stream);
    }
}
