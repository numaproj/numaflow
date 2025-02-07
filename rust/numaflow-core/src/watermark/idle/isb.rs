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

/// ISBIdleManager manages the idle partitions in the ISB, also tracks the wmb offsets for the idle partitions.
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
    pub(crate) async fn mark_active(&mut self, vertex: &'static str, partition: u16) {
        let mut write_guard = self
            .last_published_wm
            .write()
            .expect("Failed to get write lock");
        let last_published_wm = write_guard.get_mut(vertex).expect("Invalid vertex");
        last_published_wm[partition as usize].last_published_time = Utc::now();
        last_published_wm[partition as usize].ctrl_msg_offset = None;
    }

    /// fetches the offset to be used for publishing the idle watermark.
    pub(crate) async fn fetch_idle_offset(
        &self,
        vertex: &'static str,
        partition: u16,
    ) -> crate::error::Result<i64> {
        let idle_state = {
            let read_guard = self
                .last_published_wm
                .read()
                .expect("Failed to get read lock");
            let last_published_wm = read_guard.get(vertex).expect("Invalid vertex");
            last_published_wm[partition as usize].clone()
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
    pub(crate) async fn mark_idle(&mut self, vertex: &'static str, partition: u16, offset: i64) {
        let mut write_guard = self
            .last_published_wm
            .write()
            .expect("Failed to get write lock");
        let last_published_wm = write_guard.get_mut(vertex).expect("Invalid vertex");
        last_published_wm[partition as usize].ctrl_msg_offset = Some(offset);
        last_published_wm[partition as usize].last_published_time = Utc::now();
    }

    /// fetches the idle partitions for the vertices, we consider a partition as idle if the last published
    /// time is greater than the idle timeout.
    pub(crate) async fn fetch_idle_partitions(&self) -> HashMap<&'static str, Vec<u16>> {
        let read_guard = self
            .last_published_wm
            .read()
            .expect("Failed to get read lock");

        read_guard
            .iter()
            .map(|(vertex, partitions)| {
                let idle_partition = partitions
                    .iter()
                    .enumerate()
                    .filter(|(_, state)| {
                        Utc::now().timestamp_millis() - state.last_published_time.timestamp_millis()
                            > self.idle_timeout.as_millis() as i64
                    })
                    .map(|(partition, _)| partition as u16)
                    .collect();
                (*vertex, idle_partition)
            })
            .collect()
    }

    /// fetch all the partitions for the vertices.
    pub(crate) async fn fetch_all_partitions(&self) -> HashMap<&'static str, Vec<u16>> {
        let read_guard = self
            .last_published_wm
            .read()
            .expect("Failed to get read lock");

        read_guard
            .iter()
            .map(|(vertex, partitions)| {
                let all_partition = partitions
                    .iter()
                    .enumerate()
                    .map(|(partition, _)| partition as u16)
                    .collect();
                (*vertex, all_partition)
            })
            .collect()
    }
}
