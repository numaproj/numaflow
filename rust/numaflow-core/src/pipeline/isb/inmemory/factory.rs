//! In-memory ISB factory — for local testing only.
//!
//! Registers `SimpleBuffer` instances by stream name and `SimpleKVStore` instances
//! by bucket name so readers, writers, and watermark components share state.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use numaflow_shared::kv::KVStore;
use numaflow_shared::kv::inmemory::SimpleKVStore;
use parking_lot::Mutex;
use tokio_util::sync::CancellationToken;
use tracing::warn;

use crate::Result;
use crate::config::pipeline::isb::{
    BufferWriterConfig, DEFAULT_MAX_LENGTH, DEFAULT_USAGE_LIMIT, ISBConfig, Stream,
};
use crate::pipeline::isb::ISBFactory;
use crate::pipeline::isb::dyn_adapter::{ISBReaderRef, ISBWriterRef};
use crate::pipeline::isb::inmemory::SimpleBuffer;
use crate::pipeline::isb::inmemory::adapter::{SimpleReaderAdapter, SimpleWriterAdapter};

/// Factory that creates in-memory ISB readers, writers, and KV stores.
///
/// Buffers and KV stores are keyed by name and reused across create calls so that
/// a writer and reader for the same stream (or two watermark components for the
/// same bucket) share state.
pub(crate) struct InMemoryFactory {
    buffers: Mutex<HashMap<String, SimpleBuffer>>,
    kv_stores: Mutex<HashMap<String, Arc<SimpleKVStore>>>,
}

impl InMemoryFactory {
    /// Create an empty factory and log the local-testing-only warning.
    pub(crate) fn new() -> Self {
        warn!(
            "in-memory ISB backend is for local testing only: single-process, non-durable, no replay"
        );
        Self {
            buffers: Mutex::new(HashMap::new()),
            kv_stores: Mutex::new(HashMap::new()),
        }
    }

    /// Return `(pending_count, in_flight_count)` for a named buffer, if present.
    ///
    /// Useful for drain detection by embedders/tests.
    #[allow(dead_code)]
    pub(crate) fn buffer_stats(&self, name: &str) -> Option<(usize, usize)> {
        self.buffers
            .lock()
            .get(name)
            .map(|b| (b.pending_count(), b.in_flight_count()))
    }

    /// Get-or-create a buffer for `stream.name`.
    ///
    /// First creation wins for capacity / usage_limit / partition; an existing
    /// buffer (e.g. created by a reader first) is reused as-is.
    fn get_or_create_buffer(
        &self,
        stream: &Stream,
        max_length: usize,
        usage_limit: f64,
    ) -> SimpleBuffer {
        let mut buffers = self.buffers.lock();
        if let Some(existing) = buffers.get(stream.name) {
            return existing.clone();
        }
        let leaked_name = stream.name;
        let buffer = SimpleBuffer::with_config(
            max_length,
            stream.partition,
            leaked_name,
            usage_limit,
            Duration::from_secs(1),
        );
        buffers.insert(stream.name.to_string(), buffer.clone());
        buffer
    }
}

#[async_trait]
impl ISBFactory for InMemoryFactory {
    /// The in-memory backend does not compress; `isb_config` is ignored.
    async fn create_reader(
        &self,
        stream: Stream,
        _isb_config: Option<&ISBConfig>,
    ) -> Result<ISBReaderRef> {
        let buffer = self.get_or_create_buffer(&stream, DEFAULT_MAX_LENGTH, DEFAULT_USAGE_LIMIT);
        Ok(Arc::new(SimpleReaderAdapter::new(buffer.reader())))
    }

    /// Get-or-create the buffer for `stream.name`. First creation wins for capacity;
    /// an existing buffer (e.g. created by a reader first) is reused as-is.
    /// The in-memory backend does not compress; `isb_config` is ignored.
    async fn create_writer(
        &self,
        stream: Stream,
        writer_config: BufferWriterConfig,
        _isb_config: Option<&ISBConfig>,
        _cln_token: CancellationToken,
    ) -> Result<ISBWriterRef> {
        let buffer =
            self.get_or_create_buffer(&stream, writer_config.max_length, writer_config.usage_limit);
        Ok(Arc::new(SimpleWriterAdapter::new(buffer.writer())))
    }

    /// Returning the same instance per bucket name is load-bearing: watermark
    /// publisher/fetcher must share state.
    async fn create_kv_store(&self, bucket: String) -> Result<Arc<dyn KVStore>> {
        let mut stores = self.kv_stores.lock();
        if let Some(existing) = stores.get(&bucket) {
            return Ok(Arc::clone(existing) as Arc<dyn KVStore>);
        }
        let leaked_name: &'static str = Box::leak(bucket.clone().into_boxed_str());
        let store = Arc::new(SimpleKVStore::new(leaked_name));
        stores.insert(bucket, Arc::clone(&store));
        Ok(store as Arc<dyn KVStore>)
    }
}

#[cfg(test)]
#[allow(clippy::indexing_slicing)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use chrono::Utc;
    use std::collections::HashMap;
    use std::sync::Arc;

    use crate::config::pipeline::isb::{BufferWriterConfig, ISBClientConfig, Stream};
    use crate::message::{IntOffset, Message, MessageID, Offset};
    use crate::pipeline::isb::create_isb_factory;

    fn test_message(id: &str, payload: &str) -> Message {
        Message {
            typ: Default::default(),
            keys: Arc::new([]),
            tags: None,
            value: Bytes::from(payload.to_string()),
            offset: Offset::Int(IntOffset::new(0, 0)),
            event_time: Utc::now(),
            watermark: None,
            id: MessageID {
                vertex_name: "test".into(),
                index: 0,
                offset: id.to_string().into(),
            },
            headers: Arc::new(HashMap::new()),
            metadata: None,
            is_late: false,
            nack_options: None,
        }
    }

    /// Writer then reader for the same stream name share the buffer (type-erased path).
    #[tokio::test]
    async fn test_factory_writer_reader_share_buffer() {
        let factory = InMemoryFactory::new();
        let stream = Stream::new("shared-stream", "v", 0);
        let cln = CancellationToken::new();

        let writer = factory
            .create_writer(stream.clone(), BufferWriterConfig::default(), None, cln)
            .await
            .expect("create_writer");
        let reader = factory
            .create_reader(stream, None)
            .await
            .expect("create_reader");

        writer
            .write(test_message("m1", "hello"))
            .await
            .expect("write");

        let messages = reader
            .fetch(10, Duration::from_millis(100))
            .await
            .expect("fetch");
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].value, Bytes::from("hello"));

        let stats = factory.buffer_stats("shared-stream");
        assert_eq!(stats, Some((0, 1))); // pending=0 (in-flight after fetch), in_flight=1
    }

    /// Same bucket name → shared KV instance; different names → independent.
    #[tokio::test]
    async fn test_factory_kv_store_registry() {
        let factory = InMemoryFactory::new();

        let a1 = factory
            .create_kv_store("b".to_string())
            .await
            .expect("create a1");
        let a2 = factory
            .create_kv_store("b".to_string())
            .await
            .expect("create a2");
        let other = factory
            .create_kv_store("other".to_string())
            .await
            .expect("create other");

        a1.put("k", Bytes::from("v")).await.expect("put");
        assert_eq!(
            a2.get("k").await.expect("get via peer"),
            Some(Bytes::from("v")),
            "same bucket name must share state"
        );
        assert_eq!(
            other.get("k").await.expect("get other"),
            None,
            "different bucket names must be independent"
        );
    }

    /// `create_isb_factory(InMemory)` returns a working factory.
    #[tokio::test]
    async fn test_create_isb_factory_inmemory() {
        let cln = CancellationToken::new();
        let factory = create_isb_factory(&ISBClientConfig::InMemory, cln.clone())
            .await
            .expect("create_isb_factory");

        let stream = Stream::new("factory-stream", "v", 0);
        let writer = factory
            .create_writer(stream.clone(), BufferWriterConfig::default(), None, cln)
            .await
            .expect("create_writer");
        let reader = factory
            .create_reader(stream, None)
            .await
            .expect("create_reader");

        writer
            .write(test_message("m1", "via-factory"))
            .await
            .expect("write");

        let messages = reader
            .fetch(10, Duration::from_millis(100))
            .await
            .expect("fetch");
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].value, Bytes::from("via-factory"));
    }
}
