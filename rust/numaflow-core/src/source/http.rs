//! HTTP source for Numaflow.

use crate::config::{get_vertex_name, get_vertex_replica};
use crate::error::Result;
use crate::message::{Message, MessageID, Offset, StringOffset};
use crate::source;
use crate::source::{SourceAcker, SourceReader};
use numaflow_http::HttpMessage;
use std::sync::Arc;

impl From<numaflow_http::Error> for crate::error::Error {
    fn from(value: numaflow_http::Error) -> Self {
        use numaflow_http::Error;
        match value {
            Error::ChannelFull() => Self::Source(format!("HTTP source: {value:?}")),
            Error::Server(_) | Error::ChannelSend(_) | Error::ChannelRecv(_) => {
                Self::Source(format!("HTTP source: {value:?}"))
            }
        }
    }
}

impl From<HttpMessage> for Message {
    fn from(value: HttpMessage) -> Self {
        Message {
            typ: Default::default(),
            keys: Arc::from(vec![]),
            tags: None,
            value: value.body,
            offset: Offset::String(StringOffset::new(value.id.clone(), *get_vertex_replica())),
            event_time: value.event_time,
            watermark: None,
            id: MessageID {
                vertex_name: get_vertex_name().to_string().into(),
                offset: value.id.into(),
                index: 0,
            },
            headers: value.headers,
            metadata: None,
            is_late: false,
        }
    }
}

#[derive(Clone)]
pub(crate) struct CoreHttpSource {
    batch_size: usize,
    http_source: numaflow_http::HttpSourceHandle,
}

impl CoreHttpSource {
    pub(crate) fn new(batch_size: usize, http_source: numaflow_http::HttpSourceHandle) -> Self {
        Self {
            batch_size,
            http_source,
        }
    }
}

impl SourceReader for CoreHttpSource {
    fn name(&self) -> &'static str {
        "HTTP"
    }

    async fn read(&mut self) -> Result<Vec<Message>> {
        self.http_source
            .read(self.batch_size)
            .await
            .map_err(|e| e.into())
            .map(|msgs| msgs.into_iter().map(|msg| msg.into()).collect())
    }

    async fn partitions(&mut self) -> Result<Vec<u16>> {
        Ok(vec![*get_vertex_replica()])
    }
}

impl SourceAcker for CoreHttpSource {
    async fn ack(&mut self, _: Vec<Offset>) -> Result<()> {
        self.http_source.ack(vec![]).await.map_err(|e| e.into())
    }
}

impl source::LagReader for CoreHttpSource {
    async fn pending(&mut self) -> Result<Option<usize>> {
        Ok(self.http_source.pending().await)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::source::{LagReader, SourceAcker, SourceReader};
    use hyper::{Method, Request};
    use hyper_util::client::legacy::Client;
    use hyper_util::rt::TokioExecutor;
    use std::net::TcpListener;
    use std::time::Duration;
    use tokio::time::sleep;

    #[tokio::test]
    async fn test_core_http_source() {
        // Bind to a random available port
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener); // Release the listener so HttpSource can bind to it

        // Create HttpSource config
        let http_source_config = numaflow_http::HttpSourceConfigBuilder::new("test")
            .addr(addr)
            .buffer_size(10)
            .timeout(Duration::from_millis(100))
            .build();

        // Create HttpSourceHandle
        let http_source = numaflow_http::HttpSourceHandle::new(http_source_config).await;

        // Create CoreHttpSource with batch size 5
        let batch_size = 5;
        let mut core_http_source = CoreHttpSource::new(batch_size, http_source);

        // Wait a bit for the server to start
        sleep(Duration::from_millis(100)).await;

        // Create HTTP client and send requests
        let client = Client::builder(TokioExecutor::new()).build_http();

        // Send test requests
        for i in 0..7 {
            let request = Request::builder()
                .method(Method::POST)
                .uri(format!("http://{}/vertices/test", addr))
                .header("Content-Type", "application/json")
                .header("X-Numaflow-Id", format!("test-id-{}", i))
                .body(format!(r#"{{"message": "test{}"}}"#, i))
                .unwrap();

            let response = client.request(request).await.unwrap();
            assert_eq!(response.status(), 200);
        }

        // Test pending count
        let pending = core_http_source.pending().await.unwrap();
        assert_eq!(pending, Some(7), "Should have 7 pending messages");

        // Test partitions
        let partitions = core_http_source.partitions().await.unwrap();
        assert_eq!(partitions.len(), 1, "Should have 1 partition");

        // Test read method - should get batch_size (5) messages
        let messages = core_http_source.read().await.unwrap();
        assert_eq!(messages.len(), 5, "Should read 5 messages (batch size)");

        // Verify message contents
        for (i, message) in messages.iter().enumerate() {
            assert!(message.headers.contains_key("X-Numaflow-Id"));
            assert!(message.headers.contains_key("X-Numaflow-Event-Time"));
            assert!(message.headers.contains_key("content-type"));

            let body_str = String::from_utf8(message.value.to_vec()).unwrap();
            assert!(body_str.contains(&format!("test{}", i)));
        }

        // Test pending count after reading
        let pending = core_http_source.pending().await.unwrap();
        assert_eq!(
            pending,
            Some(2),
            "Should have 2 pending messages after reading 5"
        );

        // Test ack method (should always succeed for HTTP source)
        let offsets = messages.iter().map(|m| m.offset.clone()).collect();
        let ack_result = core_http_source.ack(offsets).await;
        assert!(ack_result.is_ok(), "Ack should succeed");

        // Read remaining messages
        let messages = core_http_source.read().await.unwrap();
        assert_eq!(messages.len(), 2, "Should read remaining 2 messages");

        // Verify no more pending messages
        let pending = core_http_source.pending().await.unwrap();
        assert_eq!(pending, Some(0), "Should have 0 pending messages");
    }
}
