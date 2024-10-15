use crate::message::Message;
use crate::Result;
use async_nats::jetstream::context::PublishAckFuture;
use async_nats::jetstream::publish::PublishAck;
use async_nats::jetstream::Context;
use bytes::Bytes;
use std::time::Duration;
use tokio::time::sleep;
use tracing::error;

#[derive(Clone, Debug)]
/// Writes to JetStream ISB. Exposes both sync and async methods to write messages.
pub(super) struct JetstreamWriter {
    js_ctx: Context,
}

impl JetstreamWriter {
    pub(super) fn new(js_ctx: Context) -> Self {
        Self { js_ctx }
    }

    /// Writes the message to the JetStream ISB and returns a future which can be
    /// awaited to get the PublishAck. It will do infinite retries until the message
    /// gets published successfully. If it returns an error it means it is fatal
    /// error
    pub(super) async fn async_write(
        &self,
        stream: &'static str,
        msg: Message,
    ) -> Result<PublishAckFuture> {
        let js_ctx = self.js_ctx.clone();
        let payload = Bytes::from(
            msg.to_bytes()
                .expect("message serialization should not fail"),
        );
        // TODO: expose a way to exit the retry loop during shutdown
        loop {
            match js_ctx.publish(stream, payload.clone()).await {
                Ok(paf) => return Ok(paf),
                Err(e) => {
                    error!("publishing failed, retrying: {}", e);
                    sleep(Duration::from_millis(10)).await;
                }
            }
        }
    }

    /// Writes the message to the JetStream ISB and returns the PublishAck. It will do
    /// infinite retries until the message gets published successfully. If it returns
    /// an error it means it is fatal error
    pub(super) async fn sync_write(
        &self,
        stream: &'static str,
        msg: Message,
    ) -> Result<PublishAck> {
        let js_ctx = self.js_ctx.clone();
        let payload = Bytes::from(
            msg.to_bytes()
                .expect("message serialization should not fail"),
        );

        // TODO: expose a way to exit the retry loop during shutdown
        loop {
            match js_ctx.publish(stream, payload.clone()).await {
                Ok(paf) => match paf.await {
                    Ok(ack) => return Ok(ack),
                    Err(e) => {
                        error!("awaiting publish ack failed, retrying: {}", e);
                        sleep(Duration::from_millis(10)).await;
                    }
                },
                Err(e) => {
                    error!("publishing failed, retrying: {}", e);
                    sleep(Duration::from_millis(10)).await;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::message::{Message, Offset};
    use async_nats::jetstream;
    use async_nats::jetstream::stream;
    use chrono::Utc;
    use std::collections::HashMap;

    #[tokio::test]
    async fn test_async_write() {
        let js_url = "localhost:4222";
        // Create JetStream context
        let client = async_nats::connect(js_url).await.unwrap();
        let context = jetstream::new(client);

        let stream_name = "test_async";
        let _stream = context
            .get_or_create_stream(stream::Config {
                name: stream_name.into(),
                subjects: vec![stream_name.into()],
                ..Default::default()
            })
            .await
            .unwrap();

        let writer = JetstreamWriter::new(context.clone());

        let message = Message {
            keys: vec!["key_0".to_string()],
            value: "message 0".as_bytes().to_vec(),
            offset: Offset {
                offset: "offset_0".to_string(),
                partition_id: 0,
            },
            event_time: Utc::now(),
            id: "id_0".to_string(),
            headers: HashMap::new(),
        };

        let result = writer.async_write(stream_name, message.clone()).await;
        assert!(result.is_ok());

        let publish_ack_future = result.unwrap();
        let publish_ack = publish_ack_future.await;
        assert!(publish_ack.is_ok());

        context.delete_stream(stream_name).await.unwrap();
    }

    #[tokio::test]
    async fn test_sync_write() {
        let js_url = "localhost:4222";
        // Create JetStream context
        let client = async_nats::connect(js_url).await.unwrap();
        let context = jetstream::new(client);

        let stream_name = "test_sync";
        let _stream = context
            .get_or_create_stream(stream::Config {
                name: stream_name.into(),
                subjects: vec![stream_name.into()],
                ..Default::default()
            })
            .await
            .unwrap();

        let writer = JetstreamWriter::new(context.clone());

        let message = Message {
            keys: vec!["key_0".to_string()],
            value: "message 0".as_bytes().to_vec(),
            offset: Offset {
                offset: "offset_0".to_string(),
                partition_id: 1,
            },
            event_time: Utc::now(),
            id: "id_0".to_string(),
            headers: HashMap::new(),
        };

        let result = writer.sync_write(stream_name, message.clone()).await;
        assert!(result.is_ok());

        let publish_ack = result.unwrap();
        assert_eq!(publish_ack.stream, stream_name);

        context.delete_stream(stream_name).await.unwrap();
    }
}
