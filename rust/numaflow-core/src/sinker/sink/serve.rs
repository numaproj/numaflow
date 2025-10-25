use crate::error;
use crate::message::Message;
use crate::sinker::sink::{ResponseFromSink, ResponseStatusFromSink, Sink};
use bytes::Bytes;

/// User defined serving store to store the serving responses.
#[path = "serve/user_defined.rs"]
pub(crate) mod user_defined;

/// Nats serving store to store the serving responses.
#[path = "serve/nats.rs"]
pub(crate) mod nats;

/// Enum to represent different types of serving stores.
#[derive(Clone)]
pub(crate) enum ServingStore {
    UserDefined(user_defined::UserDefinedStore),
    Nats(Box<nats::NatsServingStore>),
}

/// Entry in the serving store.
#[derive(Clone, Debug)]
pub(crate) struct StoreEntry {
    /// Pod Hash is for filtering the stream by each request originating pod while listening for
    /// "put" onto the KV store. This is used only on ISB KV store and this enables SSE (converse is
    /// that SSE is not supported on user defined store).
    pub(crate) pod_hash: String,
    /// Unique ID Of the request to which the result belongs. There could be multiple results for
    /// the same request.
    pub(crate) id: String,
    /// The result of the computation.
    pub(crate) value: Bytes,
}

/// Builtin Sink to write to the serving store.
#[derive(Debug, Default, Clone)]
pub(crate) struct ServeSink;

impl Sink for ServeSink {
    async fn sink(&mut self, messages: Vec<Message>) -> error::Result<Vec<ResponseFromSink>> {
        let mut result = Vec::with_capacity(messages.len());
        for msg in messages {
            result.push(ResponseFromSink {
                id: msg.id.to_string(),
                status: ResponseStatusFromSink::Serve(None),
            })
        }
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use chrono::Utc;

    use crate::message::IntOffset;
    use crate::message::{Message, MessageID, Offset};
    use crate::sinker::sink::serve::ServeSink;
    use crate::sinker::sink::{ResponseFromSink, ResponseStatusFromSink, Sink};

    #[tokio::test]
    async fn test_serve_sink() {
        let mut sink = ServeSink;
        let messages = vec![
            Message {
                typ: Default::default(),
                keys: Arc::from(vec![]),
                tags: None,
                value: b"Hello, World!".to_vec().into(),
                offset: Offset::Int(IntOffset::new(1, 0)),
                event_time: Utc::now(),
                headers: Default::default(),
                id: MessageID {
                    vertex_name: "vertex".to_string().into(),
                    offset: "1".to_string().into(),
                    index: 0,
                },
                ..Default::default()
            },
            Message {
                typ: Default::default(),
                keys: Arc::from(vec![]),
                tags: None,
                value: b"Hello, World!".to_vec().into(),
                offset: Offset::Int(IntOffset::new(1, 0)),
                event_time: Utc::now(),
                headers: Default::default(),
                id: MessageID {
                    vertex_name: "vertex".to_string().into(),
                    offset: "2".to_string().into(),
                    index: 1,
                },
                ..Default::default()
            },
        ];

        let expected_responses = messages
            .iter()
            .map(|msg| ResponseFromSink {
                status: ResponseStatusFromSink::Serve(None),
                id: msg.id.to_string(),
            })
            .collect::<Vec<ResponseFromSink>>();

        let responses = sink.sink(messages).await.unwrap();
        assert_eq!(responses, expected_responses);
    }
}
