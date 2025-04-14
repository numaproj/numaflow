use crate::sink::{ResponseFromSink, ResponseStatusFromSink, Sink};
use crate::{error, message::Message};

#[derive(Debug, Default, Clone)]
pub(crate) struct ServeSink;

impl Sink for ServeSink {
    async fn sink(&mut self, messages: Vec<Message>) -> error::Result<Vec<ResponseFromSink>> {
        let mut result = Vec::with_capacity(messages.len());
        for msg in messages {
            result.push(ResponseFromSink {
                id: msg.id.to_string(),
                status: ResponseStatusFromSink::Serve,
                serve_response: Some(msg.value.into()),
            })
        }
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use chrono::Utc;

    use super::ServeSink;
    use crate::message::IntOffset;
    use crate::message::{Message, MessageID, Offset};
    use crate::sink::{ResponseFromSink, ResponseStatusFromSink, Sink};

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
                watermark: None,
                metadata: None,
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
                watermark: None,
                metadata: None,
            },
        ];

        let expected_responses = messages
            .iter()
            .map(|msg| ResponseFromSink {
                status: ResponseStatusFromSink::Serve,
                id: msg.id.to_string(),
                serve_response: Some(msg.value.clone().into()),
            })
            .collect::<Vec<ResponseFromSink>>();

        let responses = sink.sink(messages).await.unwrap();
        assert_eq!(responses, expected_responses);
    }
}
