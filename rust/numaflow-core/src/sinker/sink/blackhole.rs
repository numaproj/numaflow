use super::{ResponseFromSink, ResponseStatusFromSink, Sink};
use crate::message::Message;

/// Blackhole is a sink to emulate /dev/null
pub struct BlackholeSink;

impl Sink for BlackholeSink {
    async fn sink(&mut self, messages: Vec<Message>) -> crate::Result<Vec<ResponseFromSink>> {
        let output = messages
            .into_iter()
            .map(|msg| ResponseFromSink {
                status: ResponseStatusFromSink::Success,
                id: msg.id.to_string(),
            })
            .collect();
        Ok(output)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use chrono::Utc;

    use super::BlackholeSink;
    use crate::message::IntOffset;
    use crate::message::{Message, MessageID, Offset};
    use crate::sinker::sink::{ResponseFromSink, ResponseStatusFromSink, Sink};

    #[tokio::test]
    async fn test_black_hole() {
        let mut sink = BlackholeSink;
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
                status: ResponseStatusFromSink::Success,
                id: msg.id.to_string(),
            })
            .collect::<Vec<ResponseFromSink>>();

        let responses = sink.sink(messages).await.unwrap();
        assert_eq!(responses, expected_responses);
    }
}
