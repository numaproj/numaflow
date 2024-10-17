use crate::{
    error,
    message::{Message, ResponseFromSink, ResponseStatusFromSink},
    sink::Sink,
};

pub(crate) struct LogSink;

impl Sink for LogSink {
    async fn sink(&mut self, messages: Vec<Message>) -> error::Result<Vec<ResponseFromSink>> {
        let mut result = Vec::with_capacity(messages.len());
        for msg in messages {
            let mut headers = String::new();
            msg.headers.iter().for_each(|(k, v)| {
                headers.push_str(&format!("{}: {}, ", k, v));
            });

            let log_line = format!(
                "Payload - {} Keys - {} EventTime - {} Headers - {} ID - {}",
                &String::from_utf8_lossy(&msg.value),
                msg.keys.join(","),
                msg.event_time.timestamp_millis(),
                headers,
                msg.id,
            );
            tracing::info!("{}", log_line);
            result.push(ResponseFromSink {
                id: msg.id.to_string(),
                status: ResponseStatusFromSink::Success,
            })
        }
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use chrono::Utc;

    use super::LogSink;
    use crate::message::IntOffset;
    use crate::{
        message::{Message, MessageID, Offset, ResponseFromSink, ResponseStatusFromSink},
        sink::Sink,
    };

    #[tokio::test]
    async fn test_log_sink() {
        let mut sink = LogSink;
        let messages = vec![
            Message {
                keys: vec![],
                value: b"Hello, World!".to_vec(),
                offset: Some(Offset::Int(IntOffset::new(1, 0))),
                event_time: Utc::now(),
                headers: Default::default(),
                id: MessageID {
                    vertex_name: "vertex".to_string(),
                    offset: "1".to_string(),
                    index: 0,
                },
            },
            Message {
                keys: vec![],
                value: b"Hello, World!".to_vec(),
                offset: Some(Offset::Int(IntOffset::new(1, 0))),
                event_time: Utc::now(),
                headers: Default::default(),
                id: MessageID {
                    vertex_name: "vertex".to_string(),
                    offset: "2".to_string(),
                    index: 1,
                },
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
