use crate::{
    error,
    message::{Message, ResponseFromSink, ResponseStatusFromSink},
    sink::Sink,
};

pub struct LogSink {
    vertex_name: String,
}

impl Sink for LogSink {
    async fn sink(&mut self, messages: Vec<Message>) -> error::Result<Vec<ResponseFromSink>> {
        let mut result = Vec::with_capacity(messages.len());
        for msg in messages {
            let mut headers = String::new();
            msg.headers.iter().for_each(|(k, v)| {
                headers.push_str(&format!("{}: {}, ", k, v));
            });

            let log_line = format!(
                "({}) Payload - {} Keys - {} EventTime - {} Headers - {} ID - {}",
                self.vertex_name,
                &String::from_utf8_lossy(&msg.value),
                msg.keys.join(","),
                msg.event_time.timestamp_millis(),
                headers,
                msg.id,
            );
            tracing::info!("{}", log_line);
            result.push(ResponseFromSink {
                id: msg.id,
                status: ResponseStatusFromSink::Success,
            })
        }
        Ok(result)
    }
}
