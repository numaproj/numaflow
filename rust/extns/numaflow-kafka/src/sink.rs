use std::collections::HashMap;
use bytes::Bytes;

#[derive(Debug, Clone, PartialEq)]
pub struct KafkaSinkConfig {
    pub brokers: Vec<String>,
}

pub struct KafkaSink {}

pub struct KafkaSinkResponse {
    /// ID of the message that was sent
    pub id: String,
    /// Status of the send operation
    pub status: crate::Result<()>,
}

pub struct KafkaSinkMessage{
    pub id: String,
    pub headers: HashMap<String, String>,
    pub payload: Bytes,
}

pub fn new_sink(config: KafkaSinkConfig) -> KafkaSink {
    KafkaSink {}
}

impl KafkaSink {
    pub async fn sink_messages(&mut self, messges: Vec<KafkaSinkMessage>) -> crate::Result<Vec<KafkaSinkResponse>> {
        todo!();
    }
}
