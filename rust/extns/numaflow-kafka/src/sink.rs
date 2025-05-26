use bytes::Bytes;
use futures::{StreamExt, stream::FuturesUnordered};
use rdkafka::{
    ClientConfig,
    config::RDKafkaLogLevel,
    message::{Header, OwnedHeaders},
    producer::{FutureProducer, FutureRecord},
};
use std::{collections::HashMap, time::Duration};

use crate::{KafkaSaslAuth, TlsConfig};

#[derive(Debug, Clone, PartialEq)]
pub struct KafkaSinkConfig {
    pub brokers: Vec<String>,
    pub topic: String,
    /// The authentication mechanism to use for the Kafka consumer.
    pub auth: Option<KafkaSaslAuth>,
    /// The TLS configuration for the Kafka consumer.
    pub tls: Option<TlsConfig>,
    /// Whether to set the partition key for the Kafka sink.
    pub set_partition_key: bool,
}

pub struct KafkaSink {
    topic: String,
    producer: FutureProducer,
    set_partition_key: bool,
}

pub struct KafkaSinkResponse {
    /// ID of the message that was sent
    pub id: String,
    /// Status of the send operation
    pub status: crate::Result<()>,
}

pub struct KafkaSinkMessage {
    pub id: String,
    pub partition_key: Option<String>,
    pub headers: HashMap<String, String>,
    pub payload: Bytes,
}

pub fn new_sink(config: KafkaSinkConfig) -> crate::Result<KafkaSink> {
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", config.brokers.join(","))
        .set("message.timeout.ms", "5000")
        .set("security.protocol", "PLAINTEXT")
        .set("client.id", "numaflow-kafka-sink")
        .set_log_level(RDKafkaLogLevel::Warning)
        .create()
        .map_err(|e| crate::Error::Kafka(format!("Failed to create producer: {}", e)))?;

    Ok(KafkaSink {
        producer,
        topic: config.topic,
        set_partition_key: config.set_partition_key,
    })
}

impl KafkaSink {
    pub async fn sink_messages(
        &mut self,
        messages: Vec<KafkaSinkMessage>,
    ) -> crate::Result<Vec<KafkaSinkResponse>> {
        let mut send_futures = FuturesUnordered::new();
        let message_count = messages.len();
        for msg in messages {
            let fut = async {
                let KafkaSinkMessage {
                    id,
                    partition_key,
                    headers: inp_headers,
                    payload,
                } = msg;
                let mut headers = OwnedHeaders::new();
                for (key, value) in inp_headers {
                    headers = headers.insert(Header {
                        key: &key,
                        value: Some(&value),
                    });
                }
                let mut record: FutureRecord<'_, String, _> = FutureRecord::to(&self.topic)
                    .headers(headers)
                    .payload(payload.as_ref());
                if self.set_partition_key {
                    if let Some(ref partition_key) = partition_key {
                        record = record.key(partition_key);
                    }
                }
                match self.producer.send(record, Duration::from_secs(1)).await {
                    Ok(_) => KafkaSinkResponse { id, status: Ok(()) },
                    Err(e) => {
                        tracing::error!(?e, "Sending payload to Kafka topic");
                        KafkaSinkResponse {
                            id,
                            status: Err(crate::Error::Kafka(format!(
                                "Sending payload to kafka: {e:?}"
                            ))),
                        }
                    }
                }
            };
            send_futures.push(fut);
        }
        let mut results = Vec::with_capacity(message_count);
        while let Some(status) = send_futures.next().await {
            results.push(status);
        }
        Ok(results)
    }
}
