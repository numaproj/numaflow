/*
Copyright 2022 The Numaproj Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Code generated by Openapi Generator. DO NOT EDIT.

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct KafkaSink {
    #[serde(rename = "brokers", skip_serializing_if = "Option::is_none")]
    pub brokers: Option<Vec<String>>,
    #[serde(rename = "config", skip_serializing_if = "Option::is_none")]
    pub config: Option<String>,
    #[serde(rename = "sasl", skip_serializing_if = "Option::is_none")]
    pub sasl: Option<Box<crate::models::Sasl>>,
    /// SetKey sets the Kafka key to the keys passed in the Message. When the key is null (default), the record is sent randomly to one of the available partitions of the topic. If a key exists, Kafka hashes the key, and the result is used to map the message to a specific partition. This ensures that messages with the same key end up in the same partition.
    #[serde(rename = "setKey", skip_serializing_if = "Option::is_none")]
    pub set_key: Option<bool>,
    #[serde(rename = "tls", skip_serializing_if = "Option::is_none")]
    pub tls: Option<Box<crate::models::Tls>>,
    #[serde(rename = "topic")]
    pub topic: String,
}

impl KafkaSink {
    pub fn new(topic: String) -> KafkaSink {
        KafkaSink {
            brokers: None,
            config: None,
            sasl: None,
            set_key: None,
            tls: None,
            topic,
        }
    }
}
