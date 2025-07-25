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
pub struct Sink {
    #[serde(rename = "blackhole", skip_serializing_if = "Option::is_none")]
    pub blackhole: Option<Box<crate::models::Blackhole>>,
    #[serde(rename = "fallback", skip_serializing_if = "Option::is_none")]
    pub fallback: Option<Box<crate::models::AbstractSink>>,
    #[serde(rename = "kafka", skip_serializing_if = "Option::is_none")]
    pub kafka: Option<Box<crate::models::KafkaSink>>,
    #[serde(rename = "log", skip_serializing_if = "Option::is_none")]
    pub log: Option<Box<crate::models::Log>>,
    #[serde(rename = "pulsar", skip_serializing_if = "Option::is_none")]
    pub pulsar: Option<Box<crate::models::PulsarSink>>,
    #[serde(rename = "retryStrategy", skip_serializing_if = "Option::is_none")]
    pub retry_strategy: Option<Box<crate::models::RetryStrategy>>,
    #[serde(rename = "serve", skip_serializing_if = "Option::is_none")]
    pub serve: Option<Box<crate::models::ServeSink>>,
    #[serde(rename = "sqs", skip_serializing_if = "Option::is_none")]
    pub sqs: Option<Box<crate::models::SqsSink>>,
    #[serde(rename = "udsink", skip_serializing_if = "Option::is_none")]
    pub udsink: Option<Box<crate::models::UdSink>>,
}

impl Sink {
    pub fn new() -> Sink {
        Sink {
            blackhole: None,
            fallback: None,
            kafka: None,
            log: None,
            pulsar: None,
            retry_strategy: None,
            serve: None,
            sqs: None,
            udsink: None,
        }
    }
}
