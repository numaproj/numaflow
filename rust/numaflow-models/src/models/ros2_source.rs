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

// ROS 2 source — not auto-generated; added manually for issue #3684.

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Ros2Source {
    /// ROS 2 topic to subscribe to, e.g. `/robot/telemetry`.
    #[serde(rename = "topic")]
    pub topic: String,

    /// ROS 2 message type string, e.g. `sensor_msgs/msg/LaserScan`.
    /// Required; must match the publisher's advertised type exactly.
    /// A mismatch causes silence with no error.
    #[serde(rename = "msgType")]
    pub msg_type: String,

    /// Name of the ROS 2 node that Numaflow registers.
    /// Defaults to `"numaflow_source"` when not set.
    #[serde(rename = "nodeName", skip_serializing_if = "Option::is_none")]
    pub node_name: Option<String>,

    /// ROS 2 namespace for the node. Defaults to the root namespace (`""`).
    #[serde(rename = "namespace", skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,

    /// How long to wait for messages before returning an empty batch, in
    /// milliseconds. Defaults to `1000` (1 second).
    #[serde(rename = "readTimeoutMs", skip_serializing_if = "Option::is_none")]
    pub read_timeout_ms: Option<i64>,

    /// Maximum number of messages per read batch. Defaults to `500`.
    #[serde(rename = "batchSize", skip_serializing_if = "Option::is_none")]
    pub batch_size: Option<i64>,

    /// Internal channel capacity between the ROS spin thread and the async
    /// actor. Increase for bursty publishers. Defaults to `256`.
    #[serde(rename = "channelCapacity", skip_serializing_if = "Option::is_none")]
    pub channel_capacity: Option<i64>,

    /// Emit a warning when no message has arrived for this many milliseconds.
    /// Helps detect silent failures (discovery never completed, publisher
    /// gone, QoS incompatibility). `0` or absent disables the warning.
    #[serde(rename = "silenceWarnAfterMs", skip_serializing_if = "Option::is_none")]
    pub silence_warn_after_ms: Option<i64>,
}

impl Ros2Source {
    pub fn new(topic: String, msg_type: String) -> Ros2Source {
        Ros2Source {
            topic,
            msg_type,
            node_name: None,
            namespace: None,
            read_timeout_ms: None,
            batch_size: None,
            channel_capacity: None,
            silence_warn_after_ms: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ros2_source_required_field_only() {
        let s = Ros2Source::new("/robot/vel".into(), "geometry_msgs/msg/Twist".into());
        assert_eq!(s.topic, "/robot/vel");
        assert_eq!(s.msg_type, "geometry_msgs/msg/Twist");
        assert!(s.node_name.is_none());
        assert!(s.namespace.is_none());
        assert!(s.read_timeout_ms.is_none());
        assert!(s.batch_size.is_none());
    }

    #[test]
    fn ros2_source_all_fields_round_trip_json() {
        let s = Ros2Source {
            topic: "/sensor/lidar".into(),
            msg_type: "sensor_msgs/msg/LaserScan".into(),
            node_name: Some("lidar_node".into()),
            namespace: Some("/fleet".into()),
            read_timeout_ms: Some(500),
            batch_size: Some(200),
            channel_capacity: Some(512),
            silence_warn_after_ms: Some(5000),
        };
        let json = serde_json::to_string(&s).expect("serialize");
        let back: Ros2Source = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(back, s);
    }

    #[test]
    fn ros2_source_serde_uses_camel_case_keys() {
        let s = Ros2Source {
            topic: "/t".into(),
            msg_type: "std_msgs/msg/String".into(),
            node_name: Some("n".into()),
            namespace: Some("/ns".into()),
            read_timeout_ms: Some(100),
            batch_size: Some(10),
            channel_capacity: Some(64),
            silence_warn_after_ms: Some(3000),
        };
        let json = serde_json::to_string(&s).expect("serialize");
        assert!(json.contains("\"topic\""));
        assert!(json.contains("\"msgType\""));
        assert!(json.contains("\"nodeName\""));
        assert!(json.contains("\"namespace\""));
        assert!(json.contains("\"readTimeoutMs\""));
        assert!(json.contains("\"batchSize\""));
        assert!(json.contains("\"channelCapacity\""));
        assert!(json.contains("\"silenceWarnAfterMs\""));
    }

    #[test]
    fn ros2_source_deserialize_minimal_json() {
        let json = r#"{"topic":"/imu","msgType":"sensor_msgs/msg/Imu"}"#;
        let s: Ros2Source = serde_json::from_str(json).expect("deserialize minimal");
        assert_eq!(s.topic, "/imu");
        assert_eq!(s.msg_type, "sensor_msgs/msg/Imu");
        assert!(s.node_name.is_none());
        assert!(s.batch_size.is_none());
    }

    #[test]
    fn ros2_source_empty_topic_is_allowed_at_model_layer() {
        let s = Ros2Source::new("".into(), "std_msgs/msg/String".into());
        assert_eq!(s.topic, "");
    }
}
