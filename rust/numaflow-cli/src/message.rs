//! The canonical, fully-resolved test message that the protocol drivers send on the wire,
//! plus time parsing (`+dur` relative and RFC3339 absolute).

use std::collections::HashMap;

use anyhow::{Context, anyhow};
use chrono::{DateTime, Utc};
use numaflow_udf_client::{KeyValueGroup, UdfDatum, UdfMetadata};
use prost_types::Timestamp;

/// A fully-resolved message ready to send. All defaults have been applied.
#[derive(Debug, Clone)]
pub struct Message {
    pub id: String,
    pub keys: Vec<String>,
    pub value: Vec<u8>,
    pub event_time: DateTime<Utc>,
    pub watermark: DateTime<Utc>,
    pub headers: HashMap<String, String>,
    /// user_metadata: group -> (key -> value string).
    pub user_metadata: HashMap<String, HashMap<String, String>>,
    pub previous_vertex: String,
}

impl Message {
    /// Build the proto [`Metadata`] if any user metadata / previous vertex is set, else None.
    pub fn metadata(&self) -> Option<numaflow_pb::common::metadata::Metadata> {
        use numaflow_pb::common::metadata::{KeyValueGroup, Metadata};
        if self.user_metadata.is_empty() && self.previous_vertex.is_empty() {
            return None;
        }
        let user_metadata = self
            .user_metadata
            .iter()
            .map(|(group, kvs)| {
                let key_value = kvs
                    .iter()
                    .map(|(k, v)| (k.clone(), v.clone().into_bytes()))
                    .collect();
                (group.clone(), KeyValueGroup { key_value })
            })
            .collect();
        Some(Metadata {
            previous_vertex: self.previous_vertex.clone(),
            sys_metadata: HashMap::new(),
            user_metadata,
        })
    }

    pub fn event_time_pb(&self) -> Timestamp {
        to_timestamp(self.event_time)
    }

    pub fn watermark_pb(&self) -> Timestamp {
        to_timestamp(self.watermark)
    }

    /// Convert this CLI message into a shared [`UdfDatum`] for map/reduce drivers.
    pub fn to_udf_datum(&self) -> UdfDatum {
        let metadata = if self.user_metadata.is_empty() && self.previous_vertex.is_empty() {
            None
        } else {
            Some(UdfMetadata {
                previous_vertex: self.previous_vertex.clone(),
                sys_metadata: Default::default(),
                user_metadata: self
                    .user_metadata
                    .iter()
                    .map(|(group, values)| {
                        (
                            group.clone(),
                            KeyValueGroup {
                                key_value: values
                                    .iter()
                                    .map(|(key, value)| {
                                        (key.clone(), value.clone().into_bytes().into())
                                    })
                                    .collect(),
                            },
                        )
                    })
                    .collect(),
            })
        };

        UdfDatum {
            id: self.id.clone(),
            keys: self.keys.clone(),
            value: self.value.clone().into(),
            event_time: self.event_time,
            watermark: Some(self.watermark),
            headers: self.headers.clone(),
            metadata,
        }
    }
}

/// Convert a chrono UTC time to a prost [`Timestamp`].
pub fn to_timestamp(t: DateTime<Utc>) -> Timestamp {
    Timestamp {
        seconds: t.timestamp(),
        nanos: t.timestamp_subsec_nanos() as i32,
    }
}

/// Convert a prost [`Timestamp`] back to chrono UTC (for printing response times).
pub fn from_timestamp(t: &Timestamp) -> Option<DateTime<Utc>> {
    DateTime::from_timestamp(t.seconds, t.nanos.max(0) as u32)
}

/// Parse a time spec that is either an RFC3339 timestamp or a `+dur` relative offset from
/// `base`, e.g. `+90s`, `+1m30s`.
pub fn parse_time(spec: &str, base: DateTime<Utc>) -> anyhow::Result<DateTime<Utc>> {
    let spec = spec.trim();
    if let Some(rest) = spec.strip_prefix('+') {
        let dur = humantime::parse_duration(rest)
            .with_context(|| format!("invalid relative time {spec:?}"))?;
        let chrono_dur = chrono::Duration::from_std(dur)
            .with_context(|| format!("relative time {spec:?} out of range"))?;
        return base
            .checked_add_signed(chrono_dur)
            .ok_or_else(|| anyhow!("relative time {spec:?} overflows"));
    }
    // Absolute RFC3339.
    let parsed = DateTime::parse_from_rfc3339(spec)
        .with_context(|| format!("invalid RFC3339 time {spec:?} (expected +dur or RFC3339)"))?;
    Ok(parsed.with_timezone(&Utc))
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn base() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 7, 6, 0, 0, 0).unwrap()
    }

    #[test]
    fn relative_seconds() {
        let t = parse_time("+90s", base()).unwrap();
        assert_eq!(t, base() + chrono::Duration::seconds(90));
    }

    #[test]
    fn relative_composite() {
        let t = parse_time("+1m30s", base()).unwrap();
        assert_eq!(t, base() + chrono::Duration::seconds(90));
    }

    #[test]
    fn absolute_rfc3339() {
        let t = parse_time("2026-07-06T01:02:03Z", base()).unwrap();
        assert_eq!(t, Utc.with_ymd_and_hms(2026, 7, 6, 1, 2, 3).unwrap());
    }

    #[test]
    fn bad_time_errors() {
        assert!(parse_time("not-a-time", base()).is_err());
        assert!(parse_time("+garbage", base()).is_err());
    }

    #[test]
    fn timestamp_roundtrip() {
        let t = Utc.with_ymd_and_hms(2026, 7, 6, 1, 2, 3).unwrap();
        let pb = to_timestamp(t);
        assert_eq!(from_timestamp(&pb).unwrap(), t);
    }
}
