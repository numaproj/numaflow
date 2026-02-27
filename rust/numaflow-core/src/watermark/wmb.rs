use bytes::{Bytes, BytesMut};
use chrono::{DateTime, Utc};
use prost::Message;

use crate::error::Error;

/// WMB is the watermark message that is sent by the processor to the downstream.
/// It now includes hb_time to track processor liveness, eliminating the need for
/// a separate heartbeat store.
#[derive(Clone, Copy, Debug, PartialEq)]
#[allow(clippy::upper_case_acronyms)]
pub(crate) struct WMB {
    pub(crate) idle: bool,
    pub(crate) offset: i64,
    pub(crate) watermark: i64,
    pub(crate) partition: u16,
    /// Heartbeat timestamp (epoch milliseconds) to track processor liveness.
    pub(crate) hb_time: i64,
}

impl Default for WMB {
    fn default() -> Self {
        Self {
            watermark: -1,
            offset: -1,
            idle: false,
            partition: 0,
            hb_time: 0,
        }
    }
}

/// Watermark is a monotonically increasing time.
pub(crate) type Watermark = DateTime<Utc>;

/// Converts a protobuf bytes to WMB.
impl TryFrom<Bytes> for WMB {
    type Error = Error;

    fn try_from(bytes: Bytes) -> Result<Self, Self::Error> {
        let proto_wmb = numaflow_pb::objects::watermark::Wmb::decode(bytes)
            .map_err(|e| Error::Proto(e.to_string()))?;

        Ok(WMB {
            idle: proto_wmb.idle,
            offset: proto_wmb.offset,
            watermark: proto_wmb.watermark,
            partition: proto_wmb.partition as u16,
            hb_time: proto_wmb.hb_time,
        })
    }
}

/// Converts WMB to protobuf bytes.
impl TryFrom<WMB> for BytesMut {
    type Error = Error;

    fn try_from(wmb: WMB) -> Result<Self, Self::Error> {
        let proto_wmb = numaflow_pb::objects::watermark::Wmb {
            idle: wmb.idle,
            offset: wmb.offset,
            watermark: wmb.watermark,
            partition: wmb.partition as i32,
            hb_time: wmb.hb_time,
        };

        let mut bytes = BytesMut::with_capacity(proto_wmb.encoded_len());

        proto_wmb
            .encode(&mut bytes)
            .map_err(|e| Error::Proto(e.to_string()))?;

        Ok(bytes)
    }
}
