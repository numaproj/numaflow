use bytes::{Bytes, BytesMut};
use chrono::{DateTime, Utc};
use prost::Message;

use crate::error::Error;

/// WMB is the watermark message that is sent by the processor to the downstream.
#[derive(Clone, Copy, Debug, PartialEq)]
#[allow(clippy::upper_case_acronyms)]
pub(in crate::watermark) struct WMB {
    pub idle: bool,
    pub offset: i64,
    pub watermark: i64,
    pub partition: u16,
}

impl Default for WMB {
    fn default() -> Self {
        Self {
            watermark: -1,
            offset: -1,
            idle: false,
            partition: 0,
        }
    }
}

/// Watermark is a monotonically increasing time.
pub(in crate::watermark) type Watermark = DateTime<Utc>;

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
        })
    }
}

/// Converts WMB to protobuf bytes.
impl TryFrom<WMB> for BytesMut {
    type Error = Error;

    fn try_from(wmb: WMB) -> Result<Self, Self::Error> {
        let mut bytes = BytesMut::new();
        let proto_wmb = numaflow_pb::objects::watermark::Wmb {
            idle: wmb.idle,
            offset: wmb.offset,
            watermark: wmb.watermark,
            partition: wmb.partition as i32,
        };

        proto_wmb
            .encode(&mut bytes)
            .map_err(|e| Error::Proto(e.to_string()))?;

        Ok(bytes)
    }
}
