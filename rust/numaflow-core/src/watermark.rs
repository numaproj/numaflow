use crate::error::Error;
use bytes::{Bytes, BytesMut};
use prost::Message as ProtoMessage;

mod fetcher;
mod manager;
mod timeline;

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct WMB {
    pub idle: bool,
    pub offset: i64,
    pub watermark: i64,
    pub partition: i32,
}

impl TryFrom<Bytes> for WMB {
    type Error = Error;

    fn try_from(bytes: Bytes) -> Result<Self, Self::Error> {
        let proto_wmb = numaflow_pb::objects::wmb::Wmb::decode(bytes)
            .map_err(|e| Error::Proto(e.to_string()))?;

        Ok(WMB {
            idle: proto_wmb.idle,
            offset: proto_wmb.offset,
            watermark: proto_wmb.watermark,
            partition: proto_wmb.partition,
        })
    }
}

impl TryFrom<WMB> for BytesMut {
    type Error = Error;

    fn try_from(wmb: WMB) -> Result<Self, Self::Error> {
        let mut bytes = BytesMut::new();
        let proto_wmb = numaflow_pb::objects::wmb::Wmb {
            idle: wmb.idle,
            offset: wmb.offset,
            watermark: wmb.watermark,
            partition: wmb.partition,
        };

        proto_wmb
            .encode(&mut bytes)
            .map_err(|e| Error::Proto(e.to_string()))?;

        Ok(bytes)
    }
}
