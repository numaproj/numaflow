use bytes::Bytes;

use super::UdfMetadata;

/// Correlated response for one map request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnaryMapResponse {
    pub id: String,
    pub results: Vec<MapResult>,
}

/// Correlated response for one datum in a batch map request.
///
/// Unary and batch map use the same response envelope on the wire. The alias keeps that shared
/// representation explicit without introducing a second model that could drift.
pub type BatchMapResponse = UnaryMapResponse;

/// One result chunk for a streaming map request.
///
/// Stream map uses the same response envelope as unary and batch map. The stream ends when the
/// shared client observes the per-ID EOT frame and closes the corresponding response channel.
pub type StreamMapResponse = UnaryMapResponse;

/// One output produced by a map UDF.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MapResult {
    pub keys: Vec<String>,
    pub value: Bytes,
    pub tags: Vec<String>,
    pub metadata: Option<UdfMetadata>,
    pub nack_options: Option<UdfNackOptions>,
}

/// UDF-provided message redelivery options.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UdfNackOptions {
    pub reason: Option<String>,
    pub max_deliveries: Option<u32>,
    pub delay: Option<u64>,
}
