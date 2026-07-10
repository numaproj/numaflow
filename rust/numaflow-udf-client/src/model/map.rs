use bytes::Bytes;

use super::UdfMetadata;

/// Correlated response for one unary map request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnaryMapResponse {
    pub id: String,
    pub results: Vec<MapResult>,
}

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
