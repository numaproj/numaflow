//! Shared client-side protocol support for Numaflow UDFs.

pub mod client;
pub mod error;
pub mod model;

mod wire;

pub use client::map::{
    BatchMapEvent, BatchMapSession, StreamMapReceiver, StreamMapSession, UnaryMapSession,
};
pub use error::{Result, UdfClientError};
pub use model::{
    BatchMapResponse, KeyValueGroup, MapResult, StreamMapResponse, UdfDatum, UdfMetadata,
    UdfNackOptions, UnaryMapResponse,
};
