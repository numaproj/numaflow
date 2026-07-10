//! Shared client-side protocol support for Numaflow UDFs.

pub mod client;
pub mod error;
pub mod model;

mod wire;

pub use client::map::{MapRpcStream, UnaryMapSession};
pub use error::{Result, UdfClientError};
pub use model::{
    KeyValueGroup, MapResult, UdfDatum, UdfMetadata, UdfNackOptions, UnaryMapResponse,
};
