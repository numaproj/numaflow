//! Shared client-side protocol support for Numaflow UDFs.

pub mod client;
pub mod error;
pub mod model;
pub mod window;

mod wire;

pub use client::map::{
    BatchMapEvent, BatchMapSession, StreamMapReceiver, StreamMapSession, UnaryMapSession,
};
pub use client::reduce::{AlignedReduceBook, AlignedReduceReceiver};
pub use error::{Result, UdfClientError};
pub use model::{
    AlignedReduceResult, BatchMapResponse, KeyValueGroup, MapResult, StreamMapResponse, UdfDatum,
    UdfMetadata, UdfNackOptions, UnaryMapResponse, Window,
};
pub use window::aligned::{
    AlignedWindowAction, AlignedWindowManager, FixedWindowManager, SlidingWindowManager,
    SlidingWindowSnapshot, truncate_to_duration,
};
