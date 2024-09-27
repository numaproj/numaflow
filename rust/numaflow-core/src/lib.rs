extern crate core;

use tracing::error;

pub(crate) use self::error::Result;

/// SourcerSinker orchestrates data movement from the Source to the Sink via the optional SourceTransformer.
/// The forward-a-chunk executes the following in an infinite loop till a shutdown signal is received:
/// - Read X messages from the source
/// - Invokes the SourceTransformer concurrently
/// - Calls the Sinker to write the batch to the Sink
/// - Send Acknowledgement back to the Source
mod error;
pub(crate) use crate::error::Error;
pub mod monovertex;
pub use crate::monovertex::mono_vertex;

mod config;

mod forwarder;
mod message;
mod metrics;
mod server_info;
mod shared;
mod sink;
mod source;
mod startup;
mod transformer;

