use tracing::error;

/// Custom Error handling.
mod error;
pub(crate) use crate::error::{Error, Result};

/// MonoVertex is a simplified version of the [Pipeline] spec which is ideal for high TPS, low latency
/// use-cases which do not require [ISB].
///
/// [Pipeline]: https://numaflow.numaproj.io/core-concepts/pipeline/
/// [ISB]: https://numaflow.numaproj.io/core-concepts/inter-step-buffer/
pub mod monovertex;
pub use crate::monovertex::mono_vertex;

/// Parse configs, including Numaflow specifications.
mod config;

/// Internal message structure that is passed around.
mod message;
/// Shared entities that can be used orthogonal to different modules.
mod shared;
/// [Sink] serves as the endpoint for processed data that has been outputted from the platform,
/// which is then sent to an external system or application.
///
/// [Sink]: https://numaflow.numaproj.io/user-guide/sinks/overview/
mod sink;
/// [Source] is responsible for reliable reading data from an unbounded source into Numaflow.
///
/// [Source]: https://numaflow.numaproj.io/user-guide/sources/overview/
mod source;
/// Transformer is a feature that allows users to execute custom code to transform their data at
/// [source].
///
/// [Transformer]: https://numaflow.numaproj.io/user-guide/sources/transformer/overview/
mod transformer;

/// Reads from a stream.
mod reader;
