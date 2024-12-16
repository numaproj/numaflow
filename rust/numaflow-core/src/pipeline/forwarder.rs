//! The forwarder for [Pipeline] at its core orchestrates message movement asynchronously using
//! [Stream] over channels between the components. The messages send over this channel using
//! [Actor Pattern].
//!
//! ```text
//! (source) --[c]--> (transformer)* --[c]--> ==> (map)* --[c]--> ===> (reducer)* --[c]--> ===> --[c]--> (sink)
//!    |                   |                       |                      |                                |
//!    |                   |                       |                      |                                |
//!    |                   |                       v                      |                                |
//!    +-------------------+------------------> tracker <-----------------+--------------------------------+
//!
//!
//! ==> - ISB
//! [c] - channel
//!   * - optional
//!  ```
//!
//! Most of the data move forward except for the `ack` which can happen only after the that the tracker
//! has guaranteed that the processing complete.
//! ```text
//! (Read) +-------> (UDF) -------> (Write) +
//!        |                                |
//!        |                                |
//!        +-------> {Ack} <----------------+
//!
//! {} -> Listens on a OneShot
//! () -> Streaming Interface
//! ```
//!
//! [Pipeline]: https://numaflow.numaproj.io/core-concepts/pipeline/
//! [Stream]: https://docs.rs/tokio-stream/latest/tokio_stream/wrappers/struct.ReceiverStream.html
//! [Actor Pattern]: https://ryhl.io/blog/actors-with-tokio/

/// Forwarder specific to Sink where reader is ISB, UDF is not present, while
/// the Write is User-defined Sink or builtin.
pub(crate) mod sink_forwarder;

/// Forwarder specific to Mapper where Reader is ISB, UDF is User-defined Mapper,
/// Write is ISB.
pub(crate) mod map_forwarder;

/// Source where the Reader is builtin or User-defined Source, Write is ISB,
/// with an optional Transformer.
pub(crate) mod source_forwarder;
