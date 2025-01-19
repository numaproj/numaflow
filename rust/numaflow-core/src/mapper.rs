//! Numaflow supports flatmap operation through [map::MapHandle] an actor interface.
//!
//! The [map::MapHandle] orchestrates reading messages from the input stream, invoking the map operation,
//! and sending the mapped messages to the output stream.
//!
//! The [map::MapHandle] reads messages from the input stream and invokes the map operation based on the
//! mode:
//!   - Unary: Concurrent operations controlled using permits and `tokio::spawn`.
//!   - Batch: Synchronous operations, one batch at a time, followed by an invoke.
//!   - Stream: Concurrent operations controlled using permits and `tokio::spawn`, followed by an
//!        invoke.
//!
//! Error handling in unary and stream operations with concurrency N:
//! ```text
//! (Read) <----- (error_tx) <-------- +
//!  |                                 |
//!  + -->-- (tokio map task 1) -->--- +
//!  |                                 |
//!  + -->-- (tokio map task 2) -->--- +
//!  |                                 |
//!  :                                 :
//!  |                                 |
//!  + -->-- (tokio map task N) -->--- +
//! ```
//! In case of errors in unary/stream, tasks will write to the error channel (`error_tx`), and the `MapHandle`
//! will stop reading new requests and return an error.
//!
//! Error handling in batch operation is easier because it is synchronous and one batch at a time. If there
//! is an error, the [map::MapHandle] will stop reading new requests and return an error.

pub(crate) mod map;
