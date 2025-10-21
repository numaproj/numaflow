//! The [Sink] serves as the endpoint for processed data that has been outputted from the platform,
//! which is then sent to an external system or application.
//!
//! The [SinkWriter] orchestrates writing messages to the sink, handling retries, fallback sinks,
//! and serving stores.
//!
//! The [SinkWriter] uses an actor-based pattern where:
//!   - [SinkActor] handles the actual sink operations with retry logic
//!   - [SinkWriter] orchestrates batching, fallback handling, and serving store writes
//!
//! Error handling and shutdown: There can be non-retryable errors (udsink panics etc.), in that case
//! we will cancel the token to indicate the upstream not to send any more messages to the sink, we drain
//! any inflight messages that are in the input stream and nack them using the tracker, when the upstream
//! stops sending messages the input stream will be closed, and we will stop the component.
//!
//! [Sink]: https://numaflow.numaproj.io/user-guide/sinks/overview/
//! [SinkWriter]: sink::SinkWriter
//! [SinkActor]: actor::SinkActor
pub(crate) mod sink;

mod actor;

mod builder;
