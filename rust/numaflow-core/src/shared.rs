/// All utilities related to gRPC.
pub(crate) mod grpc;

/// Start metrics servers, pending readers, and possible other metrics related helpers.
pub(crate) mod metrics;

/// Shared methods for creating Sources, Sinks, Transformers, etc. as they are required for both
/// MonoVertex and Pipeline.
pub(crate) mod create_components;

/// Shared methods for forwarding messages.
pub(crate) mod forward;

/// OpenTelemetry propagation carrier for sys_metadata (traceparent / tracestate).
pub(crate) mod otel;

/// Test server framework: helpers for spinning up numaflow SDK gRPC servers in tests.
#[cfg(test)]
pub(crate) mod test_utils;
