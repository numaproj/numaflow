//! numaflow-ros2 — ROS 2 source for Numaflow pipelines.
//!
//! # Feature flags
//! * `ros2` — enables the actual ROS 2 subscriber.  Requires a sourced ROS 2
//!   environment (i.e. `source /opt/ros/<distro>/setup.bash`) at **build time**
//!   as well as at run time, because the `r2r` crate links against the RCL C
//!   libraries.  Without this flag the crate compiles and the public types are
//!   present, but calling [`Ros2Source::connect`] returns
//!   [`Error::Ros2NotEnabled`].
//! * `ros2-tests` — implies `ros2`; gates the integration tests.

pub mod source;

pub type Result<T> = core::result::Result<T, Error>;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    /// Returned when the binary was compiled without the `ros2` feature.
    #[error("numaflow-ros2 was compiled without the 'ros2' feature; rebuild with --features ros2")]
    Ros2NotEnabled,

    /// Error creating the ROS 2 context or node.
    #[error("ROS 2 node error: {0}")]
    Node(String),

    /// Error subscribing to a topic.
    #[error("Failed to subscribe to ROS 2 topic '{topic}': {error}")]
    Subscribe { topic: String, error: String },

    /// Non-retryable runtime error (e.g. the node has been shut down).
    #[error("Non-retryable ROS 2 error: {0}")]
    NonRetryable(String),

    #[error("{0}")]
    Other(String),
}
