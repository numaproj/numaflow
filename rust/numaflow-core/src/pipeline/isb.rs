//! [Vertex] are connected via [ISB].
//!
//! [Vertex]: https://numaflow.numaproj.io/core-concepts/vertex/
//! [ISB]: https://numaflow.numaproj.io/core-concepts/inter-step-buffer/

// TODO: implement a simple ISB and a trait for ISB

pub(crate) mod compression;
pub(crate) mod jetstream;
pub(crate) mod reader;
pub(crate) mod writer;
