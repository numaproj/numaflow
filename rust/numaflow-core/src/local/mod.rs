//! Embeddable local runner: run a real production vertex forwarder against the in-memory ISB.
//!
//! This is the public facade the `nfcli` binary drives. It builds a [`crate::config::pipeline::PipelineConfig`]
//! with `ISBClientConfig::InMemory`, spawns the *production* `start_*_forwarder`, and lets the
//! caller feed input events / drain / read outputs — exercising the identical code paths a
//! deployed vertex runs. See `rust/numaflow-cli-v2.md` for the design.
//!
//! Everything here is feature-gated behind `local-runner` (default off), so the shipping binary's
//! dependency graph is unaffected.
//!
//! The whole module is `pub` because it is the crate's only public API besides `run()`; the
//! individual submodules keep their internals private.

use std::path::PathBuf;
use std::time::Duration;

mod config_builder;
mod events;
mod replay_source;
mod runner;
mod watermark_driver;

pub use events::{InputEvent, OutputEvent};
pub use runner::{DrainReport, LocalRun, LocalRunOpts};

/// Errors surfaced across the facade boundary.
///
/// The internal `crate::error::Error` is `pub(crate)` and must never leak in a public signature;
/// it is mapped to [`LocalError::Forwarder`] / [`LocalError::Internal`] at this boundary. Each
/// variant maps to a distinct CLI exit code (documented per variant).
#[derive(Debug, thiserror::Error)]
pub enum LocalError {
    #[error("configuration error: {0}")]
    Config(String),
    /// Socket / server-info / readiness not available within startup timeout → CLI exit 2.
    #[error("UDF server not reachable: {0}")]
    Startup(String),
    /// Input did not drain within the drain timeout → CLI exit 3.
    #[error("drain timed out: {0} of {1} messages still pending/in-flight")]
    DrainTimeout(usize, usize),
    /// The forwarder task ended with an error (fatal UDF failure) → CLI exit 4.
    #[error("UDF/forwarder failed: {0}")]
    Forwarder(String),
    #[error("{0}")]
    Internal(String),
}

pub type Result<T> = std::result::Result<T, LocalError>;

/// The UDF under test, together with the identity of its socket / server-info files and any
/// per-kind topology (window shape for reduce, fallback/on-success sinks for sink, …).
#[derive(Debug, Clone)]
pub enum LocalUdf {
    Map {
        socket_path: PathBuf,
        server_info_path: PathBuf,
    },
    Sink {
        socket_path: PathBuf,
        server_info_path: PathBuf,
        /// `(socket, server_info)` for a fallback sink, if the run should route failures to one.
        fallback: Option<(PathBuf, PathBuf)>,
        /// `(socket, server_info)` for an on-success sink, if configured.
        on_success: Option<(PathBuf, PathBuf)>,
    },
    Reduce {
        socket_path: PathBuf,
        server_info_path: PathBuf,
        window: LocalWindow,
        keyed: bool,
        allowed_lateness: Duration,
    },
    Transform {
        socket_path: PathBuf,
        server_info_path: PathBuf,
    },
    Source {
        socket_path: PathBuf,
        server_info_path: PathBuf,
    },
}

/// The reduce window shape. Fixed/Sliding map to the *aligned* windower; Session/Accumulator to
/// the *unaligned* windower.
#[derive(Debug, Clone, Copy)]
pub enum LocalWindow {
    Fixed { length: Duration },
    Sliding { length: Duration, slide: Duration },
    Session { gap: Duration },
    Accumulator { timeout: Duration },
}
