//! CLI error type and exit-code mapping.

use numaflow_core::local::LocalError;

/// Errors surfaced by the CLI. Each maps to a documented process exit code (see [`CliError::exit_code`]).
#[derive(Debug, thiserror::Error)]
pub enum CliError {
    /// Usage / validation error → exit 1.
    #[error("{0}")]
    Usage(String),
    /// Errors from the embedded facade (mapped to 2/3/4 per variant).
    #[error(transparent)]
    Local(#[from] LocalError),
    /// A non-facade command (side-input / ready) failed → exit 2 or 3 depending on kind.
    #[error("{0}")]
    Command(String),
    /// Readiness / connection not available → exit 2.
    #[error("{0}")]
    NotReady(String),
}

pub type CliResult<T> = std::result::Result<T, CliError>;

impl CliError {
    /// Process exit code, keeping the v1 contract:
    /// - 1 usage/validation
    /// - 2 startup / not-reachable
    /// - 3 drain timeout / internal
    /// - 4 UDF/forwarder fatal error
    pub fn exit_code(&self) -> i32 {
        match self {
            CliError::Usage(_) => 1,
            CliError::NotReady(_) => 2,
            CliError::Command(_) => 3,
            CliError::Local(e) => match e {
                LocalError::Config(_) => 1,
                LocalError::Startup(_) => 2,
                LocalError::DrainTimeout(_, _) => 3,
                LocalError::Forwarder(_) => 4,
                LocalError::Internal(_) => 3,
            },
        }
    }
}
