//! Process exit codes, matching the design doc §8.

/// The CLI's meaningful exit codes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum ExitCode {
    /// Completed; all responses received, no UDF-reported failures.
    Ok = 0,
    /// Usage error (bad flags, bad file, missing payload, …).
    Usage = 1,
    /// Connect / IsReady / handshake failure within `--timeout`.
    Connect = 2,
    /// Protocol error (gRPC error mid-stream, missing response at EOT, response timeout).
    Protocol = 3,
    /// Protocol OK but the UDF reported failures (e.g. sink FAILURE results).
    UdfFailure = 4,
}

impl ExitCode {
    pub fn code(self) -> i32 {
        self as i32
    }
}

/// A CLI error carrying the exit code it should map to. Anything bubbling up as a plain
/// `anyhow::Error` without a category is treated as [`ExitCode::Protocol`].
#[derive(Debug)]
pub struct CliError {
    pub code: ExitCode,
    pub source: anyhow::Error,
}

/// ANSI SGR codes for colouring the uncaught-exception banner.
const RED: &str = "\x1b[31m";
const RESET: &str = "\x1b[0m";

impl CliError {
    pub fn new(code: ExitCode, source: impl Into<anyhow::Error>) -> Self {
        CliError {
            code,
            source: source.into(),
        }
    }
    pub fn usage(source: impl Into<anyhow::Error>) -> Self {
        CliError::new(ExitCode::Usage, source)
    }
    pub fn connect(source: impl Into<anyhow::Error>) -> Self {
        CliError::new(ExitCode::Connect, source)
    }
    pub fn protocol(source: impl Into<anyhow::Error>) -> Self {
        CliError::new(ExitCode::Protocol, source)
    }

    /// Print this error to stderr. A UDF uncaught exception (a gRPC error mid-stream, i.e. a
    /// [`tonic::Status`] in the source chain) gets the dedicated red banner; everything else
    /// uses the plain `error: …` line.
    ///
    /// `verbose` additionally surfaces the raw gRPC status; `color` enables ANSI red (callers
    /// pass `false` when stderr is not a terminal).
    pub fn render(&self, verbose: bool, color: bool) {
        let Some(status) = self.source.downcast_ref::<tonic::Status>() else {
            eprintln!("error: {self}");
            return;
        };
        if status.code() == tonic::Code::Unimplemented {
            eprintln!("error: {self}");
            return;
        }
        let (red, reset) = if color { (RED, RESET) } else { ("", "") };
        eprintln!("{red}Uncaught Error/Exception from UDF:{reset}");
        eprintln!("{red}      {}{reset}", status.message());
        if verbose {
            eprintln!("      status: {}", status.code());
        }
    }
}

impl std::fmt::Display for CliError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:#}", self.source)
    }
}

impl std::error::Error for CliError {}

pub type CliResult<T> = Result<T, CliError>;
