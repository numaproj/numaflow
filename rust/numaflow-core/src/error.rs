use std::error::Error as StdError;
use std::io::ErrorKind;

use thiserror::Error;
use tonic::Code;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Error, Debug, Clone)]
pub enum Error {
    #[error("metrics Error - {0}")]
    Metrics(String),

    #[error("Source Error - {0}")]
    Source(String),

    #[error("Sink Error - {0}")]
    Sink(String),

    #[error("Fallback Sink Error - {0}")]
    FbSink(String),

    #[error("OnSuccess Sink Error - {0}")]
    OsSink(String),

    #[error("Transformer Error - {0}")]
    Transformer(String),

    #[error("Mapper Error - {0}")]
    Mapper(String),

    #[error("Forwarder Error - {0}")]
    Forwarder(String),

    #[error("Bypass Router Error - {0}")]
    BypassRouter(String),

    #[error("Connection Error - {0}")]
    Connection(String),

    #[error("gRPC Error - {0}")]
    Grpc(Box<tonic::Status>),

    /// A UDF stream broke and the in-flight call should be redriven after the reconnect owner
    /// publishes its readiness signal.
    #[error("UDF redrive - {0}")]
    UdfRedrive(Box<tonic::Status>),

    #[error("Config Error - {0}")]
    Config(String),

    #[error("Error in Shared - {0}")]
    Shared(numaflow_shared::error::Error),

    #[error("Proto Error - {0}")]
    Proto(String),

    #[allow(clippy::upper_case_acronyms)]
    #[error("ISB Error - {0}")]
    ISB(crate::pipeline::isb::error::ISBError),

    #[error("OneShot Receiver Error - {0}")]
    ActorPatternRecv(String),

    #[error("Ack Pending Exceeded, pending={0}")]
    AckPendingExceeded(usize),

    #[error("Offset (id={0}) not found to Ack")]
    AckOffsetNotFound(String),

    #[error("Lag cannot be fetched, {0}")]
    Lag(String),

    #[error("Task Error - {0}")]
    Tracker(String),

    #[error("Duplicate inflight offset - {0}")]
    DuplicateInflight(String),

    #[error("Watermark Error - {0}")]
    Watermark(String),

    #[error("SideInput Error - {0}")]
    SideInput(String),

    #[error("Reduce Error - {0}")]
    Reduce(String),

    #[error("Cancellation Token Cancelled")]
    Cancelled(),

    #[error("WAL Error - {0}")]
    #[allow(clippy::upper_case_acronyms)]
    WAL(String),

    #[error("Non Retryable Error - {0}")]
    NonRetryable(String),
}

impl From<numaflow_shared::error::Error> for Error {
    fn from(value: numaflow_shared::error::Error) -> Self {
        Error::Shared(value)
    }
}

const UDF_TRANSPORT_IO_ERROR_KINDS: &[ErrorKind] = &[
    ErrorKind::BrokenPipe,
    ErrorKind::ConnectionReset,
    ErrorKind::NotConnected,
];

/// Classifies whether a `tonic::Status` represents a UDF transport break.
///
/// A status is considered transport-level when:
/// - its `Code` is `Unavailable`, `Cancelled`, `Aborted`, or `DeadlineExceeded`; or
/// - its underlying I/O error chain contains a `BrokenPipe`, `ConnectionReset`, or `NotConnected`
///   kind. This last case captures `Code::Unknown` / `Code::Internal` wrappers around hyper/h2
///   transport errors that don't classify cleanly by status code alone.
///
/// Callers can use this to decide when to convert a UDF stream error into [`Error::UdfRedrive`].
/// It does not classify all UDF application errors that should be handled without exiting `numa`.
#[allow(dead_code)]
pub(crate) fn is_udf_transport_failure(status: &tonic::Status) -> bool {
    matches!(
        status.code(),
        Code::Unavailable | Code::Cancelled | Code::Aborted | Code::DeadlineExceeded
    ) || has_io_kind_in_chain(status, UDF_TRANSPORT_IO_ERROR_KINDS)
}

/// Walks the `Error::source` chain looking for a `std::io::Error` whose `kind()` matches any of
/// `kinds`.
#[allow(dead_code)]
pub(crate) fn has_io_kind_in_chain(err: &(dyn StdError + 'static), kinds: &[ErrorKind]) -> bool {
    let mut current: Option<&(dyn StdError + 'static)> = Some(err);
    while let Some(e) = current {
        if let Some(ioe) = e.downcast_ref::<std::io::Error>()
            && kinds.contains(&ioe.kind())
        {
            return true;
        }
        current = e.source();
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io;
    use tonic::Code;

    #[test]
    fn transport_codes_classify_as_transport() {
        for code in [
            Code::Unavailable,
            Code::Cancelled,
            Code::Aborted,
            Code::DeadlineExceeded,
        ] {
            let status = tonic::Status::new(code, "x");
            assert!(
                is_udf_transport_failure(&status),
                "expected {code:?} to be transport-classified"
            );
        }
    }

    #[test]
    fn non_transport_codes_do_not_classify_as_transport() {
        for code in [
            Code::InvalidArgument,
            Code::FailedPrecondition,
            Code::Unimplemented,
            Code::DataLoss,
        ] {
            let status = tonic::Status::new(code, "x");
            assert!(
                !is_udf_transport_failure(&status),
                "expected {code:?} not to be transport-classified"
            );
        }
    }

    #[test]
    fn io_kind_in_chain_finds_direct_match() {
        let io_err = io::Error::new(io::ErrorKind::BrokenPipe, "broken pipe");
        assert!(has_io_kind_in_chain(&io_err, &[ErrorKind::BrokenPipe]));
        assert!(!has_io_kind_in_chain(&io_err, &[ErrorKind::TimedOut]));
    }

    #[derive(Debug, thiserror::Error)]
    #[error("wrapped I/O error")]
    struct WrappedIoError(#[source] io::Error);

    #[test]
    fn io_kind_in_chain_finds_nested_match() {
        let io_err = io::Error::new(io::ErrorKind::ConnectionReset, "connection reset");
        let wrapped = WrappedIoError(io_err);
        assert!(has_io_kind_in_chain(
            &wrapped,
            &[ErrorKind::ConnectionReset]
        ));
    }
}
