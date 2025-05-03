use crate::reduce::wal;
use std::fmt::{Display, Formatter};

/// The result of any operation on the WAL.
pub(crate) type ReduceResult<T> = Result<T, Error>;

#[derive(Debug)]
pub(crate) enum Error {
    #[allow(clippy::upper_case_acronyms)]
    WAL(wal::error::Error),
    /// Other errors.
    Other(String),
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::Other(err) => {
                write!(f, "Error::Other {err}")
            }
            Error::WAL(err) => {
                write!(f, "Error::WAL {err}")
            }
        }
    }
}

impl From<String> for Error {
    fn from(value: String) -> Self {
        Error::Other(value)
    }
}

impl From<wal::error::Error> for Error {
    fn from(value: wal::error::Error) -> Self {
        Error::WAL(value)
    }
}
