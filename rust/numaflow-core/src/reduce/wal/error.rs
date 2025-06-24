use std::fmt::{Display, Formatter};
use std::io;

/// The result of any operation on the WAL.
pub(crate) type WalResult<T> = Result<T, Error>;

#[derive(Debug)]
pub(crate) enum Error {
    #[allow(dead_code)]
    /// Error while writing a particular WAL entry.
    WriteEntry {
        /// The unique message ID. This can be used for NACK if needed.
        id: String,
        /// Reason of the error.
        err: String,
    },
    /// IO Errors.
    Io(io::Error),
    /// Other errors.
    Other(String),
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::WriteEntry { id, err } => {
                write!(f, "Error::WriteEntry {id} {err}")
            }
            Error::Io(err) => {
                write!(f, "Error::Io {err}")
            }
            Error::Other(err) => {
                write!(f, "Error::Other {err}")
            }
        }
    }
}

impl From<io::Error> for Error {
    fn from(value: io::Error) -> Self {
        Error::Io(value)
    }
}

impl From<String> for Error {
    fn from(value: String) -> Self {
        Error::Other(value)
    }
}

impl From<Error> for crate::error::Error {
    fn from(value: Error) -> Self {
        crate::error::Error::WAL(value.to_string())
    }
}
