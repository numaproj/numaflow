use std::fmt::{Display, Formatter};
use std::io;
use thiserror::Error;

pub(crate) type WalResult<T> = Result<T, Error>;

#[derive(Debug)]
pub(crate) enum Error {
    /// Error while writing a particular WAL entry
    WriteEntry {
        id: String,
        err: String,
    },
    Io(io::Error),
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
