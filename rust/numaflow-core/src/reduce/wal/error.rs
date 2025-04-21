use std::fmt::{Display, Formatter};
use std::io;

pub(crate) type WalResult<T> = Result<T, Error>;

#[derive(Debug)]
pub(crate) enum Error {
    /// Error while writing a particular WAL entry
    WriteEntry {
        id: String,
        err: String,
    },
    Io(io::Error),
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::WriteEntry { id, err } => {
                write!(f, "failed to write {id} due to {err}")
            }
            Error::Io(err) => {
                write!(f, "failed to write due to {err}")
            }
        }
    }
}

impl From<io::Error> for Error {
    fn from(value: io::Error) -> Self {
        Error::Io(value)
    }
}
