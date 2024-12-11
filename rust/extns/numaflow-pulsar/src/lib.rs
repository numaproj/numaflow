use tokio::sync::oneshot;

pub mod source;

pub type Result<T> = core::result::Result<T, Error>;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("metrics Error - {0}")]
    Pulsar(pulsar::Error),

    #[error("Messages to be acknowledged has reached its configured limit. Pending={0}")]
    AckPendingExceeded(usize),

    #[error("Failed to receive message from channel. Actor task is terminated: {0:?}")]
    ActorTaskTerminated(oneshot::error::RecvError),

    #[error("Received unknown offset for acknowledgement. offset={0}")]
    UnknownOffset(u64),

    #[error("{0}")]
    Other(String),
}

impl From<pulsar::Error> for Error {
    fn from(value: pulsar::Error) -> Self {
        Error::Pulsar(value)
    }
}

impl From<String> for Error {
    fn from(value: String) -> Self {
        Error::Other(value)
    }
}
