
use tokio::sync::oneshot;
pub mod source;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("metrics Error - {0}")]
    SQS(aws_sdk_sqs::Error),

    #[error("Failed to receive message from channel. Actor task is terminated: {0:?}")]
    ActorTaskTerminated(oneshot::error::RecvError),

    #[error("Received unknown offset for acknowledgement. offset={0}")]
    UnknownOffset(u64),

    #[error("{0}")]
    Other(String),
}

pub type Result<T> = core::result::Result<T, Error>;

impl From<aws_sdk_sqs::Error> for Error {
    fn from(value: aws_sdk_sqs::Error) -> Self {
        Error::SQS(value)
    }
}

impl From<String> for Error {
    fn from(value: String) -> Self {
        Error::Other(value)
    }
}
