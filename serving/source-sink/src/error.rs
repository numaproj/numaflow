use thiserror::Error;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Error, Debug, Clone)]
pub enum Error {
    #[error("Metrics Error - {0}")]
    MetricsError(String),

    #[error("Source Error - {0}")]
    SourceError(String),

    #[error("Sink Error - {0}")]
    SinkError(String),

    #[error("Transformer Error - {0}")]
    TransformerError(String),

    #[error("Forwarder Error - {0}")]
    ForwarderError(String),

    #[error("Connection Error - {0}")]
    ConnectionError(String),
}

impl From<tonic::Status> for Error {
    fn from(status: tonic::Status) -> Self {
        Error::SourceError(status.to_string())
    }
}
