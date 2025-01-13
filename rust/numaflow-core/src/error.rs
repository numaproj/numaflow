use thiserror::Error;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Error, Debug, Clone)]
pub enum Error {
    #[error("metrics Error - {0}")]
    Metrics(String),

    #[error("Source Error - {0}")]
    Source(String),

    #[error("Sink Error - {0}")]
    Sink(String),

    #[error("Transformer Error - {0}")]
    Transformer(String),

    #[error("Mapper Error - {0}")]
    Mapper(String),

    #[error("Forwarder Error - {0}")]
    Forwarder(String),

    #[error("Connection Error - {0}")]
    Connection(String),

    #[error("gRPC Error - {0}")]
    Grpc(String),

    #[error("Config Error - {0}")]
    Config(String),

    #[error("ServerInfo Error - {0}")]
    ServerInfo(String),

    #[error("Proto Error - {0}")]
    Proto(String),

    #[allow(clippy::upper_case_acronyms)]
    #[error("ISB Error - {0}")]
    ISB(String),

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
}

impl From<tonic::Status> for Error {
    fn from(status: tonic::Status) -> Self {
        Error::Grpc(status.to_string())
    }
}
