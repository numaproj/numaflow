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
}

impl From<tonic::Status> for Error {
    fn from(status: tonic::Status) -> Self {
        Error::Grpc(status.to_string())
    }
}

impl From<numaflow_pulsar::Error> for Error {
    fn from(value: numaflow_pulsar::Error) -> Self {
        match value {
            numaflow_pulsar::Error::Pulsar(e) => Error::Source(e.to_string()),
            numaflow_pulsar::Error::UnknownOffset(_) => Error::Source(value.to_string()),
            numaflow_pulsar::Error::AckPendingExceeded(pending) => {
                Error::AckPendingExceeded(pending)
            }
            numaflow_pulsar::Error::ActorTaskTerminated(_) => {
                Error::ActorPatternRecv(value.to_string())
            }
            numaflow_pulsar::Error::Other(e) => Error::Source(e),
        }
    }
}
