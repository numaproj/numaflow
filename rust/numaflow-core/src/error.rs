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

    #[error("Connection Error - {0}")]
    Connection(String),

    #[error("gRPC Error - {0}")]
    Grpc(Box<tonic::Status>),

    #[error("Config Error - {0}")]
    Config(String),

    #[error("Error in Shared - {0}")]
    Shared(numaflow_shared::error::Error),

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
}

impl From<numaflow_shared::error::Error> for Error {
    fn from(value: numaflow_shared::error::Error) -> Self {
        Error::Shared(value)
    }
}
