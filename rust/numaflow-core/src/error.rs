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
}

impl From<tonic::Status> for Error {
    fn from(status: tonic::Status) -> Self {
        Error::Grpc(status.to_string())
    }
}
