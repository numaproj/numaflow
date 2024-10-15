use thiserror::Error;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Error, Debug, Clone)]
pub enum Error {
    #[error("Metrics Error - {0}")]
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

    #[error("ServerInfoError Error - {0}")]
    ServerInfo(String),
}

impl From<tonic::Status> for Error {
    fn from(status: tonic::Status) -> Self {
        Error::Grpc(status.to_string())
    }
}
