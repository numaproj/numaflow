use thiserror::Error;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Error, Debug, Clone)]
pub enum Error {
    #[error("ServerInfo Error - {0}")]
    ServerInfo(String),

    #[error("Connection Error - {0}")]
    Connection(String),

    #[error("Config Error - {0}")]
    Config(String),

    #[error("Jetstream Error - {0}")]
    Jetstream(String),
}
