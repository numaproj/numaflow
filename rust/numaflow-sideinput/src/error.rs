use thiserror::Error;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Error, Debug, Clone)]
pub enum Error {
    #[error("Schedule Error - {0}")]
    Schedule(String),

    #[error("SideInput Error - {0}")]
    SideInput(String),

    #[error("Connection Error - {0}")]
    Connection(String),

    #[error("Config Error - {0}")]
    Config(String),

    #[error("Error in Shared - {0}")]
    Shared(numaflow_shared::error::Error),
}

impl From<numaflow_shared::error::Error> for Error {
    fn from(value: numaflow_shared::error::Error) -> Self {
        Error::Shared(value)
    }
}
