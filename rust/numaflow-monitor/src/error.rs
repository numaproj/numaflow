use serde::Serialize;
use thiserror::Error;

#[derive(Serialize)]
pub struct ErrorRes {
    pub error: String,
}

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Error, Debug, Clone)]
pub enum Error {
    #[error("file Error - {0}")]
    File(String),

    #[error("Init Error - {0}")]
    Init(String),

    #[error("Router Error - {0}")]
    Router(String),

    #[error("Deserialization Error - {0}")]
    Deserialize(String),
}
