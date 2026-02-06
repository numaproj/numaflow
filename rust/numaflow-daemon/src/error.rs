use thiserror::Error;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Error, Debug, Clone)]
pub enum Error {
    #[error("Task Completion Error - {0}")]
    Completion(String),

    #[error("Connection Configuration Error - {0}")]
    ConnConfig(String),
}
