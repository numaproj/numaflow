use thiserror::Error;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Error, Debug, Clone)]
pub enum Error {
    #[error("app_err handler Error - {0}")]
    AppErrHandler(String),

    #[error("file Error - {0}")]
    FileError(String),

    #[error("Init Error - {0}")]
    InitError(String),

    #[error("Router Error - {0}")]
    Router(String),
}
