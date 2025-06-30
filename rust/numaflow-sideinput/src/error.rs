use thiserror::Error;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Error, Debug, Clone)]
pub enum Error {
    #[error("SideInput Error - {0}")]
    SideInput(String),

    #[error("Connection Error - {0}")]
    Connection(String),
}
