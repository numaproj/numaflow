use thiserror::Error;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Error, Debug, Clone)]
pub enum Error {
    #[error("Store Error - {0}")]
    Store(String),

    #[error("RedisStore Error - {0}")]
    RedisStore(String),

    #[error("CancellationToken Cancelled")]
    Cancellation,

    #[error("Redis Error - {0}")]
    Redis(String),
}
