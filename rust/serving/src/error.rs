use thiserror::Error;
use tokio::sync::oneshot;

// TODO: introduce module level error handling

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Error, Debug, Clone)]
pub enum Error {
    #[error("ParseConfig Error - {0}")]
    ParseConfig(String),

    // callback errors
    // TODO: store the ID too?
    #[error("IDNotFound Error - {0}")]
    IDNotFound(&'static str),

    #[error("SubGraphGenerator Error - {0}")]
    // subgraph generator errors
    SubGraphGenerator(String),

    #[error("Store - {0}")]
    // Store operation errors
    Store(String),

    #[error("SubGraphNotFound Error - {0}")]
    // Sub Graph Not Found Error
    SubGraphNotFound(&'static str),

    #[error("SubGraphInvalidInput Error - {0}")]
    // Sub Graph Invalid Input Error
    SubGraphInvalidInput(String),

    #[error("Metrics Error - {0}")]
    // Metrics errors
    MetricsServer(String),

    #[error("Connection Error - {0}")]
    Connection(String),

    #[error("Init Error - {0}")]
    InitError(String),

    #[error("Failed to receive message from channel. Actor task is terminated: {0:?}")]
    ActorTaskTerminated(oneshot::error::RecvError),

    #[error("Serving source error - {0}")]
    Source(String),

    #[error("Other Error - {0}")]
    // catch-all variant for now
    Other(String),
}
