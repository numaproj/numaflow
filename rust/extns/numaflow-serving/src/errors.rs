use tokio::sync::oneshot;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Initialization error - {0}")]
    InitError(String),

    #[error("Failed to parse configuration - {0}")]
    ParseConfig(String),
    //
    // callback errors
    // TODO: store the ID too?
    #[error("IDNotFound Error - {0}")]
    IDNotFound(&'static str),

    #[error("SubGraphGenerator Error - {0}")]
    // subgraph generator errors
    SubGraphGenerator(String),

    #[error("StoreWrite Error - {0}")]
    // Store write errors
    StoreWrite(String),

    #[error("SubGraphNotFound Error - {0}")]
    // Sub Graph Not Found Error
    SubGraphNotFound(&'static str),

    #[error("SubGraphInvalidInput Error - {0}")]
    // Sub Graph Invalid Input Error
    SubGraphInvalidInput(String),

    #[error("StoreRead Error - {0}")]
    // Store read errors
    StoreRead(String),

    #[error("Metrics Error - {0}")]
    // Metrics errors
    MetricsServer(String),

    #[error("Connection Error - {0}")]
    Connection(String),

    #[error("Failed to receive message from channel. Actor task is terminated: {0:?}")]
    ActorTaskTerminated(oneshot::error::RecvError),

    #[error("{0}")]
    Other(String),
}

pub type Result<T> = std::result::Result<T, Error>;
