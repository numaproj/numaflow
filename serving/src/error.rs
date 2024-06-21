pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    ConfigMissingEnv(&'static str),

    // callback errors
    // TODO: store the ID too?
    IDNotFound(&'static str),

    // subgraph generator errors
    SubGraphGeneratorError(String),

    // Store write errors
    StoreWrite(String),

    // Sub Graph Not Found Error
    SubGraphNotFound(&'static str),

    // Sub Graph Invalid Input Error
    SubGraphInvalidInput(String),

    // Store read errors
    StoreRead(String),

    // catch-all variant for now
    Other(String),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // reuse the debug implementation for now
        write!(f, "{self:?}")
    }
}

impl std::error::Error for Error {}

impl<T> From<T> for Error
where
    T: Into<String>,
{
    fn from(value: T) -> Self {
        Error::Other(value.into())
    }
}
