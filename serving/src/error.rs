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

// code coverage tests.
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_display() {
        let e = Error::ConfigMissingEnv("TEST_ENV");
        assert_eq!(format!("{}", e), "ConfigMissingEnv(\"TEST_ENV\")");

        let e = Error::IDNotFound("ID");
        assert_eq!(format!("{}", e), "IDNotFound(\"ID\")");

        let e = Error::SubGraphGeneratorError("Error message".into());
        assert_eq!(format!("{}", e), "SubGraphGeneratorError(\"Error message\")");

        let e = Error::StoreWrite("Error message".into());
        assert_eq!(format!("{}", e), "StoreWrite(\"Error message\")");

        let e = Error::SubGraphNotFound("SubGraph");
        assert_eq!(format!("{}", e), "SubGraphNotFound(\"SubGraph\")");

        let e = Error::SubGraphInvalidInput("Invalid input".into());
        assert_eq!(format!("{}", e), "SubGraphInvalidInput(\"Invalid input\")");

        let e = Error::StoreRead("Error message".into());
        assert_eq!(format!("{}", e), "StoreRead(\"Error message\")");

        let e = Error::Other("Other error".into());
        assert_eq!(format!("{}", e), "Other(\"Other error\")");
    }

    #[test]
    fn test_error_from_string() {
        let e: Error = "Error message".into();
        assert_eq!(format!("{}", e), "Other(\"Error message\")");
    }
}