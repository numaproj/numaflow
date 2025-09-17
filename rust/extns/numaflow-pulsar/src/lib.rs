use tokio::sync::oneshot;

pub mod sink;
pub mod source;

pub type Result<T> = core::result::Result<T, Error>;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("metrics Error - {0}")]
    Pulsar(pulsar::Error),

    #[error("Messages to be acknowledged has reached its configured limit. Pending={0}")]
    AckPendingExceeded(usize),

    #[error("Failed to receive message from channel. Actor task is terminated: {0:?}")]
    ActorTaskTerminated(oneshot::error::RecvError),

    #[error("Received unknown offset for acknowledgement. offset={0}")]
    UnknownOffset(u64),

    #[error("{0}")]
    Other(String),
}

impl From<pulsar::Error> for Error {
    fn from(value: pulsar::Error) -> Self {
        Error::Pulsar(value)
    }
}

impl From<String> for Error {
    fn from(value: String) -> Self {
        Error::Other(value)
    }
}

#[derive(Clone, PartialEq)]
pub enum PulsarAuth {
    JWT(String),
    HTTPBasic { username: String, password: String },
}

impl std::fmt::Debug for PulsarAuth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PulsarAuth::JWT(token) => {
                let last_part = if token.len() <= 6 {
                    "***"
                } else {
                    &token[token.len() - 3..]
                };
                write!(f, "****{last_part}")
            }
            PulsarAuth::HTTPBasic { username, password } => {
                let last_part = if password.len() <= 3 {
                    "***"
                } else {
                    &password[password.len() - 3..]
                };
                write!(f, "username={username}, password=****{last_part}")
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_jwt_debug_implementation() {
        // Test with a typical JWT token
        let jwt_token = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIiwiaWF0IjoxNTE2MjM5MDIyfQ.SflKxwRJSMeKKF2QT4fwpMeJf36POk6yJV_adQssw5c";
        let auth = PulsarAuth::JWT(jwt_token.to_string());

        let debug_output = format!("{:?}", auth);
        assert_eq!(debug_output, "****w5c");
    }

    #[test]
    fn test_jwt_debug_short_token() {
        // Test with a short JWT token (less than 12 characters)
        let short_token = "short";
        let auth = PulsarAuth::JWT(short_token.to_string());

        let debug_output = format!("{:?}", auth);
        // Should still work even with short tokens
        assert_eq!(debug_output, "*******");
    }

    #[test]
    fn test_jwt_debug_exact_length() {
        // Test with exactly 12 characters
        let exact_token = "123456789012";
        let auth = PulsarAuth::JWT(exact_token.to_string());

        let debug_output = format!("{:?}", auth);
        assert_eq!(debug_output, "****012");
    }

    #[test]
    fn test_http_basic_debug_implementation() {
        // Test with typical username and password
        let auth = PulsarAuth::HTTPBasic {
            username: "testuser".to_string(),
            password: "secretpassword123".to_string(),
        };

        let debug_output = format!("{:?}", auth);
        assert_eq!(debug_output, "username=testuser, password=****123");
    }

    #[test]
    fn test_http_basic_debug_short_password() {
        // Test with short password (less than 3 characters)
        let auth = PulsarAuth::HTTPBasic {
            username: "admin".to_string(),
            password: "ab".to_string(),
        };

        let debug_output = format!("{:?}", auth);
        // Should still work even with short passwords
        assert_eq!(debug_output, "username=admin, password=*******");
    }

    #[test]
    fn test_http_basic_debug_exact_password_length() {
        // Test with exactly 3 characters password
        let auth = PulsarAuth::HTTPBasic {
            username: "user".to_string(),
            password: "xyz".to_string(),
        };

        let debug_output = format!("{:?}", auth);
        assert_eq!(debug_output, "username=user, password=*******");
    }

    #[test]
    fn test_http_basic_debug_empty_username() {
        // Test with empty username
        let auth = PulsarAuth::HTTPBasic {
            username: "".to_string(),
            password: "mypassword".to_string(),
        };

        let debug_output = format!("{:?}", auth);
        assert_eq!(debug_output, "username=, password=****ord");
    }

    #[test]
    fn test_http_basic_debug_empty_password() {
        // Test with empty password
        let auth = PulsarAuth::HTTPBasic {
            username: "testuser".to_string(),
            password: "".to_string(),
        };

        let debug_output = format!("{:?}", auth);
        assert_eq!(debug_output, "username=testuser, password=*******");
    }

    #[test]
    fn test_http_basic_debug_special_characters() {
        // Test with special characters in username and password
        let auth = PulsarAuth::HTTPBasic {
            username: "user@domain.com".to_string(),
            password: "p@ssw0rd!".to_string(),
        };

        let debug_output = format!("{:?}", auth);
        assert_eq!(debug_output, "username=user@domain.com, password=****rd!");
    }
}
