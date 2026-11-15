use secrecy::SecretString;
use tokio::sync::oneshot;

pub mod sink;
pub mod source;

use crate::source::PulsarOffset;

pub use numaflow_shared::tls::TlsConfig;

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
    UnknownOffset(PulsarOffset),

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

#[derive(Clone, Debug)]
pub enum PulsarAuth {
    JWT(SecretString),
    HTTPBasic {
        username: String,
        password: SecretString,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn jwt_debug_redacts_token() {
        let jwt_token = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIiwiaWF0IjoxNTE2MjM5MDIyfQ.SflKxwRJSMeKKF2QT4fwpMeJf36POk6yJV_adQssw5c";
        let auth = PulsarAuth::JWT(jwt_token.into());
        for output in [format!("{auth:?}"), format!("{auth:#?}")] {
            assert!(!output.contains(jwt_token));
            assert!(output.contains("REDACTED"));
        }
    }

    #[test]
    fn http_basic_debug_redacts_password() {
        let auth = PulsarAuth::HTTPBasic {
            username: "testuser".to_string(),
            password: "secretpassword123".into(),
        };
        for output in [format!("{auth:?}"), format!("{auth:#?}")] {
            assert!(!output.contains("secretpassword123"));
            assert!(output.contains("testuser"));
            assert!(output.contains("REDACTED"));
        }
    }

    #[test]
    fn derived_debug_redacts_nested_secrets() {
        let config = source::PulsarSourceConfig {
            pulsar_server_addr: "pulsar://broker-user:secret-url@localhost:6650"
                .parse()
                .unwrap(),
            topic: "topic".into(),
            consumer_name: "consumer".into(),
            subscription: "subscription".into(),
            max_unack: 100,
            dead_letter_policy: None,
            auth: Some(PulsarAuth::HTTPBasic {
                username: "user".into(),
                password: "secret-password".into(),
            }),
            tls: Some(TlsConfig {
                insecure_skip_verify: false,
                ca_cert: Some("secret-ca".into()),
            }),
        };
        for output in [format!("{config:?}"), format!("{config:#?}")] {
            assert!(!output.contains("secret-"));
            assert!(output.contains("localhost:6650"));
            assert!(output.contains("topic"));
        }
    }
}
