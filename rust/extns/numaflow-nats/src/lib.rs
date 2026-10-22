use numaflow_shared::redacted_url::RedactedUrl;
use secrecy::SecretString;

pub mod jetstream;
pub mod nats;

mod tls;

pub type Result<T> = core::result::Result<T, Error>;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Connecting to NATS {server:?} - {error}")]
    Connection {
        server: Vec<RedactedUrl>,
        error: String,
    },

    #[error("Subscribing to NATS {subject} - {queue} - {error}")]
    Subscription {
        subject: String,
        queue: String,
        error: String,
    },
    #[error("Jestream - {0}")]
    Jetstream(String),
    #[error("NATS - {0}")]
    Nats(String),
    #[error("{0}")]
    Other(String),
}

/// Authentication for NATS connections.
/// https://numaflow.numaproj.io/user-guide/sources/nats/#auth
#[derive(Debug, Clone)]
pub enum NatsAuth {
    Basic {
        username: String,
        password: SecretString,
    },
    NKey(SecretString),
    Token(SecretString),
}

pub use numaflow_shared::tls::{TlsClientAuthCerts, TlsConfigWithClientAuth};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_nats_auth_debug_redacts_basic_password() {
        let auth = NatsAuth::Basic {
            username: "nats-user".to_string(),
            password: "nats-secret-password".into(),
        };
        let debug_output = format!("{:?}", auth);
        assert!(!debug_output.contains("nats-secret-password"));
        assert!(debug_output.contains("nats-user"));
        assert!(debug_output.contains("REDACTED"));
    }

    #[test]
    fn test_nats_auth_debug_redacts_nkey() {
        let auth = NatsAuth::NKey("SUAC-secret-nkey-material".into());
        let debug_output = format!("{:?}", auth);
        assert!(!debug_output.contains("SUAC-secret-nkey-material"));
        assert!(debug_output.contains("REDACTED"));
    }

    #[test]
    fn test_nats_auth_debug_redacts_token() {
        let auth = NatsAuth::Token("secret-nats-token".into());
        let debug_output = format!("{:?}", auth);
        assert!(!debug_output.contains("secret-nats-token"));
        assert!(debug_output.contains("REDACTED"));
    }

    #[test]
    fn derived_debug_redacts_nested_secrets() {
        let config = nats::NatsSourceConfig {
            addr: RedactedUrl::comma_separated_with_default_scheme(
                "nats://url-user:secret-url@host:4222",
                "nats",
            )
            .expect("single valid url"),
            subject: "subject".into(),
            queue: "queue".into(),
            auth: Some(NatsAuth::Basic {
                username: "user".into(),
                password: "secret-password".into(),
            }),
            tls: Some(TlsConfigWithClientAuth {
                insecure_skip_verify: false,
                ca_cert: Some("secret-ca".into()),
                client_auth: Some(TlsClientAuthCerts {
                    client_cert: "secret-cert".into(),
                    client_cert_private_key: "secret-key".into(),
                }),
            }),
        };
        for output in [format!("{config:?}"), format!("{config:#?}")] {
            assert!(!output.contains("secret-"));
            assert!(output.contains("host:4222"));
            assert!(output.contains("subject"));
        }
        for auth in [
            NatsAuth::NKey("secret-nkey".into()),
            NatsAuth::Token("secret-token".into()),
        ] {
            assert!(!format!("{auth:?}").contains("secret-"));
        }
    }

    #[test]
    fn test_jetstream_source_config_debug_redacts_addr_credentials() {
        let config = jetstream::JetstreamSourceConfig {
            addr: "nats://nats-user:secret-url-password@nats-host:4222"
                .parse()
                .unwrap(),
            stream: "my-stream".to_string(),
            consumer: "my-consumer".to_string(),
            deliver_policy: jetstream::ConsumerDeliverPolicy::ALL,
            filter_subjects: vec!["subj.>".to_string()],
            auth: Some(NatsAuth::Token("secret-token".into())),
            tls: None,
        };
        for output in [format!("{config:?}"), format!("{config:#?}")] {
            assert!(!output.contains("secret-url-password"));
            assert!(!output.contains("secret-token"));
            assert!(output.contains("nats-host:4222"));
            assert!(output.contains("my-stream"));
        }
    }

    #[test]
    fn connection_error_redacts_both_formats() {
        let error = Error::Connection {
            server: vec!["nats://user:secret-password@host:4222".parse().unwrap()],
            error: "connection refused".into(),
        };
        assert!(!error.to_string().contains("secret-password"));
        assert!(!format!("{error:?}").contains("secret-password"));
    }
}
