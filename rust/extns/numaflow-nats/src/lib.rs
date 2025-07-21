pub mod jetstream;
pub mod nats;

mod tls;

pub type Result<T> = core::result::Result<T, Error>;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Connecting to NATS {server} - {error}")]
    Connection { server: String, error: String },

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

/// Represents the authentication method used to connect to NATS.
/// https://numaflow.numaproj.io/user-guide/sources/nats/#auth
#[derive(Debug, Clone, PartialEq)]
pub enum NatsAuth {
    Basic { username: String, password: String },
    NKey(String),
    Token(String),
}

#[derive(Debug, Clone, PartialEq)]
pub struct TlsConfig {
    pub insecure_skip_verify: bool,
    pub ca_cert: Option<String>,
    pub client_auth: Option<TlsClientAuthCerts>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct TlsClientAuthCerts {
    pub client_cert: String,
    pub client_cert_private_key: String,
}
