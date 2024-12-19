use std::collections::HashMap;
use std::fmt::Debug;

use async_nats::rustls;
use base64::prelude::BASE64_STANDARD;
use base64::Engine;
use rcgen::{generate_simple_self_signed, Certificate, CertifiedKey, KeyPair};
use serde::{Deserialize, Serialize};

use crate::Error::ParseConfig;

const ENV_NUMAFLOW_SERVING_SOURCE_OBJECT: &str = "NUMAFLOW_SERVING_SOURCE_OBJECT";
const ENV_NUMAFLOW_SERVING_JETSTREAM_URL: &str = "NUMAFLOW_ISBSVC_JETSTREAM_URL";
const ENV_NUMAFLOW_SERVING_JETSTREAM_STREAM: &str = "NUMAFLOW_SERVING_JETSTREAM_STREAM";
const ENV_NUMAFLOW_SERVING_STORE_TTL: &str = "NUMAFLOW_SERVING_STORE_TTL";
const ENV_NUMAFLOW_SERVING_HOST_IP: &str = "NUMAFLOW_SERVING_HOST_IP";
const ENV_NUMAFLOW_SERVING_APP_PORT: &str = "NUMAFLOW_SERVING_APP_LISTEN_PORT";
const ENV_NUMAFLOW_SERVING_JETSTREAM_USER: &str = "NUMAFLOW_ISBSVC_JETSTREAM_USER";
const ENV_NUMAFLOW_SERVING_JETSTREAM_PASSWORD: &str = "NUMAFLOW_ISBSVC_JETSTREAM_PASSWORD";
const ENV_NUMAFLOW_SERVING_AUTH_TOKEN: &str = "NUMAFLOW_SERVING_AUTH_TOKEN";

pub fn generate_certs() -> std::result::Result<(Certificate, KeyPair), String> {
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    let CertifiedKey { cert, key_pair } = generate_simple_self_signed(vec!["localhost".into()])
        .map_err(|e| format!("Failed to generate cert {:?}", e))?;
    Ok((cert, key_pair))
}

#[derive(Deserialize, Clone, PartialEq)]
pub struct BasicAuth {
    pub username: String,
    pub password: String,
}

impl Debug for BasicAuth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let passwd_printable = if self.password.len() > 4 {
            let passwd: String = self
                .password
                .chars()
                .skip(self.password.len() - 2)
                .take(2)
                .collect();
            format!("***{}", passwd)
        } else {
            "*****".to_owned()
        };
        write!(f, "{}:{}", self.username, passwd_printable)
    }
}

#[derive(Debug, Deserialize, Clone, PartialEq)]
pub struct JetStreamConfig {
    pub stream: String,
    pub url: String,
    pub auth: Option<BasicAuth>,
}

impl Default for JetStreamConfig {
    fn default() -> Self {
        Self {
            stream: "default".to_owned(),
            url: "localhost:4222".to_owned(),
            auth: None,
        }
    }
}

#[derive(Debug, Deserialize, Clone, PartialEq)]
pub struct RedisConfig {
    pub addr: String,
    pub max_tasks: usize,
    pub retries: usize,
    pub retries_duration_millis: u16,
    pub ttl_secs: Option<u32>,
}

impl Default for RedisConfig {
    fn default() -> Self {
        Self {
            addr: "redis://127.0.0.1:6379".to_owned(),
            max_tasks: 50,
            retries: 5,
            retries_duration_millis: 100,
            // TODO: we might need an option type here. Zero value of u32 can be used instead of None
            ttl_secs: Some(1),
        }
    }
}

#[derive(Debug, Deserialize, Clone, PartialEq)]
pub struct Settings {
    pub tid_header: String,
    pub app_listen_port: u16,
    pub metrics_server_listen_port: u16,
    pub upstream_addr: String,
    pub drain_timeout_secs: u64,
    pub jetstream: JetStreamConfig,
    pub redis: RedisConfig,
    /// The IP address of the numaserve pod. This will be used to construct the value for X-Numaflow-Callback-Url header
    pub host_ip: String,
    pub api_auth_token: Option<String>,
}

impl Default for Settings {
    fn default() -> Self {
        Self {
            tid_header: "ID".to_owned(),
            app_listen_port: 3000,
            metrics_server_listen_port: 3001,
            upstream_addr: "localhost:8888".to_owned(),
            drain_timeout_secs: 10,
            jetstream: JetStreamConfig::default(),
            redis: RedisConfig::default(),
            host_ip: "127.0.0.1".to_owned(),
            api_auth_token: None,
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Serving {
    #[serde(rename = "msgIDHeaderKey")]
    pub msg_id_header_key: Option<String>,
    #[serde(rename = "store")]
    pub callback_storage: CallbackStorageConfig,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct CallbackStorageConfig {
    pub url: String,
}

/// This implementation is to load settings from env variables
impl TryFrom<HashMap<String, String>> for Settings {
    type Error = crate::Error;
    fn try_from(env_vars: HashMap<String, String>) -> std::result::Result<Self, Self::Error> {
        let host_ip = env_vars
            .get(ENV_NUMAFLOW_SERVING_HOST_IP)
            .ok_or_else(|| {
                ParseConfig(format!(
                    "Environment variable {ENV_NUMAFLOW_SERVING_HOST_IP} is not set"
                ))
            })?
            .to_owned();

        let mut settings = Settings {
            host_ip,
            ..Default::default()
        };

        if let Some(jetstream_url) = env_vars.get(ENV_NUMAFLOW_SERVING_JETSTREAM_URL) {
            settings.jetstream.url = jetstream_url.to_owned();
        }

        if let Some(jetstream_stream) = env_vars.get(ENV_NUMAFLOW_SERVING_JETSTREAM_STREAM) {
            settings.jetstream.stream = jetstream_stream.to_owned();
        }

        if let Some(api_auth_token) = env_vars.get(ENV_NUMAFLOW_SERVING_AUTH_TOKEN) {
            settings.api_auth_token = Some(api_auth_token.to_owned());
        }

        if let Some(app_port) = env_vars.get(ENV_NUMAFLOW_SERVING_APP_PORT) {
            settings.app_listen_port = app_port.parse().map_err(|e| {
                ParseConfig(format!(
                    "Parsing {ENV_NUMAFLOW_SERVING_APP_PORT}(set to '{app_port}'): {e:?}"
                ))
            })?;
        }

        // If username is set, the password also must be set
        if let Some(username) = env_vars.get(ENV_NUMAFLOW_SERVING_JETSTREAM_USER) {
            let Some(password) = env_vars.get(ENV_NUMAFLOW_SERVING_JETSTREAM_PASSWORD) else {
                return Err(ParseConfig(format!("Env variable {ENV_NUMAFLOW_SERVING_JETSTREAM_USER} is set, but {ENV_NUMAFLOW_SERVING_JETSTREAM_PASSWORD} is not set")));
            };
            settings.jetstream.auth = Some(BasicAuth {
                username: username.to_owned(),
                password: password.to_owned(),
            });
        }

        // Update redis.ttl_secs from environment variable
        if let Some(ttl_secs) = env_vars.get(ENV_NUMAFLOW_SERVING_STORE_TTL) {
            let ttl_secs: u32 = ttl_secs.parse().map_err(|e| {
                ParseConfig(format!("parsing {ENV_NUMAFLOW_SERVING_STORE_TTL}: {e:?}"))
            })?;
            settings.redis.ttl_secs = Some(ttl_secs);
        }

        let Some(source_spec_encoded) = env_vars.get(ENV_NUMAFLOW_SERVING_SOURCE_OBJECT) else {
            return Ok(settings);
        };

        let source_spec_decoded = BASE64_STANDARD
            .decode(source_spec_encoded.as_bytes())
            .map_err(|e| ParseConfig(format!("decoding NUMAFLOW_SERVING_SOURCE: {e:?}")))?;

        let source_spec = serde_json::from_slice::<Serving>(&source_spec_decoded)
            .map_err(|e| ParseConfig(format!("parsing NUMAFLOW_SERVING_SOURCE: {e:?}")))?;

        // Update tid_header from source_spec
        if let Some(msg_id_header_key) = source_spec.msg_id_header_key {
            settings.tid_header = msg_id_header_key;
        }

        // Update redis.addr from source_spec, currently we only support redis as callback storage
        settings.redis.addr = source_spec.callback_storage.url;

        Ok(settings)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_auth_debug_print() {
        let auth = BasicAuth {
            username: "js-auth-user".into(),
            password: "js-auth-password".into(),
        };
        let auth_debug = format!("{auth:?}");
        assert_eq!(auth_debug, "js-auth-user:***rd");
    }

    #[test]
    fn test_default_config() {
        let settings = Settings::default();

        assert_eq!(settings.tid_header, "ID");
        assert_eq!(settings.app_listen_port, 3000);
        assert_eq!(settings.metrics_server_listen_port, 3001);
        assert_eq!(settings.upstream_addr, "localhost:8888");
        assert_eq!(settings.drain_timeout_secs, 10);
        assert_eq!(settings.jetstream.stream, "default");
        assert_eq!(settings.jetstream.url, "localhost:4222");
        assert_eq!(settings.redis.addr, "redis://127.0.0.1:6379");
        assert_eq!(settings.redis.max_tasks, 50);
        assert_eq!(settings.redis.retries, 5);
        assert_eq!(settings.redis.retries_duration_millis, 100);
    }

    #[test]
    fn test_config_parse() {
        // Set up the environment variables
        let env_vars = [
            (
                ENV_NUMAFLOW_SERVING_JETSTREAM_URL,
                "nats://isbsvc-default-js-svc.default.svc:4222",
            ),
            (
                ENV_NUMAFLOW_SERVING_JETSTREAM_STREAM,
                "ascii-art-pipeline-in-serving-source",
            ),
            (ENV_NUMAFLOW_SERVING_JETSTREAM_USER, "js-auth-user"),
            (ENV_NUMAFLOW_SERVING_JETSTREAM_PASSWORD, "js-user-password"),
            (ENV_NUMAFLOW_SERVING_HOST_IP, "10.2.3.5"),
            (ENV_NUMAFLOW_SERVING_AUTH_TOKEN, "api-auth-token"),
            (ENV_NUMAFLOW_SERVING_APP_PORT, "8443"),
            (ENV_NUMAFLOW_SERVING_STORE_TTL, "86400"),
            (ENV_NUMAFLOW_SERVING_SOURCE_OBJECT, "eyJhdXRoIjpudWxsLCJzZXJ2aWNlIjp0cnVlLCJtc2dJREhlYWRlcktleSI6IlgtTnVtYWZsb3ctSWQiLCJzdG9yZSI6eyJ1cmwiOiJyZWRpczovL3JlZGlzOjYzNzkifX0=")
        ];

        // Call the config method
        let settings: Settings = env_vars
            .into_iter()
            .map(|(key, val)| (key.to_owned(), val.to_owned()))
            .collect::<HashMap<String, String>>()
            .try_into()
            .unwrap();

        let expected_config = Settings {
            tid_header: "X-Numaflow-Id".into(),
            app_listen_port: 8443,
            metrics_server_listen_port: 3001,
            upstream_addr: "localhost:8888".into(),
            drain_timeout_secs: 10,
            jetstream: JetStreamConfig {
                stream: "ascii-art-pipeline-in-serving-source".into(),
                url: "nats://isbsvc-default-js-svc.default.svc:4222".into(),
                auth: Some(BasicAuth {
                    username: "js-auth-user".into(),
                    password: "js-user-password".into(),
                }),
            },
            redis: RedisConfig {
                addr: "redis://redis:6379".into(),
                max_tasks: 50,
                retries: 5,
                retries_duration_millis: 100,
                ttl_secs: Some(86400),
            },
            host_ip: "10.2.3.5".into(),
            api_auth_token: Some("api-auth-token".into()),
        };

        assert_eq!(settings, expected_config);
    }
}
