use std::collections::HashMap;

use base64::prelude::BASE64_STANDARD;
use base64::Engine;
use serde::{Deserialize, Serialize};

use crate::pipeline::PipelineDCG;
use crate::Error;

const ENV_NUMAFLOW_SERVING_SOURCE_OBJECT: &str = "NUMAFLOW_SERVING_SOURCE_OBJECT";
const ENV_NUMAFLOW_SERVING_STORE_TTL: &str = "NUMAFLOW_SERVING_STORE_TTL";
const ENV_NUMAFLOW_SERVING_HOST_IP: &str = "NUMAFLOW_SERVING_HOST_IP";
const ENV_NUMAFLOW_SERVING_APP_PORT: &str = "NUMAFLOW_SERVING_APP_LISTEN_PORT";
const ENV_NUMAFLOW_SERVING_AUTH_TOKEN: &str = "NUMAFLOW_SERVING_AUTH_TOKEN";
const ENV_MIN_PIPELINE_SPEC: &str = "NUMAFLOW_SERVING_MIN_PIPELINE_SPEC";

pub(crate) const SAVED: &str = "SAVED";

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

#[derive(Debug, Clone, PartialEq)]
pub struct Settings {
    pub tid_header: String,
    pub app_listen_port: u16,
    pub metrics_server_listen_port: u16,
    pub upstream_addr: String,
    pub drain_timeout_secs: u64,
    /// The IP address of the numaserve pod. This will be used to construct the value for X-Numaflow-Callback-Url header
    pub host_ip: String,
    pub api_auth_token: Option<String>,
    pub redis: RedisConfig,
    pub pipeline_spec: PipelineDCG,
}

impl Default for Settings {
    fn default() -> Self {
        Self {
            tid_header: "ID".to_owned(),
            app_listen_port: 3000,
            metrics_server_listen_port: 3001,
            upstream_addr: "localhost:8888".to_owned(),
            drain_timeout_secs: 10,
            host_ip: "127.0.0.1".to_owned(),
            api_auth_token: None,
            redis: RedisConfig::default(),
            pipeline_spec: Default::default(),
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
    type Error = Error;
    fn try_from(env_vars: HashMap<String, String>) -> std::result::Result<Self, Self::Error> {
        let host_ip = env_vars
            .get(ENV_NUMAFLOW_SERVING_HOST_IP)
            .ok_or_else(|| {
                Error::ParseConfig(format!(
                    "Environment variable {ENV_NUMAFLOW_SERVING_HOST_IP} is not set"
                ))
            })?
            .to_owned();

        let pipeline_spec: PipelineDCG = env_vars
            .get(ENV_MIN_PIPELINE_SPEC)
            .ok_or_else(|| {
                Error::ParseConfig(format!(
                    "Pipeline spec is not set using environment variable {ENV_MIN_PIPELINE_SPEC}"
                ))
            })?
            .parse()
            .map_err(|e| {
                Error::ParseConfig(format!(
                    "Parsing pipeline spec: {}: error={e:?}",
                    env_vars.get(ENV_MIN_PIPELINE_SPEC).unwrap()
                ))
            })?;

        let mut settings = Settings {
            host_ip,
            pipeline_spec,
            ..Default::default()
        };

        if let Some(api_auth_token) = env_vars.get(ENV_NUMAFLOW_SERVING_AUTH_TOKEN) {
            settings.api_auth_token = Some(api_auth_token.to_owned());
        }

        if let Some(app_port) = env_vars.get(ENV_NUMAFLOW_SERVING_APP_PORT) {
            settings.app_listen_port = app_port.parse().map_err(|e| {
                Error::ParseConfig(format!(
                    "Parsing {ENV_NUMAFLOW_SERVING_APP_PORT}(set to '{app_port}'): {e:?}"
                ))
            })?;
        }

        // Update redis.ttl_secs from environment variable
        if let Some(ttl_secs) = env_vars.get(ENV_NUMAFLOW_SERVING_STORE_TTL) {
            let ttl_secs: u32 = ttl_secs.parse().map_err(|e| {
                Error::ParseConfig(format!("parsing {ENV_NUMAFLOW_SERVING_STORE_TTL}: {e:?}"))
            })?;
            settings.redis.ttl_secs = Some(ttl_secs);
        }

        let Some(source_spec_encoded) = env_vars.get(ENV_NUMAFLOW_SERVING_SOURCE_OBJECT) else {
            return Ok(settings);
        };

        let source_spec_decoded = BASE64_STANDARD
            .decode(source_spec_encoded.as_bytes())
            .map_err(|e| Error::ParseConfig(format!("decoding NUMAFLOW_SERVING_SOURCE: {e:?}")))?;

        let source_spec = serde_json::from_slice::<Serving>(&source_spec_decoded)
            .map_err(|e| Error::ParseConfig(format!("parsing NUMAFLOW_SERVING_SOURCE: {e:?}")))?;

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
    use crate::pipeline::{Edge, Vertex};

    use super::*;

    #[test]
    fn test_default_config() {
        let settings = Settings::default();

        assert_eq!(settings.tid_header, "ID");
        assert_eq!(settings.app_listen_port, 3000);
        assert_eq!(settings.metrics_server_listen_port, 3001);
        assert_eq!(settings.upstream_addr, "localhost:8888");
        assert_eq!(settings.drain_timeout_secs, 10);
        assert_eq!(settings.redis.addr, "redis://127.0.0.1:6379");
        assert_eq!(settings.redis.max_tasks, 50);
        assert_eq!(settings.redis.retries, 5);
        assert_eq!(settings.redis.retries_duration_millis, 100);
    }

    #[test]
    fn test_config_parse() {
        // Set up the environment variables
        let env_vars = [
            (ENV_NUMAFLOW_SERVING_HOST_IP, "10.2.3.5"),
            (ENV_NUMAFLOW_SERVING_AUTH_TOKEN, "api-auth-token"),
            (ENV_NUMAFLOW_SERVING_APP_PORT, "8443"),
            (ENV_NUMAFLOW_SERVING_STORE_TTL, "86400"),
            (ENV_NUMAFLOW_SERVING_SOURCE_OBJECT, "eyJhdXRoIjpudWxsLCJzZXJ2aWNlIjp0cnVlLCJtc2dJREhlYWRlcktleSI6IlgtTnVtYWZsb3ctSWQiLCJzdG9yZSI6eyJ1cmwiOiJyZWRpczovL3JlZGlzOjYzNzkifX0="),
            (ENV_MIN_PIPELINE_SPEC, "eyJ2ZXJ0aWNlcyI6W3sibmFtZSI6InNlcnZpbmctaW4iLCJzb3VyY2UiOnsic2VydmluZyI6eyJhdXRoIjpudWxsLCJzZXJ2aWNlIjp0cnVlLCJtc2dJREhlYWRlcktleSI6IlgtTnVtYWZsb3ctSWQiLCJzdG9yZSI6eyJ1cmwiOiJyZWRpczovL3JlZGlzOjYzNzkifX19LCJjb250YWluZXJUZW1wbGF0ZSI6eyJyZXNvdXJjZXMiOnt9LCJpbWFnZVB1bGxQb2xpY3kiOiJOZXZlciIsImVudiI6W3sibmFtZSI6IlJVU1RfTE9HIiwidmFsdWUiOiJpbmZvIn1dfSwic2NhbGUiOnsibWluIjoxfSwidXBkYXRlU3RyYXRlZ3kiOnsidHlwZSI6IlJvbGxpbmdVcGRhdGUiLCJyb2xsaW5nVXBkYXRlIjp7Im1heFVuYXZhaWxhYmxlIjoiMjUlIn19fSx7Im5hbWUiOiJzZXJ2aW5nLXNpbmsiLCJzaW5rIjp7InVkc2luayI6eyJjb250YWluZXIiOnsiaW1hZ2UiOiJxdWF5LmlvL251bWFpby9udW1hZmxvdy1ycy9zaW5rLWxvZzpzdGFibGUiLCJlbnYiOlt7Im5hbWUiOiJOVU1BRkxPV19DQUxMQkFDS19VUkxfS0VZIiwidmFsdWUiOiJYLU51bWFmbG93LUNhbGxiYWNrLVVybCJ9LHsibmFtZSI6Ik5VTUFGTE9XX01TR19JRF9IRUFERVJfS0VZIiwidmFsdWUiOiJYLU51bWFmbG93LUlkIn1dLCJyZXNvdXJjZXMiOnt9fX0sInJldHJ5U3RyYXRlZ3kiOnt9fSwiY29udGFpbmVyVGVtcGxhdGUiOnsicmVzb3VyY2VzIjp7fSwiaW1hZ2VQdWxsUG9saWN5IjoiTmV2ZXIifSwic2NhbGUiOnsibWluIjoxfSwidXBkYXRlU3RyYXRlZ3kiOnsidHlwZSI6IlJvbGxpbmdVcGRhdGUiLCJyb2xsaW5nVXBkYXRlIjp7Im1heFVuYXZhaWxhYmxlIjoiMjUlIn19fV0sImVkZ2VzIjpbeyJmcm9tIjoic2VydmluZy1pbiIsInRvIjoic2VydmluZy1zaW5rIiwiY29uZGl0aW9ucyI6bnVsbH1dLCJsaWZlY3ljbGUiOnt9LCJ3YXRlcm1hcmsiOnt9fQ==")
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
            redis: RedisConfig {
                addr: "redis://redis:6379".into(),
                max_tasks: 50,
                retries: 5,
                retries_duration_millis: 100,
                ttl_secs: Some(86400),
            },
            host_ip: "10.2.3.5".into(),
            api_auth_token: Some("api-auth-token".into()),
            pipeline_spec: PipelineDCG {
                vertices: vec![
                    Vertex {
                        name: "serving-in".into(),
                    },
                    Vertex {
                        name: "serving-sink".into(),
                    },
                ],
                edges: vec![Edge {
                    from: "serving-in".into(),
                    to: "serving-sink".into(),
                    conditions: None,
                }],
            },
        };
        assert_eq!(settings, expected_config);
    }
}
