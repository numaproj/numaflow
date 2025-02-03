use std::collections::HashMap;
use std::fmt::Debug;

use base64::prelude::BASE64_STANDARD;
use base64::Engine;
use numaflow_models::models::Vertex;
use rcgen::{generate_simple_self_signed, Certificate, CertifiedKey, KeyPair};
use serde::{Deserialize, Serialize};

use crate::{
    pipeline::PipelineDCG,
    Error::{self, ParseConfig},
};

const ENV_NUMAFLOW_SERVING_HOST_IP: &str = "NUMAFLOW_SERVING_HOST_IP";
const ENV_NUMAFLOW_SERVING_APP_PORT: &str = "NUMAFLOW_SERVING_APP_LISTEN_PORT";
const ENV_MIN_PIPELINE_SPEC: &str = "NUMAFLOW_SERVING_MIN_PIPELINE_SPEC";
const ENV_VERTEX_OBJ: &str = "NUMAFLOW_VERTEX_OBJECT";

pub const DEFAULT_ID_HEADER: &str = "X-Numaflow-Id";
pub const DEFAULT_CALLBACK_URL_HEADER_KEY: &str = "X-Numaflow-Callback-Url";

pub fn generate_certs() -> std::result::Result<(Certificate, KeyPair), String> {
    let CertifiedKey { cert, key_pair } = generate_simple_self_signed(vec!["localhost".into()])
        .map_err(|e| format!("Failed to generate cert {:?}", e))?;
    Ok((cert, key_pair))
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
            ttl_secs: Some(86400),
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
    pub redis: RedisConfig,
    /// The IP address of the numaserve pod. This will be used to construct the value for X-Numaflow-Callback-Url header
    pub host_ip: String,
    pub api_auth_token: Option<String>,
    pub pipeline_spec: PipelineDCG,
}

impl Default for Settings {
    fn default() -> Self {
        Self {
            tid_header: DEFAULT_ID_HEADER.to_owned(),
            app_listen_port: 3000,
            metrics_server_listen_port: 3001,
            upstream_addr: "localhost:8888".to_owned(),
            drain_timeout_secs: 10,
            redis: RedisConfig::default(),
            host_ip: "127.0.0.1".to_owned(),
            api_auth_token: None,
            pipeline_spec: Default::default(),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct AuthToken {
    /// Name of the configmap
    name: String,
    /// Key within the configmap
    key: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct Auth {
    token: AuthToken,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Serving {
    #[serde(rename = "msgIDHeaderKey")]
    pub msg_id_header_key: Option<String>,
    #[serde(rename = "store")]
    pub callback_storage: CallbackStorageConfig,
    auth: Option<Auth>,
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
                ParseConfig(format!(
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

        if let Some(app_port) = env_vars.get(ENV_NUMAFLOW_SERVING_APP_PORT) {
            settings.app_listen_port = app_port.parse().map_err(|e| {
                ParseConfig(format!(
                    "Parsing {ENV_NUMAFLOW_SERVING_APP_PORT}(set to '{app_port}'): {e:?}"
                ))
            })?;
        }

        // TODO: When we add support for Serving with monovertex, we should check for NUMAFLOW_MONO_VERTEX_OBJECT variable too.
        let Some(source_spec_encoded) = env_vars.get(ENV_VERTEX_OBJ) else {
            return Err(ParseConfig(format!(
                "Environment variable {ENV_VERTEX_OBJ} is not set"
            )));
        };

        let source_spec_decoded = BASE64_STANDARD
            .decode(source_spec_encoded.as_bytes())
            .map_err(|e| ParseConfig(format!("decoding {ENV_VERTEX_OBJ}: {e:?}")))?;

        let vertex_obj = serde_json::from_slice::<Vertex>(&source_spec_decoded)
            .map_err(|e| ParseConfig(format!("parsing {ENV_VERTEX_OBJ}: {e:?}")))?;

        let serving_spec = vertex_obj
            .spec
            .source
            .ok_or_else(|| {
                ParseConfig(format!("parsing {ENV_VERTEX_OBJ}: source can not be empty"))
            })?
            .serving
            .ok_or_else(|| {
                ParseConfig(format!(
                    "parsing {ENV_VERTEX_OBJ}: Serving source spec is not found"
                ))
            })?;
        // Update tid_header from source_spec
        settings.tid_header = serving_spec.msg_id_header_key;

        // Update redis.addr from source_spec, currently we only support redis as callback storage
        settings.redis.addr = serving_spec.store.url;
        settings.redis.ttl_secs = settings.redis.ttl_secs;

        if let Some(auth) = serving_spec.auth {
            let token = auth.token.unwrap();
            let auth_token = get_secret_from_volume(&token.name, &token.key)
                .map_err(|e| ParseConfig(e.to_string()))?;
            settings.api_auth_token = Some(auth_token);
        }

        Ok(settings)
    }
}

// Retrieve value from mounted secret volume
// "/var/numaflow/secrets/${secretRef.name}/${secretRef.key}" is expected to be the file path
pub(crate) fn get_secret_from_volume(name: &str, key: &str) -> Result<String, String> {
    let path = format!("/var/numaflow/secrets/{name}/{key}");
    let val = std::fs::read_to_string(path.clone())
        .map_err(|e| format!("Reading secret from file {path}: {e:?}"))?;
    Ok(val.trim().into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pipeline::{Edge, Vertex};

    #[test]
    fn test_default_config() {
        let settings = Settings::default();

        assert_eq!(settings.tid_header, "X-Numaflow-Id");
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
            (ENV_NUMAFLOW_SERVING_APP_PORT, "8443"),
            (ENV_MIN_PIPELINE_SPEC, "eyJ2ZXJ0aWNlcyI6W3sibmFtZSI6InNlcnZpbmctaW4iLCJzb3VyY2UiOnsic2VydmluZyI6eyJhdXRoIjpudWxsLCJzZXJ2aWNlIjp0cnVlLCJtc2dJREhlYWRlcktleSI6IlgtTnVtYWZsb3ctSWQiLCJzdG9yZSI6eyJ1cmwiOiJyZWRpczovL3JlZGlzOjYzNzkifX19LCJjb250YWluZXJUZW1wbGF0ZSI6eyJyZXNvdXJjZXMiOnt9LCJpbWFnZVB1bGxQb2xpY3kiOiJOZXZlciJ9LCJzY2FsZSI6eyJtaW4iOjF9LCJ1cGRhdGVTdHJhdGVneSI6eyJ0eXBlIjoiUm9sbGluZ1VwZGF0ZSIsInJvbGxpbmdVcGRhdGUiOnsibWF4VW5hdmFpbGFibGUiOiIyNSUifX19LHsibmFtZSI6InNlcnZlLXNpbmsiLCJzaW5rIjp7InVkc2luayI6eyJjb250YWluZXIiOnsiaW1hZ2UiOiJzZXJ2ZXNpbms6MC4xIiwiZW52IjpbeyJuYW1lIjoiTlVNQUZMT1dfQ0FMTEJBQ0tfVVJMX0tFWSIsInZhbHVlIjoiWC1OdW1hZmxvdy1DYWxsYmFjay1VcmwifSx7Im5hbWUiOiJOVU1BRkxPV19NU0dfSURfSEVBREVSX0tFWSIsInZhbHVlIjoiWC1OdW1hZmxvdy1JZCJ9XSwicmVzb3VyY2VzIjp7fSwiaW1hZ2VQdWxsUG9saWN5IjoiTmV2ZXIifX0sInJldHJ5U3RyYXRlZ3kiOnt9fSwiY29udGFpbmVyVGVtcGxhdGUiOnsicmVzb3VyY2VzIjp7fSwiaW1hZ2VQdWxsUG9saWN5IjoiTmV2ZXIifSwic2NhbGUiOnsibWluIjoxfSwidXBkYXRlU3RyYXRlZ3kiOnsidHlwZSI6IlJvbGxpbmdVcGRhdGUiLCJyb2xsaW5nVXBkYXRlIjp7Im1heFVuYXZhaWxhYmxlIjoiMjUlIn19fV0sImVkZ2VzIjpbeyJmcm9tIjoic2VydmluZy1pbiIsInRvIjoic2VydmUtc2luayIsImNvbmRpdGlvbnMiOm51bGx9XSwibGlmZWN5Y2xlIjp7fSwid2F0ZXJtYXJrIjp7fX0="),
            (ENV_VERTEX_OBJ, "eyJtZXRhZGF0YSI6eyJuYW1lIjoic2ltcGxlLXBpcGVsaW5lLXNlcnZpbmctaW4iLCJuYW1lc3BhY2UiOiJkZWZhdWx0IiwiY3JlYXRpb25UaW1lc3RhbXAiOm51bGx9LCJzcGVjIjp7Im5hbWUiOiJzZXJ2aW5nLWluIiwic291cmNlIjp7InNlcnZpbmciOnsiYXV0aCI6bnVsbCwic2VydmljZSI6dHJ1ZSwibXNnSURIZWFkZXJLZXkiOiJYLU51bWFmbG93LUlkIiwic3RvcmUiOnsidXJsIjoicmVkaXM6Ly9yZWRpczo2Mzc5In19fSwiY29udGFpbmVyVGVtcGxhdGUiOnsicmVzb3VyY2VzIjp7fSwiaW1hZ2VQdWxsUG9saWN5IjoiTmV2ZXIifSwibGltaXRzIjp7InJlYWRCYXRjaFNpemUiOjUwMCwicmVhZFRpbWVvdXQiOiIxcyIsImJ1ZmZlck1heExlbmd0aCI6MzAwMDAsImJ1ZmZlclVzYWdlTGltaXQiOjgwfSwic2NhbGUiOnsibWluIjoxfSwidXBkYXRlU3RyYXRlZ3kiOnsidHlwZSI6IlJvbGxpbmdVcGRhdGUiLCJyb2xsaW5nVXBkYXRlIjp7Im1heFVuYXZhaWxhYmxlIjoiMjUlIn19LCJwaXBlbGluZU5hbWUiOiJzaW1wbGUtcGlwZWxpbmUiLCJpbnRlclN0ZXBCdWZmZXJTZXJ2aWNlTmFtZSI6IiIsInJlcGxpY2FzIjowLCJ0b0VkZ2VzIjpbeyJmcm9tIjoic2VydmluZy1pbiIsInRvIjoic2VydmUtc2luayIsImNvbmRpdGlvbnMiOm51bGwsImZyb21WZXJ0ZXhUeXBlIjoiU291cmNlIiwiZnJvbVZlcnRleFBhcnRpdGlvbkNvdW50IjoxLCJmcm9tVmVydGV4TGltaXRzIjp7InJlYWRCYXRjaFNpemUiOjUwMCwicmVhZFRpbWVvdXQiOiIxcyIsImJ1ZmZlck1heExlbmd0aCI6MzAwMDAsImJ1ZmZlclVzYWdlTGltaXQiOjgwfSwidG9WZXJ0ZXhUeXBlIjoiU2luayIsInRvVmVydGV4UGFydGl0aW9uQ291bnQiOjEsInRvVmVydGV4TGltaXRzIjp7InJlYWRCYXRjaFNpemUiOjUwMCwicmVhZFRpbWVvdXQiOiIxcyIsImJ1ZmZlck1heExlbmd0aCI6MzAwMDAsImJ1ZmZlclVzYWdlTGltaXQiOjgwfX1dLCJ3YXRlcm1hcmsiOnsibWF4RGVsYXkiOiIwcyJ9fSwic3RhdHVzIjp7InBoYXNlIjoiIiwicmVwbGljYXMiOjAsImRlc2lyZWRSZXBsaWNhcyI6MCwibGFzdFNjYWxlZEF0IjpudWxsfX0=")
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
            api_auth_token: None,
            pipeline_spec: PipelineDCG {
                vertices: vec![
                    Vertex {
                        name: "serving-in".into(),
                    },
                    Vertex {
                        name: "serve-sink".into(),
                    },
                ],
                edges: vec![Edge {
                    from: "serving-in".into(),
                    to: "serve-sink".into(),
                    conditions: None,
                }],
            },
            ..Default::default()
        };
        assert_eq!(settings, expected_config);
    }
}
