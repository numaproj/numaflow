use std::env;
use std::fmt::Debug;
use std::{collections::HashMap, fmt::format};

use base64::prelude::BASE64_STANDARD;
use base64::Engine;
use numaflow_models::models::{MonoVertex, Vertex, VertexSpec};
use rcgen::{generate_simple_self_signed, Certificate, CertifiedKey, KeyPair};
use serde::{Deserialize, Serialize};

use crate::{
    pipeline::PipelineDCG,
    Error::{self, ParseConfig},
};

const ENV_NUMAFLOW_SERVING_HOST_IP: &str = "NUMAFLOW_SERVING_HOST_IP";
const ENV_NUMAFLOW_SERVING_APP_PORT: &str = "NUMAFLOW_SERVING_APP_LISTEN_PORT";
pub const ENV_MIN_PIPELINE_SPEC: &str = "NUMAFLOW_SERVING_MIN_PIPELINE_SPEC";
const ENV_VERTEX_OBJ: &str = "NUMAFLOW_VERTEX_OBJECT";
const ENV_MONOVERTEX_OBJ: &str = "NUMAFLOW_MONO_VERTEX_OBJECT";

pub const DEFAULT_ID_HEADER: &str = "X-Numaflow-Id";
pub const DEFAULT_CALLBACK_URL_HEADER_KEY: &str = "X-Numaflow-Callback-Url";
const DEFAULT_GRPC_MAX_MESSAGE_SIZE: usize = 64 * 1024 * 1024; // 64 MB
const DEFAULT_SERVING_STORE_SOCKET: &str = "/var/run/numaflow/serving.sock";
const DEFAULT_SERVING_STORE_SERVER_INFO_FILE: &str = "/var/run/numaflow/serving-server-info";
const ENV_NUMAFLOW_SERVING_JETSTREAM_USER: &str = "NUMAFLOW_ISBSVC_JETSTREAM_USER";
const ENV_NUMAFLOW_SERVING_JETSTREAM_PASSWORD: &str = "NUMAFLOW_ISBSVC_JETSTREAM_PASSWORD";

pub fn generate_certs() -> Result<(Certificate, KeyPair), String> {
    let CertifiedKey { cert, key_pair } = generate_simple_self_signed(vec!["localhost".into()])
        .map_err(|e| format!("Failed to generate cert {:?}", e))?;
    Ok((cert, key_pair))
}

#[derive(Debug, Deserialize, Clone, PartialEq)]
pub struct UserDefinedStoreConfig {
    pub grpc_max_message_size: usize,
    pub socket_path: String,
    pub server_info_path: String,
}

impl Default for UserDefinedStoreConfig {
    fn default() -> Self {
        Self {
            grpc_max_message_size: DEFAULT_GRPC_MAX_MESSAGE_SIZE,
            socket_path: DEFAULT_SERVING_STORE_SOCKET.to_string(),
            server_info_path: DEFAULT_SERVING_STORE_SERVER_INFO_FILE.to_string(),
        }
    }
}

#[derive(Debug, Deserialize, Clone, PartialEq)]
pub struct Settings {
    /// The HTTP header used to communicate to the client about the unique id assigned for a request in the store
    /// The client may also set the value of this header when sending the payload.
    pub tid_header: String,
    pub app_listen_port: u16,
    pub metrics_server_listen_port: u16,
    pub upstream_addr: String,
    pub drain_timeout_secs: u64,
    pub store_type: StoreType,
    pub js_callback_store: String,
    /// The IP address of the numaserve pod. This will be used to construct the value for X-Numaflow-Callback-Url header
    pub host_ip: String,
    pub api_auth_token: Option<String>,
    pub pipeline_spec: PipelineDCG,
    pub nats_basic_auth: Option<(String, String)>,
    pub js_message_stream: String,
    pub jetstream_url: String,
}

#[derive(Default, Debug, Deserialize, Clone, PartialEq)]
pub enum StoreType {
    UserDefined(UserDefinedStoreConfig),
    #[default]
    Nats,
}

impl Default for Settings {
    fn default() -> Self {
        Self {
            tid_header: DEFAULT_ID_HEADER.to_owned(),
            app_listen_port: 3000,
            metrics_server_listen_port: 3001,
            upstream_addr: "localhost:8888".to_owned(),
            drain_timeout_secs: 600,
            store_type: StoreType::default(),
            js_callback_store: "kv".to_owned(),
            host_ip: "127.0.0.1".to_owned(),
            api_auth_token: None,
            pipeline_spec: Default::default(),
            nats_basic_auth: None,
            js_message_stream: "test-stream".into(),
            jetstream_url: "localhost:4222".into(),
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
    fn try_from(env_vars: HashMap<String, String>) -> Result<Self, Self::Error> {
        let pipeline_spec_encoded = env_vars.get(ENV_MIN_PIPELINE_SPEC).ok_or_else(|| {
            Error::ParseConfig(format!(
                "Pipeline spec is not set using environment variable {ENV_MIN_PIPELINE_SPEC}"
            ))
        })?;

        let pipeline_spec: PipelineDCG = pipeline_spec_encoded.parse().map_err(|e| {
            Error::ParseConfig(format!(
                "Parsing pipeline spec: {}: error={e:?}",
                env_vars.get(ENV_MIN_PIPELINE_SPEC).unwrap()
            ))
        })?;
        let pipeline_spec_decoded = BASE64_STANDARD
            .decode(pipeline_spec_encoded.as_bytes())
            .map_err(|e| ParseConfig(format!("decoding {ENV_MIN_PIPELINE_SPEC}: {e:?}")))?;

        #[derive(Deserialize)]
        struct Vertex {
            source: Option<numaflow_models::models::Source>,
        }

        #[derive(Deserialize)]
        struct PipelineVertices {
            vertices: Vec<Vertex>,
        }

        let pvs: PipelineVertices = serde_json::from_slice(pipeline_spec_decoded.as_slice())
            .map_err(|e| {
                ParseConfig(format!(
                    "Parsing vertex spec from {ENV_MIN_PIPELINE_SPEC}: {e:?}"
                ))
            })?;

        let mut js_source_spec = None;
        for vtx in pvs.vertices {
            let Some(src) = vtx.source else {
                continue;
            };
            let Some(jetstream) = src.jetstream else {
                return Err(ParseConfig(format!(
                    "Expected the source to be Jetstream in {ENV_MIN_PIPELINE_SPEC}"
                )));
            };
            js_source_spec = Some(jetstream);
        }
        let js_source_spec = js_source_spec.ok_or_else(|| {
            ParseConfig(format!(
                "Jetstream source settings was not found in {ENV_MIN_PIPELINE_SPEC}"
            ))
        })?;

        let nats_username = env_vars
            .get(ENV_NUMAFLOW_SERVING_JETSTREAM_USER)
            .ok_or_else(|| {
                ParseConfig(format!(
                    "Environment variable '{ENV_NUMAFLOW_SERVING_JETSTREAM_USER}' is not set"
                ))
            })?;
        let nats_password = env_vars
            .get("NUMAFLOW_ISBSVC_JETSTREAM_PASSWORD")
            .ok_or_else(|| {
                ParseConfig(format!(
                    "Environment variable '{ENV_NUMAFLOW_SERVING_JETSTREAM_PASSWORD}' is not set"
                ))
            })?;

        let js_store =  env::var("NUMAFLOW_SERVING_KV_STORE").map_err(|_| {
            ParseConfig("Serving store is default, but environment variable NUMAFLOW_SERVING_KV_STORE is not set".into())
        })?;

        let mut settings = Settings {
            pipeline_spec,
            js_callback_store: js_store,
            nats_basic_auth: Some((nats_username.into(), nats_password.into())),
            js_message_stream: js_source_spec.stream,
            jetstream_url: js_source_spec.url,
            ..Default::default()
        };

        if let Some(app_port) = env_vars.get(ENV_NUMAFLOW_SERVING_APP_PORT) {
            settings.app_listen_port = app_port.parse().map_err(|e| {
                ParseConfig(format!(
                    "Parsing {ENV_NUMAFLOW_SERVING_APP_PORT}(set to '{app_port}'): {e:?}"
                ))
            })?;
        }

        let serving_server_settings_encoded = env::var("NUMAFLOW_SERVING_SOURCE_SETTINGS")
            .map_err(|_| {
                ParseConfig(
                    "Environment variable NUMAFLOW_SERVING_SOURCE_SETTINGS is not set".into(),
                )
            })?;
        let serving_server_settings_decoded = BASE64_STANDARD
            .decode(serving_server_settings_encoded.as_bytes())
            .map_err(|e| ParseConfig(format!("decoding {ENV_MIN_PIPELINE_SPEC}: {e:?}")))?;

        let serving_server_settings: numaflow_models::models::ServingSpec =
            serde_json::from_slice(serving_server_settings_decoded.as_slice()).map_err(|err| {
                ParseConfig(format!(
                    "Parsing Serving settings from environment variable 'NUMAFLOW_SERVING_SOURCE_SETTINGS': {err:?}"
                ))
            })?;

        settings.store_type = if serving_server_settings.store.is_some() {
            StoreType::UserDefined(UserDefinedStoreConfig::default())
        } else {
            StoreType::Nats
        };

        settings.tid_header = serving_server_settings.msg_id_header_key;

        settings.drain_timeout_secs = serving_server_settings
            .request_timeout_seconds
            .unwrap_or(120)
            .max(1) as u64; // Ensure timeout is atleast 1 second

        if let Some(auth) = serving_server_settings.auth {
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
        assert_eq!(settings.drain_timeout_secs, 600);
        assert_eq!(settings.store_type, StoreType::Nats,);
    }

    #[test]
    fn test_pipeline_config_parse() {
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
            drain_timeout_secs: 120,
            store_type: StoreType::Nats,
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

    #[test]
    fn test_monovertex_config_parse() {
        // Set up the environment variables
        let env_vars = [
            (ENV_NUMAFLOW_SERVING_HOST_IP, "10.2.3.5"),
            (ENV_NUMAFLOW_SERVING_APP_PORT, "8443"),
            (ENV_MONOVERTEX_OBJ, "eyJtZXRhZGF0YSI6eyJuYW1lIjoidHJhbnNmb3JtZXItbW9uby12ZXJ0ZXgiLCJuYW1lc3BhY2UiOiJkZWZhdWx0IiwiY3JlYXRpb25UaW1lc3RhbXAiOm51bGx9LCJzcGVjIjp7InJlcGxpY2FzIjowLCJzb3VyY2UiOnsic2VydmluZyI6eyJhdXRoIjpudWxsLCJzZXJ2aWNlIjp0cnVlLCJtc2dJREhlYWRlcktleSI6IlgtTnVtYWZsb3ctSWQiLCJzdG9yZSI6eyJ1cmwiOiJyZWRpczovL3JlZGlzOjYzNzkifX19LCJzaW5rIjp7InVkc2luayI6eyJjb250YWluZXIiOnsiaW1hZ2UiOiJzZXJ2ZXNpbms6MC4xIiwiZW52IjpbeyJuYW1lIjoiTlVNQUZMT1dfQ0FMTEJBQ0tfVVJMX0tFWSIsInZhbHVlIjoiWC1OdW1hZmxvdy1DYWxsYmFjay1VcmwifSx7Im5hbWUiOiJOVU1BRkxPV19NU0dfSURfSEVBREVSX0tFWSIsInZhbHVlIjoiWC1OdW1hZmxvdy1JZCJ9XSwicmVzb3VyY2VzIjp7fSwiaW1hZ2VQdWxsUG9saWN5IjoiTmV2ZXIifX0sInJldHJ5U3RyYXRlZ3kiOnt9fSwiY29udGFpbmVyVGVtcGxhdGUiOnsicmVzb3VyY2VzIjp7fSwiZW52IjpbeyJuYW1lIjoiTlVNQUZMT1dfQ0FMTEJBQ0tfRU5BQkxFRCIsInZhbHVlIjoidHJ1ZSJ9XX0sImxpbWl0cyI6eyJyZWFkQmF0Y2hTaXplIjo1MDAsInJlYWRUaW1lb3V0IjoiMXMifSwic2NhbGUiOnt9LCJ1cGRhdGVTdHJhdGVneSI6e30sImxpZmVjeWNsZSI6e319LCJzdGF0dXMiOnsicmVwbGljYXMiOjAsImRlc2lyZWRSZXBsaWNhcyI6MCwibGFzdFVwZGF0ZWQiOm51bGwsImxhc3RTY2FsZWRBdCI6bnVsbH19"),
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
            drain_timeout_secs: 120,
            store_type: StoreType::Nats,
            host_ip: "localhost".into(),
            api_auth_token: None,
            pipeline_spec: PipelineDCG {
                vertices: vec![Vertex {
                    name: "source".into(),
                }],
                edges: vec![],
            },
            ..Default::default()
        };
        assert_eq!(settings, expected_config);
    }
}
