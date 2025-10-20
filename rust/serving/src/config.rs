use crate::{
    Error::{self, ParseConfig},
    pipeline::PipelineDCG,
};
use base64::Engine;
use base64::prelude::BASE64_STANDARD;
use bytes::Bytes;
use rcgen::{Certificate, CertifiedKey, KeyPair, generate_simple_self_signed};
use serde::{Deserialize, Serialize};
use serde_json::from_slice;
use std::collections::HashMap;
use std::fmt::Debug;
use tracing::info;

const DEFAULT_SERVING_HTTPS_PORT: u16 = 8443;
pub const ENV_MIN_PIPELINE_SPEC: &str = "NUMAFLOW_SERVING_MIN_PIPELINE_SPEC";
pub const DEFAULT_ID_HEADER: &str = "X-Numaflow-Id";
pub const DEFAULT_POD_HASH_KEY: &str = "X-Numaflow-Pod-Hash";
pub const DEFAULT_CALLBACK_URL_HEADER_KEY: &str = "X-Numaflow-Callback-Url";
const DEFAULT_GRPC_MAX_MESSAGE_SIZE: usize = 64 * 1024 * 1024; // 64 MB
const DEFAULT_SERVING_STORE_SOCKET: &str = "/var/run/numaflow/serving.sock";
const DEFAULT_SERVING_STORE_SERVER_INFO_FILE: &str = "/var/run/numaflow/serving-server-info";
const ENV_NUMAFLOW_SERVING_JETSTREAM_USER: &str = "NUMAFLOW_ISBSVC_JETSTREAM_USER";
const ENV_NUMAFLOW_SERVING_JETSTREAM_PASSWORD: &str = "NUMAFLOW_ISBSVC_JETSTREAM_PASSWORD";
const ENV_NUMAFLOW_SERVING_CALLBACK_STORE: &str = "NUMAFLOW_SERVING_CALLBACK_STORE";
const ENV_NUMAFLOW_SERVING_RESPONSE_STORE: &str = "NUMAFLOW_SERVING_RESPONSE_STORE";
const ENV_NUMAFLOW_SERVING_STATUS_STORE: &str = "NUMAFLOW_SERVING_STATUS_STORE";
const ENV_NUMAFLOW_SERVING_SPEC: &str = "NUMAFLOW_SERVING_SPEC";
const ENV_NUMAFLOW_POD: &str = "NUMAFLOW_POD";

pub fn generate_certs() -> Result<(Certificate, KeyPair), String> {
    let CertifiedKey { cert, signing_key } = generate_simple_self_signed(vec!["localhost".into()])
        .map_err(|e| format!("Failed to generate cert {e:?}"))?;
    Ok((cert, signing_key))
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub(crate) enum RequestType {
    Sse,
    Sync,
    Async,
}

impl TryInto<Bytes> for RequestType {
    type Error = serde_json::Error;

    fn try_into(self) -> Result<Bytes, Self::Error> {
        Ok(Bytes::from(serde_json::to_vec(&self)?))
    }
}

impl TryFrom<Bytes> for RequestType {
    type Error = serde_json::Error;

    fn try_from(value: Bytes) -> Result<Self, Self::Error> {
        serde_json::from_slice(&value)
    }
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
    pub tid_header: &'static str,
    pub pod_hash: &'static str,
    pub app_listen_https_port: u16,
    pub app_listen_http_port: Option<u16>,
    pub metrics_server_listen_port: u16,
    pub upstream_addr: &'static str,
    pub drain_timeout_secs: u64,
    pub store_type: StoreType,
    pub js_callback_store: &'static str,
    pub js_response_store: &'static str,
    pub js_status_store: &'static str,
    pub api_auth_token: Option<String>,
    pub pipeline_spec: PipelineDCG,
    pub nats_basic_auth: Option<(String, String)>,
    pub js_message_stream: &'static str,
    pub jetstream_url: &'static str,
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
            tid_header: DEFAULT_ID_HEADER,
            pod_hash: "0",
            app_listen_https_port: DEFAULT_SERVING_HTTPS_PORT,
            app_listen_http_port: None,
            metrics_server_listen_port: 3001,
            upstream_addr: "localhost:8888",
            drain_timeout_secs: 600,
            store_type: StoreType::default(),
            js_callback_store: "callback-kv",
            js_response_store: "response-kv",
            js_status_store: "status-kv",
            api_auth_token: None,
            pipeline_spec: Default::default(),
            nats_basic_auth: None,
            js_message_stream: "test-stream",
            jetstream_url: "localhost:4222",
        }
    }
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

        let nats_username = env_vars.get(ENV_NUMAFLOW_SERVING_JETSTREAM_USER);
        let nats_basic_auth = match nats_username {
            Some(username) => {
                let nats_passwd = env_vars
                    .get("NUMAFLOW_ISBSVC_JETSTREAM_PASSWORD")
                    .ok_or_else(|| {
                        ParseConfig(format!(
                    "Environment variable '{ENV_NUMAFLOW_SERVING_JETSTREAM_USER}' is set, but '{ENV_NUMAFLOW_SERVING_JETSTREAM_PASSWORD}' is not set"
                ))})?;
                Some((username.into(), nats_passwd.into()))
            }
            None => None,
        };

        let js_cb_store = env_vars
            .get(ENV_NUMAFLOW_SERVING_CALLBACK_STORE)
            .ok_or_else(|| {
                ParseConfig("Serving store is default, but environment variable is not set".into())
            })?;

        let js_response_store = env_vars
            .get(ENV_NUMAFLOW_SERVING_RESPONSE_STORE)
            .ok_or_else(|| {
                ParseConfig("Serving store is default, but environment variable is not set".into())
            })?;

        let js_status_store = env_vars
            .get(ENV_NUMAFLOW_SERVING_STATUS_STORE)
            .ok_or_else(|| {
                ParseConfig("Serving store is default, but environment variable is not set".into())
            })?;

        let mut settings = Settings {
            pipeline_spec,
            js_callback_store: Box::leak(js_cb_store.clone().into_boxed_str()),
            nats_basic_auth,
            js_message_stream: Box::leak(js_source_spec.stream.into_boxed_str()),
            jetstream_url: Box::leak(js_source_spec.url.into_boxed_str()),
            js_response_store: Box::leak(js_response_store.to_string().into_boxed_str()),
            js_status_store: Box::leak(js_status_store.to_string().into_boxed_str()),
            ..Default::default()
        };

        let serving_spec = env_vars.get(ENV_NUMAFLOW_SERVING_SPEC).ok_or_else(|| {
            ParseConfig("Environment variable NUMAFLOW_SERVING_SPEC is not set".into())
        })?;

        let serving_spec_decoded = BASE64_STANDARD
            .decode(serving_spec.as_bytes())
            .map_err(|e| {
                ParseConfig(
                    format!(
                        "Failed to base64 decode value of environment variable '{ENV_NUMAFLOW_SERVING_SPEC}'. value='{serving_spec}'. Err={e:?}"
                    )
                )
            })?;
        let serving_spec: numaflow_models::models::ServingSpec =
            from_slice(serving_spec_decoded.as_slice()).map_err(|e| {
                ParseConfig(
                    format!(
                        "Failed to base64 decode value of environment variable '{ENV_NUMAFLOW_SERVING_SPEC}'. value='{serving_spec}'. Err={e:?}"
                    )
                )
            })?;

        // Configure ports from serving spec, with defaults
        if let Some(ports) = &serving_spec.ports {
            if let Some(https_port) = ports.https {
                settings.app_listen_https_port = https_port as u16;
            }
            settings.app_listen_http_port = ports.http.map(|port| port as u16);
        }

        settings.store_type = if serving_spec.store.is_some() {
            StoreType::UserDefined(UserDefinedStoreConfig::default())
        } else {
            StoreType::Nats
        };

        settings.tid_header = Box::leak(serving_spec.msg_id_header_key.into_boxed_str());
        settings.pod_hash = Box::leak(
            env_vars
                .get(ENV_NUMAFLOW_POD)
                .unwrap()
                .split('-')
                .next_back()
                .unwrap_or("0")
                .to_string()
                .into_boxed_str(),
        );

        settings.drain_timeout_secs =
            serving_spec.request_timeout_seconds.unwrap_or(120).max(1) as u64; // Ensure timeout is at least 1 second

        if let Some(auth) = serving_spec.auth {
            let token = auth.token.unwrap();
            let auth_token = get_secret_from_volume(&token.name, &token.key)
                .map_err(|e| ParseConfig(e.to_string()))?;
            settings.api_auth_token = Some(auth_token);
        }

        info!("Settings {:?}", settings);
        Ok(settings)
    }
}

#[cfg(test)]
const SECRET_BASE_PATH: &str = "/tmp/numaflow";

#[cfg(not(test))]
const SECRET_BASE_PATH: &str = "/var/numaflow/secrets";

/// Retrieve value from mounted secret volume
/// "/var/numaflow/secrets/${secretRef.name}/${secretRef.key}" is expected to be the file path
pub(crate) fn get_secret_from_volume(name: &str, key: &str) -> Result<String, String> {
    let path = format!("{SECRET_BASE_PATH}/{name}/{key}");
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
        assert_eq!(settings.pod_hash, "0");
        assert_eq!(settings.app_listen_https_port, 8443);
        assert_eq!(settings.app_listen_http_port, None);
        assert_eq!(settings.metrics_server_listen_port, 3001);
        assert_eq!(settings.upstream_addr, "localhost:8888");
        assert_eq!(settings.drain_timeout_secs, 600);
        assert_eq!(settings.store_type, StoreType::Nats,);
    }

    #[test]
    fn test_pipeline_config_parse() {
        // Set up the environment variables
        let env_vars = [
            (ENV_NUMAFLOW_POD, "serving-server-kddc"),
            ("NUMAFLOW_ISBSVC_JETSTREAM_USER", "testuser"),
            ("NUMAFLOW_ISBSVC_JETSTREAM_PASSWORD", "testpasswd"),
            ("NUMAFLOW_SERVING_CALLBACK_STORE", "test-kv-store"),
            ("NUMAFLOW_SERVING_STATUS_STORE", "test-kv-store"),
            ("NUMAFLOW_SERVING_RESPONSE_STORE", "test-kv-store"),
            (
                "NUMAFLOW_SERVING_SPEC",
                "eyJhdXRoIjpudWxsLCJzZXJ2aWNlIjp0cnVlLCJtc2dJREhlYWRlcktleSI6IlgtTnVtYWZsb3ctSWQiLCJwb3J0cyI6eyJodHRwcyI6ODQ0M319",
            ),
            (
                ENV_MIN_PIPELINE_SPEC,
                "eyJ2ZXJ0aWNlcyI6W3sibmFtZSI6ImluIiwic291cmNlIjp7ImpldHN0cmVhbSI6eyJ1cmwiOiJuYXRzOi8vaXNic3ZjLWRlZmF1bHQtanMtc3ZjLmRlZmF1bHQuc3ZjOjQyMjIiLCJzdHJlYW0iOiJzZXJ2aW5nLXNvdXJjZS1zaW1wbGUtcGlwZWxpbmUiLCJ0bHMiOm51bGwsImF1dGgiOnsiYmFzaWMiOnsidXNlciI6eyJuYW1lIjoiaXNic3ZjLWRlZmF1bHQtanMtY2xpZW50LWF1dGgiLCJrZXkiOiJjbGllbnQtYXV0aC11c2VyIn0sInBhc3N3b3JkIjp7Im5hbWUiOiJpc2JzdmMtZGVmYXVsdC1qcy1jbGllbnQtYXV0aCIsImtleSI6ImNsaWVudC1hdXRoLXBhc3N3b3JkIn19fX19LCJjb250YWluZXJUZW1wbGF0ZSI6eyJyZXNvdXJjZXMiOnt9LCJlbnYiOlt7Im5hbWUiOiJOVU1BRkxPV19DQUxMQkFDS19FTkFCTEVEIiwidmFsdWUiOiJ0cnVlIn0seyJuYW1lIjoiTlVNQUZMT1dfU0VSVklOR19TT1VSQ0VfU0VUVElOR1MiLCJ2YWx1ZSI6ImV5SmhkWFJvSWpwdWRXeHNMQ0p6WlhKMmFXTmxJanAwY25WbExDSnRjMmRKUkVobFlXUmxja3RsZVNJNklsZ3RUblZ0WVdac2IzY3RTV1FpZlE9PSJ9LHsibmFtZSI6Ik5VTUFGTE9XX1NFUlZJTkdfS1ZfU1RPUkUiLCJ2YWx1ZSI6InNlcnZpbmctc3RvcmUtc2ltcGxlLXBpcGVsaW5lX1NFUlZJTkdfS1ZfU1RPUkUifV19LCJzY2FsZSI6eyJtaW4iOjEsIm1heCI6MX0sImluaXRDb250YWluZXJzIjpbeyJuYW1lIjoidmFsaWRhdGUtc3RyZWFtLWluaXQiLCJpbWFnZSI6InF1YXkuaW8vbnVtYXByb2ovbnVtYWZsb3c6ODA4MkU2NTYtQTAxOS00QjE1LUE2ODQtNTg2RkM3RDJDQTFGIiwiYXJncyI6WyJpc2JzdmMtdmFsaWRhdGUiLCItLWlzYnN2Yy10eXBlPWpldHN0cmVhbSIsIi0tYnVmZmVycz1zZXJ2aW5nLXNvdXJjZS1zaW1wbGUtcGlwZWxpbmUiXSwiZW52IjpbeyJuYW1lIjoiTlVNQUZMT1dfUElQRUxJTkVfTkFNRSIsInZhbHVlIjoicy1zaW1wbGUtcGlwZWxpbmUifSx7Im5hbWUiOiJHT0RFQlVHIn0seyJuYW1lIjoiTlVNQUZMT1dfSVNCU1ZDX0NPTkZJRyIsInZhbHVlIjoiZXlKcVpYUnpkSEpsWVcwaU9uc2lkWEpzSWpvaWJtRjBjem92TDJselluTjJZeTFrWldaaGRXeDBMV3B6TFhOMll5NWtaV1poZFd4MExuTjJZem8wTWpJeUlpd2lZWFYwYUNJNmV5SmlZWE5wWXlJNmV5SjFjMlZ5SWpwN0ltNWhiV1VpT2lKcGMySnpkbU10WkdWbVlYVnNkQzFxY3kxamJHbGxiblF0WVhWMGFDSXNJbXRsZVNJNkltTnNhV1Z1ZEMxaGRYUm9MWFZ6WlhJaWZTd2ljR0Z6YzNkdmNtUWlPbnNpYm1GdFpTSTZJbWx6WW5OMll5MWtaV1poZFd4MExXcHpMV05zYVdWdWRDMWhkWFJvSWl3aWEyVjVJam9pWTJ4cFpXNTBMV0YxZEdndGNHRnpjM2R2Y21RaWZYMTlMQ0p6ZEhKbFlXMURiMjVtYVdjaU9pSmpiMjV6ZFcxbGNqcGNiaUFnWVdOcmQyRnBkRG9nTmpCelhHNGdJRzFoZUdGamEzQmxibVJwYm1jNklESTFNREF3WEc1dmRHSjFZMnRsZERwY2JpQWdhR2x6ZEc5eWVUb2dNVnh1SUNCdFlYaGllWFJsY3pvZ01GeHVJQ0J0WVhoMllXeDFaWE5wZW1VNklEQmNiaUFnY21Wd2JHbGpZWE02SUROY2JpQWdjM1J2Y21GblpUb2dNRnh1SUNCMGRHdzZJRE5vWEc1d2NtOWpZblZqYTJWME9seHVJQ0JvYVhOMGIzSjVPaUF4WEc0Z0lHMWhlR0o1ZEdWek9pQXdYRzRnSUcxaGVIWmhiSFZsYzJsNlpUb2dNRnh1SUNCeVpYQnNhV05oY3pvZ00xeHVJQ0J6ZEc5eVlXZGxPaUF3WEc0Z0lIUjBiRG9nTnpKb1hHNXpkSEpsWVcwNlhHNGdJR1IxY0d4cFkyRjBaWE02SURZd2MxeHVJQ0J0WVhoaFoyVTZJRGN5YUZ4dUlDQnRZWGhpZVhSbGN6b2dMVEZjYmlBZ2JXRjRiWE5uY3pvZ01UQXdNREF3WEc0Z0lISmxjR3hwWTJGek9pQXpYRzRnSUhKbGRHVnVkR2x2YmpvZ01GeHVJQ0J6ZEc5eVlXZGxPaUF3WEc0aWZYMD0ifSx7Im5hbWUiOiJOVU1BRkxPV19JU0JTVkNfSkVUU1RSRUFNX1VSTCIsInZhbHVlIjoibmF0czovL2lzYnN2Yy1kZWZhdWx0LWpzLXN2Yy5kZWZhdWx0LnN2Yzo0MjIyIn0seyJuYW1lIjoiTlVNQUZMT1dfSVNCU1ZDX0pFVFNUUkVBTV9UTFNfRU5BQkxFRCIsInZhbHVlIjoiZmFsc2UifSx7Im5hbWUiOiJOVU1BRkxPV19JU0JTVkNfSkVUU1RSRUFNX1VTRVIiLCJ2YWx1ZUZyb20iOnsic2VjcmV0S2V5UmVmIjp7Im5hbWUiOiJpc2JzdmMtZGVmYXVsdC1qcy1jbGllbnQtYXV0aCIsImtleSI6ImNsaWVudC1hdXRoLXVzZXIifX19LHsibmFtZSI6Ik5VTUFGTE9XX0lTQlNWQ19KRVRTVFJFQU1fUEFTU1dPUkQiLCJ2YWx1ZUZyb20iOnsic2VjcmV0S2V5UmVmIjp7Im5hbWUiOiJpc2JzdmMtZGVmYXVsdC1qcy1jbGllbnQtYXV0aCIsImtleSI6ImNsaWVudC1hdXRoLXBhc3N3b3JkIn19fV0sInJlc291cmNlcyI6eyJyZXF1ZXN0cyI6eyJjcHUiOiIxMDBtIiwibWVtb3J5IjoiMTI4TWkifX0sImltYWdlUHVsbFBvbGljeSI6IklmTm90UHJlc2VudCJ9XSwidXBkYXRlU3RyYXRlZ3kiOnsidHlwZSI6IlJvbGxpbmdVcGRhdGUiLCJyb2xsaW5nVXBkYXRlIjp7Im1heFVuYXZhaWxhYmxlIjoiMjUlIn19fSx7Im5hbWUiOiJjYXQiLCJ1ZGYiOnsiY29udGFpbmVyIjp7ImltYWdlIjoicXVheS5pby9udW1haW8vbnVtYWZsb3ctZ28vbWFwLWZvcndhcmQtbWVzc2FnZTpzdGFibGUiLCJyZXNvdXJjZXMiOnt9fSwiYnVpbHRpbiI6bnVsbCwiZ3JvdXBCeSI6bnVsbH0sImNvbnRhaW5lclRlbXBsYXRlIjp7InJlc291cmNlcyI6e30sImVudiI6W3sibmFtZSI6Ik5VTUFGTE9XX0NBTExCQUNLX0VOQUJMRUQiLCJ2YWx1ZSI6InRydWUifSx7Im5hbWUiOiJOVU1BRkxPV19TRVJWSU5HX1NPVVJDRV9TRVRUSU5HUyIsInZhbHVlIjoiZXlKaGRYUm9JanB1ZFd4c0xDSnpaWEoyYVdObElqcDBjblZsTENKdGMyZEpSRWhsWVdSbGNrdGxlU0k2SWxndFRuVnRZV1pzYjNjdFNXUWlmUT09In0seyJuYW1lIjoiTlVNQUZMT1dfU0VSVklOR19LVl9TVE9SRSIsInZhbHVlIjoic2VydmluZy1zdG9yZS1zaW1wbGUtcGlwZWxpbmVfU0VSVklOR19LVl9TVE9SRSJ9XX0sInNjYWxlIjp7Im1pbiI6MSwibWF4IjoxfSwidXBkYXRlU3RyYXRlZ3kiOnsidHlwZSI6IlJvbGxpbmdVcGRhdGUiLCJyb2xsaW5nVXBkYXRlIjp7Im1heFVuYXZhaWxhYmxlIjoiMjUlIn19fSx7Im5hbWUiOiJvdXQiLCJzaW5rIjp7InVkc2luayI6eyJjb250YWluZXIiOnsiaW1hZ2UiOiJxdWF5LmlvL251bWFpby9udW1hZmxvdy1nby9zaW5rLXNlcnZlOnN0YWJsZSIsInJlc291cmNlcyI6e319fSwicmV0cnlTdHJhdGVneSI6e319LCJjb250YWluZXJUZW1wbGF0ZSI6eyJyZXNvdXJjZXMiOnt9LCJlbnYiOlt7Im5hbWUiOiJOVU1BRkxPV19DQUxMQkFDS19FTkFCTEVEIiwidmFsdWUiOiJ0cnVlIn0seyJuYW1lIjoiTlVNQUZMT1dfU0VSVklOR19TT1VSQ0VfU0VUVElOR1MiLCJ2YWx1ZSI6ImV5SmhkWFJvSWpwdWRXeHNMQ0p6WlhKMmFXTmxJanAwY25WbExDSnRjMmRKUkVobFlXUmxja3RsZVNJNklsZ3RUblZ0WVdac2IzY3RTV1FpZlE9PSJ9LHsibmFtZSI6Ik5VTUFGTE9XX1NFUlZJTkdfS1ZfU1RPUkUiLCJ2YWx1ZSI6InNlcnZpbmctc3RvcmUtc2ltcGxlLXBpcGVsaW5lX1NFUlZJTkdfS1ZfU1RPUkUifV19LCJzY2FsZSI6eyJtaW4iOjEsIm1heCI6MX0sInVwZGF0ZVN0cmF0ZWd5Ijp7InR5cGUiOiJSb2xsaW5nVXBkYXRlIiwicm9sbGluZ1VwZGF0ZSI6eyJtYXhVbmF2YWlsYWJsZSI6IjI1JSJ9fX1dLCJlZGdlcyI6W3siZnJvbSI6ImluIiwidG8iOiJjYXQiLCJjb25kaXRpb25zIjpudWxsfSx7ImZyb20iOiJjYXQiLCJ0byI6Im91dCIsImNvbmRpdGlvbnMiOm51bGx9XSwibGlmZWN5Y2xlIjp7fSwid2F0ZXJtYXJrIjp7fX0=",
            ),
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
            pod_hash: "kddc".into(),
            app_listen_https_port: 8443,
            app_listen_http_port: None,
            metrics_server_listen_port: 3001,
            upstream_addr: "localhost:8888".into(),
            drain_timeout_secs: 120,
            store_type: StoreType::Nats,
            api_auth_token: None,
            js_callback_store: "test-kv-store".into(),
            js_status_store: "test-kv-store".into(),
            js_response_store: "test-kv-store".into(),
            nats_basic_auth: Some(("testuser".into(), "testpasswd".into())),
            js_message_stream: "serving-source-simple-pipeline".into(),
            jetstream_url: "nats://isbsvc-default-js-svc.default.svc:4222".into(),
            pipeline_spec: PipelineDCG {
                vertices: vec![
                    Vertex { name: "in".into() },
                    Vertex { name: "cat".into() },
                    Vertex { name: "out".into() },
                ],
                edges: vec![
                    Edge {
                        from: "in".into(),
                        to: "cat".into(),
                        conditions: None,
                    },
                    Edge {
                        from: "cat".into(),
                        to: "out".into(),
                        conditions: None,
                    },
                ],
            },
            ..Default::default()
        };
        assert_eq!(settings, expected_config);
    }
}
