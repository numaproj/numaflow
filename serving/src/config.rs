use std::fmt::Debug;
use std::path::Path;
use std::{env, sync::OnceLock};

use base64::prelude::BASE64_STANDARD;
use base64::Engine;
use config::Config;
use serde::{Deserialize, Serialize};
use tracing::info;

use crate::{Error, Result};

const ENV_PREFIX: &str = "NUMAFLOW_SERVING";
const ENV_PIPELINE_SPEC: &str = "NUMAFLOW_SERVING_PIPELINE_SPEC";
const ENV_NUMAFLOW_SERVING_SOURCE_OBJECT: &str = "NUMAFLOW_SERVING_SOURCE_OBJECT";
const ENV_NUMAFLOW_SERVING_JETSTREAM_URL: &str = "NUMAFLOW_ISBSVC_JETSTREAM_URL";
const ENV_NUMAFLOW_SERVING_JETSTREAM_STREAM: &str = "NUMAFLOW_SERVING_JETSTREAM_STREAM";

pub fn config() -> &'static Settings {
    static CONF: OnceLock<Settings> = OnceLock::new();
    CONF.get_or_init(|| {
        let config_dir = env::var("CONFIG_PATH").unwrap_or_else(|_| {
            info!("Config directory is not specified, using default config directory: './config'");
            String::from("config")
        });

        match Settings::load(config_dir) {
            Ok(v) => v,
            Err(e) => {
                panic!("Failed to load configuration: {:?}", e);
            }
        }
    })
}

#[derive(Debug, Deserialize)]
pub struct JetStreamConfig {
    pub stream: String,
    pub url: String,
    pub user: Option<String>,
    pub password: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct RedisConfig {
    pub addr: String,
    pub max_tasks: usize,
    pub retries: usize,
    pub retries_duration_millis: u16,
}

#[derive(Debug, Deserialize)]
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

impl Settings {
    fn load<P: AsRef<Path>>(config_dir: P) -> Result<Self> {
        let config_dir = config_dir.as_ref();
        if !config_dir.is_dir() {
            return Err(Error::Other(format!(
                "Path {} is not a directory",
                config_dir.to_string_lossy()
            )));
        }

        let settings = Config::builder()
            .add_source(config::File::from(config_dir.join("default.toml")))
            .add_source(
                config::Environment::with_prefix(ENV_PREFIX)
                    .prefix_separator("_")
                    .separator("."),
            )
            .build()
            .map_err(|e| format!("generating runtime configuration: {e:?}"))?;

        let mut settings = settings
            .try_deserialize::<Self>()
            .map_err(|e| format!("parsing runtime configuration: {e:?}"))?;

        // Update JetStreamConfig from environment variables
        if let Ok(url) = env::var(ENV_NUMAFLOW_SERVING_JETSTREAM_URL) {
            settings.jetstream.url = url;
        }
        if let Ok(stream) = env::var(ENV_NUMAFLOW_SERVING_JETSTREAM_STREAM) {
            settings.jetstream.stream = stream;
        }

        let source_spec_encoded = env::var(ENV_NUMAFLOW_SERVING_SOURCE_OBJECT);

        match source_spec_encoded {
            Ok(source_spec_encoded) => {
                let source_spec_decoded = BASE64_STANDARD
                    .decode(source_spec_encoded.as_bytes())
                    .map_err(|e| format!("decoding NUMAFLOW_SERVING_SOURCE: {e:?}"))?;

                let source_spec = serde_json::from_slice::<Serving>(&source_spec_decoded)
                    .map_err(|e| format!("parsing NUMAFLOW_SERVING_SOURCE: {e:?}"))?;

                // Update tid_header from source_spec
                if let Some(msg_id_header_key) = source_spec.msg_id_header_key {
                    settings.tid_header = msg_id_header_key;
                }

                // Update redis.addr from source_spec, currently we only support redis as callback storage
                settings.redis.addr = source_spec.callback_storage.url;

                Ok(settings)
            }
            Err(_) => Ok(settings),
        }
    }
}

pub fn pipeline_spec() -> &'static Pipeline {
    static PIPELINE: OnceLock<Pipeline> = OnceLock::new();
    PIPELINE.get_or_init(|| match Pipeline::load() {
        Ok(pipeline) => pipeline,
        Err(e) => panic!("Failed to load pipeline: {:?}", e),
    })
}

// OperatorType is an enum that contains the types of operators
// that can be used in the conditions for the edge.
#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub enum OperatorType {
    #[serde(rename = "and")]
    And,
    #[serde(rename = "or")]
    Or,
    #[serde(rename = "not")]
    Not,
}

#[allow(dead_code)]
impl OperatorType {
    fn as_str(&self) -> &'static str {
        match self {
            OperatorType::And => "and",
            OperatorType::Or => "or",
            OperatorType::Not => "not",
        }
    }
}

// Tag is a struct that contains the information about the tags for the edge
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Tag {
    pub operator: Option<OperatorType>,
    pub values: Vec<String>,
}

// Conditions is a struct that contains the information about the conditions for the edge
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Conditions {
    pub tags: Option<Tag>,
}

// Edge is a struct that contains the information about the edge in the pipeline.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Edge {
    pub from: String,
    pub to: String,
    pub conditions: Option<Conditions>,
}

// Pipeline is a struct that contains the information about the pipeline.
#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde()]
pub struct Pipeline {
    pub vertices: Vec<Vertex>,
    pub edges: Vec<Edge>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Vertex {
    pub name: String,
}

impl Pipeline {
    pub fn load() -> Result<Self> {
        let pipeline = match env::var(ENV_PIPELINE_SPEC) {
            Ok(env_value) => {
                // If the environment variable is set, decode and parse the pipeline
                let decoded = BASE64_STANDARD
                    .decode(env_value.as_bytes())
                    .map_err(|e| format!("decoding pipeline from env: {e:?}"))?;

                serde_json::from_slice::<Pipeline>(&decoded)
                    .map_err(|e| format!("parsing pipeline from env: {e:?}"))?
            }
            Err(_) => {
                // If the environment variable is not set, read the pipeline from a file
                let file_path = "./config/pipeline_spec.json";
                let file_contents = std::fs::read_to_string(file_path)
                    .map_err(|e| format!("reading pipeline file: {e:?}"))?;
                serde_json::from_str::<Pipeline>(&file_contents)
                    .map_err(|e| format!("parsing pipeline file: {e:?}"))?
            }
        };
        Ok(pipeline)
    }
}

#[cfg(test)]
mod tests {
    use std::env;

    use super::*;

    #[test]
    fn test_config() {
        // Set up the environment variable for the config directory
        env::set_var("RUN_ENV", "Development");
        env::set_var("APP_HOST_IP", "10.244.0.6");
        env::set_var("CONFIG_PATH", "config");

        // Call the config method
        let settings = config();

        // Assert that the settings are as expected
        assert_eq!(settings.tid_header, "ID");
        assert_eq!(settings.app_listen_port, 3000);
        assert_eq!(settings.metrics_server_listen_port, 3001);
        assert_eq!(settings.upstream_addr, "localhost:8888");
        assert_eq!(settings.drain_timeout_secs, 10);
        assert_eq!(settings.jetstream.stream, "default");
        assert_eq!(settings.jetstream.url, "localhost:4222");
        assert_eq!(settings.redis.addr, "redis://127.0.0.1/");
        assert_eq!(settings.redis.max_tasks, 50);
        assert_eq!(settings.redis.retries, 5);
        assert_eq!(settings.redis.retries_duration_millis, 100);
    }
}
