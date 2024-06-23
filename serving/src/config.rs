use std::fmt::Debug;
use std::path::Path;
use std::{env, sync::OnceLock};

use config::Config;
use serde::Deserialize;
use tracing::info;

use crate::{Error, Result};

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
    pub addr: String,
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
    pub pipeline_spec_path: String,
    /// The IP address of the numaserve pod. This will be used to construct the value for X-Numaflow-Callback-Url header
    pub host_ip: String,
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
                config::Environment::with_prefix("APP")
                    .prefix_separator("_")
                    .separator("."),
            )
            .build()
            .map_err(|e| format!("generating runtime configuration: {e:?}"))?;
        Ok(settings
            .try_deserialize::<Self>()
            .map_err(|e| format!("parsing runtime configuration: {e:?}"))?)
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
        assert_eq!(settings.jetstream.addr, "localhost:4222");
        assert_eq!(settings.redis.addr, "redis://127.0.0.1/");
        assert_eq!(settings.redis.max_tasks, 50);
        assert_eq!(settings.redis.retries, 5);
        assert_eq!(settings.redis.retries_duration_millis, 100);
        assert_eq!(settings.pipeline_spec_path, "./config/pipeline_spec.json");
    }
}
