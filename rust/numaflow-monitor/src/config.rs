//!  Config module for Sidecar monitor container
use rcgen::{Certificate, CertifiedKey, KeyPair, generate_simple_self_signed};

pub fn generate_certs() -> Result<(Certificate, KeyPair), String> {
    let CertifiedKey { cert, signing_key } = generate_simple_self_signed(vec!["localhost".into()])
        .map_err(|e| format!("Failed to generate cert {e:?}"))?;
    Ok((cert, signing_key))
}

const DEFAULT_METRICS_PORT: u16 = 2470;
const DEFAULT_SHUTDOWN_DURATION: u64 = 30;
pub(crate) const DEFAULT_RUNTIME_APPLICATION_ERRORS_PATH: &str =
    "/var/numaflow/runtime/application-errors";
pub(crate) const DEFAULT_MAX_ERROR_FILES_PER_CONTAINER: usize = 10;

#[derive(Debug, Clone)]
pub(crate) struct MonitorServerConfig {
    pub server_listen_port: u16,
    pub graceful_shutdown_duration: u64,
}

impl Default for MonitorServerConfig {
    fn default() -> Self {
        Self {
            server_listen_port: DEFAULT_METRICS_PORT,
            graceful_shutdown_duration: DEFAULT_SHUTDOWN_DURATION,
        }
    }
}

pub struct RuntimeInfoConfig {
    pub app_error_path: String,
    pub max_error_files_per_container: usize,
}
impl Default for RuntimeInfoConfig {
    fn default() -> Self {
        Self {
            app_error_path: DEFAULT_RUNTIME_APPLICATION_ERRORS_PATH.to_string(),
            max_error_files_per_container: DEFAULT_MAX_ERROR_FILES_PER_CONTAINER,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    // Successfully generates a self-signed certificate and key pair
    #[test]
    fn test_generate_certs_success() {
        let result = generate_certs();
        assert!(result.is_ok());
        let (cert, key_pair) = result.unwrap();
        assert!(!cert.pem().is_empty());
        assert!(!key_pair.serialize_der().is_empty());
    }

    // Test default values for MonitorServerConfig
    #[test]
    fn test_monitor_server_config_default() {
        let config = MonitorServerConfig::default();
        assert_eq!(config.server_listen_port, DEFAULT_METRICS_PORT);
        assert_eq!(config.graceful_shutdown_duration, DEFAULT_SHUTDOWN_DURATION);
    }

    // Test default values for RuntimeInfoConfig
    #[test]
    fn test_runtime_info_config_default() {
        let config = RuntimeInfoConfig::default();
        assert_eq!(
            config.app_error_path,
            DEFAULT_RUNTIME_APPLICATION_ERRORS_PATH.to_string()
        );
        assert_eq!(
            config.max_error_files_per_container,
            DEFAULT_MAX_ERROR_FILES_PER_CONTAINER
        );
    }
}
