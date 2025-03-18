use rcgen::{generate_simple_self_signed, Certificate, CertifiedKey, KeyPair};

pub fn generate_certs() -> std::result::Result<(Certificate, KeyPair), String> {
    let CertifiedKey { cert, key_pair } = generate_simple_self_signed(vec!["localhost".into()])
        .map_err(|e| format!("Failed to generate cert {:?}", e))?;
    Ok((cert, key_pair))
}

pub(crate) mod server {
    const DEFAULT_METRICS_PORT: u16 = 2470;
    const DEFAULT_SHUTDOWN_DURATION: u64 = 30;
    #[derive(Debug, Clone)]
    pub struct MonitorServerConfig {
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
}

pub(crate) mod info {
    const DEFAULT_RUNTIME_APPLICATION_ERRORS_PATH: &str =
        "/var/numaflow/runtime/application-errors";

    pub(crate) struct RuntimeInfoConfig {
        pub app_error_path: String,
    }
    impl Default for RuntimeInfoConfig {
        fn default() -> Self {
            Self {
                app_error_path: DEFAULT_RUNTIME_APPLICATION_ERRORS_PATH.to_string(),
            }
        }
    }
}
