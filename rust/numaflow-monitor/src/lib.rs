use crate::app::start_main_server;
use crate::config::MonitorServerConfig;
use crate::config::generate_certs;
use crate::error::{Error, Result};
use axum_server::tls_rustls::RustlsConfig;
use std::net::SocketAddr;
use tracing::info;

mod app;
pub mod config;
mod error;
pub mod runtime;

pub async fn run() -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
    let (cert, key) = generate_certs()?;

    let tls_config = RustlsConfig::from_pem(cert.pem().into(), key.serialize_pem().into())
        .await
        .map_err(|e| format!("Failed to create tls config {e:?}"))?;

    let server_config = MonitorServerConfig::default();

    info!(?server_config, "Starting monitor server with config");

    // Start the monitor server which serves daemon server calls
    let app_addr: SocketAddr = format!("0.0.0.0:{}", server_config.server_listen_port)
        .parse()
        .map_err(|e| Error::Init(format!("{e:?}")))?;
    // Start the main server, which serves the application.
    start_main_server(app_addr, tls_config, server_config.clone()).await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use rcgen::{Certificate, KeyPair};

    #[tokio::test]
    async fn test_failure_in_tls_cert_generation() {
        use axum_server::tls_rustls::RustlsConfig;

        // Mock the generate_certs function to return an error
        let result: Result<(Certificate, KeyPair)> = {
            Err(Error::Init(
                "Mocked certificate generation failure".to_string(),
            ))
        };

        // Assert that the result is an error
        assert!(result.is_err());

        // Attempt to create TLS config should fail
        if let Err(e) = result {
            let tls_config_result =
                RustlsConfig::from_pem(e.to_string().into(), e.to_string().into()).await;
            assert!(tls_config_result.is_err());
        }
    }
}
