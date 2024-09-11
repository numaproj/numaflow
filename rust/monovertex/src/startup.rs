use crate::config::config;
use crate::error::Error;
use crate::sink::{FB_SINK_SERVER_INFO_FILE, SINK_SERVER_INFO_FILE};
use crate::source::SOURCE_SERVER_INFO_FILE;
use crate::transformer::TRANSFORMER_SERVER_INFO_FILE;
use crate::{error, server_info};
use tokio_util::sync::CancellationToken;
use tracing::warn;

pub async fn check_compatibility(cln_token: &CancellationToken) -> error::Result<()> {
    server_info::check_for_server_compatibility(SOURCE_SERVER_INFO_FILE, cln_token.clone())
        .await
        .map_err(|e| {
            warn!("Error waiting for source server info file: {:?}", e);
            Error::ForwarderError("Error waiting for server info file".to_string())
        })?;

    server_info::check_for_server_compatibility(SINK_SERVER_INFO_FILE, cln_token.clone())
        .await
        .map_err(|e| {
            error!("Error waiting for sink server info file: {:?}", e);
            Error::ForwarderError("Error waiting for server info file".to_string())
        })?;

    if config().is_transformer_enabled {
        server_info::check_for_server_compatibility(
            TRANSFORMER_SERVER_INFO_FILE,
            cln_token.clone(),
        )
        .await
        .map_err(|e| {
            error!("Error waiting for transformer server info file: {:?}", e);
            Error::ForwarderError("Error waiting for server info file".to_string())
        })?;
    }

    if config().is_fallback_enabled {
        server_info::check_for_server_compatibility(FB_SINK_SERVER_INFO_FILE, cln_token.clone())
            .await
            .map_err(|e| {
                warn!("Error waiting for fallback sink server info file: {:?}", e);
                Error::ForwarderError("Error waiting for server info file".to_string())
            })?;
    }
    Ok(())
}
