use std::net::SocketAddr;
use std::time::Duration;

use crate::config::config;
use crate::error::Error;
use crate::metrics::{start_metrics_https_server, LagReader, LagReaderBuilder, MetricsState};
use crate::sink::{FB_SINK_SERVER_INFO_FILE, SINK_SERVER_INFO_FILE};
use crate::sink_pb::sink_client::SinkClient;
use crate::source::SOURCE_SERVER_INFO_FILE;
use crate::source_pb::source_client::SourceClient;
use crate::sourcetransform_pb::source_transform_client::SourceTransformClient;
use crate::transformer::TRANSFORMER_SERVER_INFO_FILE;
use crate::{error, server_info};

use tokio::task::JoinHandle;
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;
use tonic::transport::Channel;
use tonic::Request;
use tracing::{info, warn};

pub(crate) async fn check_compatibility(cln_token: &CancellationToken) -> error::Result<()> {
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

pub(crate) async fn start_metrics_server(metrics_state: MetricsState) -> JoinHandle<()> {
    tokio::spawn(async {
        // Start the metrics server, which server the prometheus metrics.
        let metrics_addr: SocketAddr = format!("0.0.0.0:{}", &config().metrics_server_listen_port)
            .parse()
            .expect("Invalid address");

        if let Err(e) = start_metrics_https_server(metrics_addr, metrics_state).await {
            error!("Metrics server error: {:?}", e);
        }
    })
}

pub(crate) async fn create_lag_reader(lag_reader_grpc_client: SourceClient<Channel>) -> LagReader {
    LagReaderBuilder::new(lag_reader_grpc_client)
        .lag_checking_interval(Duration::from_secs(
            config().lag_check_interval_in_secs.into(),
        ))
        .refresh_interval(Duration::from_secs(
            config().lag_refresh_interval_in_secs.into(),
        ))
        .build()
}

pub(crate) async fn wait_until_ready(
    cln_token: CancellationToken,
    source_client: &mut SourceClient<Channel>,
    sink_client: &mut SinkClient<Channel>,
    transformer_client: &mut Option<SourceTransformClient<Channel>>,
    fb_sink_client: &mut Option<SinkClient<Channel>>,
) -> error::Result<()> {
    loop {
        if cln_token.is_cancelled() {
            return Err(Error::ForwarderError(
                "Cancellation token is cancelled".to_string(),
            ));
        }
        let source_ready = source_client.is_ready(Request::new(())).await.is_ok();
        if !source_ready {
            info!("UDSource is not ready, waiting...");
        }

        let sink_ready = sink_client.is_ready(Request::new(())).await.is_ok();
        if !sink_ready {
            info!("UDSink is not ready, waiting...");
        }

        let transformer_ready = if let Some(client) = transformer_client {
            let ready = client.is_ready(Request::new(())).await.is_ok();
            if !ready {
                info!("UDTransformer is not ready, waiting...");
            }
            ready
        } else {
            true
        };

        let fb_sink_ready = if let Some(client) = fb_sink_client {
            let ready = client.is_ready(Request::new(())).await.is_ok();
            if !ready {
                info!("Fallback Sink is not ready, waiting...");
            }
            ready
        } else {
            true
        };

        if source_ready && sink_ready && transformer_ready && fb_sink_ready {
            break;
        }

        sleep(Duration::from_secs(1)).await;
    }

    Ok(())
}
