use crate::error::{Error, Result};
use backoff::retry::Retry;
use backoff::strategy::fixed;
use numaflow_pb::clients::sideinput::SideInputResponse;
use numaflow_pb::clients::sideinput::side_input_client::SideInputClient;
use std::path::PathBuf;
use std::time::Duration;
use tokio::net::UnixStream;
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;
use tonic::Request;
use tonic::transport::{Channel, Endpoint, Uri};
use tower::service_fn;
use tracing::{info, warn};

pub(crate) struct UserDefinedSideInputClient {
    client: SideInputClient<Channel>,
}

impl UserDefinedSideInputClient {
    pub(crate) async fn new(
        uds_path: PathBuf,
        server_info_path: PathBuf,
        cln_token: CancellationToken,
    ) -> Result<Self> {
        let channel = create_rpc_channel(uds_path).await?;
        let client = SideInputClient::new(channel);

        numaflow_shared::server_info::sdk_server_info(server_info_path, cln_token.clone()).await?;

        Ok(Self { client })
    }

    pub(crate) async fn retrieve_side_input(&mut self) -> Result<SideInputResponse> {
        let response = self
            .client
            .retrieve_side_input(Request::new(()))
            .await
            .map_err(|e| Error::SideInput(format!("Failed to retrieve side input: {e:?}")))?;
        Ok(response.into_inner())
    }
}

/// Waits until the side-input server is ready, by doing health checks
pub(crate) async fn wait_until_sideinput_ready(
    cln_token: &CancellationToken,
    client: &mut UserDefinedSideInputClient,
) -> Result<()> {
    loop {
        if cln_token.is_cancelled() {
            return Err(Error::SideInput(
                "Cancellation token is cancelled".to_string(),
            ));
        }
        match client.client.is_ready(Request::new(())).await {
            Ok(_) => break,
            Err(_) => sleep(Duration::from_secs(1)).await,
        }
        info!("Waiting for side-input client to be ready...");
    }
    Ok(())
}

async fn create_rpc_channel(socket_path: PathBuf) -> Result<Channel> {
    const RECONNECT_INTERVAL: u64 = 1000;
    const MAX_RECONNECT_ATTEMPTS: usize = usize::MAX;

    let interval = fixed::Interval::from_millis(RECONNECT_INTERVAL).take(MAX_RECONNECT_ATTEMPTS);
    let channel = Retry::new(
        interval,
        async || match connect_with_uds(socket_path.clone()).await {
            Ok(channel) => Ok(channel),
            Err(e) => {
                warn!(error = ?e, ?socket_path, "Failed to connect to UDS socket");
                Err(Error::Connection(format!(
                    "Failed to connect {socket_path:?}: {e:?}"
                )))
            }
        },
        |_: &Error| true,
    )
    .await?;
    Ok(channel)
}

async fn connect_with_uds(uds_path: PathBuf) -> Result<Channel> {
    let channel = Endpoint::try_from("http://[::1]:50051")
        .map_err(|e| Error::Connection(format!("Failed to create endpoint: {e:?}")))?
        .connect_with_connector(service_fn(move |_: Uri| {
            let uds_socket = uds_path.clone();
            async move {
                Ok::<_, std::io::Error>(hyper_util::rt::TokioIo::new(
                    UnixStream::connect(uds_socket).await?,
                ))
            }
        }))
        .await
        .map_err(|e| Error::Connection(format!("Failed to connect: {e:?}")))?;
    Ok(channel)
}
