use numaflow_pb::clients::map::{MapRequest, MapResponse, map_client::MapClient};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::Channel;
use tonic::{Request, Streaming};

use crate::error::{Result, UdfClientError};
use crate::wire::map::handshake_request;

/// An established MapFn stream after a validated start-of-transmission handshake.
pub(super) struct MapRpcStream {
    request_tx: mpsc::Sender<MapRequest>,
    response_stream: Streaming<MapResponse>,
}

impl MapRpcStream {
    /// Opens MapFn, sends its handshake, and validates the handshake response.
    pub(super) async fn open(
        mut client: MapClient<Channel>,
        request_buffer: usize,
    ) -> Result<Self> {
        if request_buffer == 0 {
            return Err(UdfClientError::InvalidConfig(
                "map request buffer must be greater than zero".to_string(),
            ));
        }

        let (request_tx, request_rx) = mpsc::channel(request_buffer);
        request_tx
            .send(handshake_request())
            .await
            .map_err(|_| UdfClientError::RequestStreamClosed)?;

        let mut response_stream = client
            .map_fn(Request::new(ReceiverStream::new(request_rx)))
            .await
            .map_err(UdfClientError::MapFnStart)?
            .into_inner();

        let handshake = response_stream
            .message()
            .await
            .map_err(UdfClientError::Grpc)?
            .ok_or(UdfClientError::HandshakeStreamClosed)?;

        if handshake.handshake.is_none_or(|value| !value.sot) {
            return Err(UdfClientError::InvalidHandshake);
        }

        Ok(Self {
            request_tx,
            response_stream,
        })
    }

    /// Splits the established stream into its raw sender and response stream.
    pub(super) fn into_parts(self) -> (mpsc::Sender<MapRequest>, Streaming<MapResponse>) {
        (self.request_tx, self.response_stream)
    }
}
