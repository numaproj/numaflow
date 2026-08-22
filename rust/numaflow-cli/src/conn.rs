//! Minimal UDS gRPC channel connector for the non-facade commands (`side-input`, `ready`).
//!
//! Replicated from `numaflow-core`'s `shared::grpc::connect_with_uds` (which is `pub(crate)` and
//! not reachable from here). It's ~20 lines and self-contained.

use std::path::PathBuf;

use tokio::net::UnixStream;
use tonic::transport::{Channel, Endpoint, Uri};
use tower::service_fn;

/// Connect to a Unix domain socket and return a tonic [`Channel`]. The HTTP authority is a
/// throwaway placeholder — tonic requires a valid URI but the UDS connector ignores it.
pub async fn connect_uds(socket: PathBuf) -> Result<Channel, tonic::transport::Error> {
    Endpoint::try_from("http://[::1]:50051")?
        .connect_with_connector(service_fn(move |_: Uri| {
            let socket = socket.clone();
            async move {
                Ok::<_, std::io::Error>(hyper_util::rt::TokioIo::new(
                    UnixStream::connect(socket).await?,
                ))
            }
        }))
        .await
}
