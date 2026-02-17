use crate::shared::grpc;
use crate::shared::test_utils::server;
use crate::shared::test_utils::server::{TestServerHandle, start_server};
use crate::source::user_defined::new_source;
use crate::source::{Source, SourceType};
use crate::tracker::Tracker;
use crate::transformer::Transformer;
use crate::transformer::test_utils::{SourceTransformerTestHandle, start_source_transform_server};
use numaflow::shared::ServerExtras;
use numaflow::source;
use numaflow::sourcetransform::SourceTransformer;
use numaflow_pb::clients::source::source_client::SourceClient;
use numaflow_pb::clients::sourcetransformer::source_transform_client::SourceTransformClient;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

/// A handle to a source component.
///
/// Contains the source handle, server handle (for the source server),
/// and an optional source transformer handle.
pub(crate) struct SourceTestHandle {
    pub source_transformer_test_handle: Option<SourceTransformerTestHandle>,
    pub source: Source<crate::typ::WithoutRateLimiter>,
    pub server_handle: TestServerHandle,
}

impl SourceTestHandle {
    /// Create a user defined source component with the given source and transformer services.
    ///
    /// Initializes the sourcer and transformer handles along with their servers.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn create_ud_source<S, T>(
        source_svc: S,
        source_transformer_svc: Option<T>,
        batch_size: usize,
        cln_token: CancellationToken,
        tracker: Tracker,
    ) -> SourceTestHandle
    where
        T: SourceTransformer + Send + Sync + 'static,
        S: source::Sourcer + Send + Sync + 'static,
    {
        // create a transformer for this source if it is provided
        let mut transformer_test_handle = match source_transformer_svc {
            Some(transformer_svc) => {
                let server_handle = start_source_transform_server(transformer_svc);

                let mut client = SourceTransformClient::new(
                    server_handle
                        .create_rpc_channel()
                        .await
                        .expect("failed to create source transformer rpc channel"),
                );

                grpc::wait_until_transformer_ready(&cln_token, &mut client)
                    .await
                    .expect("failed to wait for source transformer server to be ready");

                let transformer = Transformer::new(
                    batch_size,
                    10,
                    Duration::from_secs(10),
                    client,
                    tracker.clone(),
                )
                .await
                .expect("failed to create source transformer");

                Some(SourceTransformerTestHandle {
                    server_handle,
                    transformer: Some(transformer),
                })
            }
            None => None,
        };

        // create the source
        let server_handle = start_source_server(source_svc);
        let mut client = SourceClient::new(
            server_handle
                .create_rpc_channel()
                .await
                .expect("failed to create source rpc channel"),
        );

        grpc::wait_until_source_ready(&cln_token, &mut client)
            .await
            .expect("failed to wait for source server to be ready");

        let (src_read, src_ack, lag_reader) = new_source(
            client,
            batch_size,
            Duration::from_millis(1000),
            cln_token.clone(),
            true,
        )
        .await
        .map_err(|e| panic!("failed to create source reader: {:?}", e))
        .expect("failed to create source");

        let source: Source<crate::typ::WithoutRateLimiter> = match transformer_test_handle {
            Some(ref mut source_transform) => {
                Source::new(
                    batch_size,
                    SourceType::UserDefinedSource(
                        Box::new(src_read),
                        Box::new(src_ack),
                        lag_reader,
                    ),
                    tracker.clone(),
                    true,
                    source_transform.transformer.take(),
                    None,
                    None,
                )
                .await
            }
            None => {
                Source::new(
                    batch_size,
                    SourceType::UserDefinedSource(
                        Box::new(src_read),
                        Box::new(src_ack),
                        lag_reader,
                    ),
                    tracker.clone(),
                    true,
                    None,
                    None,
                    None,
                )
                .await
            }
        };

        SourceTestHandle {
            server_handle,
            source,
            source_transformer_test_handle: transformer_test_handle,
        }
    }
}

/// Start a source server with the given handler.
pub(crate) fn start_source_server<S>(handler: S) -> TestServerHandle
where
    S: source::Sourcer + Send + Sync + 'static,
{
    start_server(
        &format!("source-{}", server::get_rand_str()),
        |sock, info, shutdown_rx| async move {
            source::Server::new(handler)
                .with_socket_file(sock)
                .with_server_info_file(info)
                .start_with_shutdown(shutdown_rx)
                .await
                .expect("source server failed");
        },
    )
}
