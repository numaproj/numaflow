use crate::pipeline::isb::simplebuffer::WithSimpleBuffer;
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
use std::net::TcpListener;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

/// A handle to a source component.
///
/// Contains the source handle, server handle (for the source server),
/// and an optional source transformer handle.
pub(crate) struct SourceTestHandle<C: crate::typ::NumaflowTypeConfig> {
    pub source_transformer_test_handle: Option<SourceTransformerTestHandle>,
    pub source: Source<C>,
    pub server_handle: Option<TestServerHandle>,
}

impl SourceTestHandle<WithSimpleBuffer> {
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
    ) -> SourceTestHandle<WithSimpleBuffer>
    where
        T: SourceTransformer + Send + Sync + 'static,
        S: source::Sourcer + Send + Sync + 'static,
    {
        // create a transformer for this source if it is provided
        let mut transformer_test_handle = match source_transformer_svc {
            Some(transformer_svc) => {
                let _server_handle = start_source_transform_server(transformer_svc);

                let mut client = SourceTransformClient::new(
                    _server_handle
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
                    _server_handle,
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

        let source: Source<WithSimpleBuffer> = match transformer_test_handle {
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
                    cln_token.clone(),
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
                    cln_token.clone(),
                    None,
                )
                .await
            }
        };

        SourceTestHandle {
            server_handle: Some(server_handle),
            source,
            source_transformer_test_handle: transformer_test_handle,
        }
    }

    /// Create a built-in HTTP source component.
    ///
    /// Binds to a random port, starts the embedded HTTP server, and spawns a background
    /// task that sends `num_messages` POST requests to it. No separate gRPC server is
    /// started — the HTTP server is embedded in the source itself.
    pub(crate) async fn create_http_source(
        batch_size: usize,
        buffer_size: usize,
        num_messages: usize,
        tracker: Tracker,
        cln_token: CancellationToken,
    ) -> SourceTestHandle<WithSimpleBuffer> {
        use crate::source::http::CoreHttpSource;

        // HttpSourceHandle uses axum_server::bind_rustls which requires a global
        // CryptoProvider. install_default() is idempotent (returns Err if already set).
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

        // Bind to get a free port, then drop so HttpSourceHandle can bind to it.
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);

        let http_source_config = numaflow_http::HttpSourceConfigBuilder::new("test")
            .addr(addr)
            .buffer_size(buffer_size)
            .timeout(Duration::from_millis(100))
            .build();

        let http_source =
            numaflow_http::HttpSourceHandle::new(http_source_config, cln_token.clone()).await;
        let core_http_source = CoreHttpSource::new(batch_size, http_source);

        let source = Source::new(
            batch_size,
            SourceType::Http(core_http_source),
            tracker,
            true,
            None,
            None,
            cln_token,
            None,
        )
        .await;

        // Spawn a background task to send HTTP POST messages to the source.
        tokio::spawn(async move {
            let client = reqwest::Client::builder()
                .danger_accept_invalid_certs(true)
                .build()
                .expect("failed to build client");

            for i in 0..num_messages {
                let _ = client
                    .post(format!("https://{}/vertices/test", addr))
                    .header("Content-Type", "application/json")
                    .header("X-Numaflow-Id", format!("test-id-{}", i))
                    .body(format!(r#"{{"message": "test{}"}}"#, i))
                    .send()
                    .await;
            }
        });

        SourceTestHandle {
            source_transformer_test_handle: None,
            source,
            server_handle: None,
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
