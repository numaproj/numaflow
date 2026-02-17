use crate::shared::test_utils::server;
use crate::shared::test_utils::server::{TestServerHandle, start_server};
use crate::transformer::Transformer;
use chrono::Utc;
use numaflow::shared::ServerExtras;
use numaflow::sourcetransform;
use numaflow::sourcetransform::{SourceTransformRequest, SourceTransformer};

/// A handle to a source transformer component.
///
/// Contains the transformer handle and server handle (for the transformer server).
pub(crate) struct SourceTransformerTestHandle {
    pub transformer: Option<Transformer>,
    pub server_handle: TestServerHandle,
}

/// Start a source transform server with the given handler.
pub(crate) fn start_source_transform_server<T>(handler: T) -> TestServerHandle
where
    T: SourceTransformer + Send + Sync + 'static,
{
    start_server(
        &format!("src-transform-{}", server::get_rand_str()),
        |sock, info, shutdown_rx| async move {
            numaflow::sourcetransform::Server::new(handler)
                .with_socket_file(sock)
                .with_server_info_file(info)
                .start_with_shutdown(shutdown_rx)
                .await
                .expect("source transform server failed");
        },
    )
}

/// A no-op transformer for testing.
///
/// This is used for the turbofish operator to pass a `None` transformer
/// when a transformer svc is not provided.
pub(crate) struct NoOpTransformer;

#[tonic::async_trait]
impl SourceTransformer for NoOpTransformer {
    async fn transform(&self, input: SourceTransformRequest) -> Vec<sourcetransform::Message> {
        vec![
            sourcetransform::Message::new(input.value, Utc::now())
                .with_keys(input.keys)
                .with_user_metadata(input.user_metadata),
        ]
    }
}
