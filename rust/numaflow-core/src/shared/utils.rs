use std::env;
use std::sync::OnceLock;
use std::time::Duration;

use chrono::TimeZone;
use numaflow_pb::clients::sink::sink_client::SinkClient;
use numaflow_pb::clients::source::source_client::SourceClient;
use numaflow_pb::clients::sourcetransformer::source_transform_client::SourceTransformClient;
use tokio_util::sync::CancellationToken;
const NUMAFLOW_MONO_VERTEX_NAME: &str = "NUMAFLOW_MONO_VERTEX_NAME";
const NUMAFLOW_VERTEX_NAME: &str = "NUMAFLOW_VERTEX_NAME";
const NUMAFLOW_REPLICA: &str = "NUMAFLOW_REPLICA";
static VERTEX_NAME: OnceLock<String> = OnceLock::new();

pub(crate) fn get_vertex_name() -> &'static str {
    VERTEX_NAME.get_or_init(|| {
        env::var(NUMAFLOW_MONO_VERTEX_NAME)
            .or_else(|_| env::var(NUMAFLOW_VERTEX_NAME))
            .unwrap_or_default()
    })
}

static IS_MONO_VERTEX: OnceLock<bool> = OnceLock::new();

pub(crate) fn is_mono_vertex() -> &'static bool {
    IS_MONO_VERTEX.get_or_init(|| env::var(NUMAFLOW_MONO_VERTEX_NAME).is_ok())
}

static COMPONENT_TYPE: OnceLock<String> = OnceLock::new();

pub(crate) fn get_component_type() -> &'static str {
    COMPONENT_TYPE.get_or_init(|| {
        if *is_mono_vertex() {
            "mono-vertex".to_string()
        } else {
            "pipeline".to_string()
        }
    })
}

static PIPELINE_NAME: OnceLock<String> = OnceLock::new();

pub(crate) fn get_pipeline_name() -> &'static str {
    PIPELINE_NAME.get_or_init(|| env::var("NUMAFLOW_PIPELINE_NAME").unwrap_or_default())
}

static VERTEX_REPLICA: OnceLock<u16> = OnceLock::new();

// fetch the vertex replica information from the environment variable
pub(crate) fn get_vertex_replica() -> &'static u16 {
    VERTEX_REPLICA.get_or_init(|| {
        env::var(NUMAFLOW_REPLICA)
            .unwrap_or_default()
            .parse()
            .unwrap_or_default()
    })
}

#[cfg(test)]
mod tests {
    use numaflow::source::{Message, Offset, SourceReadRequest};
    use numaflow::{sink, source, sourcetransform};
    use tokio::sync::mpsc;
    use tokio::sync::mpsc::Sender;
    use tokio::time::sleep;
    use tokio_util::sync::CancellationToken;

    use super::*;
    use crate::shared::grpc::{create_rpc_channel, wait_until_sink_ready, wait_until_source_ready, wait_until_transformer_ready};

    struct SimpleSource {}

    #[tonic::async_trait]
    impl source::Sourcer for SimpleSource {
        async fn read(&self, _request: SourceReadRequest, _transmitter: Sender<Message>) {}

        async fn ack(&self, _offset: Vec<Offset>) {}

        async fn pending(&self) -> usize {
            0
        }

        async fn partitions(&self) -> Option<Vec<i32>> {
            Some(vec![0])
        }
    }

    struct SimpleTransformer;
    #[tonic::async_trait]
    impl sourcetransform::SourceTransformer for SimpleTransformer {
        async fn transform(
            &self,
            _input: sourcetransform::SourceTransformRequest,
        ) -> Vec<sourcetransform::Message> {
            vec![]
        }
    }

    struct InMemorySink {}

    #[tonic::async_trait]
    impl sink::Sinker for InMemorySink {
        async fn sink(&self, mut _input: mpsc::Receiver<sink::SinkRequest>) -> Vec<sink::Response> {
            vec![]
        }
    }

    #[tokio::test]
    async fn test_wait_until_ready() {
        // Start the source server
        let (source_shutdown_tx, source_shutdown_rx) = tokio::sync::oneshot::channel();
        let tmp_dir = tempfile::TempDir::new().unwrap();
        let source_sock_file = tmp_dir.path().join("source.sock");
        let server_info_file = tmp_dir.path().join("source-server-info");

        let server_info = server_info_file.clone();
        let source_socket = source_sock_file.clone();
        let source_server_handle = tokio::spawn(async move {
            source::Server::new(SimpleSource {})
                .with_socket_file(source_socket)
                .with_server_info_file(server_info)
                .start_with_shutdown(source_shutdown_rx)
                .await
                .unwrap();
        });

        // Start the sink server
        let (sink_shutdown_tx, sink_shutdown_rx) = tokio::sync::oneshot::channel();
        let sink_tmp_dir = tempfile::TempDir::new().unwrap();
        let sink_sock_file = sink_tmp_dir.path().join("sink.sock");
        let server_info_file = sink_tmp_dir.path().join("sink-server-info");

        let server_info = server_info_file.clone();
        let sink_socket = sink_sock_file.clone();
        let sink_server_handle = tokio::spawn(async move {
            sink::Server::new(InMemorySink {})
                .with_socket_file(sink_socket)
                .with_server_info_file(server_info)
                .start_with_shutdown(sink_shutdown_rx)
                .await
                .unwrap();
        });

        // Start the transformer server
        let (transformer_shutdown_tx, transformer_shutdown_rx) = tokio::sync::oneshot::channel();
        let tmp_dir = tempfile::TempDir::new().unwrap();
        let transformer_sock_file = tmp_dir.path().join("transformer.sock");
        let server_info_file = tmp_dir.path().join("transformer-server-info");

        let server_info = server_info_file.clone();
        let transformer_socket = transformer_sock_file.clone();
        let transformer_server_handle = tokio::spawn(async move {
            sourcetransform::Server::new(SimpleTransformer {})
                .with_socket_file(transformer_socket)
                .with_server_info_file(server_info)
                .start_with_shutdown(transformer_shutdown_rx)
                .await
                .unwrap();
        });

        // Wait for the servers to start
        sleep(Duration::from_millis(100)).await;

        let cln_token = CancellationToken::new();

        let mut source_grpc_client =
            SourceClient::new(create_rpc_channel(source_sock_file.clone()).await.unwrap());
        wait_until_source_ready(&cln_token, &mut source_grpc_client)
            .await
            .unwrap();

        let mut sink_grpc_client =
            SinkClient::new(create_rpc_channel(sink_sock_file.clone()).await.unwrap());
        wait_until_sink_ready(&cln_token, &mut sink_grpc_client)
            .await
            .unwrap();

        let mut transformer_grpc_client = Some(SourceTransformClient::new(
            create_rpc_channel(transformer_sock_file.clone())
                .await
                .unwrap(),
        ));
        wait_until_transformer_ready(&cln_token, transformer_grpc_client.as_mut().unwrap())
            .await
            .unwrap();

        source_shutdown_tx.send(()).unwrap();
        sink_shutdown_tx.send(()).unwrap();
        transformer_shutdown_tx.send(()).unwrap();

        source_server_handle.await.unwrap();
        sink_server_handle.await.unwrap();
        transformer_server_handle.await.unwrap();
    }
}
