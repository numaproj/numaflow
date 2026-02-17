use crate::shared::grpc;
use crate::shared::test_utils::server::{TestServerHandle, get_rand_str, start_server};
use crate::sinker::builder::SinkWriterBuilder;
use crate::sinker::sink::{SinkClientType, SinkWriter};
use numaflow::shared::ServerExtras;
use numaflow::sink;
use numaflow::sink::{Response, SinkRequest};
use numaflow_pb::clients::sink::sink_client::SinkClient;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

/// A no-op sink for testing.
///
/// This is used for the turbofish operator to pass a `None` sink
/// when a sink svc is not provided.
pub(crate) struct NoOpSink;

#[tonic::async_trait]
impl sink::Sinker for NoOpSink {
    async fn sink(&self, _input: tokio::sync::mpsc::Receiver<SinkRequest>) -> Vec<Response> {
        vec![]
    }
}

/// Enum to represent the type of sink to be used.
pub(crate) enum SinkType<T = NoOpSink> {
    /// Pass all sinkClientTypes except UserDefined
    BuiltIn(SinkClientType),
    /// Use for SinkClientType::UserDefined
    UserDefined(T),
}

/// A handle to a sink component.
///
/// Contains the sink writer and server handles (for each of the user defined sink servers).
pub(crate) struct SinkTestHandle {
    pub sink_writer: SinkWriter,
    pub ud_sink_server_handle: Option<TestServerHandle>,
    pub fb_ud_sink_server_handle: Option<TestServerHandle>,
    pub ons_ud_sink_server_handle: Option<TestServerHandle>,
}

impl SinkTestHandle {
    /// Create a sink component with the given sink service.
    ///
    /// Initializes the sink handle and server handle (for the user defined sink servers).
    pub(crate) async fn create_sink<T>(
        sink: SinkType<T>,
        fallback: Option<SinkType<T>>,
        on_success: Option<SinkType<T>>,
        batch_size: usize,
    ) -> SinkTestHandle
    where
        T: sink::Sinker + Send + Sync + 'static,
    {
        let (sink_client_type, ud_sink_server_handle) = match sink {
            SinkType::UserDefined(sink) => {
                let (sink_client_type, sink_server_handle) = create_ud_sink(sink).await;
                (sink_client_type, Some(sink_server_handle))
            }
            SinkType::BuiltIn(sink) => (sink, None),
        };

        let (fb_client_type, fb_ud_sink_server_handle) = match fallback {
            Some(SinkType::UserDefined(fb_sink)) => {
                let (sink_client_type, sink_server_handle) = create_ud_sink(fb_sink).await;
                (Some(sink_client_type), Some(sink_server_handle))
            }
            Some(SinkType::BuiltIn(fb_sink)) => (Some(fb_sink), None),
            None => (None, None),
        };

        let (ons_client_type, ons_ud_sink_server_handle) = match on_success {
            Some(SinkType::UserDefined(ons_sink)) => {
                let (sink_client_type, sink_server_handle) = create_ud_sink(ons_sink).await;
                (Some(sink_client_type), Some(sink_server_handle))
            }
            Some(SinkType::BuiltIn(ons_sink)) => (Some(ons_sink), None),
            None => (None, None),
        };

        let sink_writer = create_sink_writer(
            sink_client_type,
            fb_client_type,
            ons_client_type,
            batch_size,
        )
        .await;

        SinkTestHandle {
            sink_writer,
            ud_sink_server_handle,
            fb_ud_sink_server_handle,
            ons_ud_sink_server_handle,
        }
    }

    /// Start a sink server with the given handler.
    pub(crate) fn start_sink_server<S>(handler: S) -> TestServerHandle
    where
        S: sink::Sinker + Send + Sync + 'static,
    {
        start_server(
            &format!("sink-{}", get_rand_str()),
            |sock, info, shutdown_rx| async move {
                numaflow::sink::Server::new(handler)
                    .with_socket_file(sock)
                    .with_server_info_file(info)
                    .start_with_shutdown(shutdown_rx)
                    .await
                    .expect("sink server failed");
            },
        )
    }
}

async fn create_ud_sink<T>(sink_svc: T) -> (SinkClientType, TestServerHandle)
where
    T: sink::Sinker + Send + Sync + 'static,
{
    // Create the sink
    let server_handle = SinkTestHandle::start_sink_server(sink_svc);
    let mut sink_client = SinkClient::new(
        server_handle
            .create_rpc_channel()
            .await
            .expect("failed to create sink client"),
    );

    grpc::wait_until_sink_ready(&CancellationToken::new(), &mut sink_client)
        .await
        .expect("failed to wait for sink server to be ready");

    (SinkClientType::UserDefined(sink_client), server_handle)
}

async fn create_sink_writer(
    sink: SinkClientType,
    fallback: Option<SinkClientType>,
    on_success: Option<SinkClientType>,
    batch_size: usize,
) -> SinkWriter {
    let mut sink_writer = SinkWriterBuilder::new(batch_size, Duration::from_millis(100), sink);

    sink_writer = match fallback {
        Some(fallback) => sink_writer.fb_sink_client(fallback),
        None => sink_writer,
    };

    sink_writer = match on_success {
        Some(on_success) => sink_writer.on_success_sink_client(on_success),
        None => sink_writer,
    };

    sink_writer.build().await.unwrap()
}
