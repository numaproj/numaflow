use std::error::Error;
use std::path::PathBuf;
use std::time::Duration;

use bytes::Bytes;
use chrono::Utc;
use numaflow::mapstream;
use numaflow::shared::ServerExtras;
use numaflow_pb::clients::map::map_client::MapClient;
use numaflow_udf_client::{StreamMapSession, UdfDatum};
use tempfile::TempDir;
use tokio::net::UnixStream;
use tokio::sync::oneshot;
use tonic::transport::{Channel, Endpoint, Uri};
use tower::service_fn;

struct FlatmapStream;

#[tonic::async_trait]
impl mapstream::MapStreamer for FlatmapStream {
    async fn map_stream(
        &self,
        input: mapstream::MapStreamRequest,
        tx: tokio::sync::mpsc::Sender<mapstream::Message>,
    ) {
        let payload = String::from_utf8(input.value).unwrap_or_default();
        for part in payload.split(',') {
            let message = mapstream::Message::new(part.as_bytes().to_vec())
                .with_keys(input.keys.clone())
                .with_tags(vec![]);
            if tx.send(message).await.is_err() {
                break;
            }
        }
    }
}

struct NackStream;

#[tonic::async_trait]
impl mapstream::MapStreamer for NackStream {
    async fn map_stream(
        &self,
        _input: mapstream::MapStreamRequest,
        tx: tokio::sync::mpsc::Sender<mapstream::Message>,
    ) {
        let _ = tx
            .send(mapstream::Message::message_to_nack(Some(
                numaflow::shared::NackOptions {
                    delay: Some(5000),
                    max_deliveries: Some(3),
                    reason: Some("udf nack".to_string()),
                },
            )))
            .await;
    }
}

fn datum(value: &'static [u8]) -> UdfDatum {
    UdfDatum {
        id: "0".to_string(),
        keys: vec!["first".to_string()],
        value: Bytes::from_static(value),
        event_time: Utc::now(),
        watermark: None,
        headers: Default::default(),
        metadata: None,
    }
}

async fn uds_channel(socket: PathBuf) -> Result<Channel, Box<dyn Error>> {
    let channel = Endpoint::try_from("http://[::]:50051")?
        .connect_with_connector(service_fn(move |_: Uri| {
            let socket = socket.clone();
            async move {
                Ok::<_, std::io::Error>(hyper_util::rt::TokioIo::new(
                    UnixStream::connect(socket).await?,
                ))
            }
        }))
        .await?;
    Ok(channel)
}

#[tokio::test]
async fn stream_session_correlates_chunks_until_per_id_eot() -> Result<(), Box<dyn Error>> {
    let temp_dir = TempDir::new()?;
    let socket = temp_dir.path().join("map-stream.sock");
    let server_info = temp_dir.path().join("map-stream-server-info");
    let (shutdown_tx, shutdown_rx) = oneshot::channel();

    let server_socket = socket.clone();
    let server = tokio::spawn(async move {
        mapstream::Server::new(FlatmapStream)
            .with_socket_file(server_socket)
            .with_server_info_file(server_info)
            .start_with_shutdown(shutdown_rx)
            .await
            .expect("stream map server");
    });
    tokio::time::sleep(Duration::from_millis(50)).await;

    let session = StreamMapSession::open(MapClient::new(uds_channel(socket).await?), 10).await?;
    let mut receiver = session.stream(datum(b"test,map,stream")).await?;

    // Collect every response chunk until the shared session consumes this ID's EOT and closes the
    // receiver.
    let mut results = Vec::new();
    while let Some(response) = receiver.recv().await {
        results.extend(response?.results);
    }

    assert_eq!(
        results
            .iter()
            .map(|result| String::from_utf8_lossy(&result.value).into_owned())
            .collect::<Vec<_>>(),
        ["test", "map", "stream"]
    );

    // Drop the client before shutdown: an in-flight request stream keeps the SDK server alive.
    // See https://github.com/numaproj/numaflow-rs/issues/85.
    drop(session);
    shutdown_tx.send(()).expect("shutdown server");
    tokio::time::timeout(Duration::from_secs(1), server)
        .await
        .expect("server shutdown timeout")
        .expect("server task");
    Ok(())
}

#[tokio::test]
async fn stream_session_decodes_nack_options() -> Result<(), Box<dyn Error>> {
    let temp_dir = TempDir::new()?;
    let socket = temp_dir.path().join("map-stream.sock");
    let server_info = temp_dir.path().join("map-stream-server-info");
    let (shutdown_tx, shutdown_rx) = oneshot::channel();

    let server_socket = socket.clone();
    let server = tokio::spawn(async move {
        mapstream::Server::new(NackStream)
            .with_socket_file(server_socket)
            .with_server_info_file(server_info)
            .start_with_shutdown(shutdown_rx)
            .await
            .expect("stream map server");
    });
    tokio::time::sleep(Duration::from_millis(50)).await;

    let session = StreamMapSession::open(MapClient::new(uds_channel(socket).await?), 10).await?;
    let mut receiver = session.stream(datum(b"hello")).await?;
    let response = receiver
        .recv()
        .await
        .expect("response chunk")
        .expect("map response");
    let result = response.results.first().expect("one result");

    assert!(result.tags.contains(&"U+005C__NACK__".to_string()));
    let options = result.nack_options.as_ref().expect("nack options");
    assert_eq!(options.reason.as_deref(), Some("udf nack"));
    assert_eq!(options.max_deliveries, Some(3));
    assert_eq!(options.delay, Some(5000));
    assert!(receiver.recv().await.is_none(), "EOT closes the stream");

    drop(session);
    shutdown_tx.send(()).expect("shutdown server");
    tokio::time::timeout(Duration::from_secs(1), server)
        .await
        .expect("server shutdown timeout")
        .expect("server task");
    Ok(())
}
