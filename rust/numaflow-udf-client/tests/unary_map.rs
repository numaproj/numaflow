use std::error::Error;
use std::path::PathBuf;
use std::time::Duration;

use bytes::Bytes;
use chrono::Utc;
use numaflow::map;
use numaflow::shared::ServerExtras;
use numaflow_pb::clients::map::map_client::MapClient;
use numaflow_udf_client::{UdfDatum, UnaryMapSession};
use tempfile::TempDir;
use tokio::net::UnixStream;
use tokio::sync::{mpsc, oneshot};
use tonic::transport::{Channel, Endpoint, Uri};
use tower::service_fn;

struct DelayedMapper {
    started: mpsc::Sender<Vec<u8>>,
    completed: mpsc::Sender<Vec<u8>>,
}

#[tonic::async_trait]
impl map::Mapper for DelayedMapper {
    async fn map(&self, input: map::MapRequest) -> Vec<map::Message> {
        self.started
            .send(input.value.clone())
            .await
            .expect("record started request");
        if input.value == b"A" {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        self.completed
            .send(input.value.clone())
            .await
            .expect("record completed request");
        vec![
            map::Message::new(input.value)
                .with_keys(input.keys)
                .with_tags(vec!["mapped".to_string()]),
        ]
    }
}

fn datum(id: &str) -> UdfDatum {
    let now = Utc::now();
    UdfDatum {
        id: id.to_string(),
        keys: vec![format!("key-{id}")],
        value: Bytes::copy_from_slice(id.as_bytes()),
        event_time: now,
        watermark: Some(now),
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
async fn handshake_then_two_data_frames_without_eot_correlate_out_of_order()
-> Result<(), Box<dyn Error>> {
    let temp_dir = TempDir::new()?;
    let socket = temp_dir.path().join("map.sock");
    let server_info = temp_dir.path().join("mapper-server-info");
    let (started_tx, mut started_rx) = mpsc::channel(2);
    let (completed_tx, mut completed_rx) = mpsc::channel(2);
    let (shutdown_tx, shutdown_rx) = oneshot::channel();

    let server_socket = socket.clone();
    let server = tokio::spawn(async move {
        map::Server::new(DelayedMapper {
            started: started_tx,
            completed: completed_tx,
        })
        .with_socket_file(server_socket)
        .with_server_info_file(server_info)
        .start_with_shutdown(shutdown_rx)
        .await
        .expect("map server");
    });
    tokio::time::sleep(Duration::from_millis(50)).await;

    let session = UnaryMapSession::open(MapClient::new(uds_channel(socket).await?), 2).await?;
    let request_a = tokio::spawn({
        let session = session.clone();
        async move { session.map(datum("A")).await }
    });
    assert_eq!(started_rx.recv().await.as_deref(), Some(b"A".as_slice()));

    let request_b = tokio::spawn({
        let session = session.clone();
        async move { session.map(datum("B")).await }
    });
    assert_eq!(started_rx.recv().await.as_deref(), Some(b"B".as_slice()));

    assert_eq!(
        completed_rx.recv().await.as_deref(),
        Some(b"B".as_slice()),
        "B must return before A"
    );
    assert_eq!(completed_rx.recv().await.as_deref(), Some(b"A".as_slice()));

    let response_b = request_b.await??;
    let response_a = request_a.await??;
    assert_eq!(response_a.id, "A");
    assert_eq!(response_a.results.first().expect("A result").value, "A");
    assert_eq!(response_b.id, "B");
    assert_eq!(response_b.results.first().expect("B result").value, "B");

    // The SDK server would panic on a unary EOT because it has no data payload. Successful
    // completion of both calls therefore characterizes handshake + A + B with no EOT.
    drop(session);
    shutdown_tx.send(()).expect("shutdown server");
    tokio::time::timeout(Duration::from_secs(1), server)
        .await
        .expect("server shutdown timeout")
        .expect("server task");
    Ok(())
}
