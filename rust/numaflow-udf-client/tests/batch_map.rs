use std::error::Error;
use std::path::PathBuf;
use std::time::Duration;

use bytes::Bytes;
use chrono::Utc;
use numaflow::batchmap;
use numaflow::shared::ServerExtras;
use numaflow_pb::clients::map::map_client::MapClient;
use numaflow_udf_client::{BatchMapSession, UdfDatum};
use tempfile::TempDir;
use tokio::net::UnixStream;
use tokio::sync::oneshot;
use tonic::transport::{Channel, Endpoint, Uri};
use tower::service_fn;

struct UppercaseBatchMapper;

#[tonic::async_trait]
impl batchmap::BatchMapper for UppercaseBatchMapper {
    async fn batchmap(
        &self,
        mut input: tokio::sync::mpsc::Receiver<batchmap::Datum>,
    ) -> Vec<batchmap::BatchResponse> {
        let mut responses = Vec::new();
        while let Some(datum) = input.recv().await {
            let mut response = batchmap::BatchResponse::from_id(datum.id);
            response.append(batchmap::Message {
                keys: Some(datum.keys),
                value: datum.value.to_ascii_uppercase(),
                tags: Some(vec!["batch".to_string()]),
                nack_options: None,
            });
            responses.push(response);
        }
        responses.reverse();
        responses
    }
}

fn datum(id: &str, value: &'static [u8]) -> UdfDatum {
    let now = Utc::now();
    UdfDatum {
        id: id.to_string(),
        keys: vec![format!("key-{id}")],
        value: Bytes::from_static(value),
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
async fn batch_session_owns_data_eot_and_response_correlation() -> Result<(), Box<dyn Error>> {
    let temp_dir = TempDir::new()?;
    let socket = temp_dir.path().join("batch-map.sock");
    let server_info = temp_dir.path().join("batch-map-server-info");
    let (shutdown_tx, shutdown_rx) = oneshot::channel();

    let server_socket = socket.clone();
    let server = tokio::spawn(async move {
        batchmap::Server::new(UppercaseBatchMapper)
            .with_socket_file(server_socket)
            .with_server_info_file(server_info)
            .start_with_shutdown(shutdown_rx)
            .await
            .expect("batch map server");
    });
    tokio::time::sleep(Duration::from_millis(50)).await;

    let session = BatchMapSession::open(MapClient::new(uds_channel(socket).await?), 3).await?;
    let responses = tokio::time::timeout(
        Duration::from_secs(2),
        session.batch(vec![datum("A", b"first"), datum("B", b"second")]),
    )
    .await??;

    // The UDF deliberately reverses its output, while the shared session correlates responses
    // back into request order.
    assert_eq!(
        responses
            .iter()
            .map(|response| response.id.as_str())
            .collect::<Vec<_>>(),
        ["A", "B"]
    );
    let first = responses.first().expect("first response");
    let second = responses.get(1).expect("second response");
    assert_eq!(first.results.first().expect("first result").value, "FIRST");
    assert_eq!(
        second.results.first().expect("second result").value,
        "SECOND"
    );

    // Drop the client before shutdown: an in-flight request stream keeps the SDK server alive.
    drop(session);
    shutdown_tx.send(()).expect("shutdown server");
    tokio::time::timeout(Duration::from_secs(1), server)
        .await
        .expect("server shutdown timeout")
        .expect("server task");
    Ok(())
}
