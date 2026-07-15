use std::error::Error;
use std::path::PathBuf;
use std::sync::Mutex;
use std::time::Duration;

use bytes::Bytes;
use chrono::{TimeZone, Utc};
use numaflow::reduce;
use numaflow::shared::ServerExtras;
use numaflow_pb::clients::reduce::reduce_client::ReduceClient;
use numaflow_udf_client::{AlignedReduceBook, UdfClientError, UdfDatum, Window};
use tempfile::TempDir;
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::{mpsc, oneshot};
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::{Channel, Endpoint, Uri};
use tonic::{Request, Response, Status};
use tower::service_fn;

struct Counter;

struct CounterCreator;

impl reduce::ReducerCreator for CounterCreator {
    type R = Counter;

    fn create(&self) -> Self::R {
        Counter
    }
}

#[tonic::async_trait]
impl reduce::Reducer for Counter {
    async fn reduce(
        &self,
        keys: Vec<String>,
        mut input: tokio::sync::mpsc::Receiver<reduce::ReduceRequest>,
        _md: &reduce::Metadata,
    ) -> Vec<reduce::Message> {
        let mut count = 0;
        while input.recv().await.is_some() {
            count += 1;
        }
        vec![reduce::Message::new(count.to_string().into_bytes()).with_keys(keys)]
    }
}

struct MultiResult;

struct MultiResultCreator;

impl reduce::ReducerCreator for MultiResultCreator {
    type R = MultiResult;

    fn create(&self) -> Self::R {
        MultiResult
    }
}

#[tonic::async_trait]
impl reduce::Reducer for MultiResult {
    async fn reduce(
        &self,
        keys: Vec<String>,
        mut input: tokio::sync::mpsc::Receiver<reduce::ReduceRequest>,
        _md: &reduce::Metadata,
    ) -> Vec<reduce::Message> {
        while input.recv().await.is_some() {}
        vec![
            reduce::Message::new(b"first".to_vec()).with_keys(keys.clone()),
            reduce::Message::new(b"second".to_vec()).with_keys(keys),
        ]
    }
}

struct CapturingReduceService {
    captured_tx: Mutex<Option<oneshot::Sender<Vec<reduce::proto::ReduceRequest>>>>,
}

#[tonic::async_trait]
impl reduce::proto::reduce_server::Reduce for CapturingReduceService {
    type ReduceFnStream = ReceiverStream<Result<reduce::proto::ReduceResponse, Status>>;

    async fn reduce_fn(
        &self,
        request: Request<tonic::Streaming<reduce::proto::ReduceRequest>>,
    ) -> Result<Response<Self::ReduceFnStream>, Status> {
        let captured_tx = self.captured_tx.lock().expect("capture lock").take();
        let mut request_stream = request.into_inner();
        let (response_tx, response_rx) = mpsc::channel(1);

        tokio::spawn(async move {
            let mut requests = Vec::new();
            while let Ok(Some(request)) = request_stream.message().await {
                requests.push(request);
            }
            if let Some(captured_tx) = captured_tx {
                let _ = captured_tx.send(requests);
            }
            drop(response_tx);
        });

        Ok(Response::new(ReceiverStream::new(response_rx)))
    }

    async fn is_ready(
        &self,
        _request: Request<()>,
    ) -> Result<Response<reduce::proto::ReadyResponse>, Status> {
        Ok(Response::new(reduce::proto::ReadyResponse { ready: true }))
    }
}

fn test_window() -> Window {
    Window::new(
        Utc.with_ymd_and_hms(2023, 1, 1, 0, 0, 0).unwrap(),
        Utc.with_ymd_and_hms(2023, 1, 1, 0, 1, 0).unwrap(),
    )
}

fn datum(keys: &[&str], value: &[u8], offset_secs: i64) -> UdfDatum {
    let window = test_window();
    UdfDatum {
        id: String::new(),
        keys: keys.iter().map(|k| (*k).to_string()).collect(),
        value: Bytes::copy_from_slice(value),
        event_time: window.start_time + chrono::Duration::seconds(offset_secs),
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

async fn start_counter_server(
    socket: PathBuf,
    server_info: PathBuf,
    shutdown_rx: oneshot::Receiver<()>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        reduce::Server::new(CounterCreator)
            .with_socket_file(socket)
            .with_server_info_file(server_info)
            .start_with_shutdown(shutdown_rx)
            .await
            .expect("reduce server");
    })
}

async fn start_raw_reduce_server<S>(
    socket: PathBuf,
    service: S,
    shutdown_rx: oneshot::Receiver<()>,
) -> (tokio::task::JoinHandle<()>, tokio::task::JoinHandle<()>)
where
    S: reduce::proto::reduce_server::Reduce,
{
    let listener = UnixListener::bind(socket).expect("bind raw reduce server");
    let (incoming_tx, incoming_rx) = mpsc::channel(4);
    let accept_handle = tokio::spawn(async move {
        loop {
            let incoming = listener.accept().await.map(|(stream, _)| stream);
            if incoming_tx.send(incoming).await.is_err() {
                break;
            }
        }
    });
    let server_handle = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(reduce::proto::reduce_server::ReduceServer::new(service))
            .serve_with_incoming_shutdown(ReceiverStream::new(incoming_rx), async {
                let _ = shutdown_rx.await;
            })
            .await
            .expect("raw reduce server");
    });

    (server_handle, accept_handle)
}

#[tokio::test]
async fn zero_request_buffer_is_rejected() {
    let client = ReduceClient::new(Endpoint::from_static("http://[::]:50051").connect_lazy());

    let result = AlignedReduceBook::open(client, test_window(), datum(&["k"], b"x", 0), 0).await;

    assert!(matches!(result, Err(UdfClientError::InvalidConfig(_))));
}

#[tokio::test]
async fn captured_sequence_and_clean_stream_completion_without_eof() -> Result<(), Box<dyn Error>> {
    let temp_dir = TempDir::new()?;
    let socket = temp_dir.path().join("capture-reduce.sock");
    let (captured_tx, captured_rx) = oneshot::channel();
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let (server, accept_handle) = start_raw_reduce_server(
        socket.clone(),
        CapturingReduceService {
            captured_tx: Mutex::new(Some(captured_tx)),
        },
        shutdown_rx,
    )
    .await;

    let (book, mut receiver) = AlignedReduceBook::open(
        ReduceClient::new(uds_channel(socket).await?),
        test_window(),
        datum(&["key"], b"open", 1),
        10,
    )
    .await?;
    book.append(datum(&["key"], b"append", 2)).await?;
    book.close();

    let requests = tokio::time::timeout(Duration::from_secs(1), captured_rx).await??;
    assert_eq!(requests.len(), 2);
    let events: Vec<i32> = requests
        .iter()
        .map(|request| request.operation.as_ref().expect("operation").event)
        .collect();
    assert_eq!(
        events,
        [
            reduce::proto::reduce_request::window_operation::Event::Open as i32,
            reduce::proto::reduce_request::window_operation::Event::Append as i32,
        ]
    );
    assert!(
        requests.iter().all(|request| {
            request
                .operation
                .as_ref()
                .and_then(|operation| operation.windows.first())
                .is_some_and(|window| window.slot == "0")
        }),
        "every request must use the aligned slot"
    );
    assert!(
        tokio::time::timeout(Duration::from_secs(1), receiver.recv())
            .await?
            .is_none(),
        "clean gRPC completion without EOF must close the receiver"
    );

    shutdown_tx.send(()).expect("shutdown");
    tokio::time::timeout(Duration::from_secs(1), server)
        .await?
        .expect("server task");
    accept_handle.abort();
    Ok(())
}

#[tokio::test]
async fn fixed_book_open_append_and_stream_close() -> Result<(), Box<dyn Error>> {
    let temp_dir = TempDir::new()?;
    let socket = temp_dir.path().join("reduce.sock");
    let server_info = temp_dir.path().join("reduce-server-info");
    let (shutdown_tx, shutdown_rx) = oneshot::channel();

    let server = start_counter_server(socket.clone(), server_info, shutdown_rx).await;
    tokio::time::sleep(Duration::from_millis(50)).await;

    let window = test_window();
    let (book, mut receiver) = AlignedReduceBook::open(
        ReduceClient::new(uds_channel(socket).await?),
        window,
        datum(&["key1"], b"v1", 1),
        10,
    )
    .await?;

    book.append(datum(&["key1"], b"v2", 2)).await?;
    book.append(datum(&["key1"], b"v3", 3)).await?;
    book.close();

    let result = receiver.recv().await.expect("one result")?;
    assert_eq!(String::from_utf8_lossy(&result.value), "3");
    assert!(receiver.recv().await.is_none(), "EOF closes receiver");

    shutdown_tx.send(()).expect("shutdown");
    tokio::time::timeout(Duration::from_secs(1), server)
        .await?
        .expect("server task");
    Ok(())
}

#[tokio::test]
async fn multiple_keys_share_one_book() -> Result<(), Box<dyn Error>> {
    let temp_dir = TempDir::new()?;
    let socket = temp_dir.path().join("reduce.sock");
    let server_info = temp_dir.path().join("reduce-server-info");
    let (shutdown_tx, shutdown_rx) = oneshot::channel();

    let server = start_counter_server(socket.clone(), server_info, shutdown_rx).await;
    tokio::time::sleep(Duration::from_millis(50)).await;

    let window = test_window();
    let (book, mut receiver) = AlignedReduceBook::open(
        ReduceClient::new(uds_channel(socket).await?),
        window,
        datum(&["alpha"], b"open", 1),
        10,
    )
    .await?;

    book.append(datum(&["beta"], b"append", 2)).await?;
    book.close();

    let mut results = Vec::new();
    while let Some(result) = receiver.recv().await {
        results.push(result?);
    }
    results.sort_by(|left, right| left.keys.cmp(&right.keys));
    assert_eq!(results.len(), 2);
    let alpha = results.first().expect("alpha result");
    assert_eq!(alpha.keys, ["alpha"]);
    assert_eq!(alpha.value.as_ref(), b"1");
    let beta = results.get(1).expect("beta result");
    assert_eq!(beta.keys, ["beta"]);
    assert_eq!(beta.value.as_ref(), b"1");

    shutdown_tx.send(()).expect("shutdown");
    tokio::time::timeout(Duration::from_secs(1), server)
        .await
        .expect("server shutdown timeout")
        .expect("server task");
    Ok(())
}

#[tokio::test]
async fn multiple_results_before_eof() -> Result<(), Box<dyn Error>> {
    let temp_dir = TempDir::new()?;
    let socket = temp_dir.path().join("reduce.sock");
    let server_info = temp_dir.path().join("reduce-server-info");
    let (shutdown_tx, shutdown_rx) = oneshot::channel();

    let server_socket = socket.clone();
    let server = tokio::spawn(async move {
        reduce::Server::new(MultiResultCreator)
            .with_socket_file(server_socket)
            .with_server_info_file(server_info)
            .start_with_shutdown(shutdown_rx)
            .await
            .expect("reduce server");
    });
    tokio::time::sleep(Duration::from_millis(50)).await;

    let (book, mut receiver) = AlignedReduceBook::open(
        ReduceClient::new(uds_channel(socket).await?),
        test_window(),
        datum(&["k"], b"only", 1),
        10,
    )
    .await?;
    book.close();

    let first = receiver.recv().await.expect("first")?;
    assert_eq!(first.value.as_ref(), b"first");
    let second = receiver.recv().await.expect("second")?;
    assert_eq!(second.value.as_ref(), b"second");
    assert!(receiver.recv().await.is_none(), "EOF after results");

    shutdown_tx.send(()).expect("shutdown");
    tokio::time::timeout(Duration::from_secs(1), server)
        .await
        .expect("server shutdown timeout")
        .expect("server task");
    Ok(())
}

#[tokio::test]
async fn response_eof_completes_receiver() -> Result<(), Box<dyn Error>> {
    let temp_dir = TempDir::new()?;
    let socket = temp_dir.path().join("reduce.sock");
    let server_info = temp_dir.path().join("reduce-server-info");
    let (shutdown_tx, shutdown_rx) = oneshot::channel();

    let server = start_counter_server(socket.clone(), server_info, shutdown_rx).await;
    tokio::time::sleep(Duration::from_millis(50)).await;

    let (book, mut receiver) = AlignedReduceBook::open(
        ReduceClient::new(uds_channel(socket).await?),
        test_window(),
        datum(&["k"], b"x", 0),
        10,
    )
    .await?;
    book.close();

    receiver.recv().await.expect("result")?;
    assert!(receiver.recv().await.is_none());

    shutdown_tx.send(()).expect("shutdown");
    tokio::time::timeout(Duration::from_secs(1), server)
        .await
        .expect("server shutdown timeout")
        .expect("server task");
    Ok(())
}

#[tokio::test]
async fn dropped_receiver_aborts_background_task_without_hang() -> Result<(), Box<dyn Error>> {
    let temp_dir = TempDir::new()?;
    let socket = temp_dir.path().join("reduce.sock");
    let server_info = temp_dir.path().join("reduce-server-info");
    let (shutdown_tx, shutdown_rx) = oneshot::channel();

    let server = start_counter_server(socket.clone(), server_info, shutdown_rx).await;
    tokio::time::sleep(Duration::from_millis(50)).await;

    let (book, receiver) = AlignedReduceBook::open(
        ReduceClient::new(uds_channel(socket).await?),
        test_window(),
        datum(&["k"], b"x", 0),
        10,
    )
    .await?;
    drop(receiver);
    book.close();

    shutdown_tx.send(()).expect("shutdown");
    tokio::time::timeout(Duration::from_secs(1), server)
        .await
        .expect("server shutdown timeout")
        .expect("server task");
    Ok(())
}

#[tokio::test]
async fn reduce_fn_startup_failure_reaches_receiver() -> Result<(), Box<dyn Error>> {
    let temp_dir = TempDir::new()?;
    let socket = temp_dir.path().join("reduce.sock");
    let server_info = temp_dir.path().join("reduce-server-info");
    let (shutdown_tx, shutdown_rx) = oneshot::channel();

    let server = start_counter_server(socket.clone(), server_info, shutdown_rx).await;
    tokio::time::sleep(Duration::from_millis(50)).await;

    let channel = uds_channel(socket).await?;
    shutdown_tx.send(()).expect("stop server");
    tokio::time::timeout(Duration::from_secs(1), server)
        .await
        .expect("server stop timeout")
        .expect("server task");
    tokio::time::sleep(Duration::from_millis(50)).await;

    let (_book, mut receiver) = AlignedReduceBook::open(
        ReduceClient::new(channel),
        test_window(),
        datum(&["k"], b"x", 0),
        10,
    )
    .await?;

    let error = receiver
        .recv()
        .await
        .expect("startup error frame")
        .expect_err("ReduceFn should fail");
    assert!(
        matches!(error, numaflow_udf_client::UdfClientError::ReduceFnStart(_)),
        "unexpected error: {error:?}"
    );
    Ok(())
}
