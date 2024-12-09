use numaflow_pulsar::source::PulsarSource;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio::time;
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

use crate::config::{get_vertex_name, is_mono_vertex};
use crate::message::ReadAck;
use crate::metrics::{
    monovertex_metrics, mvtx_forward_metric_labels, pipeline_forward_metric_labels,
    pipeline_isb_metric_labels, pipeline_metrics,
};
use crate::tracker::TrackerHandle;
use crate::Result;
use crate::{
    message::{Message, Offset},
    reader::LagReader,
};

/// [User-Defined Source] extends Numaflow to add custom sources supported outside the builtins.
///
/// [User-Defined Source]: https://numaflow.numaproj.io/user-guide/sources/user-defined-sources/
pub(crate) mod user_defined;

/// [Generator] is a builtin to generate data for load testing and other internal use-cases.
///
/// [Generator]: https://numaflow.numaproj.io/user-guide/sources/generator/
pub(crate) mod generator;

/// [Pulsar] is a builtin to ingest data from a Pulsar topic
///
/// [Pulsar]: https://numaflow.numaproj.io/user-guide/sources/pulsar/
pub(crate) mod pulsar;

/// Set of Read related items that has to be implemented to become a Source.
pub(crate) trait SourceReader {
    #[allow(dead_code)]
    /// Name of the source.
    fn name(&self) -> &'static str;

    async fn read(&mut self) -> Result<Vec<Message>>;

    #[allow(dead_code)]
    /// number of partitions processed by this source.
    fn partitions(&self) -> Vec<u16>;
}

/// Set of Ack related items that has to be implemented to become a Source.
pub(crate) trait SourceAcker {
    /// acknowledge an offset. The implementor might choose to do it in an asynchronous way.
    async fn ack(&mut self, _: Vec<Offset>) -> Result<()>;
}

pub(crate) enum SourceType {
    UserDefinedSource(
        user_defined::UserDefinedSourceRead,
        user_defined::UserDefinedSourceAck,
        user_defined::UserDefinedSourceLagReader,
    ),
    Generator(
        generator::GeneratorRead,
        generator::GeneratorAck,
        generator::GeneratorLagReader,
    ),
    Pulsar(PulsarSource),
}

enum ActorMessage {
    #[allow(dead_code)]
    Name {
        respond_to: oneshot::Sender<&'static str>,
    },
    Read {
        respond_to: oneshot::Sender<Result<Vec<Message>>>,
    },
    Ack {
        respond_to: oneshot::Sender<Result<()>>,
        offsets: Vec<Offset>,
    },
    Pending {
        respond_to: oneshot::Sender<Result<Option<usize>>>,
    },
}

struct SourceActor<R, A, L> {
    receiver: mpsc::Receiver<ActorMessage>,
    reader: R,
    acker: A,
    lag_reader: L,
}

impl<R, A, L> SourceActor<R, A, L>
where
    R: SourceReader,
    A: SourceAcker,
    L: LagReader,
{
    fn new(receiver: mpsc::Receiver<ActorMessage>, reader: R, acker: A, lag_reader: L) -> Self {
        Self {
            receiver,
            reader,
            acker,
            lag_reader,
        }
    }

    async fn handle_message(&mut self, msg: ActorMessage) {
        match msg {
            ActorMessage::Name { respond_to } => {
                let name = self.reader.name();
                let _ = respond_to.send(name);
            }
            ActorMessage::Read { respond_to } => {
                let msgs = self.reader.read().await;
                let _ = respond_to.send(msgs);
            }
            ActorMessage::Ack {
                respond_to,
                offsets,
            } => {
                let ack = self.acker.ack(offsets).await;
                let _ = respond_to.send(ack);
            }
            ActorMessage::Pending { respond_to } => {
                let pending = self.lag_reader.pending().await;
                let _ = respond_to.send(pending);
            }
        }
    }
}

/// Source is used to read, ack, and get the pending messages count from the source.
#[derive(Clone)]
pub(crate) struct Source {
    read_batch_size: usize,
    sender: mpsc::Sender<ActorMessage>,
    tracker_handle: TrackerHandle,
}

impl Source {
    /// Create a new StreamingSource. It starts the read and ack actors in the background.
    pub(crate) fn new(
        batch_size: usize,
        src_type: SourceType,
        tracker_handle: TrackerHandle,
    ) -> Self {
        let (sender, receiver) = mpsc::channel(batch_size);
        match src_type {
            SourceType::UserDefinedSource(reader, acker, lag_reader) => {
                tokio::spawn(async move {
                    let mut actor = SourceActor::new(receiver, reader, acker, lag_reader);
                    while let Some(msg) = actor.receiver.recv().await {
                        actor.handle_message(msg).await;
                    }
                });
            }
            SourceType::Generator(reader, acker, lag_reader) => {
                tokio::spawn(async move {
                    let mut actor = SourceActor::new(receiver, reader, acker, lag_reader);
                    while let Some(msg) = actor.receiver.recv().await {
                        actor.handle_message(msg).await;
                    }
                });
            }
            SourceType::Pulsar(pulsar_source) => {
                tokio::spawn(async move {
                    let mut actor = SourceActor::new(
                        receiver,
                        pulsar_source.clone(),
                        pulsar_source.clone(),
                        pulsar_source,
                    );
                    while let Some(msg) = actor.receiver.recv().await {
                        actor.handle_message(msg).await;
                    }
                });
            }
        };
        Self {
            read_batch_size: batch_size,
            sender,
            tracker_handle,
        }
    }

    /// read messages from the source by communicating with the read actor.
    async fn read(source_handle: mpsc::Sender<ActorMessage>) -> Result<Vec<Message>> {
        let (sender, receiver) = oneshot::channel();
        let msg = ActorMessage::Read { respond_to: sender };
        // Ignore send errors. If send fails, so does the recv.await below. There's no reason
        // to check for the same failure twice.
        let _ = source_handle.send(msg).await;
        receiver
            .await
            .map_err(|e| crate::error::Error::ActorPatternRecv(e.to_string()))?
    }

    /// ack the offsets by communicating with the ack actor.
    async fn ack(source_handle: mpsc::Sender<ActorMessage>, offsets: Vec<Offset>) -> Result<()> {
        let (sender, receiver) = oneshot::channel();
        let msg = ActorMessage::Ack {
            respond_to: sender,
            offsets,
        };
        // Ignore send errors. If send fails, so does the recv.await below. There's no reason
        // to check for the same failure twice.
        let _ = source_handle.send(msg).await;
        receiver
            .await
            .map_err(|e| crate::error::Error::ActorPatternRecv(e.to_string()))?
    }

    /// get the pending messages count by communicating with the pending actor.
    pub(crate) async fn pending(&self) -> Result<Option<usize>> {
        let (sender, receiver) = oneshot::channel();
        let msg = ActorMessage::Pending { respond_to: sender };
        // Ignore send errors. If send fails, so does the recv.await below. There's no reason
        // to check for the same failure twice.
        let _ = self.sender.send(msg).await;
        receiver
            .await
            .map_err(|e| crate::error::Error::ActorPatternRecv(e.to_string()))?
    }

    /// Starts streaming messages from the source. It returns a stream of messages and
    /// a handle to the spawned task.
    pub(crate) fn streaming_read(
        &self,
        cln_token: CancellationToken,
    ) -> Result<(ReceiverStream<Message>, JoinHandle<Result<()>>)> {
        let batch_size = self.read_batch_size;
        let (messages_tx, messages_rx) = mpsc::channel(batch_size);
        let source_handle = self.sender.clone();
        let tracker_handle = self.tracker_handle.clone();

        let pipeline_labels = pipeline_forward_metric_labels("Source", Some(get_vertex_name()));
        let mvtx_labels = mvtx_forward_metric_labels();

        info!("Started streaming source with batch size: {}", batch_size);
        let handle = tokio::spawn(async move {
            let mut processed_msgs_count: usize = 0;
            let mut last_logged_at = time::Instant::now();

            loop {
                if cln_token.is_cancelled() {
                    info!("Cancellation token is cancelled. Stopping the source.");
                    return Ok(());
                }
                // Reserve the permits before invoking the read method.
                let mut permit = match messages_tx.reserve_many(batch_size).await {
                    Ok(permit) => permit,
                    Err(e) => {
                        error!("Error while reserving permits: {:?}", e);
                        return Err(crate::error::Error::Source(e.to_string()));
                    }
                };

                let read_start_time = time::Instant::now();
                let messages = match Self::read(source_handle.clone()).await {
                    Ok(messages) => messages,
                    Err(e) => {
                        error!("Error while reading messages: {:?}", e);
                        return Err(e);
                    }
                };
                let n = messages.len();
                if is_mono_vertex() {
                    monovertex_metrics()
                        .read_total
                        .get_or_create(mvtx_labels)
                        .inc_by(n as u64);
                    monovertex_metrics()
                        .read_time
                        .get_or_create(mvtx_labels)
                        .observe(read_start_time.elapsed().as_micros() as f64);
                } else {
                    pipeline_metrics()
                        .forwarder
                        .read_total
                        .get_or_create(pipeline_labels)
                        .inc_by(n as u64);
                    pipeline_metrics()
                        .forwarder
                        .read_time
                        .get_or_create(pipeline_labels)
                        .observe(read_start_time.elapsed().as_micros() as f64);
                }

                let mut ack_batch = Vec::with_capacity(n);
                for message in messages {
                    let (resp_ack_tx, resp_ack_rx) = oneshot::channel();
                    let offset = message.offset.clone().expect("offset can never be none");

                    // insert the offset and the ack one shot in the tracker.
                    tracker_handle
                        .insert(offset.to_string(), resp_ack_tx)
                        .await?;

                    // store the ack one shot in the batch to invoke ack later.
                    ack_batch.push((offset, resp_ack_rx));

                    match permit.next() {
                        Some(permit) => {
                            permit.send(message);
                        }
                        None => {
                            unreachable!(
                                "Permits should be reserved for all messages in the batch"
                            );
                        }
                    }
                }

                // start a background task to invoke ack on the source for the offsets that are acked.
                tokio::spawn(Self::invoke_ack(
                    read_start_time,
                    source_handle.clone(),
                    ack_batch,
                ));

                processed_msgs_count += n;
                if last_logged_at.elapsed().as_secs() >= 1 {
                    info!(
                        "Processed {} messages in {:?}",
                        processed_msgs_count,
                        std::time::Instant::now()
                    );
                    processed_msgs_count = 0;
                    last_logged_at = time::Instant::now();
                }
            }
        });
        Ok((ReceiverStream::new(messages_rx), handle))
    }

    /// Listens to the oneshot receivers and invokes ack on the source for the offsets that are acked.
    async fn invoke_ack(
        e2e_start_time: time::Instant,
        source_handle: mpsc::Sender<ActorMessage>,
        ack_rx_batch: Vec<(Offset, oneshot::Receiver<ReadAck>)>,
    ) -> Result<()> {
        let n = ack_rx_batch.len();
        let mut offsets_to_ack = Vec::with_capacity(n);

        for (offset, oneshot_rx) in ack_rx_batch {
            match oneshot_rx.await {
                Ok(ReadAck::Ack) => {
                    offsets_to_ack.push(offset);
                }
                Ok(ReadAck::Nak) => {
                    error!("Nak received for offset: {:?}", offset);
                }
                Err(e) => {
                    error!(
                        "Error receiving ack for offset: {:?}, error: {:?}",
                        offset, e
                    );
                }
            }
        }

        let start = time::Instant::now();
        if !offsets_to_ack.is_empty() {
            Self::ack(source_handle, offsets_to_ack).await?;
        }

        if is_mono_vertex() {
            monovertex_metrics()
                .ack_time
                .get_or_create(mvtx_forward_metric_labels())
                .observe(start.elapsed().as_micros() as f64);

            monovertex_metrics()
                .ack_total
                .get_or_create(mvtx_forward_metric_labels())
                .inc_by(n as u64);

            monovertex_metrics()
                .e2e_time
                .get_or_create(mvtx_forward_metric_labels())
                .observe(e2e_start_time.elapsed().as_micros() as f64);
        } else {
            pipeline_metrics()
                .forwarder
                .ack_time
                .get_or_create(pipeline_isb_metric_labels())
                .observe(start.elapsed().as_micros() as f64);

            pipeline_metrics()
                .forwarder
                .ack_total
                .get_or_create(pipeline_isb_metric_labels())
                .inc_by(n as u64);

            pipeline_metrics()
                .forwarder
                .processed_time
                .get_or_create(pipeline_isb_metric_labels())
                .observe(e2e_start_time.elapsed().as_micros() as f64);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use chrono::Utc;
    use numaflow::source;
    use numaflow::source::{Message, Offset, SourceReadRequest};
    use numaflow_pb::clients::source::source_client::SourceClient;
    use tokio::sync::mpsc::Sender;
    use tokio_stream::StreamExt;
    use tokio_util::sync::CancellationToken;

    use crate::shared::grpc::create_rpc_channel;
    use crate::source::user_defined::new_source;
    use crate::source::{Source, SourceType};
    use crate::tracker::TrackerHandle;

    struct SimpleSource {
        num: usize,
        sent_count: AtomicUsize,
        yet_to_ack: std::sync::RwLock<HashSet<String>>,
    }

    impl SimpleSource {
        fn new(num: usize) -> Self {
            Self {
                num,
                sent_count: AtomicUsize::new(0),
                yet_to_ack: std::sync::RwLock::new(HashSet::new()),
            }
        }
    }

    #[tonic::async_trait]
    impl source::Sourcer for SimpleSource {
        async fn read(&self, request: SourceReadRequest, transmitter: Sender<Message>) {
            let event_time = Utc::now();
            let mut message_offsets = Vec::with_capacity(request.count);

            for i in 0..request.count {
                if self.sent_count.load(Ordering::SeqCst) >= self.num {
                    return;
                }

                let offset = format!("{}-{}", event_time.timestamp_nanos_opt().unwrap(), i);
                transmitter
                    .send(Message {
                        value: b"hello".to_vec(),
                        event_time,
                        offset: Offset {
                            offset: offset.clone().into_bytes(),
                            partition_id: 0,
                        },
                        keys: vec![],
                        headers: Default::default(),
                    })
                    .await
                    .unwrap();
                message_offsets.push(offset);
                self.sent_count.fetch_add(1, Ordering::SeqCst);
            }
            self.yet_to_ack.write().unwrap().extend(message_offsets);
        }

        async fn ack(&self, offsets: Vec<Offset>) {
            for offset in offsets {
                self.yet_to_ack
                    .write()
                    .unwrap()
                    .remove(&String::from_utf8(offset.offset).unwrap());
            }
        }

        async fn pending(&self) -> usize {
            self.yet_to_ack.read().unwrap().len()
        }

        async fn partitions(&self) -> Option<Vec<i32>> {
            Some(vec![1, 2])
        }
    }

    #[tokio::test]
    async fn test_source() {
        // start the server
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let tmp_dir = tempfile::TempDir::new().unwrap();
        let sock_file = tmp_dir.path().join("source.sock");
        let server_info_file = tmp_dir.path().join("source-server-info");

        let server_info = server_info_file.clone();
        let server_socket = sock_file.clone();
        let server_handle = tokio::spawn(async move {
            // a simple source which generates total of 100 messages
            source::Server::new(SimpleSource::new(100))
                .with_socket_file(server_socket)
                .with_server_info_file(server_info)
                .start_with_shutdown(shutdown_rx)
                .await
                .unwrap()
        });

        // wait for the server to start
        // TODO: flaky
        tokio::time::sleep(Duration::from_millis(100)).await;

        let client = SourceClient::new(create_rpc_channel(sock_file).await.unwrap());

        let (src_read, src_ack, lag_reader) = new_source(client, 5, Duration::from_millis(1000))
            .await
            .map_err(|e| panic!("failed to create source reader: {:?}", e))
            .unwrap();

        let source = Source::new(
            5,
            SourceType::UserDefinedSource(src_read, src_ack, lag_reader),
            TrackerHandle::new(),
        );

        let cln_token = CancellationToken::new();

        let (mut stream, handle) = source.streaming_read(cln_token.clone()).unwrap();
        let mut offsets = vec![];
        // we should read all the 100 messages
        for _ in 0..100 {
            let message = stream.next().await.unwrap();
            assert_eq!(message.value, "hello".as_bytes());
            offsets.push(message.offset.clone().unwrap());
        }

        // ack all the messages
        Source::ack(source.sender.clone(), offsets).await.unwrap();

        // since we acked all the messages, pending should be 0
        let pending = source.pending().await.unwrap();
        assert_eq!(pending, Some(0));

        cln_token.cancel();
        let _ = handle.await.unwrap();
        drop(source);
        let _ = shutdown_tx.send(());
        server_handle.await.unwrap();
    }
}
