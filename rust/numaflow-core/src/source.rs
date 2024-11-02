use crate::config::pipeline::PipelineConfig;
use crate::message::{get_vertex_replica, ReadAck, ReadMessage};
use crate::metrics::{
    pipeline_forward_read_metric_labels, pipeline_isb_metric_labels, pipeline_metrics,
};
use crate::Result;
use crate::{
    message::{Message, Offset},
    reader::LagReader,
};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio_stream::wrappers::ReceiverStream;
use tracing::{error, info};

/// [User-Defined Source] extends Numaflow to add custom sources supported outside the builtins.
///
/// [User-Defined Source]: https://numaflow.numaproj.io/user-guide/sources/user-defined-sources/
pub(crate) mod user_defined;

/// [Generator] is a builtin to generate data for load testing and other internal use-cases.
///
/// [Generator]: https://numaflow.numaproj.io/user-guide/sources/generator/
pub(crate) mod generator;

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

#[derive(Debug)]
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

#[derive(Clone)]
pub(crate) struct SourceHandle {
    sender: mpsc::Sender<ActorMessage>,
}

impl SourceHandle {
    pub(crate) fn new(src_type: SourceType, batch_size: usize) -> Self {
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
        };
        Self { sender }
    }

    pub(crate) async fn read(&self) -> Result<Vec<Message>> {
        let (sender, receiver) = oneshot::channel();
        let msg = ActorMessage::Read { respond_to: sender };
        // Ignore send errors. If send fails, so does the recv.await below. There's no reason
        // to check for the same failure twice.
        let _ = self.sender.send(msg).await;
        receiver
            .await
            .map_err(|e| crate::error::Error::ActorPatternRecv(e.to_string()))?
    }

    pub(crate) async fn ack(&self, offsets: Vec<Offset>) -> Result<()> {
        let (sender, receiver) = oneshot::channel();
        let msg = ActorMessage::Ack {
            respond_to: sender,
            offsets,
        };
        // Ignore send errors. If send fails, so does the recv.await below. There's no reason
        // to check for the same failure twice.
        let _ = self.sender.send(msg).await;
        receiver
            .await
            .map_err(|e| crate::error::Error::ActorPatternRecv(e.to_string()))?
    }

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
}

/// ReadActorMessage is a message to the ReadActor to read messages.
#[derive(Debug)]
struct ReadActorMessage {
    respond_to: oneshot::Sender<Result<Vec<Message>>>,
}

/// ReadActor is responsible for reading messages from the source.
struct ReadActor<R> {
    reader: R,
}

impl<R> ReadActor<R>
where
    R: SourceReader,
{
    fn new(reader: R) -> Self {
        Self { reader }
    }

    async fn handle_read_message(&mut self, message: ReadActorMessage) {
        let msgs = self.reader.read().await;
        message.respond_to.send(msgs).unwrap();
    }
}

/// AckActorMessage is a message to the AckActor to acknowledge the offsets.
#[derive(Debug)]
struct AckActorMessage {
    respond_to: oneshot::Sender<Result<()>>,
    offsets: Vec<Offset>,
}

/// AckActor is responsible for acknowledging the offsets.
struct AckActor<A> {
    acker: A,
}

impl<A> AckActor<A>
where
    A: SourceAcker,
{
    fn new(acker: A) -> Self {
        Self { acker }
    }

    async fn handle_ack_message(&mut self, message: AckActorMessage) {
        let ack = self.acker.ack(message.offsets).await;
        message.respond_to.send(ack).unwrap();
    }
}

/// StreamingSource is used to read messages from the source in a streaming fashion, and ack the offsets
/// after the messages are processed.
#[derive(Clone)]
pub(crate) struct StreamingSource {
    config: PipelineConfig,
    read_batch_size: usize,
    read_sender: mpsc::Sender<ReadActorMessage>,
    ack_sender: mpsc::Sender<AckActorMessage>,
}

impl StreamingSource {
    /// Create a new StreamingSource. It starts the read and ack actors in the background.
    pub(crate) fn new(config: PipelineConfig, src_type: SourceType, batch_size: usize) -> Self {
        let (read_sender, mut read_receiver) = mpsc::channel(batch_size);
        let (ack_sender, mut ack_receiver) = mpsc::channel(batch_size);

        match src_type {
            SourceType::UserDefinedSource(reader, acker, _) => {
                let mut read_actor = ReadActor::new(reader);
                let mut ack_actor = AckActor::new(acker);

                tokio::spawn(async move {
                    while let Some(msg) = read_receiver.recv().await {
                        read_actor.handle_read_message(msg).await;
                    }
                });

                tokio::spawn(async move {
                    while let Some(msg) = ack_receiver.recv().await {
                        ack_actor.handle_ack_message(msg).await;
                    }
                });
            }

            SourceType::Generator(reader, acker, _) => {
                let mut read_actor = ReadActor::new(reader);
                let mut ack_actor = AckActor::new(acker);

                tokio::spawn(async move {
                    while let Some(msg) = read_receiver.recv().await {
                        read_actor.handle_read_message(msg).await;
                    }
                });

                tokio::spawn(async move {
                    while let Some(msg) = ack_receiver.recv().await {
                        ack_actor.handle_ack_message(msg).await;
                    }
                });
            }
        };

        Self {
            config,
            read_batch_size: batch_size,
            read_sender,
            ack_sender,
        }
    }

    /// read messages from the source by communicating with the read actor.
    async fn read(&self) -> Result<Vec<Message>> {
        let (sender, receiver) = oneshot::channel();
        let msg = ReadActorMessage { respond_to: sender };
        let _ = self.read_sender.send(msg).await;
        receiver
            .await
            .map_err(|e| crate::error::Error::ActorPatternRecv(e.to_string()))?
    }

    /// ack the offsets by communicating with the ack actor.
    async fn ack(&self, offsets: Vec<Offset>) -> Result<()> {
        let (sender, receiver) = oneshot::channel();
        let msg = AckActorMessage {
            respond_to: sender,
            offsets,
        };
        let _ = self.ack_sender.send(msg).await;
        receiver
            .await
            .map_err(|e| crate::error::Error::ActorPatternRecv(e.to_string()))?
    }

    /// Starts streaming messages from the source. It returns a stream of messages and
    /// a handle to the spawned task.
    pub(crate) fn start(&self) -> Result<(ReceiverStream<ReadMessage>, JoinHandle<Result<()>>)> {
        let batch_size = self.read_batch_size;
        let (messages_tx, messages_rx) = mpsc::channel(2 * batch_size);
        let source_read = self.clone();

        let labels = pipeline_forward_read_metric_labels(
            self.config.pipeline_name.clone().as_ref(),
            "0",
            self.config.vertex_name.clone().as_ref(),
            "Source",
            self.config.replica,
        );

        info!("Started streaming source with batch size: {}", batch_size);
        let handle = tokio::spawn(async move {
            let mut processed_msgs_count: usize = 0;
            let mut last_logged_at = tokio::time::Instant::now();

            loop {
                // Reserve the permits before invoking the read method.
                let mut permit = match messages_tx.reserve_many(batch_size).await {
                    Ok(permit) => {
                        info!("Reserved permits for {} messages", batch_size);
                        permit
                    }
                    Err(e) => {
                        error!("Error while reserving permits: {:?}", e);
                        return Err(crate::error::Error::Source(e.to_string()));
                    }
                };

                let read_start_time = tokio::time::Instant::now();
                let messages = match source_read.read().await {
                    Ok(messages) => messages,
                    Err(e) => {
                        error!("Error while reading messages: {:?}", e);
                        return Err(e);
                    }
                };
                let n = messages.len();
                info!("Read {} messages in {:?}", n, read_start_time.elapsed());

                pipeline_metrics()
                    .forwarder
                    .data_read
                    .get_or_create(labels)
                    .inc_by(n as u64);

                let mut ack_batch = Vec::with_capacity(n);
                for message in messages {
                    let (resp_ack_tx, resp_ack_rx) = oneshot::channel();
                    let offset = message.offset.clone().unwrap();

                    let read_message = ReadMessage {
                        message,
                        ack: resp_ack_tx,
                    };

                    // store the ack one shot in the batch to invoke ack later.
                    ack_batch.push((offset, resp_ack_rx));

                    match permit.next() {
                        Some(permit) => {
                            permit.send(read_message);
                        }
                        None => {
                            unreachable!(
                                "Permits should be reserved for all messages in the batch"
                            );
                        }
                    }
                }

                // Spawn a task to invoke ack for the batch of messages.
                tokio::spawn(Self::invoke_ack(source_read.clone(), ack_batch));

                processed_msgs_count += n;
                if last_logged_at.elapsed().as_secs() >= 1 {
                    info!(
                        "Processed {} messages in {:?}",
                        processed_msgs_count,
                        std::time::Instant::now()
                    );
                    processed_msgs_count = 0;
                    last_logged_at = tokio::time::Instant::now();
                }
            }
        });
        Ok((ReceiverStream::new(messages_rx), handle))
    }

    /// Listens to the oneshot receivers and invokes ack on the source for the offsets that are acked.
    pub(crate) async fn invoke_ack(
        streaming_source: StreamingSource,
        ack_rx_batch: Vec<(Offset, oneshot::Receiver<ReadAck>)>,
    ) -> Result<()> {
        pipeline_metrics()
            .isb
            .ack_tasks
            .get_or_create(pipeline_isb_metric_labels())
            .inc();

        let last_offset = ack_rx_batch.last().unwrap().0.clone();
        info!(
            "Ack was invoked with last offset: {:?} at time {:?}",
            last_offset,
            tokio::time::Instant::now()
        );

        let n = ack_rx_batch.len();
        let start = tokio::time::Instant::now();
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

        info!(
            "Ack one shots returned in for lastOffset {:?} ackTime={:?}",
            last_offset,
            start.elapsed()
        );

        if !offsets_to_ack.is_empty() {
            streaming_source.ack(offsets_to_ack).await?;
        }
        info!(
            "Acked {} offsets in {:?}, last offset - {:?}",
            n,
            start.elapsed(),
            last_offset
        );

        pipeline_metrics()
            .isb
            .ack_time
            .get_or_create(pipeline_isb_metric_labels())
            .observe(start.elapsed().as_micros() as f64);

        pipeline_metrics()
            .isb
            .ack_total
            .get_or_create(pipeline_isb_metric_labels())
            .inc_by(n as u64);

        pipeline_metrics()
            .isb
            .ack_tasks
            .get_or_create(pipeline_isb_metric_labels())
            .dec();

        Ok(())
    }
}
