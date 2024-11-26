use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

use crate::message::{get_vertex_name, is_mono_vertex, ReadAck, ReadMessage};
use crate::metrics::{
    monovertex_metrics, mvtx_forward_metric_labels, pipeline_forward_metric_labels,
    pipeline_isb_metric_labels, pipeline_metrics,
};
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

/// PendingActorMessage is a message to the PendingActor to get the pending messages count.
#[derive(Debug)]
struct PendingActorMessage {
    respond_to: oneshot::Sender<Result<Option<usize>>>,
}

/// PendingActor is responsible for getting the pending messages count.
struct PendingActor<L> {
    lag_reader: L,
}

impl<L> PendingActor<L>
where
    L: LagReader,
{
    fn new(lag_reader: L) -> Self {
        Self { lag_reader }
    }
    async fn handle_pending_message(&mut self, message: PendingActorMessage) {
        let pending = self.lag_reader.pending().await;
        message.respond_to.send(pending).unwrap();
    }
}

/// Source is used to read, ack, and get the pending messages count from the source.
#[derive(Clone)]
pub(crate) struct Source {
    read_batch_size: usize,
    read_sender: mpsc::Sender<ReadActorMessage>,
    ack_sender: mpsc::Sender<AckActorMessage>,
    pending_sender: mpsc::Sender<PendingActorMessage>,
}

impl Source {
    /// Create a new StreamingSource. It starts the read and ack actors in the background.
    pub(crate) fn new(batch_size: usize, src_type: SourceType) -> Self {
        let (read_sender, mut read_receiver) = mpsc::channel(batch_size);
        let (ack_sender, mut ack_receiver) = mpsc::channel(batch_size);
        let (pending_sender, mut pending_receiver) = mpsc::channel(1);

        // Create actors based on the source type and start the actors in the background.
        match src_type {
            SourceType::UserDefinedSource(reader, acker, lag_reader) => {
                let mut read_actor = ReadActor::new(reader);
                let mut ack_actor = AckActor::new(acker);
                let mut pending_actor = PendingActor::new(lag_reader);

                // FIXME: should we have one spawn and multiple tokio::select! for each receiver?
                // will it impact the performance?

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

                tokio::spawn(async move {
                    while let Some(msg) = pending_receiver.recv().await {
                        pending_actor.handle_pending_message(msg).await;
                    }
                });
            }

            SourceType::Generator(reader, acker, lag_reader) => {
                let mut read_actor = ReadActor::new(reader);
                let mut ack_actor = AckActor::new(acker);
                let mut pending_actor = PendingActor::new(lag_reader);

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

                tokio::spawn(async move {
                    while let Some(msg) = pending_receiver.recv().await {
                        pending_actor.handle_pending_message(msg).await;
                    }
                });
            }
        };

        Self {
            read_batch_size: batch_size,
            read_sender,
            ack_sender,
            pending_sender,
        }
    }

    /// read messages from the source by communicating with the read actor.
    pub(crate) async fn read(&self) -> Result<Vec<Message>> {
        let (sender, receiver) = oneshot::channel();
        let msg = ReadActorMessage { respond_to: sender };
        let _ = self.read_sender.send(msg).await;
        receiver
            .await
            .map_err(|e| crate::error::Error::ActorPatternRecv(e.to_string()))?
    }

    /// ack the offsets by communicating with the ack actor.
    pub(crate) async fn ack(&self, offsets: Vec<Offset>) -> Result<()> {
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

    /// get the pending messages count by communicating with the pending actor.
    pub(crate) async fn pending(&self) -> Result<Option<usize>> {
        let (sender, receiver) = oneshot::channel();
        let msg = PendingActorMessage { respond_to: sender };
        let _ = self.pending_sender.send(msg).await;
        receiver
            .await
            .map_err(|e| crate::error::Error::ActorPatternRecv(e.to_string()))?
    }

    /// Starts streaming messages from the source. It returns a stream of messages and
    /// a handle to the spawned task.
    pub(crate) fn streaming_read(
        &self,
        cln_token: CancellationToken,
    ) -> Result<(ReceiverStream<ReadMessage>, JoinHandle<Result<()>>)> {
        let batch_size = self.read_batch_size;
        let (messages_tx, messages_rx) = mpsc::channel(batch_size);
        let source_read = self.clone();

        let pipeline_labels = pipeline_forward_metric_labels("Source", Some(get_vertex_name()));
        let mvtx_labels = mvtx_forward_metric_labels();

        info!("Started streaming source with batch size: {}", batch_size);
        let handle = tokio::spawn(async move {
            let mut processed_msgs_count: usize = 0;
            let mut last_logged_at = tokio::time::Instant::now();

            loop {
                if cln_token.is_cancelled() {
                    info!("Cancellation token is cancelled. Stopping the source.");
                    return Ok(());
                }
                let permit_time = tokio::time::Instant::now();
                // Reserve the permits before invoking the read method.
                let mut permit = match messages_tx.reserve_many(batch_size).await {
                    Ok(permit) => {
                        info!(
                            "Reserved permits for {} messages in {:?}",
                            batch_size,
                            permit_time.elapsed()
                        );
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

                if *is_mono_vertex() {
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

                // start a background task to invoke ack on the source for the offsets that are acked.
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
        streaming_source: Source,
        ack_rx_batch: Vec<(Offset, oneshot::Receiver<ReadAck>)>,
    ) -> Result<()> {
        pipeline_metrics()
            .isb
            .ack_tasks
            .get_or_create(pipeline_isb_metric_labels())
            .inc();

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

        if !offsets_to_ack.is_empty() {
            streaming_source.ack(offsets_to_ack).await?;
        }
        info!("Acked {} offsets in {:?}", n, start.elapsed());

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
