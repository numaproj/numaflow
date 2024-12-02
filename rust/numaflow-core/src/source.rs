use crate::{
    message::{Message, Offset},
    reader::LagReader,
};
use numaflow_pulsar::source::PulsarSource;
use tokio::sync::{mpsc, oneshot};

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

    async fn read(&mut self) -> crate::Result<Vec<Message>>;

    #[allow(dead_code)]
    /// number of partitions processed by this source.
    fn partitions(&self) -> Vec<u16>;
}

/// Set of Ack related items that has to be implemented to become a Source.
pub(crate) trait SourceAcker {
    /// acknowledge an offset. The implementor might choose to do it in an asynchronous way.
    async fn ack(&mut self, _: Vec<Offset>) -> crate::Result<()>;
}

enum ActorMessage {
    #[allow(dead_code)]
    Name {
        respond_to: oneshot::Sender<&'static str>,
    },
    Read {
        respond_to: oneshot::Sender<crate::Result<Vec<Message>>>,
    },
    Ack {
        respond_to: oneshot::Sender<crate::Result<()>>,
        offsets: Vec<Offset>,
    },
    Pending {
        respond_to: oneshot::Sender<crate::Result<Option<usize>>>,
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
        Self { sender }
    }

    pub(crate) async fn read(&self) -> crate::Result<Vec<Message>> {
        let (sender, receiver) = oneshot::channel();
        let msg = ActorMessage::Read { respond_to: sender };
        // Ignore send errors. If send fails, so does the recv.await below. There's no reason
        // to check for the same failure twice.
        let _ = self.sender.send(msg).await;
        receiver
            .await
            .map_err(|e| crate::error::Error::ActorPatternRecv(e.to_string()))?
    }

    pub(crate) async fn ack(&self, offsets: Vec<Offset>) -> crate::Result<()> {
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

    pub(crate) async fn pending(&self) -> crate::error::Result<Option<usize>> {
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
    Pulsar(PulsarSource),
}
