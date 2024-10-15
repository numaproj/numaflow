use tokio::sync::{mpsc, oneshot};

use crate::config::config;
use crate::{
    message::{Message, Offset},
    monovertex::SourceType,
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

/* The actor task managed by this handler will be receiving messages from the sender channel here in an outer `while let Some(msg) = actor.receiver.recv().await` loop. When all copies of the SourceHandle is dropped, the sender channel will be closed. When the sender channel is closed all there are no messages remaining in the channel's buffer, the actor task will exit.
If an immediate (without waiting for the buffer to be empty) exit is required, we might have to use a cancellation token.
*/
#[derive(Clone)]
pub(crate) struct SourceHandle {
    sender: mpsc::Sender<ActorMessage>,
}

impl SourceHandle {
    pub(crate) fn new(src_type: SourceType) -> Self {
        let (sender, receiver) = mpsc::channel(config().batch_size as usize);
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

    pub(crate) async fn read(&self) -> crate::Result<Vec<Message>> {
        let (sender, receiver) = oneshot::channel();
        let msg = ActorMessage::Read { respond_to: sender };
        let _ = self.sender.send(msg).await;
        receiver.await.unwrap()
    }

    pub(crate) async fn ack(&self, offsets: Vec<Offset>) -> crate::Result<()> {
        let (sender, receiver) = oneshot::channel();
        let msg = ActorMessage::Ack {
            respond_to: sender,
            offsets,
        };
        let _ = self.sender.send(msg).await;
        receiver.await.unwrap()
    }

    pub(crate) async fn pending(&self) -> crate::error::Result<Option<usize>> {
        let (sender, receiver) = oneshot::channel();
        let msg = ActorMessage::Pending { respond_to: sender };
        let _ = self.sender.send(msg).await;
        receiver.await.unwrap()
    }
}
