use tokio::sync::{mpsc, oneshot};
use tonic::transport::Channel;

use crate::config::config;
use crate::message::{Message, ResponseFromSink};
use numaflow_grpc::clients::sink::sink_client::SinkClient;
use user_defined::UserDefinedSink;

mod log;
/// [User-Defined Sink] extends Numaflow to add custom sources supported outside the builtins.
///
/// [User-Defined Sink]: https://numaflow.numaproj.io/user-guide/sinks/user-defined-sinks/
mod user_defined;

/// Set of items to be implemented be a Numaflow Sink.
///
/// [Sink]: https://numaflow.numaproj.io/user-guide/sinks/overview/
#[trait_variant::make(Sink: Send)]
#[allow(unused)]
pub(crate) trait LocalSink {
    /// Write the messages to the Sink.
    async fn sink(&mut self, messages: Vec<Message>) -> crate::Result<Vec<ResponseFromSink>>;
}

enum ActorMessage {
    Sink {
        messages: Vec<Message>,
        respond_to: oneshot::Sender<crate::Result<Vec<ResponseFromSink>>>,
    },
}

struct SinkActor<T> {
    actor_messages: mpsc::Receiver<ActorMessage>,
    sink: T,
}

impl<T> SinkActor<T>
where
    T: Sink,
{
    fn new(actor_messages: mpsc::Receiver<ActorMessage>, sink: T) -> Self {
        Self {
            actor_messages,
            sink,
        }
    }

    async fn handle_message(&mut self, msg: ActorMessage) {
        match msg {
            ActorMessage::Sink {
                messages,
                respond_to,
            } => {
                let response = self.sink.sink(messages).await;
                let _ = respond_to.send(response);
            }
        }
    }
}

/* The actor task managed by this handler will be receiving messages from the sender channel here in an outer `while let Some(msg) = actor.receiver.recv().await` loop. When all copies of the SourceHandle is dropped, the sender channel will be closed. When the sender channel is closed all there are no messages remaining in the channel's buffer, the actor task will exit.
If an immediate (without waiting for the buffer to be empty) exit is required, we might have to use a cancellation token.
*/
#[derive(Clone)]
pub(crate) struct SinkHandle {
    sender: mpsc::Sender<ActorMessage>,
}

pub(crate) enum SinkClientType {
    Log,
    UserDefined(SinkClient<Channel>),
}

impl SinkHandle {
    pub(crate) async fn new(sink_client: SinkClientType) -> crate::Result<Self> {
        let (sender, receiver) = mpsc::channel(config().batch_size as usize);
        match sink_client {
            SinkClientType::Log => {
                let log_sink = log::LogSink;
                tokio::spawn(async {
                    let mut actor = SinkActor::new(receiver, log_sink);
                    while let Some(msg) = actor.actor_messages.recv().await {
                        actor.handle_message(msg).await;
                    }
                });
            }
            SinkClientType::UserDefined(sink_client) => {
                let sink = UserDefinedSink::new(sink_client).await?;
                tokio::spawn(async {
                    let mut actor = SinkActor::new(receiver, sink);
                    while let Some(msg) = actor.actor_messages.recv().await {
                        actor.handle_message(msg).await;
                    }
                });
            }
        };
        Ok(Self { sender })
    }

    pub(crate) async fn sink(
        &self,
        messages: Vec<Message>,
    ) -> crate::Result<Vec<ResponseFromSink>> {
        let (tx, rx) = oneshot::channel();
        let msg = ActorMessage::Sink {
            messages,
            respond_to: tx,
        };
        let _ = self.sender.send(msg).await;
        rx.await.unwrap()
    }
}
