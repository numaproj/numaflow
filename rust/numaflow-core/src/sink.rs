use crate::Result;
use numaflow_pb::clients::sink::sink_client::SinkClient;
use std::time::Duration;
use tokio::sync::mpsc::Receiver;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio::{pin, time};
use tonic::transport::Channel;
use tracing::error;
use user_defined::UserDefinedSink;

use crate::message::{Message, ReadAck, ReadMessage, ResponseFromSink, ResponseStatusFromSink};

mod blackhole;
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
    async fn sink(&mut self, messages: Vec<Message>) -> Result<Vec<ResponseFromSink>>;
}

enum ActorMessage {
    Sink {
        messages: Vec<Message>,
        respond_to: oneshot::Sender<Result<Vec<ResponseFromSink>>>,
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

#[derive(Clone)]
pub(crate) struct SinkHandle {
    sender: mpsc::Sender<ActorMessage>,
}

pub(crate) enum SinkClientType {
    Log,
    Blackhole,
    UserDefined(SinkClient<Channel>),
}

impl SinkHandle {
    pub(crate) async fn new(sink_client: SinkClientType, batch_size: usize) -> Result<Self> {
        let (sender, receiver) = mpsc::channel(batch_size);
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
            SinkClientType::Blackhole => {
                let blackhole_sink = blackhole::BlackholeSink;
                tokio::spawn(async {
                    let mut actor = SinkActor::new(receiver, blackhole_sink);
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

    pub(crate) async fn sink(&self, messages: Vec<Message>) -> Result<Vec<ResponseFromSink>> {
        let (tx, rx) = oneshot::channel();
        let msg = ActorMessage::Sink {
            messages,
            respond_to: tx,
        };
        let _ = self.sender.send(msg).await;
        rx.await.unwrap()
    }
}

#[derive(Clone)]
pub(super) struct SinkWriter {
    batch_size: usize,
    timeout: Duration,
    sink_handle: SinkHandle,
}

impl SinkWriter {
    pub(super) async fn new(
        batch_size: usize,
        timeout: Duration,
        sink_handle: SinkHandle,
    ) -> Result<Self> {
        Ok(Self {
            timeout,
            batch_size,
            sink_handle,
        })
    }

    pub(super) async fn start(
        &self,
        mut messages_rx: Receiver<ReadMessage>,
    ) -> Result<JoinHandle<()>> {
        let handle: JoinHandle<()> = tokio::spawn({
            let this = self.clone();
            async move {
                let timeout_duration = Duration::from_secs(5); // Set your desired timeout duration here

                loop {
                    let mut batch = Vec::with_capacity(this.batch_size);
                    let timeout = time::sleep(timeout_duration);
                    pin!(timeout);

                    loop {
                        tokio::select! {
                            Some(read_message) = messages_rx.recv() => {
                                batch.push(read_message);
                                if batch.len() >= this.batch_size {
                                    break;
                                }
                            }
                            _ = &mut timeout => {
                                break;
                            }
                        }
                    }

                    if batch.is_empty() {
                        continue;
                    }

                    let messages: Vec<Message> =
                        batch.iter().map(|rm| rm.message.clone()).collect();
                    let responses = match this.sink_handle.sink(messages).await {
                        Ok(responses) => responses,
                        Err(e) => {
                            error!("Failed to send messages to sink: {:?}", e);
                            continue;
                        }
                    };

                    for (read_message, response) in batch.into_iter().zip(responses) {
                        let ack = if let ResponseStatusFromSink::Success = response.status {
                            ReadAck::Ack
                        } else {
                            ReadAck::Nak
                        };
                        let _ = read_message.ack.send(ack);
                    }
                }
            }
        });
        Ok(handle)
    }
}
