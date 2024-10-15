use crate::message::Message;
use crate::pipeline::isb::jetstream::writer::JetstreamWriter;
use async_nats::jetstream::Context;
use tokio::sync::mpsc::Receiver;
use tokio::sync::{mpsc, oneshot};

/// JetStream writer exposes writing to the ISB via a channel. The channel is self-throttling, such
/// that it will not accept writes if there are more than X messages pending to be written.
pub(super) mod writer;

/// ISB Writer accepts an Actor pattern based messages.
enum ActorISBMessage {
    /// Write the messages to ISB
    Write {
        message: Message,
        /// once the message has been successfully written, we can let the sender know.
        /// This can be used to trigger Acknowledgement of the message from the Reader.
        success: oneshot::Sender<()>,
    },
    /// Stop the writer. Once Stop is send, we can drop the rx.
    Stop,
}

struct Actor {
    js_writer: JetstreamWriter,
    receiver: Receiver<ActorISBMessage>,
}

impl Actor {
    fn new(js_writer: JetstreamWriter, receiver: Receiver<ActorISBMessage>) -> Self {
        Self {
            js_writer,
            receiver,
        }
    }

    async fn handle_message(&self, msg: ActorISBMessage) {
        match msg {
            ActorISBMessage::Write {
                message: msg,
                success,
            } => {
                let _ = msg;
                let _ = success;

                // TODO: write the message to JetStream writer's channel
                todo!();

                // there is a problem, if buffer is full or JetStream is temporarily not available
                // how will we retry?

                // and then, so we can ack
                // success.send(())
            }
            ActorISBMessage::Stop => {
                unimplemented!()
            }
        }
    }
}

pub(super) struct ISBMessageHandler {
    sender: mpsc::Sender<ActorISBMessage>,
}

impl ISBMessageHandler {
    pub(super) fn new(js_ctx: Context, batch_size: usize) -> Self {
        let (sender, receiver) = mpsc::channel::<ActorISBMessage>(batch_size);

        let js_writer = JetstreamWriter::new(js_ctx, batch_size);

        let actor = Actor::new(js_writer, receiver);

        // spawn the actor

        todo!()
    }
}
