use crate::message::{ReadAck, ReadMessage};
use crate::{
    message::{Message, Offset},
    reader::LagReader,
};
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt;
use tracing::{error, info};

use crate::Result;

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
    fn new(reader: R, acker: A, lag_reader: L) -> Self {
        Self {
            reader,
            acker,
            lag_reader,
        }
    }

    async fn handle_read_message(&mut self, msg: ActorMessage) {
        if let ActorMessage::Read { respond_to } = msg {
            let msgs = self.reader.read().await;
            let _ = respond_to.send(msgs);
        }
    }

    async fn handle_ack_message(&mut self, msgs: Vec<ActorMessage>) {
        // gather all the offsets and invoke ack once
        let offsets = msgs
            .iter()
            .filter_map(|msg| {
                if let ActorMessage::Ack { offsets, .. } = msg {
                    Some(offsets)
                } else {
                    None
                }
            })
            .flatten()
            .cloned()
            .collect::<Vec<Offset>>();
        self.acker.ack(offsets).await.unwrap();

        // respond to all the ack requests
        for msg in msgs {
            if let ActorMessage::Ack { respond_to, .. } = msg {
                let _ = respond_to.send(Ok(()));
            }
        }
    }

    async fn handle_pending_message(&mut self, msg: ActorMessage) {
        if let ActorMessage::Pending { respond_to } = msg {
            let pending = self.lag_reader.pending().await;
            let _ = respond_to.send(pending);
        }
    }
}

#[derive(Clone)]
pub(crate) struct SourceHandle {
    read_sender: mpsc::Sender<ActorMessage>,
    ack_sender: mpsc::Sender<ActorMessage>,
    pending_sender: mpsc::Sender<ActorMessage>,
}

impl SourceHandle {
    pub(crate) fn new(src_type: SourceType, batch_size: usize, timeout: Duration) -> Self {
        let (read_sender, mut read_receiver) = mpsc::channel(batch_size);
        let (ack_sender, mut ack_receiver) = mpsc::channel(batch_size);
        let (pending_sender, mut pending_receiver) = mpsc::channel(batch_size);

        match src_type {
            SourceType::UserDefinedSource(reader, acker, lag_reader) => {
                tokio::spawn(async move {
                    let mut actor = SourceActor::new(reader, acker, lag_reader);

                    loop {
                        tokio::select! {
                            Some(msg) = read_receiver.recv() => {
                                actor.handle_read_message(msg).await;
                            }
                            Some(msg) = ack_receiver.recv() => {
                                actor.handle_ack_message(vec![msg]).await;
                            }
                            Some(msg) = pending_receiver.recv() => {
                                actor.handle_pending_message(msg).await;
                            }
                        }
                    }
                });
            }
            SourceType::Generator(reader, acker, lag_reader) => {
                tokio::spawn(async move {
                    let mut actor = SourceActor::new(reader, acker, lag_reader);
                    loop {
                        tokio::select! {
                            Some(msg) = read_receiver.recv() => {
                                actor.handle_read_message(msg).await;
                            }
                            Some(msg) = ack_receiver.recv() => {
                                actor.handle_ack_message(vec![msg]).await;
                            }
                            Some(msg) = pending_receiver.recv() => {
                                actor.handle_pending_message(msg).await;
                            }
                        }
                    }
                });
            }
        };
        Self {
            read_sender,
            ack_sender,
            pending_sender,
        }
    }

    pub(crate) async fn read(&self) -> crate::Result<Vec<Message>> {
        let (sender, receiver) = oneshot::channel();
        let msg = ActorMessage::Read { respond_to: sender };
        let _ = self.read_sender.send(msg).await;
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
        let _ = self.ack_sender.send(msg).await;
        receiver
            .await
            .map_err(|e| crate::error::Error::ActorPatternRecv(e.to_string()))?
    }

    pub(crate) async fn pending(&self) -> Result<Option<usize>> {
        let (sender, receiver) = oneshot::channel();
        let msg = ActorMessage::Pending { respond_to: sender };
        let _ = self.pending_sender.send(msg).await;
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

#[derive(Clone)]
pub(crate) struct StreamingSource {
    read_batch_size: usize,
    read_sender: mpsc::Sender<ActorMessage>,
    ack_sender: mpsc::Sender<ActorMessage>,
    pending_sender: mpsc::Sender<ActorMessage>,
}

impl StreamingSource {
    pub(crate) fn new(src_type: SourceType, batch_size: usize, timeout: Duration) -> Self {
        let (read_sender, mut read_receiver) = mpsc::channel(batch_size);
        let (ack_sender, mut ack_receiver) = mpsc::channel(batch_size);
        let (pending_sender, mut pending_receiver) = mpsc::channel(batch_size);

        match src_type {
            SourceType::UserDefinedSource(reader, acker, lag_reader) => {
                tokio::spawn(async move {
                    let mut actor = SourceActor::new(reader, acker, lag_reader);
                    loop {
                        tokio::select! {
                            Some(msg) = read_receiver.recv() => {
                                actor.handle_read_message(msg).await;
                            }
                            Some(msg) = ack_receiver.recv() => {
                                actor.handle_ack_message(vec![msg]).await;
                            }
                            Some(msg) = pending_receiver.recv() => {
                                actor.handle_pending_message(msg).await;
                            }
                        }
                    }
                });
            }
            SourceType::Generator(reader, acker, lag_reader) => {
                tokio::spawn(async move {
                    let mut actor = SourceActor::new(reader, acker, lag_reader);
                    loop {
                        tokio::select! {
                            Some(msg) = read_receiver.recv() => {
                                actor.handle_read_message(msg).await;
                            }
                            Some(msg) = ack_receiver.recv() => {
                                actor.handle_ack_message(vec![msg]).await;
                            }
                            Some(msg) = pending_receiver.recv() => {
                                actor.handle_pending_message(msg).await;
                            }
                        }
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

    pub(crate) async fn read(&self) -> Result<Vec<Message>> {
        let (sender, receiver) = oneshot::channel();
        let msg = ActorMessage::Read { respond_to: sender };
        let _ = self.read_sender.send(msg).await;
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
        let _ = self.ack_sender.send(msg).await;
        receiver
            .await
            .map_err(|e| crate::error::Error::ActorPatternRecv(e.to_string()))?
    }

    pub(crate) async fn pending(&self) -> Result<Option<usize>> {
        let (sender, receiver) = oneshot::channel();
        let msg = ActorMessage::Pending { respond_to: sender };
        let _ = self.pending_sender.send(msg).await;
        receiver
            .await
            .map_err(|e| crate::error::Error::ActorPatternRecv(e.to_string()))?
    }

    pub(crate) fn start_streaming(
        &self,
    ) -> Result<(
        ReceiverStream<ReadMessage>,
        JoinHandle<Result<()>>,
        JoinHandle<Result<()>>,
    )> {
        let batch_size = self.read_batch_size;
        let (messages_tx, messages_rx) = mpsc::channel(2 * batch_size);
        let source_read = self.clone();

        // Create separate ack_tx and ack_rx for doing chunked acks
        let (ack_tx, ack_rx) = mpsc::channel(2 * batch_size);

        let handle = tokio::spawn(async move {
            let mut processed_msgs_count: usize = 0;
            let mut last_logged_at = std::time::Instant::now();

            loop {
                let mut permit = match messages_tx.reserve_many(batch_size).await {
                    Ok(permit) => permit,
                    Err(e) => {
                        error!("Error while reserving permits: {:?}", e);
                        return Err(crate::error::Error::Source(e.to_string()));
                    }
                };

                let messages = match source_read.read().await {
                    Ok(messages) => messages,
                    Err(e) => {
                        error!("Error while reading messages: {:?}", e);
                        return Err(e);
                    }
                };
                let n = messages.len();

                for message in messages {
                    let (resp_ack_tx, resp_ack_rx) = oneshot::channel();
                    let offset = message.offset.clone().unwrap();

                    let read_message = ReadMessage {
                        message,
                        ack: resp_ack_tx,
                    };
                    tokio::spawn(Self::invoke_ack(ack_tx.clone(), offset, resp_ack_rx));
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

                processed_msgs_count += n;
                if last_logged_at.elapsed().as_secs() >= 1 {
                    info!(
                        "Processed {} messages in {:?}",
                        processed_msgs_count,
                        std::time::Instant::now()
                    );
                    processed_msgs_count = 0;
                    last_logged_at = std::time::Instant::now();
                }
            }
        });

        let source_ack = self.clone();
        let ack_handle = tokio::spawn(async move {
            let chunked_ack_stream =
                ReceiverStream::new(ack_rx).chunks_timeout(batch_size, Duration::from_secs(1));
            tokio::pin!(chunked_ack_stream);
            let mut start = std::time::Instant::now();
            let mut total_acks = 0;
            while let Some(offsets) = chunked_ack_stream.next().await {
                total_acks += offsets.len();
                source_ack.ack(offsets).await?;

                if start.elapsed().as_secs() >= 1 {
                    info!(
                        "Acked {} messages in {:?}",
                        total_acks,
                        std::time::Instant::now()
                    );
                    total_acks = 0;
                    start = std::time::Instant::now();
                }
            }
            Ok(())
        });

        Ok((ReceiverStream::new(messages_rx), handle, ack_handle))
    }

    pub(crate) async fn invoke_ack(
        ack_tx: mpsc::Sender<Offset>,
        offset: Offset,
        oneshot_rx: oneshot::Receiver<ReadAck>,
    ) -> Result<()> {
        let result = oneshot_rx
            .await
            .map_err(|e| crate::error::Error::Source(e.to_string()))?;
        match result {
            ReadAck::Ack => {
                ack_tx.send(offset).await.unwrap();
            }
            ReadAck::Nak => {
                error!("Nak received for offset: {:?}", offset);
            }
        }
        Ok(())
    }
}
