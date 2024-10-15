use crate::error;
use crate::error::Error;
use crate::message::{Message, Offset};
use crate::pipeline::isb::jetstream::WriterHandle;
use crate::source::SourceHandle;
use crate::transformer::user_defined::SourceTransformHandle;
use chrono::Utc;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info};

pub(crate) struct Forwarder {
    source_reader: SourceHandle,
    transformer: Option<SourceTransformHandle>,
    js_writer: WriterHandle,
    cln_token: CancellationToken,
}

pub(crate) struct ForwarderBuilder {
    source_reader: SourceHandle,
    transformer: Option<SourceTransformHandle>,
    js_writer: WriterHandle,
    cln_token: CancellationToken,
}

impl ForwarderBuilder {
    pub(crate) fn new(
        source_reader: SourceHandle,
        transformer: Option<SourceTransformHandle>,
        js_writer: WriterHandle,
        cln_token: CancellationToken,
    ) -> Self {
        Self {
            source_reader,
            transformer,
            js_writer,
            cln_token,
        }
    }

    pub(crate) fn build(self) -> Forwarder {
        Forwarder {
            source_reader: self.source_reader,
            transformer: self.transformer,
            js_writer: self.js_writer,
            cln_token: self.cln_token,
        }
    }
}

impl Forwarder {
    pub(crate) async fn start(&mut self) -> Result<(), Error> {
        let mut processed_msgs_count: usize = 0;
        let mut last_forwarded_at = std::time::Instant::now();
        info!("Forwarder has started");
        loop {
            tokio::time::Instant::now();
            if self.cln_token.is_cancelled() {
                break;
            }
            processed_msgs_count += self.read_and_process_messages().await?;

            if last_forwarded_at.elapsed().as_millis() >= 1000 {
                info!(
                    "Forwarded {} messages at time in the pipeline {}",
                    processed_msgs_count,
                    Utc::now()
                );
                processed_msgs_count = 0;
                last_forwarded_at = std::time::Instant::now();
            }
        }
        Ok(())
    }

    async fn read_and_process_messages(&mut self) -> Result<usize, Error> {
        let start_time = tokio::time::Instant::now();
        let messages = self.source_reader.read().await.map_err(|e| {
            Error::Forwarder(format!("Failed to read messages from source {:?}", e))
        })?;

        debug!(
            "Read batch size: {} and latency - {}ms",
            messages.len(),
            start_time.elapsed().as_millis()
        );

        if messages.is_empty() {
            return Ok(0);
        }

        let msg_count = messages.len() as u64;
        let offsets: Vec<Offset> = messages.iter().map(|msg| msg.offset.clone()).collect();

        // Apply transformation if transformer is present
        // FIXME: we should stream the responses back and write it to the jetstream writer
        let transformed_messages = self.apply_transformer(messages).await.map_err(|e| {
            Error::Forwarder(format!(
                "Failed to apply transformation to messages {:?}",
                e
            ))
        })?;

        self.write_to_jetstream(transformed_messages).await?;

        self.source_reader.ack(offsets).await?;

        Ok(msg_count as usize)
    }

    async fn apply_transformer(&mut self, messages: Vec<Message>) -> error::Result<Vec<Message>> {
        let Some(client) = &mut self.transformer else {
            // return early if there is no transformer
            return Ok(messages);
        };

        let start_time = tokio::time::Instant::now();
        let results = client.transform(messages).await?;

        debug!(
            "Transformer latency - {}ms",
            start_time.elapsed().as_millis()
        );

        Ok(results)
    }

    async fn write_to_jetstream(&mut self, messages: Vec<Message>) -> Result<(), Error> {
        if messages.is_empty() {
            return Ok(());
        }

        let mut results = Vec::new();
        for message in messages {
            results.push(self.js_writer.write("temp", message).await?);
        }

        // await for all the result futures to complete
        for result in results {
            // we can use the ack to publish watermark etc
            let ack = result.await.unwrap();
        }
        Ok(())
    }
}
