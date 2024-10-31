use std::collections::HashMap;

use chrono::Utc;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info};

use crate::config::pipeline::PipelineConfig;
use crate::error;
use crate::error::Error;
use crate::message::{Message, Offset};
use crate::metrics::{forward_pipeline_metrics, pipeline_forward_read_metric_labels};
use crate::pipeline::isb::jetstream::WriterHandle;
use crate::source::SourceHandle;
use crate::transformer::SourceTransformHandle;

/// Simple source forwarder that reads messages from the source, applies transformation if present
/// and writes to the messages to ISB.
pub(crate) struct Forwarder {
    source_reader: SourceHandle,
    transformer: Option<SourceTransformHandle>,
    buffer_writers: HashMap<String, Vec<WriterHandle>>,
    cln_token: CancellationToken,
    config: PipelineConfig,
}

pub(crate) struct ForwarderBuilder {
    source_reader: SourceHandle,
    transformer: Option<SourceTransformHandle>,
    buffer_writers: HashMap<String, Vec<WriterHandle>>,
    cln_token: CancellationToken,
    config: PipelineConfig,
}

impl ForwarderBuilder {
    pub(crate) fn new(
        source_reader: SourceHandle,
        transformer: Option<SourceTransformHandle>,
        buffer_writers: HashMap<String, Vec<WriterHandle>>,
        cln_token: CancellationToken,
        config: PipelineConfig,
    ) -> Self {
        Self {
            source_reader,
            transformer,
            buffer_writers,
            cln_token,
            config,
        }
    }

    pub(crate) fn build(self) -> Forwarder {
        Forwarder {
            source_reader: self.source_reader,
            transformer: self.transformer,
            buffer_writers: self.buffer_writers,
            cln_token: self.cln_token,
            config: self.config,
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

        let labels = pipeline_forward_read_metric_labels(
            self.config.pipeline_name.as_ref(),
            self.config.vertex_name.as_ref(),
            self.config.vertex_name.as_ref(),
            "Source",
            self.config.replica,
        );
        forward_pipeline_metrics()
            .forwarder
            .data_read
            .get_or_create(labels)
            .inc_by(messages.len() as u64);

        if messages.is_empty() {
            return Ok(0);
        }

        let msg_count = messages.len() as u64;
        let offsets: Vec<Offset> =
            messages
                .iter()
                .try_fold(Vec::with_capacity(messages.len()), |mut offsets, msg| {
                    if let Some(offset) = &msg.offset {
                        offsets.push(offset.clone());
                        Ok(offsets)
                    } else {
                        Err(Error::Forwarder("Message offset is missing".to_string()))
                    }
                })?;

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

    /// Applies the transformer to the messages.
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

    /// Writes messages to the jetstream, it writes to all the downstream buffers.
    async fn write_to_jetstream(&mut self, messages: Vec<Message>) -> Result<(), Error> {
        if messages.is_empty() {
            return Ok(());
        }

        let mut results = Vec::new();

        // write to all the buffers
        for i in 0..messages.len() {
            for (_, writers) in &self.buffer_writers {
                // write to the stream writers in round-robin fashion
                let partition = i % writers.len();
                let writer = &writers[partition]; // FIXME: we need to shuffle based on the message id hash
                let result = writer.write(messages[i].clone()).await?;
                results.push(result);
            }
        }

        // await for all the result futures to complete
        // FIXME: we should not await for the results to complete, that will make it sequential
        for result in results {
            // we can use the ack to publish watermark etc
            result
                .await
                .map_err(|e| Error::Forwarder(format!("Failed to write to jetstream {:?}", e)))??;
        }
        Ok(())
    }
}
