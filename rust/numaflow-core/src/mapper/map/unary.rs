use std::sync::Arc;

use numaflow_udf_client::{
    KeyValueGroup as SharedKeyValueGroup, UdfDatum, UdfMetadata, UnaryMapSession,
};
use tokio::sync::OwnedSemaphorePermit;
use tracing::error;

use crate::error::Error;
use crate::message::{Message, MessageHandle};
use crate::shared::otel;
use crate::{mark_failed, mark_success};

use super::{
    ParentMessageInfo, SharedMapTaskContext, UserDefinedMessage, map_udf_client_error,
    shared_result_to_proto, update_udf_error_metric, update_udf_read_metric,
    update_udf_write_metric,
};

/// MapUnaryTask encapsulates all the context needed to execute a unary map operation per message.
pub(in crate::mapper) struct MapUnaryTask {
    pub mapper: UnaryMapSession,
    /// Permit to achieve structured concurrency by ensuring we do not exceed the concurrency limit
    /// and all the tasks are cleaned up when the component is shutting down.
    pub permit: OwnedSemaphorePermit,
    pub msg_handle: MessageHandle,
    pub shared_ctx: Arc<SharedMapTaskContext>,
}

impl MapUnaryTask {
    /// Spawns the unary map task as a tokio task per [MapUnaryTask].
    /// The task will process the message through the UDF and send results downstream.
    pub fn spawn(self) {
        tokio::spawn(async move {
            self.execute().await;
        });
    }

    /// Executes the unary map operation.
    ///
    /// Creates a per-message `numaflow.{topology}.map` span via the OTel SDK API, scoped to the
    /// UDF call. The span's context is written into `sys_metadata["tracing_udf"]` so the UDF
    /// sees it as the parent; we strip the key from result messages before they leave the mapper.
    /// When no OTel layer is registered, span creation and metadata writes are skipped via the
    /// noop tracer path.
    async fn execute(mut self) {
        // Hold the permit until the task completes.
        let _permit = self.permit;

        // Store parent message info before sending to UDF.
        // parent_info contains offset, so we don't need to clone it separately.
        let parent_info: ParentMessageInfo = self.msg_handle.message().into();

        let results = {
            // RAII map span: ends when this scope exits.
            let _stage_span = otel::inject_stage_span(
                self.msg_handle.message_mut(),
                otel::TraceTopology::current(),
                otel::TraceStage::Map,
            );

            let datum = udf_datum_from_message(self.msg_handle.message().clone());
            update_udf_read_metric(self.shared_ctx.is_mono_vertex);

            let result = tokio::select! {
                result = self.mapper.map(datum) => result.map_err(map_udf_client_error),
                _ = self.shared_ctx.hard_shutdown_token.cancelled() => {
                    Err(Error::Mapper("unary map operation cancelled".to_string()))
                }
            };

            match result {
                Ok(response) => response.results,
                Err(error) => {
                    update_udf_error_metric(self.shared_ctx.is_mono_vertex);
                    error!(?error, offset = ?parent_info.offset, "failed to map message");
                    mark_failed!(self.msg_handle, &error, None);
                    let _ = self.shared_ctx.error_tx.send(error).await;
                    return;
                }
            }
        };

        // Convert raw results to Messages using parent info.
        // The UserDefinedMessage -> Message conversion copies sys_metadata from parent (so
        // vertex.process context stays in "tracing"). We strip tracing_udf here because the map
        // stage is done — downstream sink will inject its own tracing_udf. No-op when no key
        // was injected.
        let results_len = results.len();
        let mut mapped_messages: Vec<Message> = Vec::with_capacity(results_len);
        for (i, result) in results.into_iter().enumerate() {
            let mut mapped_msg: Message =
                UserDefinedMessage(shared_result_to_proto(result), &parent_info, i as i32).into();
            mapped_msg.strip_tracing_udf();
            mapped_messages.push(mapped_msg);
        }

        update_udf_write_metric(
            self.shared_ctx.is_mono_vertex,
            &parent_info,
            mapped_messages.len() as u64,
        );

        // Update the tracker with the number of messages sent.
        // Use parent_info.offset instead of cloning offset separately.
        self.shared_ctx
            .tracker
            .serving_update(
                &parent_info.offset,
                mapped_messages.iter().map(|m| m.tags.clone()).collect(),
            )
            .await
            .expect("failed to update tracker");

        for mapped_message in mapped_messages {
            // Each downstream handle shares the original ack tracking — ACK is deferred until
            // all mapped messages are written to ISB/sink.
            let msg_handle = self.msg_handle.with_message(mapped_message);

            // Try to bypass the message. If bypassed, try_bypass takes ownership and returns None.
            // If not bypassed, it returns Some(msg_handle) for us to send downstream.
            let msg_handle = if let Some(ref bypass_router) = self.shared_ctx.bypass_router {
                match bypass_router
                    .try_bypass(msg_handle)
                    .await
                    .expect("failed to send message to bypass channel")
                {
                    Some(msg) => msg,
                    None => {
                        // Message was bypassed (already acked by bypass_router), move to next.
                        continue;
                    }
                }
            } else {
                msg_handle
            };

            self.shared_ctx
                .output_tx
                .send(msg_handle)
                .await
                .expect("failed to send response");
        }

        // Decrement the original ref_count now that we've accounted for all downstream messages.
        // The original msg_handle held ref_count=1; mark_success brings it to 0 contribution,
        // and the downstream handles will each call mark_success when written to ISB/sink.
        mark_success!(self.msg_handle);
    }
}

fn udf_datum_from_message(message: Message) -> UdfDatum {
    UdfDatum {
        id: message.id.to_string(),
        keys: message.keys.to_vec(),
        value: message.value,
        event_time: message.event_time,
        watermark: message.watermark,
        headers: Arc::unwrap_or_clone(message.headers),
        metadata: message.metadata.map(|metadata| {
            let metadata = Arc::unwrap_or_clone(metadata);
            UdfMetadata {
                previous_vertex: metadata.previous_vertex,
                sys_metadata: metadata
                    .sys_metadata
                    .into_iter()
                    .map(|(name, group)| {
                        (
                            name,
                            SharedKeyValueGroup {
                                key_value: group.key_value,
                            },
                        )
                    })
                    .collect(),
                user_metadata: metadata
                    .user_metadata
                    .into_iter()
                    .map(|(name, group)| {
                        (
                            name,
                            SharedKeyValueGroup {
                                key_value: group.key_value,
                            },
                        )
                    })
                    .collect(),
            }
        }),
    }
}
