use std::sync::Arc;

use numaflow_udf_client::StreamMapSession;
use tokio::sync::OwnedSemaphorePermit;
use tracing::error;

use crate::error::Error;
use crate::message::{Message, MessageHandle};
use crate::shared::otel;
use crate::{mark_failed, mark_success};

use super::{
    ParentMessageInfo, SharedMapTaskContext, UserDefinedMessage, map_udf_client_error,
    shared_result_to_proto, udf_datum_from_message, update_udf_error_metric,
    update_udf_process_time_metric, update_udf_read_metric, update_udf_write_only_metric,
};

/// MapStreamTask encapsulates all the context needed to execute a stream map operation.
pub(in crate::mapper) struct MapStreamTask {
    pub mapper: StreamMapSession,
    pub permit: OwnedSemaphorePermit,
    pub msg_handle: MessageHandle,
    pub shared_ctx: Arc<SharedMapTaskContext>,
}

impl MapStreamTask {
    /// Spawns the stream map task as a tokio task.
    /// The task will process the message through the UDF and send results downstream.
    pub fn spawn(self) {
        tokio::spawn(async move {
            self.execute().await;
        });
    }

    /// Executes the stream map operation.
    ///
    /// Creates a per-message `numaflow.{topology}.map` span via the OTel SDK API, scoped to the
    /// full UDF interaction (request send through stream close). The span's context is written
    /// into `sys_metadata["tracing_udf"]` so the UDF sees it as the parent; result messages have
    /// the key stripped before leaving the mapper. When no OTel layer is registered, span
    /// creation and metadata writes are skipped via the noop tracer path.
    async fn execute(mut self) {
        // Hold the permit until the task completes.
        let _permit = self.permit;

        // RAII map span: covers the full stream lifecycle. Ends when `execute` returns.
        let _stage_span = otel::inject_stage_span(
            self.msg_handle.message_mut(),
            otel::TraceTopology::current(),
            otel::TraceStage::Map,
        );

        // Store parent message info before sending to UDF.
        // parent_info contains offset, so we don't need to clone it separately.
        let mut parent_info: ParentMessageInfo = self.msg_handle.message().into();

        // Core owns only the conversion from its internal Message; protobuf encoding and stream
        // correlation are owned by the shared UDF client.
        let datum = udf_datum_from_message(self.msg_handle.message().clone());
        update_udf_read_metric(self.shared_ctx.is_mono_vertex);

        // Call the UDF and get the receiver for decoded, protocol-neutral result chunks.
        let receiver = tokio::select! {
            result = self.mapper.stream(datum) => result.map_err(map_udf_client_error),
            _ = self.shared_ctx.hard_shutdown_token.cancelled() => {
                Err(Error::Mapper("stream map operation cancelled".to_string()))
            }
        };
        let mut receiver = match receiver {
            Ok(receiver) => receiver,
            Err(error) => {
                update_udf_error_metric(self.shared_ctx.is_mono_vertex);
                error!(?error, offset = ?parent_info.offset, "failed to map message");
                mark_failed!(self.msg_handle, &error, None);
                let _ = self.shared_ctx.error_tx.send(error).await;
                return;
            }
        };

        // We need to update the tracker with no responses, because unlike unary and batch,
        // we cannot update the responses here - we will have to append the responses.
        // Use parent_info.offset instead of cloning offset separately.
        self.shared_ctx
            .tracker
            .serving_refresh(parent_info.offset.clone())
            .await
            .expect("failed to reset tracker");

        loop {
            match receiver.recv().await {
                Some(Ok(response)) => {
                    // Convert shared results to Messages using parent info.
                    // Strip tracing_udf from each result (map stage is done; no-op when no key
                    // was injected).
                    for result in response.results {
                        let mut mapped_message: Message = UserDefinedMessage(
                            shared_result_to_proto(result),
                            &parent_info,
                            parent_info.current_index,
                        )
                        .into();
                        parent_info.current_index += 1;
                        mapped_message.strip_tracing_udf();

                        update_udf_write_only_metric(self.shared_ctx.is_mono_vertex);

                        self.shared_ctx
                            .tracker
                            .serving_append(
                                mapped_message.offset.clone(),
                                mapped_message.tags.clone(),
                            )
                            .await
                            .expect("failed to update tracker");

                        // Each downstream handle shares the original ack tracking — ACK is
                        // deferred until all mapped messages are written to ISB/sink.
                        let msg_handle = self.msg_handle.with_message(mapped_message);

                        // Try to bypass the message. If bypassed, try_bypass takes ownership and
                        // returns None. If not bypassed, it returns Some(msg_handle) for us to send
                        // downstream.
                        let msg_handle =
                            if let Some(ref bypass_router) = self.shared_ctx.bypass_router {
                                match bypass_router
                                    .try_bypass(msg_handle)
                                    .await
                                    .expect("failed to send message to bypass channel")
                                {
                                    Some(message) => message,
                                    None => continue, // Message was bypassed, move to next.
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
                }
                Some(Err(error)) => {
                    let error = map_udf_client_error(error);
                    update_udf_error_metric(self.shared_ctx.is_mono_vertex);
                    error!(?error, "failed to map message");
                    mark_failed!(self.msg_handle, &error, None);
                    let _ = self.shared_ctx.error_tx.send(error).await;
                    return;
                }
                None => {
                    // Channel closed — the shared session consumed this ID's EOT after all result
                    // chunks (including the empty-result case).
                    update_udf_process_time_metric(self.shared_ctx.is_mono_vertex);
                    break;
                }
            }
        }

        // Decrement the original ref_count now that we've accounted for all downstream messages.
        mark_success!(self.msg_handle);
    }
}
