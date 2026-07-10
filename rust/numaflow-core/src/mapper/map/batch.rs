use super::{
    ParentMessageInfo, UserDefinedMessage, map_udf_client_error, shared_result_to_proto,
    udf_datum_from_message, update_udf_error_metric, update_udf_process_time_metric,
    update_udf_read_metric, update_udf_write_metric,
};
use crate::config::pipeline::VERTEX_TYPE_MAP_UDF;
use crate::error::{Error, Result};
use crate::message::{Message, MessageHandle};
use crate::monovertex::bypass_router::MvtxBypassRouter;
use crate::shared::otel;
use crate::tracker::Tracker;
use crate::{mark_failed, mark_success};
use numaflow_udf_client::{BatchMapSession, MapResult as SharedMapResult, UdfClientError};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::error;

/// MapBatchTask encapsulates all the context needed to execute a batch map operation.
pub(in crate::mapper) struct MapBatchTask {
    pub mapper: BatchMapSession,
    pub msg_handles: Vec<MessageHandle>,
    pub output_tx: mpsc::Sender<MessageHandle>,
    pub tracker: Tracker,
    pub bypass_router: Option<MvtxBypassRouter>,
    pub is_mono_vertex: bool,
    pub cln_token: CancellationToken,
}

impl MapBatchTask {
    /// Executes the batch map operation.
    /// Returns an error if any message in the batch fails to be processed.
    pub async fn execute(mut self) -> Result<()> {
        // Store parent message info for each message before sending to UDF
        let parent_infos: Vec<ParentMessageInfo> = self
            .msg_handles
            .iter()
            .map(|rm| rm.message().into())
            .collect();

        let results = {
            // Create per-message map spans via the OTel SDK API.
            // Each span's parent is that message's `vertex.process` context (from
            // sys_metadata["tracing"]). We inject the map span context into
            // sys_metadata["tracing_udf"] so the UDF creates its processing span as a child.
            // Lexical scope ends spans after the batch UDF call.
            //
            // Invariant: tracing_udf is removed from result messages below; on error, input
            // messages are dropped, so tracing_udf never propagates further.
            //
            // When no OTel layer is registered, `inject_stage_span` returns non-recording spans
            // and the sys_metadata copy-on-write is skipped — no need to gate this call.
            let _stage_spans = otel::inject_stage_spans!(
                self.msg_handles.iter_mut().map(MessageHandle::message_mut),
                otel::TraceTopology::current(),
                otel::TraceStage::Map,
            );

            // Core owns only the conversion from its internal Message; protobuf encoding and
            // batch choreography are owned by the shared UDF client.
            let data = self
                .msg_handles
                .iter()
                .map(|rm| udf_datum_from_message(rm.message().clone()))
                .collect::<Vec<_>>();
            let request_count = data.len();

            // Update read metrics for each request
            for _ in &data {
                update_udf_read_metric(self.is_mono_vertex);
            }

            let batch_result = tokio::select! {
                result = self.mapper.batch(data) => Some(result),
                _ = self.cln_token.cancelled() => None,
            };

            let results: Vec<Result<Vec<SharedMapResult>>> = match batch_result {
                Some(Ok(responses)) => {
                    update_udf_process_time_metric(self.is_mono_vertex);
                    responses
                        .into_iter()
                        .map(|response| Ok(response.results))
                        .collect()
                }
                Some(Err(error)) => {
                    if matches!(error, UdfClientError::PartialBatchResponse { .. }) {
                        critical_error!(VERTEX_TYPE_MAP_UDF, "eot_received_from_map");
                    }
                    for _ in 0..request_count {
                        update_udf_error_metric(self.is_mono_vertex);
                    }
                    vec![Err(map_udf_client_error(error))]
                }
                None => vec![Err(Error::Mapper(
                    "batch map operation cancelled".to_string(),
                ))],
            };
            results
        };

        for (result, (msg_handle, parent_info)) in results
            .into_iter()
            .zip(self.msg_handles.into_iter().zip(parent_infos))
        {
            match result {
                Ok(results) => {
                    // Convert shared results to Messages using parent info.
                    // Remove tracing_udf from each result (map stage is done).
                    let mapped_messages: Vec<Message> = results
                        .into_iter()
                        .enumerate()
                        .map(|(i, result)| {
                            let mut mapped_msg: Message = UserDefinedMessage(
                                shared_result_to_proto(result),
                                &parent_info,
                                i as i32,
                            )
                            .into();
                            // No-op when no `tracing_udf` was injected (noop tracer path).
                            mapped_msg.strip_tracing_udf();
                            mapped_msg
                        })
                        .collect();

                    let offset = parent_info.offset.clone();

                    update_udf_write_metric(
                        self.is_mono_vertex,
                        &parent_info,
                        mapped_messages.len() as u64,
                    );

                    self.tracker
                        .serving_update(
                            &offset,
                            mapped_messages.iter().map(|m| m.tags.clone()).collect(),
                        )
                        .await?;

                    for mapped_message in mapped_messages {
                        // Each downstream handle shares the original ack tracking — ACK is
                        // deferred until all mapped messages are written to ISB/sink.
                        let downstream_handle = msg_handle.with_message(mapped_message);

                        // Try to bypass the message. If bypassed, try_bypass takes ownership and returns None.
                        // If not bypassed, it returns Some(downstream_handle) for us to send downstream.
                        let downstream_handle = if let Some(ref bypass_router) = self.bypass_router
                        {
                            match bypass_router
                                .try_bypass(downstream_handle)
                                .await
                                .expect("failed to send message to bypass channel")
                            {
                                Some(msg) => msg,
                                None => continue, // Message was bypassed, move to next
                            }
                        } else {
                            downstream_handle
                        };

                        self.output_tx
                            .send(downstream_handle)
                            .await
                            .expect("failed to send response");
                    }

                    // Decrement the original ref_count for this message now that all downstream
                    // handles have been created and sent.
                    mark_success!(msg_handle);
                }
                Err(e) => {
                    error!(err=?e, "failed to map message");
                    mark_failed!(msg_handle, &e, None);
                    return Err(e);
                }
            }
        }

        Ok(())
    }
}
