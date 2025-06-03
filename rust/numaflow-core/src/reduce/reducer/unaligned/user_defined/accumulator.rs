use crate::config::get_vertex_name;
use crate::message::{IntOffset, Message, MessageID, Offset};
use crate::reduce::reducer::unaligned::windower::{UnalignedWindowMessage, UnalignedWindowOperation, Window};
use crate::shared::grpc::{prost_timestamp_from_utc, utc_from_timestamp};
use numaflow_pb::clients::accumulator::accumulator_client::AccumulatorClient;
use numaflow_pb::clients::accumulator::{
    AccumulatorRequest, AccumulatorResponse, accumulator_request,
};

use numaflow_pb::clients::accumulator;
use std::collections::HashMap;
use std::sync::Arc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;
use tonic::transport::Channel;
use tracing::info;

impl From<Message> for accumulator::Payload {
    fn from(msg: Message) -> Self {
        Self {
            keys: msg.keys.to_vec(),
            value: msg.value.to_vec(),
            event_time: Some(prost_timestamp_from_utc(msg.event_time)),
            watermark: msg.watermark.map(prost_timestamp_from_utc),
            id: msg.id.to_string(),
            headers: msg.headers.clone(),
        }
    }
}

impl From<Window> for accumulator::KeyedWindow {
    fn from(window: Window) -> Self {
        Self {
            start: Some(prost_timestamp_from_utc(window.start_time)),
            end: Some(prost_timestamp_from_utc(window.end_time)),
            slot: "0".to_string(),
            keys: window.keys.to_vec(),
        }
    }
}

impl From<UnalignedWindowMessage> for AccumulatorRequest {
    fn from(value: UnalignedWindowMessage) -> Self {
        match value.operation {
            UnalignedWindowOperation::Open {
                message,
                window,
            } => {
                let operation = Some(accumulator_request::WindowOperation {
                    event: accumulator_request::window_operation::Event::Open as i32,
                    keyed_window: Some(window.into()),
                });

                AccumulatorRequest {
                    payload: Some(message.into()),
                    operation,
                }
            }
            UnalignedWindowOperation::Close {
                window,
            } => {
                let operation = Some(accumulator_request::WindowOperation {
                    event: accumulator_request::window_operation::Event::Close as i32,
                    keyed_window: Some(window.into()),
                });

                AccumulatorRequest {
                    payload: None,
                    operation,
                }
            }
            UnalignedWindowOperation::Append {
                message,
                window,
            } => {
                let operation = Some(accumulator_request::WindowOperation {
                    event: accumulator_request::window_operation::Event::Append as i32,
                    keyed_window: Some(window.into()),
                });

                AccumulatorRequest {
                    payload: Some(message.into()),
                    operation,
                }
            }
            _ => panic!("Unsupported operation for accumulator window"),
        }
    }
}

/// Wrapper for AccumulatorReduceResponse that includes index and vertex name.
pub(crate) struct UdAccumulatorReducerResponse {
    pub(crate) response: AccumulatorResponse,
    pub(crate) index: i32,
    pub(crate) vertex_name: &'static str,
}

impl From<AccumulatorResponse> for Message {
    fn from(response: AccumulatorResponse) -> Self {
        let result = response.payload.unwrap_or_default();
        let window = response.window.unwrap_or_default();
        let tags = response.tags;

        Message {
            typ: Default::default(),
            keys: Arc::from(result.keys),
            tags: (!tags.is_empty()).then(|| Arc::from(tags)),
            value: result.value.into(),
            offset: Offset::Int(IntOffset::new(0, 0)),
            event_time: utc_from_timestamp(window.end.unwrap()),
            watermark: window
                .end
                .map(|ts| utc_from_timestamp(ts) - chrono::Duration::milliseconds(1)),
            id: MessageID {
                vertex_name: get_vertex_name().to_string().into(),
                offset: result.id.into(),
                index: 0,
            },
            headers: HashMap::new(),
            metadata: None,
            is_late: false,
        }
    }
}

/// User-defined accumulator client.
#[derive(Clone)]
pub(crate) struct UserDefinedAccumulator {
    client: AccumulatorClient<Channel>,
}

impl UserDefinedAccumulator {
    pub(crate) async fn new(client: AccumulatorClient<Channel>) -> Self {
        UserDefinedAccumulator { client }
    }

    /// Calls the reduce_fn on the user-defined accumulator on a separate tokio task.
    /// If the cancellation token is triggered, it will stop processing and return early.
    pub(crate) async fn reduce_fn(
        &mut self,
        stream: ReceiverStream<AccumulatorRequest>,
        cln_token: CancellationToken,
    ) -> crate::error::Result<(
        ReceiverStream<AccumulatorResponse>,
        tokio::task::JoinHandle<crate::error::Result<()>>,
    )> {
        // Create a channel for responses
        let (result_tx, result_rx) = tokio::sync::mpsc::channel(100);
        let mut client = self.client.clone();

        // Start a background task to process responses
        let handle = tokio::spawn(async move {
            // Call the gRPC reduce_fn with the stream
            let mut response_stream = match client.accumulate_fn(stream).await {
                Ok(response) => response.into_inner(),
                Err(e) => {
                    return Err(crate::Error::Reduce(format!(
                        "failed to call reduce_fn: {}",
                        e
                    )));
                }
            };

            info!("Waiting for responses from accumulator reduce function");

            loop {
                tokio::select! {
                    // Check for cancellation
                    _ = cln_token.cancelled() => {
                        info!("Cancellation detected while processing responses, stopping");
                        return Err(crate::Error::Cancelled());
                    }

                    // Process next response
                    response = response_stream.message() => {
                        info!("Received response from accumulator reduce function {:?}", response);
                        let response = match response {
                            Ok(r) => r,
                            Err(e) => return Err(crate::Error::Reduce(format!("failed to receive response: {}", e))),
                        };

                        let Some(response) = response else {
                            break;
                        };

                        if result_tx.send(response).await.is_err() {
                            return Err(crate::Error::Reduce("failed to send response".to_string()));
                        }
                    }
                }
            }

            Ok(())
        });

        // Return the receiver stream and handle immediately
        Ok((ReceiverStream::new(result_rx), handle))
    }

    pub(crate) async fn ready(&mut self) -> bool {
        match self.client.is_ready(tonic::Request::new(())).await {
            Ok(response) => response.into_inner().ready,
            Err(_) => false,
        }
    }
}
