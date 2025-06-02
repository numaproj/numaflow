use crate::config::get_vertex_name;
use crate::message::{IntOffset, Message, MessageID, Offset};
use crate::reduce::reducer::unaligned::windower::{UnalignedWindowMessage, Window};
use crate::shared::grpc::{prost_timestamp_from_utc, utc_from_timestamp};
use numaflow_pb::clients::sessionreduce;
use numaflow_pb::clients::sessionreduce::session_reduce_client::SessionReduceClient;
use numaflow_pb::clients::sessionreduce::{
    SessionReduceRequest, SessionReduceResponse, session_reduce_request,
};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;
use tonic::transport::Channel;
use tracing::info;

impl From<Message> for session_reduce_request::Payload {
    fn from(msg: Message) -> Self {
        Self {
            keys: msg.keys.to_vec(),
            value: msg.value.to_vec(),
            event_time: Some(prost_timestamp_from_utc(msg.event_time)),
            watermark: msg.watermark.map(prost_timestamp_from_utc),
            headers: msg.headers.clone(),
        }
    }
}

impl From<Window> for sessionreduce::KeyedWindow {
    fn from(window: Window) -> Self {
        Self {
            start: Some(prost_timestamp_from_utc(window.start_time)),
            end: Some(prost_timestamp_from_utc(window.end_time)),
            slot: "0".to_string(),
            keys: window.keys.to_vec(),
        }
    }
}

impl From<UnalignedWindowMessage> for SessionReduceRequest {
    fn from(value: UnalignedWindowMessage) -> Self {
        match value {
            UnalignedWindowMessage::Open { message, window } => {
                let operation = Some(session_reduce_request::WindowOperation {
                    event: session_reduce_request::window_operation::Event::Open as i32,
                    keyed_windows: vec![window.into()],
                });

                SessionReduceRequest {
                    payload: Some(message.into()),
                    operation,
                }
            }
            UnalignedWindowMessage::Close(window) => {
                let operation = Some(session_reduce_request::WindowOperation {
                    event: session_reduce_request::window_operation::Event::Close as i32,
                    keyed_windows: vec![window.into()],
                });

                SessionReduceRequest {
                    payload: None,
                    operation,
                }
            }
            UnalignedWindowMessage::Append { message, window } => {
                let operation = Some(session_reduce_request::WindowOperation {
                    event: session_reduce_request::window_operation::Event::Append as i32,
                    keyed_windows: vec![window.into()],
                });

                SessionReduceRequest {
                    payload: Some(message.into()),
                    operation,
                }
            }
            UnalignedWindowMessage::Merge { windows } => {
                let operation = Some(session_reduce_request::WindowOperation {
                    event: session_reduce_request::window_operation::Event::Merge as i32,
                    keyed_windows: windows.into_iter().map(Into::into).collect(),
                });

                SessionReduceRequest {
                    payload: None,
                    operation,
                }
            }
            UnalignedWindowMessage::Expand { message, windows } => {
                let operation = Some(session_reduce_request::WindowOperation {
                    event: session_reduce_request::window_operation::Event::Expand as i32,
                    keyed_windows: windows.into_iter().map(Into::into).collect(),
                });

                SessionReduceRequest {
                    payload: Some(message.into()),
                    operation,
                }
            }
        }
    }
}

/// Wrapper for SessionReduceResponse that includes index and vertex name.
pub(crate) struct UdSessionReducerResponse {
    pub response: SessionReduceResponse,
    pub index: i32,
    pub vertex_name: &'static str,
}

impl From<SessionReduceResponse> for Message {
    fn from(response: SessionReduceResponse) -> Self {
        let result = response.result.unwrap_or_default();
        let window = response.keyed_window.unwrap_or_default();

        // Create offset from window start and end time
        let offset_str = format!(
            "{}-{}",
            utc_from_timestamp(window.start.expect("window start missing")).timestamp_millis(),
            utc_from_timestamp(window.end.expect("window end missing")).timestamp_millis()
        );

        Message {
            typ: Default::default(),
            keys: Arc::from(result.keys),
            tags: if result.tags.is_empty() {
                None
            } else {
                Some(Arc::from(result.tags))
            },
            value: result.value.into(),
            offset: Offset::Int(IntOffset::new(0, 0)),
            event_time: utc_from_timestamp(window.end.unwrap()),
            watermark: window
                .end
                .map(|ts| utc_from_timestamp(ts) - chrono::Duration::milliseconds(1)),
            // this will be unique for each response which will be used for dedup (index is used because
            // each window can have multiple reduce responses)
            id: MessageID {
                vertex_name: get_vertex_name().to_string().into(),
                offset: offset_str.into(),
                index: 0,
            },
            headers: HashMap::new(),
            metadata: None,
            is_late: false,
        }
    }
}

/// User-defined session reduce client.
#[derive(Clone)]
pub(crate) struct UserDefinedSessionReduce {
    client: SessionReduceClient<Channel>,
}

impl UserDefinedSessionReduce {
    pub(crate) async fn new(client: SessionReduceClient<Channel>) -> Self {
        UserDefinedSessionReduce { client }
    }

    /// Calls the reduce_fn on the user-defined session reducer on a separate tokio task.
    /// If the cancellation token is triggered, it will stop processing and return early.
    pub(crate) async fn reduce_fn(
        &mut self,
        stream: ReceiverStream<SessionReduceRequest>,
        cln_token: CancellationToken,
    ) -> crate::Result<(
        ReceiverStream<SessionReduceResponse>,
        JoinHandle<crate::Result<()>>,
    )> {
        // Create a channel for responses
        let (result_tx, result_rx) = tokio::sync::mpsc::channel(100);

        let mut client = self.client.clone();

        // Start a background task to process responses
        let handle = tokio::spawn(async move {
            // Call the gRPC reduce_fn with the stream
            let mut response_stream = match client.session_reduce_fn(stream).await {
                Ok(response) => response.into_inner(),
                Err(e) => {
                    return Err(crate::Error::Reduce(format!(
                        "failed to call reduce_fn: {}",
                        e
                    )));
                }
            };

            loop {
                tokio::select! {
                    // Check for cancellation
                    _ = cln_token.cancelled() => {
                        info!("Cancellation detected while processing responses, stopping");
                        return Err(crate::Error::Cancelled());
                    }

                    // Process next response
                    response = response_stream.message() => {
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
