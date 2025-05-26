use crate::config::get_vertex_name;
use crate::message::{IntOffset, Message, MessageID, Offset};
use crate::reduce::reducer::unaligned::windower::{
    UnalignedWindowMessage, Window, WindowOperation,
};
use crate::shared::grpc::{prost_timestamp_from_utc, utc_from_timestamp};
use numaflow_pb::clients::sessionreduce;
use numaflow_pb::clients::sessionreduce::session_reduce_client::SessionReduceClient;
use numaflow_pb::clients::sessionreduce::{
    SessionReduceRequest, SessionReduceResponse, session_reduce_request,
};
use std::collections::HashMap;
use std::sync::Arc;
use tokio_stream::StreamExt;
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
            UnalignedWindowMessage::Merge { message, windows } => {
                let operation = Some(session_reduce_request::WindowOperation {
                    event: session_reduce_request::window_operation::Event::Merge as i32,
                    keyed_windows: windows.into_iter().map(Into::into).collect(),
                });

                SessionReduceRequest {
                    payload: Some(message.into()),
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
struct UdSessionReducerResponse {
    pub response: SessionReduceResponse,
    pub index: i32,
    pub vertex_name: &'static str,
}

impl From<UdSessionReducerResponse> for Message {
    fn from(wrapper: UdSessionReducerResponse) -> Self {
        let result = wrapper.response.result.unwrap_or_default();
        let window = wrapper.response.keyed_window.unwrap_or_default();

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
                vertex_name: wrapper.vertex_name.into(),
                offset: offset_str.into(),
                index: wrapper.index,
            },
            headers: HashMap::new(),
            metadata: None,
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
        stream: ReceiverStream<UnalignedWindowMessage>,
        result_tx: tokio::sync::mpsc::Sender<Message>,
        cln_token: CancellationToken,
    ) -> crate::error::Result<()> {
        // Convert UnalignedWindowMessage stream to ReduceRequest stream
        let (req_tx, req_rx) = tokio::sync::mpsc::channel(100);

        // Spawn a task to convert UnalignedWindowMessages to ReduceRequests and send them to req_tx
        // NOTE: - This is not really required (for client side streaming reduce), we do this because
        //         `tonic` does not return a stream from the reduce_fn unless there is some output.
        //       - This implementation works for reduce bidi streaming.
        let request_handle = tokio::spawn(async move {
            let mut stream = stream;
            while let Some(window_msg) = stream.next().await {
                let reduce_req: SessionReduceRequest = window_msg.into();
                if req_tx.send(reduce_req).await.is_err() {
                    break;
                }
            }
        });

        // Call the gRPC reduce_fn with the converted stream, but also watch for cancellation
        let mut response_stream = tokio::select! {
            // Wait for the gRPC call to complete
            result = self.client.session_reduce_fn(ReceiverStream::new(req_rx)) => {
                result
                    .map_err(|e| crate::Error::Reduce(format!("failed to call reduce_fn: {}", e)))?
                    .into_inner()
            }

            // Check for cancellation
            _ = cln_token.cancelled() => {
                info!("Cancellation detected while waiting for reduce_fn response");
                request_handle.abort();
                return Err(crate::Error::Cancelled());
            }
        };

        // Process the response stream
        let vertex_name = get_vertex_name();
        let mut index = 0;

        loop {
            tokio::select! {
                // Check for cancellation
                _ = cln_token.cancelled() => {
                    info!("Cancellation detected while processing responses, stopping");
                    request_handle.abort();
                    return Err(crate::Error::Cancelled());
                }

                // Process next response
                response = response_stream.message() => {
                    let response = response.map_err(|e| crate::Error::Reduce(format!("failed to receive response: {}", e)))?;
                    let Some(response) = response else {
                        break;
                    };

                    if response.eof {
                        break;
                    }

                    // convert to Message so it can be sent to the ISB write channel
                    let message: Message = UdSessionReducerResponse {
                        response,
                        index,
                        vertex_name,
                    }
                    .into();

                    result_tx
                        .send(message)
                        .await
                        .expect("failed to send response");

                    index += 1;
                }
            }
        }

        // wait for the tokio task to complete
        request_handle
            .await
            .map_err(|e| crate::Error::Reduce(format!("conversion task failed: {}", e)))
    }

    pub(crate) async fn ready(&mut self) -> bool {
        match self.client.is_ready(tonic::Request::new(())).await {
            Ok(response) => response.into_inner().ready,
            Err(_) => false,
        }
    }
}
