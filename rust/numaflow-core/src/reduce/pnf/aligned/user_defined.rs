use crate::Result;
use crate::config::get_vertex_name;
use crate::message::{IntOffset, Message, MessageID, Offset};
use crate::reduce::pnf::aligned::reducer::{AlignedWindowMessage, WindowOperation};
use crate::shared::grpc::{prost_timestamp_from_utc, utc_from_timestamp};
use chrono::Utc;
use numaflow_pb::clients::reduce::reduce_client::ReduceClient;
use numaflow_pb::clients::reduce::{ReduceRequest, ReduceResponse, reduce_request};
use std::collections::HashMap;
use std::sync::Arc;
use tokio_stream::StreamExt;
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::Channel;

impl From<&Message> for reduce_request::Payload {
    fn from(msg: &Message) -> Self {
        Self {
            keys: msg.keys.to_vec(),
            value: msg.value.to_vec(),
            event_time: Some(prost_timestamp_from_utc(msg.event_time)),
            watermark: msg.watermark.map(prost_timestamp_from_utc),
            headers: msg.headers.clone(),
        }
    }
}

impl From<AlignedWindowMessage> for ReduceRequest {
    fn from(value: AlignedWindowMessage) -> Self {
        // Extract window and operation based on the message type
        let (window, operation) = match &value {
            AlignedWindowMessage::Fixed(fixed_msg) => (&fixed_msg.window, &fixed_msg.operation),
            AlignedWindowMessage::Sliding(sliding_msg) => {
                (&sliding_msg.window, &sliding_msg.operation)
            }
        };

        // Create the window object that's common for both Fixed and Sliding
        let window_obj = numaflow_pb::clients::reduce::Window {
            start: Some(prost_timestamp_from_utc(window.start_time)),
            end: Some(prost_timestamp_from_utc(window.end_time)),
            slot: "0".to_string(),
        };

        // Process the operation
        match operation {
            WindowOperation::Open(msg) => {
                let operation = Some(reduce_request::WindowOperation {
                    event: reduce_request::window_operation::Event::Open as i32,
                    windows: vec![window_obj],
                });

                ReduceRequest {
                    payload: Some(msg.into()),
                    operation,
                }
            }
            WindowOperation::Close => {
                let operation = Some(reduce_request::WindowOperation {
                    event: reduce_request::window_operation::Event::Close as i32,
                    windows: vec![window_obj],
                });

                ReduceRequest {
                    payload: None,
                    operation,
                }
            }
            WindowOperation::Append(msg) => {
                let operation = Some(reduce_request::WindowOperation {
                    event: reduce_request::window_operation::Event::Append as i32,
                    windows: vec![window_obj],
                });

                ReduceRequest {
                    payload: Some(msg.into()),
                    operation,
                }
            }
        }
    }
}

/// Wrapper for ReduceResponse that includes index and vertex name
struct UdReducerResponse {
    pub response: ReduceResponse,
    pub index: i32,
    pub vertex_name: String,
}

impl From<UdReducerResponse> for Message {
    fn from(wrapper: UdReducerResponse) -> Self {
        let result = wrapper.response.result.unwrap_or_default();
        let window = wrapper.response.window.unwrap_or_default();

        // Create offset from window start and end time
        let offset_str = if let (Some(start), Some(end)) = (window.start, window.end) {
            format!("{}-{}", start.seconds, end.seconds)
        } else {
            "0-0".to_string()
        };

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
            event_time: Utc::now(),
            watermark: window
                .end
                .map(|ts| utc_from_timestamp(ts) - chrono::Duration::milliseconds(1)),
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

#[derive(Clone)]
pub(crate) struct UserDefinedAlignedReduce {
    client: ReduceClient<Channel>,
}

impl UserDefinedAlignedReduce {
    pub(crate) async fn new(client: ReduceClient<Channel>) -> Self {
        UserDefinedAlignedReduce { client }
    }

    pub(crate) async fn reduce_fn(
        &mut self,
        stream: ReceiverStream<AlignedWindowMessage>,
        result_tx: tokio::sync::mpsc::Sender<Message>,
    ) -> Result<()> {
        // Convert AlignedWindowMessage stream to ReduceRequest stream
        let (req_tx, req_rx) = tokio::sync::mpsc::channel(100);

        // Spawn a task to convert AlignedWindowMessages to ReduceRequests
        let conversion_handle = tokio::spawn(async move {
            let mut stream = stream;
            while let Some(window_msg) = stream.next().await {
                let reduce_req: ReduceRequest = window_msg.into();
                if req_tx.send(reduce_req).await.is_err() {
                    break;
                }
            }
        });

        // Call the gRPC reduce_fn with the converted stream
        let mut response_stream = self
            .client
            .reduce_fn(ReceiverStream::new(req_rx))
            .await
            .map_err(|e| crate::Error::Reduce(format!("failed to call reduce_fn: {}", e)))?
            .into_inner();

        // Process the response stream
        let vertex_name = get_vertex_name().to_string();
        let mut index = 0;

        while let Some(response) = response_stream
            .message()
            .await
            .map_err(|e| crate::Error::Reduce(format!("failed to receive response: {}", e)))?
        {
            if response.eof {
                break;
            }

            let wrapper = UdReducerResponse {
                response,
                index,
                vertex_name: vertex_name.clone(),
            };

            let message: Message = wrapper.into();
            result_tx
                .send(message)
                .await
                .expect("failed to send response");

            index += 1;
        }

        if let Err(e) = conversion_handle.await {
            return Err(crate::Error::Reduce(format!(
                "conversion task failed: {}",
                e
            )));
        }

        Ok(())
    }
}
