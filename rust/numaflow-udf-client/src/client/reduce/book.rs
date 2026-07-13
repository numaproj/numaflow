use numaflow_pb::clients::reduce::reduce_client::ReduceClient;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::task::AbortOnDropHandle;
use tonic::transport::Channel;

use crate::error::{Result, UdfClientError};
use crate::model::{AlignedReduceResult, UdfDatum, Window};
use crate::wire::reduce::{append_request, decode_result, is_eof, open_request};

/// Buffered reduce responses for one aligned window book.
const REDUCE_RESP_CHANNEL_SIZE: usize = 64;

/// Request side of one aligned ReduceFn window book.
pub struct AlignedReduceBook {
    window: Window,
    request_tx: mpsc::Sender<numaflow_pb::clients::reduce::ReduceRequest>,
}

/// Response side of one aligned ReduceFn window book.
pub struct AlignedReduceReceiver {
    response_rx: mpsc::Receiver<Option<Result<AlignedReduceResult>>>,
    completed: bool,
    _pump: AbortOnDropHandle<()>,
}

impl AlignedReduceBook {
    /// Opens a ReduceFn stream for `window`, queues OPEN for `first`, and returns immediately.
    pub async fn open(
        client: ReduceClient<Channel>,
        window: Window,
        first: UdfDatum,
        request_buffer: usize,
    ) -> Result<(Self, AlignedReduceReceiver)> {
        if request_buffer == 0 {
            return Err(UdfClientError::InvalidConfig(
                "reduce request buffer must be greater than zero".to_string(),
            ));
        }

        let (request_tx, request_rx) = mpsc::channel(request_buffer);
        request_tx
            .send(open_request(&window, first))
            .await
            .map_err(|_| UdfClientError::ReduceRequestStreamClosed)?;

        let (response_tx, response_rx) = mpsc::channel(REDUCE_RESP_CHANNEL_SIZE);
        let expected_window = window.clone();
        let pump = tokio::spawn(async move {
            pump_reduce_fn(client, request_rx, response_tx, expected_window).await;
        });

        Ok((
            Self { window, request_tx },
            AlignedReduceReceiver {
                response_rx,
                completed: false,
                _pump: AbortOnDropHandle::new(pump),
            },
        ))
    }

    /// Sends APPEND for another datum in this book's window.
    pub async fn append(&self, datum: UdfDatum) -> Result<()> {
        self.request_tx
            .send(append_request(&self.window, datum))
            .await
            .map_err(|_| UdfClientError::ReduceRequestStreamClosed)
    }

    /// Closes the ReduceFn request stream by dropping the book.
    pub fn close(self) {}
}

impl AlignedReduceReceiver {
    /// Receives the next result for this window, or `None` after EOF / stream end.
    pub async fn recv(&mut self) -> Option<Result<AlignedReduceResult>> {
        if self.completed {
            return None;
        }

        match self.response_rx.recv().await {
            Some(Some(result)) => Some(result),
            Some(None) => {
                self.completed = true;
                None
            }
            None => {
                self.completed = true;
                Some(Err(UdfClientError::ReduceResponseChannelClosed))
            }
        }
    }
}

async fn pump_reduce_fn(
    mut client: ReduceClient<Channel>,
    request_rx: mpsc::Receiver<numaflow_pb::clients::reduce::ReduceRequest>,
    response_tx: mpsc::Sender<Option<Result<AlignedReduceResult>>>,
    expected_window: Window,
) {
    let mut response_stream = match client.reduce_fn(ReceiverStream::new(request_rx)).await {
        Ok(response) => response.into_inner(),
        Err(status) => {
            let _ = response_tx
                .send(Some(Err(UdfClientError::ReduceFnStart(status))))
                .await;
            let _ = response_tx.send(None).await;
            return;
        }
    };

    loop {
        match response_stream.message().await {
            Ok(Some(response)) => {
                if is_eof(&response) {
                    break;
                }
                match decode_result(response, Some(&expected_window)) {
                    Ok(Some(result)) => {
                        if response_tx.send(Some(Ok(result))).await.is_err() {
                            break;
                        }
                    }
                    Ok(None) => {}
                    Err(error) => {
                        let _ = response_tx.send(Some(Err(error))).await;
                        break;
                    }
                }
            }
            Ok(None) => break,
            Err(status) => {
                let _ = response_tx
                    .send(Some(Err(UdfClientError::ReduceGrpc(status))))
                    .await;
                break;
            }
        }
    }

    let _ = response_tx.send(None).await;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn receiver_reports_unexpected_internal_channel_close_once() {
        let (response_tx, response_rx) = mpsc::channel(1);
        drop(response_tx);
        let pump = tokio::spawn(async {});
        let mut receiver = AlignedReduceReceiver {
            response_rx,
            completed: false,
            _pump: AbortOnDropHandle::new(pump),
        };

        assert!(matches!(
            receiver.recv().await,
            Some(Err(UdfClientError::ReduceResponseChannelClosed))
        ));
        assert!(receiver.recv().await.is_none());
    }
}
