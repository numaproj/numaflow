//! Merging of multiple per-edge ISB reader streams into a single sequence.
//!
//! Buffers are edge-owned, so a vertex with more than one ingress edge (a join) reads a
//! separate physical buffer per source and has one [`ISBReaderOrchestrator`] per
//! `(edge, partition)` it is responsible for. Where those readers land in the same pod —
//! reduce, and ordered map/sink — their streams have to be merged before the data is handed
//! to a single processing stage.
//!
//! [`ISBReaderOrchestrator`]: crate::pipeline::isb::reader::ISBReaderOrchestrator

use tokio_stream::StreamExt;
use tokio_stream::wrappers::ReceiverStream;

use crate::message::MessageHandle;

/// Merges the per-edge ISB reader streams into one sequence, taking one message from
/// each non-exhausted stream in turn.
///
/// Round-robin gives every edge equal turns, so one fast source can not starve the others:
/// the downstream stage sees both sides of a join interleaved rather than all of x before
/// any of y.
///
/// A stream that yields `None` is exhausted and dropped from the rotation; the merge ends
/// when every stream is exhausted. Each `MessageHandle` carries its own ack path back to the
/// reader that produced it, so merging the data streams does not disturb acking.
///
/// Note that [`Self::next`] awaits the current edge rather than polling all edges for
/// whichever is ready first. For a join that is the intent — wait for the slow source
/// instead of letting the fast one race ahead — and it does not deadlock on a genuinely
/// idle edge because an idle partition still emits a WMB control message.
pub(crate) struct RoundRobinReaders {
    streams: Vec<ReceiverStream<MessageHandle>>,
    next: usize,
}

impl RoundRobinReaders {
    pub(crate) fn new(streams: Vec<ReceiverStream<MessageHandle>>) -> Self {
        Self { streams, next: 0 }
    }

    /// Returns the next message, rotating across the live streams. `None` once all
    /// streams are exhausted.
    pub(crate) async fn next(&mut self) -> Option<MessageHandle> {
        while !self.streams.is_empty() {
            if self.next >= self.streams.len() {
                self.next = 0;
            }
            match self.streams[self.next].next().await {
                Some(msg) => {
                    // Advance so the next call starts at the following edge.
                    self.next += 1;
                    return Some(msg);
                }
                None => {
                    // This edge is done; drop it and retry at the same index, which
                    // now holds the next stream.
                    self.streams.remove(self.next);
                }
            }
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::Arc;

    use chrono::Utc;
    use tokio::sync::mpsc;

    use crate::message::Message;

    /// Builds a MessageHandle carrying `body`, with a throwaway ack channel.
    fn handle(body: &str) -> MessageHandle {
        use crate::message::{MessageID, Offset, StringOffset};
        let message = Message {
            typ: Default::default(),
            keys: Arc::from(vec![]),
            tags: None,
            value: body.as_bytes().to_vec().into(),
            offset: Offset::String(StringOffset::new(body.to_string(), 0)),
            event_time: Utc::now(),
            watermark: None,
            id: MessageID {
                vertex_name: "vertex".to_string().into(),
                offset: body.to_string().into(),
                index: 0,
            },
            ..Default::default()
        };
        let (ack_tx, _ack_rx) = tokio::sync::oneshot::channel();
        MessageHandle::new(message, ack_tx)
    }

    /// Feeds `bodies` into a ReceiverStream, mimicking one edge's ISB reader.
    fn edge_stream(bodies: &[&str]) -> ReceiverStream<MessageHandle> {
        let (tx, rx) = mpsc::channel(16);
        for b in bodies {
            tx.try_send(handle(b)).expect("channel capacity");
        }
        drop(tx);
        ReceiverStream::new(rx)
    }

    async fn drain(mut rr: RoundRobinReaders) -> Vec<String> {
        let mut out = vec![];
        while let Some(h) = rr.next().await {
            out.push(String::from_utf8(h.message.value.to_vec()).unwrap());
        }
        out
    }

    /// Two equal-length edges alternate one message at a time.
    ///
    /// This is the join case: x and y both feed the pod, and neither should be
    /// drained ahead of the other.
    #[tokio::test]
    async fn test_round_robin_alternates_between_edges() {
        let rr = RoundRobinReaders::new(vec![
            edge_stream(&["x0", "x1", "x2"]),
            edge_stream(&["y0", "y1", "y2"]),
        ]);
        assert_eq!(drain(rr).await, vec!["x0", "y0", "x1", "y1", "x2", "y2"]);
    }

    /// A short edge drops out of the rotation and the rest keep draining.
    ///
    /// Without removing the exhausted stream the merge would either stall or spin.
    #[tokio::test]
    async fn test_round_robin_drains_remaining_edges_after_one_ends() {
        let rr =
            RoundRobinReaders::new(vec![edge_stream(&["x0"]), edge_stream(&["y0", "y1", "y2"])]);
        assert_eq!(drain(rr).await, vec!["x0", "y0", "y1", "y2"]);
    }

    /// Every message from every edge is delivered exactly once.
    ///
    /// The regression this guards: reading only the first edge, which would silently
    /// drop the other source's data in a join.
    #[tokio::test]
    async fn test_round_robin_loses_no_messages() {
        let rr = RoundRobinReaders::new(vec![
            edge_stream(&["x0", "x1"]),
            edge_stream(&["y0"]),
            edge_stream(&["z0", "z1", "z2"]),
        ]);
        let mut got = drain(rr).await;
        got.sort();
        assert_eq!(got, vec!["x0", "x1", "y0", "z0", "z1", "z2"]);
    }

    /// A single edge (the non-join case) passes through in order.
    #[tokio::test]
    async fn test_round_robin_single_edge_is_passthrough() {
        let rr = RoundRobinReaders::new(vec![edge_stream(&["a", "b", "c"])]);
        assert_eq!(drain(rr).await, vec!["a", "b", "c"]);
    }
}
