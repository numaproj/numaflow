//! Merging of multiple per-edge ISB reader streams into a single sequence.
//!
//! Buffers are edge-owned, so a vertex with more than one ingress edge (a join) reads a
//! separate physical buffer per source and has one [`ISBReaderOrchestrator`] per
//! `(edge, partition)` it is responsible for. Where those readers land in the same pod —
//! reduce, and ordered map/sink — their streams have to be merged before the data is handed
//! to a single processing stage.
//!
//! [`ISBReaderOrchestrator`]: crate::pipeline::isb::reader::ISBReaderOrchestrator

use futures::StreamExt;
use futures::stream::SelectAll;
use tokio_stream::wrappers::ReceiverStream;

use crate::message::MessageHandle;

/// Merges the per-edge ISB reader streams into one sequence, yielding a message from
/// whichever edge has one ready.
///
/// The merge must never wait on one particular edge. A slow or idle edge would otherwise
/// gate every other edge: the fast side of a join backs up behind it, and a source that
/// produces nothing (an idle edge only gets a WMB control message once its source has a
/// watermark to report) stops the whole vertex. The backlog that builds up is not just a
/// throughput problem either. Once it is older than the watermark timeline keeps, its
/// messages can no longer be given a watermark and reach the reducer behind a watermark
/// that other edges have moved on, so they are dropped as late.
///
/// Letting a fast edge run ahead is safe for windowing. Every message carries the vertex
/// watermark, the minimum of the last processed watermark across all edges and partitions,
/// so a window still does not close until the slowest edge has been read past it.
///
/// [`SelectAll`] puts an edge at the back of the queue after it yields, so no edge starves
/// the others while several are ready, and each edge's own messages stay in order. A
/// stream that yields `None` is exhausted and dropped. The merge ends when every stream is
/// exhausted. Each `MessageHandle` carries its own ack path back to the reader that produced
/// it, so merging the data streams does not disturb acking.
pub(crate) struct MergedReaders {
    streams: SelectAll<ReceiverStream<MessageHandle>>,
}

impl MergedReaders {
    pub(crate) fn new(streams: Vec<ReceiverStream<MessageHandle>>) -> Self {
        Self {
            streams: futures::stream::select_all(streams),
        }
    }

    /// Returns the next message from any edge that has one ready. `None` once all
    /// streams are exhausted.
    pub(crate) async fn next(&mut self) -> Option<MessageHandle> {
        self.streams.next().await
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

    fn body(h: &MessageHandle) -> String {
        String::from_utf8(h.message.value.to_vec()).unwrap()
    }

    async fn drain(mut merged: MergedReaders) -> Vec<String> {
        let mut out = vec![];
        while let Some(h) = merged.next().await {
            out.push(body(&h));
        }
        out
    }

    /// The messages from one edge, in the order the merge yielded them.
    fn from_edge(got: &[String], prefix: char) -> Vec<String> {
        got.iter()
            .filter(|b| b.starts_with(prefix))
            .cloned()
            .collect()
    }

    /// An edge that never yields must not block the others. This is the join with an idle
    /// source: x has data, y's reader is open but has nothing to send.
    #[tokio::test]
    async fn test_merge_does_not_wait_on_an_idle_edge() {
        let (_idle_tx, idle_rx) = mpsc::channel::<MessageHandle>(1);
        let mut merged = MergedReaders::new(vec![
            ReceiverStream::new(idle_rx),
            edge_stream(&["x0", "x1", "x2"]),
        ]);
        for want in ["x0", "x1", "x2"] {
            let got = tokio::time::timeout(std::time::Duration::from_secs(1), merged.next())
                .await
                .expect("merge blocked on the idle edge");
            assert_eq!(body(&got.unwrap()), want);
        }
    }

    /// While several edges have data, each gets turns, so a fast edge cannot hold back a
    /// slower one, and each edge's own order is kept.
    #[tokio::test]
    async fn test_merge_interleaves_ready_edges_in_order() {
        let merged = MergedReaders::new(vec![
            edge_stream(&["x0", "x1", "x2"]),
            edge_stream(&["y0", "y1", "y2"]),
        ]);
        let got = drain(merged).await;
        assert_eq!(from_edge(&got, 'x'), vec!["x0", "x1", "x2"]);
        assert_eq!(from_edge(&got, 'y'), vec!["y0", "y1", "y2"]);
        // Neither edge is fully drained before the other starts.
        let first_y = got.iter().position(|b| b.starts_with('y')).unwrap();
        assert!(first_y < 3, "y waited for all of x: {got:?}");
    }

    /// A short edge drops out and the rest keep draining.
    #[tokio::test]
    async fn test_merge_drains_remaining_edges_after_one_ends() {
        let merged =
            MergedReaders::new(vec![edge_stream(&["x0"]), edge_stream(&["y0", "y1", "y2"])]);
        let got = drain(merged).await;
        assert_eq!(from_edge(&got, 'x'), vec!["x0"]);
        assert_eq!(from_edge(&got, 'y'), vec!["y0", "y1", "y2"]);
    }

    /// Every message from every edge is delivered exactly once.
    ///
    /// The regression this guards: reading only the first edge, which would silently
    /// drop the other source's data in a join.
    #[tokio::test]
    async fn test_merge_loses_no_messages() {
        let merged = MergedReaders::new(vec![
            edge_stream(&["x0", "x1"]),
            edge_stream(&["y0"]),
            edge_stream(&["z0", "z1", "z2"]),
        ]);
        let mut got = drain(merged).await;
        got.sort();
        assert_eq!(got, vec!["x0", "x1", "y0", "z0", "z1", "z2"]);
    }

    /// A single edge (the non-join case) passes through in order.
    #[tokio::test]
    async fn test_merge_single_edge_is_passthrough() {
        let merged = MergedReaders::new(vec![edge_stream(&["a", "b", "c"])]);
        assert_eq!(drain(merged).await, vec!["a", "b", "c"]);
    }
}
