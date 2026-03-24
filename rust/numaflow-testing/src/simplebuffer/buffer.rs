//! Internal buffer state and core types for the simple buffer.

use std::collections::{HashMap, VecDeque};

use bytes::Bytes;

/// Offset for identifying messages in the buffer.
#[derive(Debug, Clone, Hash, Eq, PartialEq, Ord, PartialOrd)]
pub struct Offset {
    /// Monotonically increasing sequence number for the message.
    pub sequence: i64,
    /// Partition index of the buffer.
    pub partition_idx: u16,
}

impl Offset {
    pub fn new(sequence: i64, partition_idx: u16) -> Self {
        Self {
            sequence,
            partition_idx,
        }
    }
}

impl std::fmt::Display for Offset {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}-{}", self.sequence, self.partition_idx)
    }
}

/// Message read from the buffer.
///
/// This is what the reader returns - just the payload, headers, and offset.
/// The ISB doesn't care about numaflow-core specific fields like keys, tags, event_time, etc.
#[derive(Debug, Clone)]
pub struct ReadMessage {
    /// The payload bytes.
    pub payload: Bytes,
    /// Message headers.
    pub headers: HashMap<String, String>,
    /// Offset for ack/nack operations.
    pub offset: Offset,
}

/// A slot in the circular buffer.
#[derive(Debug, Clone)]
pub(crate) struct BufferSlot {
    /// The payload bytes.
    pub(crate) payload: Bytes,
    /// Message headers.
    pub(crate) headers: HashMap<String, String>,
    /// Message ID for deduplication.
    pub(crate) id: String,
    /// Offset assigned to this message.
    pub(crate) offset: Offset,
    /// State of the message.
    pub(crate) state: MessageState,
    /// Monotonic sequence number.
    pub(crate) sequence: i64,
    /// Timestamp when the message was fetched (for WIP timeout tracking).
    pub(crate) fetched_at: Option<std::time::Instant>,
}

/// State of a message in the buffer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MessageState {
    /// Written but not yet fetched by any reader.
    Pending,
    /// Fetched but not yet acknowledged.
    InFlight,
    /// Successfully acknowledged (can be reclaimed).
    Acked,
}

/// Internal state of the circular buffer shared between readers and writers.
#[derive(Debug)]
pub(crate) struct BufferState {
    /// The circular buffer storage using [VecDeque] for efficient front/back operations.
    pub(crate) slots: VecDeque<BufferSlot>,
    /// Maximum capacity of the buffer.
    pub(crate) capacity: usize,
    /// Next sequence number to assign.
    pub(crate) next_sequence: i64,
    /// Mapping from offset to index in slots for fast lookup.
    pub(crate) offset_to_index: HashMap<Offset, usize>,
    /// Deduplication window: message_id (String) -> sequence number.
    pub(crate) dedup_window: HashMap<String, i64>,
    /// Usage limit (0.0 to 1.0) at which buffer is considered full.
    pub(crate) usage_limit: f64,
}

impl BufferState {
    pub(crate) fn new(capacity: usize, usage_limit: f64) -> Self {
        Self {
            slots: VecDeque::with_capacity(capacity),
            capacity,
            next_sequence: 1,
            offset_to_index: HashMap::new(),
            dedup_window: HashMap::new(),
            usage_limit,
        }
    }

    /// Calculate current buffer usage as a fraction.
    pub(crate) fn usage(&self) -> f64 {
        let active_count = self
            .slots
            .iter()
            .filter(|s| s.state != MessageState::Acked)
            .count();
        active_count as f64 / self.capacity as f64
    }

    /// Check if buffer is full based on usage limit.
    pub(crate) fn is_full(&self) -> bool {
        self.usage() >= self.usage_limit
    }

    /// Count pending messages (written but not fetched).
    pub(crate) fn pending_count(&self) -> usize {
        self.slots
            .iter()
            .filter(|s| s.state == MessageState::Pending)
            .count()
    }

    /// Count in-flight messages (fetched but not acked).
    pub(crate) fn in_flight_count(&self) -> usize {
        self.slots
            .iter()
            .filter(|s| s.state == MessageState::InFlight)
            .count()
    }

    /// Reclaim acked slots from the front of the buffer.
    pub(crate) fn reclaim_acked(&mut self) {
        while let Some(front) = self.slots.front() {
            if front.state == MessageState::Acked {
                let slot = self.slots.pop_front().unwrap();
                self.offset_to_index.remove(&slot.offset);
                self.dedup_window.remove(&slot.id);
                // FIXME: this does not look very efficient since we will be calling reclaim_acked
                //   very often.
                // Update indices for remaining slots
                for (_, idx) in self.offset_to_index.iter_mut() {
                    if *idx > 0 {
                        *idx -= 1;
                    }
                }
            } else {
                break;
            }
        }
    }

    /// Find a slot by offset.
    pub(crate) fn find_slot_mut(&mut self, offset: &Offset) -> Option<&mut BufferSlot> {
        self.offset_to_index
            .get(offset)
            .copied()
            .and_then(|idx| self.slots.get_mut(idx))
    }

    /// Find a slot by offset (immutable).
    #[allow(dead_code)]
    pub(crate) fn find_slot(&self, offset: &Offset) -> Option<&BufferSlot> {
        self.offset_to_index
            .get(offset)
            .copied()
            .and_then(|idx| self.slots.get(idx))
    }
}

#[cfg(test)]
#[allow(clippy::indexing_slicing)]
mod tests {
    use super::*;
    use bytes::Bytes;

    fn create_test_slot(seq: i64, state: MessageState) -> BufferSlot {
        BufferSlot {
            payload: Bytes::from("test"),
            headers: HashMap::new(),
            id: format!("msg-{}", seq),
            offset: Offset::new(seq, 0),
            state,
            sequence: seq,
            fetched_at: None,
        }
    }

    #[test]
    fn test_buffer_state_new_and_usage() {
        // Test initial state
        let state = BufferState::new(100, 0.8);
        assert_eq!(state.capacity, 100);
        assert_eq!(state.next_sequence, 1);
        assert!(state.slots.is_empty());
        assert!((state.usage_limit - 0.8).abs() < f64::EPSILON);
        assert!((state.usage() - 0.0).abs() < f64::EPSILON);

        // Test usage with pending messages
        let mut state = BufferState::new(10, 0.8);
        state
            .slots
            .push_back(create_test_slot(1, MessageState::Pending));
        state
            .slots
            .push_back(create_test_slot(2, MessageState::Pending));
        assert!((state.usage() - 0.2).abs() < f64::EPSILON);

        // Test usage excludes acked
        let mut state = BufferState::new(10, 0.8);
        state
            .slots
            .push_back(create_test_slot(1, MessageState::Pending));
        state
            .slots
            .push_back(create_test_slot(2, MessageState::Acked));
        state
            .slots
            .push_back(create_test_slot(3, MessageState::InFlight));
        assert!((state.usage() - 0.2).abs() < f64::EPSILON);
    }

    #[test]
    fn test_buffer_is_full() {
        // Full when at usage_limit
        let mut state = BufferState::new(10, 0.5);
        for i in 1..=5 {
            state
                .slots
                .push_back(create_test_slot(i, MessageState::Pending));
        }
        assert!(state.is_full());

        // Not full when below usage_limit
        let mut state = BufferState::new(10, 0.5);
        for i in 1..=4 {
            state
                .slots
                .push_back(create_test_slot(i, MessageState::Pending));
        }
        assert!(!state.is_full());
    }

    #[test]
    fn test_pending_and_in_flight_counts() {
        let mut state = BufferState::new(10, 0.8);
        state
            .slots
            .push_back(create_test_slot(1, MessageState::Pending));
        state
            .slots
            .push_back(create_test_slot(2, MessageState::InFlight));
        state
            .slots
            .push_back(create_test_slot(3, MessageState::Pending));
        state
            .slots
            .push_back(create_test_slot(4, MessageState::Acked));
        state
            .slots
            .push_back(create_test_slot(5, MessageState::InFlight));

        assert_eq!(state.pending_count(), 2);
        assert_eq!(state.in_flight_count(), 2);
    }

    #[test]
    fn test_reclaim_acked() {
        // Test reclaiming from front
        let mut state = BufferState::new(10, 0.8);
        let slot1 = create_test_slot(1, MessageState::Acked);
        let slot2 = create_test_slot(2, MessageState::Acked);
        let slot3 = create_test_slot(3, MessageState::Pending);

        state.offset_to_index.insert(Offset::new(1, 0), 0);
        state.offset_to_index.insert(Offset::new(2, 0), 1);
        state.offset_to_index.insert(Offset::new(3, 0), 2);
        state.dedup_window.insert(slot1.id.clone(), 1);
        state.dedup_window.insert(slot2.id.clone(), 2);
        state.dedup_window.insert(slot3.id.clone(), 3);
        state.slots.push_back(slot1);
        state.slots.push_back(slot2);
        state.slots.push_back(slot3);

        state.reclaim_acked();
        assert_eq!(state.slots.len(), 1);
        assert_eq!(state.slots[0].sequence, 3);

        // Test reclaim stops at non-acked
        let mut state = BufferState::new(10, 0.8);
        let slot1 = create_test_slot(1, MessageState::Acked);
        let slot2 = create_test_slot(2, MessageState::Pending);
        let slot3 = create_test_slot(3, MessageState::Acked);

        state.offset_to_index.insert(Offset::new(1, 0), 0);
        state.offset_to_index.insert(Offset::new(2, 0), 1);
        state.offset_to_index.insert(Offset::new(3, 0), 2);
        state.dedup_window.insert(slot1.id.clone(), 1);
        state.dedup_window.insert(slot2.id.clone(), 2);
        state.dedup_window.insert(slot3.id.clone(), 3);
        state.slots.push_back(slot1);
        state.slots.push_back(slot2);
        state.slots.push_back(slot3);

        state.reclaim_acked();
        assert_eq!(state.slots.len(), 2);
        assert_eq!(state.slots[0].sequence, 2);
    }

    #[test]
    fn test_find_slot() {
        let mut state = BufferState::new(10, 0.8);
        let slot = create_test_slot(1, MessageState::Pending);
        state.slots.push_back(slot);
        state.offset_to_index.insert(Offset::new(1, 0), 0);

        // Mutable find
        assert!(state.find_slot_mut(&Offset::new(1, 0)).is_some());
        assert_eq!(state.find_slot_mut(&Offset::new(1, 0)).unwrap().sequence, 1);
        assert!(state.find_slot_mut(&Offset::new(999, 0)).is_none());

        // Immutable find
        assert!(state.find_slot(&Offset::new(1, 0)).is_some());
        assert_eq!(state.find_slot(&Offset::new(1, 0)).unwrap().sequence, 1);
    }

    #[test]
    fn test_message_state_equality() {
        assert_eq!(MessageState::Pending, MessageState::Pending);
        assert_ne!(MessageState::Pending, MessageState::InFlight);
        assert_ne!(MessageState::InFlight, MessageState::Acked);
    }

    // ========== Offset tests ==========

    #[test]
    fn test_offset() {
        use std::collections::HashSet;

        // Basic creation and display
        let offset = Offset::new(42, 3);
        assert_eq!(offset.sequence, 42);
        assert_eq!(offset.partition_idx, 3);
        assert_eq!(format!("{}", offset), "42-3");

        // Equality and clone
        let o1 = Offset::new(1, 0);
        let o2 = Offset::new(1, 0);
        let o3 = Offset::new(2, 0);
        assert_eq!(o1, o2);
        assert_eq!(o1, o1.clone());
        assert_ne!(o1, o3);

        // Ordering
        assert!(Offset::new(1, 0) < Offset::new(2, 0));
        assert!(Offset::new(1, 0) < Offset::new(1, 1));

        // Hash (works in HashSet)
        let mut set = HashSet::new();
        set.insert(Offset::new(1, 0));
        set.insert(Offset::new(1, 0)); // duplicate
        set.insert(Offset::new(2, 0));
        assert_eq!(set.len(), 2);
    }

    // ========== ReadMessage tests ==========

    #[test]
    fn test_read_message() {
        let msg = ReadMessage {
            payload: Bytes::from("hello world"),
            headers: {
                let mut h = HashMap::new();
                h.insert("key".to_string(), "value".to_string());
                h
            },
            offset: Offset::new(42, 0),
        };
        assert_eq!(msg.payload, Bytes::from("hello world"));
        assert_eq!(msg.headers.len(), 1);
        assert_eq!(msg.offset.sequence, 42);
    }
}
