use std::cmp::Ordering;
use std::collections::VecDeque;
use std::fmt;
use std::sync::Arc;

use tokio::sync::RwLock;
use tracing::{error, warn};

use crate::watermark::wmb::WMB;

/// OffsetTimeline is to store the event time to the offset records.
/// Our list is sorted by event time from highest to lowest.
#[derive(Clone)]
pub struct OffsetTimeline {
    watermarks: Arc<RwLock<VecDeque<WMB>>>,
    capacity: usize,
}

impl OffsetTimeline {
    pub(crate) fn new(capacity: usize) -> Self {
        let mut watermarks = VecDeque::with_capacity(capacity);
        for _ in 0..capacity {
            watermarks.push_back(WMB::default());
        }

        OffsetTimeline {
            watermarks: Arc::new(RwLock::new(watermarks)),
            capacity,
        }
    }

    /// Put inserts the WMB into list. It ensures that the list will remain sorted after the insert.
    pub(crate) async fn put(&self, node: WMB) {
        let mut watermarks = self.watermarks.write().await;

        let element_node = watermarks
            .front_mut()
            .expect("timeline should never be empty");

        // Different cases:
        // 1. Watermark is the same but the offset is larger - we should store the larger offset
        // 2. Watermark is the same but the offset is smaller - we should skip
        // 3. Watermark is larger and the offset is larger - we should store the larger offset and the watermark
        // 4. Watermark is larger but the offset is smaller - should not happen (offset should be increasing)
        // 5. Watermark is smaller - should not happen (watermark should be increasing)

        match (
            node.watermark.cmp(&element_node.watermark),
            node.offset.cmp(&element_node.offset),
        ) {
            (Ordering::Equal, Ordering::Greater) => {
                element_node.offset = node.offset;
            }
            (Ordering::Equal, _) => {
                warn!("Watermark the same but input offset smaller than the existing offset - skipping");
            }
            (Ordering::Greater, Ordering::Greater) => {
                watermarks.push_front(node);
                if watermarks.len() > self.capacity {
                    watermarks.pop_back();
                }
            }
            (Ordering::Greater, _) => {
                error!("The new input offset should never be smaller than the existing offset");
            }
            (Ordering::Less, _) => {}
        }
        if watermarks.len() > self.capacity {
            watermarks.pop_back();
        }
    }

    /// GetHeadOffset returns the offset of the head WMB.
    pub(crate) async fn get_head_offset(&self) -> i64 {
        let watermarks = self.watermarks.read().await;
        watermarks.front().map_or(-1, |w| w.offset)
    }

    /// GetHeadWatermark returns the watermark of the head WMB.
    pub(crate) async fn get_head_watermark(&self) -> i64 {
        let watermarks = self.watermarks.read().await;
        watermarks.front().map_or(-1, |w| w.watermark)
    }

    /// GetHeadWMB returns the head WMB.
    pub(crate) async fn get_head_wmb(&self) -> Option<WMB> {
        let watermarks = self.watermarks.read().await;
        watermarks.front().copied()
    }

    /// GetEventTime returns the event time of the nearest WMB that has an offset less than the input offset.
    pub(crate) async fn get_event_time(&self, input_offset: i64) -> i64 {
        let watermarks = self.watermarks.read().await;
        for w in watermarks.iter() {
            if w.offset < input_offset {
                return w.watermark;
            }
        }
        -1
    }
}

impl fmt::Debug for OffsetTimeline {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "OffsetTimeline {{ capacity: {}, watermarks: {:?} }}",
            self.capacity, self.watermarks
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::watermark::wmb::WMB;

    #[tokio::test]
    async fn test_put_offsets_in_order() {
        let timeline = OffsetTimeline::new(10);
        let wmb1 = WMB {
            watermark: 100,
            offset: 1,
            idle: false,
            partition: 0,
        };
        let wmb2 = WMB {
            watermark: 200,
            offset: 2,
            idle: false,
            partition: 0,
        };
        let wmb3 = WMB {
            watermark: 250,
            offset: 3,
            idle: false,
            partition: 0,
        };
        let wmb4 = WMB {
            watermark: 250,
            offset: 4,
            idle: false,
            partition: 0,
        };
        let wmb5 = WMB {
            watermark: 300,
            offset: 5,
            idle: false,
            partition: 0,
        };

        timeline.put(wmb1).await;
        timeline.put(wmb2).await;
        timeline.put(wmb3).await;
        timeline.put(wmb4).await;
        timeline.put(wmb5).await;

        let head_offset = timeline.get_head_offset().await;
        assert_eq!(head_offset, 5);

        let head_watermark = timeline.get_head_watermark().await;
        assert_eq!(head_watermark, 300);

        let head_wmb = timeline.get_head_wmb().await;
        assert!(head_wmb.is_some());
        assert_eq!(head_wmb.unwrap().watermark, 300);

        let event_time = timeline.get_event_time(3).await;
        assert_eq!(event_time, 200);
    }

    #[tokio::test]
    async fn test_put_out_of_order_offsets() {
        let timeline = OffsetTimeline::new(10);
        let wmb1 = WMB {
            watermark: 50,
            offset: 62,
            idle: false,
            partition: 0,
        };
        let wmb2 = WMB {
            watermark: 100,
            offset: 65,
            idle: false,
            partition: 0,
        };
        let wmb3 = WMB {
            watermark: 200,
            offset: 63, // out of order should not be considered
            idle: false,
            partition: 0,
        };
        let wmb4 = WMB {
            watermark: 250,
            offset: 70,
            idle: false,
            partition: 0,
        };
        let wmb5 = WMB {
            watermark: 250,
            offset: 80,
            idle: false,
            partition: 0,
        };
        let wmb6 = WMB {
            watermark: 300,
            offset: 86,
            idle: false,
            partition: 0,
        };

        timeline.put(wmb1).await;
        timeline.put(wmb2).await;
        timeline.put(wmb3).await;
        timeline.put(wmb4).await;
        timeline.put(wmb5).await;
        timeline.put(wmb6).await;

        let head_offset = timeline.get_head_offset().await;
        assert_eq!(head_offset, 86);

        let head_watermark = timeline.get_head_watermark().await;
        assert_eq!(head_watermark, 300);

        let head_wmb = timeline.get_head_wmb().await;
        assert!(head_wmb.is_some());
        assert_eq!(head_wmb.unwrap().watermark, 300);

        let event_time = timeline.get_event_time(65).await;
        assert_eq!(event_time, 50);
    }

    #[tokio::test]
    async fn test_put_same_watermark_different_offset() {
        let timeline = OffsetTimeline::new(10);
        let wmb1 = WMB {
            watermark: 100,
            offset: 1,
            idle: false,
            partition: 0,
        };
        let wmb2 = WMB {
            watermark: 100,
            offset: 2,
            idle: false,
            partition: 0,
        };
        let wmb3 = WMB {
            watermark: 100,
            offset: 3,
            idle: false,
            partition: 0,
        };
        let wmb4 = WMB {
            watermark: 100,
            offset: 4,
            idle: false,
            partition: 0,
        };
        let wmb5 = WMB {
            watermark: 100,
            offset: 5,
            idle: false,
            partition: 0,
        };

        timeline.put(wmb1).await;
        timeline.put(wmb2).await;
        timeline.put(wmb3).await;
        timeline.put(wmb4).await;
        timeline.put(wmb5).await;

        // should only consider the largest offset
        let head_offset = timeline.get_head_offset().await;
        assert_eq!(head_offset, 5);

        let head_watermark = timeline.get_head_watermark().await;
        assert_eq!(head_watermark, 100);

        let head_wmb = timeline.get_head_wmb().await;
        assert!(head_wmb.is_some());
        assert_eq!(head_wmb.unwrap().watermark, 100);

        // only one entry, so should return -1
        let event_time = timeline.get_event_time(5).await;
        assert_eq!(event_time, -1);
    }

    #[tokio::test]
    async fn test_put_idle_cases() {
        let timeline = OffsetTimeline::new(10);
        let wmb1 = WMB {
            watermark: 100,
            offset: 1,
            idle: false,
            partition: 0,
        };
        let wmb2 = WMB {
            watermark: 200,
            offset: 2,
            idle: false,
            partition: 1,
        };
        let wmb3 = WMB {
            watermark: 150,
            offset: 3,
            idle: true,
            partition: 0,
        };

        timeline.put(wmb1).await;
        timeline.put(wmb2).await;
        timeline.put(wmb3).await; // should be ignored since the watermark is smaller than the head

        let head_offset = timeline.get_head_offset().await;
        assert_eq!(head_offset, 2);

        let head_watermark = timeline.get_head_watermark().await;
        assert_eq!(head_watermark, 200);

        // valid idle watermark
        let idle_wmb = WMB {
            watermark: 250,
            offset: 4,
            idle: true,
            partition: 0,
        };

        timeline.put(idle_wmb).await;
        let head_offset = timeline.get_head_offset().await;
        assert_eq!(head_offset, 4);

        let head_watermark = timeline.get_head_watermark().await;
        assert_eq!(head_watermark, 250);

        // same watermark but different offset (larger should be stored)
        let idle_wmb = WMB {
            watermark: 250,
            offset: 5,
            idle: true,
            partition: 0,
        };

        timeline.put(idle_wmb).await;
        let head_offset = timeline.get_head_offset().await;
        assert_eq!(head_offset, 5);

        let head_watermark = timeline.get_head_watermark().await;
        assert_eq!(head_watermark, 250);
    }
}
