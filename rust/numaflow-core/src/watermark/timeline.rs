use crate::watermark::WMB;
use std::cmp::Ordering;
use std::collections::VecDeque;
use std::fmt;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, error, info, warn};

#[derive(Clone)]
pub struct OffsetTimeline {
    watermarks: Arc<RwLock<VecDeque<WMB>>>,
    capacity: usize,
}

impl OffsetTimeline {
    pub(crate) fn new(capacity: usize) -> Self {
        let mut watermarks = VecDeque::with_capacity(capacity);
        for _ in 0..capacity {
            watermarks.push_back(WMB {
                watermark: -1,
                offset: -1,
                idle: false,
                partition: 0,
            });
        }
        OffsetTimeline {
            watermarks: Arc::new(RwLock::new(watermarks)),
            capacity,
        }
    }

    pub(crate) async fn put(&self, node: WMB) {
        let mut watermarks = self.watermarks.write().await;
        for i in 0..watermarks.len() {
            let element_node = watermarks[i];
            match node.watermark.cmp(&element_node.watermark) {
                Ordering::Equal => {
                    if node.offset > element_node.offset {
                        watermarks[i] = node;
                    } else {
                        debug!("Watermark the same but input offset smaller than the existing offset - skipping");
                    }
                    return;
                }
                Ordering::Greater => {
                    if node.offset < element_node.offset {
                        error!(
                            "The new input offset should never be smaller than the existing offset"
                        );
                        return;
                    }
                    info!("Inserting watermark {:?} at index {}", node, i);
                    watermarks.insert(i, node);
                    if watermarks.len() > self.capacity {
                        watermarks.pop_back();
                    }
                    return;
                }
                Ordering::Less => continue,
            }
        }
    }

    pub(crate) async fn put_idle(&self, node: WMB) {
        let mut watermarks = self.watermarks.write().await;
        if let Some(front) = watermarks.front() {
            if front.idle {
                if node.watermark < front.watermark {
                    return;
                }
                match node.watermark.cmp(&front.watermark) {
                    Ordering::Equal => {
                        if node.offset > front.offset {
                            watermarks.pop_front();
                            watermarks.push_front(node);
                        }
                        return;
                    }
                    Ordering::Greater => {
                        watermarks.pop_front();
                        watermarks.push_front(node);
                        return;
                    }
                    Ordering::Less => {
                        warn!("Idle watermark is smaller than the existing watermark - skipping");
                        return;
                    }
                }
            }
            if node.watermark > front.watermark && node.offset > front.offset {
                watermarks.push_front(node);
                if watermarks.len() > self.capacity {
                    watermarks.pop_back();
                }
            }
        }
    }

    pub(crate) async fn get_head_offset(&self) -> i64 {
        let watermarks = self.watermarks.read().await;
        watermarks.front().map_or(-1, |w| w.offset)
    }

    pub(crate) async fn get_head_watermark(&self) -> i64 {
        let watermarks = self.watermarks.read().await;
        watermarks.front().map_or(-1, |w| w.watermark)
    }

    pub(crate) async fn get_head_wmb(&self) -> Option<WMB> {
        let watermarks = self.watermarks.read().await;
        watermarks.front().copied()
    }

    pub(crate) async fn get_offset(&self, event_time: i64) -> i64 {
        let watermarks = self.watermarks.read().await;
        for w in watermarks.iter() {
            if event_time >= w.watermark {
                return w.offset;
            }
        }
        -1
    }

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
