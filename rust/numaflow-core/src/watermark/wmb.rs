use bytes::{Bytes, BytesMut};
use chrono::{DateTime, Utc};
use prost::Message;

use crate::error::Error;

/// WMB is the watermark message that is sent by the processor to the downstream.
#[derive(Clone, Copy, Debug, PartialEq)]
#[allow(clippy::upper_case_acronyms)]
pub(crate) struct WMB {
    pub(crate) idle: bool,
    pub(crate) offset: i64,
    pub(crate) watermark: i64,
    pub(crate) partition: u16,
}

impl Default for WMB {
    fn default() -> Self {
        Self {
            watermark: -1,
            offset: -1,
            idle: false,
            partition: 0,
        }
    }
}

/// Watermark is a monotonically increasing time.
pub(crate) type Watermark = DateTime<Utc>;

/// Converts a protobuf bytes to WMB.
impl TryFrom<Bytes> for WMB {
    type Error = Error;

    fn try_from(bytes: Bytes) -> Result<Self, Self::Error> {
        let proto_wmb = numaflow_pb::objects::watermark::Wmb::decode(bytes)
            .map_err(|e| Error::Proto(e.to_string()))?;

        Ok(WMB {
            idle: proto_wmb.idle,
            offset: proto_wmb.offset,
            watermark: proto_wmb.watermark,
            partition: proto_wmb.partition as u16,
        })
    }
}

/// Converts WMB to protobuf bytes.
impl TryFrom<WMB> for BytesMut {
    type Error = Error;

    fn try_from(wmb: WMB) -> Result<Self, Self::Error> {
        let mut bytes = BytesMut::new();
        let proto_wmb = numaflow_pb::objects::watermark::Wmb {
            idle: wmb.idle,
            offset: wmb.offset,
            watermark: wmb.watermark,
            partition: wmb.partition as i32,
        };

        proto_wmb
            .encode(&mut bytes)
            .map_err(|e| Error::Proto(e.to_string()))?;

        Ok(bytes)
    }
}

/// WMBChecker checks if the idle watermark is valid. It checks by making sure the Idle WMB's offset has not been
/// changed in X iterations. This check is required because we have to make sure the time at which the idleness has been
/// detected matches with reality (processes could hang in between). The only way to do that is by multiple iterations.
#[derive(Debug, Clone)]
pub struct WMBChecker {
    iteration_counter: usize,
    iterations: usize,
    wmb: Option<WMB>,
}

impl WMBChecker {
    /// Returns a WMBChecker to check if the wmb is idle.
    /// If all the iterations get the same wmb offset, the wmb is considered as valid
    /// and will be used to publish a wmb to the toBuffer partitions of the next vertex.
    pub fn new(num_of_iteration: usize) -> Self {
        Self {
            iteration_counter: 0,
            iterations: num_of_iteration,
            wmb: None,
        }
    }

    /// Checks if the head wmb is idle, and it has the same wmb offset from the previous iteration.
    /// If all the iterations get the same wmb offset, returns true.
    pub fn validate_head_wmb(&mut self, wmb: WMB) -> bool {
        if !wmb.idle {
            // if wmb is not idle, skip and reset the iteration_counter
            self.iteration_counter = 0;
            return false;
        }

        // check the iteration_counter value
        if self.iteration_counter == 0 {
            self.iteration_counter += 1;
            // the wmb only writes once when iteration_counter is zero
            self.wmb = Some(wmb);
        } else if self.iteration_counter < self.iterations - 1 {
            self.iteration_counter += 1;
            if let Some(ref stored_wmb) = self.wmb {
                if stored_wmb.offset == wmb.offset {
                    // we get the same wmb, meaning the wmb is valid, continue
                } else {
                    // else, start over
                    self.iteration_counter = 0;
                    self.wmb = None;
                }
            }
        } else if self.iteration_counter >= self.iterations - 1 {
            self.iteration_counter = 0;
            if let Some(ref stored_wmb) = self.wmb
                && stored_wmb.offset == wmb.offset
            {
                // reach max iteration, if still get the same wmb,
                // then the wmb is considered as valid, return true
                self.wmb = None; // Reset for next validation cycle
                return true;
            }
            self.wmb = None; // Reset for next validation cycle
        }
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::watermark::wmb::WMBChecker;

    #[test]
    fn test_wmb_checker_new() {
        let checker = WMBChecker::new(3);
        assert_eq!(checker.iteration_counter, 0);
        assert_eq!(checker.iterations, 3);
        assert!(checker.wmb.is_none());
    }

    #[test]
    fn test_validate_head_wmb_not_idle() {
        let mut checker = WMBChecker::new(3);
        let wmb = WMB {
            watermark: 100,
            offset: 1,
            idle: false,
            partition: 0,
        };

        let result = checker.validate_head_wmb(wmb);
        assert!(!result);
        assert_eq!(checker.iteration_counter, 0);
    }

    #[test]
    fn test_validate_head_wmb_same_offset_success() {
        let mut checker = WMBChecker::new(3);
        let wmb = WMB {
            watermark: 100,
            offset: 1,
            idle: true,
            partition: 0,
        };

        // First iteration
        let result = checker.validate_head_wmb(wmb);
        assert!(!result);
        assert_eq!(checker.iteration_counter, 1);

        // Second iteration with same offset
        let result = checker.validate_head_wmb(wmb);
        assert!(!result);
        assert_eq!(checker.iteration_counter, 2);

        // Third iteration with same offset - should return true
        let result = checker.validate_head_wmb(wmb);
        assert!(result);
        assert_eq!(checker.iteration_counter, 0); // Reset after validation
    }

    #[test]
    fn test_validate_head_wmb_different_offset_reset() {
        let mut checker = WMBChecker::new(3);
        let wmb1 = WMB {
            watermark: 100,
            offset: 1,
            idle: true,
            partition: 0,
        };
        let wmb2 = WMB {
            watermark: 100,
            offset: 2, // Different offset
            idle: true,
            partition: 0,
        };

        // First iteration
        let result = checker.validate_head_wmb(wmb1);
        assert!(!result);
        assert_eq!(checker.iteration_counter, 1);

        // Second iteration with different offset - should reset
        let result = checker.validate_head_wmb(wmb2);
        assert!(!result);
        assert_eq!(checker.iteration_counter, 0); // Reset due to different offset
    }

    #[test]
    fn test_validate_head_wmb_non_idle_resets_counter() {
        let mut checker = WMBChecker::new(3);
        let idle_wmb = WMB {
            watermark: 100,
            offset: 1,
            idle: true,
            partition: 0,
        };
        let non_idle_wmb = WMB {
            watermark: 100,
            offset: 1,
            idle: false,
            partition: 0,
        };

        // First iteration with idle WMB
        let result = checker.validate_head_wmb(idle_wmb);
        assert!(!result);
        assert_eq!(checker.iteration_counter, 1);

        // Second iteration with non-idle WMB - should reset
        let result = checker.validate_head_wmb(non_idle_wmb);
        assert!(!result);
        assert_eq!(checker.iteration_counter, 0); // Reset due to non-idle
    }
}
