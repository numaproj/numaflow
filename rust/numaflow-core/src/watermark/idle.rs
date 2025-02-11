//! Idling happens during following scenarios
//!
//! > Read == 0
//!
//! If no messages are being read from the source or ISB, we should be able to detect
//! and publish the idle watermark.
//!
//! > Branch Idling
//!
//! With conditional forwarding, some streams(branches) will get less frequent writes
//! than others. In such cases, we should be able to detect and publish the idle
//! watermark to those branches.

/// Idle detection and handling for ISB.
pub(super) mod isb;

/// Idle detection and handling for Source.
pub(super) mod source;
