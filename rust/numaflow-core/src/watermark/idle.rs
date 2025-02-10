//! Idling happens during following scenarios
//!
//! > Read == 0
//!
//! TODO
//!
//! > Branch Idling
//!
//! TODO

/// Idle detection and handling for ISB.
pub(super) mod isb;

/// Idle detection and handling for Source.
pub(super) mod source;
