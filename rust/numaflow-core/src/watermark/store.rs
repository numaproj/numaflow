//! Watermark store implementations for pluggable storage backends.
//!
//! The [`WatermarkStore`](super::WatermarkStore) trait is defined in the parent module.
//! This module contains concrete implementations.

#![allow(dead_code)] // FIXME: remove after integration
pub(crate) mod jetstream;
