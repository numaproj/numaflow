//! Reduce watermark emulation.
//!
//! Reduce vertices close windows on watermark progression, so a local reduce run needs *something*
//! to play the upstream vertex's watermark publisher. This driver does exactly that: it publishes
//! a floor watermark at startup, tracks the high-watermark as events are written, and — at drain —
//! publishes a terminal far-future idle watermark that makes the real windower close every open
//! window through the production close path.
//!
//! Only used for reduce kinds. Mirrors the reduce-forwarder template test's watermark handling.

use std::collections::HashMap;
use std::sync::Arc;

use crate::config::pipeline::isb::Stream;
use crate::local::config_builder::{
    INPUT_OT_BUCKET, INPUT_STREAM, INPUT_VERTEX, input_bucket_config,
};
use crate::local::events::InputEvent;
// `create_kv_store` is an `ISBFactory` trait method, so the trait must be in scope to call it.
use crate::pipeline::isb::factory::ISBFactory;
use crate::pipeline::isb::inmemory::InMemoryFactory;
use crate::watermark::isb::wm_publisher::ISBWatermarkPublisher;

/// 9999-12-31T23:59:59Z in Unix milliseconds; advances the watermark past every window (matches
/// the template test's `TERMINAL_WATERMARK_MS`).
const TERMINAL_WATERMARK_MS: i64 = 253_402_300_799_000;

/// Emulates the upstream vertex's watermark publisher for a local reduce run.
pub(crate) struct WatermarkDriver {
    publisher: ISBWatermarkPublisher,
    input_stream: Stream,
    /// Monotonic high-watermark in Unix millis. Watermarks must be monotonic, so we publish this
    /// (not raw per-event values) even when input arrives out of order.
    high_watermark_ms: i64,
    /// Highest offset written so far; the terminal watermark is published at `last_offset + 1`.
    last_offset: i64,
}

/// Initial floor watermark (Unix millis). It must sit *below* every realistic event time so no
/// real data is flagged late before the reducer sees it — the facade does not know event times at
/// construction, so we cannot floor at "earliest event - 1". `1970-01-01T00:00:00Z` covers every
/// event time a test tool will use (event times are wall-clock timestamps).
const FLOOR_WATERMARK_MS: i64 = 0;

impl WatermarkDriver {
    /// Build the driver: create the input OT KV store, construct the publisher, and publish the
    /// initial floor watermark so the fetcher has a lower bound before any data arrives.
    pub(crate) async fn new(factory: &Arc<InMemoryFactory>) -> crate::Result<Self> {
        let ot_store = factory.create_kv_store(INPUT_OT_BUCKET.to_string()).await?;
        let input_bucket = input_bucket_config();
        let input_stream = Stream::new(INPUT_STREAM, INPUT_VERTEX, 0);

        let mut publisher = ISBWatermarkPublisher::new(
            // Processor name — unique synthetic upstream processor for this run.
            "nfcli-in-0".to_string(),
            HashMap::from([(INPUT_VERTEX, ot_store)]),
            std::slice::from_ref(&input_bucket),
            false,
        );

        // Floor watermark at offset 0, not idle: a lower bound before any data arrives.
        publisher
            .publish_watermark(&input_stream, 0, FLOOR_WATERMARK_MS, false)
            .await;

        Ok(Self {
            publisher,
            input_stream,
            high_watermark_ms: FLOOR_WATERMARK_MS,
            last_offset: 0,
        })
    }

    /// Publish the watermark for a written event. The watermark value is the event's explicit
    /// `watermark` if set, else its `event_time`; the published value is the running max so it
    /// never regresses. Setting an early event's `watermark` far ahead lets later older-`event_time`
    /// messages be flagged late by the real windower.
    pub(crate) async fn on_written(&mut self, offset: i64, event: &InputEvent) {
        let wm = event
            .watermark
            .unwrap_or(event.event_time)
            .timestamp_millis();
        self.high_watermark_ms = self.high_watermark_ms.max(wm);
        self.last_offset = self.last_offset.max(offset);
        self.publisher
            .publish_watermark(&self.input_stream, offset, self.high_watermark_ms, false)
            .await;
    }

    /// Publish the terminal idle watermark (far future) at `last_offset + 1`. Called repeatedly in
    /// the drain loop: a single publish can be missed while the synthetic processor registers, and
    /// re-publishing keeps that processor alive while the forwarder observes and processes the idle
    /// watermark (G9).
    pub(crate) async fn publish_terminal(&mut self) {
        let terminal_offset = self.last_offset + 1;
        self.publisher
            .publish_watermark(
                &self.input_stream,
                terminal_offset,
                TERMINAL_WATERMARK_MS,
                true,
            )
            .await;
    }
}
