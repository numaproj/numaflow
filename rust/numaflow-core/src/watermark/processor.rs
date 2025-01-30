//! Processor is an entity which can emit watermarks, it could be a pod or a partition. Each processor
//! will have a per-partition offset timeline to track the offsets and watermarks. At Vn vertex we will
//! have to track all the Vn-1 active processors to determine the watermark for the read offset. For
//! source the processor will be partition since watermark originates at source. Looking at all the
//! active processors and their timelines fetcher will determine the watermark for the read offset.

/// manager for managing the processors (pod or partition).
pub(super) mod manager;

/// offset timeline for tracking processor offsets and watermarks.
pub(super) mod timeline;
