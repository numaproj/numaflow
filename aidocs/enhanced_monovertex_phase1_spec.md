# Enhanced MonoVertex Phase 1 Specification: Nervous System Hardening

**Version:** 1.0
**Date:** 2025-12-18
**Status:** Implemented

## 1. Overview
The "Nervous System" phase focused on hardening the ownership and transport layers of the Enhanced MonoVertex. The primary goal was to prevent "zombie" processes (revoked vertices) from corrupting data or state by enforcing strict identity verification and immediate revocation.

## 2. Gap Analysis & Resolution

| Critical Gap | Risk | Resolution |
| :--- | :--- | :--- |
| **Loose SHM Protocol** | Receiver could read partial/corrupt data; no epoch validation. | **SHM header enforcement** with Magic bytes, Version, and GenerationID. |
| **Identity Amnesia** | Components (Source/Map) unaware of their unique `generation_id`. | **Identity Injection** via config and propagation in `Message` metadata. |
| **Graceful-Only Shutdown** | Revoked vertices finished processing bounds, risking split-brain. | **Immediate Fencing** via `Tracker` kill-switch and Sink-side enforcement. |

## 3. Architecture Changes

### 3.1 SHM Protocol Hardening
A strict wire format was introduced for the Shared Memory ring buffer to ensure data integrity and ownership validation.

**New Header Layout (Rust representation):**
```rust
#[repr(C)]
pub struct ShmHeader {
    pub magic: u16,         // 0x5050 (ASCII 'PP')
    pub version: u16,       // Protocol version (currently 1)
    pub length: u32,        // Payload length
    pub epoch_id: u64,      // Monotonically increasing epoch (scope: generation)
    pub generation_id: u64, // Unique vertex lifespan ID
    pub partition_id: u32,  // Pagination/Partition ID (Future proof)
    pub flags: u32,         // Status/Validity flags (Bit 0: Published)
}
```

**Behavior:**
- **Write Protocol (Two-Phase)**:
    1.  Write Header with `flags = 0` (Invalid).
    2.  Write Payload.
    3.  Memory Barrier (Release).
    4.  Update Header `flags = 1` (Published).
    5.  Advance Ring Buffer Head.
- **Read Protocol**:
    1.  Read Header.
    2.  Check `magic`, `version`, and `flags == 1`.
    3.  Validate `generation_id` matches session.
    4.  *Action on GenID Mismatch*: **Stop Consuming**, **Stop Producing**, **Trigger Tracker Fence**, and **Surface Terminal Error**.

### 3.2 Identity Injection
The concept of `generation_id` (a monotonic integer representing the lifespan of a vertex assignment) was threaded through the entire stack.

**Component Updates:**
- **PipelineConfig**: Parses `NUMAFLOW_GENERATION_ID` env var.
- **Source**: Accepts `generation_id` in constructor; stamps all read `Message`s with it.
- **Mapper**: Propagates the vertex's `generation_id` to all output messages, replacing the default `0`.
- **Message Struct**: Added `generation_id` field to internal metadata.

### 3.3 Revocation Signal (Fencing)
A mechanism for immediate, non-negotiable termination of processing was added to handle revocation events (e.g., K8s pod deletion, partition revocation/rebalance, extensive lag, reassignment).

**Mechanism:**
1.  **Tracker**: Central authority for message lifecycle. Added `is_fenced` (AtomicBool) and `cln_token` (CancellationToken).
2.  **Fence Trigger**: `tracker.fence()` sets `is_fenced = true` and cancels the global token.
3.  **Enforcement Points**:
    *   **Source**: Checks `is_fenced()` before reading next batch. if true, breaks loop immediately.
    *   **SinkWriter**: Checks `is_fenced()` before writing to downstream/external sink. If true, **drops** the batch, marks messages as failed (Nak), and prevents external side-effects. **Critical:** Nak must ensure upstream Source does *not* commit the offset. If this guarantee cannot be met, the process MUST Fail Fast (exit).

## 4. Component Interactions

```mermaid
sequenceDiagram
    participant Orchestrator
    participant Tracker
    participant Source
    participant Mapper
    participant SinkWriter

    Note over Orchestrator: Decision to Revoke
    Orchestrator->>Tracker: fence()
    Tracker->>Tracker: set is_fenced = true
    Tracker->>Source: cancel token / signals
    
    Source->>Tracker: is_fenced?
    Tracker-->>Source: true
    Source->>Source: Stop Reading
    
    Mapper->>SinkWriter: Send Processed Messages
    SinkWriter->>Tracker: is_fenced?
    Tracker-->>SinkWriter: true
    SinkWriter->>SinkWriter: DROP Batch (send Nak)
    Note over SinkWriter: External Write Prevented
```

## 5. Verification
The following tests verify the correctness of these changes:

*   **`monovertex::forwarder::tests::test_.*`**: Verifies `generation_id` is propagated correctly from Source to Sink.
*   **`sinker::sink::tests::test_streaming_write_fenced`**: Verifies that when `tracker.fence()` is called, the `SinkWriter` actively drops in-flight messages and does not invoke the underlying Sink Actor.

## 6. Future Work (Phase 2 & 3)
- **Persist GenID**: Include `generation_id` in the Commit Record (RocksDB) to fence off zombie writers at the persistence layer.
- **Remote checks**: Validate `generation_id` against a remote lease/lock (e.g. in controlling-plane) for split-brain protection.
