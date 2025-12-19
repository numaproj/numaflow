# Enhanced MonoVertex — Final Technical Implementation Guide

*(Invariant-Aligned, Production-Safe)*

This guide describes **what to build, in what order, and what must be true before advancing**.
Each phase has **explicit exit criteria** so correctness is enforced, not assumed.

---

## **Phase 1 — Nervous System**

### Ownership, Fencing, Flow Control, Metadata

**Goal:** Make the runtime *safe to execute* before making it fast or feature-rich.

### Build

* **Partition ownership manager**

  * Kafka consumer group assignment (primary)
  * Optional stable assignment later
* **Immediate revocation rule**

  * On revoke: stop reads, state writes, and sink writes immediately
* **GenerationID lifecycle**

  * Increment on every ownership session
  * Validate on **every** state mutation and sink write
* **SHM transport with credits**

  * Credit-based producer permission
  * No unbounded writes, no busy waiting
* **Message framing contract**

  * Every batch carries:

    * `epoch_id`
    * `generation_id`
    * `schema_id`
    * `partition`
    * source position range
    * batch_id

### Must Be True Before Advancing

* Revoked pod stops all writes *before* sink attempts (not after errors).
* SHM remains bounded under load with near-zero spin CPU.
* Any write with stale GenerationID is rejected.

---

## **Phase 2 — Root of Truth**

### Commit Records, Persistence, Recovery

**Goal:** Make restarts deterministic and provably correct.

### Build

* **Commit Record (authoritative)**

  * `(generation_id, epoch_id, source positions, sink ref, schema_id, watermark)`
  * Prepared vs committed must be distinguishable
* **RocksDB CF separation**

  * `commit_meta`
  * `state_data`
  * (future) `timers`
* **Atomic persist path**

  * State flush + source positions + commit record advance in one WriteBatch
* **Dual commit record storage**

  * Local RocksDB + remote (S3 / metadata stream)
* **Recovery bootstrap**

  * Load last committed record
  * Restore state
  * Seek source
  * Ignore all uncommitted epochs

### Must Be True Before Advancing

* Crash at any point before commit → restart resumes from last committed epoch.
* Disk loss → remote commit record restores correctly.
* No partial epoch is ever treated as authoritative.

---

## **Phase 3 — Vectorized Core**

### Hot/Warm State, Dirty Tracking, Output Buffering

**Goal:** Achieve throughput without violating transactional boundaries.

### Build

* **Mode-aware state layouts**

  * Agg: compact per-key blobs (struct / protobuf)
  * Join/table: time-bucketed or shard-segmented state (Arrow IPC)
* **Epoch-scoped state deltas**

  * Partial epochs isolated via epoch-tagged keys or delta buffers
* **Dirty Index**

  * Key/segment granularity
  * Incremental checkpointing only
* **Epoch Output Buffer**

  * Outputs tagged with `(epoch_id, generation_id)`
  * Bounded memory + backpressure on overflow
* **Flush triggers**

  * Barrier
  * Time
  * Count
  * Memory pressure

### Must Be True Before Advancing

* Partial epoch state cannot leak across restarts.
* Dirty-only flush keeps checkpoints bounded.
* Sink slowdown backpressures ingestion instead of growing buffers.

---

## **Phase 4 — Ledger Completion**

### Sink Alignment, Visibility, Watermarks

**Goal:** Make the engine “streaming complete” for Kafka, NATS, and S3.

### Build

* **4-phase boundary**

  * Seal → Persist → Prepare → Commit
* **Strict commit ordering**

  * Commit record is marked *committed* **only after** sink visibility succeeds
* **Kafka EOS sink**

  * Partition-scoped transactional IDs
  * GenerationID fencing
  * Abort on revoke
* **JetStream sink**

  * Deterministic Msg-ID `(epoch, seq, partition, generation)`
  * Enforce checkpoint interval < dedupe window
* **S3 sink**

  * Deterministic staging paths
  * Manifest-per-epoch as the only visibility signal
* **Watermarks**

  * Event-time extraction
  * Min-across-partitions
  * Idle detection with safety rule:

    * no buffered work may be skipped

### Must Be True Before Advancing

* Commit record never claims committed unless sink is visible.
* Kafka crash tests show zero duplicates in EOS mode.
* S3 never exposes partial data.
* Windows close correctly under idle partitions.

---

## **Phase 5 — Hardening**

### Backpressure, Resources, Schema Lifecycle, Chaos

**Goal:** Survive real production conditions.

### Build

* **Checkpoint lag monitor + ladder**

  1. Throttle ingestion
  2. Bounded batch tuning (latency caps)
  3. Flush / evict within IO budget
  4. Fail fast if RTO exceeded
* **Memory & IO pressure signals**

  * Dirty flush before eviction
  * Compaction-aware throttling
* **Schema enforcement**

  * Envelope checks everywhere
  * Forward-only evolution
  * Upcaster or explicit failure
* **Fail-fast conditions (explicit)**

  * Schema incompatibility
  * Ownership / generation mismatch
  * Commit record unavailable when required
  * Persistent lag beyond RTO
* **Chaos suite**

  * Crash points across Seal/Persist/Prepare/Commit
  * Rebalance fencing
  * Disk loss recovery
  * JetStream dedupe-window violations

### Must Be True Before Shipping

* No OOM under compaction pressure.
* CI passes failure matrix consistently.
* Schema upgrades are deterministic (upcast or stop).

---

## **The Five Things to Guard Relentlessly**

1. Immediate revocation + fencing
2. Commit record is the only truth
3. Commit record never lies about visibility
4. Buffers are bounded and lag drives backpressure
5. Watermarks always make forward progress
