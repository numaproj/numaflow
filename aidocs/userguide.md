# Enhanced MonoVertex — Consumer Guide

## Safe Processing Models, Defaults, and Pitfalls

Enhanced MonoVertex is a **ledger-based streaming runtime**. Each fused pod is authoritative for the data it owns and commits results atomically using epochs.

This guide shows:

* **Which processing models are safe**
* **What problems each model solves**
* **What defaults to start with**
* **What mistakes to avoid**

If you stay within these models, you get **fast recovery, predictable correctness, and no global coordination overhead**.

---

## Model 1 — Stateless Transform (Map / Filter / Project)

### What it is

Pure transformations with **no retained state**.

### Typical diagram (conceptual)

```
Source → Map/Filter → Sink
```

### Solves

* Parsing & normalization
* Routing / filtering
* Lightweight enrichment from payload

### Safe because

* Replay only re-executes pure logic
* Sink visibility is epoch-gated

### Recommended defaults

* Batch size: 500–2,000 events
* Epoch interval: 100–500 ms
* Sink mode: Kafka EOS or idempotent writes

### Common pitfalls

* ❌ Adding hidden state (static globals, caches)
* ❌ Non-deterministic logic (randoms, wall-clock time)

---

## Model 2 — Keyed Aggregation (Rolling or Unwindowed)

### What it is

Per-key state updated continuously.

### Diagram

```
Source → keyBy → Agg State → Sink
```

### Solves

* Counters, sums, min/max
* Rate limiting
* Rolling features per entity

### State formats

* **Struct Schema** → hot, high-frequency metrics
* **Protobuf** → richer aggregates

### Recommended defaults

* Checkpoint interval: 500 ms – 2 s
* Dirty-key flush only
* State TTL (if applicable): explicit

### Common pitfalls

* ❌ Forgetting TTL (state grows forever)
* ❌ Using per-event RocksDB writes (kills throughput)

---

## Model 3 — Windowed Aggregation (Event-Time)

### What it is

Keyed aggregation scoped to event-time windows.

### Diagram

```
Source → keyBy → Window → Agg → Sink
```

### Solves

* Metrics dashboards
* Time-bucketed analytics
* Sessionization

### Required policies

* Allowed lateness
* Late event handling: **drop or side-output**

### Recommended defaults

* Allowed lateness: 1–5 minutes
* Idle partition timeout: 5–10 seconds
* No deep retractions (start simple)

### Common pitfalls

* ❌ No idle detection → stuck watermarks
* ❌ Expecting historical rewrites without retractions

---

## Model 4 — Storage-First Materialized View Join

### (Safe Entity Enrichment)

### What it is

Multiple streams build a **wide entity row** via versioned partial updates.

### Diagram

```
Stream A ┐
Stream B ├→ Versioned Upserts → Materialized View
Stream C ┘
```

### Solves

* User/device/entity profiles
* Feature store pipelines
* Dim/fact enrichment

### Safety rules (mandatory)

* Writes are **versioned** (event_time / epoch)
* Reads are **AS-OF watermark**
* Explicit completeness policy
* TTL + tombstones supported

### Recommended defaults

* Versioning: event_time + tie-breaker
* Emit only when required fields present
* TTL: domain-specific (hours/days)

### Common pitfalls

* ❌ “Latest write wins” without versioning
* ❌ Treating this like a relational join
* ❌ No delete semantics

---

## Model 5 — Windowed Event Correlation Join

### (Assembler Pattern)

### What it is

A dedicated stage assembles events across streams within a window.

### Diagram

```
Stream A → Normalize ┐
Stream B → Normalize ├→ Join Assembler → Sink
Stream C → Normalize ┘
```

### Solves

* Fraud detection
* Multi-signal correlation
* Event matching (“A and B within 10 min”)

### Why it’s safe

* Join assembler is itself ledgered
* Window close driven by watermarks
* Replay is deterministic

### Recommended defaults

* Window size: explicit & bounded
* Allowed lateness: minimal
* Output IDs deterministic

### Common pitfalls

* ❌ Trying to force this into Model 4
* ❌ Unbounded buffering without window limits

---

## Model 6 — Hierarchical Aggregation (Global Invariants)

### What it is

Local summaries → global reducer.

### Diagram

```
Shard 1 → Local Top-K ┐
Shard 2 → Local Top-K ├→ Global Reducer → Sink
Shard N → Local Top-K ┘
```

### Solves

* Global Top-K
* Heavy hitters
* Approximate global stats

### Why it’s safe

* Heavy work is sharded
* Reducer only sees committed summaries
* No global snapshot required

### Recommended defaults

* K small (10–1,000)
* Epoch-aligned emissions
* Reducer keyed by invariant

### Common pitfalls

* ❌ Sending raw events to reducer
* ❌ No epoch tagging → stale merges

---

## Model 7 — Idempotent Output Ledger

### (Effectively-Once Sinks)

### What it is

Make retries safe by construction.

### Solves

* S3 data lakes
* JetStream outputs
* Any non-transactional sink

### Required techniques

* Deterministic output IDs
* Overwrite-or-ignore semantics
* Manifest or dedupe gate

### Recommended defaults

* S3: `epoch=N/part=M` + success manifest
* JetStream: Msg-ID = `(epoch, key, seq)`
* Retention > worst-case recovery time

### Common pitfalls

* ❌ Relying on “exactly once” claims from object stores
* ❌ Non-deterministic filenames or IDs

---

## Quick Decision Guide

| You need…             | Use model |
| --------------------- | --------- |
| Pure transforms       | Model 1   |
| Per-key metrics       | Model 2   |
| Time windows          | Model 3   |
| Entity enrichment     | Model 4   |
| Event correlation     | Model 5   |
| Global Top-K          | Model 6   |
| S3 / JetStream output | Model 7   |

---

## What Enhanced MonoVertex Is **Not** Optimized For

Avoid designing workloads that require:

* One global snapshot across a huge DAG
* Unlimited deep retractions
* EOS across arbitrary legacy databases

Those require **global coordination** and are better suited to Flink-style engines.

---

## Final Rule for Users

> **If your problem can be expressed as committed facts + deterministic merges, Enhanced MonoVertex will be faster, simpler, and easier to operate than globally coordinated streaming engines.**
