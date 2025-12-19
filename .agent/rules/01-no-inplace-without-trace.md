---
trigger: always_on
---

# No In-Place Fixes Without End-to-End Trace (Always On)

## Rule
If a requested change affects correctness or performance-critical paths (SHM, batching, epoch manager, RocksDB flush, sink commit, watermark logic),
DO NOT apply an in-place fix unless you can trace the full dataflow:

Source → SHM → Batching → UDF boundary → State hot/warm → Barrier/Epoch → Output buffer → Sink prepare/commit → Commit record → Recovery

## If trace is incomplete
- Provide a proposed patch plan, and request/locate the missing context in-repo (types, interfaces, call sites) before editing.
- Prefer adding assertions/metrics/guards rather than changing semantics.
