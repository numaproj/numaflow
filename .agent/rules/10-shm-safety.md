---
trigger: always_on
---

# SHM & Pointer Safety (Always On)

Shared-memory uses pointer/offset-based handoff. Mistakes cause UAF/OOB/OOM.

## Mandatory rules
- Never change SHM header layout, alignment, or buffer reclamation rules without updating:
  - versioning / compatibility checks
  - producer/consumer credit protocol
  - tests that validate reuse safety
- Never introduce unbounded SHM writes. Credits must gate writes.
- Any buffer reuse must be justified by an explicit lifecycle rule (epoch/sequence/ack).

## Forbidden patterns
- "Just increase ring buffer size" as a fix without backpressure/lag analysis.
- Passing raw pointers across process boundaries without versioned offsets and bounds checks.
