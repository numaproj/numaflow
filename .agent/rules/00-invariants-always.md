---
trigger: always_on
---

# System Invariants Guardrail (Always On)

This repository implements Enhanced MonoVertex / Fused Pod Streaming Engine. Correctness is defined by invariants.

## Hard rule
- Any change touching ownership, epoching, commit record, sinks, state, watermarks, or SHM must explicitly list which invariants are impacted and how they remain satisfied.

## Required invariants checklist for any such change
- Ownership: exclusive ownership, immediate revocation, GenerationID fencing
- Epoching: seal before persist, no cross-epoch leakage, partial epochs non-authoritative
- Commit record: authoritative, atomic persist, dual persistence, no implicit wipe
- Sink visibility: prepare vs commit, commit record never lies about visibility
- Source progress: commit-gated offsets/sequences, replay uncommitted epochs
- Memory: bounded buffers, backpressure on lag, dirty-flush before eviction
- Watermarks: idle handling cannot skip buffered work
- Schema: SchemaID checked, forward-only, upcaster or fail-fast

## Output format for invariant-sensitive edits
Before editing, write:
1) affected components
2) invariants touched
3) failure modes introduced/avoided
4) test/verification plan
