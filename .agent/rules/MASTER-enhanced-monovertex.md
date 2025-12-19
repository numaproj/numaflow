---
trigger: always_on
---

# Enhanced MonoVertex — Workspace Master Rule (Always On)

This repository implements the **Enhanced MonoVertex / Fused Pod Streaming Engine**.
Correctness is governed by explicit system invariants and ledger-style commit semantics.

This MASTER rule exists to:
- prevent unsafe in-place edits
- prevent partial fixes without end-to-end reasoning
- enforce streaming correctness invariants
- coordinate multiple sub-rules as a single policy bundle

---

## 1. Rule Aggregation

This rule ENFORCES the following workspace rules. They are considered active even if not explicitly mentioned elsewhere.

- @.agent/rules/00-invariants-always.md
- @.agent/rules/01-no-inplace-without-trace.md
- @.agent/rules/10-shm-safety.md
- @.agent/rules/20-commit-record-visibility.md
- @.agent/rules/30-state-rocksdb.md
- @.agent/rules/40-go-operator-udf.md

Optional (manual activation only):
- @.agent/rules/90-big-change-manual.md

---

## 2. Default Agent Behavior (Non-Negotiable)

### Safety First
- The agent MUST prioritize correctness over performance, refactoring convenience, or brevity.
- If invariants cannot be proven safe, the agent MUST stop and propose a plan instead of editing.

### No In-Place Semantic Fixes
- The agent MUST NOT apply in-place fixes to correctness-critical paths unless it can trace:
  Source → SHM → Batch → UDF → State → Epoch → Output → Sink → CommitRecord → Recovery

### Minimal Diff Bias
- Prefer the smallest possible change.
- Do not reorganize code, rename symbols, or reformat unrelated files unless explicitly requested.

---

## 3. Invariant-Aware Editing Contract

For any change touching **ownership, epoching, state, sinks, SHM, commit records, watermarks, or schema**, the agent MUST:

1) List impacted components
2) List impacted invariants
3) Describe failure modes avoided or introduced
4) Describe crash/replay behavior
5) Propose verification steps

If any of the above cannot be satisfied, the agent MUST NOT modify code.

---

## 4. Streaming-Specific Prohibitions

The following fixes are FORBIDDEN unless explicitly requested and justified:

- “Increase buffer size” as a primary solution
- Removing fencing or GenerationID checks
- Advancing source offsets outside commit logic
- Treating prepared data as committed/visible
- Silently coercing schema or decoding without SchemaID validation
- Introducing unbounded queues or channels
- Using wall-clock time for correctness decisions (except idle heuristics)

---

## 5. SHM & IPC Strictness

- SHM header layout, alignment, and lifecycle are **wire protocols**
- Any change requires:
  - versioning
  - backward compatibility analysis
  - Rust + Go updates
  - tests for reuse safety
- Credits MUST gate all writes

---

## 6. Sink & Commit Ordering Law

- Commit records MUST NOT be marked committed unless sink visibility has succeeded.
- Prepared and committed states MUST be distinguishable.
- Kafka EOS, S3 manifests, and JetStream dedupe MUST preserve replay safety.

---

## 7. Backpressure & Memory Law

- Buffers MUST be bounded.
- Checkpoint lag MUST drive backpressure.
- Dirty state MUST flush before eviction.
- OOM is considered a correctness failure.

---

## 8. Schema Evolution Law

- SchemaID is part of state identity.
- Forward-only evolution by default.
- Incompatible schema changes require explicit upcasting or fail-fast.
- Silent schema mismatch is forbidden.

---

## 9. Go Operator / UDF Isolation

- Rust core must treat UDFs as untrusted.
- IPC boundaries must remain stable.
- Any protocol change must update both sides.

---

## 10. When in Doubt

If the agent is unsure:
- STOP
- Produce an RFC-style plan
- Ask for clarification
- Or activate @.agent/rules/90-big-change-manual.md

---

## Final Authority Clause

If a change cannot be justified using the **System Invariants**,
it MUST NOT be implemented.

Correctness > Performance > Convenience.
