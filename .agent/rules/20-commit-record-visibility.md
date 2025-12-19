---
trigger: always_on
---

# Commit Record & Sink Visibility (Always On)

## Non-negotiable ordering
- A commit record for epoch N may only be marked COMMITTED after sink visibility commit succeeds.
- Prepared markers (if any) must be distinct from committed markers.

## Crash safety
- If a crash can happen between steps, replay must not cause:
  - committed output duplication (Kafka EOS)
  - partial visibility (S3 must be manifest-gated)
  - progress beyond last committed source position

## Edits touching commit/sink
- Must describe crash points: before/after persist, before/after prepare, before/after commit.
- Must describe replay behavior per sink mode.
