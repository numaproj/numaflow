---
trigger: manual
---

# Big Change Protocol (Manual)

When activated:
- Produce an RFC-style plan first:
  - motivation, invariants touched, alternatives, migration plan
- Identify all impacted modules/files
- Require a staged rollout approach (flags or compatibility layers)
- Add/modify tests before or alongside implementation
- Keep diffs in small, reviewable commits
