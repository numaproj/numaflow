---
description: Safely implement a feature or bugfix without violating streaming invariants.
---

## Steps
1) Summarize requested change in 2-3 bullets.
2) Trace end-to-end flow: Source→SHM→Batch→UDF→State→Epoch→Output→Sink→CommitRecord→Recovery.
3) List invariants impacted and how preserved.
4) Identify files/modules to modify (no edits yet).
5) Propose minimal diff strategy + test plan.
6) Only then implement changes.
7) Run/describe verification: unit tests, integration crash-point tests, fmt/lint.
