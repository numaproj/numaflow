---
description: Validate commit ordering and replay safety by enumerating crash points.
---

## Steps
1) Identify boundary: Seal/Persist/Prepare/Commit.
2) Enumerate crash points between each step.
3) For each sink mode (Kafka EOS / S3 manifest / JetStream dedupe), state expected replay behavior.
4) Confirm: commit record never lies, no partial visibility, source progress gated.
5) Output as a small truth table.
