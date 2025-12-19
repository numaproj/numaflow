---
description: Safely change SHM headers, codec envelopes, or wire formats.
---

## Steps
1) Identify versioning strategy and backward compatibility requirements.
2) List all readers/writers that must change (Rust + Go).
3) Add compatibility tests and bounds checks.
4) Plan staged rollout (accept old+new for one release).
5) Implement and verify.
