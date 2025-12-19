---
trigger: always_on
---

# Go Operator / UDF Isolation (Always On)

## Rules
- The Rust core must not assume UDF correctness. Treat UDF as untrusted.
- IPC boundaries must remain stable: SHM + fallback RPC (if present).
- Any changes to headers/protocols must update both sides and include compatibility/versioning.

## Go changes
- gofmt required
- No goroutine leaks in long-running loops
- Any backpressure changes must be mirrored in credit protocol behavior
