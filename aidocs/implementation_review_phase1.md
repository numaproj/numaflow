# Enhanced MonoVertex Phase 1 Review
 **Commit**: `43e42ad1a4a84404a559dc79b44dc944fff6b0aa`
 **Status**: **PARTIAL** (Foundation present, Invariants missing)

## 1. Compliance Checklist

| Requirement | Status | Findings |
| :--- | :--- | :--- |
| **SHM Transport** | ✅ **Pass** | Ring buffer implemented with `mmap` and `unsafe` ops. |
| **Credits/Flow** | ⚠️ **Partial** | Basic blocking/backoff loop exists, but precise credit accounting is implicit. |
| **Framing Contract** | ❌ **Fail** | Wire format is `[Len][ProtoBytes]`. Missing `EpochID`, `GenID` metadata in header. |
| **GenerationID** | ❌ **Fail** | No `GenerationID` variable or check found in `Source` or `Forwarder` hot paths. |
| **Revocation** | ⚠️ **Risk** | Relies on `CancellationToken` (graceful) rather than immediate fencing/abort. |

## 2. Detailed Findings

### A. Shared Memory (SHM) Framing
**Current State**:
The `ShmMapClient` writes:
```rust
framed.extend_from_slice(&(buf.len() as u32).to_le_bytes());
framed.extend_from_slice(&buf); // Raw protobuf bytes
```
**Violation**:
The spec requires "Every batch carries: `epoch_id`, `generation_id`, `schema_id`, `partition`".
Currently, these are not serialized. If the UDF receives a batch, it has no way to validate the `GenerationID` or `Epoch`.

**Remediation**:
Define a struct for the SHM header that precedes the data payload:
```rust
#[repr(C)]
struct BatchHeader {
    length: u32,
    epoch_id: u64,
    generation_id: u64,
    partition_id: u16,
    // ...
}
```

### B. GenerationID Fencing
**Current State**:
`Source` creates messages and sends them. `Forwarder` pipes them.
There is **no verification** that the `GenerationID` held by the pod matches the `GenerationID` of the partition assignment during the write.

**Violation**:
"Any write with stale GenerationID is rejected."
Without this check, a zombie pod could continue writing to SHM (and thus to Sink) after it has lost ownership.

**Remediation**:
1. Inject `GenerationID` into `Source` on assignment.
2. Embed `GenerationID` in every `Message`.
3. Verify `GenerationID` matches current assignment before:
    *   Writing to SHM.
    *   Writing to Sink.

### C. Immediate Revocation
**Current State**:
`Source` listens to `cln_token.cancelled()` and waits for `ack`s to drain.
```rust
// wait for all the ack tasks to be completed before stopping
```
**Violation**:
"On revoke: stop reads, state writes, and sink writes **immediately**."
Graceful draining is dangerous in a split-brain scenario. If revoked, we must stop *writes* instantly. We can optionally try to nack/abort, but we must not commit or persist anything further.

**Remediation**:
Introduce a `Guard` or `Fence` atomic flag.
`REVOKED = true` -> All subsequent `write()` calls return `Err(Fenced)`.

## 3. Next Steps
1.  **Refactor SHM Wire Format**: Add strict metadata header.
2.  **Add GenerationID**: Propagate from assignments to `Source` -> `Forwarder`.
3.  **Implement Fencing**: Add `AtomicBool` for ownership status that gates all writes.
