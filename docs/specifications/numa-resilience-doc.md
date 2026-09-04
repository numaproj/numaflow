# Numa Container Resilience

| | |
| --- | --- |
| Status | Proposed |
| Tracking issues | [#3367](https://github.com/numaproj/numaflow/issues/3367) (umbrella), [#3368](https://github.com/numaproj/numaflow/issues/3368) (this proposal) |
| Related future work | [#3369](https://github.com/numaproj/numaflow/issues/3369), [#3370](https://github.com/numaproj/numaflow/issues/3370), [#3371](https://github.com/numaproj/numaflow/issues/3371) |
| Built-in sources | Separate resilience track; not covered by UDF reconnect. |
| Reduce vertices | Excluded from automatic reconnect in Phase 1 pending durable-state design. |
| Target Release | 1.9 |

## Context

Today each vertex and monovertex pod has a `monitor` sidecar, a `numa` container, and UDF sidecars. The monitor sidecar serves runtime errors from the shared emptyDir because the `numa` process exits on runtime failures such as UDF gRPC errors. While `numa` is restarting, it cannot host the error endpoint.

The umbrella goal in #3367 is to remove the monitor sidecar. Phase 1, tracked by #3368, makes that possible by keeping `numa` alive across non-reduce UDF failures. Built-in sources are also part of the broader resilience story, but they do not use UDF gRPC streams and need connector-specific follow-up work.

## Goals

- For source, transformer, map, and sink UDFs, `numa` must not exit on UDF-side gRPC failures.
- `numa` must continue serving metrics and health endpoints during UDF outage and reconnect cycles.
- UDF errors remain observable through logs, `critical_error_total`, and persisted runtime-error files.
- Processing resumes automatically once the UDF sidecar is healthy again.
- Built-in source connectors get a separate resilience track for transient broker or service failures.
- Reduce vertices keep today's persist-and-exit behavior until durable reduce state is designed and approved.

## Non-Goals

- Moving `/runtime/errors` into `numa`; that is Phase 2, #3369.
- Changing daemon-server polling or UI-facing APIs; that is Phase 3, #3370.
- Adding e2e failure coverage; that is Phase 4, #3371.
- Changing source SDK at-least-once contracts.
- Implementing built-in source resilience in Phase 1; this document only records the direction and required follow-up.
- Changing Go controllers, CRDs, UI, or monitor-sidecar code in this phase.

## Proposed Design

All non-reduce UDF gRPC failures are handled uniformly. There is no transient vs non-transient classifier. On UDF failure, `numa`:

1. Logs the error.
2. Increments `critical_error_total` with a UDF-specific reason.
3. Persists the runtime error.
4. Reconnects to the UDF sidecar.
5. Redrives the in-flight request without exiting.

Built-in source failures are not handled by this UDF reconnect path. Built-in connectors such as Kafka, Pulsar, NATS, Jetstream, SQS, HTTP, and generator sources are Rust implementations that talk directly to external systems through connector-specific APIs.

### Reconnect Helper

Reconnect reuses the startup flow:

- Re-read SDK server-info.
- Recreate a fresh `tonic::transport::Channel`.
- Rebuild the typed client stub.
- Run the typed readiness check.
- Reopen the BiDi stream and handshake.

Retries use the existing infinite one-second retry pattern and are bounded only by the normal shutdown cancellation token. A failure of `sdk_server_info` during reconnect is itself retried at the same one-second cadence; the reconnect helper does not surface it as a hard error.

### Map Clients

Map unary, batch, and stream use task-level retry. The receiver task does not cache requests; the per-message or per-batch task owns the original `MapRequest`.

Map clients need shared mutable stream state because old `read_tx` clones are permanently dead after the BiDi receiver drops. Reconnect must:

1. Create the new `(read_tx, read_rx)`.
2. Open the new BiDi stream and complete handshake.
3. Install the new sender in shared state.
4. Reset `SenderMapState.closed = false` and clear residual sender-map entries.
5. Fire `reconnect_ready`.

This ordering prevents retried calls from observing either a stale sender or a still-closed sender map.

Streaming map can emit partial output before a stream failure. Already-emitted outputs are not rolled back. On retry, the UDF may re-emit them, producing duplicates. `parent_info.current_index` continues monotonically so post-reconnect outputs get fresh `MessageID.index` values.

### Source Transformer

Transformer retry happens per message, not per batch. Each future inside `transform_batch` retries its own `Transformer::transform(...)` call on `Error::UdfRedrive`.

Only after `transform()` returns `Ok` does the future update the tracker, create output handles, and call `mark_success!`. Sibling futures that already completed are not rerun.

### Sink

`UserDefinedSink::sink()` reconnects in place when the response stream fails, reopens the stream, and re-sends the current `Vec<SinkRequest>` plus EOT. `mark_success_batch!` remains gated on `sink()` returning `Ok`.

External sinks may see duplicates if the old UDF process wrote data before crashing. That is consistent with at-least-once contract.

### Source Read, Ack, and Nack

Source `read()` reconnects in place and returns `Ok(vec![])` for the broken tick. The source SDK remains responsible for redelivering unacked offsets.

Source `ack()` and `nack()` also reconnect in place. Transport failures from `ack_tx.send(...)`, `ack_resp_stream.message()`, or stream-closed `tonic::Status` are redrivable, not `NonRetryable`. Truly terminal internal actor-shutdown cases remain terminal.

The user-defined source SDK contract remains unchanged: offsets must not be committed until an explicit ACK is received and persisted. In-flight NAKs must either be persisted or leave the offset uncommitted so it is redelivered.

### Reduce Exception

Reduce vertices retain current behavior: persist the error and exit. Reduce UDFs hold window state in-process, so reconnecting after a crash could drop or double-count aggregates. This carve-out requires issue-owner signoff.

### Built-in Source Resilience Track

Built-in sources need connector-owned retry and classification instead of the UDF `tonic::Status` reconnect path. The follow-up design should:

- Retry transient startup, pending, partition-discovery, read, ack, commit, or delete errors in place when retrying does not advance source state incorrectly.
- Preserve terminal errors for misconfiguration, invalid credentials, missing permissions, unsupported broker responses, or required resources that cannot recover without operator action.
- Keep at-least-once behavior explicit per connector: source state must not be advanced until downstream processing completes, and duplicate ack/commit/delete attempts must be safe.
- Start with connectors that can restart-loop during broker coordination, rebalance, throttling, or timeout states, then audit the remaining built-in sources.

This track complements UDF reconnect work. UDF reconnect does not automatically solve built-in source restart loops.

## Observability

Reuse `critical_error_total` with new reason strings:

- `source_runtime_error`
- `source_transformer_runtime_error`
- `udf_runtime_error`
- `sink_runtime_error`
- `fallback_sink_runtime_error`

Runtime errors continue to be written under `/var/numaflow/runtime/application-errors/<container>/`.

Two persistence changes are required:

- Remove the `OnceLock` gate so more than one error per process can be recorded.
- Replace second-resolution filenames with collision-safe names: `<unix_nanos>-<sequence>-numa.json`, and switch the per-call temp filename from the shared `current-numa.json` to a matching unique `<unix_nanos>-<sequence>-numa.tmp` so concurrent writers do not race.

The existing ten-file rotation cap remains.

## At-Least-Once Safety

At-least-once depends on `MessageHandle` and `AckHandle` invariants:

- `mark_success!` only decrements the ref count. ACK happens later when the final handle drops and the ref count is zero.
- `mark_failed!` records a reason but does not decrement the ref count. Drop then NAKs.
- Dropping a handle without success NAKs.

Redrive must therefore never call `mark_success!` or `mark_failed!` on `Error::UdfRedrive`, and must never leak a `MessageHandle`.

Safety by path:

| Path | Loss risk | Duplicate risk |
| --- | --- | --- |
| Source read reconnect | None, SDK redelivers unacked offsets | Possible |
| Source ack reconnect | None, ACK is idempotent | Possible extra ACK frame |
| Source nack reconnect | Conditional on SDK not committing implicit ACKs | Possible extra NAK frame |
| Map and transformer redrive | None, task holds handle until success or drop | Possible UDF reinvocation |
| Sink redrive | None, success is gated on sink response | Possible external writes |
| Built-in source retry | Connector-specific commit/delete/ack contract | Connector-specific; should be none for transient startup probes and idempotent ack retries |

The only conditional UDF case is source NAK reconnect, which depends on the existing user-defined-source SDK contract. Built-in source safety is connector-specific and must be covered by the built-in source resilience track.

## Risks

- Poison messages can loop forever. This is accepted for Phase 1.
- Reconnect storms are possible during sustained UDF outages. The retry interval stays at the current one-second startup retry cadence.
- Reduce still restarts. This is an explicit carve-out.
- SDK NAK behavior must be audited in numaflow-go, numaflow-java, numaflow-python, and numaflow-rs **as a pre-merge gate**. Any non-compliant SDK is either remediated first or carved out from the resilience guarantee.
- Built-in source classifiers can hide terminal errors if they are too broad. Each connector follow-up must document retryable and terminal errors.
- Built-in source startup probes can restart-loop if transient broker states fail fast. Follow-up work should retry those probes in place with cancellation-aware backoff.

## Testing

Required tests:

- Per-client reconnect tests for source read, source ack, source nack, transformer, map unary, map batch, map stream, and sink.
- Tests that `critical_error_total` increments and runtime errors persist for each UDF failure path.
- Tests that upstream cancellation tokens are not cancelled for redrivable UDF failures.
- Map stale-sender and reopened-state regression tests.
- Transformer partial-completion retry test.
- Map-stream partial-output retry test.
- `ack_tx.send` reconnect test.
- Runtime-error filename collision test.
- MessageHandle drop-NAK invariant test during outage.
- Top-level assertion that non-reduce UDF errors do not make `run()` return `Err`.
- User-defined-source SDK NAK-preservation test or audit.

Built-in source follow-up tests:

- Startup probe retry tests for transient metadata, pending-message, offset, or partition-discovery failures.
- Ack/commit/delete retry tests that verify source state is not advanced incorrectly.
- Terminal error tests for credentials, authorization, unsupported configuration, and permanently missing required resources.
- At-least-once tests for redelivery before source-state advancement and duplicate-safe retry after source-state advancement.
