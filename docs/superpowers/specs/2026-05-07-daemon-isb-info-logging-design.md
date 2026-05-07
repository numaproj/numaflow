# Daemon-side ISB Info Logging — Design

**Date:** 2026-05-07
**Branch:** `isb-info-logging`
**Status:** Approved (brainstorming complete)

## Goal

Periodically emit, at the daemon pod, a structured log line that captures the
current state of every ISB stream (and its consumer) used by the pipeline. This
gives operators a single, low-cost place to:

1. Detect messages stuck in an ISB (pending or ack-pending).
2. See whether messages are being re-delivered (signal of an ack/processing
   problem).
3. Observe the relationship between "messages ever published into the stream"
   and "messages fully acked," i.e. whether messages have been lost.
4. Capture a final, definitive snapshot of ISB state at the moment the
   pipeline shuts down.

Logging is centralised in the daemon — one pod per pipeline — to avoid the
per-dataplane-pod fan-out cost.

## Scope

- **In scope:** JetStream ISB service (the only daemon-side `ISBService`
  implementation today).
- **Out of scope:** Redis and Kafka ISB (no daemon-side `ISBService` impl);
  Prometheus metrics for the new fields; per-pipeline configurability of
  this logging.

## Decisions (from brainstorming)

| Choice | Decision |
|---|---|
| Field richness | Rich JetStream stream + consumer state (not just the existing 3-field `BufferInfo`). |
| Polling cadence | Piggyback on the existing 10 s health-checker tick — zero extra NATS calls. |
| Shutdown snapshot | Periodic logging plus an explicit final log line tagged `event=shutdown_snapshot`. |
| Log line shape | One aggregated log line per tick with all buffers nested as a JSON array. |
| Toggle | Always on. Operators silence via global log level. |
| Implementation approach | Extend the internal `isbsvc.BufferInfo` struct (gRPC proto unchanged) and hook the logger into the health checker's tick loop. |

## Architecture

```
┌────────────────────── daemon pod ──────────────────────┐
│                                                         │
│  daemon_server.Run(ctx)                                 │
│    └── HealthChecker.startHealthCheck(ctx)              │
│              │                                          │
│              ▼  (every 10s)                             │
│          listBuffers(...)                               │
│              │                                          │
│              ▼                                          │
│          isbSvcClient.GetBufferInfo(buf)  ──┐           │
│              │                              │           │
│              │     ┌── populates ───────────┘           │
│              │     ▼                                    │
│          isbsvc.BufferInfo {                            │
│            // existing                                  │
│            PendingCount, AckPendingCount, TotalMessages │
│            // NEW (daemon-internal, not in gRPC proto)  │
│            StreamFirstSeq, StreamLastSeq, StreamBytes,  │
│            ConsumerNumRedelivered, ConsumerNumWaiting,  │
│            ConsumerDeliveredStreamSeq,                  │
│            ConsumerAckFloorStreamSeq                    │
│          }                                              │
│              │                                          │
│              ▼                                          │
│          logISBSnapshot(buffers, "periodic")            │
│              │                                          │
│              ▼                                          │
│          zap.Infow("ISB snapshot", ...)                 │
│                                                         │
│    on ctx.Done():                                       │
│       (uses fresh 5s-timeout ctx)                       │
│       listBuffers(...)                                  │
│       logISBSnapshot(buffers, "shutdown_snapshot")      │
│                                                         │
└─────────────────────────────────────────────────────────┘
```

### Components

- **`pkg/isbsvc/interface.go`** — extend the internal `BufferInfo` struct with
  the new optional rich fields. The struct is daemon-internal; it is *not* the
  gRPC `daemon.BufferInfo` proto, so adding fields is not a public API change.
  The `pipeline_metrics_query.go` translation layer continues to copy only the
  existing three fields into the gRPC response.

- **`pkg/isbsvc/jetstream_service.go:GetBufferInfo`** — populate the new
  fields from the same `StreamInfo` and `ConsumerInfo` calls that already run
  there. No additional NATS round-trips.

- **`pkg/daemon/server/service/isb_info_logger.go` (new file)** — single
  exported function:
  ```go
  func logISBSnapshot(ctx context.Context, pipeline string,
      isbSvcType string, event string, buffers []*isbsvc.BufferInfo)
  ```
  It builds a single `zap.Infow("ISB snapshot", ...)` line with the schema
  documented below. No goroutine of its own.

- **`pkg/daemon/server/service/health_status.go`** — at the end of each
  tick, after `listBuffers` succeeds, the health checker also calls
  `logISBSnapshot(ctx, pipeline, isbSvcType, "periodic", richBuffers)`. The
  health checker's existing buffer-fetch loop is amended so that, in
  addition to the gRPC `[]*daemon.BufferInfo`, it also retains the slice of
  internal `[]*isbsvc.BufferInfo` and passes that to the logger.

- **`pkg/daemon/server/service/pipeline_metrics_query.go:listBuffers`** —
  return type changes (or a parallel return) to provide both the gRPC slice
  and the internal slice. Only callers that need the rich fields use the
  internal slice; existing callers keep using the gRPC slice unchanged.

- **Shutdown path** — inside the `startHealthCheck` loop, the
  `case <-ctx.Done()` branch is upgraded: before returning, it builds a
  fresh `ctx2, cancel := context.WithTimeout(context.Background(),
  5*time.Second)`, calls `listBuffers(ctx2, ...)`, and emits a final
  `logISBSnapshot(ctx2, ..., "shutdown_snapshot", ...)` line. If
  `listBuffers` fails, log a warning indicating the snapshot was
  unavailable, then return. Never block shutdown.

## Log line schema

**Message:** `"ISB snapshot"` (zap `Infow`)

**Top-level fields**

| Field | Type | Source / notes |
|---|---|---|
| `pipeline` | string | `pipeline.Name` |
| `isb_svc_type` | string | `"jetstream"` |
| `event` | string | `"periodic"` or `"shutdown_snapshot"` |
| `num_buffers` | int | `len(buffers)` |
| `buffers` | array of objects | one entry per ISB stream, schema below |

**Per-buffer object**

All numeric fields are stored on the internal `isbsvc.BufferInfo` struct as
`int64` (matching the existing convention) and are emitted as JSON numbers by
zap. Source values from the NATS Go SDK are converted with a plain `int64(...)`
cast at the point of population in `jetstream_service.go:GetBufferInfo`.

| Field | Type | Source | Why useful |
|---|---|---|---|
| `buffer` | string | buffer / stream name | identifies the ISB edge |
| `vertex` | string | owning vertex via `pipeline.FindVertexWithBuffer` | groups by pipeline stage |
| `stream_msgs` | int64 | `stream.State.Msgs` | current depth in the stream |
| `stream_first_seq` | int64 | `stream.State.FirstSeq` | oldest retained msg seq |
| `stream_last_seq` | int64 | `stream.State.LastSeq` | total messages ever published into this stream (monotonic) |
| `stream_bytes` | int64 | `stream.State.Bytes` | byte size of the stream |
| `pending` | int64 | `consumer.NumPending` | not yet delivered to a consumer |
| `ack_pending` | int64 | `consumer.NumAckPending` | delivered but not yet acked |
| `num_redelivered` | int64 | `consumer.NumRedelivered` | re-delivery count (rising = ack/processing problem) |
| `num_waiting` | int64 | `consumer.NumWaiting` | pull-mode consumers waiting |
| `delivered_stream_seq` | int64 | `consumer.Delivered.Stream` | highest seq ever delivered |
| `ack_floor_stream_seq` | int64 | `consumer.AckFloor.Stream` | seq below which everything has been acked |
| `inflight` | int64 | derived: `stream_last_seq − ack_floor_stream_seq` | in-flight (not-fully-acked) total |

### Debug queries this supports

- *Are messages stuck?* — `pending + ack_pending > 0` over consecutive ticks.
- *Are messages being re-delivered?* — `num_redelivered` rising tick-over-tick.
- *How many messages were left at shutdown?* — grep for
  `event=shutdown_snapshot` and read `inflight` per buffer.
- *Total messages produced into a stream over the run?* — `stream_last_seq`
  on the shutdown snapshot.

## Error handling

- **Periodic tick:** if `listBuffers` fails, the health checker already logs
  the error and skips this tick. The new logger is simply not invoked for
  that tick. The next 10 s tick retries.
- **Shutdown snapshot:** the parent context is cancelled, so we use a fresh
  `context.WithTimeout(context.Background(), 5*time.Second)`. If `listBuffers`
  still fails (e.g. NATS is already gone), log a single warning
  (`"ISB snapshot at shutdown unavailable"`, with the underlying error) and
  return. Shutdown must never block on this.

## Testing

- **`pkg/daemon/server/service/isb_info_logger_test.go` (new):** build a
  fixture `[]*isbsvc.BufferInfo` slice with known values; capture zap output
  via `zaptest/observer`; assert every documented field is present and
  correctly typed for both `event="periodic"` and `event="shutdown_snapshot"`.
- **`pkg/isbsvc/jetstream_service_test.go` (extend):** assert that
  `GetBufferInfo` populates the new rich fields by feeding a known
  `StreamInfo` / `ConsumerInfo` to the existing JetStream test setup.
- **No new e2e tests.** This is a logging-only change. Existing e2e suites
  remain unchanged.

## Out of scope (deliberately)

- Redis / Kafka ISB backends — no daemon-side `ISBService` implementation
  exists today.
- Prometheus metrics for these fields. Logs only.
- Per-pipeline or env-var configurability. Always on; operators can suppress
  via global log level.
- Reduce-vertex PBQ (Persistent Buffer Queue) state. PBQ is owned by the
  reduce dataplane pod, not the daemon, and is intentionally not covered.

## Acceptance

- One `"ISB snapshot"` log line is emitted every 10 s by the daemon pod for
  any running pipeline whose ISB type is JetStream, containing every field
  in the schema above for every buffer in the pipeline.
- One `"ISB snapshot"` log line tagged `event=shutdown_snapshot` is emitted
  when the daemon pod receives SIGTERM, before the process exits, unless
  NATS is already unreachable.
- No additional NATS round-trips beyond what the existing health checker
  already performs.
- Existing daemon gRPC API (`daemon.BufferInfo`) is unchanged on the wire.
- Unit tests cover both the logger and the JetStream-side rich-field
  population.
