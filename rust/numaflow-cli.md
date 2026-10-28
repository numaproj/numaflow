# Numaflow UDF Test CLI — Design Plan

A standalone, non-interactive CLI (`nfcli`) that impersonates the numa container and talks
directly to a user-defined-function (UDF) gRPC server so that UDF authors can test their
code without deploying a pipeline. It connects over a unix domain socket (like numa does in
k8s) or over TCP (like the Java SDK's local mode), sends one or more test messages, prints
the UDF's responses, and exits.

---

## 1. Goals

- Test any UDF supported by numaflow: map (unary / batch / stream), source transformer,
  reduce (fixed / sliding), session reduce, accumulator, sink (incl. fallback and
  on-success sinks), source, and side input.
- Connect via `--socket <path>` (UDS) **or** `--tcp <[host:]port>` — exactly one, never both.
- **Never read the `server-info` file.** The user states the UDF type (subcommand) and, for
  map, the mode (`--mode`). This matters because the Java SDK's local/TCP mode does not
  write a server-info file at all, and the file carries no host/port information anyway.
- Non-interactive: the command runs, drives the full protocol choreography (handshake,
  batching, EOT, window open/close), prints results, and completes with a meaningful exit
  code.
- Single messages can be given inline on the command line; multi-message scenarios
  (reduce, sessions, batch map, …) read messages from a human-readable file that supports
  multi-line payloads.
- Batching (`--batch-size`) and pacing (`--delay`) between batches, with defaults that
  mirror numa's own behavior.

### Non-goals

- No interactive/REPL mode.
- No reading of `server-info` files or `MAP_MODE` metadata.
- No pipeline-spec parsing; everything is explicit CLI input.
- No TLS/auth (neither SDK supports it on the UDF socket today); can be added later.
- The CLI does not emulate the ISB, persistence, retries, or WAL replay — it drives the
  gRPC contract only.

---

## 2. UDF types covered

The gRPC contract per type, as implemented by numa (`rust/numaflow-core`) and both SDKs
(`numaflow-go`, `numaflow-java`):

| Subcommand        | gRPC service (proto)                  | Primary RPC (kind)                          | Handshake | Default SDK socket                    |
|-------------------|----------------------------------------|---------------------------------------------|-----------|----------------------------------------|
| `map` (unary)     | `map.v1.Map`                           | `MapFn` (bidi)                              | yes       | `/var/run/numaflow/map.sock`           |
| `map --mode batch`| `map.v1.Map`                           | `MapFn` (bidi)                              | yes       | `/var/run/numaflow/batchmap.sock`      |
| `map --mode stream`| `map.v1.Map`                          | `MapFn` (bidi)                              | yes       | `/var/run/numaflow/mapstream.sock`     |
| `transform`       | `sourcetransform.v1.SourceTransform`   | `SourceTransformFn` (bidi)                  | yes       | `/var/run/numaflow/sourcetransform.sock`|
| `reduce`          | `reduce.v1.Reduce`                     | `ReduceFn` (bidi, one stream per window)    | no        | `/var/run/numaflow/reduce.sock` (streamer: `reducestream.sock`) |
| `session-reduce`  | `sessionreduce.v1.SessionReduce`       | `SessionReduceFn` (bidi, single stream)     | no        | `/var/run/numaflow/sessionreduce.sock` |
| `accumulator`     | `accumulator.v1.Accumulator`           | `AccumulateFn` (bidi, single stream)        | no        | `/var/run/numaflow/accumulator.sock`   |
| `sink`            | `sink.v1.Sink`                         | `SinkFn` (bidi)                             | yes       | `/var/run/numaflow/sink.sock` (fallback: `fb-sink.sock`, on-success: `ons-sink.sock`) |
| `source`          | `source.v1.Source`                     | `ReadFn`/`AckFn` (bidi) + `NackFn`/`PendingFn`/`PartitionsFn` (unary) | yes (read & ack streams separately) | `/var/run/numaflow/source.sock` |
| `side-input`      | `sideinput.v1.SideInput`               | `RetrieveSideInput` (unary)                 | n/a       | `/var/run/numaflow/sideinput.sock`     |

Notes:

- All three map modes share the **same** `map.v1.Map/MapFn` bidi RPC; only the client-side
  choreography differs (that is exactly what `MAP_MODE` in server-info tells numa — the CLI
  replaces it with `--mode`). The old standalone `mapstream.proto` is not used by numa
  anymore and is not supported by the CLI.
- **Reduce streamer** servers (Go `reducestreamer`, Java `reducestreamer`) speak the same
  `reduce.v1` protocol as plain reduce; the only difference is that results arrive before
  the window closes. The `reduce` subcommand works unchanged for both — just point
  `--socket`/`--tcp` at the right server.
- **Fallback / on-success sinks** speak the same `sink.v1` protocol; use the `sink`
  subcommand pointed at the corresponding socket/port.
- Every service also exposes `IsReady`; the CLI calls it (with retries) before sending
  anything, and a `ready` subcommand exposes it directly.

---

## 3. Connecting: `--tcp` vs `--socket`

Exactly one of the two must be given; providing both or neither is a usage error.

```
--socket <path>          # unix domain socket, e.g. /var/run/numaflow/map.sock
--tcp <[host:]port>      # plain gRPC over TCP; host defaults to localhost
```

- `--socket` matches how numa connects in k8s (tonic/grpc-go over UDS). Works on Linux and
  macOS. The per-type default SDK socket paths are listed in the table above — the CLI does
  **not** assume them; the user passes the path explicitly (keeps behavior obvious and
  avoids accidentally talking to the wrong mode's socket).
- `--tcp` matches the Java SDK's local mode: when a Java UDF runs outside a pod
  (`NUMAFLOW_POD` env var absent, or `GRPCConfig.newBuilder().isLocal(true).port(...)`),
  it listens with `ServerBuilder.forPort(port)`, default port **50051**. So
  `--tcp 50051` / `--tcp localhost:50051` are the canonical forms.
  (The Go SDK is UDS-only today; `--tcp` is future-proofing for other SDKs growing a TCP
  listener.)
- Connection settings mirror numa: max gRPC message size 64 MiB in both directions
  (override with `--max-message-size`), no custom gRPC metadata headers, retry
  connect + `IsReady` once per second until `--timeout` (default 5s) elapses.

---

## 4. Input messages

### 4.1 Which fields exist on the wire

Union of the request payload fields across all UDF protos (`pkg/apis/proto/*/v1/*.proto`):

| Field       | Proto type                    | Used by                                   | Required? |
|-------------|-------------------------------|-------------------------------------------|-----------|
| `value`     | `bytes`                       | all data UDFs                             | **yes** (CLI makes payload compulsory) |
| `keys`      | `repeated string`             | all data UDFs                             | optional (default `[]`; drives per-key grouping in reduce family) |
| `event_time`| `google.protobuf.Timestamp`   | all data UDFs                             | required by proto → CLI defaults it (see below); drives window assignment in reduce family |
| `watermark` | `google.protobuf.Timestamp`   | all data UDFs                             | required by proto → CLI defaults to `event_time` |
| `headers`   | `map<string,string>`          | all data UDFs                             | optional (default `{}`) |
| `id`        | `string`                      | map (envelope), transform, sink, accumulator | required for response correlation → CLI auto-generates `msg-<n>` if not given |
| `metadata`  | `metadata.Metadata` (`previous_vertex`, `user_metadata`, `sys_metadata`) | all data UDFs | optional; CLI exposes `userMetadata` and `previousVertex`, never `sys_metadata` (platform-internal) |

**Tags are output-only.** No request proto carries tags; they exist only on responses
(`Result.tags`) where the UDF sets them for conditional forwarding, dropping
(`\__DROP__`), or nacking (`\__NACK__` + `nack_options`). The CLI therefore never accepts
tags as input — it prints them prominently in the output instead, and renders the drop/nack
sentinels as `DROPPED` / `NACKED(delay=…)`.

### 4.2 Inline single message (quick tests)

For one-shot tests the message can be given entirely on the command line:

```
--payload <string>            # UTF-8 payload (compulsory: this, --payload-file, or -f)
--payload-file <path>         # payload = raw bytes of the file (binary-safe)
--payload-base64 <string>     # payload = decoded bytes
--key <k>                     # repeatable → keys list
--header <k>=<v>              # repeatable → headers map
--event-time <RFC3339 | +dur> # default: base time (see 4.4)
--watermark  <RFC3339 | +dur> # default: same as event time
--id <string>                 # default: msg-1
```

Exactly one payload source (`--payload` / `--payload-file` / `--payload-base64` / `-f`)
must be provided — that satisfies "payload is compulsory".

### 4.3 Message file format (`-f, --file`)

Multi-message input comes from a **YAML multi-document stream**: one document per message,
separated by `---`. Rationale:

- Human-readable and hand-editable, supports `#` comments.
- Multi-line payloads are natural with block scalars (`payload: |`), with no escaping —
  the payload can be JSON, XML, log lines, anything.
- Binary payloads via `payloadBase64` or an external `payloadFile`.
- Rejected alternatives: JSONL (multi-line payloads require `\n`-escaping, unreadable),
  custom delimiter formats (ambiguous when the payload itself contains the delimiter),
  one-file-per-message (unmanageable for reduce scenarios).

Per-document schema (unknown fields are a hard error, to catch typos):

| Field           | Type              | Default                  | Notes |
|-----------------|-------------------|--------------------------|-------|
| `payload`       | string            | — (one of the three payload fields is required) | UTF-8 bytes; use `\|-` block scalar for exact multi-line content |
| `payloadBase64` | string            | —                        | binary-safe alternative |
| `payloadFile`   | string (path)     | —                        | raw file bytes; relative to the message file's directory |
| `keys`          | list of strings   | `[]`                     | |
| `headers`       | map string→string | `{}`                     | |
| `eventTime`     | RFC3339 or `+dur` | base time (see 4.4)      | `+dur` is relative to base time, e.g. `+90s` |
| `watermark`     | RFC3339 or `+dur` | = `eventTime`            | |
| `id`            | string            | `msg-<n>` (1-based file order) | ignored (with a warning) by reduce/session-reduce, whose payloads carry no id |
| `userMetadata`  | map group→(map key→string) | `{}`            | maps to `metadata.user_metadata` |
| `previousVertex`| string            | `""`                     | maps to `metadata.previous_vertex` |
| `repeat`        | int ≥ 1           | `1`                      | expands to N copies of this message (auto ids `msg-<n>`, same eventTime); handy for volume in reduce tests |

Example (`events.yaml`):

```yaml
# Two keys, three events in the first 60s window, one in the next.
---
payload: |-
  {"user": "alice", "amount": 30,
   "note": "multi-line payloads
  are fine"}
keys: [alice]
headers:
  source: checkout
eventTime: "+1s"
---
payload: '{"user": "bob", "amount": 25}'
keys: [bob]
eventTime: "+20s"
repeat: 2          # sent twice: msg-2, msg-3
---
payloadBase64: "3q2+7w=="   # 0xDEADBEEF — binary payload
keys: [alice]
eventTime: "+70s"           # lands in the next fixed 60s window
```

`-f -` reads the YAML stream from stdin, so messages can be generated and piped in.

Messages are sent **in file order** (after `repeat` expansion). The file holds per-message
fields only; everything about transport, batching, and windowing stays on the command line,
as requested.

### 4.4 Time handling

- **Base time**: `--base-time <RFC3339>` anchors all relative (`+dur`) times. Default:
  CLI invocation time — except for the reduce family, where it defaults to the invocation
  time truncated down to the window boundary (fixed/sliding are epoch-aligned in numaflow),
  so that `+0s … +59s` predictably land in one 60s window.
- **Event time default**: base time. In a file, give explicit `eventTime` values whenever
  windowing matters.
- **Watermark default**: equal to the message's event time. Real pipelines lag the
  watermark slightly behind event time, but "watermark = event time" is the most useful
  deterministic default for testing; override per message when testing late-data handling.

---

## 5. Batching and pacing

```
--batch-size <n>    # messages taken from the file per batch. Default: 500
--delay <dur>       # sleep between batches, e.g. 500ms, 2s. Default: 0s
```

Defaults mirror numa: `500` is numaflow's default `limits.readBatchSize`, and numa inserts
no artificial delay between forwarded batches.

What a "batch" means per UDF type (matches numa's client choreography exactly):

| Subcommand     | Batch semantics on the wire |
|----------------|------------------------------|
| `map` (unary)  | Send `n` id-tagged requests on the stream, await all `n` responses (id-correlated), then delay. No EOT involved. |
| `map --mode batch` | Send `n` requests, then an explicit EOT (`status.eot=true`) request; await one response per id **plus** the terminating EOT response. A response missing at EOT is an error (`UDF_PARTIAL_RESPONSE`), same as numa. |
| `map --mode stream` | Send `n` id-tagged requests; each id's response stream is read until its own EOT-marked response; then delay. |
| `transform`    | Same as unary map (id-correlated, no EOT). |
| `sink`         | Send `n` requests + EOT request; read responses (results correlated by `Result.id`) until the EOT response. |
| `reduce` / `session-reduce` / `accumulator` | Batch/delay only pace message emission (windows don't care about batches); useful to watch streamed results arrive between batches. |
| `source`       | Not applicable — `--count` and `--rounds` control read sizing; `--delay` applies between rounds. |

---

## 6. Global options (all subcommands)

```
Connection (exactly one required):
  --socket <path>               UDS path to the UDF server
  --tcp <[host:]port>           TCP endpoint (host defaults to localhost)

Tuning:
  --timeout <dur>               connect/ready/response-wait timeout per phase [default: 5s]
  --max-message-size <bytes>    gRPC max send/recv size [default: 64MiB]

Input (data subcommands):
  -f, --file <path|->           YAML message stream ('-' = stdin)
  --payload / --payload-file / --payload-base64, --key, --header,
  --event-time, --watermark, --id      (inline single message, see 4.2)
  --base-time <RFC3339>         anchor for relative times [default: see 4.4]

Pacing:
  --batch-size <n>              [default: 500]
  --delay <dur>                 [default: 0s]

Output:
  -o, --output <text|json|raw>  [default: text]  json = one JSON object per event (JSONL);
                                raw = response payload bytes only, concatenated to stdout
  -v, --verbose                 show wire-level events (handshake, EOT, window ops, timings)
  -q, --quiet                   summary line only
```

Text output renders payloads as UTF-8 when valid, otherwise as base64 with a `(base64)`
marker.

---

## 7. Subcommands, usage, and examples

### 7.1 `map` — unary, batch, and stream modes

```
nfcli map [--mode unary|batch|stream] (--socket <p> | --tcp <hp>) (payload flags | -f file) [pacing/output flags]
```

`--mode` (default `unary`) replaces the server-info `MAP_MODE` metadata
(`unary-map` / `batch-map` / `stream-map`). The wire protocol is the unified `MapFn` bidi
stream for all three; the mode only changes the choreography (see §5 and §9). The mode must
match how the server was built (e.g. a Go `batchmapper.NewServer` expects batch
choreography on `batchmap.sock`).

Examples:

```bash
# Quick unary test against a Java UDF running in local mode (TCP, default port)
nfcli map --tcp 50051 --payload '{"temp_c": 21.5}' --key sensor-1 --header source=test

# Unary map over UDS, many messages, 20 per batch, 500ms between batches
nfcli map --socket /var/run/numaflow/map.sock -f messages.yaml --batch-size 20 --delay 500ms

# Batch map (server must be a batch mapper)
nfcli map --mode batch --socket /var/run/numaflow/batchmap.sock -f messages.yaml --batch-size 50

# Stream map: one input can yield many streamed results
nfcli map --mode stream --socket /var/run/numaflow/mapstream.sock --payload-file big-doc.json
```

Sample text output:

```
✓ ready (uds:/var/run/numaflow/map.sock) · handshake ok
[msg-1] 2 results (3.1ms)
  1: keys=[sensor-1] tags=[metric] payload={"temp_f": 70.7}
  2: keys=[sensor-1] tags=[audit]  payload={"seen": true}
[msg-2] DROPPED (tags=[\__DROP__])
──
sent=2 · results=2 · dropped=1 · failed=0 · elapsed=18ms
```

### 7.2 `transform` — source transformer

```
nfcli transform (--socket <p> | --tcp <hp>) (payload flags | -f file) [pacing/output flags]
```

Identical input model to unary map. The difference is on output: each result carries its
own (re-assigned) `event_time`, which the CLI prints — that is the transformer's defining
feature. Drop messages still carry an event time (`MessageToDrop(eventTime)`), shown too.

```bash
nfcli transform --socket /var/run/numaflow/sourcetransform.sock \
  --payload '{"ts": "2026-07-06T01:02:03Z", "v": 1}' --event-time 2026-07-06T01:00:00Z
```

```
[msg-1] 1 result
  1: keys=[] tags=[] eventTime=2026-07-06T01:02:03Z payload={"v": 1}
```

### 7.3 `reduce` — aligned windows (fixed & sliding)

```
nfcli reduce (--socket <p> | --tcp <hp>) -f <file>
             --window fixed --length <dur>
             [--window sliding --length <dur> --slide <dur>]
             [pacing/output flags]
```

Because numa (not the SDK) owns window assignment, the CLI emulates it:

1. Parse all messages; assign each to its epoch-aligned window(s) by `eventTime`
   (fixed: one window; sliding: every window of size `--length` starting each `--slide`
   that contains the event time).
2. Per window, open a dedicated `ReduceFn` bidi stream (numa does one stream per window);
   send the first message with `WindowOperation{event: OPEN, windows: [start, end, slot:"0"]}`
   and subsequent ones with `APPEND`. Keys ride inside the payload; per-key fan-out happens
   inside the SDK.
3. After all input is sent (batches/delays applied across windows in file order), the CLI
   treats end-of-input as **the watermark advancing to +∞** — the bounded-input analogue of
   numaflow's idle-source watermark progression — and closes every still-open window by
   closing its request stream (for aligned reduce, stream close *is* the CLOSE signal).
   Note the windows themselves are still multiple and purely event-time-driven; end-of-input
   only decides *when* they all fire, so every run terminates with complete, deterministic
   results.
4. Read each window's responses until `EOF=true` or stream end; print results grouped by
   window. Reduce-streamer servers emit results early; the CLI prints them as they arrive,
   so the same subcommand covers `reducestream.sock` servers.

Design note — there is deliberately **no watermark-driven ("progressive") window close**,
i.e. no mode that closes a window mid-run once a later message's watermark passes its end.
It would only change wall-clock interleaving and enable late-data drops, but late data is
dropped by numa *before* it ever reaches the UDF — such a mode would test the platform's
windower, not the user's UDF. Per window, the UDF observes the identical
OPEN/APPEND/stream-close sequence either way; closing everything at end-of-input keeps a
bounded test run deterministic and guaranteed to terminate.

Inline single-message flags are allowed but pointless for reduce; a file is the expected
input. `id` fields are ignored (reduce payloads carry no id).

Examples:

```bash
# Fixed 60s windows: count events per key
nfcli reduce --socket /var/run/numaflow/reduce.sock -f counts.yaml --window fixed --length 60s

# Sliding windows: 60s length sliding every 10s (each event lands in 6 windows)
nfcli reduce --socket /var/run/numaflow/reduce.sock -f counts.yaml \
  --window sliding --length 60s --slide 10s
```

`counts.yaml`:

```yaml
---
payload: "1"
keys: [blue]
eventTime: "+1s"
repeat: 3
---
payload: "1"
keys: [red]
eventTime: "+5s"
---
payload: "1"
keys: [red]
eventTime: "+70s"   # next 60s window
```

Sample output:

```
✓ ready (uds:/var/run/numaflow/reduce.sock)
window [00:00:00 → 00:01:00) · 4 msgs sent · closed
  result: keys=[blue] tags=[] payload=3
  result: keys=[red]  tags=[] payload=1
window [00:01:00 → 00:02:00) · 1 msg sent · closed
  result: keys=[red]  tags=[] payload=1
──
windows=2 · sent=5 · results=3 · elapsed=41ms
```

### 7.4 `session-reduce` — session windows

```
nfcli session-reduce (--socket <p> | --tcp <hp>) -f <file> --gap <dur> [pacing/output flags]
```

`--gap` is the session inactivity gap (the `timeout` field of a `session` window in a
pipeline spec). The CLI ports numa's session windower over a **single** multiplexed
`SessionReduceFn` stream, sending explicit keyed-window operations:

- first message for a key → `OPEN` (payload + `KeyedWindow{start=et, end=et+gap, slot:"0", keys}`)
- message extending an active session → `EXPAND` (payload + `[old window, new window]`)
- message inside the current bounds → `APPEND`
- message bridging two active sessions of the same key → `MERGE` (windows list, no payload)
  followed by the append/expand of the message itself
- end of input → explicit `CLOSE` (no payload) for every still-open session — unlike
  aligned reduce, session CLOSE is a real wire message.

Responses stream back tagged with their `keyedWindow`; `EOF=true` marks a window done.

```bash
nfcli session-reduce --socket /var/run/numaflow/sessionreduce.sock -f clicks.yaml --gap 10s
```

`clicks.yaml`:

```yaml
---
payload: click
keys: [alice]
eventTime: "+0s"
---
payload: click
keys: [alice]
eventTime: "+8s"     # within 10s gap → same session (EXPAND)
---
payload: click
keys: [alice]
eventTime: "+30s"    # gap exceeded → new session (CLOSE + OPEN)
```

```
session [alice] [+0s → +18s)  · 2 msgs · closed
  result: keys=[alice] payload=2
session [alice] [+30s → +40s) · 1 msg  · closed
  result: keys=[alice] payload=1
```

### 7.5 `accumulator` — ordered accumulation

```
nfcli accumulator (--socket <p> | --tcp <hp>) -f <file> [pacing/output flags]
```

Single `AccumulateFn` bidi stream. Per key: `OPEN` (payload + `KeyedWindow`) for the first
message, `APPEND` for the rest (note the enum quirk: accumulator `APPEND=2`, not 4), and an
explicit `CLOSE` (no payload) per key at end of input. Window bounds computation is ported
from numa's unaligned windower (`numaflow-core/src/reduce/reducer/unaligned`). Accumulator
payloads carry an `id` (auto-generated as usual) which the server echoes back — the CLI
prints responses (full payload incl. id, event time, watermark, plus tags) as they stream
until `EOF` per key. Useful for testing sorters/re-orderers: feed out-of-order `eventTime`s
in the file and observe the emission order.

```bash
nfcli accumulator --socket /var/run/numaflow/accumulator.sock -f out-of-order.yaml --batch-size 1 --delay 200ms
```

### 7.6 `sink` — incl. fallback and on-success sinks

```
nfcli sink (--socket <p> | --tcp <hp>) (payload flags | -f file) [pacing/output flags]
```

Choreography per batch: handshake once, then `n` requests (each `Request` carries its `id`)
followed by an EOT request; read responses until the EOT response; correlate `Result.id`
back to inputs. Output shows each message's status from the sink `Status` enum —
`SUCCESS`, `FAILURE` (+ `err_msg`), `FALLBACK`, `ON_SUCCESS` (+ the on-success message),
`NACK` (+ nack options).

To test a fallback or on-success sink server, point the connection at its socket/port
(`fb-sink.sock` / `ons-sink.sock`) — same subcommand, same protocol.

```bash
nfcli sink --tcp 50051 -f orders.yaml --batch-size 100
nfcli sink --socket /var/run/numaflow/fb-sink.sock --payload 'poison-pill' --id order-17
```

```
✓ ready (tcp:localhost:50051) · handshake ok
batch 1/1 (100 msgs, 12.4ms)
  msg-1 … msg-98   SUCCESS
  msg-99           FALLBACK
  msg-100          FAILURE  err="connection refused to db"
──
sent=100 · success=98 · fallback=1 · failure=1 · exit=4 (failures present)
```

### 7.7 `source` — user-defined source

The CLI plays numa's reader role, so there is no input payload/file; instead it requests
data from the UDF:

```
nfcli source (--socket <p> | --tcp <hp>)
             [--count <n>]         # records per ReadRequest        [default: 500 = numa readBatchSize]
             [--read-timeout <dur>]# ReadRequest timeout_in_ms      [default: 1s = numa readTimeout]
             [--rounds <n>]        # number of ReadRequests         [default: 1]
             [--delay <dur>]       # pause between rounds           [default: 0s]
             [--no-ack]            # read without acking
             [--pending]           # also print PendingFn before/after
             [--partitions]        # also print PartitionsFn
             [output flags]
```

Choreography: open the Read stream and the Ack stream (each with its own `sot` handshake),
then per round: send `ReadRequest{num_records, timeout_in_ms}`, collect `ReadResponse`s
until `status.eot=true`, print each message (offset + partition, event time, keys, headers,
payload), then — unless `--no-ack` — send one `AckRequest` with all offsets and await its
`AckResponse`.

```bash
# Read 5 records twice, 1s apart, acking each round; show pending counts
nfcli source --socket /var/run/numaflow/source.sock --count 5 --rounds 2 --delay 1s --pending
```

```
✓ ready · read handshake ok · ack handshake ok
pending=42
round 1: 5 msgs (2.2ms)
  offset=b2Zmc2V0LTE=/p0 eventTime=…T10:00:01Z keys=[] payload={"seq": 1}
  …
  acked 5 offsets ✓
round 2: 5 msgs · acked ✓
pending=32
```

### 7.8 `side-input`

```
nfcli side-input (--socket <p> | --tcp <hp>) [output flags]
```

Single unary `RetrieveSideInput` call; prints the returned value and whether the retriever
chose `no_broadcast` (i.e. "don't update the side input"). No payload flags apply.

```bash
nfcli side-input --socket /var/run/numaflow/sideinput.sock
# → broadcast=true payload={"rates": {"EUR": 1.09}}
```

### 7.9 `ready` — smoke test any UDF server

```
nfcli ready <type> (--socket <p> | --tcp <hp>)
# <type> ∈ map | transform | reduce | session-reduce | accumulator | sink | source | side-input
```

Calls the given service's `IsReady` and exits 0/2. Handy in scripts to wait for a UDF to
come up before running the real test.

---

## 8. Output details and exit codes

- `text` (default): human-oriented, as in the examples; payloads shown as UTF-8 when valid
  else base64. `-v` adds wire events (handshake, EOT, window operations, per-batch
  latency).
- `json`: one JSON object per line (JSONL) per event — `{"type":"result", "id":…,
  "window":…, "keys":…, "tags":…, "payloadBase64":…, "eventTime":…}` etc. — for scripting
  and golden-file assertions.
- `raw`: concatenated response payload bytes to stdout (diagnostics to stderr), for piping.
- Drop/nack sentinels (`\__DROP__`, `\__NACK__` — U+005C is the backslash) are detected in
  response tags and rendered as `DROPPED` / `NACKED(reason=…, delay=…)`.

Exit codes:

| Code | Meaning |
|------|---------|
| 0 | completed; all responses received, no UDF-reported failures |
| 1 | usage error (both/neither of `--tcp`/`--socket`, bad file, missing payload, …) |
| 2 | connect / `IsReady` / handshake failure within `--timeout` |
| 3 | protocol error (gRPC error mid-stream, missing response at EOT — numa's `UDF_PARTIAL_RESPONSE`, response timeout) |
| 4 | protocol OK but UDF reported failures (e.g. sink `FAILURE` results) |

---

## 9. Wire-protocol cheat sheet (implementation-critical)

Verified against `rust/numaflow-core` (the authoritative client) and the SDKs:

| Aspect | Detail |
|--------|--------|
| Handshake | First stream message with `Handshake{sot:true}` and empty request; first response must echo `sot:true`. Required on: map (all modes), transform, sink, source (read stream **and** ack stream separately). **Not** used by reduce/session/accumulator (operation-driven) or the unary services. |
| EOT (client→server) | `TransmissionStatus{eot:true}` terminator request: batch map (after each batch) and sink (after each batch). |
| EOT (server→client) | Batch map & sink: final EOT response per batch. Stream map: EOT response terminates each id's result stream. Source read: `status.eot` ends each ReadRequest's results. |
| Aligned reduce | One `ReduceFn` stream per window. `Event{OPEN=0, CLOSE=1, APPEND=4}` (2,3 skipped). `Window{start,end,slot:"0"}`, no keys. CLOSE = **drop the request stream** (no wire message). Read until `EOF=true` (field 3). |
| Session reduce | Single stream, `KeyedWindow{start,end,slot,keys}`. `Event{OPEN=0, CLOSE=1, EXPAND=2, MERGE=3, APPEND=4}`. CLOSE/MERGE have no payload; EXPAND carries payload + [old,new] windows. `EOF` field 3. |
| Accumulator | Single stream. `Event{OPEN=0, CLOSE=1, APPEND=2}` (≠ reduce's APPEND=4). Payload has its own `id` field. Response = full `Payload` + `tags` + `EOF` (field **4**). |
| Correlation | By `id`: map (envelope `MapRequest.id`), transform (`Request.id`), sink (`Result.id`), accumulator (`Payload.id`). By window: reduce (`window`), session (`keyedWindow`). |
| Sink statuses | `SUCCESS=0, FAILURE=1, FALLBACK=2, ON_SUCCESS=4, NACK=5` (ordinal 3 is reserved). |
| gRPC settings | 64 MiB max encode/decode; UDS via tonic `connect_with_connector` + dummy URI (`http://[::1]:50051`); no gRPC metadata headers needed. |
| Partial response | If a batch-map/sink EOT response arrives before all ids answered → error (numa treats it fatal: `UDF_PARTIAL_RESPONSE`). CLI exits 3. |

---

## 10. Implementation plan

**Language / location**: Rust, new workspace member `rust/numaflow-cli` (binary `nfcli`)
in this repo — it can reuse the existing `numaflow-pb` crate, which already contains
generated tonic client stubs for every service needed (`clients::{map, sourcetransformer,
reduce, sessionreduce, accumulator, sink, source, sideinput}`).

**Dependencies**: `clap` (derive; subcommands, `ArgGroup` for the `--tcp` XOR `--socket`
and payload-source exclusivity), `tonic`/`prost`/`prost-types`, `tokio`, `serde` +
`serde_yaml` (multi-doc via `Deserializer::from_str` iterator, `deny_unknown_fields`),
`base64`, `humantime` (durations), `jiff` or `chrono` (RFC3339 + epoch-aligned window
math), `serde_json` (json output).

**Not** a dependency: `numaflow-core` (heavy, internal). Two small pieces get ported from
it instead: the UDS connector (~30 lines from `shared/grpc.rs`) and the window-assignment
logic (aligned fixed/sliding from `reduce/reducer/aligned`, session/accumulator windowers
from `reduce/reducer/unaligned`), with unit tests asserting parity.

**Milestones** (each independently verifiable):

1. **Scaffolding + transport**: clap CLI skeleton with all subcommands stubbed; UDS/TCP
   channel builder; `IsReady` retry loop; `ready` subcommand working end-to-end.
   *Verify*: `nfcli ready map --tcp 50051` against a Go/Java example UDF.
2. **Input layer**: YAML multi-doc parser (payload variants, relative times, `repeat`,
   strict unknown-field errors), inline-flag message builder, batching iterator.
   *Verify*: unit tests incl. multi-line/binary payloads and error cases.
3. **Id-correlated types**: `map` (unary → batch → stream), `transform`, `sink`.
   *Verify*: against `numaflow-go` example UDFs over UDS and a `numaflow-java` example in
   local mode over TCP (`isLocal=true`, port 50051).
4. **Client-driven types**: `source`, `side-input`.
   *Verify*: against the SDK example simple-source; ack/pending counts move as expected.
5. **Reduce family**: aligned windower + `reduce` (fixed, then sliding), `session-reduce`,
   `accumulator`. *Verify*: window-assignment unit tests + e2e against SDK example
   reducers (counter, session counter, stream sorter).
6. **Polish**: `json`/`raw` output, exit codes, `--verbose` wire tracing, README, shell
   completions; e2e test script that spins up SDK example servers and runs the full matrix.

**Testing note**: the Go SDK binds UDS on macOS fine (kqueue), so local dev testing works
for both `--socket` (Go examples) and `--tcp` (Java examples, default port 50051).

---

## 11. Open questions / future work

- **Assertions** (`--expect-*` flags or an expected-output YAML) to turn `nfcli` runs into
  self-contained CI tests for UDFs. The JSONL output already enables golden-file testing.
- **`nack_options` round-trip testing** for sources (`NackFn`) once SDK support settles.
