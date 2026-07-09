# nfcli — Numaflow UDF test CLI

`nfcli` is a standalone, non-interactive CLI that impersonates the numa container and talks
directly to a user-defined-function (UDF) gRPC server, so UDF authors can test their code
without deploying a pipeline. It connects over a unix domain socket (like numa does in k8s)
or over TCP (like the Java SDK's local mode), sends one or more test messages, prints the
UDF's responses, and exits with a meaningful code.

It never reads a `server-info` file — you state the UDF type (subcommand) and, for map, the
mode (`--mode`).

## Build

```bash
cargo build --release -p numaflow-cli   # binary: target/release/nfcli
```

## Connecting

Exactly one of the two is required (both/neither is a usage error):

```
--socket <path>        # unix domain socket, e.g. /var/run/numaflow/map.sock
--tcp <[host:]port>    # plain gRPC over TCP; host defaults to localhost (e.g. 50051)
```

`--socket` mirrors how numa connects in k8s. `--tcp 50051` matches the Java SDK's local mode
(`GRPCConfig...isLocal(true).port(50051)`, default port 50051). IPv6 hosts must be bracketed,
for example `--tcp [::1]:50051`.

## Subcommands

| Subcommand       | UDF type                              | Notes |
|------------------|----------------------------------------|-------|
| `map`            | map (`--mode unary\|batch\|stream`)   | `--mode` replaces server-info `MAP_MODE`; must match how the server was built |
| `transform`      | source transformer                    | prints each result's re-assigned event time |
| `reduce`         | aligned reduce (fixed & sliding)      | `--window fixed --length 60s`, or `--window sliding --length 60s --slide 10s` |
| `session-reduce` | session windows                       | `--gap 10s` |
| `accumulator`    | accumulator                           | echoes payloads (incl. id) as they stream |
| `sink`           | sink (incl. fallback / on-success)    | point `--socket` at the corresponding socket |
| `source`         | user-defined source                   | `--count`, `--rounds`, `--delay`, `--no-ack`, `--pending`, `--partitions` |
| `side-input`     | side input                            | single unary retrieve |
| `ready <type>`   | any service's `IsReady`               | smoke-test / wait for a server to come up |

## Input messages

For quick one-shot tests, give the message inline:

```
--payload <string> | --payload-file <path> | --payload-base64 <string>   # exactly one, required
--key <k>          (repeatable)      --header <k>=<v>   (repeatable)
--event-time <RFC3339|+dur>          --watermark <RFC3339|+dur>          --id <string>
```

For multi-message scenarios, pass a **YAML multi-document stream** with `-f <file>` (or
`-f -` for stdin). File mode provides all message fields, so it cannot be combined with
`--key`, `--header`, `--event-time`, `--watermark`, or `--id`; put those fields in the YAML
documents instead. One document per message, separated by `---`:

```yaml
---
payload: |-
  {"user": "alice", "amount": 30}
keys: [alice]
headers: {source: checkout}
eventTime: "+1s"          # relative to base time
---
payload: '{"user": "bob"}'
keys: [bob]
eventTime: "+20s"
repeat: 2                 # sent twice, auto ids msg-2 / msg-3
---
payloadBase64: "3q2+7w=="  # binary payload (0xDEADBEEF)
keys: [alice]
eventTime: "+70s"
```

Per-document fields: `payload` / `payloadBase64` / `payloadFile` (one required), `keys`,
`headers`, `eventTime`, `watermark`, `id`, `userMetadata`, `previousVertex`, `repeat`.
Unknown fields are a hard error (to catch typos). Message ids must be unique for response
correlation; repeated documents with an explicit `id` use auto ids.

### Time handling

- `--base-time <RFC3339>` anchors relative (`+dur`) times. Default: invocation time — except
  the reduce family, where it defaults to invocation time truncated to the window boundary so
  `+0s..+59s` land in one 60s window.
- Event time defaults to base time; watermark defaults to the message's event time.

## Batching and pacing

```
--batch-size <n>   # messages per batch [default: 500, numa's readBatchSize]
--delay <dur>      # sleep between batches, e.g. 500ms [default: 0s]
```

## Output

```
-o, --output <text|json|raw>   # [default: text]
-v, --verbose                  # wire-level events (handshake, EOT, window ops)
-q, --quiet                    # summary line only
```

Shared flags such as `-v`, `-o`, `-q`, `--timeout`, and `--max-message-size` may be placed
before or after the subcommand.

- `text`: human-readable; payloads shown as UTF-8 when valid, else base64 with a `(base64)`
  marker. Drop/nack sentinel tags render as `DROPPED` / `NACKED(…)` — both the form the SDKs
  emit on the wire (`U+005C__DROP__`, `U+005C__NACK__`) and the literal-backslash form
  (`\__DROP__`, `\__NACK__`) are recognized.
- `json`: one JSON object per line (JSONL) per event — for scripting / golden-file assertions.
- `raw`: response payload bytes only, concatenated to stdout — for piping.

## Exit codes

| Code | Meaning |
|------|---------|
| 0 | completed; no UDF-reported failures |
| 1 | usage error (bad flags, bad file, missing payload, invalid input flags) |
| 2 | connect / `IsReady` / handshake failure within `--timeout` |
| 3 | protocol error (gRPC error mid-stream, partial response at EOT, response timeout) |
| 4 | protocol OK but the UDF reported failures or NACKed messages (e.g. sink `FAILURE`, `\__NACK__` tag) |

## Completions

Generate shell completions with:

```bash
nfcli completions fish | source
```

## Examples

```bash
# Quick unary map test against a Java UDF in local mode (TCP)
nfcli map --tcp 50051 --payload '{"temp_c": 21.5}' --key sensor-1 --header source=test

# Unary map over UDS, many messages, 20 per batch, 500ms between batches
nfcli map --socket /var/run/numaflow/map.sock -f messages.yaml --batch-size 20 --delay 500ms

# Batch / stream map (server must match the mode)
nfcli map --mode batch  --socket /var/run/numaflow/batchmap.sock -f messages.yaml
nfcli map --mode stream --socket /var/run/numaflow/mapstream.sock --payload-file big-doc.json

# Fixed 60s reduce windows
nfcli reduce --socket /var/run/numaflow/reduce.sock -f counts.yaml --window fixed --length 60s

# Sliding windows: 60s length, sliding every 10s
nfcli reduce --socket /var/run/numaflow/reduce.sock -f counts.yaml \
  --window sliding --length 60s --slide 10s

# Session reduce with a 10s inactivity gap
nfcli session-reduce --socket /var/run/numaflow/sessionreduce.sock -f clicks.yaml --gap 10s

# Read 5 records twice from a source, 1s apart, acking each round; show pending counts
nfcli source --socket /var/run/numaflow/source.sock --count 5 --rounds 2 --delay 1s --pending

# Sink; fallback/on-success sinks use the same subcommand pointed at their socket
nfcli sink --socket /var/run/numaflow/fb-sink.sock --payload 'poison-pill' --id order-17

# Wait for a server to come up (scripts)
nfcli ready map --tcp 50051
```

## Tests

```bash
cargo test -p numaflow-cli
```

Unit tests cover time parsing, the YAML input layer, and window-assignment math (asserted
against numaflow-core's own boundary test values). End-to-end tests stand up real UDF servers
via the [numaflow-rs](https://github.com/numaproj/numaflow-rs) SDK over a UDS and run the
compiled binary against them, covering all three map modes, transform, sink (incl. failure →
exit 4), source, side-input, and the full reduce family (fixed/sliding/session/accumulator).
