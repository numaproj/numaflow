# Per-Message Nack

> **What it is.** Per-message nack lets your user-defined function (UDF) explicitly tell Numaflow
> "I could not process this message — redeliver it later," optionally with a redelivery **delay**, a
> **max-deliveries** hint, and a human-readable **reason**. It is available in the source, source
> transformer, map, and sink. It is opt-in: a message is nacked only when your code asks for it.

## What is per-message nack

A **nack** ("negative acknowledgement") is the opposite of an ack. When Numaflow acks a message, the
message is considered processed and is removed from the source / inter-step buffer. When a message is
**nacked**, it is *not* removed — it is **redelivered** so it can be processed again.

Per-message nack exposes this at the UDF level. Instead of only succeeding (ack) or hard-failing
(crash), your code can return a *third* outcome for an individual message: "nack this one, and here is
how I'd like it redelivered." The redelivery is governed by an optional `NackOptions`:

| Field            | Type     | Meaning                                                                 |
| ---------------- | -------- | ----------------------------------------------------------------------- |
| `delay`          | duration (ms) | Wait this long before redelivering the message.                    |
| `maxDeliveries`  | uint32   | Hint for the maximum number of redelivery attempts (see [Caveats](#caveats)). |
| `reason`         | string   | Human-readable reason for the nack; logged for observability.           |

All three fields are **optional**. A nack with no options simply asks for the message to be
redelivered with the backend's default behavior.

## Why we need it

Numaflow has **always** had negative acknowledgement at the platform level: if a vertex fails to
process a message (an error, a crash, an unacked read that times out), the message is not acked and is
redelivered. This is fundamental to Numaflow's at-least-once delivery guarantee.

What was missing is **user control**. That redelivery machinery was entirely internal — there was no
way for your UDF to *intentionally* trigger it for a specific message. Common situations had no clean
answer:

- A sink's downstream (a database, an API) is **temporarily unavailable** and you want Numaflow to
  retry the message in a few seconds rather than drop it or crash the pod.
- A message **isn't ready** to be processed yet (a dependency hasn't arrived) and you want to defer it.
- You want a transient error to result in **redelivery with backoff** instead of a hard failure.

Per-message nack surfaces the platform's existing redelivery capability to user code, and adds
per-message control over *when* (delay) and *how often* (max-deliveries) a message comes back.

<!-- TODO: add python SDK examples -->

## NackOptions

`NackOptions` is the same shape across all SDKs (field names follow each language's conventions):

=== "Go"

    ```go
    // pkg/sinker, pkg/mapper, pkg/sourcetransformer, pkg/sourcer all expose NackOptions
    opts := &sinker.NackOptions{
        Delay:         ptr(uint64(5000)), // 5s, *uint64
        MaxDeliveries: ptr(uint32(3)),    // *uint32
        Reason:        ptr("downstream temporarily unavailable"), // *string
    }
    ```

=== "Java"

    ```java
    import io.numaproj.numaflow.shared.NackOptions;

    NackOptions opts = NackOptions.newBuilder()
            .delay(5000L)                                  // ms
            .maxDeliveries(3)
            .reason("downstream temporarily unavailable")
            .build();
    ```

=== "Rust"

    ```rust
    use numaflow::shared::NackOptions;

    let opts = NackOptions {
        delay: Some(5000),          // ms
        max_deliveries: Some(3),
        reason: Some("downstream temporarily unavailable".into()),
    };
    ```

> **SDK availability.** Per-message nack is available in the **Rust**, **Go**, and **Java** SDKs.
> All fields are optional — pass "no options" (`nil` / `null` / `None`) for a plain redelivery.

## How to use it

The mechanism differs slightly by component: the **sink**, **map**, and **source transformer**
*produce* a nack (they tell Numaflow a message should be redelivered), while a **user-defined source**
*receives* a nack (Numaflow calls your source so it can redeliver).

### Sink

Return a **nack response** for the message instead of a success/failure response.

=== "Go"

    ```go
    func (s *mySink) Sink(ctx context.Context, datumCh <-chan sinker.Datum) sinker.Responses {
        responses := sinker.ResponsesBuilder()
        for d := range datumCh {
            if err := s.write(d); err != nil && isRetryable(err) {
                // ask Numaflow to redeliver this message after 5s
                responses = responses.Append(sinker.ResponseNack(d.ID(),
                    &sinker.NackOptions{Delay: ptr(uint64(5000)), Reason: ptr(err.Error())}))
            } else {
                responses = responses.Append(sinker.ResponseOK(d.ID()))
            }
        }
        return responses
    }
    ```

=== "Java"

    ```java
    @Override
    public ResponseList processMessages(DatumIterator datums) {
        ResponseList.ResponseListBuilder builder = ResponseList.newBuilder();
        Datum d;
        while ((d = datums.next()) != null) {
            if (writeFailedRetryably(d)) {
                builder.addResponse(Response.responseNack(d.getId(),
                        NackOptions.newBuilder().delay(5000L).reason("downstream unavailable").build()));
            } else {
                builder.addResponse(Response.responseOK(d.getId()));
            }
        }
        return builder.build();
    }
    ```

=== "Rust"

    ```rust
    async fn sink(&self, mut input: Receiver<SinkRequest>) -> Vec<Response> {
        let mut responses = Vec::new();
        while let Some(req) = input.recv().await {
            if self.write(&req).await.is_err() {
                responses.push(Response::nack(req.id, Some(NackOptions {
                    delay: Some(5000),
                    reason: Some("downstream unavailable".into()),
                    ..Default::default()
                })));
            } else {
                responses.push(Response::ok(req.id));
            }
        }
        responses
    }
    ```

### Map

Return a special **nack message** in place of the normal output. This signals that the *input*
message should be redelivered (see the [whole-message caveat](#caveats)).

=== "Go"

    ```go
    func (m *myMapper) Map(ctx context.Context, keys []string, d mapper.Datum) mapper.Messages {
        if !ready(d) {
            return mapper.MessagesBuilder().Append(
                mapper.MessageToNack(&mapper.NackOptions{Delay: ptr(uint64(2000))}))
        }
        return mapper.MessagesBuilder().Append(mapper.NewMessage(transform(d.Value())))
    }
    ```

=== "Java"

    ```java
    @Override
    public MessageList processMessage(String[] keys, Datum d) {
        if (!ready(d)) {
            return MessageList.newBuilder()
                    .addMessage(Message.toNack(NackOptions.newBuilder().delay(2000L).build()))
                    .build();
        }
        return MessageList.newBuilder().addMessage(new Message(transform(d.getValue()))).build();
    }
    ```

=== "Rust"

    ```rust
    async fn map(&self, input: MapRequest) -> Vec<Message> {
        if !ready(&input) {
            return vec![Message::message_to_nack(Some(NackOptions { delay: Some(2000), ..Default::default() }))];
        }
        vec![Message::new(transform(input.value))]
    }
    ```

### Source transformer

Same as map, but the nack message also carries an **event time** (the transformer is responsible for
event-time assignment, and the message still counts toward watermark progression).

=== "Go"

    ```go
    func (t *myTransformer) Transform(ctx context.Context, keys []string, d sourcetransformer.Datum) sourcetransformer.Messages {
        if !ready(d) {
            return sourcetransformer.MessagesBuilder().Append(
                sourcetransformer.MessageToNack(d.EventTime(), &sourcetransformer.NackOptions{Delay: ptr(uint64(2000))}))
        }
        // ... normal transform ...
    }
    ```

=== "Java"

    ```java
    @Override
    public MessageList processMessage(String[] keys, Datum d) {
        if (!ready(d)) {
            return MessageList.newBuilder()
                    .addMessage(Message.toNack(d.getEventTime(),
                            NackOptions.newBuilder().delay(2000L).build()))
                    .build();
        }
        // ... normal transform ...
    }
    ```

=== "Rust"

    ```rust
    async fn transform(&self, input: SourceTransformRequest) -> Vec<Message> {
        if !ready(&input) {
            return vec![Message::message_to_nack(input.eventtime, Some(NackOptions { delay: Some(2000), ..Default::default() }))];
        }
        // ... normal transform ...
    }
    ```

### Source (user-defined)

A user-defined source *receives* nacks: when a message it produced is nacked downstream, Numaflow
calls the source's **nack handler** with the offsets to redeliver and the `NackOptions`. Implement it
to re-queue those offsets so a subsequent `Read` returns them again (honoring `delay` if you wish).
Built-in sources (Kafka, JetStream, Pulsar, etc.) redeliver natively and need no user code.

=== "Go"

    ```go
    func (s *mySource) Nack(ctx context.Context, req sourcer.NackRequest) {
        opts := req.NackOptions() // may be nil; carries Delay / MaxDeliveries / Reason
        for _, offset := range req.Offsets() {
            s.requeue(offset, opts) // make the next Read() return this offset again
        }
    }
    ```

=== "Java"

    ```java
    @Override
    public void nack(NackRequest request) {
        NackOptions opts = request.getNackOptions(); // may be null
        for (Offset offset : request.getOffsets()) {
            requeue(offset, opts);
        }
    }
    ```

=== "Rust"

    ```rust
    async fn nack(&self, offsets: Vec<Offset>, nack_options: Option<NackOptions>) {
        for offset in offsets {
            self.requeue(offset, &nack_options); // re-deliver on a later read()
        }
    }
    ```

## Caveats

- **Nacking is per *input* message, not per *output* message.** A map or source transformer can
  return many output messages for a single input. Nack is a property of the **input** message, so
  nacking *any* output (or returning a single `MessageToNack`) causes the **entire input message to
  be redelivered** — the UDF will be invoked again on that whole input and will re-emit *all* of its
  outputs. Design your UDF so that re-processing a redelivered input is safe (idempotent), and so it
  deterministically decides whether to nack again. You cannot nack just one element of a fan-out and
  keep the others.

- **Redelivery is at-least-once, not transactional.** A nack does not roll anything back. Outputs that
  were already forwarded, or sink writes that already succeeded for *other* messages in the batch, are
  not undone. Redelivered messages may produce duplicates downstream; consumers should tolerate them.

- **`delay`/`maxDeliveries` honoring is backend-dependent.** Between vertices, redelivery goes through 
  the inter-step buffer; the JetStream ISB honors the `delay` before redelivering. For a user-defined 
  source, `delay`/`maxDeliveries` is passed to your nack handler and it is up to you to act on it. 
  Built-in sources redeliver using their native mechanism and may not apply a per-message delay.

- **`reason` is informational.** It is logged for observability and does not affect routing or
  redelivery behavior.

## Appendix: how nack works internally

**Signaling.** Numaflow needs a uniform way for a UDF to mark a single message as "nack" over the
wire. The mechanism varies by component but converges on one internal disposition:

- **Sink** responses carry an explicit status. A nack response uses the sink status `NACK`, with the
  `NackOptions` attached to that response.
- **Map / source transformer** outputs reuse the same tag-based mechanism as the built-in *drop*
  signal: a nack message is an output tagged with an internal magic tag (`U+005C__NACK__`) and
  carrying the `NackOptions`. The runtime recognizes the tag and treats the message as a nack of the
  corresponding input rather than as data to forward.
- **Source** nacks are delivered to the user-defined source via a dedicated `Nack` gRPC call carrying
  the offsets and `NackOptions`.

**Translation to the input message.** Whatever the surface, the runtime converts the signal into a
negative-ack of the **input** message that produced it (internally `ReadAck::Nak(options)`). This is
why nack is per-input-message: the unit being acked or nacked is always the message that was *read*,
not the individual outputs of a UDF.

**Redelivery paths.** The negative-ack is honored by whichever layer the message was read from:

- **Inter-step buffer (JetStream).** The buffer reader issues a negative acknowledgement with the
  delay (`AckKind::Nak(delay)`). JetStream then redelivers the message to the vertex after the delay
  elapses. This is the path used between vertices in a pipeline.
- **Source.** The source acker negatively-acknowledges the offsets. Offsets are grouped by their
  distinct `NackOptions` so each group is nacked in a single call. For a user-defined source this
  invokes your `Nack` handler; for built-in sources it triggers the source's native redelivery. This
  is the path used at the source vertex and in a MonoVertex (which has no inter-step buffer).

**Relationship to the platform's existing nack.** None of this is a new delivery guarantee — it is the
same redelivery machinery that already underpinned Numaflow's at-least-once semantics (an unacked read
is redelivered). Per-message nack simply gives user code a first-class, parameterized way to trigger
it for a chosen message.
