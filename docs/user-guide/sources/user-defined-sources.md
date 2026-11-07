# User-defined Sources

A `Pipeline` may have multiple Sources, those sources could either be a pre-defined source such as `kafka`, `http`, etc., or a `user-defined source`.

With no source data transformer, A pre-defined source vertex runs single-container pods; a user-defined source runs two-container pods.

## Build Your Own User-defined Sources

You can build your own user-defined sources in multiple languages.

A user-defined source implements a small set of handlers: `read` sends up to the requested number of messages,
`ack` and `nack` acknowledge (or negatively acknowledge) the offsets of those messages, `pending` returns the
number of messages yet to be processed, and `active_partitions` returns the partitions the source reads from.

Check the snippets below to see the handlers for different languages, and follow the links for the full examples.

=== "Go"

    ```go
    func (s *SimpleSource) Read(_ context.Context, readRequest sourcesdk.ReadRequest, messageCh chan<- sourcesdk.Message) {
        // Send up to readRequest.Count() messages, returning early once readRequest.TimeOut() elapses.
        messageCh <- sourcesdk.NewMessage(
            []byte(strconv.FormatInt(s.readIdx, 10)),
            sourcesdk.NewOffsetWithDefaultPartitionId(serializeOffset(s.readIdx)),
            time.Now())
        // ...
    }

    func (s *SimpleSource) Ack(_ context.Context, request sourcesdk.AckRequest)   { /* ... */ }
    func (s *SimpleSource) Nack(_ context.Context, request sourcesdk.NackRequest) { /* ... */ }
    func (s *SimpleSource) Pending(_ context.Context) int64                       { /* ... */ }
    func (s *SimpleSource) ActivePartitions(_ context.Context) []int32 {
        return sourcesdk.DefaultPartitions()
    }
    func (s *SimpleSource) TotalPartitions(_ context.Context) *int32 { return nil }
    ```
    [View the full example on numaflow-go GitHub](https://github.com/numaproj/numaflow-go/tree/main/examples/sourcer/simple_source/)

=== "Python"

    ```python
    class AsyncSource(Sourcer):
        async def read_handler(self, datum: ReadRequest, output: NonBlockingIterator):
            # Send up to datum.num_records messages.
            await output.put(
                Message(
                    payload=str(self.read_idx).encode(),
                    offset=Offset.offset_with_default_partition_id(str(idx).encode()),
                    event_time=datetime.now(),
                )
            )
            # ...

        async def ack_handler(self, ack_request: AckRequest): ...
        async def nack_handler(self, ack_request: NackRequest): ...
        async def pending_handler(self) -> PendingResponse: ...

        async def active_partitions_handler(self) -> PartitionsResponse:
            return PartitionsResponse(partitions=get_default_partitions())
    ```
    [View the full example on numaflow-python GitHub](https://github.com/numaproj/numaflow-python/tree/main/packages/pynumaflow/examples/source/simple_source)

=== "Java"

    ```java
    public class SimpleSource extends Sourcer {
        @Override
        public void read(ReadRequest request, OutputObserver observer) {
            // Send up to request.getCount() messages, returning early once request.getTimeout() elapses.
            observer.send(constructMessage(index));
            // ...
        }

        @Override
        public void ack(AckRequest request) { /* ... */ }

        @Override
        public void nack(NackRequest request) { /* ... */ }

        @Override
        public long getPending() { /* ... */ }

        @Override
        public List<Integer> getActivePartitions() {
            return Sourcer.defaultPartitions();
        }
    }
    ```
    [View the full example on numaflow-java GitHub](https://github.com/numaproj/numaflow-java/tree/main/examples/src/main/java/io/numaproj/numaflow/examples/source/simple/)

=== "Rust"

    ```rust
    #[tonic::async_trait]
    impl Sourcer for SimpleSource {
        async fn read(&self, request: SourceReadRequest, transmitter: Sender<Message>) {
            // Send up to request.count messages.
            transmitter
                .send(self.create_message(offset.clone()).await)
                .await
                .unwrap();
            // ...
        }

        async fn ack(&self, offset: Vec<Offset>) { /* ... */ }
        async fn nack(&self, offset: Vec<NackOffset>) { /* ... */ }
        async fn pending(&self) -> Option<usize> { /* ... */ }

        async fn active_partitions(&self) -> Option<Vec<i32>> {
            Some(vec![0])
        }
    }
    ```
    [View the full example on numaflow-rs GitHub](https://github.com/numaproj/numaflow-rs/tree/main/examples/simple-source)

After building a docker image for the written user-defined source, specify the image as below in the vertex spec.

```yaml
spec:
  vertices:
    - name: input
      source:
        udsource:
          container:
            image: my-source:latest
```

## Available Environment Variables

Some environment variables are available in the user-defined source container:

- `NUMAFLOW_NAMESPACE` - Namespace.
- `NUMAFLOW_POD` - Pod name.
- `NUMAFLOW_REPLICA` - Replica index.
- `NUMAFLOW_PIPELINE_NAME` - Name of the pipeline.
- `NUMAFLOW_VERTEX_NAME` - Name of the vertex.

## User-defined sources contributed from the open source community

If you're looking for examples and usages contributed by the open source community, head over to [the numaproj-contrib repositories](https://github.com/orgs/numaproj-contrib/repositories).

These user-defined sources like AWS SQS, GCP Pub/Sub, provide valuable insights and guidance on how to use and write a user-defined source.
