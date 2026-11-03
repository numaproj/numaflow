# Sliding

Sliding windows are similar to Fixed windows, the size of the windows is measured in time and is fixed.
The important difference from the Fixed window is the fact that it allows an element to be present in
more than one window. The additional window slide parameter controls how frequently a sliding window
is started. Hence, sliding windows will be overlapping and the slide should be smaller than the window
length.

![plot](../../../../assets/sliding.png)

## Configuration

```yaml
vertices:
  - name: my-udf
    udf:
      groupBy:
        window:
          sliding:
            length: duration
            slide: duration
```

NOTE: A duration string is a possibly signed sequence of decimal numbers, each with optional fraction
and a unit suffix, such as "300ms", "1.5h" or "2h45m". Valid time units are "ns", "us" (or "µs"), "ms", "s", "m", "h".

### Length

The `length` is the window size of the fixed window.

### Slide

`slide` is the slide parameter that controls the frequency at which the sliding window is created.

## Example

To create a sliding window of length 1 minute which slides every 10 seconds, we can use the following snippet.

```yaml
vertices:
  - name: my-udf
    udf:
      groupBy:
        window:
          sliding:
            length: 60s
            slide: 10s
```

The yaml snippet above contains an example spec of a _reduce_ vertex that uses sliding window aggregation. As we can see, 
the length of the window is 60s and sliding frequency is once every 10s. This means there will be multiple windows
active at any point in time. 

Let's say, `time.now()` in the pipeline is `2031-09-29T18:46:30Z` the active window boundaries will be as follows (there
are total of 6 windows `60s/10s`)

```text
[2031-09-29T18:45:40Z, 2031-09-29T18:46:40Z)
[2031-09-29T18:45:50Z, 2031-09-29T18:46:50Z) # notice the 10 sec shift from the above window
[2031-09-29T18:46:00Z, 2031-09-29T18:47:00Z)
[2031-09-29T18:46:10Z, 2031-09-29T18:47:10Z)
[2031-09-29T18:46:20Z, 2031-09-29T18:47:20Z)
[2031-09-29T18:46:30Z, 2031-09-29T18:47:30Z)
```

The window start time is always be left inclusive and right exclusive. That is why `[2031-09-29T18:45:30Z, 2031-09-29T18:46:30Z)` 
window is not considered active (it fell on the previous window, right exclusive) but `[2031-09-29T18:46:30Z, 2031-09-29T18:47:30Z)` 
is an active (left inclusive).

The first window always ends after the sliding seconds from the `time.Now()`, the start time of the window will be the 
nearest integer multiple of the slide which is less than the message's event time. So the first window starts in the 
past and ends _sliding_duration (based on time progression in the pipeline and not the wall time) from present. It is 
important to note that regardless of the window boundary (starting in the past or ending in the future) the target element 
set totally depends on the matching time (in case of event time, all the elements with the time that falls with in the 
boundaries of the window, and in case of system time, all the elements that arrive from the `present` until the end of 
window `present + sliding`)

From the point above, it follows then that immediately upon startup, for the first window, fewer elements may get 
aggregated depending on the current _lateness_ of the data stream.

Check the snippets below to see the UDF examples for different languages.

=== "Go"

    ```go
    func reduceCounter(_ context.Context, keys []string, inputCh <-chan reducer.Datum, md reducer.Metadata) reducer.Messages {
        // count the incoming events
        var resultKeys = keys
        var resultVal []byte
        var counter = 0
        for range inputCh {
            counter++
        }
        resultVal = []byte(strconv.Itoa(counter))
        return reducer.MessagesBuilder().Append(reducer.NewMessage(resultVal).WithKeys(resultKeys))
    }
    ```
    [View the full example on numaflow-go GitHub](https://github.com/numaproj/numaflow-go/tree/main/examples/reducer/counter)

=== "Python"

    ```python
    async def reduce_handler(keys: list[str], datums: AsyncIterable[Datum], md: Metadata) -> Messages:
        interval_window = md.interval_window
        counter = 0
        async for _ in datums:
            counter += 1
        msg = (
            f"counter:{counter} interval_window_start:{interval_window.start} "
            f"interval_window_end:{interval_window.end}"
        )
        return Messages(Message(str.encode(msg), keys=keys))
    ```
    [View the full example on numaflow-python GitHub](https://github.com/numaproj/numaflow-python/tree/main/packages/pynumaflow/examples/reduce/counter)

=== "Java"

    ```java
    public class SumFunction extends Reducer {
        private int sum = 0;

        @Override
        public void addMessage(String[] keys, Datum datum, Metadata md) {
            try {
                sum += Integer.parseInt(new String(datum.getValue()));
            } catch (NumberFormatException e) {
                log.info("error while parsing integer - {}", e.getMessage());
            }
        }

        @Override
        public MessageList getOutput(String[] keys, Metadata md) {
            return MessageList
                    .newBuilder()
                    .addMessage(new Message(String.valueOf(sum).getBytes()))
                    .build();
        }
    }
    ```
    [View the full example on numaflow-java GitHub](https://github.com/numaproj/numaflow-java/tree/main/examples/src/main/java/io/numaproj/numaflow/examples/reduce/sum)

=== "Rust"

    ```rust
    #[async_trait]
    impl Reducer for Counter {
        async fn reduce(
            &self,
            keys: Vec<String>,
            mut input: Receiver<ReduceRequest>,
            _md: &Metadata,
        ) -> Vec<Message> {
            let mut counter = 0;
            while input.recv().await.is_some() {
                counter += 1;
            }
            vec![Message::new(counter.to_string().into_bytes()).with_keys(keys.clone())]
        }
    }
    ```
    [View the full example on numaflow-rs GitHub](https://github.com/numaproj/numaflow-rs/tree/main/examples/reduce-counter)


## Streaming Mode

Reduce can be enabled on streaming mode to stream messages or forward partial responses to the next vertex.
This is useful for custom triggering, where we want to forward responses to the next vertex quickly,
even before the fixed window closes. The close-of-book and a final triggering will still happen even if
partial results have been emitted.


To enable reduce streaming, set the `streaming` flag to `true` in the sliding window configuration.

```yaml
vertices:
  - name: my-udf
    udf:
      groupBy:
        window:
          sliding:
            length: duration
            slide: duration
            streaming: true # set streaming to true to enable reduce streamer
```

Note: UDFs should use the ReduceStreamer functionality in the SDKs to use this feature.

Check the snippets below to see the UDF examples in streaming mode for different languages.

=== "Go"

    ```go
    // reduceCounter is a ReduceStreamer that counts the incoming events and outputs the count every 10 events.
    func reduceCounter(_ context.Context, keys []string, inputCh <-chan reducestreamer.Datum, outputCh chan<- reducestreamer.Message, md reducestreamer.Metadata) {
        var resultKeys = keys
        var resultVal []byte
        var counter = 0
        for range inputCh {
            counter++
            if counter >= 10 {
                resultVal = []byte(strconv.Itoa(counter))
                outputCh <- reducestreamer.NewMessage(resultVal).WithKeys(resultKeys)
                counter = 0
            }
        }
        resultVal = []byte(strconv.Itoa(counter))
        outputCh <- reducestreamer.NewMessage(resultVal).WithKeys(resultKeys)
    }
    ```
    [View the full example on numaflow-go GitHub](https://github.com/numaproj/numaflow-go/tree/main/examples/reducestreamer/counter)

=== "Python"

    ```python
    class ReduceCounter(ReduceStreamer):
        def __init__(self, counter):
            self.counter = counter

        async def handler(
            self,
            keys: list[str],
            datums: AsyncIterable[Datum],
            output: NonBlockingIterator,
            md: Metadata,
        ):
            async for _ in datums:
                self.counter += 1
                if self.counter > 10:
                    msg = f"counter:{self.counter}"
                    await output.put(Message(str.encode(msg), keys=keys))
                    self.counter = 0
            msg = f"counter:{self.counter}"
            await output.put(Message(str.encode(msg), keys=keys))
    ```
    [View the full example on numaflow-python GitHub](https://github.com/numaproj/numaflow-python/tree/main/packages/pynumaflow/examples/reducestream/counter)

=== "Java"

    ```java
    public class SumFunction extends ReduceStreamer {
        private int sum = 0;

        @Override
        public void processMessage(
                String[] keys,
                Datum datum,
                OutputStreamObserver outputStreamObserver,
                Metadata md) {
            try {
                sum += Integer.parseInt(new String(datum.getValue()));
            } catch (NumberFormatException e) {
                log.info("error while parsing integer - {}", e.getMessage());
            }
            if (sum >= 100) {
                outputStreamObserver.send(new Message(String.valueOf(sum).getBytes(), keys));
                sum = 0;
            }
        }

        @Override
        public void handleEndOfStream(
                String[] keys,
                OutputStreamObserver outputStreamObserver,
                Metadata md) {
            outputStreamObserver.send(new Message(String.valueOf(sum).getBytes()));
        }
    }
    ```
    [View the full example on numaflow-java GitHub](https://github.com/numaproj/numaflow-java/tree/main/examples/src/main/java/io/numaproj/numaflow/examples/reducestreamer/sum)

=== "Rust"

    ```rust
    #[async_trait]
    impl ReduceStreamer for StreamingCounter {
        async fn reducestream(
            &self,
            keys: Vec<String>,
            mut input: Receiver<ReduceStreamRequest>,
            output: Sender<Message>,
            _md: &Metadata,
        ) {
            let mut counter = 0;
            // Stream intermediate results every 10 messages
            while input.recv().await.is_some() {
                counter += 1;
                if counter % 10 == 0 {
                    let message = Message::new(format!("intermediate: {}", counter).into_bytes())
                        .with_keys(keys.clone());
                    if output.send(message).await.is_err() {
                        return;
                    }
                }
            }
            // Send final count when the window closes
            let final_message =
                Message::new(format!("final: {}", counter).into_bytes()).with_keys(keys.clone());
            let _ = output.send(final_message).await;
        }
    }
    ```
    [View the full example on numaflow-rs GitHub](https://github.com/numaproj/numaflow-rs/tree/main/examples/reduce-stream-counter)





