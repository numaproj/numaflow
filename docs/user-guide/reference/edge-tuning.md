# Edge Tuning

## Drop message onFull

We need to have an edge level setting to drop the messages if the `buffer.isFull == true`. Even if the UDF or UDSink drops
a message due to some internal error in the user-defined code, the processing latency will spike up causing a natural 
back pressure. A kill switch to drop messages can help alleviate/avoid any repercussions on the rest of the DAG.

This setting is an edge-level setting and can be enabled by `onFull` and the default is `retryUntilSuccess` (other option
is `discardLatest`).

This is a **data loss scenario** but can be useful in cases where we are doing user-introduced experimentations, 
like A/B testing, on the pipeline. It is totally okay for the experimentation side of the DAG to have data loss while 
the production is unaffected.

### discardLatest

Setting `onFull` to `discardLatest` will drop the message on the floor if the edge is full.

```yaml
  edges:
    - from: a
      to: b
      onFull: discardLatest
```

### retryUntilSuccess

The default setting for `onFull` in `retryUntilSuccess` which will make sure the message is retried until successful. 

```yaml
  edges:
    - from: a
      to: b
      onFull: retryUntilSuccess
```