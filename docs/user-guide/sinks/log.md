# Log Sink

A `Log` sink is very useful for debugging, it prints all the received messages to `stdout`.

```yaml
spec:
  vertices:
    - name: output
      sink:
        log: {}
```
