# Blackhole Sink

A `Blackhole` sink is where the output is drained without writing to any sink, it is to emulate `/dev/null`.

```yaml
spec:
  vertices:
    - name: output
      sink:
        blackhole: {}
```
