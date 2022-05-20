# User Defined Sinks

A `Pipleline` may have multiple Sinks, those sinks could either be a pre-defined sink such as `kafka`, `log`, etc, or a `User Defined Sink`.

A pre-defined sink vertex runs single-container pods, a user defined sink runs two-container pods.

A user defined sink vertex looks like below.

```yaml
spec:
  vertices:
    - name: output
      sink:
        udsink:
          container:
            image: my-sink:latest
```
