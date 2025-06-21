# Built-in Functions [Deprecated]

  > ⚠️ Note: Builtins for transformer are deprecated and will be removed with 1.6 release.

Numaflow provides some built-in source data transformers that can be used directly.

**Filter**

A `filter` built-in transformer filters the message based on expression. `payload` keyword represents message object.
see documentation for filter expression [here](filter.md#expression)

```yaml
spec:
  vertices:
    - name: in
      source:
        http: {}
        transformer:
          builtin:
            name: filter
            kwargs:
              expression: int(json(payload).id) < 100
```

**Event Time Extractor**

A `eventTimeExtractor` built-in transformer extracts event time from the payload of the message, based on expression and user-specified format. `payload` keyword represents message object.
see documentation for event time extractor expression [here](event-time-extractor.md#expression).

If you want to handle event times in epoch format, you can find helpful resource [here](event-time-extractor.md#epoch-format).

```yaml
spec:
  vertices:
    - name: in
      source:
        http: {}
        transformer:
          builtin:
            name: eventTimeExtractor
            kwargs:
              expression: json(payload).item[0].time
              format: 2006-01-02T15:04:05Z07:00
```

**Time Extraction Filter**

A `timeExtractionFilter` implements both the `eventTimeExtractor` and `filter` built-in functions. It evaluates a message on a pipeline and if valid, extracts event time from the payload of the messsage.

```yaml
spec:
  vertices:
    - name: in
      source:
        http: {}
        transformer:
          builtin:
            name: timeExtractionFilter
            kwargs:
              filterExpr: int(json(payload).id) < 100
              eventTimeExpr: json(payload).item[1].time
              eventTimeFormat: 2006-01-02T15:04:05Z07:00
```
