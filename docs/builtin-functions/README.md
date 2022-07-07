# Builtin Functions

Numaflow provides some builtin functions that can be used directly.

**Cat**

A `cat` builtin UDF does nothing but return the same messages it receives, it is very useful for debugging and testing.

```yaml
spec:
  vertices:
    - name: cat-vertex
      udf:
        builtin:
          name: cat
```

**Filter**

A `filter` builtin UDF does filter the message based on expression. `payload` keyword represents message object.
see documentation for expression [here](FILTER.md#expression)

```yaml
spec:
  vertices:
    - name: filter-vertex
      udf:
        builtin:
          name: filter
          kwargs:
            expression: int(object(payload).id) > 100
```
