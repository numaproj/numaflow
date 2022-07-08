# Conditional Forwarding

After processing the data, conditional forwarding is doable based on the `Key` returned in the result.

For example, there's a UDF used to process numbers, and forward the result to different vertices based on the number is even or odd. In this case, you can set the `key` to `even` or `odd` in each of the returned messages, and define the edges as below:

```yaml
edges:
  - from: p1
    to: even-vertex
    conditions:
      keyIn:
        - even
  - from: p1
    to: odd-vertex
    conditions:
      keyIn:
        - odd
```

### `U+005C__ALL__`

If the returned `key` is `U+005C__ALL__`, the data will be forwarded to all the connected vertices no matter what kind of conditions is defined in the spec.

### `U+005C__DROP__`

If the returned `key` is `U+005C__DROP__`, the data will **NOT** be forwarded to any of the connected vertices no matter what kind of conditions is defined in the spec.
