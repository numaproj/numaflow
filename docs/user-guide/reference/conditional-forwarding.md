# Conditional Forwarding

Conditional forwarding is supported in both [Pipelines](../../core-concepts/pipeline.md) and [MonoVertex](../../core-concepts/monovertex.md).
After processing, messages can be routed to different destinations based on `tags` returned in the result.

---

## Pipeline

In a [pipeline](../../core-concepts/pipeline.md), after processing the data, conditional forwarding is doable based on the `tags` returned in the result.
Below is a list of different logic operations that can be done on tags.

- **and** - forwards the message if all the tags specified are present in Message's tags.
- **or** - forwards the message if one of the tags specified is present in Message's tags.
- **not** - forwards the message if all the tags specified are not present in Message's tags.

For example, there's a UDF used to process numbers, and forward the result to different vertices based on the number is 
even or odd. In this case, you can set the `tag` to `even-tag` or `odd-tag` in each of the returned messages,
and define the edges as below:

### Default Behavior

* If no `conditions` are specified in the spec, the message will be forwarded to all the downstream vertices (independent
  of the `tags` in the `Messages`).
* In the code, if the `Messages` are not tagged but conditions are configured, we will still honour the edge conditions.

### Syntax

```yaml
edges:
  - from: ...
    to: ...
    conditions:
      tags:
        operator: ...
        values:
          - ...
```

### Example

```yaml
edges:
  - from: p1
    to: even-vertex
    conditions:
      tags:
        operator: or # Optional, defaults to "or".
        values:
          - even-tag
  - from: p1
    to: odd-vertex
    conditions:
      tags:
        operator: not
        values:
          - odd-tag
  - from: p1
    to: all
    conditions:
      tags:
        operator: and
        values:
          - odd-tag
          - even-tag
```

---

## MonoVertex

MonoVertex supports [bypass routing](monovertex-bypass.md), which allows tagging messages so they skip components (e.g. UDF or primary sink)
and are routed directly to a `fallback` or `onSuccess` sink. Conditions follow the same `tags` operator pattern (`and`, `or`, `not`) as pipeline conditional forwarding.

For example, a message flagged as faulty in the transformer can be tagged and sent straight to a DLQ, bypassing the UDF and primary sink entirely.

See [MonoVertex Bypass Routing](monovertex-bypass.md) for the full spec and caveats.
