# Conditional Forwarding

After processing the data, conditional forwarding is doable based on the `Tags` returned in the result. 
Below is a list of different logic operations that can be done on tags.
- **and** - forwards the message if all the tags specified are present in Message's tags.
- **or** - forwards the message if one of the tags specified is present in Message's tags.
- **not** - forwards the message if all the tags specified are not present in Message's tags.

For example, there's a UDF used to process numbers, and forward the result to different vertices based on the number is even or odd. In this case, you can set the `tag` to `even-tag` or `odd-tag` in each of the returned messages,
and define the edges as below:

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

