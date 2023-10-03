# Cat

A `cat` builtin function does nothing but return the same messages it receives, it is very useful for debugging and testing.

```yaml
spec:
  vertices:
    - name: cat-vertex
      udf:
        builtin:
          name: cat
```
