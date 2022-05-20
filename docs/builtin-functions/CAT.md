# Cat

A `cat` builtin function does nothing but return the same messages it receives.

```yaml
spec:
  vertices:
    - name: cat-vertex
      udf:
        builtin:
          name: cat
```
