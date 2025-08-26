# Cat [Deprecated]

  > ⚠️ Note: Builtins for UDF are deprecated. An example in Go for `cat` can be found [here.](https://github.com/numaproj/numaflow-go/tree/main/examples/mapper/cat)

A `cat` builtin function does nothing but return the same messages it receives, it is very useful for debugging and testing.

```yaml
spec:
  vertices:
    - name: cat-vertex
      udf:
        builtin:
          name: cat
```
