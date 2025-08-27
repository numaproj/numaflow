# Filter [Deprecated]

  > ⚠️ Note: Builtins for transformer are deprecated. An example in Go for filter can be found [here.](https://github.com/numaproj/numaflow-go/tree/main/examples/sourcetransformer/filter) 

A `filter` is a special-purpose built-in function. It is used to evaluate on each message in a pipeline and
is often used to filter the number of messages that are passed to next vertices.

Filter function supports comprehensive expression language which extends flexibility write complex expressions.

`payload` will be root element to represent the message object in expression.

## Expression

Filter expression implemented with `expr` and `sprig` libraries.

### Data conversion functions

These function can be accessed directly in expression.

- `json` - Convert payload in JSON object. e.g: `json(payload)`
- `int` - Convert element/payload into `int` value. e.g: `int(json(payload).id)`
- `string` - Convert element/payload into `string` value. e.g: `string(json(payload).amount)`

### Sprig functions

`Sprig` library has 70+ functions. `sprig` prefix need to be added to access the sprig functions.

[sprig functions](http://masterminds.github.io/sprig/)

E.g:

- `sprig.contains('James', json(payload).name)` # `James` is contained in the value of `name`.
- `int(json(sprig.b64dec(payload)).id) < 100`

## Filter Spec

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
