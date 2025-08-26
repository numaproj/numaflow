# Time Extraction Filter [Deprecated]

  > ⚠️ Note: Builtins for transformer are deprecated. An example in Go for time extraction filter can be found [here.](https://github.com/numaproj/numaflow-go/tree/main/examples/sourcetransformer/time_extraction_filter) 

A `timeExtractionFilter` built-in transformer implements both the `eventTimeExtractor` and `filter` built-in functions. It evaluates a message on a pipeline and if valid, extracts event time from the payload of the messsage.

`filterExpr` is used to evaluate and drop invalid messages.

`eventTimeExpr` is used to compile the payload to a string representation of the event time.

`format` is used to convert the event time in string format to a `time.Time` object.

## Expression (required)

The expressions for the filter and event time extractor are implemented with `expr` and `sprig` libraries.

### Data conversion functions

These function can be accessed directly in expression. `payload` keyword represents the message object. It will be the root element to represent the message object in expression.

- `json` - Convert payload in JSON object. e.g: `json(payload)`
- `int` - Convert element/payload into `int` value. e.g: `int(json(payload).id)`
- `string` - Convert element/payload into `string` value. e.g: `string(json(payload).amount)`

### Sprig functions

`Sprig` library has 70+ functions. `sprig` prefix need to be added to access the sprig functions.

[sprig functions](http://masterminds.github.io/sprig/)

E.g:

- `sprig.trim(string(json(payload).timestamp))` # Remove spaces from either side of the value of field `timestamp`

## Format (optional)

Depending on whether a `format` is specified, the Event Time Extractor uses different approaches to convert the event time string to a `time.Time` object.

## Time Extraction Filter Spec

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
