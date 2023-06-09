# Time Extraction Filter

A `timeExtractionFilter` built-in transformer implements both the `eventTimeExtractor` and `filter` built-in functions. It evaluates a message on a pipeline and if valid, extracts event time from the payload of the messsage.

`filterExpr`

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

### When specified
When `format` is specified, the native [time.Parse(layout, value string)](https://pkg.go.dev/time#Parse) library is used to make the conversion. In this process, the `format` parameter is passed as the layout input to the time.Parse() function, while the event time string is passed as the value parameter.

### When not specified
When `format` is not specified, the extractor uses [dateparse](https://github.com/araddon/dateparse) to parse the event time string without knowing the format in advance.

### How to specify format
Please refer to [golang format library](https://cs.opensource.google/go/go/+/refs/tags/go1.19.5:src/time/format.go).

### Error Scenarios
When encountering parsing errors, event time extractor skips the extraction and passes on the message without modifying the original input message event time. Errors can occur for a variety of reasons, including:

1. `format` is specified but the event time string can't parse to the specified format.
1. `format` is not specified but dataparse can't convert the event time string to a `time.Time` object.

### Ambiguous event time strings
Event time strings can be ambiguous when it comes to date format, such as MM/DD/YYYY versus DD/MM/YYYY. When using such format, you're required to explicitly specify `format`, to avoid confusion.
If no format is provided, event time extractor treats ambiguous event time strings as an error scenario.

### Epoch format
If the event time string in your message payload is in epoch format, you can skip specifying a `format`. You can rely on `dateparse` to recognize a wide range of epoch timestamp formats, including Unix seconds, milliseconds, microseconds, and nanoseconds.

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
              format: 2006-01-02T15:04:05Z07:00
```