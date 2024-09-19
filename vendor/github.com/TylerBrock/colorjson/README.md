ColorJSON: The Fast Color JSON Marshaller for Go
================================================
![ColorJSON Output](https://i.imgur.com/pLtCXhb.png)
What is this?
-------------

This package is based heavily on hokaccha/go-prettyjson but has some noticible differences:
 - Over twice as fast (recursive descent serialization uses buffer instead of string concatenation)
   ```
   BenchmarkColorJSONMarshall-4     500000      2498 ns/op
   BenchmarkPrettyJSON-4            200000      6145 ns/op
   ```
 - more customizable (ability to have zero indent, print raw json strings, etc...)
 - better defaults (less bold colors)

ColorJSON was built in order to produce fast beautiful colorized JSON output for [Saw](http://github.com/TylerBrock/saw).

Installation
------------

```sh
go get -u github.com/TylerBrock/colorjson
```

Usage
-----

Setup

```go
import "github.com/TylerBrock/colorjson"

str := `{
  "str": "foo",
  "num": 100,
  "bool": false,
  "null": null,
  "array": ["foo", "bar", "baz"],
  "obj": { "a": 1, "b": 2 }
}`

// Create an intersting JSON object to marshal in a pretty format
var obj map[string]interface{}
json.Unmarshal([]byte(str), &obj)
```

Vanilla Usage

```go
s, _ := colorjson.Marshal(obj)
fmt.Println(string(s))
```

Customization (Custom Indent)
```go
f := colorjson.NewFormatter()
f.Indent = 2

s, _ := f.Marshal(v)
fmt.Println(string(s))
```
