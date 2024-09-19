package httpexpect

import (
	"errors"
	"reflect"
)

// Value provides methods to inspect attached interface{} object
// (Go representation of arbitrary JSON value) and cast it to
// concrete type.
type Value struct {
	chain *chain
	value interface{}
}

// NewValue returns a new Value instance.
//
// If reporter is nil, the function panics.
// Value may be nil.
//
// Example:
//
//	value := NewValue(t, map[string]interface{}{"foo": 123})
//	value.IsObject()
//
//	value := NewValue(t, []interface{}{"foo", 123})
//	value.IsArray()
//
//	value := NewValue(t, "foo")
//	value.IsString()
//
//	value := NewValue(t, 123)
//	value.IsNumber()
//
//	value := NewValue(t, true)
//	value.IsBoolean()
//
//	value := NewValue(t, nil)
//	value.IsNull()
func NewValue(reporter Reporter, value interface{}) *Value {
	return newValue(newChainWithDefaults("Value()", reporter), value)
}

// NewValueC returns a new Value instance with config.
//
// Requirements for config are same as for WithConfig function.
// Value may be nil.
//
// See NewValue for usage example.
func NewValueC(config Config, value interface{}) *Value {
	return newValue(newChainWithConfig("Value()", config.withDefaults()), value)
}

func newValue(parent *chain, val interface{}) *Value {
	v := &Value{parent.clone(), nil}

	opChain := v.chain.enter("")
	defer opChain.leave()

	if val != nil {
		v.value, _ = canonValue(opChain, val)
	}

	return v
}

// Raw returns underlying value attached to Value.
// This is the value originally passed to NewValue, converted to canonical form.
//
// Example:
//
//	value := NewValue(t, "foo")
//	assert.Equal(t, "foo", number.Raw().(string))
func (v *Value) Raw() interface{} {
	return v.value
}

// Decode unmarshals the underlying value attached to the Object to a target variable
// target should be pointer to any type.
//
// Example:
//
//	type S struct {
//		Foo int             `json:"foo"`
//		Bar []interface{}   `json:"bar"`
//		Baz struct{ A int } `json:"baz"`
//	}
//
//	m := map[string]interface{}{
//		"foo": 123,
//		"bar": []interface{}{"123", 456.0},
//		"baz": struct{ A int }{123},
//	}
//
//	value = NewValue(reporter,m)
//
//	var target S
//	value.Decode(&target)
//
//	assert.Equal(t, S{123, []interface{}{"123", 456.0}, struct{ A int }{123}, target})
func (v *Value) Decode(target interface{}) *Value {
	opChain := v.chain.enter("Decode()")
	defer opChain.leave()

	if opChain.failed() {
		return v
	}

	canonDecode(opChain, v.value, target)
	return v
}

// Alias returns a new Value object with alias.
// When a test of Value object with alias is failed,
// an assertion is displayed as a chain starting from the alias.
//
// Example:
//
//	// In this example, GET /example responds "foo"
//	foo := e.GET("/example").Expect().Status(http.StatusOK).JSON().Object()
//
//	// When a test is failed, an assertion without alias is
//	// Request("GET").Expect().JSON().Object().IsEqual()
//	foo.IsEqual("bar")
//
//	// Set Alias
//	fooWithAlias := e.GET("/example").
//		Expect().
//		Status(http.StatusOK).JSON().Object().Alias("foo")
//
//	// When a test is failed, an assertion with alias is
//	// foo.IsEqual()
//	fooWithAlias.IsEqual("bar")
func (v *Value) Alias(name string) *Value {
	opChain := v.chain.enter("Alias(%q)", name)
	defer opChain.leave()

	v.chain.setAlias(name)
	return v
}

// Path returns a new Value object for child object(s) matching given
// JSONPath expression.
//
// JSONPath is a simple XPath-like query language.
// See http://goessner.net/articles/JsonPath/.
//
// We currently use https://github.com/yalp/jsonpath, which implements
// only a subset of JSONPath, yet useful for simple queries. It doesn't
// support filters and requires double quotes for strings.
//
// Example 1:
//
//	json := `{"users": [{"name": "john"}, {"name": "bob"}]}`
//	value := NewValue(t, json)
//
//	value.Path("$.users[0].name").String().IsEqual("john")
//	value.Path("$.users[1].name").String().IsEqual("bob")
//
// Example 2:
//
//	json := `{"yfGH2a": {"user": "john"}, "f7GsDd": {"user": "john"}}`
//	value := NewValue(t, json)
//
//	for _, user := range value.Path("$..user").Array().Iter() {
//		user.String().IsEqual("john")
//	}
func (v *Value) Path(path string) *Value {
	opChain := v.chain.enter("Path(%q)", path)
	defer opChain.leave()

	return jsonPath(opChain, v.value, path)
}

// Schema succeeds if value matches given JSON Schema.
//
// JSON Schema specifies a JSON-based format to define the structure of
// JSON data. See http://json-schema.org/.
// We use https://github.com/xeipuuv/gojsonschema implementation.
//
// schema should be one of the following:
//   - go value that can be json.Marshal-ed to a valid schema
//   - type convertible to string containing valid schema
//   - type convertible to string containing valid http:// or file:// URI,
//     pointing to reachable and valid schema
//
// Example 1:
//
//	 schema := `{
//	   "type": "object",
//	   "properties": {
//		  "foo": {
//			  "type": "string"
//		  },
//		  "bar": {
//			  "type": "integer"
//		  }
//	  },
//	  "require": ["foo", "bar"]
//	}`
//
//	value := NewValue(t, map[string]interface{}{
//		"foo": "a",
//		"bar": 1,
//	})
//
//	value.Schema(schema)
//
// Example 2:
//
//	value := NewValue(t, data)
//	value.Schema("http://example.com/schema.json")
func (v *Value) Schema(schema interface{}) *Value {
	opChain := v.chain.enter("Schema()")
	defer opChain.leave()

	jsonSchema(opChain, v.value, schema)
	return v
}

// Object returns a new Object attached to underlying value.
//
// If underlying value is not an object (map[string]interface{}), failure is reported
// and empty (but non-nil) value is returned.
//
// Example:
//
//	value := NewValue(t, map[string]interface{}{"foo": 123})
//	value.Object().ContainsKey("foo")
func (v *Value) Object() *Object {
	opChain := v.chain.enter("Object()")
	defer opChain.leave()

	if opChain.failed() {
		return newObject(opChain, nil)
	}

	data, ok := v.value.(map[string]interface{})

	if !ok {
		opChain.fail(AssertionFailure{
			Type:   AssertValid,
			Actual: &AssertionValue{v.value},
			Errors: []error{
				errors.New("expected: value is object"),
			},
		})
		return newObject(opChain, nil)
	}

	return newObject(opChain, data)
}

// Array returns a new Array attached to underlying value.
//
// If underlying value is not an array ([]interface{}), failure is reported and empty
// (but non-nil) value is returned.
//
// Example:
//
//	value := NewValue(t, []interface{}{"foo", 123})
//	value.Array().ConsistsOf("foo", 123)
func (v *Value) Array() *Array {
	opChain := v.chain.enter("Array()")
	defer opChain.leave()

	if opChain.failed() {
		return newArray(opChain, nil)
	}

	data, ok := v.value.([]interface{})

	if !ok {
		opChain.fail(AssertionFailure{
			Type:   AssertValid,
			Actual: &AssertionValue{v.value},
			Errors: []error{
				errors.New("expected: value is array"),
			},
		})
		return newArray(opChain, nil)
	}

	return newArray(opChain, data)
}

// String returns a new String attached to underlying value.
//
// If underlying value is not a string, failure is reported and empty (but non-nil)
// value is returned.
//
// Example:
//
//	value := NewValue(t, "foo")
//	value.String().IsEqualFold("FOO")
func (v *Value) String() *String {
	opChain := v.chain.enter("String()")
	defer opChain.leave()

	if opChain.failed() {
		return newString(opChain, "")
	}

	data, ok := v.value.(string)

	if !ok {
		opChain.fail(AssertionFailure{
			Type:   AssertValid,
			Actual: &AssertionValue{v.value},
			Errors: []error{
				errors.New("expected: value is string"),
			},
		})
		return newString(opChain, "")
	}

	return newString(opChain, data)
}

// Number returns a new Number attached to underlying value.
//
// If underlying value is not a number (numeric type convertible to float64), failure
// is reported and empty (but non-nil) value is returned.
//
// Example:
//
//	value := NewValue(t, 123)
//	value.Number().InRange(100, 200)
func (v *Value) Number() *Number {
	opChain := v.chain.enter("Number()")
	defer opChain.leave()

	if opChain.failed() {
		return newNumber(opChain, 0)
	}

	data, ok := v.value.(float64)

	if !ok {
		opChain.fail(AssertionFailure{
			Type:   AssertValid,
			Actual: &AssertionValue{v.value},
			Errors: []error{
				errors.New("expected: value is number"),
			},
		})
		return newNumber(opChain, 0)
	}

	return newNumber(opChain, data)
}

// Boolean returns a new Boolean attached to underlying value.
//
// If underlying value is not a bool, failure is reported and empty (but non-nil)
// value is returned.
//
// Example:
//
//	value := NewValue(t, true)
//	value.Boolean().IsTrue()
func (v *Value) Boolean() *Boolean {
	opChain := v.chain.enter("Boolean()")
	defer opChain.leave()

	if opChain.failed() {
		return newBoolean(opChain, false)
	}

	data, ok := v.value.(bool)

	if !ok {
		opChain.fail(AssertionFailure{
			Type:   AssertValid,
			Actual: &AssertionValue{v.value},
			Errors: []error{
				errors.New("expected: value is boolean"),
			},
		})
		return newBoolean(opChain, false)
	}

	return newBoolean(opChain, data)
}

// IsNull succeeds if value is nil.
//
// Note that non-nil interface{} that points to nil value (e.g. nil slice or map)
// is also treated as null value. Empty (non-nil) slice or map, empty string, and
// zero number are not treated as null value.
//
// Example:
//
//	value := NewValue(t, nil)
//	value.IsNull()
//
//	value := NewValue(t, []interface{}(nil))
//	value.IsNull()
func (v *Value) IsNull() *Value {
	opChain := v.chain.enter("IsNull()")
	defer opChain.leave()

	if opChain.failed() {
		return v
	}

	if !(v.value == nil) {
		opChain.fail(AssertionFailure{
			Type:   AssertNil,
			Actual: &AssertionValue{v.value},
			Errors: []error{
				errors.New("expected: value is null"),
			},
		})
	}

	return v
}

// NotNull succeeds if value is not nil.
//
// Note that non-nil interface{} that points to nil value (e.g. nil slice or map)
// is also treated as null value. Empty (non-nil) slice or map, empty string, and
// zero number are not treated as null value.
//
// Example:
//
//	value := NewValue(t, "")
//	value.NotNull()
//
//	value := NewValue(t, make([]interface{}, 0))
//	value.NotNull()
func (v *Value) NotNull() *Value {
	opChain := v.chain.enter("NotNull()")
	defer opChain.leave()

	if opChain.failed() {
		return v
	}

	if v.value == nil {
		opChain.fail(AssertionFailure{
			Type:   AssertNotNil,
			Actual: &AssertionValue{v.value},
			Errors: []error{
				errors.New("expected: value is non-null"),
			},
		})
	}

	return v
}

// Deprecated: use IsNull instead.
func (v *Value) Null() *Value {
	return v.IsNull()
}

// IsObject succeeds if the underlying value is an object.
//
// If underlying value is not an object (map[string]interface{}), failure is reported.
//
// Example:
//
//	value := NewValue(t, map[string]interface{}{"foo": 123})
//	value.IsObject()
func (v *Value) IsObject() *Value {
	opChain := v.chain.enter("IsObject()")
	defer opChain.leave()

	if opChain.failed() {
		return v
	}

	if _, ok := v.value.(map[string]interface{}); !ok {
		opChain.fail(AssertionFailure{
			Type:   AssertValid,
			Actual: &AssertionValue{v.value},
			Errors: []error{
				errors.New("expected: value is object"),
			},
		})
	}

	return v
}

// NotObject succeeds if the underlying value is not an object.
//
// If underlying value is an object (map[string]interface{}), failure is reported.
//
// Example:
//
//	value := NewValue(t, nil)
//	value.NotObject()
func (v *Value) NotObject() *Value {
	opChain := v.chain.enter("NotObject()")
	defer opChain.leave()

	if opChain.failed() {
		return v
	}

	if _, ok := v.value.(map[string]interface{}); ok {
		opChain.fail(AssertionFailure{
			Type:   AssertValid,
			Actual: &AssertionValue{v.value},
			Errors: []error{
				errors.New("expected: value is not object"),
			},
		})
	}

	return v
}

// IsArray succeeds if the underlying value is an array.
//
// If underlying value is not an array ([]interface{}), failure is reported.
//
// Example:
//
//	value := NewValue(t, []interface{}{"foo", "123"})
//	value.IsArray()
func (v *Value) IsArray() *Value {
	opChain := v.chain.enter("IsArray()")
	defer opChain.leave()

	if opChain.failed() {
		return v
	}

	if _, ok := v.value.([]interface{}); !ok {
		opChain.fail(AssertionFailure{
			Type:   AssertValid,
			Actual: &AssertionValue{v.value},
			Errors: []error{
				errors.New("expected: value is array"),
			},
		})
	}

	return v
}

// NotArray succeeds if the underlying value is not an array.
//
// If underlying value is an array ([]interface{}), failure is reported.
//
// Example:
//
//	value := NewValue(t, nil)
//	value.NotArray()
func (v *Value) NotArray() *Value {
	opChain := v.chain.enter("NotArray()")
	defer opChain.leave()

	if opChain.failed() {
		return v
	}

	if _, ok := v.value.([]interface{}); ok {
		opChain.fail(AssertionFailure{
			Type:   AssertValid,
			Actual: &AssertionValue{v.value},
			Errors: []error{
				errors.New("expected: value is not array"),
			},
		})
	}

	return v
}

// IsString succeeds if the underlying value is a string.
//
// If underlying value is not a string, failure is reported.
//
// Example:
//
//	value := NewValue(t, "foo")
//	value.IsString()
func (v *Value) IsString() *Value {
	opChain := v.chain.enter("IsString()")
	defer opChain.leave()

	if opChain.failed() {
		return v
	}

	if _, ok := v.value.(string); !ok {
		opChain.fail(AssertionFailure{
			Type:   AssertValid,
			Actual: &AssertionValue{v.value},
			Errors: []error{
				errors.New("expected: value is string"),
			},
		})
	}

	return v
}

// NotString succeeds if the underlying value is not a string.
//
// If underlying value is a string, failure is reported.
//
// Example:
//
//	value := NewValue(t, nil)
//	value.NotString()
func (v *Value) NotString() *Value {
	opChain := v.chain.enter("NotString()")
	defer opChain.leave()

	if opChain.failed() {
		return v
	}

	if _, ok := v.value.(string); ok {
		opChain.fail(AssertionFailure{
			Type:   AssertValid,
			Actual: &AssertionValue{v.value},
			Errors: []error{
				errors.New("expected: value is not string"),
			},
		})
	}

	return v
}

// IsNumber succeeds if the underlying value is a number.
//
// If underlying value is not a number (numeric type convertible to float64),
// failure is reported
//
// Example:
//
//	value := NewValue(t, 123)
//	value.IsNumber()
func (v *Value) IsNumber() *Value {
	opChain := v.chain.enter("IsNumber()")
	defer opChain.leave()

	if opChain.failed() {
		return v
	}

	if _, ok := v.value.(float64); !ok {
		opChain.fail(AssertionFailure{
			Type:   AssertValid,
			Actual: &AssertionValue{v.value},
			Errors: []error{
				errors.New("expected: value is number"),
			},
		})
	}

	return v
}

// NotNumber succeeds if the underlying value is a not a number.
//
// If underlying value is a number (numeric type convertible to float64),
// failure is reported
//
// Example:
//
//	value := NewValue(t, nil)
//	value.NotNumber()
func (v *Value) NotNumber() *Value {
	opChain := v.chain.enter("NotNumber()")
	defer opChain.leave()

	if opChain.failed() {
		return v
	}

	if _, ok := v.value.(float64); ok {
		opChain.fail(AssertionFailure{
			Type:   AssertValid,
			Actual: &AssertionValue{v.value},
			Errors: []error{
				errors.New("expected: value is not number"),
			},
		})
	}

	return v
}

// IsBoolean succeeds if the underlying value is a boolean.
//
// If underlying value is not a boolean, failure is reported.
//
// Example:
//
//	value := NewValue(t, true)
//	value.IsBoolean()
func (v *Value) IsBoolean() *Value {
	opChain := v.chain.enter("IsBoolean()")
	defer opChain.leave()

	if opChain.failed() {
		return v
	}

	if _, ok := v.value.(bool); !ok {
		opChain.fail(AssertionFailure{
			Type:   AssertValid,
			Actual: &AssertionValue{v.value},
			Errors: []error{
				errors.New("expected: value is boolean"),
			},
		})
	}

	return v
}

// NotBoolean succeeds if the underlying value is not a boolean.
//
// If underlying value is a boolean, failure is reported.
//
// Example:
//
//	value := NewValue(t, nil)
//	value.NotBoolean()
func (v *Value) NotBoolean() *Value {
	opChain := v.chain.enter("NotBoolean()")
	defer opChain.leave()

	if opChain.failed() {
		return v
	}

	if _, ok := v.value.(bool); ok {
		opChain.fail(AssertionFailure{
			Type:   AssertValid,
			Actual: &AssertionValue{v.value},
			Errors: []error{
				errors.New("expected: value is not boolean"),
			},
		})
	}

	return v
}

// IsEqual succeeds if value is equal to another value (e.g. map, slice, string, etc).
// Before comparison, both values are converted to canonical form.
//
// Example:
//
//	value := NewValue(t, "foo")
//	value.IsEqual("foo")
func (v *Value) IsEqual(value interface{}) *Value {
	opChain := v.chain.enter("IsEqual()")
	defer opChain.leave()

	if opChain.failed() {
		return v
	}

	expected, ok := canonValue(opChain, value)
	if !ok {
		return v
	}

	if !reflect.DeepEqual(expected, v.value) {
		opChain.fail(AssertionFailure{
			Type:     AssertEqual,
			Actual:   &AssertionValue{v.value},
			Expected: &AssertionValue{expected},
			Errors: []error{
				errors.New("expected: values are equal"),
			},
		})
	}

	return v
}

// NotEqual succeeds if value is not equal to another value (e.g. map, slice,
// string, etc). Before comparison, both values are converted to canonical form.
//
// Example:
//
//	value := NewValue(t, "foo")
//	value.NorEqual("bar")
func (v *Value) NotEqual(value interface{}) *Value {
	opChain := v.chain.enter("NotEqual()")
	defer opChain.leave()

	if opChain.failed() {
		return v
	}

	expected, ok := canonValue(opChain, value)
	if !ok {
		return v
	}

	if reflect.DeepEqual(expected, v.value) {
		opChain.fail(AssertionFailure{
			Type:     AssertNotEqual,
			Actual:   &AssertionValue{v.value},
			Expected: &AssertionValue{expected},
			Errors: []error{
				errors.New("expected: values are non-equal"),
			},
		})
	}

	return v
}

// Deprecated: use IsEqual instead.
func (v *Value) Equal(value interface{}) *Value {
	return v.IsEqual(value)
}

// InList succeeds if whole value is equal to one of the values from given
// list of values (e.g. map, slice, string, etc). Before comparison, all
// values are converted to canonical form.
//
// If at least one value has wrong type, failure is reported.
//
// Example:
//
//	value := NewValue(t, "foo")
//	value.InList("foo", 123)
func (v *Value) InList(values ...interface{}) *Value {
	opChain := v.chain.enter("InList()")
	defer opChain.leave()

	if opChain.failed() {
		return v
	}

	if len(values) == 0 {
		opChain.fail(AssertionFailure{
			Type: AssertUsage,
			Errors: []error{
				errors.New("unexpected empty list argument"),
			},
		})
		return v
	}

	var isListed bool
	for _, val := range values {
		expected, ok := canonValue(opChain, val)
		if !ok {
			return v
		}

		if reflect.DeepEqual(expected, v.value) {
			isListed = true
			// continue loop to check that all values are correct
		}
	}

	if !isListed {
		opChain.fail(AssertionFailure{
			Type:     AssertBelongs,
			Actual:   &AssertionValue{v.value},
			Expected: &AssertionValue{AssertionList(values)},
			Errors: []error{
				errors.New("expected: value is equal to one of the values"),
			},
		})
	}

	return v
}

// NotInList succeeds if the whole value is not equal to any of the values from
// given list of values (e.g. map, slice, string, etc).
// Before comparison, all values are converted to canonical form.
//
// If at least one value has wrong type, failure is reported.
//
// Example:
//
//	value := NewValue(t, "foo")
//	value.NotInList("bar", 123)
func (v *Value) NotInList(values ...interface{}) *Value {
	opChain := v.chain.enter("NotInList()")
	defer opChain.leave()

	if opChain.failed() {
		return v
	}

	if len(values) == 0 {
		opChain.fail(AssertionFailure{
			Type: AssertUsage,
			Errors: []error{
				errors.New("unexpected empty list argument"),
			},
		})
		return v
	}

	for _, val := range values {
		expected, ok := canonValue(opChain, val)
		if !ok {
			return v
		}

		if reflect.DeepEqual(expected, v.value) {
			opChain.fail(AssertionFailure{
				Type:     AssertNotBelongs,
				Actual:   &AssertionValue{v.value},
				Expected: &AssertionValue{AssertionList(values)},
				Errors: []error{
					errors.New("expected: value is not equal to any of the values"),
				},
			})
			return v
		}
	}

	return v
}
