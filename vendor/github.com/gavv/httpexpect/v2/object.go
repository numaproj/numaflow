package httpexpect

import (
	"errors"
	"fmt"
	"reflect"
	"sort"
)

// Object provides methods to inspect attached map[string]interface{} object
// (Go representation of JSON object).
type Object struct {
	noCopy noCopy
	chain  *chain
	value  map[string]interface{}
}

// NewObject returns a new Object instance.
//
// If reporter is nil, the function panics.
// If value is nil, failure is reported.
//
// Example:
//
//	object := NewObject(t, map[string]interface{}{"foo": 123})
func NewObject(reporter Reporter, value map[string]interface{}) *Object {
	return newObject(newChainWithDefaults("Object()", reporter), value)
}

// NewObjectC returns a new Object instance with config.
//
// Requirements for config are same as for WithConfig function.
// If value is nil, failure is reported.
//
// Example:
//
//	object := NewObjectC(config, map[string]interface{}{"foo": 123})
func NewObjectC(config Config, value map[string]interface{}) *Object {
	return newObject(newChainWithConfig("Object()", config.withDefaults()), value)
}

func newObject(parent *chain, val map[string]interface{}) *Object {
	o := &Object{chain: parent.clone(), value: nil}

	opChain := o.chain.enter("")
	defer opChain.leave()

	if val == nil {
		opChain.fail(AssertionFailure{
			Type:   AssertNotNil,
			Actual: &AssertionValue{val},
			Errors: []error{
				errors.New("expected: non-nil map"),
			},
		})
	} else {
		o.value, _ = canonMap(opChain, val)
	}

	return o
}

// Raw returns underlying value attached to Object.
// This is the value originally passed to NewObject, converted to canonical form.
//
// Example:
//
//	object := NewObject(t, map[string]interface{}{"foo": 123})
//	assert.Equal(t, map[string]interface{}{"foo": 123.0}, object.Raw())
func (o *Object) Raw() map[string]interface{} {
	return o.value
}

// Decode unmarshals the underlying value attached to the Object to a target variable
// target should be one of this:
//
//   - pointer to an empty interface
//   - pointer to a map
//   - pointer to a struct
//
// Example:
//
//	type S struct{
//		Foo int                    `json:"foo"`
//		Bar []interface{}          `json:"bar"`
//		Baz map[string]interface{} `json:"baz"`
//		Bat struct{ A int }        `json:"bat"`
//	}
//
//	m := map[string]interface{}{
//		"foo": 123,
//		"bar": []interface{}{"123", 234.0},
//		"baz": map[string]interface{}{
//			"a": "b",
//		},
//		"bat": struct{ A int }{123},
//	}
//
//	value := NewObject(t, value)
//
//	var target S
//	value.Decode(&target)
//
//	assert.Equal(t, S{123,[]interface{}{"123", 234.0},
//		map[string]interface{}{"a": "b"}, struct{ A int }{123},
//	}, target)
func (o *Object) Decode(target interface{}) *Object {
	opChain := o.chain.enter("Decode()")
	defer opChain.leave()

	if opChain.failed() {
		return o
	}

	canonDecode(opChain, o.value, target)
	return o
}

// Alias is similar to Value.Alias.
func (o *Object) Alias(name string) *Object {
	opChain := o.chain.enter("Alias(%q)", name)
	defer opChain.leave()

	o.chain.setAlias(name)
	return o
}

// Path is similar to Value.Path.
func (o *Object) Path(path string) *Value {
	opChain := o.chain.enter("Path(%q)", path)
	defer opChain.leave()

	return jsonPath(opChain, o.value, path)
}

// Schema is similar to Value.Schema.
func (o *Object) Schema(schema interface{}) *Object {
	opChain := o.chain.enter("Schema()")
	defer opChain.leave()

	jsonSchema(opChain, o.value, schema)
	return o
}

// Keys returns a new Array instance with object's keys.
// Keys are sorted in ascending order.
//
// Example:
//
//	object := NewObject(t, map[string]interface{}{"foo": 123, "bar": 456})
//	object.Keys().ContainsOnly("foo", "bar")
func (o *Object) Keys() *Array {
	opChain := o.chain.enter("Keys()")
	defer opChain.leave()

	if opChain.failed() {
		return newArray(opChain, nil)
	}

	keys := []interface{}{}
	for _, kv := range o.sortedKV() {
		keys = append(keys, kv.key)
	}

	return newArray(opChain, keys)
}

// Values returns a new Array instance with object's values.
// Values are sorted by keys ascending order.
//
// Example:
//
//	object := NewObject(t, map[string]interface{}{"foo": 123, "bar": 456})
//	object.Values().ContainsOnly(123, 456)
func (o *Object) Values() *Array {
	opChain := o.chain.enter("Values()")
	defer opChain.leave()

	if opChain.failed() {
		return newArray(opChain, nil)
	}

	values := []interface{}{}
	for _, kv := range o.sortedKV() {
		values = append(values, kv.val)
	}

	return newArray(opChain, values)
}

// Value returns a new Value instance with value for given key.
//
// Example:
//
//	object := NewObject(t, map[string]interface{}{"foo": 123})
//	object.Value("foo").Number().IsEqual(123)
func (o *Object) Value(key string) *Value {
	opChain := o.chain.enter("Value(%q)", key)
	defer opChain.leave()

	if opChain.failed() {
		return newValue(opChain, nil)
	}

	value, ok := o.value[key]

	if !ok {
		opChain.fail(AssertionFailure{
			Type:     AssertContainsKey,
			Actual:   &AssertionValue{o.value},
			Expected: &AssertionValue{key},
			Errors: []error{
				errors.New("expected: map contains key"),
			},
		})
		return newValue(opChain, nil)
	}

	return newValue(opChain, value)
}

// HasValue succeeds if object's value for given key is equal to given value.
// Before comparison, both values are converted to canonical form.
//
// value should be map[string]interface{} or struct.
//
// Example:
//
//	object := NewObject(t, map[string]interface{}{"foo": 123})
//	object.HasValue("foo", 123)
func (o *Object) HasValue(key string, value interface{}) *Object {
	opChain := o.chain.enter("HasValue(%q)", key)
	defer opChain.leave()

	if opChain.failed() {
		return o
	}

	if !containsKey(opChain, o.value, key) {
		opChain.fail(AssertionFailure{
			Type:     AssertContainsKey,
			Actual:   &AssertionValue{o.value},
			Expected: &AssertionValue{key},
			Errors: []error{
				errors.New("expected: map contains key"),
			},
		})
		return o
	}

	expected, ok := canonValue(opChain, value)
	if !ok {
		return o
	}

	if !reflect.DeepEqual(expected, o.value[key]) {
		opChain.fail(AssertionFailure{
			Type:     AssertEqual,
			Actual:   &AssertionValue{o.value[key]},
			Expected: &AssertionValue{value},
			Errors: []error{
				fmt.Errorf(
					"expected: map value for key %q is equal to given value",
					key),
			},
		})
		return o
	}

	return o
}

// NotHasValue succeeds if object's value for given key is not equal to given
// value. Before comparison, both values are converted to canonical form.
//
// value should be map[string]interface{} or struct.
//
// If object doesn't contain any value for given key, failure is reported.
//
// Example:
//
//	object := NewObject(t, map[string]interface{}{"foo": 123})
//	object.NotHasValue("foo", "bad value")  // success
//	object.NotHasValue("bar", "bad value")  // failure! (key is missing)
func (o *Object) NotHasValue(key string, value interface{}) *Object {
	opChain := o.chain.enter("NotHasValue(%q)", key)
	defer opChain.leave()

	if opChain.failed() {
		return o
	}

	if !containsKey(opChain, o.value, key) {
		opChain.fail(AssertionFailure{
			Type:     AssertContainsKey,
			Actual:   &AssertionValue{o.value},
			Expected: &AssertionValue{key},
			Errors: []error{
				errors.New("expected: map contains key"),
			},
		})
		return o
	}

	expected, ok := canonValue(opChain, value)
	if !ok {
		return o
	}

	if reflect.DeepEqual(expected, o.value[key]) {
		opChain.fail(AssertionFailure{
			Type:     AssertNotEqual,
			Actual:   &AssertionValue{o.value[key]},
			Expected: &AssertionValue{value},
			Errors: []error{
				fmt.Errorf(
					"expected: map value for key %q is non-equal to given value",
					key),
			},
		})
		return o
	}

	return o
}

// Deprecated: use HasValue instead.
func (o *Object) ValueEqual(key string, value interface{}) *Object {
	return o.HasValue(key, value)
}

// Deprecated: use NotHasValue instead.
func (o *Object) ValueNotEqual(key string, value interface{}) *Object {
	return o.NotHasValue(key, value)
}

// Iter returns a new map of Values attached to object elements.
//
// Example:
//
//	numbers := map[string]interface{}{"foo": 123, "bar": 456}
//	object := NewObject(t, numbers)
//
//	for key, value := range object.Iter() {
//		value.Number().IsEqual(numbers[key])
//	}
func (o *Object) Iter() map[string]Value {
	opChain := o.chain.enter("Iter()")
	defer opChain.leave()

	if opChain.failed() {
		return map[string]Value{}
	}

	ret := map[string]Value{}

	for k, v := range o.value {
		func() {
			valueChain := opChain.replace("Iter[%q]", k)
			defer valueChain.leave()

			ret[k] = *newValue(valueChain, v)
		}()
	}

	return ret
}

// Every runs the passed function for all the key value pairs in the object.
//
// If assertion inside function fails, the original Object is marked failed.
//
// Every will execute the function for all values in the object irrespective
// of assertion failures for some values in the object.
//
// The function is invoked for key value pairs sorted by keys in ascending order.
//
// Example:
//
//	object := NewObject(t, map[string]interface{}{"foo": 123, "bar": 456})
//
//	object.Every(func(key string, value *httpexpect.Value) {
//	  value.String().NotEmpty()
//	})
func (o *Object) Every(fn func(key string, value *Value)) *Object {
	opChain := o.chain.enter("Every()")
	defer opChain.leave()

	if opChain.failed() {
		return o
	}

	if fn == nil {
		opChain.fail(AssertionFailure{
			Type: AssertUsage,
			Errors: []error{
				errors.New("unexpected nil function argument"),
			},
		})
		return o
	}

	for _, kv := range o.sortedKV() {
		func() {
			valueChain := opChain.replace("Every[%q]", kv.key)
			defer valueChain.leave()

			fn(kv.key, newValue(valueChain, kv.val))
		}()
	}

	return o
}

// Filter accepts a function that returns a boolean. The function is ran
// over the object elements. If the function returns true, the element passes
// the filter and is added to the new object of filtered elements. If false,
// the value is skipped (or in other words filtered out). After iterating
// through all the elements of the original object, the new filtered object
// is returned.
//
// If there are any failed assertions in the filtering function, the
// element is omitted without causing test failure.
//
// The function is invoked for key value pairs sorted by keys in ascending order.
//
// Example:
//
//	object := NewObject(t, map[string]interface{}{
//		"foo": "bar",
//		"baz": 6,
//		"qux": "quux",
//	})
//	filteredObject := object.Filter(func(key string, value *httpexpect.Value) bool {
//		value.String().NotEmpty()		//fails on 6
//		return value.Raw() != "bar"		//fails on "bar"
//	})
//	filteredObject.IsEqual(map[string]interface{}{"qux":"quux"})	//succeeds
func (o *Object) Filter(fn func(key string, value *Value) bool) *Object {
	opChain := o.chain.enter("Filter()")
	defer opChain.leave()

	if opChain.failed() {
		return newObject(opChain, nil)
	}

	if fn == nil {
		opChain.fail(AssertionFailure{
			Type: AssertUsage,
			Errors: []error{
				errors.New("unexpected nil function argument"),
			},
		})
		return newObject(opChain, nil)
	}

	filteredObject := map[string]interface{}{}

	for _, kv := range o.sortedKV() {
		func() {
			valueChain := opChain.replace("Filter[%q]", kv.key)
			defer valueChain.leave()

			valueChain.setRoot()
			valueChain.setSeverity(SeverityLog)

			if fn(kv.key, newValue(valueChain, kv.val)) && !valueChain.treeFailed() {
				filteredObject[kv.key] = kv.val
			}
		}()
	}

	return newObject(opChain, filteredObject)
}

// Transform runs the passed function on all the elements in the Object
// and returns a new object without effecting original object.
//
// The function is invoked for key value pairs sorted by keys in ascending order.
//
// Example:
//
//	object := NewObject(t, []interface{}{"x": "foo", "y": "bar"})
//	transformedObject := object.Transform(
//		func(key string, value interface{}) interface{} {
//			return strings.ToUpper(value.(string))
//		})
//	transformedObject.IsEqual([]interface{}{"x": "FOO", "y": "BAR"})
func (o *Object) Transform(fn func(key string, value interface{}) interface{}) *Object {
	opChain := o.chain.enter("Transform()")
	defer opChain.leave()

	if opChain.failed() {
		return newObject(opChain, nil)
	}

	if fn == nil {
		opChain.fail(AssertionFailure{
			Type: AssertUsage,
			Errors: []error{
				errors.New("unexpected nil function argument"),
			},
		})
		return newObject(opChain, nil)
	}

	transformedObject := map[string]interface{}{}

	for _, kv := range o.sortedKV() {
		transformedObject[kv.key] = fn(kv.key, kv.val)
	}

	return newObject(opChain, transformedObject)
}

// Find accepts a function that returns a boolean, runs it over the object
// elements, and returns the first element on which it returned true.
//
// If there are any failed assertions in the predicate function, the
// element is skipped without causing test failure.
//
// If no elements were found, a failure is reported.
//
// The function is invoked for key value pairs sorted by keys in ascending order.
//
// Example:
//
//	object := NewObject(t, map[string]interface{}{
//		"a": 1,
//		"b": "foo",
//		"c": 101,
//		"d": "bar",
//		"e": 201,
//	})
//	foundValue := object.Find(func(key string, value *httpexpect.Value)  bool {
//		num := value.Number()      // skip if element is not a string
//		return num.Raw() > 100     // check element value
//	})
//	foundValue.IsEqual(101) // succeeds
func (o *Object) Find(fn func(key string, value *Value) bool) *Value {
	opChain := o.chain.enter("Find()")
	defer opChain.leave()

	if opChain.failed() {
		return newValue(opChain, nil)
	}

	if fn == nil {
		opChain.fail(AssertionFailure{
			Type: AssertUsage,
			Errors: []error{
				errors.New("unexpected nil function argument"),
			},
		})
		return newValue(opChain, nil)
	}

	for _, kv := range o.sortedKV() {
		found := false

		func() {
			valueChain := opChain.replace("Find[%q]", kv.key)
			defer valueChain.leave()

			valueChain.setRoot()
			valueChain.setSeverity(SeverityLog)

			if fn(kv.key, newValue(valueChain, kv.val)) && !valueChain.treeFailed() {
				found = true
			}
		}()

		if found {
			return newValue(opChain, kv.val)
		}
	}

	opChain.fail(AssertionFailure{
		Type:   AssertValid,
		Actual: &AssertionValue{o.value},
		Errors: []error{
			errors.New("expected: at least one object element matches predicate"),
		},
	})

	return newValue(opChain, nil)
}

// FindAll accepts a function that returns a boolean, runs it over the object
// elements, and returns all the elements on which it returned true.
//
// If there are any failed assertions in the predicate function, the
// element is skipped without causing test failure.
//
// If no elements were found, empty slice is returned without reporting error.
//
// The function is invoked for key value pairs sorted by keys in ascending order.
//
// Example:
//
//	object := NewObject(t, map[string]interface{}{
//		"a": 1,
//		"b": "foo",
//		"c": 101,
//		"d": "bar",
//		"e": 201,
//	})
//	foundValues := object.FindAll(func(key string, value *httpexpect.Value)  bool {
//		num := value.Number()      // skip if element is not a string
//		return num.Raw() > 100     // check element value
//	})
//
//	assert.Equal(t, len(foundValues), 2)
//	foundValues[0].IsEqual(101)
//	foundValues[1].IsEqual(201)
func (o *Object) FindAll(fn func(key string, value *Value) bool) []*Value {
	opChain := o.chain.enter("FindAll()")
	defer opChain.leave()

	if opChain.failed() {
		return []*Value{}
	}

	if fn == nil {
		opChain.fail(AssertionFailure{
			Type: AssertUsage,
			Errors: []error{
				errors.New("unexpected nil function argument"),
			},
		})
		return []*Value{}
	}

	foundValues := make([]*Value, 0, len(o.value))

	for _, kv := range o.sortedKV() {
		func() {
			valueChain := opChain.replace("FindAll[%q]", kv.key)
			defer valueChain.leave()

			valueChain.setRoot()
			valueChain.setSeverity(SeverityLog)

			if fn(kv.key, newValue(valueChain, kv.val)) && !valueChain.treeFailed() {
				foundValues = append(foundValues, newValue(opChain, kv.val))
			}
		}()
	}

	return foundValues
}

// NotFind accepts a function that returns a boolean, runs it over the object
// elelements, and checks that it does not return true for any of the elements.
//
// If there are any failed assertions in the predicate function, the
// element is skipped without causing test failure.
//
// If the predicate function did not fail and returned true for at least
// one element, a failure is reported.
//
// The function is invoked for key value pairs sorted by keys in ascending order.
//
// Example:
//
//	object := NewObject(t, map[string]interface{}{
//		"a": 1,
//		"b": "foo",
//		"c": 2,
//		"d": "bar",
//	})
//	object.NotFind(func(key string, value *httpexpect.Value) bool {
//		num := value.Number()    // skip if element is not a number
//		return num.Raw() > 100   // check element value
//	}) // succeeds
func (o *Object) NotFind(fn func(key string, value *Value) bool) *Object {
	opChain := o.chain.enter("NotFind()")
	defer opChain.leave()

	if opChain.failed() {
		return o
	}

	if fn == nil {
		opChain.fail(AssertionFailure{
			Type: AssertUsage,
			Errors: []error{
				errors.New("unexpected nil function argument"),
			},
		})
		return o
	}

	for _, kv := range o.sortedKV() {
		found := false

		func() {
			valueChain := opChain.replace("NotFind[%q]", kv.key)
			defer valueChain.leave()

			valueChain.setRoot()
			valueChain.setSeverity(SeverityLog)

			if fn(kv.key, newValue(valueChain, kv.val)) && !valueChain.treeFailed() {
				found = true
			}
		}()

		if found {
			opChain.fail(AssertionFailure{
				Type:     AssertNotContainsElement,
				Expected: &AssertionValue{kv.val},
				Actual:   &AssertionValue{o.value},
				Errors: []error{
					errors.New("expected: none of the object elements match predicate"),
					fmt.Errorf("element with key %q matches predicate", kv.key),
				},
			})
			return o
		}
	}

	return o
}

// IsEmpty succeeds if object is empty.
//
// Example:
//
//	object := NewObject(t, map[string]interface{}{})
//	object.IsEmpty()
func (o *Object) IsEmpty() *Object {
	opChain := o.chain.enter("IsEmpty()")
	defer opChain.leave()

	if opChain.failed() {
		return o
	}

	if !(len(o.value) == 0) {
		opChain.fail(AssertionFailure{
			Type:   AssertEmpty,
			Actual: &AssertionValue{o.value},
			Errors: []error{
				errors.New("expected: map is empty"),
			},
		})
	}

	return o
}

// NotEmpty succeeds if object is non-empty.
//
// Example:
//
//	object := NewObject(t, map[string]interface{}{"foo": 123})
//	object.NotEmpty()
func (o *Object) NotEmpty() *Object {
	opChain := o.chain.enter("NotEmpty()")
	defer opChain.leave()

	if opChain.failed() {
		return o
	}

	if len(o.value) == 0 {
		opChain.fail(AssertionFailure{
			Type:   AssertNotEmpty,
			Actual: &AssertionValue{o.value},
			Errors: []error{
				errors.New("expected: map is non-empty"),
			},
		})
	}

	return o
}

// Deprecated: use IsEmpty instead.
func (o *Object) Empty() *Object {
	return o.IsEmpty()
}

// IsEqual succeeds if object is equal to given value.
// Before comparison, both object and value are converted to canonical form.
//
// value should be map[string]interface{} or struct.
//
// Example:
//
//	object := NewObject(t, map[string]interface{}{"foo": 123})
//	object.IsEqual(map[string]interface{}{"foo": 123})
func (o *Object) IsEqual(value interface{}) *Object {
	opChain := o.chain.enter("IsEqual()")
	defer opChain.leave()

	if opChain.failed() {
		return o
	}

	expected, ok := canonMap(opChain, value)
	if !ok {
		return o
	}

	if !reflect.DeepEqual(expected, o.value) {
		opChain.fail(AssertionFailure{
			Type:     AssertEqual,
			Actual:   &AssertionValue{o.value},
			Expected: &AssertionValue{expected},
			Errors: []error{
				errors.New("expected: maps are equal"),
			},
		})
	}

	return o
}

// NotEqual succeeds if object is not equal to given value.
// Before comparison, both object and value are converted to canonical form.
//
// value should be map[string]interface{} or struct.
//
// Example:
//
//	object := NewObject(t, map[string]interface{}{"foo": 123})
//	object.IsEqual(map[string]interface{}{"bar": 123})
func (o *Object) NotEqual(value interface{}) *Object {
	opChain := o.chain.enter("NotEqual()")
	defer opChain.leave()

	if opChain.failed() {
		return o
	}

	expected, ok := canonMap(opChain, value)
	if !ok {
		return o
	}

	if reflect.DeepEqual(expected, o.value) {
		opChain.fail(AssertionFailure{
			Type:     AssertNotEqual,
			Actual:   &AssertionValue{o.value},
			Expected: &AssertionValue{expected},
			Errors: []error{
				errors.New("expected: maps are non-equal"),
			},
		})
	}

	return o
}

// Deprecated: use IsEqual instead.
func (o *Object) Equal(value interface{}) *Object {
	return o.IsEqual(value)
}

// InList succeeds if whole object is equal to one of the values from given list
// of objects. Before comparison, each value is converted to canonical form.
//
// Each value should be map[string]interface{} or struct. If at least one value
// has wrong type, failure is reported.
//
// Example:
//
//	object := NewObject(t, map[string]interface{}{"foo": 123})
//	object.InList(
//		map[string]interface{}{"foo": 123},
//		map[string]interface{}{"bar": 456},
//	)
func (o *Object) InList(values ...interface{}) *Object {
	opChain := o.chain.enter("InList()")
	defer opChain.leave()

	if opChain.failed() {
		return o
	}

	if len(values) == 0 {
		opChain.fail(AssertionFailure{
			Type: AssertUsage,
			Errors: []error{
				errors.New("unexpected empty list argument"),
			},
		})
		return o
	}

	var isListed bool
	for _, v := range values {
		expected, ok := canonMap(opChain, v)
		if !ok {
			return o
		}

		if reflect.DeepEqual(expected, o.value) {
			isListed = true
			// continue loop to check that all values are correct
		}
	}

	if !isListed {
		opChain.fail(AssertionFailure{
			Type:     AssertBelongs,
			Actual:   &AssertionValue{o.value},
			Expected: &AssertionValue{AssertionList(values)},
			Errors: []error{
				errors.New("expected: map is equal to one of the values"),
			},
		})
		return o
	}

	return o
}

// NotInList succeeds if the whole object is not equal to any of the values
// from given list of objects. Before comparison, each value is converted to
// canonical form.
//
// Each value should be map[string]interface{} or struct. If at least one value
// has wrong type, failure is reported.
//
// Example:
//
//	object := NewObject(t, map[string]interface{}{"foo": 123})
//	object.NotInList(
//		map[string]interface{}{"bar": 456},
//		map[string]interface{}{"baz": 789},
//	)
func (o *Object) NotInList(values ...interface{}) *Object {
	opChain := o.chain.enter("NotInList()")
	defer opChain.leave()

	if opChain.failed() {
		return o
	}

	if len(values) == 0 {
		opChain.fail(AssertionFailure{
			Type: AssertUsage,
			Errors: []error{
				errors.New("unexpected empty list argument"),
			},
		})
		return o
	}

	for _, v := range values {
		expected, ok := canonMap(opChain, v)
		if !ok {
			return o
		}

		if reflect.DeepEqual(expected, o.value) {
			opChain.fail(AssertionFailure{
				Type:     AssertNotBelongs,
				Actual:   &AssertionValue{o.value},
				Expected: &AssertionValue{AssertionList(values)},
				Errors: []error{
					errors.New("expected: map is not equal to any of the values"),
				},
			})
			return o
		}
	}

	return o
}

// ContainsKey succeeds if object contains given key.
//
// Example:
//
//	object := NewObject(t, map[string]interface{}{"foo": 123})
//	object.ContainsKey("foo")
func (o *Object) ContainsKey(key string) *Object {
	opChain := o.chain.enter("ContainsKey()")
	defer opChain.leave()

	if opChain.failed() {
		return o
	}

	if !containsKey(opChain, o.value, key) {
		opChain.fail(AssertionFailure{
			Type:     AssertContainsKey,
			Actual:   &AssertionValue{o.value},
			Expected: &AssertionValue{key},
			Errors: []error{
				errors.New("expected: map contains key"),
			},
		})
	}

	return o
}

// NotContainsKey succeeds if object doesn't contain given key.
//
// Example:
//
//	object := NewObject(t, map[string]interface{}{"foo": 123})
//	object.NotContainsKey("bar")
func (o *Object) NotContainsKey(key string) *Object {
	opChain := o.chain.enter("NotContainsKey()")
	defer opChain.leave()

	if opChain.failed() {
		return o
	}

	if containsKey(opChain, o.value, key) {
		opChain.fail(AssertionFailure{
			Type:     AssertNotContainsKey,
			Actual:   &AssertionValue{o.value},
			Expected: &AssertionValue{key},
			Errors: []error{
				errors.New("expected: map does not contain key"),
			},
		})
	}

	return o
}

// ContainsValue succeeds if object contains given value with any key.
// Before comparison, both object and value are converted to canonical form.
//
// Example:
//
//	object := NewObject(t, map[string]interface{}{"foo": 123})
//	object.ContainsValue(123)
func (o *Object) ContainsValue(value interface{}) *Object {
	opChain := o.chain.enter("ContainsValue()")
	defer opChain.leave()

	if opChain.failed() {
		return o
	}

	if _, ok := containsValue(opChain, o.value, value); !ok {
		opChain.fail(AssertionFailure{
			Type:     AssertContainsElement,
			Actual:   &AssertionValue{o.value},
			Expected: &AssertionValue{value},
			Errors: []error{
				errors.New("expected: map contains element (with any key)"),
			},
		})
	}

	return o
}

// NotContainsValue succeeds if object does not contain given value with any key.
// Before comparison, both object and value are converted to canonical form.
//
// Example:
//
//	object := NewObject(t, map[string]interface{}{"foo": 123})
//	object.NotContainsValue(456)
func (o *Object) NotContainsValue(value interface{}) *Object {
	opChain := o.chain.enter("NotContainsValue()")
	defer opChain.leave()

	if opChain.failed() {
		return o
	}

	if key, ok := containsValue(opChain, o.value, value); ok {
		opChain.fail(AssertionFailure{
			Type:     AssertNotContainsElement,
			Actual:   &AssertionValue{o.value},
			Expected: &AssertionValue{value},
			Errors: []error{
				errors.New("expected: map does not contain element (with any key)"),
				fmt.Errorf("found matching element with key %q", key),
			},
		})
	}

	return o
}

// ContainsSubset succeeds if given value is a subset of object.
// Before comparison, both object and value are converted to canonical form.
//
// value should be map[string]interface{} or struct.
//
// Example:
//
//	object := NewObject(t, map[string]interface{}{
//		"foo": 123,
//		"bar": []interface{}{"x", "y"},
//		"bar": map[string]interface{}{
//			"a": true,
//			"b": false,
//		},
//	})
//
//	object.ContainsSubset(map[string]interface{}{  // success
//		"foo": 123,
//		"bar": map[string]interface{}{
//			"a": true,
//		},
//	})
//
//	object.ContainsSubset(map[string]interface{}{  // failure
//		"foo": 123,
//		"qux": 456,
//	})
//
//	object.ContainsSubset(map[string]interface{}{  // failure, slices should match exactly
//		"bar": []interface{}{"x"},
//	})
func (o *Object) ContainsSubset(value interface{}) *Object {
	opChain := o.chain.enter("ContainsSubset()")
	defer opChain.leave()

	if opChain.failed() {
		return o
	}

	if !containsSubset(opChain, o.value, value) {
		opChain.fail(AssertionFailure{
			Type:     AssertContainsSubset,
			Actual:   &AssertionValue{o.value},
			Expected: &AssertionValue{value},
			Errors: []error{
				errors.New("expected: map contains sub-map"),
			},
		})
	}

	return o
}

// NotContainsSubset succeeds if given value is not a subset of object.
// Before comparison, both object and value are converted to canonical form.
//
// value should be map[string]interface{} or struct.
//
// Example:
//
//	object := NewObject(t, map[string]interface{}{"foo": 123, "bar": 456})
//	object.NotContainsSubset(map[string]interface{}{"foo": 123, "bar": "no-no-no"})
func (o *Object) NotContainsSubset(value interface{}) *Object {
	opChain := o.chain.enter("NotContainsSubset()")
	defer opChain.leave()

	if opChain.failed() {
		return o
	}

	if containsSubset(opChain, o.value, value) {
		opChain.fail(AssertionFailure{
			Type:     AssertNotContainsSubset,
			Actual:   &AssertionValue{o.value},
			Expected: &AssertionValue{value},
			Errors: []error{
				errors.New("expected: map does not contain sub-map"),
			},
		})
	}

	return o
}

// Deprecated: use ContainsSubset instead.
func (o *Object) ContainsMap(value interface{}) *Object {
	return o.ContainsSubset(value)
}

// Deprecated: use NotContainsSubset instead.
func (o *Object) NotContainsMap(value interface{}) *Object {
	return o.NotContainsSubset(value)
}

type kv struct {
	key string
	val interface{}
}

func (o *Object) sortedKV() []kv {
	kvs := make([]kv, 0, len(o.value))

	for key, val := range o.value {
		kvs = append(kvs, kv{key: key, val: val})
	}

	sort.Slice(kvs, func(i, j int) bool {
		return kvs[i].key < kvs[j].key
	})

	return kvs
}

func containsKey(
	opChain *chain, obj map[string]interface{}, key string,
) bool {
	for k := range obj {
		if k == key {
			return true
		}
	}
	return false
}

func containsValue(
	opChain *chain, obj map[string]interface{}, val interface{},
) (string, bool) {
	canonVal, ok := canonValue(opChain, val)
	if !ok {
		return "", false
	}

	for k, v := range obj {
		if reflect.DeepEqual(canonVal, v) {
			return k, true
		}
	}

	return "", false
}

func containsSubset(
	opChain *chain, obj map[string]interface{}, val interface{},
) bool {
	canonVal, ok := canonMap(opChain, val)
	if !ok {
		return false
	}

	return isSubset(obj, canonVal)
}

func isSubset(outer, inner map[string]interface{}) bool {
	for k, iv := range inner {
		ov, ok := outer[k]
		if !ok {
			return false
		}

		if ovm, ok := ov.(map[string]interface{}); ok {
			if ivm, ok := iv.(map[string]interface{}); ok {
				if !isSubset(ovm, ivm) {
					return false
				}
				continue
			}
		}

		if !reflect.DeepEqual(ov, iv) {
			return false
		}
	}

	return true
}
