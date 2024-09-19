package httpexpect

import (
	"errors"
)

// Boolean provides methods to inspect attached bool value
// (Go representation of JSON boolean).
type Boolean struct {
	noCopy noCopy
	chain  *chain
	value  bool
}

// NewBoolean returns a new Boolean instance.
//
// If reporter is nil, the function panics.
//
// Example:
//
//	boolean := NewBoolean(t, true)
//	boolean.IsTrue()
func NewBoolean(reporter Reporter, value bool) *Boolean {
	return newBoolean(newChainWithDefaults("Boolean()", reporter), value)
}

// NewBooleanC returns a new Boolean instance with config.
//
// Requirements for config are same as for WithConfig function.
//
// Example:
//
//	boolean := NewBooleanC(config, true)
//	boolean.IsTrue()
func NewBooleanC(config Config, value bool) *Boolean {
	return newBoolean(newChainWithConfig("Boolean()", config.withDefaults()), value)
}

func newBoolean(parent *chain, val bool) *Boolean {
	return &Boolean{chain: parent.clone(), value: val}
}

// Raw returns underlying value attached to Boolean.
// This is the value originally passed to NewBoolean.
//
// Example:
//
//	boolean := NewBoolean(t, true)
//	assert.Equal(t, true, boolean.Raw())
func (b *Boolean) Raw() bool {
	return b.value
}

// Decode unmarshals the underlying value attached to the Boolean to a target variable.
// target should be one of these:
//
//   - pointer to an empty interface
//   - pointer to a boolean
//
// Example:
//
//	value := NewBoolean(t, true)
//
//	var target bool
//	value.Decode(&target)
//
//	assert.Equal(t, true, target)
func (b *Boolean) Decode(target interface{}) *Boolean {
	opChain := b.chain.enter("Decode()")
	defer opChain.leave()

	if opChain.failed() {
		return b
	}

	canonDecode(opChain, b.value, target)
	return b
}

// Alias is similar to Value.Alias.
func (b *Boolean) Alias(name string) *Boolean {
	opChain := b.chain.enter("Alias(%q)", name)
	defer opChain.leave()

	b.chain.setAlias(name)
	return b
}

// Path is similar to Value.Path.
func (b *Boolean) Path(path string) *Value {
	opChain := b.chain.enter("Path(%q)", path)
	defer opChain.leave()

	return jsonPath(opChain, b.value, path)
}

// Schema is similar to Value.Schema.
func (b *Boolean) Schema(schema interface{}) *Boolean {
	opChain := b.chain.enter("Schema()")
	defer opChain.leave()

	jsonSchema(opChain, b.value, schema)
	return b
}

// IsTrue succeeds if boolean is true.
//
// Example:
//
//	boolean := NewBoolean(t, true)
//	boolean.IsTrue()
func (b *Boolean) IsTrue() *Boolean {
	opChain := b.chain.enter("IsTrue()")
	defer opChain.leave()

	if opChain.failed() {
		return b
	}

	if !(b.value == true) {
		opChain.fail(AssertionFailure{
			Type:     AssertEqual,
			Actual:   &AssertionValue{b.value},
			Expected: &AssertionValue{true},
			Errors: []error{
				errors.New("expected: boolean is true"),
			},
		})
	}

	return b
}

// IsFalse succeeds if boolean is false.
//
// Example:
//
//	boolean := NewBoolean(t, false)
//	boolean.IsFalse()
func (b *Boolean) IsFalse() *Boolean {
	opChain := b.chain.enter("IsFalse()")
	defer opChain.leave()

	if opChain.failed() {
		return b
	}

	if !(b.value == false) {
		opChain.fail(AssertionFailure{
			Type:     AssertEqual,
			Actual:   &AssertionValue{b.value},
			Expected: &AssertionValue{false},
			Errors: []error{
				errors.New("expected: boolean is false"),
			},
		})
	}

	return b
}

// Deprecated: use IsTrue instead.
func (b *Boolean) True() *Boolean {
	return b.IsTrue()
}

// Deprecated: use IsFalse instead.
func (b *Boolean) False() *Boolean {
	return b.IsFalse()
}

// IsEqual succeeds if boolean is equal to given value.
//
// Example:
//
//	boolean := NewBoolean(t, true)
//	boolean.IsEqual(true)
func (b *Boolean) IsEqual(value bool) *Boolean {
	opChain := b.chain.enter("IsEqual()")
	defer opChain.leave()

	if opChain.failed() {
		return b
	}

	if !(b.value == value) {
		opChain.fail(AssertionFailure{
			Type:     AssertEqual,
			Actual:   &AssertionValue{b.value},
			Expected: &AssertionValue{value},
			Errors: []error{
				errors.New("expected: booleans are equal"),
			},
		})
	}

	return b
}

// NotEqual succeeds if boolean is not equal to given value.
//
// Example:
//
//	boolean := NewBoolean(t, true)
//	boolean.NotEqual(false)
func (b *Boolean) NotEqual(value bool) *Boolean {
	opChain := b.chain.enter("NotEqual()")
	defer opChain.leave()

	if opChain.failed() {
		return b
	}

	if b.value == value {
		opChain.fail(AssertionFailure{
			Type:     AssertNotEqual,
			Actual:   &AssertionValue{b.value},
			Expected: &AssertionValue{value},
			Errors: []error{
				errors.New("expected: booleans are non-equal"),
			},
		})
	}

	return b
}

// Deprecated: use IsEqual instead.
func (b *Boolean) Equal(value bool) *Boolean {
	return b.IsEqual(value)
}

// InList succeeds if boolean is equal to one of the values from given
// list of booleans.
//
// Example:
//
//	boolean := NewBoolean(t, true)
//	boolean.InList(true, false)
func (b *Boolean) InList(values ...bool) *Boolean {
	opChain := b.chain.enter("InList()")
	defer opChain.leave()

	if opChain.failed() {
		return b
	}

	if len(values) == 0 {
		opChain.fail(AssertionFailure{
			Type: AssertUsage,
			Errors: []error{
				errors.New("unexpected empty list argument"),
			},
		})
		return b
	}

	var isListed bool
	for _, v := range values {
		if b.value == v {
			isListed = true
			break
		}
	}

	if !isListed {
		valueList := make([]interface{}, 0, len(values))
		for _, v := range values {
			valueList = append(valueList, v)
		}

		opChain.fail(AssertionFailure{
			Type:     AssertBelongs,
			Actual:   &AssertionValue{b.value},
			Expected: &AssertionValue{AssertionList(valueList)},
			Errors: []error{
				errors.New("expected: boolean is equal to one of the values"),
			},
		})
	}

	return b
}

// NotInList succeeds if boolean is not equal to any of the values from
// given list of booleans.
//
// Example:
//
//	boolean := NewBoolean(t, true)
//	boolean.NotInList(true, false) // failure
func (b *Boolean) NotInList(values ...bool) *Boolean {
	opChain := b.chain.enter("NotInList()")
	defer opChain.leave()

	if opChain.failed() {
		return b
	}

	if len(values) == 0 {
		opChain.fail(AssertionFailure{
			Type: AssertUsage,
			Errors: []error{
				errors.New("unexpected empty list argument"),
			},
		})
		return b
	}

	for _, v := range values {
		if b.value == v {
			valueList := make([]interface{}, 0, len(values))
			for _, v := range values {
				valueList = append(valueList, v)
			}

			opChain.fail(AssertionFailure{
				Type:     AssertNotBelongs,
				Actual:   &AssertionValue{b.value},
				Expected: &AssertionValue{AssertionList(valueList)},
				Errors: []error{
					errors.New("expected: boolean is not equal to any of the values"),
				},
			})

			return b
		}
	}

	return b
}
