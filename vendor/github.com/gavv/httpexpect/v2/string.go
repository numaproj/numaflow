package httpexpect

import (
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode"
)

// String provides methods to inspect attached string value
// (Go representation of JSON string).
type String struct {
	noCopy noCopy
	chain  *chain
	value  string
}

// NewString returns a new String instance.
//
// If reporter is nil, the function panics.
//
// Example:
//
//	str := NewString(t, "Hello")
func NewString(reporter Reporter, value string) *String {
	return newString(newChainWithDefaults("String()", reporter), value)
}

// NewStringC returns a new String instance with config.
//
// Requirements for config are same as for WithConfig function.
//
// Example:
//
//	str := NewStringC(config, "Hello")
func NewStringC(config Config, value string) *String {
	return newString(newChainWithConfig("String()", config.withDefaults()), value)
}

func newString(parent *chain, val string) *String {
	return &String{chain: parent.clone(), value: val}
}

// Raw returns underlying value attached to String.
// This is the value originally passed to NewString.
//
// Example:
//
//	str := NewString(t, "Hello")
//	assert.Equal(t, "Hello", str.Raw())
func (s *String) Raw() string {
	return s.value
}

// Decode unmarshals the underlying value attached to the String to a target variable.
// target should be one of these:
//
//   - pointer to an empty interface
//   - pointer to a string
//
// Example:
//
//	value := NewString(t, "foo")
//
//	var target string
//	value.Decode(&target)
//
//	assert.Equal(t, "foo", target)
func (s *String) Decode(target interface{}) *String {
	opChain := s.chain.enter("Decode()")
	defer opChain.leave()

	if opChain.failed() {
		return s
	}

	canonDecode(opChain, s.value, target)
	return s
}

// Alias is similar to Value.Alias.
func (s *String) Alias(name string) *String {
	opChain := s.chain.enter("Alias(%q)", name)
	defer opChain.leave()

	s.chain.setAlias(name)
	return s
}

// Path is similar to Value.Path.
func (s *String) Path(path string) *Value {
	opChain := s.chain.enter("Path(%q)", path)
	defer opChain.leave()

	return jsonPath(opChain, s.value, path)
}

// Schema is similar to Value.Schema.
func (s *String) Schema(schema interface{}) *String {
	opChain := s.chain.enter("Schema()")
	defer opChain.leave()

	jsonSchema(opChain, s.value, schema)
	return s
}

// Length returns a new Number instance with string length.
//
// Example:
//
//	str := NewString(t, "Hello")
//	str.Length().IsEqual(5)
func (s *String) Length() *Number {
	opChain := s.chain.enter("Length()")
	defer opChain.leave()

	if opChain.failed() {
		return newNumber(opChain, 0)
	}

	return newNumber(opChain, float64(len(s.value)))
}

// IsEmpty succeeds if string is empty.
//
// Example:
//
//	str := NewString(t, "")
//	str.IsEmpty()
func (s *String) IsEmpty() *String {
	opChain := s.chain.enter("IsEmpty()")
	defer opChain.leave()

	if opChain.failed() {
		return s
	}

	if !(s.value == "") {
		opChain.fail(AssertionFailure{
			Type:   AssertEmpty,
			Actual: &AssertionValue{s.value},
			Errors: []error{
				errors.New("expected: string is empty"),
			},
		})
	}

	return s
}

// NotEmpty succeeds if string is non-empty.
//
// Example:
//
//	str := NewString(t, "Hello")
//	str.NotEmpty()
func (s *String) NotEmpty() *String {
	opChain := s.chain.enter("NotEmpty()")
	defer opChain.leave()

	if opChain.failed() {
		return s
	}

	if s.value == "" {
		opChain.fail(AssertionFailure{
			Type:   AssertNotEmpty,
			Actual: &AssertionValue{s.value},
			Errors: []error{
				errors.New("expected: string is non-empty"),
			},
		})
	}

	return s
}

// Deprecated: use IsEmpty instead.
func (s *String) Empty() *String {
	return s.IsEmpty()
}

// IsEqual succeeds if string is equal to given Go string.
//
// Example:
//
//	str := NewString(t, "Hello")
//	str.IsEqual("Hello")
func (s *String) IsEqual(value string) *String {
	opChain := s.chain.enter("IsEqual()")
	defer opChain.leave()

	if opChain.failed() {
		return s
	}

	if !(s.value == value) {
		opChain.fail(AssertionFailure{
			Type:     AssertEqual,
			Actual:   &AssertionValue{s.value},
			Expected: &AssertionValue{value},
			Errors: []error{
				errors.New("expected: strings are equal"),
			},
		})
	}

	return s
}

// NotEqual succeeds if string is not equal to given Go string.
//
// Example:
//
//	str := NewString(t, "Hello")
//	str.NotEqual("Goodbye")
func (s *String) NotEqual(value string) *String {
	opChain := s.chain.enter("NotEqual()")
	defer opChain.leave()

	if opChain.failed() {
		return s
	}

	if s.value == value {
		opChain.fail(AssertionFailure{
			Type:     AssertNotEqual,
			Actual:   &AssertionValue{s.value},
			Expected: &AssertionValue{value},
			Errors: []error{
				errors.New("expected: strings are non-equal"),
			},
		})
	}

	return s
}

// Deprecated: use IsEqual instead.
func (s *String) Equal(value string) *String {
	return s.IsEqual(value)
}

// IsEqualFold succeeds if string is equal to given Go string after applying Unicode
// case-folding (so it's a case-insensitive match).
//
// Example:
//
//	str := NewString(t, "Hello")
//	str.IsEqualFold("hELLo")
func (s *String) IsEqualFold(value string) *String {
	opChain := s.chain.enter("IsEqualFold()")
	defer opChain.leave()

	if opChain.failed() {
		return s
	}

	if !strings.EqualFold(s.value, value) {
		opChain.fail(AssertionFailure{
			Type:     AssertEqual,
			Actual:   &AssertionValue{s.value},
			Expected: &AssertionValue{value},
			Errors: []error{
				errors.New("expected: strings are equal (if folded)"),
			},
		})
	}

	return s
}

// NotEqualFold succeeds if string is not equal to given Go string after applying
// Unicode case-folding (so it's a case-insensitive match).
//
// Example:
//
//	str := NewString(t, "Hello")
//	str.NotEqualFold("gOODBYe")
func (s *String) NotEqualFold(value string) *String {
	opChain := s.chain.enter("NotEqualFold()")
	defer opChain.leave()

	if opChain.failed() {
		return s
	}

	if strings.EqualFold(s.value, value) {
		opChain.fail(AssertionFailure{
			Type:     AssertNotEqual,
			Actual:   &AssertionValue{s.value},
			Expected: &AssertionValue{value},
			Errors: []error{
				errors.New("expected: strings are non-equal (if folded)"),
			},
		})
	}

	return s
}

// Deprecated: use IsEqualFold instead.
func (s *String) EqualFold(value string) *String {
	return s.IsEqualFold(value)
}

// InList succeeds if the string is equal to one of the values from given
// list of strings.
//
// Example:
//
//	str := NewString(t, "Hello")
//	str.InList("Hello", "Goodbye")
func (s *String) InList(values ...string) *String {
	opChain := s.chain.enter("InList()")
	defer opChain.leave()

	if opChain.failed() {
		return s
	}

	if len(values) == 0 {
		opChain.fail(AssertionFailure{
			Type: AssertUsage,
			Errors: []error{
				errors.New("unexpected empty list argument"),
			},
		})
		return s
	}

	var isListed bool
	for _, v := range values {
		if s.value == v {
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
			Actual:   &AssertionValue{s.value},
			Expected: &AssertionValue{AssertionList(valueList)},
			Errors: []error{
				errors.New("expected: string is equal to one of the values"),
			},
		})
	}

	return s
}

// NotInList succeeds if the string is not equal to any of the values from
// given list of strings.
//
// Example:
//
//	str := NewString(t, "Hello")
//	str.NotInList("Sayonara", "Goodbye")
func (s *String) NotInList(values ...string) *String {
	opChain := s.chain.enter("NotInList()")
	defer opChain.leave()

	if opChain.failed() {
		return s
	}

	if len(values) == 0 {
		opChain.fail(AssertionFailure{
			Type: AssertUsage,
			Errors: []error{
				errors.New("unexpected empty list argument"),
			},
		})
		return s
	}

	for _, v := range values {
		if s.value == v {
			valueList := make([]interface{}, 0, len(values))
			for _, v := range values {
				valueList = append(valueList, v)
			}

			opChain.fail(AssertionFailure{
				Type:     AssertNotBelongs,
				Actual:   &AssertionValue{s.value},
				Expected: &AssertionValue{AssertionList(valueList)},
				Errors: []error{
					errors.New("expected: string is not equal to any of the values"),
				},
			})

			return s
		}
	}

	return s
}

// InListFold succeeds if the string is equal to one of the values from given
// list of strings after applying Unicode case-folding (so it's a case-insensitive match).
//
// Example:
//
//	str := NewString(t, "Hello")
//	str.InListFold("hEllo", "Goodbye")
func (s *String) InListFold(values ...string) *String {
	opChain := s.chain.enter("InListFold()")
	defer opChain.leave()

	if opChain.failed() {
		return s
	}

	if len(values) == 0 {
		opChain.fail(AssertionFailure{
			Type: AssertUsage,
			Errors: []error{
				errors.New("unexpected empty list argument"),
			},
		})
		return s
	}

	var isListed bool
	for _, v := range values {
		if strings.EqualFold(s.value, v) {
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
			Actual:   &AssertionValue{s.value},
			Expected: &AssertionValue{AssertionList(valueList)},
			Errors: []error{
				errors.New("expected: string is equal to one of the values (if folded)"),
			},
		})
	}

	return s
}

// NotInListFold succeeds if the string is not equal to any of the values from given
// list of strings after applying Unicode case-folding (so it's a case-insensitive match).
//
// Example:
//
//	str := NewString(t, "Hello")
//	str.NotInListFold("Bye", "Goodbye")
func (s *String) NotInListFold(values ...string) *String {
	opChain := s.chain.enter("NotInListFold()")
	defer opChain.leave()

	if opChain.failed() {
		return s
	}

	if len(values) == 0 {
		opChain.fail(AssertionFailure{
			Type: AssertUsage,
			Errors: []error{
				errors.New("unexpected empty list argument"),
			},
		})
		return s
	}

	for _, v := range values {
		if strings.EqualFold(s.value, v) {
			valueList := make([]interface{}, 0, len(values))
			for _, v := range values {
				valueList = append(valueList, v)
			}

			opChain.fail(AssertionFailure{
				Type:     AssertNotBelongs,
				Actual:   &AssertionValue{s.value},
				Expected: &AssertionValue{AssertionList(valueList)},
				Errors: []error{
					errors.New("expected: string is not equal to any of the values (if folded)"),
				},
			})

			return s
		}
	}

	return s
}

// Contains succeeds if string contains given Go string as a substring.
//
// Example:
//
//	str := NewString(t, "Hello")
//	str.Contains("ell")
func (s *String) Contains(value string) *String {
	opChain := s.chain.enter("Contains()")
	defer opChain.leave()

	if opChain.failed() {
		return s
	}

	if !strings.Contains(s.value, value) {
		opChain.fail(AssertionFailure{
			Type:     AssertContainsSubset,
			Actual:   &AssertionValue{s.value},
			Expected: &AssertionValue{value},
			Errors: []error{
				errors.New("expected: string contains sub-string"),
			},
		})
	}

	return s
}

// NotContains succeeds if string doesn't contain Go string as a substring.
//
// Example:
//
//	str := NewString(t, "Hello")
//	str.NotContains("bye")
func (s *String) NotContains(value string) *String {
	opChain := s.chain.enter("NotContains()")
	defer opChain.leave()

	if opChain.failed() {
		return s
	}

	if strings.Contains(s.value, value) {
		opChain.fail(AssertionFailure{
			Type:     AssertNotContainsSubset,
			Actual:   &AssertionValue{s.value},
			Expected: &AssertionValue{value},
			Errors: []error{
				errors.New("expected: string does not contain sub-string"),
			},
		})
	}

	return s
}

// ContainsFold succeeds if string contains given Go string as a substring after
// applying Unicode case-folding (so it's a case-insensitive match).
//
// Example:
//
//	str := NewString(t, "Hello")
//	str.ContainsFold("ELL")
func (s *String) ContainsFold(value string) *String {
	opChain := s.chain.enter("ContainsFold()")
	defer opChain.leave()

	if opChain.failed() {
		return s
	}

	if !strings.Contains(strings.ToLower(s.value), strings.ToLower(value)) {
		opChain.fail(AssertionFailure{
			Type:     AssertContainsSubset,
			Actual:   &AssertionValue{s.value},
			Expected: &AssertionValue{value},
			Errors: []error{
				errors.New("expected: string contains sub-string (if folded)"),
			},
		})
	}

	return s
}

// NotContainsFold succeeds if string doesn't contain given Go string as a substring
// after applying Unicode case-folding (so it's a case-insensitive match).
//
// Example:
//
//	str := NewString(t, "Hello")
//	str.NotContainsFold("BYE")
func (s *String) NotContainsFold(value string) *String {
	opChain := s.chain.enter("NotContainsFold()")
	defer opChain.leave()

	if opChain.failed() {
		return s
	}

	if strings.Contains(strings.ToLower(s.value), strings.ToLower(value)) {
		opChain.fail(AssertionFailure{
			Type:     AssertNotContainsSubset,
			Actual:   &AssertionValue{s.value},
			Expected: &AssertionValue{value},
			Errors: []error{
				errors.New("expected: string does not contain sub-string (if folded)"),
			},
		})
	}

	return s
}

// HasPrefix succeeds if string has given Go string as prefix
//
// Example:
//
//	str := NewString(t, "Hello World")
//	str.HasPrefix("Hello")
func (s *String) HasPrefix(value string) *String {
	opChain := s.chain.enter("HasPrefix()")
	defer opChain.leave()

	if opChain.failed() {
		return s
	}

	if !strings.HasPrefix(s.value, value) {
		opChain.fail(AssertionFailure{
			Type:     AssertContainsSubset,
			Actual:   &AssertionValue{s.value},
			Expected: &AssertionValue{value},
			Errors: []error{
				errors.New("expected: string has prefix"),
			},
		})
	}

	return s
}

// NotHasPrefix succeeds if string doesn't have given Go string as prefix
//
// Example:
//
//	str := NewString(t, "Hello World")
//	str.NotHasPrefix("Bye")
func (s *String) NotHasPrefix(value string) *String {
	opChain := s.chain.enter("NotHasPrefix()")
	defer opChain.leave()

	if opChain.failed() {
		return s
	}

	if strings.HasPrefix(s.value, value) {
		opChain.fail(AssertionFailure{
			Type:     AssertNotContainsSubset,
			Actual:   &AssertionValue{s.value},
			Expected: &AssertionValue{value},
			Errors: []error{
				errors.New("expected: string doesn't have prefix"),
			},
		})
	}

	return s
}

// HasSuffix succeeds if string has given Go string as suffix
//
// Example:
//
//	str := NewString(t, "Hello World")
//	str.HasSuffix("World")
func (s *String) HasSuffix(value string) *String {
	opChain := s.chain.enter("HasSuffix()")
	defer opChain.leave()

	if opChain.failed() {
		return s
	}

	if !strings.HasSuffix(s.value, value) {
		opChain.fail(AssertionFailure{
			Type:     AssertContainsSubset,
			Actual:   &AssertionValue{s.value},
			Expected: &AssertionValue{value},
			Errors: []error{
				errors.New("expected: string has suffix"),
			},
		})
	}

	return s
}

// NotHasSuffix succeeds if string doesn't have given Go string as suffix
//
// Example:
//
//	str := NewString(t, "Hello World")
//	str.NotHasSuffix("Hello")
func (s *String) NotHasSuffix(value string) *String {
	opChain := s.chain.enter("NotHasSuffix()")
	defer opChain.leave()

	if opChain.failed() {
		return s
	}

	if strings.HasSuffix(s.value, value) {
		opChain.fail(AssertionFailure{
			Type:     AssertNotContainsSubset,
			Actual:   &AssertionValue{s.value},
			Expected: &AssertionValue{value},
			Errors: []error{
				errors.New("expected: string doesn't have suffix"),
			},
		})
	}

	return s
}

// HasPrefixFold succeeds if string has given Go string as prefix
// after applying Unicode case-folding (so it's a case-insensitive match).
//
// Example:
//
//	str := NewString(t, "Hello World")
//	str.HasPrefixFold("hello")
func (s *String) HasPrefixFold(value string) *String {
	opChain := s.chain.enter("HasPrefixFold()")
	defer opChain.leave()

	if opChain.failed() {
		return s
	}

	if !strings.HasPrefix(strings.ToLower(s.value), strings.ToLower(value)) {
		opChain.fail(AssertionFailure{
			Type:     AssertContainsSubset,
			Actual:   &AssertionValue{s.value},
			Expected: &AssertionValue{value},
			Errors: []error{
				errors.New("expected: string has prefix (if folded)"),
			},
		})
	}

	return s
}

// NotHasPrefixFold succeeds if string doesn't have given Go string as prefix
// after applying Unicode case-folding (so it's a case-insensitive match).
//
// Example:
//
//	str := NewString(t, "Hello World")
//	str.NotHasPrefixFold("Bye")
func (s *String) NotHasPrefixFold(value string) *String {
	opChain := s.chain.enter("NotHasPrefixFold()")
	defer opChain.leave()

	if opChain.failed() {
		return s
	}

	if strings.HasPrefix(strings.ToLower(s.value), strings.ToLower(value)) {
		opChain.fail(AssertionFailure{
			Type:     AssertNotContainsSubset,
			Actual:   &AssertionValue{s.value},
			Expected: &AssertionValue{value},
			Errors: []error{
				errors.New("expected: string doesn't have prefix (if folded)"),
			},
		})
	}

	return s
}

// HasSuffixFold succeeds if string has given Go string as suffix
// after applying Unicode case-folding (so it's a case-insensitive match).
//
// Example:
//
//	str := NewString(t, "Hello World")
//	str.HasSuffixFold("world")
func (s *String) HasSuffixFold(value string) *String {
	opChain := s.chain.enter("HasSuffixFold()")
	defer opChain.leave()

	if opChain.failed() {
		return s
	}

	if !strings.HasSuffix(strings.ToLower(s.value), strings.ToLower(value)) {
		opChain.fail(AssertionFailure{
			Type:     AssertContainsSubset,
			Actual:   &AssertionValue{s.value},
			Expected: &AssertionValue{value},
			Errors: []error{
				errors.New("expected: string has suffix (if folded)"),
			},
		})
	}

	return s
}

// NotHasSuffixFold succeeds if string doesn't have given Go string as suffix
// after applying Unicode case-folding (so it's a case-insensitive match).
//
// Example:
//
//	str := NewString(t, "Hello World")
//	str.NotHasSuffixFold("Bye")
func (s *String) NotHasSuffixFold(value string) *String {
	opChain := s.chain.enter("NotHasSuffix()")
	defer opChain.leave()

	if opChain.failed() {
		return s
	}

	if strings.HasSuffix(strings.ToLower(s.value), strings.ToLower(value)) {
		opChain.fail(AssertionFailure{
			Type:     AssertNotContainsSubset,
			Actual:   &AssertionValue{s.value},
			Expected: &AssertionValue{value},
			Errors: []error{
				errors.New("expected: string doesn't have suffix (if folded)"),
			},
		})
	}

	return s
}

// Match matches the string with given regexp and returns a new Match instance
// with found submatches.
//
// If regexp is invalid or string doesn't match regexp, Match fails and returns
// empty (but non-nil) instance. regexp.Compile is used to construct regexp, and
// Regexp.FindStringSubmatch is used to construct matches.
//
// Example:
//
//	s := NewString(t, "http://example.com/users/john")
//	m := s.Match(`http://(?P<host>.+)/users/(?P<user>.+)`)
//
//	m.NotEmpty()
//	m.Length().IsEqual(3)
//
//	m.Index(0).IsEqual("http://example.com/users/john")
//	m.Index(1).IsEqual("example.com")
//	m.Index(2).IsEqual("john")
//
//	m.Name("host").IsEqual("example.com")
//	m.Name("user").IsEqual("john")
func (s *String) Match(re string) *Match {
	opChain := s.chain.enter("Match()")
	defer opChain.leave()

	if opChain.failed() {
		return newMatch(opChain, nil, nil)
	}

	rx, err := regexp.Compile(re)
	if err != nil {
		opChain.fail(AssertionFailure{
			Type:   AssertValid,
			Actual: &AssertionValue{re},
			Errors: []error{
				errors.New("expected: valid regexp"),
				err,
			},
		})
		return newMatch(opChain, nil, nil)
	}

	match := rx.FindStringSubmatch(s.value)
	if match == nil {
		opChain.fail(AssertionFailure{
			Type:     AssertMatchRegexp,
			Actual:   &AssertionValue{s.value},
			Expected: &AssertionValue{re},
			Errors: []error{
				errors.New("expected: string matches regexp"),
			},
		})
		return newMatch(opChain, nil, nil)
	}

	return newMatch(opChain, match, rx.SubexpNames())
}

// NotMatch succeeds if the string doesn't match to given regexp.
//
// regexp.Compile is used to construct regexp, and Regexp.MatchString
// is used to perform match.
//
// Example:
//
//	s := NewString(t, "a")
//	s.NotMatch(`[^a]`)
func (s *String) NotMatch(re string) *String {
	opChain := s.chain.enter("NotMatch()")
	defer opChain.leave()

	rx, err := regexp.Compile(re)
	if err != nil {
		opChain.fail(AssertionFailure{
			Type:   AssertValid,
			Actual: &AssertionValue{re},
			Errors: []error{
				errors.New("expected: valid regexp"),
				err,
			},
		})
		return s
	}

	if rx.MatchString(s.value) {
		opChain.fail(AssertionFailure{
			Type:     AssertNotMatchRegexp,
			Actual:   &AssertionValue{s.value},
			Expected: &AssertionValue{re},
			Errors: []error{
				errors.New("expected: string does not match regexp"),
			},
		})
		return s
	}

	return s
}

// MatchAll find all matches in string for given regexp and returns a list
// of found matches.
//
// If regexp is invalid or string doesn't match regexp, MatchAll fails and
// returns empty (but non-nil) slice. regexp.Compile is used to construct
// regexp, and Regexp.FindAllStringSubmatch is used to find matches.
//
// Example:
//
//	s := NewString(t,
//	   "http://example.com/users/john http://example.com/users/bob")
//
//	m := s.MatchAll(`http://(?P<host>\S+)/users/(?P<user>\S+)`)
//
//	m[0].Name("user").IsEqual("john")
//	m[1].Name("user").IsEqual("bob")
func (s *String) MatchAll(re string) []Match {
	opChain := s.chain.enter("MatchAll()")
	defer opChain.leave()

	if opChain.failed() {
		return []Match{}
	}

	rx, err := regexp.Compile(re)
	if err != nil {
		opChain.fail(AssertionFailure{
			Type:   AssertValid,
			Actual: &AssertionValue{re},
			Errors: []error{
				errors.New("expected: valid regexp"),
				err,
			},
		})
		return []Match{}
	}

	matches := rx.FindAllStringSubmatch(s.value, -1)
	if matches == nil {
		opChain.fail(AssertionFailure{
			Type:     AssertMatchRegexp,
			Actual:   &AssertionValue{s.value},
			Expected: &AssertionValue{re},
			Errors: []error{
				errors.New("expected: string matches regexp"),
			},
		})
		return []Match{}
	}

	ret := []Match{}
	for _, match := range matches {
		ret = append(ret, *newMatch(
			opChain,
			match,
			rx.SubexpNames()))
	}

	return ret
}

// IsASCII succeeds if all string characters belongs to ASCII.
//
// Example:
//
//	str := NewString(t, "Hello")
//	str.IsASCII()
func (s *String) IsASCII() *String {
	opChain := s.chain.enter("IsASCII()")
	defer opChain.leave()

	if opChain.failed() {
		return s
	}

	isASCII := true
	for _, c := range s.value {
		if c > unicode.MaxASCII {
			isASCII = false
			break
		}
	}

	if !isASCII {
		opChain.fail(AssertionFailure{
			Type:   AssertValid,
			Actual: &AssertionValue{s.value},
			Errors: []error{
				errors.New("expected: all string characters are ascii"),
			},
		})
	}

	return s
}

// NotASCII succeeds if at least one string character does not belong to ASCII.
//
// Example:
//
//	str := NewString(t, "こんにちは")
//	str.NotASCII()
func (s *String) NotASCII() *String {
	opChain := s.chain.enter("NotASCII()")
	defer opChain.leave()

	if opChain.failed() {
		return s
	}

	isASCII := true
	for _, c := range s.value {
		if c > unicode.MaxASCII {
			isASCII = false
			break
		}
	}

	if isASCII {
		opChain.fail(AssertionFailure{
			Type:   AssertValid,
			Actual: &AssertionValue{s.value},
			Errors: []error{
				errors.New("expected: at least one string character is not ascii"),
			},
		})
	}

	return s
}

// Deprecated: use NotASCII instead.
func (s *String) NotIsASCII() *String {
	return s.NotASCII()
}

// AsNumber parses float from string and returns a new Number instance
// with result.
//
// If base is 10 or omitted, uses strconv.ParseFloat.
// Otherwise, uses strconv.ParseInt or strconv.ParseUint with given base.
//
// Example:
//
//	str := NewString(t, "100")
//	str.AsNumber().IsEqual(100)
//
// Specifying base:
//
//	str.AsNumber(10).IsEqual(100)
//	str.AsNumber(16).IsEqual(256)
func (s *String) AsNumber(base ...int) *Number {
	opChain := s.chain.enter("AsNumber()")
	defer opChain.leave()

	if opChain.failed() {
		return newNumber(opChain, 0)
	}

	if len(base) > 1 {
		opChain.fail(AssertionFailure{
			Type: AssertUsage,
			Errors: []error{
				errors.New("unexpected multiple base arguments"),
			},
		})
		return newNumber(opChain, 0)
	}

	b := 10
	if len(base) != 0 {
		b = base[0]
	}

	var fnum float64
	var inum int64
	var unum uint64
	var err error

	inum, err = strconv.ParseInt(s.value, b, 64)
	fnum = float64(inum)

	if err == nil && int64(fnum) != inum {
		opChain.fail(AssertionFailure{
			Type:   AssertValid,
			Actual: &AssertionValue{s.value},
			Errors: []error{
				errors.New("expected:" +
					" number can be represented as float64 without precision loss"),
			},
		})
		return newNumber(opChain, 0)
	}

	if err != nil && errors.Is(err, strconv.ErrRange) {
		unum, err = strconv.ParseUint(s.value, b, 64)
		fnum = float64(unum)

		if err == nil && uint64(fnum) != unum {
			opChain.fail(AssertionFailure{
				Type:   AssertValid,
				Actual: &AssertionValue{s.value},
				Errors: []error{
					errors.New("expected:" +
						" number can be represented as float64 without precision loss"),
				},
			})
			return newNumber(opChain, 0)
		}
	}

	if err != nil && b == 10 {
		fnum, err = strconv.ParseFloat(s.value, 64)
	}

	if err != nil {
		if b == 10 {
			opChain.fail(AssertionFailure{
				Type:   AssertValid,
				Actual: &AssertionValue{s.value},
				Errors: []error{
					errors.New("expected: string can be parsed to integer or float"),
					err,
				},
			})
		} else {
			opChain.fail(AssertionFailure{
				Type:   AssertValid,
				Actual: &AssertionValue{s.value},
				Errors: []error{
					fmt.Errorf(
						"expected: string can be parsed to integer with base %d",
						base[0]),
					err,
				},
			})
		}
		return newNumber(opChain, 0)
	}

	return newNumber(opChain, fnum)
}

// AsBoolean parses true/false value string and returns a new Boolean instance
// with result.
//
// Accepts string values "true", "True", "false", "False".
//
// Example:
//
//	str := NewString(t, "true")
//	str.AsBoolean().IsTrue()
func (s *String) AsBoolean() *Boolean {
	opChain := s.chain.enter("AsBoolean()")
	defer opChain.leave()

	if opChain.failed() {
		return newBoolean(opChain, false)
	}

	switch s.value {
	case "true", "True":
		return newBoolean(opChain, true)

	case "false", "False":
		return newBoolean(opChain, false)
	}

	opChain.fail(AssertionFailure{
		Type:   AssertValid,
		Actual: &AssertionValue{s.value},
		Errors: []error{
			errors.New("expected: string can be parsed to boolean"),
		},
	})

	return newBoolean(opChain, false)
}

// AsDateTime parses date/time from string and returns a new DateTime instance
// with result.
//
// If format is given, AsDateTime() uses time.Parse() with every given format.
// Otherwise, it uses the list of predefined common formats.
//
// If the string can't be parsed with any format, AsDateTime reports failure
// and returns empty (but non-nil) instance.
//
// Example:
//
//	str := NewString(t, "Tue, 15 Nov 1994 08:12:31 GMT")
//	str.AsDateTime().Lt(time.Now())
//
//	str := NewString(t, "15 Nov 94 08:12 GMT")
//	str.AsDateTime(time.RFC822).Lt(time.Now())
func (s *String) AsDateTime(format ...string) *DateTime {
	opChain := s.chain.enter("AsDateTime()")
	defer opChain.leave()

	if opChain.failed() {
		return newDateTime(opChain, time.Unix(0, 0))
	}

	var formatList []datetimeFormat

	if len(format) != 0 {
		for _, f := range format {
			formatList = append(formatList, datetimeFormat{layout: f})
		}
	} else {
		formatList = []datetimeFormat{
			{http.TimeFormat, "RFC1123+GMT"},

			{time.RFC850, "RFC850"},

			{time.ANSIC, "ANSIC"},
			{time.UnixDate, "Unix"},
			{time.RubyDate, "Ruby"},

			{time.RFC1123, "RFC1123"},
			{time.RFC1123Z, "RFC1123Z"},
			{time.RFC822, "RFC822"},
			{time.RFC822Z, "RFC822Z"},
			{time.RFC3339, "RFC3339"},
			{time.RFC3339Nano, "RFC3339+nano"},
		}
	}

	var (
		tm  time.Time
		err error
	)
	for _, f := range formatList {
		tm, err = time.Parse(f.layout, s.value)
		if err == nil {
			break
		}
	}

	if err != nil {
		if len(formatList) == 1 {
			opChain.fail(AssertionFailure{
				Type:     AssertMatchFormat,
				Actual:   &AssertionValue{s.value},
				Expected: &AssertionValue{formatList[0]},
				Errors: []error{
					errors.New("expected: string can be parsed to datetime" +
						" with given format"),
				},
			})
		} else {
			var expectedFormats []interface{}
			for _, f := range formatList {
				expectedFormats = append(expectedFormats, f)
			}
			opChain.fail(AssertionFailure{
				Type:     AssertMatchFormat,
				Actual:   &AssertionValue{s.value},
				Expected: &AssertionValue{AssertionList(expectedFormats)},
				Errors: []error{
					errors.New("expected: string can be parsed to datetime" +
						" with one of the formats from list"),
				},
			})
		}
		return newDateTime(opChain, time.Unix(0, 0))
	}

	return newDateTime(opChain, tm)
}

type datetimeFormat struct {
	layout string
	name   string
}

func (f datetimeFormat) String() string {
	if f.name != "" {
		return fmt.Sprintf("%q (%s)", f.layout, f.name)
	} else {
		return fmt.Sprintf("%q", f.layout)
	}
}

// Deprecated: use AsNumber instead.
func (s *String) Number() *Number {
	return s.AsNumber()
}

// Deprecated: use AsDateTime instead.
func (s *String) DateTime(layout ...string) *DateTime {
	return s.AsDateTime(layout...)
}
