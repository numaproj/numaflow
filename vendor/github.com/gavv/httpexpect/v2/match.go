package httpexpect

import (
	"errors"
	"reflect"
)

// Match provides methods to inspect attached regexp match results.
type Match struct {
	chain      *chain
	submatches []string
	names      map[string]int
}

// NewMatch returns a new Match instance.
//
// If reporter is nil, the function panics.
// Both submatches and names may be nil.
//
// Example:
//
//	s := "http://example.com/users/john"
//	r := regexp.MustCompile(`http://(?P<host>.+)/users/(?P<user>.+)`)
//
//	m := NewMatch(t, r.FindStringSubmatch(s), r.SubexpNames())
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
func NewMatch(reporter Reporter, submatches []string, names []string) *Match {
	return newMatch(newChainWithDefaults("Match()", reporter), submatches, names)
}

// NewMatchC returns a new Match instance with config.
//
// Requirements for config are same as for WithConfig function.
// Both submatches and names may be nil.
//
// See NewMatch for usage example.
func NewMatchC(config Config, submatches []string, names []string) *Match {
	return newMatch(newChainWithConfig("Match()", config.withDefaults()), submatches, names)
}

func newMatch(parent *chain, matchList []string, nameList []string) *Match {
	m := &Match{parent.clone(), nil, nil}

	if matchList != nil {
		m.submatches = matchList
	} else {
		m.submatches = []string{}
	}

	m.names = map[string]int{}
	for n, name := range nameList {
		if name != "" {
			m.names[name] = n
		}
	}

	return m
}

// Raw returns underlying submatches attached to Match.
// This is the value originally passed to NewMatch.
//
// Example:
//
//	m := NewMatch(t, submatches, names)
//	assert.Equal(t, submatches, m.Raw())
func (m *Match) Raw() []string {
	return m.submatches
}

// Alias is similar to Value.Alias.
func (m *Match) Alias(name string) *Match {
	opChain := m.chain.enter("Alias(%q)", name)
	defer opChain.leave()

	m.chain.setAlias(name)
	return m
}

// Length returns a new Number instance with number of submatches.
//
// Example:
//
//	m := NewMatch(t, submatches, names)
//	m.Length().IsEqual(len(submatches))
func (m *Match) Length() *Number {
	opChain := m.chain.enter("Length()")
	defer opChain.leave()

	if opChain.failed() {
		return newNumber(opChain, 0)
	}

	return newNumber(opChain, float64(len(m.submatches)))
}

// Index returns a new String instance with submatch for given index.
//
// Note that submatch with index 0 contains the whole match. If index is out
// of bounds, Index reports failure and returns empty (but non-nil) instance.
//
// Example:
//
//	s := "http://example.com/users/john"
//
//	r := regexp.MustCompile(`http://(.+)/users/(.+)`)
//	m := NewMatch(t, r.FindStringSubmatch(s), nil)
//
//	m.Index(0).IsEqual("http://example.com/users/john")
//	m.Index(1).IsEqual("example.com")
//	m.Index(2).IsEqual("john")
func (m *Match) Index(index int) *String {
	opChain := m.chain.enter("Index(%d)", index)
	defer opChain.leave()

	if opChain.failed() {
		return newString(opChain, "")
	}

	if index < 0 || index >= len(m.submatches) {
		opChain.fail(AssertionFailure{
			Type:   AssertInRange,
			Actual: &AssertionValue{index},
			Expected: &AssertionValue{AssertionRange{
				Min: 0,
				Max: len(m.submatches) - 1,
			}},
			Errors: []error{
				errors.New("expected: valid sub-match index"),
			},
		})
		return newString(opChain, "")
	}

	return newString(opChain, m.submatches[index])
}

// Name returns a new String instance with submatch for given name.
//
// If there is no submatch with given name, Name reports failure and returns
// empty (but non-nil) instance.
//
// Example:
//
//	s := "http://example.com/users/john"
//
//	r := regexp.MustCompile(`http://(?P<host>.+)/users/(?P<user>.+)`)
//	m := NewMatch(t, r.FindStringSubmatch(s), r.SubexpNames())
//
//	m.Name("host").IsEqual("example.com")
//	m.Name("user").IsEqual("john")
func (m *Match) Name(name string) *String {
	opChain := m.chain.enter("Name(%q)", name)
	defer opChain.leave()

	if opChain.failed() {
		return newString(opChain, "")
	}

	index, ok := m.names[name]
	if !ok {
		nameList := make([]interface{}, 0, len(m.names))
		for n := range m.names {
			nameList = append(nameList, n)
		}

		opChain.fail(AssertionFailure{
			Type:     AssertBelongs,
			Actual:   &AssertionValue{name},
			Expected: &AssertionValue{AssertionList(nameList)},
			Errors: []error{
				errors.New("expected: existing sub-match name"),
			},
		})

		return newString(opChain, "")
	}

	return newString(opChain, m.submatches[index])
}

// IsEmpty succeeds if submatches array is empty.
//
// Example:
//
//	m := NewMatch(t, submatches, names)
//	m.IsEmpty()
func (m *Match) IsEmpty() *Match {
	opChain := m.chain.enter("IsEmpty()")
	defer opChain.leave()

	if opChain.failed() {
		return m
	}

	if !(len(m.submatches) == 0) {
		opChain.fail(AssertionFailure{
			Type:   AssertEmpty,
			Actual: &AssertionValue{m.submatches},
			Errors: []error{
				errors.New("expected: empty sub-match list"),
			},
		})
	}

	return m
}

// NotEmpty succeeds if submatches array is non-empty.
//
// Example:
//
//	m := NewMatch(t, submatches, names)
//	m.NotEmpty()
func (m *Match) NotEmpty() *Match {
	opChain := m.chain.enter("NotEmpty()")
	defer opChain.leave()

	if opChain.failed() {
		return m
	}

	if !(len(m.submatches) != 0) {
		opChain.fail(AssertionFailure{
			Type:   AssertNotEmpty,
			Actual: &AssertionValue{m.submatches},
			Errors: []error{
				errors.New("expected: non-empty sub-match list"),
			},
		})
	}

	return m
}

// Deprecated: use IsEmpty instead.
func (m *Match) Empty() *Match {
	return m.IsEmpty()
}

// Values succeeds if submatches array, starting from index 1, is equal to
// given array.
//
// Note that submatch with index 0 contains the whole match and is not
// included into this check.
//
// Example:
//
//	s := "http://example.com/users/john"
//	r := regexp.MustCompile(`http://(.+)/users/(.+)`)
//	m := NewMatch(t, r.FindStringSubmatch(s), nil)
//	m.Values("example.com", "john")
func (m *Match) Values(values ...string) *Match {
	opChain := m.chain.enter("Values()")
	defer opChain.leave()

	if opChain.failed() {
		return m
	}

	if values == nil {
		values = []string{}
	}

	if !reflect.DeepEqual(values, m.getValues()) {
		opChain.fail(AssertionFailure{
			Type:     AssertEqual,
			Actual:   &AssertionValue{m.submatches},
			Expected: &AssertionValue{values},
			Errors: []error{
				errors.New("expected: sub-match lists are equal"),
			},
		})
	}

	return m
}

// NotValues succeeds if submatches array, starting from index 1, is not
// equal to given array.
//
// Note that submatch with index 0 contains the whole match and is not
// included into this check.
//
// Example:
//
//	s := "http://example.com/users/john"
//	r := regexp.MustCompile(`http://(.+)/users/(.+)`)
//	m := NewMatch(t, r.FindStringSubmatch(s), nil)
//	m.NotValues("example.com", "bob")
func (m *Match) NotValues(values ...string) *Match {
	opChain := m.chain.enter("NotValues()")
	defer opChain.leave()

	if values == nil {
		values = []string{}
	}

	if reflect.DeepEqual(values, m.getValues()) {
		opChain.fail(AssertionFailure{
			Type:     AssertNotEqual,
			Actual:   &AssertionValue{m.submatches},
			Expected: &AssertionValue{values},
			Errors: []error{
				errors.New("expected: sub-match lists are non-equal"),
			},
		})
	}

	return m
}

func (m *Match) getValues() []string {
	if len(m.submatches) > 1 {
		return m.submatches[1:]
	}
	return []string{}
}
