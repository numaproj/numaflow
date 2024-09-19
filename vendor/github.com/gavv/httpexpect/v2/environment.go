package httpexpect

import (
	"errors"
	"sort"
	"sync"
	"time"

	"github.com/gobwas/glob"
)

// Environment provides a container for arbitrary data shared between tests.
//
// Example:
//
//	env := NewEnvironment(t)
//	env.Put("key", "value")
//	value := env.GetString("key")
type Environment struct {
	mu    sync.RWMutex
	chain *chain
	data  map[string]interface{}
}

// NewEnvironment returns a new Environment.
//
// If reporter is nil, the function panics.
//
// Example:
//
//	env := NewEnvironment(t)
func NewEnvironment(reporter Reporter) *Environment {
	return newEnvironment(newChainWithDefaults("Environment()", reporter))
}

// NewEnvironmentC returns a new Environment with config.
//
// Requirements for config are same as for WithConfig function.
//
// Example:
//
//	env := NewEnvironmentC(config)
func NewEnvironmentC(config Config) *Environment {
	return newEnvironment(newChainWithConfig("Environment()", config.withDefaults()))
}

func newEnvironment(parent *chain) *Environment {
	return &Environment{
		chain: parent.clone(),
		data:  make(map[string]interface{}),
	}
}

// Put saves the value with key in the environment.
//
// Example:
//
//	env := NewEnvironment(t)
//	env.Put("key1", "str")
//	env.Put("key2", 123)
func (e *Environment) Put(key string, value interface{}) {
	opChain := e.chain.enter("Put(%q)", key)
	defer opChain.leave()

	e.mu.Lock()
	defer e.mu.Unlock()

	e.data[key] = value
}

// Delete removes the value with key from the environment.
//
// Example:
//
//	env := NewEnvironment(t)
//	env.Put("key1", "str")
//	env.Delete("key1")
func (e *Environment) Delete(key string) {
	opChain := e.chain.enter("Delete(%q)", key)
	defer opChain.leave()

	e.mu.Lock()
	defer e.mu.Unlock()

	delete(e.data, key)
}

// Clear will delete all key value pairs from the environment
//
// Example:
//
//	env := NewEnvironment(t)
//	env.Put("key1", 123)
//	env.Put("key2", 456)
//	env.Clear()
func (e *Environment) Clear() {
	opChain := e.chain.enter("Clear()")
	defer opChain.leave()

	e.mu.Lock()
	defer e.mu.Unlock()

	e.data = make(map[string]interface{})
}

// Has returns true if value exists in the environment.
//
// Example:
//
//	if env.Has("key1") {
//	   ...
//	}
func (e *Environment) Has(key string) bool {
	opChain := e.chain.enter("Has(%q)", key)
	defer opChain.leave()

	e.mu.RLock()
	defer e.mu.RUnlock()

	_, ok := e.data[key]
	return ok
}

// Get returns value stored in the environment.
//
// If value does not exist, reports failure and returns nil.
//
// Example:
//
//	value1 := env.Get("key1").(string)
//	value2 := env.Get("key1").(int)
func (e *Environment) Get(key string) interface{} {
	opChain := e.chain.enter("Get(%q)", key)
	defer opChain.leave()

	e.mu.RLock()
	defer e.mu.RUnlock()

	value, _ := envValue(opChain, e.data, key)

	return value
}

// GetBool returns value stored in the environment, casted to bool.
//
// If value does not exist, or is not bool, reports failure and returns false.
//
// Example:
//
//	value := env.GetBool("key")
func (e *Environment) GetBool(key string) bool {
	opChain := e.chain.enter("GetBool(%q)", key)
	defer opChain.leave()

	e.mu.RLock()
	defer e.mu.RUnlock()

	value, ok := envValue(opChain, e.data, key)
	if !ok {
		return false
	}

	casted, ok := value.(bool)
	if !ok {
		opChain.fail(AssertionFailure{
			Type:   AssertType,
			Actual: &AssertionValue{value},
			Errors: []error{
				errors.New("expected: bool value"),
			},
		})
		return false
	}

	return casted
}

// GetInt returns value stored in the environment, casted to int64.
//
// If value does not exist, or is not signed or unsigned integer that can be
// represented as int without overflow, reports failure and returns zero.
//
// Example:
//
//	value := env.GetInt("key")
func (e *Environment) GetInt(key string) int {
	opChain := e.chain.enter("GetInt(%q)", key)
	defer opChain.leave()

	e.mu.RLock()
	defer e.mu.RUnlock()

	value, ok := envValue(opChain, e.data, key)
	if !ok {
		return 0
	}

	var casted int

	const (
		intSize = 32 << (^uint(0) >> 63) // 32 or 64
		maxInt  = 1<<(intSize-1) - 1
		minInt  = -1 << (intSize - 1)
	)

	switch num := value.(type) {
	case int8:
		casted = int(num)
		ok = (int64(num) >= minInt) && (int64(num) <= maxInt)
	case int16:
		casted = int(num)
		ok = (int64(num) >= minInt) && (int64(num) <= maxInt)
	case int32:
		casted = int(num)
		ok = (int64(num) >= minInt) && (int64(num) <= maxInt)
	case int64:
		casted = int(num)
		ok = (int64(num) >= minInt) && (int64(num) <= maxInt)
	case int:
		casted = num
		ok = (int64(num) >= minInt) && (int64(num) <= maxInt)

	case uint8:
		casted = int(num)
		ok = (uint64(num) <= maxInt)
	case uint16:
		casted = int(num)
		ok = (uint64(num) <= maxInt)
	case uint32:
		casted = int(num)
		ok = (uint64(num) <= maxInt)
	case uint64:
		casted = int(num)
		ok = (uint64(num) <= maxInt)
	case uint:
		casted = int(num)
		ok = (uint64(num) <= maxInt)

	default:
		opChain.fail(AssertionFailure{
			Type:   AssertType,
			Actual: &AssertionValue{value},
			Errors: []error{
				errors.New("expected: signed or unsigned integer"),
			},
		})
		return 0
	}

	if !ok {
		opChain.fail(AssertionFailure{
			Type:     AssertInRange,
			Actual:   &AssertionValue{value},
			Expected: &AssertionValue{AssertionRange{minInt, maxInt}},
			Errors: []error{
				errors.New(
					"expected: value can be represented as int without overflow"),
			},
		})
		return 0
	}

	return casted
}

// GetFloat returns value stored in the environment, casted to float64.
//
// If value does not exist, or is not floating point value, reports failure
// and returns zero value.
//
// Example:
//
//	value := env.GetFloat("key")
func (e *Environment) GetFloat(key string) float64 {
	opChain := e.chain.enter("GetFloat(%q)", key)
	defer opChain.leave()

	e.mu.RLock()
	defer e.mu.RUnlock()

	value, ok := envValue(opChain, e.data, key)
	if !ok {
		return 0
	}

	var casted float64

	switch num := value.(type) {
	case float32:
		casted = float64(num)

	case float64:
		casted = num

	default:
		opChain.fail(AssertionFailure{
			Type:   AssertType,
			Actual: &AssertionValue{value},
			Errors: []error{
				errors.New("expected: float32 or float64"),
			},
		})
		return 0
	}

	return casted
}

// GetString returns value stored in the environment, casted to string.
//
// If value does not exist, or is not string, reports failure and returns
// empty string.
//
// Example:
//
//	value := env.GetString("key")
func (e *Environment) GetString(key string) string {
	opChain := e.chain.enter("GetString(%q)", key)
	defer opChain.leave()

	e.mu.RLock()
	defer e.mu.RUnlock()

	value, ok := envValue(opChain, e.data, key)
	if !ok {
		return ""
	}

	casted, ok := value.(string)
	if !ok {
		opChain.fail(AssertionFailure{
			Type:   AssertType,
			Actual: &AssertionValue{value},
			Errors: []error{
				errors.New("expected: string value"),
			},
		})
		return ""
	}

	return casted
}

// GetBytes returns value stored in the environment, casted to []byte.
//
// If value does not exist, or is not []byte slice, reports failure and returns nil.
//
// Example:
//
//	value := env.GetBytes("key")
func (e *Environment) GetBytes(key string) []byte {
	opChain := e.chain.enter("GetBytes(%q)", key)
	defer opChain.leave()

	e.mu.RLock()
	defer e.mu.RUnlock()

	value, ok := envValue(opChain, e.data, key)
	if !ok {
		return nil
	}

	casted, ok := value.([]byte)
	if !ok {
		opChain.fail(AssertionFailure{
			Type:   AssertType,
			Actual: &AssertionValue{value},
			Errors: []error{
				errors.New("expected: []byte slice"),
			},
		})
		return nil
	}

	return casted
}

// GetDuration returns value stored in the environment, casted to time.Duration.
//
// If value does not exist, is not time.Duration, reports failure and returns
// zero duration.
//
// Example:
//
//	value := env.GetDuration("key")
func (e *Environment) GetDuration(key string) time.Duration {
	opChain := e.chain.enter("GetDuration(%q)", key)
	defer opChain.leave()

	e.mu.RLock()
	defer e.mu.RUnlock()

	value, ok := envValue(opChain, e.data, key)
	if !ok {
		return time.Duration(0)
	}

	casted, ok := value.(time.Duration)
	if !ok {
		opChain.fail(AssertionFailure{
			Type:   AssertType,
			Actual: &AssertionValue{value},
			Errors: []error{
				errors.New("expected: time.Duration value"),
			},
		})
		return time.Duration(0)
	}

	return casted
}

// GetTime returns value stored in the environment, casted to time.Time.
//
// If value does not exist, is not time.Time, reports failure and returns
// zero time.
//
// Example:
//
//	value := env.GetTime("key")
func (e *Environment) GetTime(key string) time.Time {
	opChain := e.chain.enter("GetTime(%q)", key)
	defer opChain.leave()

	e.mu.RLock()
	defer e.mu.RUnlock()

	value, ok := envValue(opChain, e.data, key)
	if !ok {
		return time.Unix(0, 0)
	}

	casted, ok := value.(time.Time)
	if !ok {
		opChain.fail(AssertionFailure{
			Type:   AssertType,
			Actual: &AssertionValue{value},
			Errors: []error{
				errors.New("expected: time.Time value"),
			},
		})
		return time.Unix(0, 0)
	}

	return casted
}

// List returns a sorted slice of keys.
//
// Example:
//
//	env := NewEnvironment(t)
//
//	for _, key := range env.List() {
//		...
//	}
func (e *Environment) List() []string {
	opChain := e.chain.enter("List()")
	defer opChain.leave()

	e.mu.RLock()
	defer e.mu.RUnlock()

	keys := []string{}

	for key := range e.data {
		keys = append(keys, key)
	}

	sort.Slice(keys, func(i, j int) bool {
		return keys[i] < keys[j]
	})
	return keys
}

// Glob accepts a glob pattern and returns a sorted slice of
// keys that match the pattern.
//
// If the pattern is invalid, reports failure and returns an
// empty slice.
//
// Example:
//
//	env := NewEnvironment(t)
//
//	for _, key := range env.Glob("foo.*") {
//		...
//	}
func (e *Environment) Glob(pattern string) []string {
	opChain := e.chain.enter("Glob(%q)", pattern)
	defer opChain.leave()

	e.mu.RLock()
	defer e.mu.RUnlock()

	glb, err := glob.Compile(pattern)
	if err != nil {
		opChain.fail(AssertionFailure{
			Type: AssertUsage,
			Errors: []error{
				errors.New("unexpected invalid glob pattern"),
			},
		})
		return []string{}
	}

	keys := []string{}
	for key := range e.data {
		if glb.Match(key) {
			keys = append(keys, key)
		}
	}

	sort.Slice(keys, func(i, j int) bool {
		return keys[i] < keys[j]
	})
	return keys
}

func envValue(chain *chain, env map[string]interface{}, key string) (interface{}, bool) {
	v, ok := env[key]

	if !ok {
		chain.fail(AssertionFailure{
			Type:     AssertContainsKey,
			Actual:   &AssertionValue{env},
			Expected: &AssertionValue{key},
			Errors: []error{
				errors.New("expected: environment contains key"),
			},
		})
		return nil, false
	}

	return v, true
}
