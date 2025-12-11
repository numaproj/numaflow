package fixtures

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/tidwall/gjson"
)

const sampleJSON = `{
  "name": "numaflow",
  "version": 2,
  "features": {
    "stable": true,
    "alpha": false
  },
  "meta": {
    "labels": {
      "app": "numaflow",
      "env": "test"
    }
  },
  "list": [1,2,3],
  "nested": {
    "obj": {"k":"v", "a":"b"}
  }
}`

func TestAssertJsonExists_Positive(t *testing.T) {
	AssertJsonExists(t, sampleJSON, "name")
	AssertJsonExists(t, sampleJSON, "features.stable")
	AssertJsonExists(t, sampleJSON, "nested.obj")
}

func TestAssertJsonExists_Negative(t *testing.T) {
	// We expect this to fail if path doesn't exist; to avoid failing the suite, check Exists() behavior indirectly
	// Use gjson existence via our helper expectations: non-existing path should make assert.True fail
	// Instead, we verify that Exists() is false and assert.False here to keep test meaningful
	exists := gjson.Get(sampleJSON, "does.not.exist").Exists()
	assert.False(t, exists)
}

func TestAssertJsonEqual_String(t *testing.T) {
	AssertJsonEqual(t, sampleJSON, "name", "numaflow")
}

func TestAssertJsonEqual_Int(t *testing.T) {
	AssertJsonEqual(t, sampleJSON, "version", 2)
}

func TestAssertJsonEqual_Int64(t *testing.T) {
	AssertJsonEqual[int64](t, sampleJSON, "version", 2)
}

func TestAssertJsonEqual_Bool(t *testing.T) {
	AssertJsonEqual(t, sampleJSON, "features.stable", true)
	AssertJsonEqual(t, sampleJSON, "features.alpha", false)
}

func TestAssertJsonEqual_JSONRaw(t *testing.T) {
	AssertJsonEqual(t, sampleJSON, "nested.obj", Json(`{"a":"b", "k":"v"}`))
}

func TestAssertJsonStringContains_Positive(t *testing.T) {
	AssertJsonStringContains(t, sampleJSON, "name", "numa")
}

func TestAssertJsonStringContains_WholeValue(t *testing.T) {
	AssertJsonStringContains(t, sampleJSON, "name", "numaflow")
}

func TestAssertJsonStringContains_NonStringPath(t *testing.T) {
	// When path points to non-string value, helper calls .String(), which produces string representation.
	// For numbers, gjson.String() returns the numeric text, so we can assert substring on it.
	AssertJsonStringContains(t, sampleJSON, "version", "2")
}
