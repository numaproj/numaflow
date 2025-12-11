/*
Copyright 2022 The Numaproj Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package fixtures

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/tidwall/gjson"
)

type Json string

// AssertJsonExists asserts the path exists in the json
func AssertJsonExists(t *testing.T, json string, path string) {
	assert.True(t, gjson.Get(json, path).Exists())
}

// AssertJsonEqual asserts the path value of the json equals to the expected value
func AssertJsonEqual[T string | int | int64 | bool | Json](t *testing.T, json string, path string, expected T) {
	AssertJsonExists(t, json, path)
	switch any(expected).(type) {
	case string:
		assert.Equal(t, expected, gjson.Get(json, path).String())
	case int64:
		assert.Equal(t, expected, gjson.Get(json, path).Int())
	case int:
		assert.Equal(t, expected, int(gjson.Get(json, path).Int()))
	case bool:
		assert.True(t, gjson.Get(json, path).IsBool())
		assert.Equal(t, expected, gjson.Get(json, path).Bool())
	case Json:
		j := any(expected).(Json)
		assert.JSONEq(t, string(j), gjson.Get(json, path).Raw)
	default:
		assert.Fail(t, "unknow type")
	}
}

func AssertJsonStringContains(t *testing.T, json string, path string, expectedContains string) {
	AssertJsonExists(t, json, path)
	assert.Contains(t, gjson.Get(json, path).String(), expectedContains)
}
