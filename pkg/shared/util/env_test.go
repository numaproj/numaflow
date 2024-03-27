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

package util

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLookupEnvStringOr(t *testing.T) {
	assert.Equal(t, LookupEnvStringOr("fake_env", "hello"), "hello")
	assert.Equal(t, LookupEnvStringOr("HOME", "#")[0], "/"[0])
}

func TestLookupEnvIntOr(t *testing.T) {
	assert.Equal(t, LookupEnvIntOr("fake_int_env", 3), 3)
	os.Setenv("fake_int_env", "4")
	assert.Equal(t, LookupEnvIntOr("fake_int_env", 3), 4)
}

func TestLookupEnvBoolOr(t *testing.T) {
	assert.Equal(t, LookupEnvBoolOr("fake_bool_env", false), false)
	os.Setenv("fake_bool_env", "1")
	assert.Equal(t, LookupEnvBoolOr("fake_bool_env", false), true)
	os.Setenv("fake_bool_env", "True")
	assert.Equal(t, LookupEnvBoolOr("fake_bool_env", false), true)
	os.Setenv("fake_bool_env", "TRUE")
	assert.Equal(t, LookupEnvBoolOr("fake_bool_env", false), true)
	os.Setenv("fake_bool_env", "False")
	assert.Equal(t, LookupEnvBoolOr("fake_bool_env", false), false)
	os.Setenv("fake_bool_env", "5")
	assert.Panics(t, func() { LookupEnvBoolOr("fake_bool_env", false) })
}

func TestLookUpEnvListOr(t *testing.T) {
	assert.Equal(t, LookUpEnvListOr([]string{"key1"}, []string{"key1"}), false)
	os.Setenv("fake_bool_env", "1")
}
