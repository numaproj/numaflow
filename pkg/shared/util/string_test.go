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
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRandomString(t *testing.T) {
	str := RandomString(20)
	assert.Equal(t, 20, len(str))
}

func TestRandomLowercaseString(t *testing.T) {
	str := RandomLowerCaseString(20)
	assert.Equal(t, 20, len(str))
	assert.Equal(t, str, strings.ToLower(str))
}

func TestStringSliceContains(t *testing.T) {
	assert.False(t, StringSliceContains(nil, "b"))
	assert.False(t, StringSliceContains([]string{}, "b"))
	list := []string{"a", "b", "c"}
	assert.True(t, StringSliceContains(list, "b"))
	assert.False(t, StringSliceContains(list, "e"))
}
