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

package expr

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestExpand(t *testing.T) {
	m := map[string]interface{}{
		"name": "test",
		"a":    "2",
		"a.b":  "3",
		"a.c":  "4",
	}
	m1 := Expand(m)
	assert.IsType(t, m1["a"], m1)
	assert.Len(t, m1["a"], 2)
	assert.Equal(t, "test", m1["name"])
	c1 := m1["a"].(map[string]interface{})
	assert.Equal(t, "3", c1["b"])
	assert.Equal(t, "4", c1["c"])
}
