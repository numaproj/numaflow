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

func Test_compile_expression(t *testing.T) {
	t.Run("test a simple compile case", func(t *testing.T) {
		b, err := Compile(`json(payload).a`, []byte(`{"a": "b"}`))
		assert.NoError(t, err)
		assert.Equal(t, "b", b)
	})

	t.Run("test nested json compile case", func(t *testing.T) {
		c, err := Compile(`json(payload).a.b`, []byte(`{"a": {"b": "c"}}`))
		assert.NoError(t, err)
		assert.Equal(t, "c", c)
	})

	t.Run("test nested json compile case, list of items", func(t *testing.T) {
		time, err := Compile(`json(payload).item[1].time`, []byte(`{"test": 21, "item": [{"id": 1, "name": "bala", "time": "2021-02-18T21:54:42.123Z"},{"id": 2, "name": "bala", "time": "2021-02-18T21:54:42.123Z"}]}`))
		assert.NoError(t, err)
		assert.Equal(t, "2021-02-18T21:54:42.123Z", time)
	})

	t.Run("test invalid expression", func(t *testing.T) {
		_, err := Compile(`ab\na`, []byte(`{"a": "b"}`))
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "unable to compile expression")
	})
}
