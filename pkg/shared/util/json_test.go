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
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMustJson(t *testing.T) {
	assert.Equal(t, "1", MustJSON(1))
	t.Run("error", func(t *testing.T) {
		assert.Panics(t, func() {
			type InvalidMarshal struct {
				Channel chan int
			}
			// This should cause a panic because `json.Marshal` cannot handle channels
			MustJSON(&InvalidMarshal{Channel: make(chan int)})
		})
	})
}

func TestUnJSON(t *testing.T) {
	var in int
	MustUnJSON("1", &in)
	assert.Equal(t, 1, in)

	t.Run("invalid json", func(t *testing.T) {
		assert.Panics(t, func() {
			var in int
			MustUnJSON("invalid json", &in)
		})
	})

	t.Run("invalid type for in", func(t *testing.T) {
		assert.Panics(t, func() {
			MustUnJSON("1", 1)
		})
	})

	t.Run("unsupported type for v", func(t *testing.T) {
		assert.Panics(t, func() {
			var in int
			MustUnJSON(1, &in)
		})
	})
}
