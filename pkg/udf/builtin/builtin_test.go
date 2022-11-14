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

package builtin

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetExecutors(t *testing.T) {
	t.Run("test good", func(t *testing.T) {
		builtins := []Builtin{
			{
				Name: "cat",
			},
			{
				Name:   "filter",
				KWArgs: map[string]string{"expression": `json(payload).a=="b"`},
			},
		}
		for _, b := range builtins {
			e, err := b.executor()
			assert.NoError(t, err)
			assert.NotNil(t, e)
		}
	})

	t.Run("test bad", func(t *testing.T) {
		b := &Builtin{
			Name: "catt",
		}
		_, err := b.executor()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "unrecognized function")
	})
}

func Test_Start(t *testing.T) {

}
