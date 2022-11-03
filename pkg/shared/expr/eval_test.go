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
	"time"

	"github.com/stretchr/testify/assert"
)

func Test_eval_json(t *testing.T) {
	t.Run("test nil", func(t *testing.T) {
		m := _json(nil)
		assert.Nil(t, m)
	})

	t.Run("test invalid json bytes", func(t *testing.T) {
		assert.Panics(t, func() { _json([]byte("abc")) })
	})

	t.Run("test valid json bytes", func(t *testing.T) {
		m := _json([]byte(`{"a": "b"}`))
		assert.Equal(t, 1, len(m))
		assert.Equal(t, "b", m["a"])
	})

	t.Run("test valid string", func(t *testing.T) {
		m := _json(`{"a": "b"}`)
		assert.Equal(t, 1, len(m))
		assert.Equal(t, "b", m["a"])
	})

	t.Run("test invalid json string", func(t *testing.T) {
		assert.Panics(t, func() { _json("abc") })
	})

	t.Run("test default panic", func(t *testing.T) {
		assert.Panics(t, func() { _json(222) })
	})
}

func Test_eval_string(t *testing.T) {
	t.Run("test string", func(t *testing.T) {
		s := _string("a")
		assert.Equal(t, "a", s)
	})

	t.Run("test bytes", func(t *testing.T) {
		s := _string([]byte("a"))
		assert.Equal(t, "a", s)
	})

	t.Run("test default", func(t *testing.T) {
		s := _string(444)
		assert.Equal(t, "444", s)
	})
}

func Test_eval_int(t *testing.T) {
	t.Run("test bytes", func(t *testing.T) {
		s := _int([]byte("1"))
		assert.Equal(t, 1, s)
	})

	t.Run("test bytes panic", func(t *testing.T) {
		assert.Panics(t, func() { _int([]byte{}) })
	})

	t.Run("test string", func(t *testing.T) {
		s := _int("1")
		assert.Equal(t, 1, s)
	})

	t.Run("test string panic", func(t *testing.T) {
		assert.Panics(t, func() { _int("") })
	})

	t.Run("test float", func(t *testing.T) {
		s := _int(float64(1.2))
		assert.Equal(t, 1, s)
	})

	t.Run("test int", func(t *testing.T) {
		s := _int(1)
		assert.Equal(t, 1, s)
	})

	t.Run("test default panic", func(t *testing.T) {
		assert.Panics(t, func() { _int(time.Second) })
	})
}

func Test_eval_getFuncMap(t *testing.T) {
	a := getFuncMap(map[string]interface{}{"a": "b"})
	assert.Contains(t, a, "a")
	assert.NotContains(t, a, "b")
	assert.Contains(t, a, "string")
	assert.Contains(t, a, "int")
	assert.Contains(t, a, "json")
	assert.Contains(t, a, "sprig")
}

func Test_eval_EvalBool(t *testing.T) {
	t.Run("test good", func(t *testing.T) {
		a, err := EvalBool(`json(payload).a == "b"`, []byte(`{"a": "b"}`))
		assert.NoError(t, err)
		assert.True(t, a)
	})

	t.Run("test bad", func(t *testing.T) {
		a, err := EvalBool(`json(payload).a == "c"`, []byte(`{"a": "b"}`))
		assert.NoError(t, err)
		assert.False(t, a)
	})

	t.Run("test invalid expression", func(t *testing.T) {
		_, err := EvalBool(`json(payload).a`, []byte(`{"a": "b"}`))
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "unable to cast expression result")
	})

	t.Run("test invalid expression again", func(t *testing.T) {
		_, err := EvalBool(`ab\na`, []byte(`{"a": "b"}`))
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "unable to evaluate expression")
	})
}
