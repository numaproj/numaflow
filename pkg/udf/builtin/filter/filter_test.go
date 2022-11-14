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

package filter

import (
	"context"
	"encoding/base64"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

var _key = ""
var jsonMsg = `{"test": 21, "item": [{"id": 1, "name": "bala"},{"id": 2, "name": "bala"}]}`
var strMsg = `welcome to numaflow`
var base64Msg = base64.StdEncoding.EncodeToString([]byte(strMsg))

type testDatum struct {
	value     []byte
	eventTime time.Time
	watermark time.Time
}

func (h *testDatum) Value() []byte {
	return h.value
}

func (h *testDatum) EventTime() time.Time {
	return h.eventTime
}

func (h *testDatum) Watermark() time.Time {
	return h.watermark
}

func TestExpression(t *testing.T) {
	t.Run("missing expression", func(t *testing.T) {
		_, err := New(map[string]string{})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "missing")
	})

	t.Run("Json expression valid", func(t *testing.T) {
		args := map[string]string{"expression": "int(json(payload).item[1].id) == 2"}

		handle, err := New(args)
		assert.NoError(t, err)

		result := handle(context.Background(), _key, &testDatum{
			value:     []byte(jsonMsg),
			eventTime: time.Time{},
			watermark: time.Time{},
		})
		assert.Equal(t, jsonMsg, string(result.Items()[0].Value))
	})

	t.Run("invalid expression", func(t *testing.T) {
		args := map[string]string{"expression": "ab\nc"}

		handle, err := New(args)
		assert.NoError(t, err)

		result := handle(context.Background(), _key, &testDatum{
			value:     []byte(jsonMsg),
			eventTime: time.Time{},
			watermark: time.Time{},
		})
		assert.Equal(t, "", string(result.Items()[0].Value))
	})

	t.Run("Json expression invalid", func(t *testing.T) {
		args := map[string]string{"expression": "int(json(payload).item[1].id) == 3"}

		handle, err := New(args)
		assert.NoError(t, err)

		result := handle(context.Background(), _key, &testDatum{
			value:     []byte(jsonMsg),
			eventTime: time.Time{},
			watermark: time.Time{},
		})
		assert.Equal(t, "", string(result.Items()[0].Value))
	})

	t.Run("String expression invalid", func(t *testing.T) {
		args := map[string]string{"expression": "sprig.contains('hello', payload)"}

		handle, err := New(args)
		assert.NoError(t, err)

		result := handle(context.Background(), _key, &testDatum{
			value:     []byte(jsonMsg),
			eventTime: time.Time{},
			watermark: time.Time{},
		})
		assert.Equal(t, "", string(result.Items()[0].Value))
	})

	t.Run("base64 expression valid", func(t *testing.T) {
		args := map[string]string{"expression": "sprig.contains('numaflow', sprig.b64dec(payload))"}

		handle, err := New(args)
		assert.NoError(t, err)

		result := handle(context.Background(), _key, &testDatum{
			value:     []byte(base64Msg),
			eventTime: time.Time{},
			watermark: time.Time{},
		})
		assert.Equal(t, base64Msg, string(result.Items()[0].Value))
	})

}
