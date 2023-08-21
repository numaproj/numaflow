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

package timeextractionfilter

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type testDatum struct {
	value     []byte
	eventTime time.Time
	watermark time.Time
	metadata  testDatumMetadata
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

type testDatumMetadata struct {
	id           string
	numDelivered uint64
}

func (t testDatumMetadata) ID() string {
	return t.id
}

func (t testDatumMetadata) NumDelivered() uint64 {
	return t.numDelivered
}

var (
	testJsonMsg = `{"test": 21, "item": [{"id": 1, "name": "numa", "time": "2022-02-18T21:54:42.123Z"},{"id": 2, "name": "numa", "time": "2021-02-18T21:54:42.123Z"}]}`
)

func TestFilterEventTime(t *testing.T) {

	t.Run("Missing both expressions, return error", func(t *testing.T) {
		_, err := New(map[string]string{})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "missing \"filterExpr\"")
	})

	t.Run("Missing eventTime expr, return error", func(t *testing.T) {
		_, err := New(map[string]string{"filterExpr": ""})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "missing \"eventTimeExpr\"")
	})

	t.Run("Missing filter expr, return error", func(t *testing.T) {
		_, err := New(map[string]string{"eventTimeExpr": ""})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "missing \"filterExpr\"")
	})

	t.Run("Valid JSON expression for filter and eventTimeExtractor", func(t *testing.T) {
		handle, err := New(map[string]string{"filterExpr": "int(json(payload).item[1].id) == 2", "eventTimeExpr": "json(payload).item[1].time", "eventTimeFormat": time.RFC3339})
		assert.NoError(t, err)

		result := handle(context.Background(), []string{"test-key"}, &testDatum{
			value:     []byte(testJsonMsg),
			eventTime: time.Time{},
			watermark: time.Time{},
		})

		// check that messsage has not changed
		assert.Equal(t, testJsonMsg, string(result.Items()[0].Value()))

		// check that event time has changed
		time.Local, _ = time.LoadLocation("UTC")
		expected, _ := time.Parse(time.RFC3339, "2021-02-18T21:54:42.123Z")
		assert.True(t, expected.Equal(result.Items()[0].EventTime()))
	})

	t.Run("Invalid JSON expression for filter", func(t *testing.T) {
		handle, err := New(map[string]string{"filterExpr": "int(json(payload).item[1].id) == 3", "eventTimeExpr": "json(payload).item[1].time", "eventTimeFormat": time.RFC3339})
		assert.NoError(t, err)

		result := handle(context.Background(), []string{"test-key"}, &testDatum{
			value:     []byte(testJsonMsg),
			eventTime: time.Time{},
			watermark: time.Time{},
		})

		assert.Equal(t, "", string(result.Items()[0].Value()))
	})

	t.Run("Valid JSON expression for filter, incorrect format to eventTime", func(t *testing.T) {
		handle, err := New(map[string]string{"filterExpr": "int(json(payload).item[1].id) == 2", "eventTimeExpr": "json(payload).item[1].time", "eventTimeFormat": time.ANSIC})
		assert.NoError(t, err)

		testInputEventTime := time.Date(2022, 1, 4, 2, 3, 4, 5, time.UTC)
		result := handle(context.Background(), []string{"test-key"}, &testDatum{
			value:     []byte(testJsonMsg),
			eventTime: testInputEventTime,
			watermark: time.Time{},
		})

		assert.Equal(t, testInputEventTime, result.Items()[0].EventTime())
		assert.Equal(t, testJsonMsg, string(result.Items()[0].Value()))
	})

}
