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

package eventtime

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
	headers   map[string]string
}

func (h *testDatum) Headers() map[string]string {
	return h.headers
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

func TestEventTimeExtractor(t *testing.T) {
	t.Run("Missing expression, return error", func(t *testing.T) {
		_, err := New(map[string]string{})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "missing \"expression\"")
	})

	t.Run("Json expression valid, assign a new event time to the message - format not specified", func(t *testing.T) {
		args := map[string]string{"expression": "json(payload).item[1].time"}
		handle, err := New(args)
		assert.NoError(t, err)

		testJsonMsg := `{"test": 21, "item": [{"id": 1, "name": "numa", "time": "2022-02-18T21:54:42.123Z"},{"id": 2, "name": "numa", "time": "2021-02-18T21:54:42.123Z"}]}`
		result := handle(context.Background(), []string{"test-key"}, &testDatum{
			value:     []byte(testJsonMsg),
			eventTime: time.Time{},
			watermark: time.Time{},
		})

		time.Local, _ = time.LoadLocation("UTC")
		expected, _ := time.Parse(time.RFC3339, "2021-02-18T21:54:42.123Z")
		// Verify new event time is assigned to the message.
		assert.True(t, expected.Equal(result.Items()[0].EventTime()))
		// Verify the payload remains unchanged.
		assert.Equal(t, testJsonMsg, string(result.Items()[0].Value()))
	})

	t.Run("Json expression valid, assign a new event time to the message - format specified", func(t *testing.T) {
		args := map[string]string{"expression": "json(payload).item[1].time", "format": time.RFC3339}
		handle, err := New(args)
		assert.NoError(t, err)

		testJsonMsg := `{"test": 21, "item": [{"id": 1, "name": "numa", "time": "2022-02-18T21:54:42.123Z"},{"id": 2, "name": "numa", "time": "2021-02-18T21:54:42.123Z"}]}`
		result := handle(context.Background(), []string{"test-key"}, &testDatum{
			value:     []byte(testJsonMsg),
			eventTime: time.Time{},
			watermark: time.Time{},
		})

		time.Local, _ = time.LoadLocation("UTC")
		expected, _ := time.Parse(time.RFC3339, "2021-02-18T21:54:42.123Z")
		// Verify new event time is assigned to the message.
		assert.True(t, expected.Equal(result.Items()[0].EventTime()))
		// Verify the payload remains unchanged.
		assert.Equal(t, testJsonMsg, string(result.Items()[0].Value()))
	})

	t.Run("Time string not matching user-provided format, pass on the message without assigning new event time", func(t *testing.T) {
		args := map[string]string{"expression": "json(payload).item[1].time", "format": time.ANSIC}
		handle, err := New(args)
		assert.NoError(t, err)

		testInputEventTime := time.Date(2022, 1, 4, 2, 3, 4, 5, time.UTC)
		// Handler receives format as time.ANSIC but in the message, we use time.RFC3339. Format is not matched.
		testJsonMsg := `{"test": 21, "item": [{"id": 1, "name": "numa", "time": "2022-02-18T21:54:42.123Z"},{"id": 2, "name": "numa", "time": "2021-02-18T21:54:42.123Z"}]}`
		result := handle(context.Background(), []string{"test-key"}, &testDatum{
			value:     []byte(testJsonMsg),
			eventTime: testInputEventTime,
			watermark: time.Time{},
		})

		// Verify event time remains unchanged.
		assert.Equal(t, testInputEventTime, result.Items()[0].EventTime())
		// Verify the payload remains unchanged.
		assert.Equal(t, testJsonMsg, string(result.Items()[0].Value()))
	})

	t.Run("Cannot compile json expression, pass on the message without assigning new event time", func(t *testing.T) {
		args := map[string]string{"expression": "json(payload).item[1].non-exist-field-name"}
		handle, err := New(args)
		assert.NoError(t, err)

		testInputEventTime := time.Date(2022, 1, 4, 2, 3, 4, 5, time.UTC)
		testJsonMsg := `{"test": 21, "item": [{"id": 1, "name": "numa", "time": "2022-02-18T21:54:42.123Z"},{"id": 2, "name": "numa", "time": "2021-02-18T21:54:42.123Z"}]}`
		result := handle(context.Background(), []string{"test-key"}, &testDatum{
			value:     []byte(testJsonMsg),
			eventTime: testInputEventTime,
			watermark: time.Time{},
		})

		expected := testInputEventTime
		// Verify event time remains unchanged.
		assert.True(t, expected.Equal(result.Items()[0].EventTime()))
		// Verify the payload remains unchanged.
		assert.Equal(t, testJsonMsg, string(result.Items()[0].Value()))
	})

	t.Run("The time string is in epoch format with a granularity of seconds, assign a new event time to the message", func(t *testing.T) {
		args := map[string]string{"expression": "json(payload).item[1].time"}
		handle, err := New(args)
		assert.NoError(t, err)

		testInputEventTime := time.Date(2022, 1, 4, 2, 3, 4, 5, time.UTC)
		// Handler receives format as time.ANSIC but in the message, we use time.RFC3339. Format is not matched.
		testJsonMsg := `{"test": 21, "item": [{"id": 1, "name": "numa", "time": "1673239888"},{"id": 2, "name": "numa", "time": "1673239888"}]}`
		result := handle(context.Background(), []string{"test-key"}, &testDatum{
			value:     []byte(testJsonMsg),
			eventTime: testInputEventTime,
			watermark: time.Time{},
		})

		time.Local, _ = time.LoadLocation("UTC")
		expected := time.Unix(1673239888, 0)
		// Verify new event time is assigned to the message.
		assert.True(t, expected.Equal(result.Items()[0].EventTime()))
		// Verify the payload remains unchanged.
		assert.Equal(t, testJsonMsg, string(result.Items()[0].Value()))
	})

	t.Run("The time string is in epoch format with a granularity of milliseconds, assign a new event time to the message", func(t *testing.T) {
		args := map[string]string{"expression": "json(payload).item[1].time"}
		handle, err := New(args)
		assert.NoError(t, err)

		testInputEventTime := time.Date(2022, 1, 4, 2, 3, 4, 5, time.UTC)
		testJsonMsg := `{"test": 21, "item": [{"id": 1, "name": "numa", "time": "1673239888123"},{"id": 2, "name": "numa", "time": "1673239888123"}]}`
		result := handle(context.Background(), []string{"test-key"}, &testDatum{
			value:     []byte(testJsonMsg),
			eventTime: testInputEventTime,
			watermark: time.Time{},
		})

		time.Local, _ = time.LoadLocation("UTC")
		expected := time.Unix(1673239888, 123000000)
		// Verify new event time is assigned to the message.
		assert.True(t, expected.Equal(result.Items()[0].EventTime()))
		// Verify the payload remains unchanged.
		assert.Equal(t, testJsonMsg, string(result.Items()[0].Value()))
	})

	t.Run("The time string is ambiguous, pass on the message without assigning new event time", func(t *testing.T) {
		args := map[string]string{"expression": "json(payload).item[1].time"}
		handle, err := New(args)
		assert.NoError(t, err)

		testInputEventTime := time.Date(2022, 1, 4, 2, 3, 4, 5, time.UTC)
		// 04/08/2014 is ambiguous because it could be mm/dd/yyyy or dd/mm/yyyy.
		testJsonMsg := `{"test": 21, "item": [{"id": 1, "name": "numa", "time": "04/08/2014 22:05"},{"id": 2, "name": "numa", "time": "04/08/2014 22:05"}]}`
		result := handle(context.Background(), []string{"test-key"}, &testDatum{
			value:     []byte(testJsonMsg),
			eventTime: testInputEventTime,
			watermark: time.Time{},
		})

		time.Local, _ = time.LoadLocation("UTC")
		expected := testInputEventTime
		// Verify event time remains unchanged.
		assert.True(t, expected.Equal(result.Items()[0].EventTime()))
		// Verify the payload remains unchanged.
		assert.Equal(t, testJsonMsg, string(result.Items()[0].Value()))
	})
}
